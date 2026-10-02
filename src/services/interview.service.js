/* eslint-disable no-await-in-loop */
//
// Orquestrador do worker de fila e da integração com WhatsApp/backend.
//
// [NOTA SOBRE eslint-disable no-await-in-loop]
// O `await` dentro de loops é intencional em dois sítios:
//
//   1. _processPendingTurns: iteramos telefones da fila para
//      processar cada um sequencialmente. Processar em paralelo
//      quebraria a ordem FIFO por telefone.
//
//   2. _processPhoneQueue: iteramos itens do mesmo telefone. Cada
//      item tem de terminar antes do próximo começar; é assim que
//      a entrega pendente (kind=delivery) não é ultrapassada por
//      um novo turno.
//
// [FIX-CAP-1]  _assertYaneCapabilities corre no boot.
// [FIX-CAP-2]  Guardas de métodos antes de chamar.
// [FIX-QUEUE-1] _metricSet com labels condicionais.
// [FIX-PROCESSING] turn.processing=true → requeue, não ack.
//
// [FIX-ROUTING-1]
//   _resolveInterviewForIncoming passa a delegar ao
//   yane.resolveInboundMessage(). O antigo findActiveInterviewByPhone
//   devolvia sempre a entrevista mais recente e, quando o mesmo
//   telefone tinha duas entrevistas activas, o turno ia para a
//   errada sem que ninguém soubesse.
//
// [FIX-ROUTING-2]
//   Disambiguação em estado de sessão (Redis + TTL 5min). Quando
//   o backend devolve confidence=ambiguous, o bot guarda os
//   candidatos e pergunta ao candidato. A resposta numérica é
//   resolvida localmente sem voltar a chamar o backend.
//
// [FIX-ROUTING-3]
//   replied_to_message_id propagado em todo o pipeline. O Baileys
//   entrega o ID da mensagem original do convite em
//   contextInfo.stanzaId; esse ID alimenta o passo 1 do resolver
//   (reply_to), que é o caminho mais fiável.
//
// [FIX-ROUTING-4]
//   Cache local `resolved:<phone>` com TTL 5min. Depois de o
//   backend confirmar uma entrevista para o telefone, evitamos
//   re-resolver em cada turno consecutivo do mesmo candidato.

'use strict';

const { randomUUID } = require('crypto');
const pino = require('pino');

const RedisService = require('./redis.service');
const YaneIntegrationService = require('./yane-integration.service');
const metrics = require('./metrics.service');

// =============================================================================
// CONFIGURAÇÃO
// =============================================================================

const CONFIG = Object.freeze({
  typingMsPerChar: 22,
  typingMaxMs: 2_600,

  pendingPhonesKey: 'pending:phones',
  pendingTurnKeyPrefix: 'pending:turn',
  deadLetterKey: 'dead:interview',
  queueSchemaVersion: 1,

  pendingMaxAttempts: 10,
  maxPendingPerPhone: 20,

  backoffBaseMs: 30_000,
  backoffMaxMs: 15 * 60_000,
  deadLetterRetryMs: 60 * 60_000,

  workerIntervalMs: 10_000,
  workerBatchSize: 20,
  workerMaxPerPhone: 3,

  lockTtlSeconds: 300,
  lockRefreshMs: 30_000,

  deadLetterReportIntervalMs: 60 * 60_000,
  maxLookupAttempts: 5,

  softRecoveryMessage: 'Um momento, por favor. Já lhe respondo.',
  queueFullMessage:
    'Recebi a sua mensagem, mas estou com muitos pedidos. ' +
    'Vou responder assim que possível.',

  fallbackCooldownMs: 60_000,
  fallbackSweepThreshold: 1_000,
  fallbackHardLimit: 5_000,

  bubblePauseMs: 300,

  // [FIX-ROUTING-2] TTL da sessão de disambiguação.
  disambiguationTtlSeconds: 5 * 60,
  // [FIX-ROUTING-4] TTL do cache local de entrevista resolvida.
  resolvedInterviewTtlSeconds: 5 * 60,
});

// [FIX-CAP-1] Métodos que o YaneIntegrationService tem de
// expor para o InterviewService funcionar. Validados no boot.
const REQUIRED_YANE_METHODS = Object.freeze([
  'sendInterviewTurn',
  'findActiveInterviewByPhone',   // mantido para compatibilidade
  'resolveInboundMessage',        // [FIX-ROUTING-1] novo
  'reportUnmatchedIncoming',
  'sendMessageStatus',
  'healthCheck',
]);

// =============================================================================
// HELPERS
// =============================================================================

function clean(value) {
  return String(value ?? '')
    .replace(/^["'`]+|["'`]+$ /g, '')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{4,}/g, '\n\n')
    .trim();
}

function normalizePhone(phone) {
  const digits = String(phone ?? '').replace(/\D/g, '');
  if (!digits) return '';

  if (digits.startsWith('00258')) return digits.slice(2);
  if (digits.startsWith('258')) return digits;
  if (digits.length === 9) return `258${digits}`;
  if (digits.length === 10 && digits.startsWith('0')) {
    return `258${digits.slice(1)}`;
  }

  return '';
}

function maskPhone(phone) {
  const normalized = normalizePhone(phone) || String(phone || '');
  const digits = normalized.replace(/\D/g, '');

  if (digits.length < 6) return '***';

  return `${digits.slice(0, 3)}***${digits.slice(-3)}`;
}

function maskIdentifier(value) {
  const raw = String(value ?? '');
  if (raw.length <= 6) return '***';

  return `${raw.slice(0, 3)}***${raw.slice(-3)}`;
}

function safeNumber(value, fallback) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function errorMessage(error) {
  return clean(
    error?.message || error?.error || 'unknown_error'
  ).slice(0, 500);
}

function getRetryAfterMs(error) {
  const value = Number(error?.retryAfterMs);
  return Number.isFinite(value) && value > 0 ? value : null;
}

function isRetryableError(error) {
  if (typeof error?.retryable === 'boolean') return error.retryable;
  return true;
}

function isFailedSendResult(result) {
  return (
    result === false ||
    (result && typeof result === 'object' && result.status === 'failed')
  );
}

// =============================================================================
// SERVIÇO
// =============================================================================

class InterviewService {
  constructor(redis = null, options = {}) {
    this.redis = redis || new RedisService();
    this._ownsRedis = !redis;

    this.yane = options.yane || new YaneIntegrationService();
    this.whatsapp = options.whatsapp || null;

    this.typingMsPerChar = Math.max(
      0,
      safeNumber(
        process.env.WHATSAPP_TYPING_MS_PER_CHAR,
        CONFIG.typingMsPerChar
      )
    );

    this.typingMax = Math.max(
      0,
      safeNumber(
        process.env.WHATSAPP_TYPING_MAX_MS,
        CONFIG.typingMaxMs
      )
    );

    this._initialized = false;
    this._initializing = null;
    this._closing = false;
    this._lifecycleGeneration = 0;

    this._workerTimer = null;
    this._deadLetterTimer = null;
    this._workerRunning = false;
    this._workerPromise = null;

    // [FIX-CAP-1] Flag para evitar repetir o aviso de capacidades.
    this._yaneCapabilitiesChecked = false;

    this._fallbackSentAt = new Map();
    this._fallbackInFlight = new Set();

    this.logger = pino({
      level: process.env.LOG_LEVEL || 'info',
      base: { service: 'interview' },
    });
  }

  // ===========================================================================
  // LOGGING / MÉTRICAS
  // ===========================================================================

  _log(level, message, context = {}) {
    try {
      const method = this.logger?.[level];
      if (typeof method !== 'function') return;

      method.call(this.logger, context, message);
    } catch (_) {
      // Logging é best-effort.
    }
  }

  _logContext(phone, extra = {}) {
    const context = {};
    if (phone) context.phone = maskPhone(phone);

    for (const [key, value] of Object.entries(extra)) {
      if (value === undefined || value === null) continue;

      if (key === 'preview' || key === 'message' || key === 'body') {
        continue;
      }

      if (key === 'remoteJid' || key === 'jid' || key === 'lid') {
        context[key] = maskIdentifier(value);
        continue;
      }

      context[key] = value;
    }

    return context;
  }

  _metricIncrement(name, labels = {}) {
    try {
      metrics[name]?.inc?.(labels);
    } catch (_) {}
  }

  _metricSet(name, value, labels = null) {
    try {
      const metric = metrics[name];
      if (!metric || typeof metric.set !== 'function') return;

      const hasLabels =
        labels &&
        typeof labels === 'object' &&
        Object.keys(labels).length > 0;

      if (hasLabels) {
        metric.set(labels, value);
      } else {
        metric.set(value);
      }
    } catch (_) {}
  }

  log(message, phone = null, extra = {}) {
    this._log('info', message, this._logContext(phone, extra));
  }

  logWarn(message, phone = null, extra = {}) {
    this._log('warn', message, this._logContext(phone, extra));
  }

  logError(message, error = null, phone = null, extra = {}) {
    const context = this._logContext(phone, extra);

    if (error) {
      context.error = errorMessage(error);
      context.code = error?.code || null;
      context.status = error?.status || null;
      context.retryable = isRetryableError(error);
    }

    this._log('error', message, context);
  }

  logDebug(message, phone = null, extra = {}) {
    this._log('debug', message, this._logContext(phone, extra));
  }

  // ===========================================================================
  // LIFECYCLE
  // ===========================================================================

  async initialize() {
    if (this._initialized && !this._closing) return;
    if (this._initializing) return this._initializing;

    const generation = ++this._lifecycleGeneration;
    this._closing = false;

    this._initializing = this._initialize(generation);

    try {
      await this._initializing;
    } finally {
      this._initializing = null;
    }
  }

  async _initialize(generation) {
    await this.redis.initialize();

    if (this._isStaleLifecycle(generation)) return;

    this._assertYaneCapabilities();

    await this._checkBackendHealthAtBoot();

    if (this._isStaleLifecycle(generation)) return;

    this._initialized = true;

    this._startWorker();
    this._triggerWorker();

    this.log('InterviewService inicializado.');
  }

  _assertYaneCapabilities() {
    if (this._yaneCapabilitiesChecked) return true;

    this._yaneCapabilitiesChecked = true;

    const missing = [];

    for (const method of REQUIRED_YANE_METHODS) {
      if (typeof this.yane?.[method] !== 'function') {
        missing.push(method);
      }
    }

    if (missing.length === 0) return true;

    try {
      console.error(
        '[INTERVIEW] YaneIntegrationService incompleto. ' +
          'Métodos em falta: ' + missing.join(', ') + '. ' +
          'Isto normalmente significa que o processo Node está a ' +
          'correr uma versão antiga do módulo. Reinicie o processo ' +
          'após substituir o ficheiro yane-integration.service.js.'
      );
    } catch (_) {}

    this._log('error', 'YaneIntegrationService incompleto.', {
      missingMethods: missing,
    });

    return false;
  }

  _isStaleLifecycle(generation) {
    return (
      this._closing ||
      generation !== this._lifecycleGeneration
    );
  }

  async _checkBackendHealthAtBoot() {
    if (typeof this.yane?.healthCheck !== 'function') {
      this.logWarn('yane.healthCheck() indisponível no boot.');
      return;
    }

    try {
      const healthy = await this.yane.healthCheck();

      if (healthy) {
        this.log('Backend Python saudável no boot.');
      } else {
        this.logWarn(
          'Backend Python não respondeu ao health check no boot.'
        );
      }
    } catch (error) {
      this.logError(
        'Falha no health check do backend no boot.',
        error
      );
    }
  }

  async close() {
    if (this._closing) return;

    this._closing = true;
    ++this._lifecycleGeneration;

    this._stopWorker();

    if (this._workerPromise) {
      try {
        await this._workerPromise;
      } catch (error) {
        this.logError('Erro durante shutdown do worker.', error);
      }
    }

    this._initialized = false;

    if (this._ownsRedis) {
      try {
        await this.redis.close();
      } catch (error) {
        this.logError('Erro ao fechar Redis.', error);
      }
    }

    this.log('InterviewService encerrado.');
  }

  invalidatePendingInitialize() {
    ++this._lifecycleGeneration;
    this._initializing = null;
  }

  setWhatsAppService(service) {
    this.whatsapp = service || null;
  }

  _getWhatsAppService() {
    return this.whatsapp || global.whatsappService || null;
  }

  _isWhatsAppReady(client = this._getWhatsAppService()) {
    if (!client) return false;

    if (typeof client.isReady === 'boolean') {
      return client.isReady;
    }

    return typeof client.sendMessage === 'function';
  }

  // ===========================================================================
  // MAPEAMENTO PHONE ↔ INTERVIEW (compatibilidade)
  // ===========================================================================

  async rememberInterview(phone, interviewId) {
    const normalizedPhone = normalizePhone(phone);
    if (!normalizedPhone || !interviewId) return false;

    try {
      return Boolean(
        await this.redis.rememberInterview(
          normalizedPhone,
          interviewId
        )
      );
    } catch (error) {
      this.logError(
        'Falha ao guardar mapping telefone → entrevista.',
        error,
        normalizedPhone,
        { interviewId }
      );

      return false;
    }
  }

  async resolveInterviewId(phone) {
    const normalizedPhone = normalizePhone(phone);
    if (!normalizedPhone) return null;

    try {
      return (
        (await this.redis.resolveInterviewId(normalizedPhone)) || null
      );
    } catch (error) {
      this.logError(
        'Falha ao resolver entrevista por telefone.',
        error,
        normalizedPhone
      );

      return null;
    }
  }

  async forgetInterview(phone) {
    const normalizedPhone = normalizePhone(phone);
    if (!normalizedPhone) return false;

    try {
      return Boolean(
        await this.redis.forgetInterview(normalizedPhone)
      );
    } catch (error) {
      this.logError(
        'Falha ao remover mapping telefone → entrevista.',
        error,
        normalizedPhone
      );

      return false;
    }
  }

  // ===========================================================================
  // MAPEAMENTO LID → PHONE
  // ===========================================================================

  async rememberLidMapping(lidJid, phone) {
    try {
      if (typeof this.redis.rememberLidMapping !== 'function') {
        return false;
      }

      return Boolean(
        await this.redis.rememberLidMapping(lidJid, phone)
      );
    } catch (error) {
      this.logError(
        'Falha ao persistir LID → PN.',
        error,
        phone,
        { lid: lidJid }
      );

      return false;
    }
  }

  async resolveLidToPhone(lidJid) {
    try {
      if (typeof this.redis.resolveLidMapping !== 'function') {
        return null;
      }

      return await this.redis.resolveLidMapping(lidJid);
    } catch (error) {
      this.logError(
        'Falha ao consultar LID no Redis.',
        error,
        null,
        { lid: lidJid }
      );

      return null;
    }
  }

  async forgetLidMapping(lidJid) {
    try {
      if (typeof this.redis.forgetLidMapping !== 'function') {
        return false;
      }

      return Boolean(
        await this.redis.forgetLidMapping(lidJid)
      );
    } catch (error) {
      this.logError(
        'Falha ao remover LID → PN.',
        error,
        null,
        { lid: lidJid }
      );

      return false;
    }
  }

  // ===========================================================================
  // [FIX-ROUTING-2/4] CACHE LOCAL DE ROUTING E DISAMBIGUAÇÃO
  // ===========================================================================

  async _rememberResolvedInterview(phone, interviewId) {
    const normalized = normalizePhone(phone);
    if (!normalized || !interviewId) return;

    try {
      if (typeof this.redis.rememberResolvedInterview === 'function') {
        await this.redis.rememberResolvedInterview(
          normalized,
          interviewId,
          CONFIG.resolvedInterviewTtlSeconds
        );
      }
    } catch (error) {
      this.logDebug(
        'Falha ao guardar entrevista resolvida.',
        normalized,
        { error: errorMessage(error) }
      );
    }
  }

  async _getResolvedInterview(phone) {
    const normalized = normalizePhone(phone);
    if (!normalized) return null;

    try {
      if (typeof this.redis.getResolvedInterview !== 'function') {
        return null;
      }

      return (
        (await this.redis.getResolvedInterview(normalized)) || null
      );
    } catch (error) {
      this.logDebug(
        'Falha ao ler entrevista resolvida.',
        normalized,
        { error: errorMessage(error) }
      );

      return null;
    }
  }

  async _clearResolvedInterview(phone) {
    const normalized = normalizePhone(phone);
    if (!normalized) return;

    try {
      if (typeof this.redis.clearResolvedInterview === 'function') {
        await this.redis.clearResolvedInterview(normalized);
      }
    } catch (_) {}
  }

  async _storeDisambiguationState(phone, state) {
    const normalized = normalizePhone(phone);
    if (!normalized) return;

    try {
      if (typeof this.redis.setDisambiguation === 'function') {
        await this.redis.setDisambiguation(
          normalized,
          {
            ...state,
            expiresAt: Date.now() + CONFIG.disambiguationTtlSeconds * 1000,
          },
          CONFIG.disambiguationTtlSeconds
        );
      }
    } catch (error) {
      this.logError(
        'Falha ao guardar estado de disambiguação.',
        error,
        normalized
      );
    }
  }

  async _getDisambiguationState(phone) {
    const normalized = normalizePhone(phone);
    if (!normalized) return null;

    try {
      if (typeof this.redis.getDisambiguation !== 'function') {
        return null;
      }

      const state = await this.redis.getDisambiguation(normalized);
      if (!state) return null;

      if (state.expiresAt && Date.now() > state.expiresAt) {
        await this._clearDisambiguationState(normalized);
        return null;
      }

      return state;
    } catch (error) {
      this.logError(
        'Falha ao ler estado de disambiguação.',
        error,
        normalized
      );

      return null;
    }
  }

  async _clearDisambiguationState(phone) {
    const normalized = normalizePhone(phone);
    if (!normalized) return;

    try {
      if (typeof this.redis.clearDisambiguation === 'function') {
        await this.redis.clearDisambiguation(normalized);
      }
    } catch (_) {}
  }

  _parseDisambiguationChoice(message, candidates) {
    if (!Array.isArray(candidates) || !candidates.length) return null;

    const text = clean(message);
    if (!text) return null;

    const match = text.match(/^\s*(\d{1,2})\b/);
    if (!match) return null;

    const index = Number(match[1]);
    if (!Number.isFinite(index)) return null;

    return (
      candidates.find((candidate) => candidate.index === index) || null
    );
  }

  async _sendDisambiguationPrompt(phone, candidates, options = {}) {
    const retry = Boolean(options.retry);

    const header = retry
      ? 'Não percebi. Responde só com o número da opção (1, 2, ...).'
      : 'Olá! Vejo que tens mais do que uma conversa aberta com a Yane.\n\n'
        + 'Sobre qual queres falar agora?';

    const lines = candidates
      .map((candidate) => {
        const title = clean(candidate.job_title) || 'vaga';
        const company = clean(candidate.company_name);

        return company
          ? `${candidate.index}) ${title} — ${company}`
          : `${candidate.index}) ${title}`;
      })
      .join('\n');

    const body = `${header}\n\n${lines}`;

    const client = this._getWhatsAppService();

    try {
      await this._sendHumanBubble(phone, body, client);
    } catch (error) {
      this.logError(
        'Falha ao enviar prompt de disambiguação.',
        error,
        phone
      );
    }
  }

  // ===========================================================================
  // INÍCIO DA ENTREVISTA
  // ===========================================================================

  async startInterview(payload = {}) {
    const phone = normalizePhone(payload.phone);

    const interviewId =
      payload.interviewId ||
      payload.interview_id ||
      null;

    const initialMessage = clean(
      payload.initialMessage || payload.initial_message
    );

    if (!phone || !interviewId || !initialMessage) {
      this.logWarn(
        'startInterview ignorado: payload inválido.',
        phone,
        {
          interviewId,
          hasMessage: Boolean(initialMessage),
        }
      );

      return { success: false, reason: 'invalid_payload' };
    }

    const mapped = await this.rememberInterview(
      phone,
      interviewId
    );

    if (!mapped) {
      this._metricIncrement('errorsTotal', { subsystem: 'redis' });

      return { success: false, reason: 'state_unavailable' };
    }

    this._metricIncrement('interviewsStarted');

    // Invalida qualquer cache de routing anterior — o candidato
    // acabou de receber um novo convite e a próxima mensagem dele
    // pode ir para esta entrevista nova.
    await this._clearResolvedInterview(phone);

    const delivery = await this._sendText(phone, initialMessage);

    if (delivery.ok) {
      this.log('Convite enviado.', phone, { interviewId });

      return { success: true, interviewId, phone };
    }

    const queued = await this._enqueuePendingDelivery({
      phone,
      bubbles: delivery.remaining,
      interviewId,
      turnId: randomUUID(),
      finished: false,
      interviewStatus: 'in_progress',
    });

    if (queued === 1) {
      this._metricIncrement('interviewsQueued');
    } else {
      this._metricIncrement('errorsTotal', { subsystem: 'redis' });
      await this._safeSendFallback(phone);
    }

    this.logWarn('Convite não entregue.', phone, {
      interviewId,
      queued: queued === 1,
    });

    return {
      success: true,
      interviewId,
      phone,
      queued: queued === 1,
    };
  }

  // ===========================================================================
  // MENSAGEM RECEBIDA
  // ===========================================================================

  async handleIncomingMessage(from, text, options = {}) {
    const phone = normalizePhone(from);
    const message = clean(text);
    const messageId = options.messageId || null;
    const isButton = Boolean(options.isButton);
    // [FIX-ROUTING-3] reply-to extraído pelo whatsapp.service.js.
    const repliedToMessageId = options.repliedToMessageId || null;

    if (!phone || !message) {
      return { handled: false, reason: 'empty' };
    }

    if (!this._isRedisAvailable()) {
      this._metricIncrement('errorsTotal', { subsystem: 'redis' });

      this.logError(
        'Redis indisponível — mensagem não processável.',
        new Error('redis_unavailable'),
        phone,
        { msgId: messageId }
      );

      await this._safeSendFallback(phone);

      return {
        handled: true,
        queued: false,
        degraded: true,
        reason: 'redis_unavailable',
      };
    }

    const dedupeResult = await this._handleDedupe(phone, messageId);

    if (dedupeResult === 'duplicate') {
      return { handled: false, reason: 'duplicate' };
    }

    if (dedupeResult === 'unavailable') {
      await this._safeSendFallback(phone);

      return {
        handled: true,
        queued: false,
        degraded: true,
        reason: 'dedupe_unavailable',
      };
    }

    let result;

    try {
      result = await this._withPhoneLock(phone, () =>
        this._handleLockedMessage({
          phone,
          message,
          messageId,
          isButton,
          repliedToMessageId,
        })
      );
    } catch (error) {
      this._metricIncrement('errorsTotal', { subsystem: 'redis' });

      this.logError(
        'Erro ao executar mensagem sob lock.',
        error,
        phone,
        { msgId: messageId }
      );

      result = null;
    }

    if (result !== null) return result;

    const queued = await this._queueIncomingTurn({
      phone,
      message,
      messageId,
      isButton,
      repliedToMessageId,
      reason: 'lock_busy',
    });

    if (queued === 1) {
      this._metricIncrement('turnsTotal', { status: 'queued' });

      return {
        handled: true,
        queued: true,
        reason: 'lock_busy',
      };
    }

    await this._forgetMessageSeen(messageId);
    await this._safeSendFallback(phone);

    return {
      handled: true,
      queued: false,
      reason: 'queue_unavailable',
    };
  }

  async _handleDedupe(phone, messageId) {
    if (!messageId) return 'new';

    try {
      const isNew = await this.redis.markMessageSeen(messageId);

      if (!isNew) {
        this._metricIncrement('turnsTotal', { status: 'duplicate' });
        this.logDebug('Mensagem duplicada ignorada.', phone, {
          msgId: messageId,
        });

        return 'duplicate';
      }

      return 'new';
    } catch (error) {
      this._metricIncrement('errorsTotal', { subsystem: 'redis' });

      this.logError(
        'Falha no dedupe da mensagem.',
        error,
        phone,
        { msgId: messageId }
      );

      return 'unavailable';
    }
  }

  async _handleLockedMessage({
    phone,
    message,
    messageId,
    isButton = false,
    repliedToMessageId = null,
  }) {
    const pending = await this._pendingCount(phone);

    if (pending > 0) {
      const queued = await this._queueIncomingTurn({
        phone,
        message,
        messageId,
        isButton,
        repliedToMessageId,
        reason: 'pending_queue',
      });

      if (queued === 1) {
        this._metricIncrement('turnsTotal', { status: 'queued' });

        return {
          handled: true,
          queued: true,
          reason: 'pending_queue',
        };
      }

      await this._forgetMessageSeen(messageId);
      await this._safeSendFallback(phone);

      return {
        handled: true,
        queued: false,
        reason: 'queue_unavailable',
      };
    }

    const turnId = randomUUID();

    const resolution = await this._resolveInterviewForIncoming(
      phone,
      { message, messageId, turnId, isButton, repliedToMessageId }
    );

    // [FIX-ROUTING-2] Ambiguidade: prompt enviado, mensagem
    // original NÃO é processada como turno. O próximo turno do
    // candidato será interpretado como escolha.
    if (resolution.status === 'ambiguous') {
      await this._sendDisambiguationPrompt(
        phone,
        resolution.candidates
      );

      this._metricIncrement('turnsTotal', {
        status: 'disambiguation_prompted',
      });

      return {
        handled: true,
        queued: false,
        reason: 'ambiguous_routing',
      };
    }

    if (resolution.status === 'disambiguation_invalid') {
      await this._sendDisambiguationPrompt(
        phone,
        resolution.candidates,
        { retry: true }
      );

      this._metricIncrement('turnsTotal', {
        status: 'disambiguation_invalid',
      });

      return {
        handled: true,
        queued: false,
        reason: 'disambiguation_invalid',
      };
    }

    if (resolution.status === 'queued') {
      return {
        handled: true,
        queued: true,
        reason: resolution.reason,
      };
    }

    if (resolution.status !== 'found') {
      this._metricIncrement('turnsTotal', { status: 'no_interview' });

      this.logWarn(
        'Sem entrevista activa para o telefone — reportando órfã.',
        phone,
        { msgId: messageId, turnId }
      );

      void this._reportOrphan({ phone, message, messageId });

      return {
        handled: false,
        reason: 'no_active_interview',
      };
    }

    const interviewId = resolution.interviewId;
    let turn;

    try {
      turn = await this._sendInterviewTurn({
        interviewId,
        phone,
        message,
        messageId,
        turnId,
        isButton,
      });
    } catch (error) {
      this._metricIncrement('errorsTotal', { subsystem: 'backend' });

      this.logWarn(
        'Backend indisponível — turno em fila.',
        phone,
        {
          interviewId,
          turnId,
          msgId: messageId,
          error: errorMessage(error),
          retryable: isRetryableError(error),
        }
      );

      const queued = await this._queueIncomingTurn({
        phone,
        message,
        messageId,
        isButton,
        turnId,
        interviewId,
        repliedToMessageId,
        reason: 'backend_error',
      });

      if (queued !== 1) {
        await this._forgetMessageSeen(messageId);
        await this._safeSendFallback(phone);
      }

      return {
        handled: true,
        queued: queued === 1,
        reason:
          queued === 1
            ? 'backend_unavailable'
            : 'queue_unavailable',
      };
    }

    return this._handleTurnResponse({
      phone,
      interviewId,
      turnId,
      turn,
    });
  }

  // ===========================================================================
  // [FIX-ROUTING-1/2/4] RESOLUÇÃO DE ENTREVISTA PARA MENSAGEM INBOUND
  // ===========================================================================

  async _resolveInterviewForIncoming(
    phone,
    {
      message,
      messageId,
      turnId,
      isButton = false,
      repliedToMessageId = null,
    }
  ) {
    // 1) Disambiguação pendente tem prioridade ABSOLUTA.
    //    Se estamos a meio de uma escolha, o texto do candidato
    //    é interpretado como resposta, não como mensagem nova.
    const pendingDisambiguation = await this._getDisambiguationState(phone);

    if (pendingDisambiguation) {
      const chosen = this._parseDisambiguationChoice(
        message,
        pendingDisambiguation.candidates
      );

      if (chosen) {
        await this._clearDisambiguationState(phone);
        await this._rememberResolvedInterview(
          phone,
          chosen.interview_id
        );

        this._metricIncrement('turnsTotal', {
          status: 'disambiguation_resolved',
        });

        return {
          status: 'found',
          interviewId: chosen.interview_id,
          resolvedBy: 'disambiguation',
        };
      }

      // Resposta inválida — repetimos o prompt com a lista original.
      return {
        status: 'disambiguation_invalid',
        candidates: pendingDisambiguation.candidates,
      };
    }

    // 2) Reply-to é determinístico — se temos o ID original, não
    //    precisamos de cache nem de resolver no backend.
    if (repliedToMessageId) {
      const cached = await this._getResolvedInterview(phone);

      // Não usamos o cache quando há reply-to: o ID manda.
      // Limpamos para evitar que o próximo turno sem reply-to
      // use um valor que já não corresponde.
      if (cached) {
        await this._clearResolvedInterview(phone);
      }
    } else {
      // 3) Sem reply-to: o cache local de routing é válido durante
      //    a janela da sessão. Só o usamos se não houver ambiguidade
      //    pendente (já tratada acima) e se não vier um reply-to.
      const cached = await this._getResolvedInterview(phone);
      if (cached) {
        return {
          status: 'found',
          interviewId: cached,
          resolvedBy: 'cache',
        };
      }
    }

    // 4) Delegar ao backend.
    if (typeof this.yane?.resolveInboundMessage !== 'function') {
      this.logError(
        'yane.resolveInboundMessage indisponível — ' +
          'verifique se o processo Node foi reiniciado após ' +
          'actualizar yane-integration.service.js.',
        null,
        phone,
        { msgId: messageId, turnId }
      );

      return { status: 'unavailable' };
    }

    let resolution;

    try {
      resolution = await this.yane.resolveInboundMessage({
        phone,
        message,
        repliedToMessageId,
      });
    } catch (error) {
      this._metricIncrement('errorsTotal', { subsystem: 'backend' });

      this.logWarn(
        'Resolve-inbound falhou — turno em fila.',
        phone,
        {
          msgId: messageId,
          turnId,
          error: errorMessage(error),
        }
      );

      const queued = await this._queueIncomingTurn({
        phone,
        message,
        messageId,
        isButton,
        turnId,
        repliedToMessageId,
        reason: 'resolve_failed',
      });

      if (queued === 1) {
        return { status: 'queued', reason: 'resolve_failed' };
      }

      await this._forgetMessageSeen(messageId);
      await this._safeSendFallback(phone);

      return { status: 'unavailable' };
    }

    if (!resolution || typeof resolution !== 'object') {
      return { status: 'none' };
    }

    const confidence = clean(resolution.confidence);

    // 5) Routing seguro — cache local e devolvemos.
    if (
      (confidence === 'high' || confidence === 'medium') &&
      resolution.interview_id
    ) {
      await this._rememberResolvedInterview(
        phone,
        resolution.interview_id
      );

      this._metricIncrement('turnsTotal', {
        status: 'routing_resolved',
        method: clean(resolution.method) || 'unknown',
      });

      return {
        status: 'found',
        interviewId: resolution.interview_id,
        resolvedBy: resolution.method || 'backend',
      };
    }

    // 6) Ambíguo — devolvemos os candidatos ao caller.
    if (confidence === 'ambiguous') {
      const candidates = Array.isArray(resolution.candidates)
        ? resolution.candidates
        : [];

      if (!candidates.length) {
        // Backend disse ambíguo mas não devolveu candidatos.
        // Tratamos como none — o caller reporta órfã.
        return { status: 'none' };
      }

      await this._storeDisambiguationState(phone, {
        candidates,
        originalMessage: message,
        originalMessageId: messageId,
        originalTurnId: turnId,
      });

      this._metricIncrement('turnsTotal', {
        status: 'routing_ambiguous',
        candidateCount: String(candidates.length),
      });

      return {
        status: 'ambiguous',
        candidates,
      };
    }

    // 7) none / invalid_phone → sem entrevista.
    this._metricIncrement('turnsTotal', {
      status: 'routing_none',
      confidence: confidence || 'unknown',
    });

    return { status: 'none' };
  }

  async _sendInterviewTurn({
    interviewId,
    phone,
    message,
    messageId,
    turnId,
    isButton,
  }) {
    const turn = await metrics.time(
      metrics.turnDuration,
      { endpoint: 'turn' },
      () =>
        this.yane.sendInterviewTurn({
          interviewId,
          phone,
          message,
          turnId,
          messageId,
          isButton,
        })
    );

    if (!turn) {
      throw new Error('backend_empty_response');
    }

    return turn;
  }

  async _handleTurnResponse({
    phone,
    interviewId,
    turnId,
    turn,
  }) {
    // [FIX-PROCESSING] Backend ainda a processar.
    if (turn?.processing === true) {
      this.logDebug(
        'Backend ainda a processar turno anterior; colocando em fila.',
        phone,
        {
          interviewId,
          turnId,
        }
      );

      const queued = await this._queueIncomingTurn({
        phone,
        message: null,
        messageId: null,
        isButton: false,
        turnId,
        interviewId,
        reason: 'backend_still_processing',
      });

      if (queued === 1) {
        this._metricIncrement('turnsTotal', {
          status: 'backend_processing_requeued',
        });

        return {
          handled: true,
          queued: true,
          reason: 'backend_still_processing',
        };
      }

      this.logWarn(
        'Backend ainda a processar e fila indisponível.',
        phone,
        { interviewId, turnId }
      );

      return {
        handled: true,
        queued: false,
        reason: 'backend_still_processing_queue_failed',
      };
    }

    const bubbles = this._extractBubbles(turn?.bubbles);
    const finished = Boolean(turn?.finished);
    const interviewStatus = turn?.interview_status || 'in_progress';

    if (finished) {
      await this.forgetInterview(phone);
      await this._clearResolvedInterview(phone);
      await this._clearDisambiguationState(phone);

      this._metricIncrement('interviewsFinished', {
        status: interviewStatus,
      });

      this.log('Entrevista terminada.', phone, {
        interviewId,
        status: interviewStatus,
        credits: turn?.credits_charged,
        turnId,
      });
    }

    if (!bubbles.length) {
      this._metricIncrement('turnsTotal', { status: 'success' });

      return { handled: true, finished, status: interviewStatus };
    }

    const delivery = await this._sendBubblesWithResult(
      phone,
      bubbles
    );

    if (delivery.ok) {
      this._metricIncrement('turnsTotal', { status: 'success' });

      return { handled: true, finished, status: interviewStatus };
    }

    const queued = await this._enqueuePendingDelivery({
      phone,
      bubbles: delivery.remaining,
      interviewId,
      turnId,
      finished,
      interviewStatus,
    });

    if (queued !== 1) {
      this._metricIncrement('errorsTotal', { subsystem: 'redis' });
      await this._safeSendFallback(phone);
    }

    this.logWarn('Entrega WhatsApp em fila.', phone, {
      interviewId,
      turnId,
      queued: queued === 1,
    });

    return {
      handled: true,
      queued: queued === 1,
      finished,
      status: interviewStatus,
      reason:
        queued === 1
          ? 'whatsapp_delivery_queued'
          : 'delivery_lost',
    };
  }

  // ===========================================================================
  // ÓRFÃS
  // ===========================================================================

  async _reportOrphan({ phone, message, messageId }) {
    try {
      if (
        typeof this.yane.reportUnmatchedIncoming !== 'function'
      ) {
        return;
      }

      await this.yane.reportUnmatchedIncoming({
        phone,
        message,
        messageId,
      });
    } catch (error) {
      this.logDebug(
        'Falha ao reportar mensagem órfã.',
        phone,
        { msgId: messageId, error: errorMessage(error) }
      );
    }
  }

  // ===========================================================================
  // FILA
  // ===========================================================================

  _isRedisAvailable() {
    if (typeof this.redis.isAvailable !== 'function') return true;
    return Boolean(this.redis.isAvailable());
  }

  _getQueueKeys(phone) {
    return {
      queueKey: this.redis.key(
        CONFIG.pendingTurnKeyPrefix,
        phone
      ),
      phonesKey: this.redis.key(CONFIG.pendingPhonesKey),
      deadKey: this.redis.key(CONFIG.deadLetterKey),
    };
  }

  async _queueIncomingTurn({
    phone,
    message,
    messageId,
    isButton = false,
    turnId = randomUUID(),
    interviewId = null,
    repliedToMessageId = null,
    reason = 'unknown',
  }) {
    const resolvedInterviewId =
      interviewId ||
      (await this.resolveInterviewId(phone));

    return this._enqueuePendingItem(phone, {
      kind: 'turn',
      schemaVersion: CONFIG.queueSchemaVersion,

      phone,
      message,
      messageId: messageId || null,

      isButton: Boolean(isButton),

      interviewId: resolvedInterviewId || null,
      turnId,

      // [FIX-ROUTING-3] Preservar reply-to para resolução posterior.
      repliedToMessageId: repliedToMessageId || null,

      attempts: 0,
      lookupAttempts: 0,

      enqueuedAt: Date.now(),
      lastError: null,
      queueReason: reason,
    });
  }

  async _enqueuePendingDelivery({
    phone,
    bubbles,
    interviewId,
    turnId,
    finished = false,
    interviewStatus = 'in_progress',
  }) {
    const cleanBubbles = this._extractBubbles(bubbles);
    if (!cleanBubbles.length) return 1;

    return this._enqueuePendingItem(phone, {
      kind: 'delivery',
      schemaVersion: CONFIG.queueSchemaVersion,

      phone,
      bubbles: cleanBubbles,

      interviewId: interviewId || null,
      turnId: turnId || randomUUID(),

      finished: Boolean(finished),
      interviewStatus,

      attempts: 0,
      enqueuedAt: Date.now(),
      lastError: null,
    });
  }

  async _enqueuePendingItem(phone, payload) {
    const { queueKey, phonesKey } = this._getQueueKeys(phone);

    try {
      if (
        typeof this.redis.enqueuePendingItem !== 'function'
      ) {
        throw new Error('enqueuePendingItem_unavailable');
      }

      const result = await this.redis.enqueuePendingItem({
        queueKey,
        phonesKey,
        phone,
        payload: JSON.stringify(payload),
        score: Date.now(),
        maxItems: CONFIG.maxPendingPerPhone,
      });

      if (result === 1) {
        this._metricIncrement('queueEnqueued', {
          kind: payload?.kind || 'turn',
          reason: payload?.queueReason || 'unknown',
        });
      }

      if (result === 0) {
        this.logWarn('Fila cheia — a notificar candidato.', phone, {
          kind: payload?.kind || null,
          turnId: payload?.turnId || null,
        });

        await this._sendQueueFullNotice(phone);
      }

      return result;
    } catch (error) {
      this.logError('Falha ao enfileirar item.', error, phone, {
        kind: payload?.kind || null,
        turnId: payload?.turnId || null,
      });

      return 0;
    }
  }

  async _sendQueueFullNotice(phone) {
    try {
      const client = this._getWhatsAppService();
      if (!this._isWhatsAppReady(client)) return;

      const normalizedPhone = normalizePhone(phone);
      if (!normalizedPhone) return;

      await client.sendMessage(
        normalizedPhone,
        CONFIG.queueFullMessage
      );
    } catch (error) {
      this.logDebug('Falha ao enviar aviso de fila cheia.', phone, {
        error: errorMessage(error),
      });
    }
  }

  async _peekPendingItem(phone) {
    const { queueKey } = this._getQueueKeys(phone);
    return this.redis.lindex(queueKey, -1);
  }

  async _dequeuePendingItem(phone) {
    const { queueKey } = this._getQueueKeys(phone);
    return this.redis.rpop(queueKey);
  }

  async _pendingCount(phone) {
    const { queueKey } = this._getQueueKeys(phone);

    try {
      return Number(await this.redis.llen(queueKey)) || 0;
    } catch (error) {
      this.logError(
        'Falha ao obter profundidade da fila.',
        error,
        phone
      );
      throw error;
    }
  }

  // ===========================================================================
  // WORKER
  // ===========================================================================

  _startWorker() {
    if (this._workerTimer || this._closing) return;

    this._workerTimer = setInterval(
      () => this._triggerWorker(),
      CONFIG.workerIntervalMs
    );

    this._unrefTimer(this._workerTimer);

    this._deadLetterTimer = setInterval(
      () => { void this._reportDeadLetterDepth(); },
      CONFIG.deadLetterReportIntervalMs
    );

    this._unrefTimer(this._deadLetterTimer);

    void this._reportDeadLetterDepth();

    this.log('Worker de recuperação iniciado.', null, {
      intervalMs: CONFIG.workerIntervalMs,
      batchSize: CONFIG.workerBatchSize,
      maxPerPhone: CONFIG.workerMaxPerPhone,
    });
  }

  _stopWorker() {
    if (this._workerTimer) {
      clearInterval(this._workerTimer);
      this._workerTimer = null;
    }

    if (this._deadLetterTimer) {
      clearInterval(this._deadLetterTimer);
      this._deadLetterTimer = null;
    }

    this.log('Worker de recuperação parado.');
  }

  _unrefTimer(timer) {
    if (timer && typeof timer.unref === 'function') {
      timer.unref();
    }
  }

  async _reportDeadLetterDepth() {
    try {
      if (typeof this.redis.llen !== 'function') return;

      const deadKey = this.redis.key(CONFIG.deadLetterKey);
      const depth = Number(await this.redis.llen(deadKey)) || 0;

      this._metricSet('deadLetterDepth', depth);

      if (depth > 0) {
        this.logWarn('Dead-letter com itens pendentes.', null, {
          depth,
        });
      }
    } catch (error) {
      this.logDebug(
        'Falha ao reportar profundidade da dead-letter.',
        null,
        { error: errorMessage(error) }
      );
    }
  }

  _triggerWorker() {
    if (this._closing || this._workerRunning) return;

    const promise = this._processPendingTurns();
    this._workerPromise = promise;

    promise
      .catch((error) => this.logError('Worker falhou.', error))
      .finally(() => {
        if (this._workerPromise === promise) {
          this._workerPromise = null;
        }
      });
  }

  async _processPendingTurns() {
    if (this._workerRunning || this._closing) return;
    if (!this._isWhatsAppReady()) return;
    if (!this._isRedisAvailable()) return;

    this._workerRunning = true;

    try {
      const phonesKey = this.redis.key(CONFIG.pendingPhonesKey);
      const now = Date.now();

      const phones = await this.redis.zrangeByScore(
        phonesKey,
        '-inf',
        now,
        0,
        CONFIG.workerBatchSize
      );

      this._metricSet('queueDepth', phones.length, {
        kind: 'batch',
      });

      for (const phone of phones) {
        if (this._closing) break;

        try {
          await this._processPhoneQueue(phone);
        } catch (error) {
          this.logError(
            'Falha ao processar fila do telefone.',
            error,
            phone
          );
        }
      }
    } finally {
      this._workerRunning = false;
    }
  }

  // ===========================================================================
  // LOCK
  // ===========================================================================

  _startLockRefresh(phone, token) {
    if (typeof this.redis.refreshLock !== 'function') return null;

    const timer = setInterval(() => {
      if (this._closing) return;

      this.redis
        .refreshLock(phone, token, CONFIG.lockTtlSeconds)
        .catch((error) => {
          this.logWarn('Falha ao renovar lock.', phone, {
            error: errorMessage(error),
          });
        });
    }, CONFIG.lockRefreshMs);

    this._unrefTimer(timer);

    return timer;
  }

  async _withPhoneLock(phone, fn) {
    const token = await this.redis.acquireLock(
      phone,
      CONFIG.lockTtlSeconds
    );

    if (!token) return null;

    const refreshTimer = this._startLockRefresh(phone, token);

    try {
      return await fn();
    } finally {
      if (refreshTimer) clearInterval(refreshTimer);

      try {
        await this.redis.releaseLock(phone, token);
      } catch (error) {
        this.logWarn('Falha ao libertar lock.', phone, {
          error: errorMessage(error),
        });
      }
    }
  }

  // ===========================================================================
  // FILA POR TELEFONE
  // ===========================================================================

  async _processPhoneQueue(phone) {
    const token = await this.redis.acquireLock(
      phone,
      CONFIG.lockTtlSeconds
    );

    if (!token) return;

    const refreshTimer = this._startLockRefresh(phone, token);

    try {
      let processed = 0;

      while (
        processed < CONFIG.workerMaxPerPhone &&
        !this._closing
      ) {
        const raw = await this._peekPendingItem(phone);
        if (!raw) break;

        let payload;

        try {
          payload = JSON.parse(raw);
        } catch (error) {
          this.logError('Item inválido na fila.', error, phone);

          const moved = await this._deadLetterPendingItem(phone, {
            reason: 'invalid_json',
            raw,
          });

          if (moved) {
            processed += 1;
            continue;
          }

          await this._quarantinePendingItem(phone, {
            kind: 'unknown',
            raw,
            attempts: CONFIG.pendingMaxAttempts,
          });

          break;
        }

        payload = this._normalizePendingPayload(phone, payload);

        const result = await this._processPendingItem(phone, payload);

        if (result.action === 'ack') {
          await this._dequeuePendingItem(phone);
          processed += 1;
          continue;
        }

        if (result.action === 'dead_letter') {
          const moved = await this._deadLetterPendingItem(phone, {
            reason: result.reason || 'terminal_failure',
            payload: result.payload || payload,
          });

          if (moved) {
            processed += 1;
            continue;
          }

          await this._quarantinePendingItem(
            phone,
            result.payload || payload
          );

          break;
        }

        if (result.action === 'retry') {
          const retryResult = await this._reschedulePendingItem(
            phone,
            result.payload || payload,
            result.retryAfterMs || 0,
            result.retryable !== false
          );

          if (retryResult.removed) processed += 1;

          break;
        }

        this.logWarn(
          'Resultado desconhecido do processamento; item será reagendado.',
          phone,
          {
            kind: payload.kind || 'unknown',
            turnId: payload.turnId || null,
          }
        );

        const retryResult = await this._reschedulePendingItem(
          phone,
          { ...payload, lastError: 'unknown_processing_result' }
        );

        if (retryResult.removed) processed += 1;

        break;
      }

      const remaining = await this._pendingCount(phone);

      if (remaining === 0) {
        const { phonesKey } = this._getQueueKeys(phone);
        await this.redis.zrem(phonesKey, phone);
      }
    } finally {
      if (refreshTimer) clearInterval(refreshTimer);

      try {
        await this.redis.releaseLock(phone, token);
      } catch (error) {
        this.logWarn(
          'Falha ao libertar lock do worker.',
          phone,
          { error: errorMessage(error) }
        );
      }
    }
  }

  _normalizePendingPayload(phone, payload) {
    if (!payload || typeof payload !== 'object') {
      return {
        kind: 'turn',
        schemaVersion: CONFIG.queueSchemaVersion,
        phone,
        attempts: 0,
        lookupAttempts: 0,
        repliedToMessageId: null,
      };
    }

    return {
      schemaVersion:
        payload.schemaVersion || CONFIG.queueSchemaVersion,

      kind: payload.kind || 'turn',

      phone: normalizePhone(payload.phone) || phone,

      message: payload.message
        ? String(payload.message)
        : undefined,

      messageId: payload.messageId || null,
      isButton: Boolean(payload.isButton),
      interviewId: payload.interviewId || null,
      turnId: payload.turnId || randomUUID(),

      // [FIX-ROUTING-3] Preservar entre tentativas.
      repliedToMessageId: payload.repliedToMessageId || null,

      bubbles: this._extractBubbles(payload.bubbles),

      finished: Boolean(payload.finished),
      interviewStatus:
        payload.interviewStatus || 'in_progress',

      attempts: Math.max(0, Number(payload.attempts) || 0),
      lookupAttempts: Math.max(
        0,
        Number(payload.lookupAttempts) || 0
      ),

      enqueuedAt: Number(payload.enqueuedAt) || Date.now(),
      lastError: payload.lastError || null,
      queueReason: payload.queueReason || null,
    };
  }

  // ===========================================================================
  // PROCESSAMENTO DE ITEM PENDENTE
  // ===========================================================================

  async _processPendingItem(phone, payload) {
    if (payload.kind === 'delivery') {
      return this._processPendingDelivery(phone, payload);
    }

    return this._processPendingTurn(phone, payload);
  }

  // ===========================================================================
  // TURNO PENDENTE → BACKEND
  // ===========================================================================

  async _processPendingTurn(phone, payload) {
    let { interviewId, lookupAttempts = 0 } = payload;

    const {
      message,
      messageId,
      turnId,
      isButton = false,
      repliedToMessageId = null,
    } = payload;

    if (payload.attempts >= CONFIG.pendingMaxAttempts) {
      return {
        action: 'dead_letter',
        reason: 'max_attempts_reached',
        payload,
      };
    }

    // [FIX-ROUTING-1] Sem entrevista em cache: usar resolve-inbound
    // em vez de findActiveInterviewByPhone. Esta é a única forma
    // de acertar quando o mesmo telefone tem múltiplas activas.
    if (!interviewId) {
      interviewId = await this.resolveInterviewId(phone);

      if (!interviewId) {
        interviewId = await this._getResolvedInterview(phone);
      }

      if (
        !interviewId &&
        lookupAttempts < CONFIG.maxLookupAttempts
      ) {
        if (typeof this.yane?.resolveInboundMessage !== 'function') {
          return {
            action: 'dead_letter',
            reason: 'yane_missing_resolve_method',
            payload: {
              ...payload,
              lastError: 'yane.resolveInboundMessage indisponível',
            },
          };
        }

        try {
          const resolution = await this.yane.resolveInboundMessage({
            phone,
            message: message || '',
            repliedToMessageId,
          });

          const confidence = clean(resolution?.confidence);

          if (
            (confidence === 'high' || confidence === 'medium') &&
            resolution?.interview_id
          ) {
            interviewId = resolution.interview_id;

            await this.rememberInterview(phone, interviewId);
            await this._rememberResolvedInterview(phone, interviewId);
          } else if (confidence === 'ambiguous') {
            // Turno em fila com ambiguidade resolvida por fora:
            // o candidato não está no meio da conversa, é uma
            // mensagem atrasada. Reportamos como órfã.
            this.logWarn(
              'Turno recuperado caiu em routing ambíguo — a reportar órfã.',
              phone,
              { turnId }
            );

            await this._reportOrphan({ phone, message, messageId });

            return { action: 'ack' };
          }
        } catch (error) {
          lookupAttempts += 1;

          this._metricIncrement('errorsTotal', {
            subsystem: 'backend',
          });

          this.logWarn('Resolve-inbound falhou em retry.', phone, {
            turnId,
            lookupAttempts,
            maxLookups: CONFIG.maxLookupAttempts,
            error: errorMessage(error),
          });

          if (lookupAttempts >= CONFIG.maxLookupAttempts) {
            return {
              action: 'dead_letter',
              reason: 'lookup_exhausted',
              payload: {
                ...payload,
                lookupAttempts,
                lastError: errorMessage(error),
              },
            };
          }

          return {
            action: 'retry',
            payload: {
              ...payload,
              lookupAttempts,
              lastError: errorMessage(error),
            },
            retryable: isRetryableError(error),
            retryAfterMs: getRetryAfterMs(error),
          };
        }
      }

      if (!interviewId) {
        if (lookupAttempts >= CONFIG.maxLookupAttempts) {
          return {
            action: 'dead_letter',
            reason: 'no_interview_resolvable',
            payload: { ...payload, lookupAttempts },
          };
        }

        this.logWarn(
          'Turno recuperado sem entrevista resolvível — reportando órfã.',
          phone,
          { turnId }
        );

        await this._reportOrphan({ phone, message, messageId });

        return { action: 'ack' };
      }

      payload = {
        ...payload,
        interviewId,
        lookupAttempts,
        repliedToMessageId,
      };
    }

    let turn;

    try {
      turn = await metrics.time(
        metrics.turnDuration,
        { endpoint: 'turn-recovered' },
        () =>
          this.yane.sendInterviewTurn({
            interviewId,
            phone,
            message,
            turnId,
            messageId,
            isButton,
          })
      );

      if (!turn) throw new Error('backend_empty_response');
    } catch (error) {
      this._metricIncrement('errorsTotal', { subsystem: 'backend' });

      if (!isRetryableError(error)) {
        this.logWarn(
          'Erro definitivo do backend — dead-letter directo.',
          phone,
          {
            interviewId,
            turnId,
            code: error?.code || null,
            status: error?.status || null,
          }
        );

        return {
          action: 'dead_letter',
          reason: 'backend_terminal_error',
          payload: {
            ...payload,
            interviewId,
            lastError: errorMessage(error),
          },
        };
      }

      this.logWarn('Recuperação do turno falhou.', phone, {
        interviewId,
        turnId,
        attempts: payload.attempts + 1,
        error: errorMessage(error),
        retryAfterMs: getRetryAfterMs(error),
      });

      return {
        action: 'retry',
        payload: {
          ...payload,
          interviewId,
          lastError: errorMessage(error),
        },
        retryable: true,
        retryAfterMs: getRetryAfterMs(error),
      };
    }

    // [FIX-PROCESSING] Backend ainda a processar.
    if (turn?.processing === true) {
      this.logDebug(
        'Backend ainda a processar; reagendando turno recuperado.',
        phone,
        { interviewId, turnId, attempts: payload.attempts + 1 }
      );

      this._metricIncrement('turnsTotal', {
        status: 'backend_processing_requeued',
      });

      return {
        action: 'retry',
        payload: {
          ...payload,
          interviewId,
          repliedToMessageId,
          lastError: 'backend_still_processing',
        },
        retryable: true,
        retryAfterMs: 30_000,
      };
    }

    this._metricIncrement('turnsTotal', { status: 'recovered' });

    const bubbles = this._extractBubbles(turn.bubbles);
    const finished = Boolean(turn.finished);
    const interviewStatus = turn.interview_status || 'in_progress';

    if (finished) {
      await this.forgetInterview(phone);
      await this._clearResolvedInterview(phone);
      await this._clearDisambiguationState(phone);

      this._metricIncrement('interviewsFinished', {
        status: interviewStatus,
      });

      this.log('Entrevista terminada (recuperada).', phone, {
        interviewId,
        status: interviewStatus,
        credits: turn.credits_charged,
        turnId,
      });
    }

    if (!bubbles.length) return { action: 'ack' };

    const delivery = await this._sendBubblesWithResult(phone, bubbles);

    if (delivery.ok) {
      this.log('Turno recuperado e entregue.', phone, {
        interviewId,
        turnId,
      });

      return { action: 'ack' };
    }

    return {
      action: 'retry',
      payload: {
        kind: 'delivery',
        schemaVersion: CONFIG.queueSchemaVersion,
        phone,
        bubbles: delivery.remaining,
        interviewId,
        turnId,
        finished,
        interviewStatus,
        attempts: 0,
        enqueuedAt: Date.now(),
        lastError: 'whatsapp_delivery_failed',
      },
      retryable: true,
    };
  }

  // ===========================================================================
  // ENTREGA PENDENTE → WHATSAPP
  // ===========================================================================

  async _processPendingDelivery(phone, payload) {
    const bubbles = this._extractBubbles(payload.bubbles);
    if (!bubbles.length) return { action: 'ack' };

    const delivery = await this._sendBubblesWithResult(phone, bubbles);

    if (delivery.ok) {
      this.log('Resposta pendente entregue.', phone, {
        turnId: payload.turnId,
      });

      return { action: 'ack' };
    }

    return {
      action: 'retry',
      payload: {
        ...payload,
        bubbles: delivery.remaining,
        lastError: 'whatsapp_delivery_failed',
      },
      retryable: true,
    };
  }

  // ===========================================================================
  // RETRY / BACKOFF
  // ===========================================================================

  async _reschedulePendingItem(
    phone,
    payload,
    retryAfterMs = 0,
    retryable = true
  ) {
    const attempts = Number(payload.attempts) + 1;
    const updated = { ...payload, attempts };

    if (!retryable) {
      const moved = await this._deadLetterPendingItem(phone, {
        reason: 'non_retryable_error',
        payload: updated,
      });

      if (moved) return { removed: true };

      await this._quarantinePendingItem(phone, updated);
      return { removed: false };
    }

    if (attempts >= CONFIG.pendingMaxAttempts) {
      const moved = await this._deadLetterPendingItem(phone, {
        reason: 'max_attempts_reached',
        payload: updated,
      });

      if (moved) return { removed: true };

      await this._quarantinePendingItem(phone, updated);
      return { removed: false };
    }

    const exponential =
      CONFIG.backoffBaseMs * Math.pow(2, attempts - 1);

    const jitter = 0.8 + Math.random() * 0.4;
    const computedBackoff = Math.min(
      exponential * jitter,
      CONFIG.backoffMaxMs
    );

    const backoff =
      Number.isFinite(retryAfterMs) && retryAfterMs > 0
        ? Math.min(
            Math.max(retryAfterMs, 1_000),
            CONFIG.backoffMaxMs
          )
        : computedBackoff;

    const scheduledAt = Date.now() + Math.round(backoff);
    const { queueKey, phonesKey } = this._getQueueKeys(phone);

    try {
      if (
        typeof this.redis.reschedulePendingItem !== 'function'
      ) {
        throw new Error('reschedulePendingItem_unavailable');
      }

      const updatedOk = await this.redis.reschedulePendingItem({
        queueKey,
        phonesKey,
        phone,
        payload: JSON.stringify(updated),
        score: scheduledAt,
      });

      if (!updatedOk) {
        throw new Error('reschedule_pending_item_failed');
      }

      this._metricIncrement('queueRequeued', {
        kind: updated.kind || 'turn',
      });

      this.log('Item reagendado.', phone, {
        kind: updated.kind || 'turn',
        attempts,
        backoffSeconds: Math.round(backoff / 1000),
        turnId: updated.turnId || null,
        source: retryAfterMs > 0 ? 'retry_after' : 'backoff',
      });

      return { removed: false };
    } catch (error) {
      this.logError(
        'Falha ao reagendar item.',
        error,
        phone,
        { turnId: payload.turnId || null }
      );

      return { removed: false };
    }
  }

  async _quarantinePendingItem(phone, payload) {
    try {
      const updated = {
        ...payload,
        attempts: CONFIG.pendingMaxAttempts,
        quarantinedAt: Date.now(),
      };

      const score = Date.now() + CONFIG.deadLetterRetryMs;
      const { queueKey, phonesKey } = this._getQueueKeys(phone);

      if (
        typeof this.redis.reschedulePendingItem !== 'function'
      ) {
        throw new Error('reschedulePendingItem_unavailable');
      }

      const ok = await this.redis.reschedulePendingItem({
        queueKey,
        phonesKey,
        phone,
        payload: JSON.stringify(updated),
        score,
      });

      if (!ok) throw new Error('quarantine_reschedule_failed');

      this.logWarn('Item colocado em quarentena.', phone, {
        turnId: payload.turnId || null,
        retryInMs: CONFIG.deadLetterRetryMs,
      });
    } catch (error) {
      this.logError(
        'Falha na quarentena do item.',
        error,
        phone,
        { turnId: payload.turnId || null }
      );
    }
  }

  async _deadLetterPendingItem(phone, context = {}) {
    const { queueKey, phonesKey, deadKey } =
      this._getQueueKeys(phone);

    const item = {
      phone,
      failedAt: Date.now(),
      ...context,
    };

    try {
      if (
        typeof this.redis.movePendingToDeadLetter !== 'function'
      ) {
        throw new Error('movePendingToDeadLetter_unavailable');
      }

      const moved = await this.redis.movePendingToDeadLetter({
        queueKey,
        phonesKey,
        deadKey,
        phone,
        payload: JSON.stringify(item),
      });

      if (moved) {
        this._metricIncrement('queueDeadLettered', {
          kind: context.payload?.kind || 'turn',
          reason: context.reason || 'unknown',
        });

        this.logWarn('Item enviado para dead-letter.', phone, {
          reason: context.reason || 'unknown',
          turnId: context.payload?.turnId || null,
        });
      }

      return Boolean(moved);
    } catch (error) {
      this.logError('Dead-letter falhou.', error, phone, {
        reason: context.reason || 'unknown',
        turnId: context.payload?.turnId || null,
      });

      return false;
    }
  }

  // ===========================================================================
  // DEDUPE
  // ===========================================================================

  async _forgetMessageSeen(messageId) {
    if (!messageId) return;

    try {
      await this.redis.del(this.redis.key('msg', messageId));
    } catch (error) {
      this.logDebug('Falha ao remover marker de dedupe.', null, {
        msgId: messageId,
        error: errorMessage(error),
      });
    }
  }

  // ===========================================================================
  // ENVIO
  // ===========================================================================

  async sendBubbles(phone, bubbles, client = null) {
    const result = await this._sendBubblesWithResult(
      phone,
      bubbles,
      client || this._getWhatsAppService()
    );

    return result.ok;
  }

  async sendHuman(phone, text, client = null) {
    const bubbles = this.splitBubbles(text);
    if (!bubbles.length) return true;

    const result = await this._sendBubblesWithResult(
      phone,
      bubbles,
      client || this._getWhatsAppService()
    );

    return result.ok;
  }

  async _sendText(phone, text, client = null) {
    return this._sendBubblesWithResult(
      phone,
      this.splitBubbles(text),
      client || this._getWhatsAppService()
    );
  }

  async _sendBubblesWithResult(phone, bubbles, client = null) {
    const normalized = this._extractBubbles(bubbles);
    if (!normalized.length) return { ok: true, remaining: [] };

    const whatsappClient = client || this._getWhatsAppService();

    if (!this._isWhatsAppReady(whatsappClient)) {
      return { ok: false, remaining: normalized };
    }

    for (let index = 0; index < normalized.length; index += 1) {
      const sent = await this._sendHumanBubble(
        phone,
        normalized[index],
        whatsappClient
      );

      if (!sent) {
        return { ok: false, remaining: normalized.slice(index) };
      }

      if (index < normalized.length - 1) {
        await this.delay(CONFIG.bubblePauseMs);
      }
    }

    return { ok: true, remaining: [] };
  }

  async _sendHumanBubble(phone, text, client) {
    try {
      if (typeof client.sendPresenceUpdate === 'function') {
        try {
          await client.sendPresenceUpdate('composing', phone);
        } catch (_) {
          // Presence é best-effort.
        }
      }

      const typingDelay = Math.min(
        text.length * this.typingMsPerChar,
        this.typingMax
      );

      if (typingDelay > 0) {
        await this.delay(typingDelay);
      }

      const result = await client.sendMessage(phone, text);
      return !isFailedSendResult(result);
    } catch (error) {
      this.logError('Falha ao enviar mensagem.', error, phone);
      return false;
    }
  }

  // ===========================================================================
  // FALLBACK
  // ===========================================================================

  _isFallbackCoolingDown(phone) {
    const last = this._fallbackSentAt.get(phone);

    return Boolean(
      last && Date.now() - last < CONFIG.fallbackCooldownMs
    );
  }

  _trackFallback(phone, timestamp = Date.now()) {
    this._fallbackSentAt.set(phone, timestamp);

    if (
      this._fallbackSentAt.size < CONFIG.fallbackSweepThreshold
    ) {
      return;
    }

    for (const [key, createdAt] of this._fallbackSentAt) {
      if (timestamp - createdAt > CONFIG.fallbackCooldownMs) {
        this._fallbackSentAt.delete(key);
      }
    }

    if (
      this._fallbackSentAt.size <= CONFIG.fallbackHardLimit
    ) {
      return;
    }

    const overflow =
      this._fallbackSentAt.size - CONFIG.fallbackHardLimit;

    let removed = 0;

    for (const key of this._fallbackSentAt.keys()) {
      if (removed >= overflow) break;

      this._fallbackSentAt.delete(key);
      removed += 1;
    }
  }

  async _safeSendFallback(phone, client = null) {
    const whatsappClient = client || this._getWhatsAppService();

    if (!this._isWhatsAppReady(whatsappClient)) return false;

    const normalizedPhone = normalizePhone(phone);
    if (!normalizedPhone) return false;

    if (
      this._isFallbackCoolingDown(normalizedPhone) ||
      this._fallbackInFlight.has(normalizedPhone)
    ) {
      return false;
    }

    this._fallbackInFlight.add(normalizedPhone);

    try {
      const result = await whatsappClient.sendMessage(
        normalizedPhone,
        CONFIG.softRecoveryMessage
      );

      if (isFailedSendResult(result)) return false;

      this._trackFallback(normalizedPhone);
      return true;
    } catch (error) {
      this.logError('Fallback falhou.', error, normalizedPhone);
      return false;
    } finally {
      this._fallbackInFlight.delete(normalizedPhone);
    }
  }

  // ===========================================================================
  // BUBBLES
  // ===========================================================================

  _extractBubbles(bubbles) {
    if (!Array.isArray(bubbles)) return [];
    return bubbles.map(clean).filter(Boolean);
  }

  splitBubbles(text) {
    const parts = String(text || '')
      .split(/\n{2,}/)
      .map(clean)
      .filter(Boolean);

    if (!parts.length) return [];

    const merged = this.mergeSmallBubbles(parts);
    if (merged.length <= 3) return merged;

    return [
      merged[0],
      merged.slice(1, -1).join('\n\n'),
      merged[merged.length - 1],
    ];
  }

  mergeSmallBubbles(parts) {
    const result = [];

    for (const part of parts) {
      const previous = result[result.length - 1];

      if (
        previous &&
        (previous.length < 60 || part.length < 40)
      ) {
        result[result.length - 1] = `${previous}\n\n${part}`;
      } else {
        result.push(part);
      }
    }

    return result;
  }

  // ===========================================================================
  // UTILITÁRIOS
  // ===========================================================================

  delay(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, Math.max(0, Number(ms) || 0));
    });
  }
}

module.exports = InterviewService;