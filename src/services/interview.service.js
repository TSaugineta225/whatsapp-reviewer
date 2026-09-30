//
// Camada de transporte entre WhatsApp e backend Python.
//
// [FIXES]
// [FIX-1] reportUnmatchedIncoming chamado em todos os caminhos órfãos.
// [FIX-2] isButton propagado ao sendInterviewTurn.
// [FIX-3] PII (phone, preview) mascarada nos logs.
// [FIX-4] _reschedulePendingItem respeita error.retryable e retryAfterMs.
//         Se o erro for definitivo (retryable=false), vai logo para
//         dead-letter em vez de queimar 10 tentativas.
// [FIX-5] Fila cheia → fallback com mensagem explícita ao candidato
//         em vez de descartar silenciosamente.
// [FIX-6] _processPendingTurns verifica _closing dentro do loop
//         de telefones.
// [FIX-7] Helpers rememberLidMapping / resolveLidToPhone expostos
//         para simetria com WhatsAppService. Delegam ao Redis.
//
// Responsabilidades:
//   - FIFO lógico por telefone
//   - lock distribuído por telefone
//   - dedupe de mensagens recebidas
//   - retry do backend com backoff
//   - retry da entrega WhatsApp sem repetir o backend
//   - limite de fila por telefone
//   - dead-letter para falhas persistentes
//   - renovação de lock em operações longas
//   - shutdown seguro do worker

'use strict';

const { randomUUID } = require('crypto');
const pino = require('pino');

const RedisService = require('./redis.service');
const YaneIntegrationService = require('./yane-integration.service');
const metrics = require('./metrics.service');

// =============================================================================
// CONFIG
// =============================================================================

const DEFAULT_TYPING_MS_PER_CHAR = 22;
const DEFAULT_TYPING_MAX_MS = 2_600;

const PENDING_PHONES_KEY = 'pending:phones';
const PENDING_TURN_KEY_PREFIX = 'pending:turn';
const DEAD_LETTER_KEY = 'dead:interview';

const QUEUE_SCHEMA_VERSION = 1;

const PENDING_MAX_ATTEMPTS = 10;
const MAX_PENDING_PER_PHONE = 20;

const PENDING_BACKOFF_BASE_MS = 30_000;
const PENDING_BACKOFF_MAX_MS = 15 * 60_000;

const DEAD_LETTER_RETRY_MS = 60 * 60_000;

const WORKER_INTERVAL_MS = 10_000;
const WORKER_BATCH_SIZE = 20;
const WORKER_MAX_PER_PHONE = 3;

const WORKER_LOCK_TTL = 300;
const WORKER_LOCK_REFRESH_MS = 60_000;

const MAX_LOOKUP_ATTEMPTS = 5;

const SOFT_RECOVERY_MESSAGE = 'Um momento, por favor. Já lhe respondo.';

// [FIX-5] Mensagem enviada quando a fila está cheia.
const QUEUE_FULL_MESSAGE =
  'Recebi a sua mensagem, mas estou com muitos pedidos. ' +
  'Vou responder assim que possível.';

const FALLBACK_COOLDOWN_MS = 60_000;
const FALLBACK_MAP_SWEEP_THRESHOLD = 1_000;
const FALLBACK_MAP_HARD_LIMIT = 5_000;

const BUBBLE_PAUSE_MS = 300;

// =============================================================================
// HELPERS
// =============================================================================

function clean(value) {
  return String(value ?? '')
    .replace(/^["'`]+|["'`]+$/g, '')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{4,}/g, '\n\n')
    .trim();
}

function normalizePhone(phone) {
  const digits = String(phone ?? '').replace(/\D/g, '');

  if (!digits) {
    return '';
  }

  if (digits.startsWith('00258')) {
    return digits.slice(2);
  }

  if (digits.startsWith('258')) {
    return digits;
  }

  if (digits.length === 9) {
    return `258${digits}`;
  }

  if (digits.length === 10 && digits.startsWith('0')) {
    return `258${digits.slice(1)}`;
  }

  return '';
}

// [FIX-3] helper de masking para logs.
function maskPhone(phone) {
  const normalized = normalizePhone(phone) || String(phone || '');

  const digits = normalized.replace(/\D/g, '');

  if (digits.length < 6) {
    return '***';
  }

  return `${digits.slice(0, 3)}***${digits.slice(-3)}`;
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

// [FIX-4] extrai retryAfterMs de um YaneIntegrationError.
function getRetryAfterMs(error) {
  const value = Number(error?.retryAfterMs);
  return Number.isFinite(value) && value > 0 ? value : null;
}

// [FIX-4] determina se vale a pena retentar.
function isRetryableError(error) {
  // Erros tipados têm retryable explícito.
  if (typeof error?.retryable === 'boolean') {
    return error.retryable;
  }

  // Fallback: qualquer coisa sem classificação é retentável.
  return true;
}

// =============================================================================
// SERVIÇO
// =============================================================================

class InterviewService {
  constructor(redis = null, options = {}) {
    this.redis = redis || new RedisService();
    this._ownsRedis = !redis;

    this.yane =
      options.yane ||
      new YaneIntegrationService();

    this.whatsapp = options.whatsapp || null;

    this.typingMsPerChar = Math.max(
      0,
      safeNumber(
        process.env.WHATSAPP_TYPING_MS_PER_CHAR,
        DEFAULT_TYPING_MS_PER_CHAR
      )
    );

    this.typingMax = Math.max(
      0,
      safeNumber(
        process.env.WHATSAPP_TYPING_MAX_MS,
        DEFAULT_TYPING_MAX_MS
      )
    );

    this._initialized = false;
    this._initializing = null;
    this._closing = false;
    this._lifecycleGeneration = 0;

    this._workerTimer = null;
    this._workerRunning = false;
    this._workerPromise = null;

    this._fallbackSentAt = new Map();

    this.logger = pino({
      level: process.env.LOG_LEVEL || 'info',
      base: { service: 'interview' },
    });
  }

  // ===========================================================================
  // LOGGING
  // ===========================================================================

  _log(level, message, context = {}) {
    try {
      const method = this.logger?.[level];

      if (typeof method !== 'function') {
        return;
      }

      method.call(this.logger, context, message);
    } catch (_) {
      // Logging nunca deve interromper a pipeline.
    }
  }

  // [FIX-3] masking de PII no contexto de log.
  _logContext(phone, extra = {}) {
    const context = {};

    if (phone) {
      context.phone = maskPhone(phone);
    }

    for (const [key, value] of Object.entries(extra)) {
      if (value === undefined || value === null) {
        continue;
      }

      if (key === 'preview' || key === 'message' || key === 'body') {
        // Nunca registar conteúdo.
        continue;
      }

      if (key === 'remoteJid' || key === 'jid') {
        context[key] = maskPhone(value);
        continue;
      }

      context[key] = value;
    }

    return context;
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
      context.retryable = error?.retryable || false;
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
    if (this._initialized && !this._closing) {
      return;
    }

    if (this._initializing) {
      return this._initializing;
    }

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

    if (this._closing || generation !== this._lifecycleGeneration) {
      return;
    }

    await this._checkBackendHealthAtBoot();

    if (this._closing || generation !== this._lifecycleGeneration) {
      return;
    }

    this._initialized = true;

    this._startWorker();
    this._triggerWorker();

    this.log('InterviewService inicializado.');
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
    if (this._closing) {
      return;
    }

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

  // Exposto para ser chamado por fora (shutdown, tests, manutenção).
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
  // MAPEAMENTO PHONE ↔ INTERVIEW
  // ===========================================================================

  async rememberInterview(phone, interviewId) {
    const normalizedPhone = normalizePhone(phone);

    if (!normalizedPhone || !interviewId) {
      return false;
    }

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

    if (!normalizedPhone) {
      return null;
    }

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
  // [FIX-7] MAPEAMENTO LID → PHONE
  //
  // Simetria com WhatsAppService. Normalmente é o WhatsAppService
  // que persiste o mapping ao enviar o convite, mas estes helpers
  // permitem consulta/reparação a partir de scripts.
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
  // START
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
      this.logWarn('startInterview ignorado: payload inválido.', phone, {
        interviewId,
        hasMessage: Boolean(initialMessage),
      });

      return { success: false, reason: 'invalid_payload' };
    }

    const mapped = await this.rememberInterview(phone, interviewId);

    if (!mapped) {
      metrics.errorsTotal.inc({ subsystem: 'redis' });

      return { success: false, reason: 'state_unavailable' };
    }

    metrics.interviewsStarted.inc();

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
      metrics.interviewsQueued.inc();
    } else {
      metrics.errorsTotal.inc({ subsystem: 'redis' });

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

    if (!phone || !message) {
      return { handled: false, reason: 'empty' };
    }

    // -------------------------------------------------------------------------
    // REDIS
    // -------------------------------------------------------------------------

    try {
      if (
        typeof this.redis.isAvailable === 'function' &&
        !this.redis.isAvailable()
      ) {
        throw new Error('redis_unavailable');
      }
    } catch (error) {
      metrics.errorsTotal.inc({ subsystem: 'redis' });

      this.logError(
        'Redis indisponível — mensagem não processável.',
        error,
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

    // -------------------------------------------------------------------------
    // DEDUPE
    // -------------------------------------------------------------------------

    if (messageId) {
      let isNew;

      try {
        isNew = await this.redis.markMessageSeen(messageId);
      } catch (error) {
        metrics.errorsTotal.inc({ subsystem: 'redis' });

        this.logError(
          'Falha no dedupe da mensagem.',
          error,
          phone,
          { msgId: messageId }
        );

        await this._safeSendFallback(phone);

        return {
          handled: true,
          queued: false,
          degraded: true,
          reason: 'dedupe_unavailable',
        };
      }

      if (!isNew) {
        metrics.turnsTotal.inc({ status: 'duplicate' });

        this.logDebug('Mensagem duplicada ignorada.', phone, {
          msgId: messageId,
        });

        return { handled: false, reason: 'duplicate' };
      }
    }

    // -------------------------------------------------------------------------
    // LOCK
    // -------------------------------------------------------------------------

    let result;

    try {
      result = await this._withPhoneLock(phone, () =>
        this._handleLockedMessage({
          phone,
          message,
          messageId,
          isButton: Boolean(options.isButton),
        })
      );
    } catch (error) {
      metrics.errorsTotal.inc({ subsystem: 'redis' });

      this.logError(
        'Erro ao executar mensagem sob lock.',
        error,
        phone,
        { msgId: messageId }
      );

      result = null;
    }

    if (result === null) {
      const queued = await this._queueIncomingTurn({
        phone,
        message,
        messageId,
        reason: 'lock_busy',
      });

      if (queued === 1) {
        metrics.turnsTotal.inc({ status: 'queued' });

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

    return result;
  }

  async _handleLockedMessage({
    phone,
    message,
    messageId,
    isButton = false,
  }) {
    // -------------------------------------------------------------------------
    // Preservar FIFO.
    // -------------------------------------------------------------------------

    const pending = await this._pendingCount(phone);

    if (pending > 0) {
      const queued = await this._queueIncomingTurn({
        phone,
        message,
        messageId,
        reason: 'pending_queue',
      });

      if (queued === 1) {
        metrics.turnsTotal.inc({ status: 'queued' });

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

    // -------------------------------------------------------------------------
    // Resolver entrevista
    // -------------------------------------------------------------------------

    let interviewId = await this.resolveInterviewId(phone);

    if (!interviewId) {
      try {
        const active = await this.yane.findActiveInterviewByPhone(
          phone
        );

        if (active?.id) {
          interviewId = active.id;

          await this.rememberInterview(phone, interviewId);
        }
      } catch (error) {
        metrics.errorsTotal.inc({ subsystem: 'backend' });

        this.logWarn(
          'Lookup da entrevista falhou — turno em fila.',
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
          turnId,
          interviewId: null,
          reason: 'lookup_failed',
        });

        if (queued === 1) {
          metrics.turnsTotal.inc({ status: 'queued' });

          return {
            handled: true,
            queued: true,
            reason: 'lookup_failed',
          };
        }

        await this._forgetMessageSeen(messageId);
        await this._safeSendFallback(phone);

        return {
          handled: true,
          queued: false,
          reason: 'state_unavailable',
        };
      }
    }

    if (!interviewId) {
      metrics.turnsTotal.inc({ status: 'no_interview' });

      this.logWarn(
        'Sem entrevista activa para o telefone — reportando órfã.',
        phone,
        { msgId: messageId }
      );

      // [FIX-1] reportar ao backend antes de descartar.
      // Best-effort: não bloqueia nem propaga erro.
      void this._reportOrphan({
        phone,
        message,
        messageId,
      });

      return {
        handled: false,
        reason: 'no_active_interview',
      };
    }

    // -------------------------------------------------------------------------
    // Backend
    // -------------------------------------------------------------------------

    let turn;

    try {
      turn = await metrics.time(
        metrics.turnDuration,
        { endpoint: 'turn' },
        () =>
          this.yane.sendInterviewTurn({
            interviewId,
            phone,
            message,
            turnId,
            messageId,
            // [FIX-2] propagar isButton
            isButton,
          })
      );

      if (!turn) {
        throw new Error('backend_empty_response');
      }
    } catch (error) {
      metrics.turnsTotal.inc({ status: 'queued' });
      metrics.errorsTotal.inc({ subsystem: 'backend' });

      this.logWarn(
        'Backend indisponível — turno em fila.',
        phone,
        {
          interviewId,
          turnId,
          msgId: messageId,
          error: errorMessage(error),
          retryable: error?.retryable !== false,
        }
      );

      const queued = await this._queueIncomingTurn({
        phone,
        message,
        messageId,
        turnId,
        interviewId,
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

    // -------------------------------------------------------------------------
    // Resposta
    // -------------------------------------------------------------------------

    const bubbles = this._extractBubbles(turn?.bubbles);
    const finished = Boolean(turn?.finished);
    const interviewStatus = turn?.interview_status || 'in_progress';

    if (finished) {
      await this.forgetInterview(phone);

      metrics.interviewsFinished.inc({ status: interviewStatus });

      this.log('Entrevista terminada.', phone, {
        interviewId,
        status: interviewStatus,
        credits: turn?.credits_charged,
        turnId,
      });
    }

    if (!bubbles.length) {
      metrics.turnsTotal.inc({ status: 'success' });

      return { handled: true, finished, status: interviewStatus };
    }

    // -------------------------------------------------------------------------
    // WhatsApp
    // -------------------------------------------------------------------------

    const delivery = await this._sendBubblesWithResult(
      phone,
      bubbles
    );

    if (delivery.ok) {
      metrics.turnsTotal.inc({ status: 'success' });

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
      metrics.errorsTotal.inc({ subsystem: 'redis' });
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

  // [FIX-1] helper central para reportar mensagens sem entrevista.
  async _reportOrphan({ phone, message, messageId }) {
    try {
      if (typeof this.yane.reportUnmatchedIncoming !== 'function') {
        return;
      }

      await this.yane.reportUnmatchedIncoming({
        phone,
        message,
        messageId,
      });
    } catch (error) {
      // Best-effort.
      this.logDebug(
        'Falha ao reportar mensagem órfã.',
        phone,
        {
          msgId: messageId,
          error: errorMessage(error),
        }
      );
    }
  }

  // ===========================================================================
  // FILA
  // ===========================================================================

  async _queueIncomingTurn({
    phone,
    message,
    messageId,
    turnId = randomUUID(),
    interviewId = null,
    reason = 'unknown',
  }) {
    let resolvedInterviewId = interviewId;

    if (!resolvedInterviewId) {
      resolvedInterviewId = await this.resolveInterviewId(phone);
    }

    return this._enqueuePendingItem(phone, {
      kind: 'turn',
      schemaVersion: QUEUE_SCHEMA_VERSION,

      phone,
      message,
      messageId: messageId || null,
      interviewId: resolvedInterviewId || null,
      turnId,

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

    if (!cleanBubbles.length) {
      return 1;
    }

    return this._enqueuePendingItem(phone, {
      kind: 'delivery',
      schemaVersion: QUEUE_SCHEMA_VERSION,

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
    const queueKey = this.redis.key(
      PENDING_TURN_KEY_PREFIX,
      phone
    );

    const phonesKey = this.redis.key(PENDING_PHONES_KEY);

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
        maxItems: MAX_PENDING_PER_PHONE,
      });

      // [FIX-5] fila cheia: avisar o candidato em vez de descartar
      // silenciosamente.
      if (result === 0) {
        this.logWarn(
          'Fila cheia — a notificar candidato.',
          phone,
          {
            kind: payload?.kind || null,
            turnId: payload?.turnId || null,
          }
        );

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

  // [FIX-5] notificação explícita quando a fila está cheia.
  async _sendQueueFullNotice(phone) {
    try {
      const client = this._getWhatsAppService();

      if (!this._isWhatsAppReady(client)) {
        return;
      }

      await client.sendMessage(phone, QUEUE_FULL_MESSAGE);
    } catch (error) {
      this.logDebug(
        'Falha ao enviar aviso de fila cheia.',
        phone,
        { error: errorMessage(error) }
      );
    }
  }

  async _peekPendingItem(phone) {
    const queueKey = this.redis.key(
      PENDING_TURN_KEY_PREFIX,
      phone
    );

    return this.redis.lindex(queueKey, -1);
  }

  async _dequeuePendingItem(phone) {
    const queueKey = this.redis.key(
      PENDING_TURN_KEY_PREFIX,
      phone
    );

    return this.redis.rpop(queueKey);
  }

  async _pendingCount(phone) {
    const queueKey = this.redis.key(
      PENDING_TURN_KEY_PREFIX,
      phone
    );

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
    if (this._workerTimer || this._closing) {
      return;
    }

    this._workerTimer = setInterval(
      () => this._triggerWorker(),
      WORKER_INTERVAL_MS
    );

    if (typeof this._workerTimer.unref === 'function') {
      this._workerTimer.unref();
    }

    this.log('Worker de recuperação iniciado.', null, {
      intervalMs: WORKER_INTERVAL_MS,
      batchSize: WORKER_BATCH_SIZE,
      maxPerPhone: WORKER_MAX_PER_PHONE,
    });
  }

  _stopWorker() {
    if (!this._workerTimer) {
      return;
    }

    clearInterval(this._workerTimer);
    this._workerTimer = null;

    this.log('Worker de recuperação parado.');
  }

  _triggerWorker() {
    if (this._closing || this._workerRunning) {
      return;
    }

    const promise = this._processPendingTurns();

    this._workerPromise = promise;

    promise
      .catch((error) => {
        this.logError('Worker falhou.', error);
      })
      .finally(() => {
        if (this._workerPromise === promise) {
          this._workerPromise = null;
        }
      });
  }

  async _processPendingTurns() {
    if (this._workerRunning || this._closing) {
      return;
    }

    if (!this._isWhatsAppReady()) {
      return;
    }

    if (
      typeof this.redis.isAvailable === 'function' &&
      !this.redis.isAvailable()
    ) {
      return;
    }

    this._workerRunning = true;

    try {
      const phonesKey = this.redis.key(PENDING_PHONES_KEY);

      const now = Date.now();

      const phones = await this.redis.zrangeByScore(
        phonesKey,
        '-inf',
        now,
        0,
        WORKER_BATCH_SIZE
      );

      try {
        metrics.queueDepth?.set(phones.length);
      } catch (_) {
        // Best-effort.
      }

      for (const phone of phones) {
        // [FIX-6] abortar o loop de telefones se estivermos a fechar.
        if (this._closing) {
          break;
        }

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
    if (typeof this.redis.refreshLock !== 'function') {
      return null;
    }

    const timer = setInterval(() => {
      if (this._closing) return;

      this.redis
        .refreshLock(phone, token, WORKER_LOCK_TTL)
        .catch((error) => {
          this.logWarn('Falha ao renovar lock.', phone, {
            error: errorMessage(error),
          });
        });
    }, WORKER_LOCK_REFRESH_MS);

    if (typeof timer.unref === 'function') {
      timer.unref();
    }

    return timer;
  }

  async _withPhoneLock(phone, fn) {
    const token = await this.redis.acquireLock(
      phone,
      WORKER_LOCK_TTL
    );

    if (!token) return null;

    const refreshTimer = this._startLockRefresh(phone, token);

    try {
      return await fn();
    } finally {
      if (refreshTimer) {
        clearInterval(refreshTimer);
      }

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
      WORKER_LOCK_TTL
    );

    if (!token) return;

    const refreshTimer = this._startLockRefresh(phone, token);

    try {
      let processed = 0;

      while (
        processed < WORKER_MAX_PER_PHONE &&
        !this._closing
      ) {
        const raw = await this._peekPendingItem(phone);

        if (!raw) break;

        let payload;

        try {
          payload = JSON.parse(raw);
        } catch (error) {
          this.logError(
            'Item inválido na fila.',
            error,
            phone
          );

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
            attempts: PENDING_MAX_ATTEMPTS,
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

          if (retryResult.removed) {
            processed += 1;
          }

          // Preservar FIFO.
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

        if (retryResult.removed) {
          processed += 1;
        }

        break;
      }

      const remaining = await this._pendingCount(phone);

      if (remaining === 0) {
        const phonesKey = this.redis.key(PENDING_PHONES_KEY);

        await this.redis.zrem(phonesKey, phone);
      }
    } finally {
      if (refreshTimer) {
        clearInterval(refreshTimer);
      }

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
        schemaVersion: QUEUE_SCHEMA_VERSION,
        phone,
        attempts: 0,
        lookupAttempts: 0,
      };
    }

    return {
      schemaVersion:
        payload.schemaVersion || QUEUE_SCHEMA_VERSION,

      kind: payload.kind || 'turn',

      phone: normalizePhone(payload.phone) || phone,

      message: payload.message
        ? String(payload.message)
        : undefined,

      messageId: payload.messageId || null,

      interviewId: payload.interviewId || null,

      turnId: payload.turnId || randomUUID(),

      bubbles: this._extractBubbles(payload.bubbles),

      finished: Boolean(payload.finished),

      interviewStatus: payload.interviewStatus || 'in_progress',

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
  // PROCESSAMENTO DE ITEM
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

    const { message, messageId, turnId } = payload;

    if (payload.attempts >= PENDING_MAX_ATTEMPTS) {
      return {
        action: 'dead_letter',
        reason: 'max_attempts_reached',
        payload,
      };
    }

    if (!interviewId) {
      interviewId = await this.resolveInterviewId(phone);

      if (!interviewId && lookupAttempts < MAX_LOOKUP_ATTEMPTS) {
        try {
          const active =
            await this.yane.findActiveInterviewByPhone(phone);

          if (active?.id) {
            interviewId = active.id;

            await this.rememberInterview(phone, interviewId);
          }
        } catch (error) {
          lookupAttempts += 1;

          metrics.errorsTotal.inc({ subsystem: 'backend' });

          this.logWarn('Lookup falhou em retry.', phone, {
            turnId,
            lookupAttempts,
            maxLookups: MAX_LOOKUP_ATTEMPTS,
            error: errorMessage(error),
          });

          if (lookupAttempts >= MAX_LOOKUP_ATTEMPTS) {
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
        if (lookupAttempts >= MAX_LOOKUP_ATTEMPTS) {
          return {
            action: 'dead_letter',
            reason: 'no_interview_resolvable',
            payload: { ...payload, lookupAttempts },
          };
        }

        // [FIX-1] reportar órfã antes de descartar.
        this.logWarn(
          'Turno recuperado sem entrevista resolvível — reportando órfã.',
          phone,
          { turnId }
        );

        await this._reportOrphan({
          phone,
          message,
          messageId,
        });

        return { action: 'ack' };
      }

      payload = {
        ...payload,
        interviewId,
        lookupAttempts,
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
          })
      );

      if (!turn) {
        throw new Error('backend_empty_response');
      }
    } catch (error) {
      metrics.errorsTotal.inc({ subsystem: 'backend' });

      // [FIX-4] erros definitivos vão logo para dead-letter.
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

    metrics.turnsTotal.inc({ status: 'recovered' });

    const bubbles = this._extractBubbles(turn.bubbles);
    const finished = Boolean(turn.finished);
    const interviewStatus = turn.interview_status || 'in_progress';

    if (finished) {
      await this.forgetInterview(phone);

      metrics.interviewsFinished.inc({ status: interviewStatus });

      this.log('Entrevista terminada (recuperada).', phone, {
        interviewId,
        status: interviewStatus,
        credits: turn.credits_charged,
        turnId,
      });
    }

    if (!bubbles.length) {
      return { action: 'ack' };
    }

    const delivery = await this._sendBubblesWithResult(
      phone,
      bubbles
    );

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
        schemaVersion: QUEUE_SCHEMA_VERSION,

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

    if (!bubbles.length) {
      return { action: 'ack' };
    }

    const delivery = await this._sendBubblesWithResult(
      phone,
      bubbles
    );

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

  // [FIX-4] assinatura estendida: retryAfterMs, retryable.
  async _reschedulePendingItem(
    phone,
    payload,
    retryAfterMs = 0,
    retryable = true
  ) {
    const attempts = Number(payload.attempts) + 1;

    const updated = { ...payload, attempts };

    // [FIX-4] erro definitivo: dead-letter imediato.
    if (!retryable) {
      const moved = await this._deadLetterPendingItem(phone, {
        reason: 'non_retryable_error',
        payload: updated,
      });

      if (moved) {
        return { removed: true };
      }

      await this._quarantinePendingItem(phone, updated);
      return { removed: false };
    }

    if (attempts >= PENDING_MAX_ATTEMPTS) {
      const moved = await this._deadLetterPendingItem(phone, {
        reason: 'max_attempts_reached',
        payload: updated,
      });

      if (moved) {
        return { removed: true };
      }

      await this._quarantinePendingItem(phone, updated);
      return { removed: false };
    }

    const exponential =
      PENDING_BACKOFF_BASE_MS *
      Math.pow(2, attempts - 1);

    const jitter = 0.8 + Math.random() * 0.4;

    const computedBackoff = Math.min(
      exponential * jitter,
      PENDING_BACKOFF_MAX_MS
    );

    // [FIX-4] quando o servidor indicou retryAfterMs, respeitar
    // (com um mínimo razoável para não bloquear a fila).
    const backoff = Number.isFinite(retryAfterMs) && retryAfterMs > 0
      ? Math.min(Math.max(retryAfterMs, 1_000), PENDING_BACKOFF_MAX_MS)
      : computedBackoff;

    const scheduledAt = Date.now() + Math.round(backoff);

    const queueKey = this.redis.key(
      PENDING_TURN_KEY_PREFIX,
      phone
    );

    const phonesKey = this.redis.key(PENDING_PHONES_KEY);

    try {
      if (
        typeof this.redis.reschedulePendingItem !== 'function'
      ) {
        throw new Error('reschedulePendingItem_unavailable');
      }

      const updatedOk =
        await this.redis.reschedulePendingItem({
          queueKey,
          phonesKey,
          phone,
          payload: JSON.stringify(updated),
          score: scheduledAt,
        });

      if (!updatedOk) {
        throw new Error('reschedule_pending_item_failed');
      }

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
        attempts: PENDING_MAX_ATTEMPTS,
        quarantinedAt: Date.now(),
      };

      const score = Date.now() + DEAD_LETTER_RETRY_MS;

      const queueKey = this.redis.key(
        PENDING_TURN_KEY_PREFIX,
        phone
      );

      const phonesKey = this.redis.key(PENDING_PHONES_KEY);

      await this.redis.reschedulePendingItem({
        queueKey,
        phonesKey,
        phone,
        payload: JSON.stringify(updated),
        score,
      });

      this.logWarn('Item colocado em quarentena.', phone, {
        turnId: payload.turnId || null,
        retryInMs: DEAD_LETTER_RETRY_MS,
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
    const queueKey = this.redis.key(
      PENDING_TURN_KEY_PREFIX,
      phone
    );

    const phonesKey = this.redis.key(PENDING_PHONES_KEY);

    const deadKey = this.redis.key(DEAD_LETTER_KEY);

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
      this.logDebug(
        'Falha ao remover marker de dedupe.',
        null,
        { msgId: messageId, error: errorMessage(error) }
      );
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

    if (!normalized.length) {
      return { ok: true, remaining: [] };
    }

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
        return {
          ok: false,
          remaining: normalized.slice(index),
        };
      }

      if (index < normalized.length - 1) {
        await this.delay(BUBBLE_PAUSE_MS);
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

      if (result === false) return false;

      if (
        result &&
        typeof result === 'object' &&
        result.status === 'failed'
      ) {
        return false;
      }

      return true;
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

    if (!last) return false;

    return Date.now() - last < FALLBACK_COOLDOWN_MS;
  }

  _trackFallback(phone) {
    const now = Date.now();

    this._fallbackSentAt.set(phone, now);

    if (
      this._fallbackSentAt.size < FALLBACK_MAP_SWEEP_THRESHOLD
    ) {
      return;
    }

    for (const [key, timestamp] of this._fallbackSentAt) {
      if (now - timestamp > FALLBACK_COOLDOWN_MS) {
        this._fallbackSentAt.delete(key);
      }
    }

    if (
      this._fallbackSentAt.size <= FALLBACK_MAP_HARD_LIMIT
    ) {
      return;
    }

    const overflow =
      this._fallbackSentAt.size - FALLBACK_MAP_HARD_LIMIT;

    let removed = 0;

    for (const key of this._fallbackSentAt.keys()) {
      if (removed >= overflow) break;

      this._fallbackSentAt.delete(key);
      removed += 1;
    }
  }

  async _safeSendFallback(phone, client = null) {
    const whatsappClient = client || this._getWhatsAppService();

    if (!this._isWhatsAppReady(whatsappClient)) {
      return false;
    }

    const normalizedPhone = normalizePhone(phone);

    if (!normalizedPhone) return false;

    if (this._isFallbackCoolingDown(normalizedPhone)) {
      return false;
    }

    this._trackFallback(normalizedPhone);

    try {
      const result = await whatsappClient.sendMessage(
        normalizedPhone,
        SOFT_RECOVERY_MESSAGE
      );

      if (result === false) return false;

      return true;
    } catch (error) {
      this.logError('Fallback falhou.', error, normalizedPhone);
      return false;
    }
  }

  // ===========================================================================
  // BUBBLES
  // ===========================================================================

  _extractBubbles(bubbles) {
    if (!Array.isArray(bubbles)) {
      return [];
    }

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