//
// Camada de transporte entre o WhatsApp e o backend Python.
//
// Garantias:
//
//   - FIFO por telefone
//   - lock distribuído por telefone
//   - dedupe de mensagens recebidas
//   - retry do backend com backoff
//   - retry da entrega WhatsApp sem repetir o backend
//   - limite de fila por telefone
//   - dead-letter para falhas persistentes
//   - renovação de lock em operações longas
//   - shutdown seguro do worker
//
// Fluxo:
//
//   WhatsApp
//      ↓
//   handleIncomingMessage()
//      ↓
//   dedupe
//      ↓
//   lock por telefone
//      ↓
//   fila pendente? ── sim ──→ fila FIFO
//      │
//      não
//      ↓
//   backend Python
//      ↓
//   resposta
//      ↓
//   WhatsApp
//
// Recuperação:
//
//   backend falhou
//      ↓
//   pending turn
//      ↓
//   worker
//      ↓
//   retry com backoff
//
//   backend respondeu
//   WhatsApp falhou
//      ↓
//   pending delivery
//      ↓
//   worker
//      ↓
//   retry somente da entrega
//
// O candidato nunca recebe mensagens técnicas.
//

const { randomUUID } = require('crypto');

const RedisService = require('./redis.service');
const YaneIntegrationService = require('./yane-integration.service');
const metrics = require('./metrics.service');

// ============================================================
// CONFIG
// ============================================================

const DEFAULT_TYPING_MS_PER_CHAR = 22;
const DEFAULT_TYPING_MAX_MS = 2600;

const PENDING_PHONES_KEY = 'pending:phones';
const PENDING_TURN_KEY_PREFIX = 'pending:turn';
const DEAD_LETTER_KEY = 'dead:interview';

const QUEUE_SCHEMA_VERSION = 1;

const PENDING_MAX_ATTEMPTS = 10;
const MAX_PENDING_PER_PHONE = 20;

const PENDING_BACKOFF_BASE_MS = 30_000;
const PENDING_BACKOFF_MAX_MS = 15 * 60_000;

const DEAD_LETTER_RETRY_MS = 60 * 60_000;

const WORKER_INTERVAL_MS = 20_000;
const WORKER_BATCH_SIZE = 5;
const WORKER_MAX_PER_PHONE = 3;

const WORKER_LOCK_TTL = 300;
const WORKER_LOCK_REFRESH_MS = 60_000;

const SOFT_RECOVERY_MESSAGE =
  'Um momento, por favor. Já lhe respondo.';

const FALLBACK_COOLDOWN_MS = 60_000;

// ============================================================
// HELPERS PUROS
// ============================================================

function clean(value) {
  return String(value ?? '')
    .replace(/^["'`]+|["'`]+$/g, '')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function normalizePhone(phone) {
  if (!phone) return '';

  const digits = String(phone).replace(/\D/g, '');

  if (!digits) return '';

  if (digits.startsWith('258')) {
    return digits;
  }

  if (digits.startsWith('0')) {
    return `258${digits.slice(1)}`;
  }

  return `258${digits}`;
}

function safeNumber(value, fallback) {
  const number = Number(value);

  return Number.isFinite(number)
    ? number
    : fallback;
}

function errorMessage(error) {
  return clean(
    error?.message || 'unknown_error'
  ).slice(0, 500);
}

// ============================================================
// SERVIÇO
// ============================================================

class InterviewService {
  /**
   * `redis` continua sendo aceito como primeiro argumento para
   * preservar compatibilidade com o código existente.
   *
   * O segundo argumento permite injetar dependências em testes
   * e evita depender obrigatoriamente de globals.
   *
   * Exemplo:
   *
   * new InterviewService(redis, {
   *   yane,
   *   whatsapp,
   * });
   */
  constructor(redis = null, options = {}) {
    this.redis = redis || new RedisService();
    this._ownsRedis = !redis;

    this.yane =
      options.yane ||
      new YaneIntegrationService();

    this.whatsapp =
      options.whatsapp ||
      null;

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

    this._workerTimer = null;
    this._workerRunning = false;
    this._workerPromise = null;

    this._initializing = null;
    this._initialized = false;
    this._closing = false;

    // Evita mandar a mensagem de fallback dezenas
    // de vezes seguidas durante uma falha de infraestrutura.
    this._fallbackSentAt = new Map();
  }

  // ============================================================
  // CICLO DE VIDA
  // ============================================================

  async initialize() {
    if (this._initialized) {
      return;
    }

    if (this._initializing) {
      return this._initializing;
    }

    this._initializing = this._initialize();

    try {
      await this._initializing;
    } finally {
      this._initializing = null;
    }
  }

  async _initialize() {
    this._closing = false;

    await this.redis.initialize();

    this._initialized = true;

    this._startWorker();

    // Primeiro ciclo imediato.
    this._triggerWorker();
  }

  async close() {
    if (this._closing) {
      return;
    }

    this._closing = true;

    this._stopWorker();

    // Não abandona um ciclo do worker no meio.
    if (this._workerPromise) {
      try {
        await this._workerPromise;
      } catch (error) {
        console.error(
          '[INTERVIEW] Erro durante shutdown do worker:',
          error.message
        );
      }
    }

    this._initialized = false;

    if (this._ownsRedis) {
      await this.redis.close();
    }
  }

  setWhatsAppService(service) {
    this.whatsapp = service || null;
  }

  _getWhatsAppService() {
    return (
      this.whatsapp ||
      global.whatsappService ||
      null
    );
  }

  _isWhatsAppReady() {
    const client =
      this._getWhatsAppService();

    return Boolean(
      client &&
      client.isReady !== false
    );
  }

  // ============================================================
  // MAPEAMENTO PHONE ↔ INTERVIEW
  // ============================================================

  async rememberInterview(
    phone,
    interviewId
  ) {
    const normalizedPhone =
      normalizePhone(phone);

    if (!normalizedPhone || !interviewId) {
      return false;
    }

    return this.redis.rememberInterview(
      normalizedPhone,
      interviewId
    );
  }

  async resolveInterviewId(phone) {
    const normalizedPhone =
      normalizePhone(phone);

    if (!normalizedPhone) {
      return null;
    }

    return this.redis.resolveInterviewId(
      normalizedPhone
    );
  }

  async forgetInterview(phone) {
    const normalizedPhone =
      normalizePhone(phone);

    if (!normalizedPhone) {
      return false;
    }

    return this.redis.forgetInterview(
      normalizedPhone
    );
  }

  // ============================================================
  // START
  // ============================================================

  async startInterview(payload = {}) {
    const phone =
      normalizePhone(payload.phone);

    const interviewId =
      payload.interviewId ||
      payload.interview_id;

    const initialMessage =
      clean(
        payload.initialMessage ||
        payload.initial_message
      );

    if (
      !phone ||
      !interviewId ||
      !initialMessage
    ) {
      console.warn(
        '[INTERVIEW] startInterview ignorado: payload inválido.',
        {
          phone,
          interviewId,
          hasMessage:
            Boolean(initialMessage),
        }
      );

      return {
        success: false,
        reason: 'invalid_payload',
      };
    }

    // O mapping é parte essencial do estado.
    const mapped =
      await this.rememberInterview(
        phone,
        interviewId
      );

    if (!mapped) {
      metrics.errorsTotal.inc({
        subsystem: 'redis',
      });

      console.error(
        `[INTERVIEW] Não foi possível persistir ` +
        `mapping da entrevista | ` +
        `interview=${interviewId} | phone=${phone}`
      );

      return {
        success: false,
        reason: 'state_unavailable',
      };
    }

    metrics.interviewsStarted.inc();

    const delivery =
      await this._sendText(
        phone,
        initialMessage
      );

    if (delivery.ok) {
      console.log(
        `[INTERVIEW] Convite enviado | ` +
        `interview=${interviewId} | phone=${phone}`
      );

      return {
        success: true,
        interviewId,
        phone,
      };
    }

    // Backend já criou a entrevista.
    //
    // Não repetimos a criação.
    // Guardamos somente a entrega.
    const queued =
      await this._enqueuePendingDelivery({
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
      metrics.errorsTotal.inc({
        subsystem: 'redis',
      });

      await this._safeSendFallback(phone);
    }

    console.warn(
      `[INTERVIEW] Convite não entregue | ` +
      `interview=${interviewId} | ` +
      `phone=${phone} | queued=${queued === 1}`
    );

    return {
      success: true,
      interviewId,
      phone,
      queued: queued === 1,
    };
  }

  // ============================================================
  // MENSAGEM RECEBIDA
  // ============================================================

  async handleIncomingMessage(
    from,
    text,
    options = {}
  ) {
    const phone =
      normalizePhone(from);

    const message =
      clean(text);

    const messageId =
      options.messageId || null;

    if (!phone || !message) {
      return {
        handled: false,
        reason: 'empty',
      };
    }

    // Redis é infraestrutura de estado desta conversa.
    //
    // Sem ele não temos:
    //   - dedupe
    //   - lock
    //   - fila
    //   - mapping confiável
    //
    // Portanto não fingimos que "não existe entrevista".
    if (
      typeof this.redis.isAvailable ===
        'function' &&
      !this.redis.isAvailable()
    ) {
      metrics.errorsTotal.inc({
        subsystem: 'redis',
      });

      await this._safeSendFallback(phone);

      return {
        handled: true,
        queued: false,
        degraded: true,
        reason: 'redis_unavailable',
      };
    }

    // ----------------------------------------------------------
    // DEDUPE
    // ----------------------------------------------------------

    if (messageId) {
      const isNew =
        await this.redis.markMessageSeen(
          messageId
        );

      if (!isNew) {
        metrics.turnsTotal.inc({
          status: 'duplicate',
        });

        console.log(
          `[INTERVIEW] Mensagem duplicada ignorada | ` +
          `msgId=${messageId}`
        );

        return {
          handled: false,
          reason: 'duplicate',
        };
      }
    }

    // ----------------------------------------------------------
    // LOCK
    // ----------------------------------------------------------

    const result =
      await this._withPhoneLock(
        phone,
        async () => {
          return this._handleLockedMessage({
            phone,
            message,
            messageId,
          });
        }
      );

    // ----------------------------------------------------------
    // LOCK OCUPADO
    // ----------------------------------------------------------

    if (result === null) {
      const queued =
        await this._queueIncomingTurn({
          phone,
          message,
          messageId,
          reason: 'lock_busy',
        });

      if (queued === 1) {
        metrics.turnsTotal.inc({
          status: 'queued',
        });

        return {
          handled: true,
          queued: true,
          reason: 'lock_busy',
        };
      }

      // A mensagem não foi persistida.
      // Libera dedupe para permitir retry do webhook.
      await this._forgetMessageSeen(
        messageId
      );

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
  }) {
    // ----------------------------------------------------------
    // FILA JÁ TEM TRABALHO
    // ----------------------------------------------------------

    const pending =
      await this._pendingCount(phone);

    if (pending > 0) {
      const queued =
        await this._queueIncomingTurn({
          phone,
          message,
          messageId,
          reason: 'pending_queue',
        });

      if (queued === 1) {
        metrics.turnsTotal.inc({
          status: 'queued',
        });

        return {
          handled: true,
          queued: true,
          reason: 'pending_queue',
        };
      }

      await this._forgetMessageSeen(
        messageId
      );

      await this._safeSendFallback(phone);

      return {
        handled: true,
        queued: false,
        reason: 'queue_unavailable',
      };
    }

    const turnId = randomUUID();

    // ----------------------------------------------------------
    // RESOLVE ENTREVISTA
    // ----------------------------------------------------------

    let interviewId =
      await this.resolveInterviewId(phone);

    // Mapping pode ter expirado.
    // Tentamos o backend como fallback.
    if (!interviewId) {
      try {
        const active =
          await this.yane
            .findActiveInterviewByPhone(
              phone
            );

        if (active?.id) {
          interviewId = active.id;

          await this.rememberInterview(
            phone,
            interviewId
          );
        }
      } catch (error) {
        metrics.errorsTotal.inc({
          subsystem: 'backend',
        });

        console.warn(
          `[INTERVIEW] Lookup falhou | ` +
          `phone=${phone} | ` +
          `error=${error.message}`
        );

        const queued =
          await this._queueIncomingTurn({
            phone,
            message,
            messageId,
            turnId,
            interviewId: null,
            reason: 'lookup_failed',
          });

        if (queued === 1) {
          metrics.turnsTotal.inc({
            status: 'queued',
          });

          return {
            handled: true,
            queued: true,
            reason: 'lookup_failed',
          };
        }

        await this._forgetMessageSeen(
          messageId
        );

        await this._safeSendFallback(phone);

        return {
          handled: true,
          queued: false,
          reason: 'state_unavailable',
        };
      }
    }

    if (!interviewId) {
      metrics.turnsTotal.inc({
        status: 'no_interview',
      });

      return {
        handled: false,
        reason: 'no_active_interview',
      };
    }

    // ----------------------------------------------------------
    // BACKEND
    // ----------------------------------------------------------

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
          })
      );
    } catch (error) {
      metrics.turnsTotal.inc({
        status: 'queued',
      });

      metrics.errorsTotal.inc({
        subsystem: 'backend',
      });

      console.warn(
        `[INTERVIEW] Backend indisponível — ` +
        `turno em fila | ` +
        `interview=${interviewId} | ` +
        `phone=${phone} | ` +
        `error=${error.message}`
      );

      const queued =
        await this._queueIncomingTurn({
          phone,
          message,
          messageId,
          turnId,
          interviewId,
          reason: 'backend_error',
        });

      if (queued !== 1) {
        await this._forgetMessageSeen(
          messageId
        );

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

    // ----------------------------------------------------------
    // RESPOSTA
    // ----------------------------------------------------------

    const bubbles =
      this._extractBubbles(
        turn?.bubbles
      );

    const finished =
      Boolean(turn?.finished);

    const interviewStatus =
      turn?.interview_status ||
      'in_progress';

    // Se a entrevista acabou, o mapping deixa de ser necessário.
    //
    // A entrega da resposta final é independente do mapping.
    if (finished) {
      await this.forgetInterview(
        phone
      );

      metrics.interviewsFinished.inc({
        status: interviewStatus,
      });

      console.log(
        `[INTERVIEW] Terminada | ` +
        `interview=${interviewId} | ` +
        `status=${interviewStatus} | ` +
        `credits=${turn?.credits_charged}`
      );
    }

    if (!bubbles.length) {
      metrics.turnsTotal.inc({
        status: 'success',
      });

      return {
        handled: true,
        finished,
        status: interviewStatus,
      };
    }

    // ----------------------------------------------------------
    // WHATSAPP
    // ----------------------------------------------------------

    const delivery =
      await this._sendBubblesWithResult(
        phone,
        bubbles
      );

    if (delivery.ok) {
      metrics.turnsTotal.inc({
        status: 'success',
      });

      return {
        handled: true,
        finished,
        status: interviewStatus,
      };
    }

    // O backend JÁ processou este turnId.
    //
    // Só a entrega fica pendente.
    const queued =
      await this._enqueuePendingDelivery({
        phone,
        bubbles: delivery.remaining,
        interviewId,
        turnId,
        finished,
        interviewStatus,
      });

    if (queued !== 1) {
      metrics.errorsTotal.inc({
        subsystem: 'redis',
      });

      await this._safeSendFallback(phone);
    }

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

  // ============================================================
  // FILA
  // ============================================================

  async _queueIncomingTurn({
    phone,
    message,
    messageId,
    turnId = randomUUID(),
    interviewId = null,
    reason = 'unknown',
  }) {
    let resolvedInterviewId =
      interviewId;

    // Guardamos o ID no payload sempre que possível.
    if (!resolvedInterviewId) {
      resolvedInterviewId =
        await this.resolveInterviewId(
          phone
        );
    }

    return this._enqueuePendingItem(
      phone,
      {
        kind: 'turn',
        schemaVersion:
          QUEUE_SCHEMA_VERSION,

        phone,
        message,
        messageId:
          messageId || null,

        interviewId:
          resolvedInterviewId || null,

        turnId,

        attempts: 0,
        enqueuedAt: Date.now(),
        lastError: null,
        queueReason: reason,
      }
    );
  }

  async _enqueuePendingDelivery({
    phone,
    bubbles,
    interviewId,
    turnId,
    finished = false,
    interviewStatus = 'in_progress',
  }) {
    const cleanBubbles =
      this._extractBubbles(
        bubbles
      );

    if (!cleanBubbles.length) {
      return 1;
    }

    return this._enqueuePendingItem(
      phone,
      {
        kind: 'delivery',
        schemaVersion:
          QUEUE_SCHEMA_VERSION,

        phone,
        bubbles: cleanBubbles,

        interviewId,
        turnId,

        finished:
          Boolean(finished),

        interviewStatus,

        attempts: 0,
        enqueuedAt: Date.now(),
        lastError: null,
      }
    );
  }

  async _enqueuePendingItem(
    phone,
    payload
  ) {
    const queueKey =
      this.redis.key(
        PENDING_TURN_KEY_PREFIX,
        phone
      );

    const phonesKey =
      this.redis.key(
        PENDING_PHONES_KEY
      );

    try {
      if (
        typeof this.redis
          .enqueuePendingItem !==
        'function'
      ) {
        console.error(
          '[INTERVIEW] RedisService sem enqueuePendingItem().'
        );

        return 0;
      }

      /*
       * IMPORTANTE:
       *
       * A operação é atômica no Redis:
       *
       *   LLEN
       *   limite
       *   LPUSH
       *   ZADD NX
       *
       * Portanto não existe:
       *
       *   fila gravada + telefone não agendado
       *
       * e uma nova mensagem não antecipa um retry
       * já agendado.
       */
      return await this.redis.enqueuePendingItem({
        queueKey,
        phonesKey,
        phone,
        payload:
          JSON.stringify(payload),
        score: Date.now(),
        maxItems:
          MAX_PENDING_PER_PHONE,
      });
    } catch (error) {
      console.error(
        `[INTERVIEW] Falha ao enfileirar | ` +
        `phone=${phone} | ` +
        `error=${error.message}`
      );

      return 0;
    }
  }

  async _dequeuePendingTurn(phone) {
    const queueKey =
      this.redis.key(
        PENDING_TURN_KEY_PREFIX,
        phone
      );

    return this.redis.rpop(queueKey);
  }

  async _peekPendingTurn(phone) {
    const queueKey =
      this.redis.key(
        PENDING_TURN_KEY_PREFIX,
        phone
      );

    // LPUSH coloca itens novos no início.
    // O item mais antigo está no final.
    return this.redis.lindex(
      queueKey,
      -1
    );
  }

  async _pendingCount(phone) {
    const queueKey =
      this.redis.key(
        PENDING_TURN_KEY_PREFIX,
        phone
      );

    return this.redis.llen(queueKey);
  }

  // ============================================================
  // WORKER
  // ============================================================

  _startWorker() {
    if (this._workerTimer) {
      return;
    }

    this._workerTimer =
      setInterval(() => {
        this._triggerWorker();
      }, WORKER_INTERVAL_MS);

    if (
      typeof this._workerTimer.unref ===
      'function'
    ) {
      this._workerTimer.unref();
    }

    console.log(
      '[INTERVIEW] Worker de recuperação iniciado.'
    );
  }

  _stopWorker() {
    if (!this._workerTimer) {
      return;
    }

    clearInterval(
      this._workerTimer
    );

    this._workerTimer = null;

    console.log(
      '[INTERVIEW] Worker de recuperação parado.'
    );
  }

  _triggerWorker() {
    if (
      this._closing ||
      this._workerRunning
    ) {
      return;
    }

    const promise =
      this._processPendingTurns();

    this._workerPromise = promise;

    promise
      .catch((error) => {
        console.error(
          '[INTERVIEW] Worker erro:',
          error.message
        );
      })
      .finally(() => {
        if (
          this._workerPromise ===
          promise
        ) {
          this._workerPromise = null;
        }
      });
  }

  async _processPendingTurns() {
    if (
      this._workerRunning ||
      this._closing
    ) {
      return;
    }

    if (!this._isWhatsAppReady()) {
      return;
    }

    if (
      typeof this.redis.isAvailable ===
        'function' &&
      !this.redis.isAvailable()
    ) {
      return;
    }

    this._workerRunning = true;

    try {
      const phonesKey =
        this.redis.key(
          PENDING_PHONES_KEY
        );

      const now =
        Date.now();

      const phones =
        await this.redis.zrangeByScore(
          phonesKey,
          '-inf',
          now,
          0,
          WORKER_BATCH_SIZE
        );

      for (
        const phone of phones
      ) {
        if (this._closing) {
          break;
        }

        await this._processPhoneQueue(
          phone
        );
      }
    } finally {
      this._workerRunning = false;
    }
  }

  // ============================================================
  // LOCK POR TELEFONE
  // ============================================================

  async _withPhoneLock(
    phone,
    fn
  ) {
    const token =
      await this.redis.acquireLock(
        phone
      );

    if (!token) {
      return null;
    }

    const refreshTimer =
      setInterval(() => {
        if (
          typeof this.redis
            .refreshLock !==
          'function'
        ) {
          return;
        }

        this.redis
          .refreshLock(
            phone,
            token,
            WORKER_LOCK_TTL
          )
          .catch((error) => {
            console.warn(
              `[INTERVIEW] Falha renovando lock | ` +
              `phone=${phone} | ` +
              `error=${error.message}`
            );
          });
      }, WORKER_LOCK_REFRESH_MS);

    if (
      typeof refreshTimer.unref ===
      'function'
    ) {
      refreshTimer.unref();
    }

    try {
      return await fn();
    } finally {
      clearInterval(
        refreshTimer
      );

      await this.redis.releaseLock(
        phone,
        token
      );
    }
  }

  // ============================================================
  // PROCESSAMENTO DE FILA POR TELEFONE
  // ============================================================

  async _processPhoneQueue(phone) {
    const token =
      await this.redis.acquireLock(
        phone,
        WORKER_LOCK_TTL
      );

    if (!token) {
      return;
    }

    const refreshTimer =
      setInterval(() => {
        if (
          typeof this.redis
            .refreshLock !==
          'function'
        ) {
          return;
        }

        this.redis
          .refreshLock(
            phone,
            token,
            WORKER_LOCK_TTL
          )
          .catch((error) => {
            console.warn(
              `[INTERVIEW] Falha renovando lock do worker | ` +
              `phone=${phone} | ` +
              `error=${error.message}`
            );
          });
      }, WORKER_LOCK_REFRESH_MS);

    if (
      typeof refreshTimer.unref ===
      'function'
    ) {
      refreshTimer.unref();
    }

    try {
      let processed = 0;

      while (
        processed <
        WORKER_MAX_PER_PHONE
      ) {
        if (this._closing) {
          break;
        }

        const raw =
          await this._peekPendingTurn(
            phone
          );

        if (!raw) {
          break;
        }

        let payload;

        try {
          payload =
            JSON.parse(raw);
        } catch (error) {
          console.error(
            `[INTERVIEW] Item inválido na fila | ` +
            `phone=${phone}`
          );

          await this._deadLetterPendingItem(
            phone,
            {
              reason: 'invalid_json',
              raw,
            }
          );

          processed++;
          continue;
        }

        // Recuperação após crash entre as tentativas
        // e o dead-letter.
        if (
          Number(payload.attempts || 0) >=
          PENDING_MAX_ATTEMPTS
        ) {
          const moved =
            await this._deadLetterPendingItem(
              phone,
              {
                reason:
                  'max_attempts_reached',
                payload,
              }
            );

          if (moved) {
            processed++;
            continue;
          }

          // Infraestrutura de dead-letter também falhou.
          // Não martelamos o backend.
          await this._quarantinePendingItem(
            phone,
            payload
          );

          break;
        }

        const result =
          await this._processPendingItem(
            phone,
            payload
          );

        if (result.success) {
          await this._dequeuePendingTurn(
            phone
          );

          processed++;
          continue;
        }

        const nextPayload =
          result.payload ||
          payload;

        const retryResult =
          await this._reschedulePendingItem(
            phone,
            nextPayload
          );

        // Foi removido para dead-letter.
        if (retryResult.removed) {
          processed++;
        }

        // Retry normal: preservar ordem e
        // esperar o próximo agendamento.
        break;
      }

      const remaining =
        await this._pendingCount(
          phone
        );

      if (remaining === 0) {
        const phonesKey =
          this.redis.key(
            PENDING_PHONES_KEY
          );

        await this.redis.zrem(
          phonesKey,
          phone
        );
      }
    } finally {
      clearInterval(
        refreshTimer
      );

      await this.redis.releaseLock(
        phone,
        token
      );
    }
  }

  // ============================================================
  // PROCESSAMENTO DE ITEM
  // ============================================================

  async _processPendingItem(
    phone,
    payload
  ) {
    if (
      payload.kind ===
      'delivery'
    ) {
      return this._processPendingDelivery(
        phone,
        payload
      );
    }

    return this._processPendingTurn(
      phone,
      payload
    );
  }

  // ============================================================
  // TURNO PENDENTE → BACKEND
  // ============================================================

  async _processPendingTurn(
    phone,
    payload
  ) {
    const {
      message,
      messageId,
      turnId,
      attempts = 0,
    } = payload;

    let interviewId =
      payload.interviewId ||
      await this.resolveInterviewId(
        phone
      );

    // Compatibilidade com itens antigos
    // que não guardavam interviewId.
    if (!interviewId) {
      try {
        const active =
          await this.yane
            .findActiveInterviewByPhone(
              phone
            );

        if (active?.id) {
          interviewId =
            active.id;

          await this.rememberInterview(
            phone,
            interviewId
          );
        }
      } catch (error) {
        metrics.errorsTotal.inc({
          subsystem: 'backend',
        });

        return {
          success: false,
          payload: {
            ...payload,
            lastError:
              errorMessage(error),
            attempts,
          },
        };
      }
    }

    if (!interviewId) {
      console.log(
        `[INTERVIEW] Turno descartado ` +
        `(sem entrevista) | ` +
        `phone=${phone} | ` +
        `turnId=${turnId}`
      );

      return {
        success: true,
      };
    }

    let turn;

    try {
      turn = await metrics.time(
        metrics.turnDuration,
        {
          endpoint:
            'turn-recovered',
        },
        () =>
          this.yane.sendInterviewTurn({
            interviewId,
            phone,
            message,
            turnId,
            messageId,
          })
      );
    } catch (error) {
      metrics.turnsTotal.inc({
        status:
          'backend_error',
      });

      metrics.errorsTotal.inc({
        subsystem:
          'backend',
      });

      console.warn(
        `[INTERVIEW] Recuperação falhou | ` +
        `interview=${interviewId} | ` +
        `phone=${phone} | ` +
        `attempts=${attempts + 1} | ` +
        `error=${error.message}`
      );

      return {
        success: false,
        payload: {
          ...payload,
          interviewId,
          lastError:
            errorMessage(error),
        },
      };
    }

    metrics.turnsTotal.inc({
      status: 'recovered',
    });

    const bubbles =
      this._extractBubbles(
        turn?.bubbles
      );

    const finished =
      Boolean(turn?.finished);

    const interviewStatus =
      turn?.interview_status ||
      'in_progress';

    if (finished) {
      await this.forgetInterview(
        phone
      );

      metrics.interviewsFinished.inc({
        status:
          interviewStatus,
      });

      console.log(
        `[INTERVIEW] Terminada (recuperada) | ` +
        `interview=${interviewId} | ` +
        `status=${interviewStatus} | ` +
        `credits=${turn?.credits_charged}`
      );
    }

    if (!bubbles.length) {
      return {
        success: true,
      };
    }

    const delivery =
      await this._sendBubblesWithResult(
        phone,
        bubbles
      );

    if (delivery.ok) {
      console.log(
        `[INTERVIEW] Turno recuperado | ` +
        `interview=${interviewId} | ` +
        `phone=${phone}`
      );

      return {
        success: true,
      };
    }

    // O backend já processou esse turnId.
    // Agora só guardamos a entrega.
    return {
      success: false,
      payload: {
        kind: 'delivery',
        schemaVersion:
          QUEUE_SCHEMA_VERSION,

        phone,
        bubbles:
          delivery.remaining,

        interviewId,
        turnId,

        finished,
        interviewStatus,

        attempts: 0,
        enqueuedAt:
          Date.now(),

        lastError:
          'whatsapp_delivery_failed',
      },
    };
  }

  // ============================================================
  // ENTREGA PENDENTE → WHATSAPP
  // ============================================================

  async _processPendingDelivery(
    phone,
    payload
  ) {
    const bubbles =
      this._extractBubbles(
        payload.bubbles
      );

    if (!bubbles.length) {
      return {
        success: true,
      };
    }

    const delivery =
      await this._sendBubblesWithResult(
        phone,
        bubbles
      );

    if (delivery.ok) {
      console.log(
        `[INTERVIEW] Resposta pendente entregue | ` +
        `phone=${phone} | ` +
        `turnId=${payload.turnId}`
      );

      return {
        success: true,
      };
    }

    return {
      success: false,
      payload: {
        ...payload,
        bubbles:
          delivery.remaining,

        lastError:
          'whatsapp_delivery_failed',
      },
    };
  }

  // ============================================================
  // RETRY / BACKOFF
  // ============================================================

  async _reschedulePendingItem(
    phone,
    payload
  ) {
    const attempts =
      Number(payload.attempts || 0) + 1;

    const updated = {
      ...payload,
      attempts,
    };

    // ----------------------------------------------------------
    // DEAD LETTER
    // ----------------------------------------------------------

    if (
      attempts >= PENDING_MAX_ATTEMPTS
    ) {
      const moved =
        await this._deadLetterPendingItem(
          phone,
          {
            reason:
              'max_attempts_reached',
            payload: updated,
          }
        );

      if (moved) {
        return {
          removed: true,
        };
      }

      // Dead-letter também falhou.
      //
      // Coloca em quarentena por uma hora.
      // O worker não volta a chamar o backend
      // a cada 20 segundos.
      await this._quarantinePendingItem(
        phone,
        updated
      );

      return {
        removed: false,
      };
    }

    // ----------------------------------------------------------
    // BACKOFF
    // ----------------------------------------------------------

    const exponential =
      PENDING_BACKOFF_BASE_MS *
      Math.pow(
        2,
        attempts - 1
      );

    const jitter =
      0.8 +
      Math.random() * 0.4;

    const backoff =
      Math.min(
        exponential * jitter,
        PENDING_BACKOFF_MAX_MS
      );

    const scheduledAt =
      Date.now() + backoff;

    const queueKey =
      this.redis.key(
        PENDING_TURN_KEY_PREFIX,
        phone
      );

    const phonesKey =
      this.redis.key(
        PENDING_PHONES_KEY
      );

    const updatedOk =
      await this.redis
        .reschedulePendingItem({
          queueKey,
          phonesKey,
          phone,
          payload:
            JSON.stringify(updated),
          score:
            scheduledAt,
        });

    if (!updatedOk) {
      console.error(
        `[INTERVIEW] Falha ao reagendar item | ` +
        `phone=${phone} | ` +
        `turnId=${payload.turnId}`
      );

      return {
        removed: false,
      };
    }

    console.log(
      `[INTERVIEW] Item reagendado | ` +
      `phone=${phone} | ` +
      `kind=${updated.kind || 'turn'} | ` +
      `attempts=${attempts} | ` +
      `backoff=${Math.round(
        backoff / 1000
      )}s`
    );

    return {
      removed: false,
    };
  }

  async _quarantinePendingItem(
    phone,
    payload
  ) {
    const queueKey =
      this.redis.key(
        PENDING_TURN_KEY_PREFIX,
        phone
      );

    const phonesKey =
      this.redis.key(
        PENDING_PHONES_KEY
      );

    const score =
      Date.now() +
      DEAD_LETTER_RETRY_MS;

    const updated = {
      ...payload,
      attempts:
        PENDING_MAX_ATTEMPTS,
      quarantinedAt:
        Date.now(),
    };

    try {
      await this.redis
        .reschedulePendingItem({
          queueKey,
          phonesKey,
          phone,
          payload:
            JSON.stringify(updated),
          score,
        });

      console.error(
        `[INTERVIEW] Item em quarentena | ` +
        `phone=${phone} | ` +
        `turnId=${payload.turnId}`
      );
    } catch (error) {
      console.error(
        `[INTERVIEW] Falha na quarentena | ` +
        `phone=${phone} | ` +
        `error=${error.message}`
      );
    }
  }

  async _deadLetterPendingItem(
    phone,
    context = {}
  ) {
    const queueKey =
      this.redis.key(
        PENDING_TURN_KEY_PREFIX,
        phone
      );

    const phonesKey =
      this.redis.key(
        PENDING_PHONES_KEY
      );

    const deadKey =
      this.redis.key(
        DEAD_LETTER_KEY
      );

    const item = {
      phone,
      failedAt:
        Date.now(),
      ...context,
    };

    try {
      const moved =
        await this.redis
          .movePendingToDeadLetter({
            queueKey,
            phonesKey,
            deadKey,
            phone,
            payload:
              JSON.stringify(item),
          });

      if (moved) {
        console.error(
          `[INTERVIEW] Item enviado para dead-letter | ` +
          `phone=${phone}`
        );
      }

      return Boolean(moved);
    } catch (error) {
      console.error(
        `[INTERVIEW] Dead-letter falhou | ` +
        `phone=${phone} | ` +
        `error=${error.message}`
      );

      return false;
    }
  }

  // ============================================================
  // DEDUPE
  // ============================================================

  async _forgetMessageSeen(
    messageId
  ) {
    if (!messageId) {
      return;
    }

    try {
      await this.redis.del(
        this.redis.key(
          'msg',
          messageId
        )
      );
    } catch (_) {
      // Best-effort.
    }
  }

  // ============================================================
  // ENVIO
  // ============================================================

  async sendBubbles(
    phone,
    bubbles,
    client = this._getWhatsAppService()
  ) {
    const result =
      await this._sendBubblesWithResult(
        phone,
        bubbles,
        client
      );

    return result.ok;
  }

  async sendHuman(
    phone,
    text,
    client = this._getWhatsAppService()
  ) {
    const bubbles =
      this.splitBubbles(text);

    if (!bubbles.length) {
      return true;
    }

    const result =
      await this._sendBubblesWithResult(
        phone,
        bubbles,
        client
      );

    return result.ok;
  }

  async _sendText(
    phone,
    text,
    client = this._getWhatsAppService()
  ) {
    return this._sendBubblesWithResult(
      phone,
      this.splitBubbles(text),
      client
    );
  }

  async _sendBubblesWithResult(
    phone,
    bubbles,
    client = this._getWhatsAppService()
  ) {
    const normalized =
      this._extractBubbles(
        bubbles
      );

    if (!normalized.length) {
      return {
        ok: true,
        remaining: [],
      };
    }

    if (!client) {
      return {
        ok: false,
        remaining: normalized,
      };
    }

    if (client.isReady === false) {
      return {
        ok: false,
        remaining: normalized,
      };
    }

    for (
      let index = 0;
      index < normalized.length;
      index++
    ) {
      const sent =
        await this._sendHumanBubble(
          phone,
          normalized[index],
          client
        );

      if (!sent) {
        return {
          ok: false,
          remaining:
            normalized.slice(index),
        };
      }
    }

    return {
      ok: true,
      remaining: [],
    };
  }

  async _sendHumanBubble(
    phone,
    text,
    client
  ) {
    try {
      if (
        typeof client.sendPresenceUpdate ===
        'function'
      ) {
        try {
          await client.sendPresenceUpdate(
            'composing',
            phone
          );
        } catch (_) {
          // Presença é cosmética.
        }
      }

      const typingDelay =
        Math.min(
          text.length *
            this.typingMsPerChar,
          this.typingMax
        );

      await this.delay(
        typingDelay
      );

      await client.sendMessage(
        phone,
        text
      );

      await this.delay(300);

      return true;
    } catch (error) {
      console.error(
        `[WHATSAPP] Falha ao enviar mensagem | ` +
        `phone=${phone} | ` +
        `error=${error.message}`
      );

      return false;
    }
  }

  // ============================================================
  // FALLBACK
  // ============================================================

  async _safeSendFallback(
    phone,
    client = this._getWhatsAppService()
  ) {
    if (
      !client ||
      client.isReady === false
    ) {
      return false;
    }

    const now =
      Date.now();

    const previous =
      this._fallbackSentAt.get(
        phone
      );

    if (
      previous &&
      now - previous <
        FALLBACK_COOLDOWN_MS
    ) {
      return false;
    }

    this._fallbackSentAt.set(
      phone,
      now
    );

    // Pequena limpeza da memória local.
    if (
      this._fallbackSentAt.size >
      5000
    ) {
      for (
        const [
          storedPhone,
          timestamp,
        ]
          of this._fallbackSentAt
      ) {
        if (
          now - timestamp >
          FALLBACK_COOLDOWN_MS
        ) {
          this._fallbackSentAt.delete(
            storedPhone
          );
        }
      }
    }

    try {
      await client.sendMessage(
        phone,
        SOFT_RECOVERY_MESSAGE
      );

      return true;
    } catch (error) {
      console.error(
        `[INTERVIEW] Fallback falhou | ` +
        `phone=${phone} | ` +
        `error=${error.message}`
      );

      return false;
    }
  }

  // ============================================================
  // BOLHAS
  // ============================================================

  _extractBubbles(
    bubbles
  ) {
    if (!Array.isArray(bubbles)) {
      return [];
    }

    return bubbles
      .map(clean)
      .filter(Boolean);
  }

  splitBubbles(text) {
    const parts =
      String(text || '')
        .split(/\n{2,}/)
        .map(clean)
        .filter(Boolean);

    if (!parts.length) {
      return [];
    }

    const merged =
      this.mergeSmallBubbles(
        parts
      );

    if (merged.length <= 3) {
      return merged;
    }

    return [
      merged[0],
      merged
        .slice(1, -1)
        .join('\n\n'),
      merged[merged.length - 1],
    ];
  }

  mergeSmallBubbles(parts) {
    const result = [];

    for (const part of parts) {
      const previous =
        result[result.length - 1];

      if (
        previous &&
        (
          previous.length < 60 ||
          part.length < 40
        )
      ) {
        result[result.length - 1] =
          `${previous}\n\n${part}`;
      } else {
        result.push(part);
      }
    }

    return result;
  }

  // ============================================================
  // UTILITÁRIO
  // ============================================================

  delay(ms) {
    return new Promise(
      (resolve) => {
        setTimeout(
          resolve,
          Math.max(
            0,
            Number(ms) || 0
          )
        );
      }
    );
  }
}

module.exports = InterviewService;