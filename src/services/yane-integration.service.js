// src/services/yane-integration.service.js
//
// Cliente HTTP entre o serviço WhatsApp/Node e o backend Python do Yane.
//
// [FIXES]
// [FIX-1]  isButton propagado para o payload do turno (metadata).
// [FIX-2]  response.__yaneRequestId / __yaneCorrelationId anexados antes
//          de devolver — permite correlação em parseJson.
// [FIX-3]  suppressErrorLog permite ao caller marcar status esperados
//          (ex: 404 no lookup) sem poluir o log de erro.
// [FIX-4]  reader.cancel() garantido em qualquer caminho de excepção
//          ao ler body.
// [FIX-5]  retryAfterMs extraído também de erros de rede/timeout.
// [FIX-6]  reportUnmatchedIncoming aceita phone OU jid como
//          identificador mínimo — mensagens com LID não resolvível
//          podem ser reportadas sem número.
//
// POLÍTICA DE RETRY
// -----------------------------------------------------------------------------
// ESTE SERVIÇO NÃO FAZ RETRY.
// O InterviewService decide quando repetir uma operação.
//
// Em turnos:
//   - turn_id identifica a operação de negócio
//   - Idempotency-Key usa exactamente o mesmo turn_id
//   - cada HTTP attempt recebe um X-Request-Id diferente

'use strict';

const { randomUUID } = require('crypto');

const BaseService = require('./base.service');

// =============================================================================
// CONFIG
// =============================================================================

const DEFAULT_API_URL = 'http://localhost:8000/api';

const DEFAULT_TIMEOUTS = Object.freeze({
  default: 15_000,
  turn: 45_000,
  lookup: 10_000,
  status: 5_000,
  health: 3_000,
});

const MAX_ERROR_BODY_CHARS = 2_000;
const MAX_RESPONSE_BODY_BYTES = 2 * 1024 * 1024;

const ENDPOINTS = Object.freeze({
  interviewTurn: (interviewId) =>
    `/interviews/${encodeURIComponent(String(interviewId))}/turn`,

  interviewByPhone: (phone) =>
    `/interviews/by-phone/${encodeURIComponent(String(phone))}`,

  messageStatus: '/webhooks/message-status',

  unmatchedIncoming: '/webhooks/unmatched-incoming',

  health: '/health',
});

const RETRYABLE_HTTP_STATUSES = new Set([
  408, 425, 429,
  500, 501, 502, 503, 504, 505, 507, 509,
  520, 521, 522, 523, 524,
]);

// =============================================================================
// HELPERS
// =============================================================================

function clean(value) {
  return String(value ?? '').trim();
}

function normalizeUrl(url) {
  return clean(url).replace(/\/+$/, '');
}

function toPositiveNumber(value, fallback) {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? number : fallback;
}

function isJsonContentType(contentType) {
  return clean(contentType).toLowerCase().includes('json');
}

function truncate(value, max = MAX_ERROR_BODY_CHARS) {
  return clean(value).slice(0, max);
}

function maskPhone(phone) {
  const digits = String(phone || '').replace(/\D/g, '');

  if (digits.length < 6) {
    return '***';
  }

  return `${digits.slice(0, 3)}***${digits.slice(-3)}`;
}

function maskJid(jid) {
  const value = clean(jid);

  if (!value) {
    return '';
  }

  if (value.includes('@')) {
    const [local, suffix] = value.split('@');

    if (/\d/.test(local)) {
      return `${maskPhone(local)}@${suffix}`;
    }
  }

  return maskPhone(value);
}

function getErrorCode(error) {
  return clean(error?.code || error?.cause?.code || '').toUpperCase();
}

function isAbortError(error) {
  return error?.name === 'AbortError' || error?.name === 'TimeoutError';
}

function safeJsonStringify(value) {
  try {
    return JSON.stringify(value);
  } catch (error) {
    throw new YaneIntegrationError(
      'Não foi possível serializar o payload da API Yane.',
      {
        code: 'YANE_SERIALIZATION_ERROR',
        retryable: false,
        cause: error,
      }
    );
  }
}

function parseRetryAfter(value) {
  const raw = clean(value);

  if (!raw) {
    return null;
  }

  const seconds = Number(raw);

  if (Number.isFinite(seconds) && seconds >= 0) {
    return Math.round(seconds * 1_000);
  }

  const timestamp = Date.parse(raw);

  if (Number.isFinite(timestamp)) {
    return Math.max(0, timestamp - Date.now());
  }

  return null;
}

function safeStringifyForError(value) {
  try {
    return JSON.stringify(value);
  } catch (_) {
    return '';
  }
}

// =============================================================================
// ERRO TIPADO
// =============================================================================

class YaneIntegrationError extends Error {
  constructor(
    message,
    {
      status = null,
      code = 'YANE_HTTP_ERROR',
      retryable = false,
      endpoint = null,
      method = null,
      correlationId = null,
      requestId = null,
      retryAfterMs = null,
      cause = null,
    } = {}
  ) {
    super(message);

    this.name = 'YaneIntegrationError';

    this.status = Number.isInteger(status) ? status : null;
    this.code = clean(code) || 'YANE_HTTP_ERROR';
    this.retryable = Boolean(retryable);

    this.endpoint = endpoint || null;
    this.method = clean(method).toUpperCase() || null;

    this.correlationId = correlationId || null;
    this.requestId = requestId || null;

    this.retryAfterMs = Number.isFinite(retryAfterMs)
      ? Math.max(0, retryAfterMs)
      : null;

    if (cause) {
      this.cause = cause;
    }

    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, YaneIntegrationError);
    }
  }

  toLogObject() {
    return {
      name: this.name,
      message: this.message,
      status: this.status,
      code: this.code,
      retryable: this.retryable,
      endpoint: this.endpoint,
      method: this.method,
      correlationId: this.correlationId,
      requestId: this.requestId,
      retryAfterMs: this.retryAfterMs,
    };
  }
}

// =============================================================================
// SERVIÇO
// =============================================================================

class YaneIntegrationService extends BaseService {
  constructor(options = {}) {
    super();

    this.yaneApiUrl = normalizeUrl(
      process.env.YANE_API_URL || DEFAULT_API_URL
    );

    this.defaultTimeoutMs = toPositiveNumber(
      process.env.YANE_API_TIMEOUT,
      DEFAULT_TIMEOUTS.default
    );

    this.timeouts = Object.freeze({
      default: this.defaultTimeoutMs,
      turn: toPositiveNumber(
        process.env.YANE_TURN_TIMEOUT,
        DEFAULT_TIMEOUTS.turn
      ),
      lookup: toPositiveNumber(
        process.env.YANE_LOOKUP_TIMEOUT,
        DEFAULT_TIMEOUTS.lookup
      ),
      status: toPositiveNumber(
        process.env.YANE_STATUS_TIMEOUT,
        DEFAULT_TIMEOUTS.status
      ),
      health: toPositiveNumber(
        process.env.YANE_HEALTH_TIMEOUT,
        DEFAULT_TIMEOUTS.health
      ),
    });

    this.serviceToken =
      clean(process.env.YANE_SERVICE_TOKEN) ||
      clean(process.env.YANE_API_KEY) ||
      null;

    this.fetch = options.fetchImpl || globalThis.fetch;

    this.logger = options.logger || this._createLogger();

    if (typeof this.fetch !== 'function') {
      throw new Error(
        'YaneIntegrationService requer global fetch ou options.fetchImpl.'
      );
    }

    if (!this.serviceToken) {
      this.logWarn(
        'Nenhum token configurado. Defina YANE_SERVICE_TOKEN ou YANE_API_KEY.'
      );
    }

    this.log('Serviço de integração Yane inicializado.', {
      baseUrl: this.yaneApiUrl,
      timeouts: this.timeouts,
      authenticated: Boolean(this.serviceToken),
    });
  }

  _createLogger() {
    const pino = require('pino');

    return pino({
      level: process.env.LOG_LEVEL || 'info',
      base: { service: 'yane-integration' },
    });
  }

  // ===========================================================================
  // LOGGING
  // ===========================================================================

  _safeLogContext(context = {}) {
    const safe = {};

    for (const [key, value] of Object.entries(context)) {
      if (value === undefined || value === null) {
        continue;
      }

      if (key === 'phone' || key === 'recipient') {
        safe[key] = maskPhone(value);
        continue;
      }

      if (key === 'jid' || key === 'remoteJid' || key === 'altJid') {
        safe[key] = maskJid(value);
        continue;
      }

      // Nunca aceitar body/mensagem como contexto de log.
      if (key === 'body' || key === 'message' || key === 'preview') {
        continue;
      }

      safe[key] = value;
    }

    return safe;
  }

  _log(level, message, context = {}) {
    try {
      const loggerMethod = this.logger?.[level];

      if (typeof loggerMethod !== 'function') {
        return;
      }

      loggerMethod.call(this.logger, this._safeLogContext(context), message);
    } catch (_) {
      // Logging nunca pode derrubar a integração.
    }
  }

  log(message, context = {}) {
    this._log('info', message, context);
  }

  logWarn(message, context = {}) {
    this._log('warn', message, context);
  }

  logError(message, error = null, context = {}) {
    const safeContext = this._safeLogContext(context);

    if (error) {
      if (error instanceof YaneIntegrationError) {
        Object.assign(safeContext, error.toLogObject());
      } else {
        safeContext.error = error?.message || String(error);
        safeContext.code = error?.code || null;
      }
    }

    this._log('error', message, safeContext);
  }

  // ===========================================================================
  // URL / HEADERS
  // ===========================================================================

  buildUrl(endpoint) {
    const normalizedEndpoint = `/${clean(endpoint).replace(/^\/+/, '')}`;
    return `${this.yaneApiUrl}${normalizedEndpoint}`;
  }

  getHeaders({
    includeAuth = true,
    idempotencyKey = null,
    requestId = null,
    correlationId = null,
    extra = {},
  } = {}) {
    const headers = {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      ...extra,
    };

    if (includeAuth && this.serviceToken) {
      headers.Authorization = `Bearer ${this.serviceToken}`;
    }

    if (idempotencyKey) {
      headers['Idempotency-Key'] = String(idempotencyKey);
    }

    if (requestId) {
      headers['X-Request-Id'] = String(requestId);
    }

    if (correlationId) {
      headers['X-Correlation-Id'] = String(correlationId);
    }

    return headers;
  }

  // ===========================================================================
  // HTTP CORE
  // ===========================================================================

  async request(
    endpoint,
    {
      method = 'GET',
      body = undefined,
      includeAuth = true,
      timeoutMs = this.defaultTimeoutMs,
      idempotencyKey = null,
      correlationId = null,
      logLabel = 'YANE',
      logContext = {},
      headers = {},
      // [FIX-3] Status que o caller trata explicitamente — não
      // escrever em log.error quando aparecem.
      suppressErrorLog = [],
    } = {}
  ) {
    const normalizedMethod = clean(method).toUpperCase();
    const url = this.buildUrl(endpoint);

    const normalizedTimeout = toPositiveNumber(
      timeoutMs,
      this.defaultTimeoutMs
    );

    const operationCorrelationId = correlationId || randomUUID();
    const requestId = randomUUID();

    const controller = new AbortController();

    const timeoutHandle = setTimeout(
      () => controller.abort(),
      normalizedTimeout
    );

    if (typeof timeoutHandle.unref === 'function') {
      timeoutHandle.unref();
    }

    const options = {
      method: normalizedMethod,

      headers: this.getHeaders({
        includeAuth,
        idempotencyKey,
        requestId,
        correlationId: operationCorrelationId,
        extra: headers,
      }),

      signal: controller.signal,
    };

    if (body !== undefined) {
      try {
        options.body = safeJsonStringify(body);
      } catch (error) {
        if (error instanceof YaneIntegrationError) {
          error.endpoint = endpoint;
          error.method = normalizedMethod;
          error.correlationId = operationCorrelationId;
          error.requestId = requestId;
        }

        clearTimeout(timeoutHandle);
        throw error;
      }
    }

    // [FIX-3] normalizar suppressErrorLog.
    const suppressed = Array.isArray(suppressErrorLog)
      ? new Set(suppressErrorLog)
      : new Set();

    try {
      const response = await this.fetch(url, options);

      // [FIX-2] anexar IDs ao response para uso posterior em parseJson.
      try {
        response.__yaneRequestId = requestId;
        response.__yaneCorrelationId = operationCorrelationId;
      } catch (_) {
        // Response pode ser frozen em alguns runtimes.
      }

      if (!response.ok) {
        const httpError = await this.createHttpError(response, {
          endpoint,
          method: normalizedMethod,
          correlationId: operationCorrelationId,
          requestId,
        });

        // [FIX-3] não registar se o status está na lista de supressão.
        if (!suppressed.has(httpError.status)) {
          this.logError(`${logLabel} request falhou.`, httpError, {
            ...logContext,
            endpoint,
            method: normalizedMethod,
            requestId,
            correlationId: operationCorrelationId,
          });
        }

        throw httpError;
      }

      return response;
    } catch (error) {
      const normalized = this.normalizeRequestError(error, {
        endpoint,
        method: normalizedMethod,
        timeoutMs: normalizedTimeout,
        correlationId: operationCorrelationId,
        requestId,
      });

      // [FIX-3] se já registámos (via createHttpError acima) não
      // registamos de novo. Marcamos com um flag interno.
      const alreadyLogged = normalized.__yaneLogged === true;

      if (!alreadyLogged && !suppressed.has(normalized.status)) {
        this.logError(`${logLabel} request falhou.`, normalized, {
          ...logContext,
          endpoint,
          method: normalizedMethod,
          requestId,
          correlationId: operationCorrelationId,
        });
      }

      throw normalized;
    } finally {
      clearTimeout(timeoutHandle);
    }
  }

  normalizeRequestError(
    error,
    { endpoint, method, timeoutMs, correlationId, requestId }
  ) {
    if (error instanceof YaneIntegrationError) {
      if (!error.endpoint) error.endpoint = endpoint;
      if (!error.method) error.method = method;
      if (!error.correlationId) error.correlationId = correlationId;
      if (!error.requestId) error.requestId = requestId;

      // [FIX-3] marcar como já logado se veio do ramo HTTP.
      if (error.status !== null) {
        error.__yaneLogged = true;
      }

      return error;
    }

    if (isAbortError(error)) {
      return new YaneIntegrationError(
        `Timeout após ${timeoutMs}ms.`,
        {
          code: 'YANE_TIMEOUT',
          retryable: true,
          endpoint,
          method,
          correlationId,
          requestId,
          cause: error,
        }
      );
    }

    const code = getErrorCode(error);

    if (code === 'ETIMEDOUT' || code === 'ESOCKETTIMEDOUT') {
      return new YaneIntegrationError(
        `Timeout após ${timeoutMs}ms.`,
        {
          code: 'YANE_TIMEOUT',
          retryable: true,
          endpoint,
          method,
          correlationId,
          requestId,
          cause: error,
        }
      );
    }

    return new YaneIntegrationError(
      clean(error?.message) || 'Falha de rede ao contactar a API Yane.',
      {
        code: 'YANE_NETWORK_ERROR',
        retryable: true,
        endpoint,
        method,
        correlationId,
        requestId,
        cause: error,
      }
    );
  }

  async createHttpError(
    response,
    { endpoint, method, correlationId, requestId }
  ) {
    const status = Number(response?.status) || null;

    const contentType =
      response.headers?.get?.('content-type') || '';

    const retryAfterMs = parseRetryAfter(
      response.headers?.get?.('retry-after')
    );

    let detail = '';

    try {
      const text = await this.readResponseText(response, {
        endpoint,
        method,
        correlationId,
        requestId,
      });

      if (clean(text)) {
        if (isJsonContentType(contentType)) {
          try {
            const data = JSON.parse(text);
            detail = this.extractErrorDetail(data);
          } catch (_) {
            detail = text;
          }
        } else {
          detail = text;
        }
      }
    } catch (bodyError) {
      if (
        bodyError instanceof YaneIntegrationError &&
        bodyError.code === 'YANE_RESPONSE_TOO_LARGE'
      ) {
        return new YaneIntegrationError(
          `HTTP ${status}: resposta de erro excedeu o tamanho permitido.`,
          {
            status,
            code: `YANE_HTTP_${status}`,
            retryable: this.isRetryableStatus(status),
            endpoint,
            method,
            correlationId,
            requestId,
            retryAfterMs,
            cause: bodyError,
          }
        );
      }
    }

    const message =
      truncate(detail) ||
      clean(response.statusText) ||
      'Erro desconhecido na API Yane.';

    return new YaneIntegrationError(
      `HTTP ${status}: ${message}`,
      {
        status,
        code: `YANE_HTTP_${status}`,
        retryable: this.isRetryableStatus(status),
        endpoint,
        method,
        correlationId,
        requestId,
        retryAfterMs,
      }
    );
  }

  extractErrorDetail(data) {
    if (data === null || data === undefined) return '';

    if (typeof data === 'string') return data;

    if (typeof data?.detail === 'string') return data.detail;
    if (typeof data?.message === 'string') return data.message;
    if (typeof data?.error === 'string') return data.error;

    if (data?.detail !== undefined) {
      return safeStringifyForError(data.detail);
    }

    try {
      return JSON.stringify(data);
    } catch (_) {
      return '';
    }
  }

  isRetryableStatus(status) {
    if (!Number.isInteger(status)) return false;
    return RETRYABLE_HTTP_STATUSES.has(status) || status >= 500;
  }

  // ===========================================================================
  // RESPONSE BODY
  // ===========================================================================

  async readResponseText(
    response,
    { endpoint = null, method = null, correlationId = null, requestId = null } = {}
  ) {
    const contentLength = response.headers?.get?.('content-length');

    if (contentLength) {
      const declaredSize = Number(contentLength);

      if (
        Number.isFinite(declaredSize) &&
        declaredSize > MAX_RESPONSE_BODY_BYTES
      ) {
        throw new YaneIntegrationError(
          'Resposta da API Yane excedeu o tamanho permitido.',
          {
            status: Number.isInteger(response.status) ? response.status : null,
            code: 'YANE_RESPONSE_TOO_LARGE',
            retryable: false,
            endpoint,
            method,
            correlationId,
            requestId,
          }
        );
      }
    }

    // Alguns mocks/testes podem não expor body.
    if (!response.body) {
      try {
        const text = await response.text();

        if (
          Buffer.byteLength(text, 'utf8') > MAX_RESPONSE_BODY_BYTES
        ) {
          throw new YaneIntegrationError(
            'Resposta da API Yane excedeu o tamanho permitido.',
            {
              status: Number.isInteger(response.status)
                ? response.status
                : null,
              code: 'YANE_RESPONSE_TOO_LARGE',
              retryable: false,
              endpoint,
              method,
              correlationId,
              requestId,
            }
          );
        }

        return text;
      } catch (error) {
        if (error instanceof YaneIntegrationError) {
          throw error;
        }

        throw new YaneIntegrationError(
          'Falha ao ler resposta da API Yane.',
          {
            code: 'YANE_RESPONSE_READ_ERROR',
            retryable: false,
            endpoint,
            method,
            correlationId,
            requestId,
            cause: error,
          }
        );
      }
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    const chunks = [];
    let totalBytes = 0;

    // [FIX-4] cancelamento do reader garantido em qualquer saída.
    const cancelReader = async () => {
      try {
        await reader.cancel();
      } catch (_) {
        // Best-effort.
      }
    };

    try {
      while (true) {
        const { done, value } = await reader.read();

        if (done) break;
        if (!value) continue;

        totalBytes += value.byteLength;

        if (totalBytes > MAX_RESPONSE_BODY_BYTES) {
          await cancelReader();

          throw new YaneIntegrationError(
            'Resposta da API Yane excedeu o tamanho permitido.',
            {
              status: Number.isInteger(response.status)
                ? response.status
                : null,
              code: 'YANE_RESPONSE_TOO_LARGE',
              retryable: false,
              endpoint,
              method,
              correlationId,
              requestId,
            }
          );
        }

        chunks.push(decoder.decode(value, { stream: true }));
      }

      chunks.push(decoder.decode());
      return chunks.join('');
    } catch (error) {
      // [FIX-4] em qualquer erro, cancelar o reader antes de propagar.
      await cancelReader();

      if (error instanceof YaneIntegrationError) {
        throw error;
      }

      throw new YaneIntegrationError(
        'Falha ao ler resposta da API Yane.',
        {
          status: Number.isInteger(response.status)
            ? response.status
            : null,
          code: 'YANE_RESPONSE_READ_ERROR',
          retryable: false,
          endpoint,
          method,
          correlationId,
          requestId,
          cause: error,
        }
      );
    }
  }

  async requestJson(endpoint, options = {}) {
    const response = await this.request(endpoint, options);

    // [FIX-2] IDs vêm do response anotados em request().
    return this.parseJson(response, {
      correlationId:
        options.correlationId ||
        response?.__yaneCorrelationId ||
        null,
      endpoint,
      method: options.method || 'GET',
      requestId: response?.__yaneRequestId || null,
    });
  }

  async parseJson(
    response,
    { endpoint = null, method = null, correlationId = null, requestId = null } = {}
  ) {
    const contentType =
      response.headers?.get?.('content-type') || '';

    const text = await this.readResponseText(response, {
      endpoint,
      method,
      correlationId,
      requestId,
    });

    if (!clean(text)) {
      return {};
    }

    if (isJsonContentType(contentType)) {
      try {
        return JSON.parse(text);
      } catch (error) {
        throw new YaneIntegrationError(
          'A API Yane respondeu JSON inválido.',
          {
            status: Number.isInteger(response.status)
              ? response.status
              : null,
            code: 'YANE_INVALID_JSON',
            retryable: false,
            endpoint,
            method,
            correlationId,
            requestId,
            cause: error,
          }
        );
      }
    }

    try {
      return JSON.parse(text);
    } catch (_) {
      return { response: text };
    }
  }

  // ===========================================================================
  // COMPATIBILIDADE COM BaseService
  // ===========================================================================

  handleIntegrationError(error, context) {
    this.logError(`${context || 'Yane'} falhou.`, error);

    if (typeof this.handleError === 'function') {
      try {
        const handled = this.handleError(error, context);
        return handled || error;
      } catch (handlingError) {
        this.logWarn('handleError() do BaseService falhou.', {
          context,
          error: handlingError?.message || String(handlingError),
        });
      }
    }

    return error;
  }

  // ===========================================================================
  // ENTREVISTA
  // ===========================================================================

  /**
   * Envia um turno do candidato.
   *
   * [FIX-1] isButton é propagado como metadata no payload.
   */
  async sendInterviewTurn({
    interviewId,
    phone,
    message,
    turnId,
    messageId,
    isButton = false,
    correlationId = null,
  }) {
    const normalizedInterviewId = clean(interviewId);
    const normalizedPhone = clean(phone);
    const normalizedMessage = clean(message);
    const normalizedTurnId = clean(turnId);
    const normalizedMessageId = clean(messageId);

    if (!normalizedInterviewId) {
      throw new YaneIntegrationError('interviewId é obrigatório.', {
        code: 'INVALID_INTERVIEW_ID',
        retryable: false,
      });
    }

    if (!normalizedPhone) {
      throw new YaneIntegrationError('phone é obrigatório.', {
        code: 'INVALID_TURN_PHONE',
        retryable: false,
      });
    }

    if (!normalizedMessage) {
      throw new YaneIntegrationError('message é obrigatório.', {
        code: 'INVALID_TURN_MESSAGE',
        retryable: false,
      });
    }

    if (!normalizedTurnId) {
      throw new YaneIntegrationError(
        'turnId é obrigatório para manter idempotência.',
        {
          code: 'INVALID_TURN_ID',
          retryable: false,
        }
      );
    }

    const payload = {
      phone: normalizedPhone,
      message: normalizedMessage,
      turn_id: normalizedTurnId,
    };

    if (normalizedMessageId) {
      payload.message_id = normalizedMessageId;
    }

    // [FIX-1] sinalizar origem da mensagem.
    if (isButton) {
      payload.is_button = true;
    }

    const response = await this.request(
      ENDPOINTS.interviewTurn(normalizedInterviewId),
      {
        method: 'POST',
        body: payload,
        timeoutMs: this.timeouts.turn,
        idempotencyKey: normalizedTurnId,
        correlationId,
        logLabel: 'YANE TURN',
        logContext: {
          interviewId: normalizedInterviewId,
          phone: normalizedPhone,
          turnId: normalizedTurnId,
          messageId: normalizedMessageId || null,
        },
      }
    );

    return this.parseJson(response, {
      endpoint: ENDPOINTS.interviewTurn(normalizedInterviewId),
      method: 'POST',
      correlationId:
        correlationId ||
        response?.__yaneCorrelationId ||
        null,
      requestId: response?.__yaneRequestId || null,
    });
  }

  // ===========================================================================
  // LOOKUP POR TELEFONE
  // ===========================================================================

  async findActiveInterviewByPhone(phone, { correlationId = null } = {}) {
    const normalizedPhone = clean(phone);

    if (!normalizedPhone) {
      return null;
    }

    const endpoint = ENDPOINTS.interviewByPhone(normalizedPhone);

    try {
      const response = await this.request(endpoint, {
        method: 'GET',
        timeoutMs: this.timeouts.lookup,
        correlationId,
        logLabel: 'YANE LOOKUP',
        logContext: { phone: normalizedPhone },
        // [FIX-3] 404 é estado normal: não poluir logs.
        suppressErrorLog: [404],
      });

      return this.parseJson(response, {
        endpoint,
        method: 'GET',
        correlationId:
          correlationId ||
          response?.__yaneCorrelationId ||
          null,
        requestId: response?.__yaneRequestId || null,
      });
    } catch (error) {
      if (
        error instanceof YaneIntegrationError &&
        error.status === 404
      ) {
        return null;
      }

      throw error;
    }
  }

  // ===========================================================================
  // STATUS DA MENSAGEM
  // ===========================================================================

  async sendMessageStatus({
    phone,
    messageId,
    status = 'read',
    correlationId = null,
  }) {
    const normalizedPhone = clean(phone);
    const normalizedMessageId = clean(messageId);

    if (!normalizedPhone || !normalizedMessageId) {
      return false;
    }

    try {
      const response = await this.request(ENDPOINTS.messageStatus, {
        method: 'POST',
        body: {
          phone: normalizedPhone,
          message_id: normalizedMessageId,
          status: clean(status) || 'read',
          timestamp: new Date().toISOString(),
        },
        timeoutMs: this.timeouts.status,
        idempotencyKey: normalizedMessageId,
        correlationId,
        logLabel: 'YANE STATUS',
        logContext: {
          phone: normalizedPhone,
          messageId: normalizedMessageId,
        },
        suppressErrorLog: [404, 405],
      });

      await this.readResponseText(response, {
        endpoint: ENDPOINTS.messageStatus,
        method: 'POST',
        correlationId:
          correlationId ||
          response?.__yaneCorrelationId ||
          null,
        requestId: response?.__yaneRequestId || null,
      });

      return response.ok;
    } catch (error) {
      this.logError('Erro ao enviar status de leitura.', error, {
        phone: normalizedPhone,
        messageId: normalizedMessageId,
      });

      return false;
    }
  }

  // ===========================================================================
  // MENSAGEM ÓRFÃ
  // ===========================================================================

  /**
   * [FIX-6] Aceita phone OU jid como identificador mínimo.
   *
   * Mensagens com LID não resolvível chegam aqui sem `phone`. O
   * backend recebe o `jid` cru e decide como reconciliar (por
   * mapping, por revisão manual, etc.).
   */
  async reportUnmatchedIncoming({
    phone = null,
    jid = null,
    altJid = null,
    message,
    messageId = null,
    correlationId = null,
  }) {
    const normalizedPhone = clean(phone);
    const normalizedJid = clean(jid);
    const normalizedAltJid = clean(altJid);
    const normalizedMessage = clean(message);
    const normalizedMessageId = clean(messageId);

    // [FIX-6] aceita phone OU jid como identificador mínimo.
    if (
      (!normalizedPhone && !normalizedJid) ||
      !normalizedMessage
    ) {
      return false;
    }

    try {
      const response = await this.request(ENDPOINTS.unmatchedIncoming, {
        method: 'POST',
        body: {
          phone: normalizedPhone || null,
          jid: normalizedJid || null,
          alt_jid: normalizedAltJid || null,
          message: normalizedMessage.slice(0, 4_000),
          message_id: normalizedMessageId || null,
          received_at: new Date().toISOString(),
        },
        timeoutMs: this.timeouts.status,
        idempotencyKey: normalizedMessageId || null,
        correlationId,
        logLabel: 'YANE UNMATCHED',
        logContext: {
          phone: normalizedPhone || null,
          jid: normalizedJid || null,
          msgId: normalizedMessageId || null,
        },
        // [FIX-3] o backend pode não implementar ainda.
        suppressErrorLog: [404, 405],
      });

      await this.readResponseText(response, {
        endpoint: ENDPOINTS.unmatchedIncoming,
        method: 'POST',
        correlationId:
          correlationId ||
          response?.__yaneCorrelationId ||
          null,
        requestId: response?.__yaneRequestId || null,
      });

      return response.ok;
    } catch (error) {
      if (
        error instanceof YaneIntegrationError &&
        (error.status === 404 || error.status === 405)
      ) {
        return false;
      }

      this.logWarn('Falha ao reportar mensagem órfã.', {
        phone: normalizedPhone || null,
        jid: normalizedJid || null,
        msgId: normalizedMessageId || null,
        code: error?.code || null,
        status: error?.status || null,
      });

      return false;
    }
  }

  // ===========================================================================
  // HEALTH
  // ===========================================================================

  async healthCheck() {
    try {
      const response = await this.request(ENDPOINTS.health, {
        method: 'GET',
        includeAuth: false,
        timeoutMs: this.timeouts.health,
        logLabel: 'YANE HEALTH',
      });

      await this.readResponseText(response, {
        endpoint: ENDPOINTS.health,
        method: 'GET',
        requestId: response?.__yaneRequestId || null,
      });

      return response.ok;
    } catch (error) {
      this.logWarn('Health check do backend Yane falhou.', {
        code: error?.code || null,
        status: error?.status || null,
        retryable: error?.retryable || false,
      });

      return false;
    }
  }
}

// =============================================================================
// EXPORTS
// =============================================================================

module.exports = YaneIntegrationService;
module.exports.YaneIntegrationError = YaneIntegrationError;