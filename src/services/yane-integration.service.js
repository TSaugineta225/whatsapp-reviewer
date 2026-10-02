// src/services/yane-integration.service.js
//
// Cliente HTTP entre o serviço WhatsApp/Node e o backend Python do Yane.
//
// POLÍTICA DE RETRY
// -----------------
// ESTE SERVIÇO NÃO FAZ RETRY.
// O InterviewService decide quando repetir uma operação.
//
//   - turn_id identifica a operação de negócio.
//   - Idempotency-Key usa exactamente o mesmo turn_id.
//   - Cada tentativa HTTP recebe um X-Request-Id diferente.
//
// [FIX-TIMEOUT]
//   O timeout do `turn` sobe de 45s para 300s.
//
// [FIX-ROUTING-1]
//   Novo método resolveInboundMessage(). Delega ao backend
//   POST /interviews/resolve-inbound o routing de uma mensagem
//   inbound quando o mesmo telefone tem múltiplas entrevistas
//   activas. Substitui o uso de findActiveInterviewByPhone no
//   caminho crítico, mantendo o método antigo disponível para
//   compatibilidade.
//
// [FIX-ROUTING-2]
//   Header X-Bot-Secret enviado em todos os pedidos quando
//   YANE_SERVICE_TOKEN está definido. O backend valida-o no
//   endpoint /resolve-inbound com settings.WHATSAPP_BOT_SECRET.

'use strict';

const { randomUUID } = require('crypto');
const BaseService = require('./base.service');

// =============================================================================
// CONFIG
// =============================================================================

const DEFAULT_API_URL = 'http://localhost:8000/api';

const DEFAULT_TIMEOUTS = Object.freeze({
  default: 15_000,
  turn: 300_000,
  lookup: 10_000,
  status: 5_000,
  health: 3_000,
});

const MAX_ERROR_BODY_CHARS = 2_000;
const MAX_MESSAGE_BODY_CHARS = 4_000;
const MAX_RESPONSE_BODY_BYTES = 2 * 1024 * 1024;

const ENDPOINTS = Object.freeze({
  interviewTurn: (interviewId) =>
    `/interviews/${encodeURIComponent(String(interviewId))}/turn`,

  interviewCancel: (interviewId) =>
    `/interviews/${encodeURIComponent(String(interviewId))}/cancel`,

  interviewByPhone: (phone) =>
    `/interviews/by-phone/${encodeURIComponent(String(phone))}`,

  // [FIX-ROUTING-1] Endpoint central de routing inbound.
  resolveInbound: '/interviews/resolve-inbound',

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
  if (digits.length < 6) return '***';
  return `${digits.slice(0, 3)}***${digits.slice(-3)}`;
}

function maskJid(jid) {
  const value = clean(jid);
  if (!value) return '';

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
    const serialized = JSON.stringify(value);
    if (serialized === undefined && value !== undefined) {
      throw new TypeError('JSON.stringify() retornou undefined.');
    }
    return serialized;
  } catch (error) {
    if (error instanceof YaneIntegrationError) throw error;

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

function safeStringifyForError(value) {
  try {
    return JSON.stringify(value);
  } catch (_) {
    return '';
  }
}

function parseRetryAfter(value) {
  const raw = clean(value);
  if (!raw) return null;

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

function extractRetryAfterMs(error) {
  const direct = Number(error?.retryAfterMs);
  if (Number.isFinite(direct) && direct >= 0) {
    return Math.round(direct);
  }

  const candidates = [
    error?.headers?.get?.('retry-after'),
    error?.response?.headers?.get?.('retry-after'),
    error?.cause?.headers?.get?.('retry-after'),
    error?.cause?.response?.headers?.get?.('retry-after'),
  ];

  for (const candidate of candidates) {
    const parsed = parseRetryAfter(candidate);
    if (parsed !== null) return parsed;
  }

  return null;
}

function normalizeSuppressedStatuses(value) {
  if (!value) return new Set();
  if (value instanceof Set) return value;
  if (Array.isArray(value)) return new Set(value);
  return new Set([value]);
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

    if (cause) this.cause = cause;

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

    // [FIX-ROUTING-2] Segredo partilhado com o backend para o
    // endpoint /interviews/resolve-inbound. Se não estiver
    // definido, usa o mesmo serviceToken — o backend aceita
    // ambos no header.
    this.botSecret =
      clean(process.env.WHATSAPP_BOT_SECRET) ||
      this.serviceToken;

    this.fetch = options.fetchImpl || globalThis.fetch;
    this.logger = options.logger || this._createLogger();

    this._responseMetadata = new WeakMap();

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

    if (!this.botSecret) {
      this.logWarn(
        'Nenhum segredo do bot configurado. Defina WHATSAPP_BOT_SECRET ' +
        'ou YANE_SERVICE_TOKEN. /resolve-inbound falhará com 401.'
      );
    }

    this.log('Serviço de integração Yane inicializado.', {
      baseUrl: this.yaneApiUrl,
      timeouts: this.timeouts,
      authenticated: Boolean(this.serviceToken),
      botSecretConfigured: Boolean(this.botSecret),
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
      if (value === undefined || value === null) continue;

      if (key === 'phone' || key === 'recipient') {
        safe[key] = maskPhone(value);
        continue;
      }

      if (key === 'jid' || key === 'remoteJid' || key === 'altJid') {
        safe[key] = maskJid(value);
        continue;
      }

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
      if (typeof loggerMethod !== 'function') return;

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

    // [FIX-ROUTING-2] Segredo partilhado com o backend.
    // Enviado sempre que configurado; o backend só o valida
    // no endpoint /resolve-inbound.
    if (this.botSecret) {
      headers['X-Bot-Secret'] = String(this.botSecret);
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
  // RESPONSE METADATA
  // ===========================================================================

  _attachResponseMetadata(response, metadata) {
    if (!response || typeof response !== 'object') return;

    this._responseMetadata.set(
      response,
      Object.freeze({ ...metadata })
    );

    try {
      response.__yaneRequestId = metadata.requestId;
      response.__yaneCorrelationId = metadata.correlationId;
    } catch (_) {
      // Response pode ser frozen.
    }
  }

  _getResponseMetadata(response) {
    if (!response || typeof response !== 'object') return {};

    return this._responseMetadata.get(response) || {
      requestId: response.__yaneRequestId || null,
      correlationId: response.__yaneCorrelationId || null,
    };
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
      suppressErrorLog = [],
    } = {}
  ) {
    const normalizedMethod = clean(method).toUpperCase();
    const url = this.buildUrl(endpoint);

    const normalizedTimeout = toPositiveNumber(
      timeoutMs,
      this.defaultTimeoutMs
    );

    const operationCorrelationId = clean(correlationId) || randomUUID();
    const requestId = randomUUID();

    const suppressedStatuses = normalizeSuppressedStatuses(suppressErrorLog);

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

    try {
      if (body !== undefined) {
        options.body = safeJsonStringify(body);
      }

      const response = await this.fetch(url, options);

      this._attachResponseMetadata(response, {
        requestId,
        correlationId: operationCorrelationId,
      });

      if (!response.ok) {
        const httpError = await this.createHttpError(response, {
          endpoint,
          method: normalizedMethod,
          correlationId: operationCorrelationId,
          requestId,
        });

        if (!suppressedStatuses.has(httpError.status)) {
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
        signal: controller.signal,
      });

      const alreadyLogged = normalized.__yaneLogged === true;

      if (!alreadyLogged && !suppressedStatuses.has(normalized.status)) {
        this.logError(`${logLabel} request falhou.`, normalized, {
          ...logContext,
          endpoint,
          method: normalizedMethod,
          requestId,
          correlationId: operationCorrelationId,
        });

        normalized.__yaneLogged = true;
      }

      throw normalized;
    } finally {
      clearTimeout(timeoutHandle);
    }
  }

  normalizeRequestError(
    error,
    { endpoint, method, timeoutMs, correlationId, requestId, signal = null }
  ) {
    if (error instanceof YaneIntegrationError) {
      if (!error.endpoint) error.endpoint = endpoint;
      if (!error.method) error.method = method;
      if (!error.correlationId) error.correlationId = correlationId;
      if (!error.requestId) error.requestId = requestId;

      if (error.retryAfterMs === null) {
        error.retryAfterMs = extractRetryAfterMs(error);
      }

      if (error.status !== null) {
        error.__yaneLogged = true;
      }

      return error;
    }

    const timedOut = Boolean(signal?.aborted) || isAbortError(error);

    if (timedOut) {
      return new YaneIntegrationError(
        `Timeout após ${timeoutMs}ms.`,
        {
          code: 'YANE_TIMEOUT',
          retryable: true,
          endpoint,
          method,
          correlationId,
          requestId,
          retryAfterMs: extractRetryAfterMs(error),
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
          retryAfterMs: extractRetryAfterMs(error),
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
        retryAfterMs: extractRetryAfterMs(error),
        cause: error,
      }
    );
  }

  async createHttpError(
    response,
    { endpoint, method, correlationId, requestId }
  ) {
    const status = Number(response?.status) || null;
    const contentType = response?.headers?.get?.('content-type') || '';
    const retryAfterMs = parseRetryAfter(
      response?.headers?.get?.('retry-after')
    );

    let detail = '';
    let bodyError = null;

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
    } catch (error) {
      bodyError = error;
    }

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

    const message =
      truncate(detail) ||
      clean(response?.statusText) ||
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
        cause: bodyError,
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

    return safeStringifyForError(data);
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
    if (!response) {
      throw new YaneIntegrationError('Resposta HTTP ausente.', {
        code: 'YANE_RESPONSE_MISSING',
        retryable: false,
        endpoint,
        method,
        correlationId,
        requestId,
      });
    }

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
        if (error instanceof YaneIntegrationError) throw error;

        throw new YaneIntegrationError(
          'Falha ao ler resposta da API Yane.',
          {
            status: Number.isInteger(response.status) ? response.status : null,
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

    if (typeof response.body.getReader !== 'function') {
      throw new YaneIntegrationError(
        'Resposta HTTP não suporta leitura por stream.',
        {
          status: Number.isInteger(response.status) ? response.status : null,
          code: 'YANE_RESPONSE_STREAM_ERROR',
          retryable: false,
          endpoint,
          method,
          correlationId,
          requestId,
        }
      );
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    const chunks = [];

    let totalBytes = 0;
    let readerCancelled = false;

    const cancelReader = async () => {
      if (readerCancelled) return;
      readerCancelled = true;

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
      await cancelReader();

      if (error instanceof YaneIntegrationError) throw error;

      throw new YaneIntegrationError(
        'Falha ao ler resposta da API Yane.',
        {
          status: Number.isInteger(response.status) ? response.status : null,
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
    const metadata = this._getResponseMetadata(response);

    return this.parseJson(response, {
      correlationId:
        options.correlationId || metadata.correlationId || null,
      endpoint,
      method: options.method || 'GET',
      requestId: metadata.requestId || null,
    });
  }

  async parseJson(
    response,
    { endpoint = null, method = null, correlationId = null, requestId = null } = {}
  ) {
    const metadata = this._getResponseMetadata(response);
    const resolvedCorrelationId = correlationId || metadata.correlationId || null;
    const resolvedRequestId = requestId || metadata.requestId || null;

    const contentType = response?.headers?.get?.('content-type') || '';

    const text = await this.readResponseText(response, {
      endpoint,
      method,
      correlationId: resolvedCorrelationId,
      requestId: resolvedRequestId,
    });

    if (!clean(text)) return {};

    if (isJsonContentType(contentType)) {
      try {
        return JSON.parse(text);
      } catch (error) {
        throw new YaneIntegrationError(
          'A API Yane respondeu JSON inválido.',
          {
            status: Number.isInteger(response?.status)
              ? response.status
              : null,
            code: 'YANE_INVALID_JSON',
            retryable: false,
            endpoint,
            method,
            correlationId: resolvedCorrelationId,
            requestId: resolvedRequestId,
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
        { code: 'INVALID_TURN_ID', retryable: false }
      );
    }

    const endpoint = ENDPOINTS.interviewTurn(normalizedInterviewId);

    const payload = {
      phone: normalizedPhone,
      message: normalizedMessage,
      turn_id: normalizedTurnId,
      is_button: Boolean(isButton),
    };

    if (normalizedMessageId) {
      payload.message_id = normalizedMessageId;
    }

    const response = await this.request(endpoint, {
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
    });

    const metadata = this._getResponseMetadata(response);

    return this.parseJson(response, {
      endpoint,
      method: 'POST',
      correlationId:
        metadata.correlationId || correlationId || null,
      requestId: metadata.requestId || null,
    });
  }

  async cancelInterview(
    interviewId,
    reason = 'candidate_reset',
    { correlationId = null } = {}
  ) {
    const normalizedInterviewId = clean(interviewId);

    if (!normalizedInterviewId) {
      throw new YaneIntegrationError('interviewId é obrigatório.', {
        code: 'INVALID_INTERVIEW_ID',
        retryable: false,
      });
    }

    const normalizedReason = clean(reason) || 'candidate_reset';
    const endpoint = ENDPOINTS.interviewCancel(normalizedInterviewId);

    const response = await this.request(endpoint, {
      method: 'POST',
      body: { reason: normalizedReason },
      timeoutMs: this.timeouts.default,
      idempotencyKey: `cancel:${normalizedInterviewId}`,
      correlationId,
      logLabel: 'YANE CANCEL',
      logContext: {
        interviewId: normalizedInterviewId,
        reason: normalizedReason,
      },
      suppressErrorLog: [404, 405],
    });

    const metadata = this._getResponseMetadata(response);

    return this.parseJson(response, {
      endpoint,
      method: 'POST',
      correlationId:
        metadata.correlationId || correlationId || null,
      requestId: metadata.requestId || null,
    });
  }

  // ===========================================================================
  // [FIX-ROUTING-1] RESOLVE INBOUND
  // ===========================================================================

  /**
   * Determina a que entrevista pertence uma mensagem inbound.
   *
   * Este é o método central do routing multi-entrevista. Quando o
   * mesmo telefone tem várias entrevistas activas, o backend decide
   * qual delas é a correcta com base em:
   *   1. replied_to_message_id (reply-to)
   *   2. única activa
   *   3. match de conteúdo
   *   4. sessão recente
   *   5. ambíguo → devolve candidatos
   *
   * Contrato de resposta:
   *   {
   *     interview_id: string | null,
   *     confidence: 'high' | 'medium' | 'ambiguous' | 'none' | 'invalid_phone',
   *     method: string | null,
   *     candidates: [{ index, interview_id, job_title, company_name, score }]
   *   }
   *
   * @param {object} params
   * @param {string} params.phone
   * @param {string} params.message
   * @param {string|null} [params.repliedToMessageId]
   * @param {string|null} [params.correlationId]
   * @returns {Promise<object>}
   */
  async resolveInboundMessage({
    phone,
    message,
    repliedToMessageId = null,
    correlationId = null,
  }) {
    const normalizedPhone = clean(phone);
    const normalizedMessage = clean(message);
    const normalizedReply = clean(repliedToMessageId);

    if (!normalizedPhone) {
      throw new YaneIntegrationError('phone é obrigatório.', {
        code: 'INVALID_RESOLVE_PHONE',
        retryable: false,
      });
    }

    if (!normalizedMessage) {
      throw new YaneIntegrationError('message é obrigatório.', {
        code: 'INVALID_RESOLVE_MESSAGE',
        retryable: false,
      });
    }

    const endpoint = ENDPOINTS.resolveInbound;

    const payload = {
      phone: normalizedPhone,
      message: normalizedMessage,
    };

    if (normalizedReply) {
      payload.replied_to_message_id = normalizedReply;
    }

    const response = await this.request(endpoint, {
      method: 'POST',
      body: payload,
      timeoutMs: this.timeouts.lookup,
      correlationId,
      logLabel: 'YANE RESOLVE',
      logContext: {
        phone: normalizedPhone,
        hasReply: Boolean(normalizedReply),
      },
      suppressErrorLog: [404],
    });

    const metadata = this._getResponseMetadata(response);

    const parsed = await this.parseJson(response, {
      endpoint,
      method: 'POST',
      correlationId:
        metadata.correlationId || correlationId || null,
      requestId: metadata.requestId || null,
    });

    // Normalização defensiva: garante que o caller encontra
    // sempre as chaves esperadas, mesmo se o backend devolver
    // um payload ligeiramente diferente.
    return {
      interview_id:
        parsed?.interview_id || null,
      confidence:
        clean(parsed?.confidence) || 'none',
      method:
        clean(parsed?.method) || null,
      candidates:
        Array.isArray(parsed?.candidates)
          ? parsed.candidates
          : [],
      scores:
        parsed?.scores && typeof parsed.scores === 'object'
          ? parsed.scores
          : null,
    };
  }

  // ===========================================================================
  // LOOKUP POR TELEFONE (legacy)
  // ===========================================================================

  /**
   * Devolve a entrevista activa mais recente para um telefone.
   *
   * @deprecated desde v9.3. Mantido apenas para compatibilidade.
   * Prefira `resolveInboundMessage()` — este método perde
   * entrevistas quando o mesmo telefone tem múltiplas activas.
   */
  async findActiveInterviewByPhone(
    phone,
    { correlationId = null } = {}
  ) {
    const normalizedPhone = clean(phone);
    if (!normalizedPhone) return null;

    const endpoint = ENDPOINTS.interviewByPhone(normalizedPhone);

    try {
      const response = await this.request(endpoint, {
        method: 'GET',
        timeoutMs: this.timeouts.lookup,
        correlationId,
        logLabel: 'YANE LOOKUP',
        logContext: { phone: normalizedPhone },
        suppressErrorLog: [404],
      });

      const metadata = this._getResponseMetadata(response);

      return this.parseJson(response, {
        endpoint,
        method: 'GET',
        correlationId:
          metadata.correlationId || correlationId || null,
        requestId: metadata.requestId || null,
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
    const normalizedStatus = clean(status) || 'read';

    if (!normalizedPhone || !normalizedMessageId) {
      return false;
    }

    const endpoint = ENDPOINTS.messageStatus;

    try {
      const response = await this.request(endpoint, {
        method: 'POST',
        body: {
          phone: normalizedPhone,
          message_id: normalizedMessageId,
          status: normalizedStatus,
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

      const metadata = this._getResponseMetadata(response);

      await this.readResponseText(response, {
        endpoint,
        method: 'POST',
        correlationId:
          metadata.correlationId || correlationId || null,
        requestId: metadata.requestId || null,
      });

      return response.ok;
    } catch (error) {
      this.logWarn('Status de mensagem não enviado.', {
        phone: normalizedPhone,
        messageId: normalizedMessageId,
        code: error?.code || null,
        status: error?.status || null,
      });

      return false;
    }
  }

  // ===========================================================================
  // MENSAGEM ÓRFÃ
  // ===========================================================================

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

    if (
      (!normalizedPhone && !normalizedJid) ||
      !normalizedMessage
    ) {
      return false;
    }

    const endpoint = ENDPOINTS.unmatchedIncoming;

    try {
      const response = await this.request(endpoint, {
        method: 'POST',
        body: {
          phone: normalizedPhone || null,
          jid: normalizedJid || null,
          alt_jid: normalizedAltJid || null,
          message: normalizedMessage.slice(0, MAX_MESSAGE_BODY_CHARS),
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
        suppressErrorLog: [404, 405],
      });

      const metadata = this._getResponseMetadata(response);

      await this.readResponseText(response, {
        endpoint,
        method: 'POST',
        correlationId:
          metadata.correlationId || correlationId || null,
        requestId: metadata.requestId || null,
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
    const endpoint = ENDPOINTS.health;

    try {
      const response = await this.request(endpoint, {
        method: 'GET',
        includeAuth: false,
        timeoutMs: this.timeouts.health,
        logLabel: 'YANE HEALTH',
      });

      const metadata = this._getResponseMetadata(response);

      await this.readResponseText(response, {
        endpoint,
        method: 'GET',
        requestId: metadata.requestId || null,
        correlationId: metadata.correlationId || null,
      });

      return response.ok;
    } catch (error) {
      this.logWarn('Health check do backend Yane falhou.', {
        code: error?.code || null,
        status: error?.status || null,
        retryable: error?.retryable || false,
        retryAfterMs: error?.retryAfterMs ?? null,
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