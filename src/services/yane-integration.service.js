// src/services/yane-integration.service.js
//
// Cliente HTTP entre o Node e o backend Python do Yane.
//
// FILOSOFIA
//   - Sem retry interno. O InterviewService decide quando repetir.
//   - Erros normalizados (status, code, retryable, retryAfterMs).
//   - Idempotency-Key = turn_id em todos os turnos.
//   - Endpoints inexistentes no backend degradam graciosamente.
//
// [FALLBACK-RESOLVE]
//   resolveInboundMessage tenta POST /interviews/resolve-inbound.
//   Se o endpoint não existir (404/405/501) ou rebentar (500), cai
//   automaticamente para GET /interviews/by-phone/{phone} e devolve
//   a mesma forma de resposta. O bot continua operacional.
//
// [FALLBACK-CANCEL]
//   cancelInterview devolve { success: true, skipped: true } quando
//   o endpoint não existe, em vez de lançar. O !reset do candidato
//   deixa de partir.

'use strict';

const { randomUUID } = require('crypto');
const pino = require('pino');
const BaseService = require('./base.service');

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
const MAX_REQUEST_BODY_BYTES = 512 * 1024;

const BACKEND_CAPABILITY_CACHE_TTL_MS = 10 * 60_000;
const RESOLVE_FALLBACK_COOLDOWN_MS = 30_000;

const REQUEST_ID_HEADER = 'X-Request-Id';
const CORRELATION_ID_HEADER = 'X-Correlation-Id';
const CLIENT_HEADER = 'X-Yane-Client';
const CLIENT_NAME = 'yane-node-integration';

const ENDPOINTS = Object.freeze({
  interviewTurn: (id) =>
    `/interviews/${encodeURIComponent(String(id))}/turn`,

  interviewCancel: (id) =>
    `/interviews/${encodeURIComponent(String(id))}/cancel`,

  interviewByPhone: (p) =>
    `/interviews/by-phone/${encodeURIComponent(String(p))}`,

  resolveInbound: '/interviews/resolve-inbound',
  messageStatus: '/webhooks/message-status',
  unmatchedIncoming: '/webhooks/unmatched-incoming',
  health: '/health',
});

const RETRYABLE_STATUSES = new Set([
  408,
  425,
  429,
  500,
  501,
  502,
  503,
  504,
  505,
  507,
  509,
  520,
  521,
  522,
  523,
  524,
]);

const MISSING_ENDPOINT_STATUSES = new Set([
  404,
  405,
  501,
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
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : fallback;
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

function isAbortError(error) {
  return (
    error?.name === 'AbortError' ||
    error?.name === 'TimeoutError'
  );
}

function parseRetryAfter(value) {
  const raw = clean(value);

  if (!raw) return null;

  const seconds = Number(raw);

  if (
    Number.isFinite(seconds) &&
    seconds >= 0
  ) {
    return Math.round(seconds * 1000);
  }

  const timestamp = Date.parse(raw);

  if (Number.isFinite(timestamp)) {
    return Math.max(0, timestamp - Date.now());
  }

  return null;
}

function extractRetryAfterMs(error) {
  const direct = Number(error?.retryAfterMs);

  if (
    Number.isFinite(direct) &&
    direct >= 0
  ) {
    return Math.round(direct);
  }

  const candidates = [
    error?.headers?.get?.('retry-after'),
    error?.response?.headers?.get?.('retry-after'),
    error?.cause?.headers?.get?.('retry-after'),
  ];

  for (const candidate of candidates) {
    const parsed = parseRetryAfter(candidate);

    if (parsed !== null) {
      return parsed;
    }
  }

  return null;
}

function safeJsonStringify(
  value,
  maxBytes = MAX_REQUEST_BODY_BYTES
) {
  try {
    const serialized = JSON.stringify(value);

    if (
      serialized === undefined &&
      value !== undefined
    ) {
      throw new TypeError(
        'JSON.stringify() devolveu undefined.'
      );
    }

    if (
      typeof serialized === 'string' &&
      Buffer.byteLength(serialized, 'utf8') > maxBytes
    ) {
      throw new YaneIntegrationError(
        'Payload excedeu o tamanho permitido.',
        {
          code: 'YANE_REQUEST_TOO_LARGE',
          retryable: false,
        }
      );
    }

    return serialized;
  } catch (error) {
    if (error instanceof YaneIntegrationError) {
      throw error;
    }

    throw new YaneIntegrationError(
      'Não foi possível serializar o payload.',
      {
        code: 'YANE_SERIALIZATION_ERROR',
        retryable: false,
        cause: error,
      }
    );
  }
}

function safeStringify(value) {
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
  constructor(message, {
    status = null,
    code = 'YANE_HTTP_ERROR',
    retryable = false,
    endpoint = null,
    method = null,
    correlationId = null,
    requestId = null,
    retryAfterMs = null,
    cause = null,
  } = {}) {
    super(message);

    this.name = 'YaneIntegrationError';

    this.status = Number.isInteger(status)
      ? status
      : null;

    this.code =
      clean(code) || 'YANE_HTTP_ERROR';

    this.retryable =
      Boolean(retryable);

    this.endpoint =
      endpoint || null;

    this.method =
      clean(method).toUpperCase() || null;

    this.correlationId =
      correlationId || null;

    this.requestId =
      requestId || null;

    this.retryAfterMs =
      Number.isFinite(retryAfterMs)
        ? Math.max(0, retryAfterMs)
        : null;

    if (cause) {
      this.cause = cause;
    }

    if (Error.captureStackTrace) {
      Error.captureStackTrace(
        this,
        YaneIntegrationError
      );
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
      process.env.YANE_API_URL ||
      DEFAULT_API_URL
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

    this.botSecret =
      clean(process.env.WHATSAPP_BOT_SECRET) ||
      this.serviceToken;

    this.fetch =
      options.fetchImpl ||
      globalThis.fetch;

    this.logger =
      options.logger ||
      pino({
        level:
          process.env.LOG_LEVEL || 'info',
        base: {
          service: 'yane-integration',
        },
      });

    this._responseMetadata = new WeakMap();

    // Cache curto de capacidades evita repetir chamadas
    // 404/405/501 em cada mensagem quando uma versão
    // do backend ainda não possui determinado endpoint.
    this._capabilityCache = new Map();

    // Impede que um backend temporariamente instável
    // provoque um storm de POST /resolve-inbound em cada
    // mensagem recebida.
    this._resolveFallbackUntil = 0;

    if (typeof this.fetch !== 'function') {
      throw new Error(
        'YaneIntegrationService requer global fetch ou options.fetchImpl.'
      );
    }

    if (!this.serviceToken) {
      this.logWarn(
        'Nenhum token configurado. Defina YANE_SERVICE_TOKEN.'
      );
    }

    this.log(
      'YaneIntegrationService inicializado.',
      {
        baseUrl: this.yaneApiUrl,
        authenticated:
          Boolean(this.serviceToken),
        botSecretConfigured:
          Boolean(this.botSecret),
      }
    );
  }

  // ===========================================================================
  // LOGGING
  // ===========================================================================

  _safeLogContext(context = {}) {
    const safe = {};

    for (const [key, value] of Object.entries(context)) {
      if (
        value === undefined ||
        value === null
      ) {
        continue;
      }

      if (
        key === 'phone' ||
        key === 'recipient'
      ) {
        safe[key] = maskPhone(value);
        continue;
      }

      if (
        key === 'jid' ||
        key === 'remoteJid' ||
        key === 'altJid' ||
        key === 'lid'
      ) {
        safe[key] = maskJid(value);
        continue;
      }

      if (
        key === 'body' ||
        key === 'message' ||
        key === 'preview' ||
        key === 'text'
      ) {
        continue;
      }

      safe[key] = value;
    }

    return safe;
  }

  _log(
    level,
    message,
    context = {}
  ) {
    try {
      const method =
        this.logger?.[level];

      if (
        typeof method !== 'function'
      ) {
        return;
      }

      method.call(
        this.logger,
        this._safeLogContext(context),
        message
      );
    } catch (_) {
      // Logging nunca deve derrubar o serviço.
    }
  }

  log(message, context) {
    this._log(
      'info',
      message,
      context
    );
  }

  logWarn(message, context) {
    this._log(
      'warn',
      message,
      context
    );
  }

  logError(
    message,
    error = null,
    context = {}
  ) {
    const safe =
      this._safeLogContext(context);

    if (error) {
      if (
        error instanceof
        YaneIntegrationError
      ) {
        Object.assign(
          safe,
          error.toLogObject()
        );
      } else {
        safe.error =
          error?.message ||
          String(error);

        safe.code =
          error?.code ||
          null;
      }
    }

    this._log(
      'error',
      message,
      safe
    );
  }

  // ===========================================================================
  // URL / HEADERS
  // ===========================================================================

  buildUrl(endpoint) {
    const normalizedEndpoint =
      `/${clean(endpoint).replace(/^\/+/, '')}`;

    return `${this.yaneApiUrl}${normalizedEndpoint}`;
  }

  getHeaders({
    includeAuth = true,
    idempotencyKey = null,
    requestId = null,
    correlationId = null,
    hasBody = false,
    extra = {},
  } = {}) {
    const headers = {
      Accept: 'application/json',
      [CLIENT_HEADER]: CLIENT_NAME,
      ...extra,
    };

    if (
      hasBody &&
      !headers['Content-Type'] &&
      !headers['content-type']
    ) {
      headers['Content-Type'] =
        'application/json';
    }

    if (
      includeAuth &&
      this.serviceToken
    ) {
      headers.Authorization =
        `Bearer ${this.serviceToken}`;
    }

    if (this.botSecret) {
      headers['X-Bot-Secret'] =
        String(this.botSecret);
    }

    if (idempotencyKey) {
      headers['Idempotency-Key'] =
        String(idempotencyKey);
    }

    if (requestId) {
      headers[REQUEST_ID_HEADER] =
        String(requestId);
    }

    if (correlationId) {
      headers[CORRELATION_ID_HEADER] =
        String(correlationId);
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
      body,
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
    const normalizedMethod =
      clean(method).toUpperCase();

    const url =
      this.buildUrl(endpoint);

    const normalizedTimeout =
      toPositiveNumber(
        timeoutMs,
        this.defaultTimeoutMs
      );

    const operationCorrelationId =
      clean(correlationId) ||
      randomUUID();

    const requestId =
      randomUUID();

    const suppressed = new Set(
      Array.isArray(suppressErrorLog)
        ? suppressErrorLog
        : [suppressErrorLog]
    );

    const controller =
      new AbortController();

    const timer = setTimeout(
      () => controller.abort(),
      normalizedTimeout
    );

    if (
      typeof timer.unref === 'function'
    ) {
      timer.unref();
    }

    const options = {
      method: normalizedMethod,

      headers: this.getHeaders({
        includeAuth,
        idempotencyKey,
        requestId,
        correlationId:
          operationCorrelationId,
        hasBody:
          body !== undefined,
        extra: headers,
      }),

      signal: controller.signal,
    };

    try {
      if (body !== undefined) {
        options.body =
          safeJsonStringify(body);
      }

      const response =
        await this.fetch(
          url,
          options
        );

      this._responseMetadata.set(
        response,
        Object.freeze({
          requestId,
          correlationId:
            operationCorrelationId,
        })
      );

      if (!response.ok) {
        const httpError =
          await this._createHttpError(
            response,
            {
              endpoint,
              method:
                normalizedMethod,
              correlationId:
                operationCorrelationId,
              requestId,
            }
          );

        if (
          !suppressed.has(
            httpError.status
          )
        ) {
          this.logError(
            `${logLabel} request falhou.`,
            httpError,
            {
              ...logContext,
              endpoint,
              method:
                normalizedMethod,
              requestId,
              correlationId:
                operationCorrelationId,
            }
          );
        }

        throw httpError;
      }

      return response;
    } catch (error) {
      const normalized =
        this._normalizeRequestError(
          error,
          {
            endpoint,
            method:
              normalizedMethod,
            timeoutMs:
              normalizedTimeout,
            correlationId:
              operationCorrelationId,
            requestId,
            signal:
              controller.signal,
          }
        );

      const alreadyLogged =
        normalized.__yaneLogged === true;

      if (
        !alreadyLogged &&
        !suppressed.has(
          normalized.status
        )
      ) {
        this.logError(
          `${logLabel} request falhou.`,
          normalized,
          {
            ...logContext,
            endpoint,
            method:
              normalizedMethod,
            requestId,
            correlationId:
              operationCorrelationId,
          }
        );

        normalized.__yaneLogged =
          true;
      }

      throw normalized;
    } finally {
      clearTimeout(timer);
    }
  }

  _markCapabilityUnavailable(
    name,
    ttlMs =
      BACKEND_CAPABILITY_CACHE_TTL_MS
  ) {
    const key = clean(name);

    if (!key) return;

    this._capabilityCache.set(
      key,
      Date.now() +
        Math.max(0, ttlMs)
    );
  }

  _isCapabilityUnavailable(name) {
    const key = clean(name);

    if (!key) return false;

    const expiresAt =
      this._capabilityCache.get(key);

    if (!expiresAt) return false;

    if (
      expiresAt <= Date.now()
    ) {
      this._capabilityCache.delete(
        key
      );

      return false;
    }

    return true;
  }

  _clearCapabilityCache(name) {
    const key = clean(name);

    if (!key) return;

    this._capabilityCache.delete(
      key
    );
  }

  _normalizeRequestError(
    error,
    {
      endpoint,
      method,
      timeoutMs,
      correlationId,
      requestId,
      signal,
    }
  ) {
    if (
      error instanceof
      YaneIntegrationError
    ) {
      if (!error.endpoint) {
        error.endpoint =
          endpoint;
      }

      if (!error.method) {
        error.method =
          method;
      }

      if (!error.correlationId) {
        error.correlationId =
          correlationId;
      }

      if (!error.requestId) {
        error.requestId =
          requestId;
      }

      if (
        error.retryAfterMs === null
      ) {
        error.retryAfterMs =
          extractRetryAfterMs(
            error
          );
      }

      // HTTP errors levantados por
      // _createHttpError já foram
      // tratados/logados no caller.
      if (
        error.status !== null
      ) {
        error.__yaneLogged =
          true;
      }

      return error;
    }

    const timedOut =
      Boolean(signal?.aborted) ||
      isAbortError(error);

    if (timedOut) {
      return new YaneIntegrationError(
        `Timeout após ${timeoutMs}ms.`,
        {
          code:
            'YANE_TIMEOUT',
          retryable: true,
          endpoint,
          method,
          correlationId,
          requestId,
          cause: error,
        }
      );
    }

    const code =
      clean(
        error?.code ||
        error?.cause?.code
      ).toUpperCase();

    if (
      code === 'ETIMEDOUT' ||
      code === 'ESOCKETTIMEDOUT'
    ) {
      return new YaneIntegrationError(
        `Timeout após ${timeoutMs}ms.`,
        {
          code:
            'YANE_TIMEOUT',
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
      clean(error?.message) ||
        'Falha de rede ao contactar a API Yane.',
      {
        code:
          'YANE_NETWORK_ERROR',
        retryable: true,
        endpoint,
        method,
        correlationId,
        requestId,
        retryAfterMs:
          extractRetryAfterMs(
            error
          ),
        cause: error,
      }
    );
  }

  async _createHttpError(
    response,
    {
      endpoint,
      method,
      correlationId,
      requestId,
    }
  ) {
    const status =
      Number(response?.status) ||
      null;

    const contentType =
      response?.headers?.get?.(
        'content-type'
      ) || '';

    const retryAfterMs =
      parseRetryAfter(
        response?.headers?.get?.(
          'retry-after'
        )
      );

    let detail = '';
    let bodyError = null;

    try {
      const text =
        await this._readResponseText(
          response,
          {
            endpoint,
            method,
            correlationId,
            requestId,
          }
        );

      if (clean(text)) {
        if (
          clean(contentType)
            .toLowerCase()
            .includes('json')
        ) {
          try {
            const data =
              JSON.parse(text);

            detail =
              this._extractErrorDetail(
                data
              );
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

    const message =
      truncate(detail) ||
      clean(
        response?.statusText
      ) ||
      'Erro desconhecido.';

    return new YaneIntegrationError(
      `HTTP ${status}: ${message}`,
      {
        status,
        code:
          `YANE_HTTP_${status}`,
        retryable:
          this._isRetryableStatus(
            status
          ),
        endpoint,
        method,
        correlationId,
        requestId,
        retryAfterMs,
        cause: bodyError,
      }
    );
  }

  _extractErrorDetail(data) {
    if (data == null) {
      return '';
    }

    if (typeof data === 'string') {
      return data;
    }

    if (
      typeof data?.detail ===
      'string'
    ) {
      return data.detail;
    }

    if (
      typeof data?.message ===
      'string'
    ) {
      return data.message;
    }

    if (
      typeof data?.error ===
      'string'
    ) {
      return data.error;
    }

    if (
      data?.detail !== undefined
    ) {
      return safeStringify(
        data.detail
      );
    }

    return safeStringify(data);
  }

  _isRetryableStatus(status) {
    if (!Number.isInteger(status)) {
      return false;
    }

    return (
      RETRYABLE_STATUSES.has(
        status
      ) ||
      status >= 500
    );
  }

  async _readResponseText(
    response,
    {
      endpoint,
      method,
      correlationId,
      requestId,
    } = {}
  ) {
    if (!response) {
      throw new YaneIntegrationError(
        'Resposta HTTP ausente.',
        {
          code:
            'YANE_RESPONSE_MISSING',
          retryable: false,
          endpoint,
          method,
          correlationId,
          requestId,
        }
      );
    }

    const contentLength =
      response.headers?.get?.(
        'content-length'
      );

    if (contentLength) {
      const declared =
        Number(contentLength);

      if (
        Number.isFinite(declared) &&
        declared >
          MAX_RESPONSE_BODY_BYTES
      ) {
        throw new YaneIntegrationError(
          'Resposta excedeu o tamanho permitido.',
          {
            status:
              Number.isInteger(
                response.status
              )
                ? response.status
                : null,

            code:
              'YANE_RESPONSE_TOO_LARGE',

            retryable: false,
            endpoint,
            method,
            correlationId,
            requestId,
          }
        );
      }
    }

    try {
      const text =
        await response.text();

      if (
        Buffer.byteLength(
          text,
          'utf8'
        ) >
        MAX_RESPONSE_BODY_BYTES
      ) {
        throw new YaneIntegrationError(
          'Resposta excedeu o tamanho permitido.',
          {
            status:
              Number.isInteger(
                response.status
              )
                ? response.status
                : null,

            code:
              'YANE_RESPONSE_TOO_LARGE',

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
      if (
        error instanceof
        YaneIntegrationError
      ) {
        throw error;
      }

      throw new YaneIntegrationError(
        'Falha ao ler resposta.',
        {
          status:
            Number.isInteger(
              response.status
            )
              ? response.status
              : null,

          code:
            'YANE_RESPONSE_READ_ERROR',

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

  async _parseJson(
    response,
    {
      endpoint,
      method,
      correlationId,
      requestId,
    } = {}
  ) {
    const metadata =
      this._responseMetadata.get(
        response
      ) || {};

    const resolvedCorrelationId =
      correlationId ||
      metadata.correlationId ||
      null;

    const resolvedRequestId =
      requestId ||
      metadata.requestId ||
      null;

    const contentType =
      response?.headers?.get?.(
        'content-type'
      ) || '';

    const text =
      await this._readResponseText(
        response,
        {
          endpoint,
          method,
          correlationId:
            resolvedCorrelationId,
          requestId:
            resolvedRequestId,
        }
      );

    if (!clean(text)) {
      return {};
    }

    if (
      clean(contentType)
        .toLowerCase()
        .includes('json')
    ) {
      try {
        return JSON.parse(text);
      } catch (error) {
        throw new YaneIntegrationError(
          'JSON inválido.',
          {
            status:
              Number.isInteger(
                response?.status
              )
                ? response.status
                : null,

            code:
              'YANE_INVALID_JSON',

            retryable: false,

            endpoint,
            method,

            correlationId:
              resolvedCorrelationId,

            requestId:
              resolvedRequestId,

            cause: error,
          }
        );
      }
    }

    try {
      return JSON.parse(text);
    } catch (_) {
      return {
        response: text,
      };
    }
  }

  handleIntegrationError(
    error,
    context
  ) {
    this.logError(
      `${context || 'Yane'} falhou.`,
      error
    );

    return error;
  }

  // ===========================================================================
  // ENTREVISTA — TURNO
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
    const iid =
      clean(interviewId);

    const p =
      clean(phone);

    const m =
      truncate(
        message,
        MAX_MESSAGE_BODY_CHARS
      );

    const t =
      clean(turnId);

    const mid =
      clean(messageId);

    if (!iid) {
      throw new YaneIntegrationError(
        'interviewId é obrigatório.',
        {
          code:
            'INVALID_INTERVIEW_ID',
          retryable: false,
        }
      );
    }

    if (!p) {
      throw new YaneIntegrationError(
        'phone é obrigatório.',
        {
          code:
            'INVALID_TURN_PHONE',
          retryable: false,
        }
      );
    }

    if (!m) {
      throw new YaneIntegrationError(
        'message é obrigatório.',
        {
          code:
            'INVALID_TURN_MESSAGE',
          retryable: false,
        }
      );
    }

    if (!t) {
      throw new YaneIntegrationError(
        'turnId é obrigatório.',
        {
          code:
            'INVALID_TURN_ID',
          retryable: false,
        }
      );
    }

    const endpoint =
      ENDPOINTS.interviewTurn(
        iid
      );

    const payload = {
      phone: p,
      message: m,
      turn_id: t,
      is_button:
        Boolean(isButton),
    };

    if (mid) {
      payload.message_id =
        mid;
    }

    const response =
      await this.request(
        endpoint,
        {
          method: 'POST',

          body: payload,

          timeoutMs:
            this.timeouts.turn,

          idempotencyKey: t,

          correlationId,

          logLabel:
            'YANE TURN',

          logContext: {
            interviewId: iid,
            phone: p,
            turnId: t,
            messageId:
              mid || null,
          },
        }
      );

    return this._parseJson(
      response,
      {
        endpoint,
        method: 'POST',
        correlationId,
      }
    );
  }

  // ===========================================================================
  // ENTREVISTA — LOOKUP
  // ===========================================================================

  async findActiveInterviewByPhone(
    phone,
    {
      correlationId = null,
    } = {}
  ) {
    const p = clean(phone);

    if (!p) {
      return null;
    }

    const endpoint =
      ENDPOINTS.interviewByPhone(
        p
      );

    try {
      const response =
        await this.request(
          endpoint,
          {
            method: 'GET',

            timeoutMs:
              this.timeouts.lookup,

            correlationId,

            logLabel:
              'YANE LOOKUP',

            logContext: {
              phone: p,
            },

            suppressErrorLog: [
              404,
            ],
          }
        );

      return this._parseJson(
        response,
        {
          endpoint,
          method: 'GET',
          correlationId,
        }
      );
    } catch (error) {
      if (
        error instanceof
          YaneIntegrationError &&
        error.status === 404
      ) {
        return null;
      }

      throw error;
    }
  }

  // ===========================================================================
  // RESOLVE INBOUND — com fallback
  // ===========================================================================

  async resolveInboundMessage({
    phone,
    message,
    repliedToMessageId = null,
    correlationId = null,
  }) {
    const p =
      clean(phone);

    const m =
      clean(message);

    const reply =
      clean(repliedToMessageId);

    if (!p) {
      throw new YaneIntegrationError(
        'phone é obrigatório.',
        {
          code:
            'INVALID_RESOLVE_PHONE',
          retryable: false,
        }
      );
    }

    if (!m) {
      throw new YaneIntegrationError(
        'message é obrigatório.',
        {
          code:
            'INVALID_RESOLVE_MESSAGE',
          retryable: false,
        }
      );
    }

    const endpoint =
      ENDPOINTS.resolveInbound;

    const payload = {
      phone: p,
      message: m,
    };

    // Quando sabemos que este endpoint esteve
    // indisponível recentemente, evitamos uma ida
    // extra ao backend em cada mensagem.
    if (
      this._isCapabilityUnavailable(
        'resolveInbound'
      ) &&
      this._resolveFallbackUntil >
        Date.now()
    ) {
      return this._resolveByPhoneFallback(
        p,
        {
          correlationId,
        }
      );
    }

    if (reply) {
      payload.replied_to_message_id =
        reply;
    }

    let response;

    try {
      response =
        await this.request(
          endpoint,
          {
            method: 'POST',

            body: payload,

            timeoutMs:
              this.timeouts.lookup,

            correlationId,

            logLabel:
              'YANE RESOLVE',

            logContext: {
              phone: p,
              hasReply:
                Boolean(reply),
            },

            suppressErrorLog: [
              404,
              405,
              500,
              501,
            ],
          }
        );
    } catch (error) {
      if (
        this._shouldFallbackFromResolve(
          error
        )
      ) {
        const fallbackTtl =
          MISSING_ENDPOINT_STATUSES.has(
            error?.status
          )
            ? BACKEND_CAPABILITY_CACHE_TTL_MS
            : RESOLVE_FALLBACK_COOLDOWN_MS;

        this._markCapabilityUnavailable(
          'resolveInbound',
          fallbackTtl
        );

        this._resolveFallbackUntil =
          Date.now() +
          fallbackTtl;

        this.logWarn(
          'resolve-inbound indisponível — a recorrer a by-phone.',
          {
            phone: p,
            fallbackReason:
              error?.code ||
              null,
            status:
              error?.status ||
              null,
            fallbackTtl,
          }
        );

        return this._resolveByPhoneFallback(
          p,
          {
            correlationId,
          }
        );
      }

      throw error;
    }

    const parsed =
      await this._parseJson(
        response,
        {
          endpoint,
          method: 'POST',
          correlationId,
        }
      );

    // O endpoint voltou a responder,
    // portanto o fallback deixa de ser necessário.
    this._clearCapabilityCache(
      'resolveInbound'
    );

    this._resolveFallbackUntil =
      0;

    return this._normalizeResolution(
      parsed
    );
  }

  _shouldFallbackFromResolve(
    error
  ) {
    if (
      !(error instanceof
        YaneIntegrationError)
    ) {
      return false;
    }

    if (
      error.status === null
    ) {
      return false;
    }

    return (
      MISSING_ENDPOINT_STATUSES.has(
        error.status
      ) ||
      error.status === 500
    );
  }

  async _resolveByPhoneFallback(
    phone,
    {
      correlationId = null,
    } = {}
  ) {
    const interview =
      await this.findActiveInterviewByPhone(
        phone,
        {
          correlationId,
        }
      );

    const id =
      clean(
        interview?.id ||
        interview?.interview_id ||
        ''
      );

    if (!id) {
      return {
        interview_id: null,
        confidence: 'none',
        method:
          'legacy_by_phone',
        candidates: [],
        scores: null,
      };
    }

    return {
      interview_id: id,
      confidence: 'high',
      method:
        'legacy_by_phone',
      candidates: [],
      scores: null,
    };
  }

  _normalizeResolution(
    parsed
  ) {
    if (
      !parsed ||
      typeof parsed !==
        'object'
    ) {
      return {
        interview_id: null,
        confidence: 'none',
        method: null,
        candidates: [],
        scores: null,
      };
    }

    const candidates =
      Array.isArray(
        parsed.candidates
      )
        ? parsed.candidates
            .filter(
              (candidate) =>
                candidate &&
                typeof candidate ===
                  'object'
            )
            .map(
              (candidate) => ({
                index:
                  Number.isFinite(
                    Number(
                      candidate.index
                    )
                  )
                    ? Number(
                        candidate.index
                      )
                    : null,

                interview_id:
                  clean(
                    candidate.interview_id
                  ) || null,

                job_title:
                  clean(
                    candidate.job_title
                  ) || '',

                company_name:
                  clean(
                    candidate.company_name
                  ) || '',

                score:
                  Number.isFinite(
                    Number(
                      candidate.score
                    )
                  )
                    ? Number(
                        candidate.score
                      )
                    : null,
              })
            )
        : [];

    const confidence =
      clean(
        parsed.confidence
      ).toLowerCase() ||
      'none';

    return {
      interview_id:
        clean(
          parsed.interview_id
        ) || null,

      confidence,

      method:
        clean(
          parsed.method
        ) || null,

      candidates,

      scores:
        parsed.scores &&
        typeof parsed.scores ===
          'object'
          ? parsed.scores
          : null,
    };
  }

  // ===========================================================================
  // CANCELAR — tolerante a endpoint inexistente
  // ===========================================================================

  async cancelInterview(
    interviewId,
    reason = 'candidate_reset',
    {
      correlationId = null,
    } = {}
  ) {
    const iid =
      clean(interviewId);

    const r =
      clean(reason) ||
      'candidate_reset';

    if (!iid) {
      throw new YaneIntegrationError(
        'interviewId é obrigatório.',
        {
          code:
            'INVALID_INTERVIEW_ID',
          retryable: false,
        }
      );
    }

    const endpoint =
      ENDPOINTS.interviewCancel(
        iid
      );

    if (
      this._isCapabilityUnavailable(
        'cancelInterview'
      )
    ) {
      return {
        success: true,
        skipped: true,
        reason:
          'endpoint_unavailable_cached',
      };
    }

    try {
      const response =
        await this.request(
          endpoint,
          {
            method: 'POST',

            body: {
              reason: r,
            },

            timeoutMs:
              this.timeouts.default,

            idempotencyKey:
              `cancel:${iid}`,

            correlationId,

            logLabel:
              'YANE CANCEL',

            logContext: {
              interviewId: iid,
              reason: r,
            },

            suppressErrorLog: [
              404,
              405,
              501,
            ],
          }
        );

      const parsed =
        await this._parseJson(
          response,
          {
            endpoint,
            method: 'POST',
            correlationId,
          }
        );

      this._clearCapabilityCache(
        'cancelInterview'
      );

      return parsed;
    } catch (error) {
      if (
        error instanceof
          YaneIntegrationError &&
        MISSING_ENDPOINT_STATUSES.has(
          error.status
        )
      ) {
        this._markCapabilityUnavailable(
          'cancelInterview'
        );

        this.logWarn(
          'cancel-interview indisponível — ignorado.',
          {
            interviewId: iid,
            status:
              error.status,
          }
        );

        return {
          success: true,
          skipped: true,
          reason:
            'endpoint_unavailable',
        };
      }

      throw error;
    }
  }

  // ===========================================================================
  // STATUS DE MENSAGEM
  // ===========================================================================

  async sendMessageStatus({
    phone,
    messageId,
    status = 'read',
    correlationId = null,
  }) {
    const p =
      clean(phone);

    const mid =
      clean(messageId);

    const s =
      clean(status) || 'read';

    if (!p || !mid) {
      return false;
    }

    const endpoint =
      ENDPOINTS.messageStatus;

    try {
      const response =
        await this.request(
          endpoint,
          {
            method: 'POST',

            body: {
              phone: p,
              message_id: mid,
              status: s,
              timestamp:
                new Date().toISOString(),
            },

            timeoutMs:
              this.timeouts.status,

            idempotencyKey:
              `status:${s}:${mid}`,

            correlationId,

            logLabel:
              'YANE STATUS',

            logContext: {
              phone: p,
              messageId: mid,
            },

            suppressErrorLog: [
              404,
              405,
            ],
          }
        );

      await this._readResponseText(
        response,
        {
          endpoint,
          method: 'POST',
        }
      );

      return response.ok;
    } catch (error) {
      this.logWarn(
        'Status de mensagem não enviado.',
        {
          phone: p,
          messageId: mid,
          code:
            error?.code || null,
          status:
            error?.status || null,
        }
      );

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
    const p =
      clean(phone);

    const j =
      clean(jid);

    const a =
      clean(altJid);

    const m =
      clean(message);

    const mid =
      clean(messageId);

    if (
      (!p && !j) ||
      !m
    ) {
      return false;
    }

    const endpoint =
      ENDPOINTS.unmatchedIncoming;

    try {
      const response =
        await this.request(
          endpoint,
          {
            method: 'POST',

            body: {
              phone:
                p || null,

              jid:
                j || null,

              alt_jid:
                a || null,

              message:
                m.slice(
                  0,
                  MAX_MESSAGE_BODY_CHARS
                ),

              message_id:
                mid || null,

              received_at:
                new Date().toISOString(),
            },

            timeoutMs:
              this.timeouts.status,

            idempotencyKey:
              mid
                ? `unmatched:${mid}`
                : null,

            correlationId,

            logLabel:
              'YANE UNMATCHED',

            logContext: {
              phone:
                p || null,

              jid:
                j || null,

              msgId:
                mid || null,
            },

            suppressErrorLog: [
              404,
              405,
            ],
          }
        );

      await this._readResponseText(
        response,
        {
          endpoint,
          method: 'POST',
        }
      );

      return response.ok;
    } catch (error) {
      if (
        error instanceof
          YaneIntegrationError &&
        (
          error.status === 404 ||
          error.status === 405
        )
      ) {
        return false;
      }

      this.logWarn(
        'Falha ao reportar órfã.',
        {
          phone:
            p || null,

          jid:
            j || null,

          msgId:
            mid || null,

          code:
            error?.code || null,

          status:
            error?.status || null,
        }
      );

      return false;
    }
  }

  // ===========================================================================
  // CAPACIDADES DO BACKEND
  // ===========================================================================

  getBackendCapabilities() {
    const now =
      Date.now();

    const capability =
      (name) => {
        const expiresAt =
          this._capabilityCache.get(
            name
          ) || 0;

        if (
          expiresAt <= now
        ) {
          if (expiresAt) {
            this._capabilityCache.delete(
              name
            );
          }

          return {
            available: true,
            expiresAt: null,
          };
        }

        return {
          available: false,
          expiresAt:
            new Date(
              expiresAt
            ).toISOString(),
        };
      };

    return {
      resolveInbound:
        capability(
          'resolveInbound'
        ),

      cancelInterview:
        capability(
          'cancelInterview'
        ),
    };
  }

  // ===========================================================================
  // HEALTH
  // ===========================================================================

  async healthCheck() {
    const endpoint =
      ENDPOINTS.health;

    try {
      const response =
        await this.request(
          endpoint,
          {
            method: 'GET',

            includeAuth: false,

            timeoutMs:
              this.timeouts.health,

            logLabel:
              'YANE HEALTH',

            suppressErrorLog: [
              404,
              500,
              502,
              503,
              504,
            ],
          }
        );

      await this._readResponseText(
        response,
        {
          endpoint,
          method: 'GET',
        }
      );

      return response.ok;
    } catch (error) {
      this.logWarn(
        'Health check falhou.',
        {
          code:
            error?.code || null,

          status:
            error?.status || null,
        }
      );

      return false;
    }
  }
}

module.exports =
  YaneIntegrationService;

module.exports.YaneIntegrationError =
  YaneIntegrationError;