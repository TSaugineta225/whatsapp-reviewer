// src/services/yane-integration.service.js
//
// Cliente HTTP entre o WhatsApp service (Node) e o backend Python.
//
// Responsabilidades:
//
//   - construir URLs
//   - autenticar chamadas internas
//   - aplicar timeouts
//   - normalizar erros HTTP
//   - preservar status/code/retryability
//   - enviar idempotency key dos turnos
//   - expor operações específicas da entrevista
//
// IMPORTANTES GARANTIAS:
//
//   O InterviewService é responsável pelos retries.
//   Este serviço NÃO faz retry automático.
//
//   Um turnId é enviado:
//      - no body como turn_id
//      - no header Idempotency-Key
//
// Isso permite que o backend Python reconheça retries do
// mesmo turno e não processe/cobre o mesmo turno duas vezes.
//
// Node.js fornece fetch global e AbortSignal.timeout()
// nas versões modernas suportadas pelo projeto.
//

const BaseService = require('./base.service');

// ============================================================
// CONFIG
// ============================================================

const DEFAULT_API_URL =
  'http://localhost:8000/api';

const DEFAULT_TIMEOUT_MS =
  15_000;

const TURN_TIMEOUT_MS =
  45_000;

const STATUS_TIMEOUT_MS =
  5_000;

const HEALTH_TIMEOUT_MS =
  3_000;

const MAX_ERROR_BODY_CHARS =
  2_000;

const MAX_RESPONSE_BODY_BYTES =
  2 * 1024 * 1024; // 2 MB

const ENDPOINTS = Object.freeze({
  interviewTurn: (interviewId) =>
    `/interviews/${encodeURIComponent(
      String(interviewId)
    )}/turn`,

  interviewByPhone: (phone) =>
    `/interviews/by-phone/${encodeURIComponent(
      String(phone)
    )}`,

  messageStatus:
    '/webhooks/message-status',

  health:
    '/health',
});

// ============================================================
// HELPERS
// ============================================================

function clean(value) {
  return String(value ?? '').trim();
}

function normalizeUrl(url) {
  return clean(url)
    .replace(/\/+$/, '');
}

function toPositiveNumber(
  value,
  fallback
) {
  const number =
    Number(value);

  return Number.isFinite(number) &&
    number > 0
    ? number
    : fallback;
}

function isJsonContentType(
  contentType
) {
  return clean(contentType)
    .toLowerCase()
    .includes('application/json');
}

function truncate(
  value,
  max = MAX_ERROR_BODY_CHARS
) {
  return clean(value).slice(
    0,
    max
  );
}

function isAbortError(error) {
  return (
    error?.name ===
      'AbortError' ||
    error?.name ===
      'TimeoutError'
  );
}

// ============================================================
// ERRO HTTP
// ============================================================

class YaneIntegrationError extends Error {
  constructor(
    message,
    {
      status = null,
      code = 'YANE_HTTP_ERROR',
      retryable = false,
      endpoint = null,
      method = null,
      cause = null,
    } = {}
  ) {
    super(message);

    this.name =
      'YaneIntegrationError';

    this.status =
      Number.isInteger(status)
        ? status
        : null;

    this.code =
      code;

    this.retryable =
      Boolean(retryable);

    this.endpoint =
      endpoint;

    this.method =
      method;

    if (cause) {
      this.cause = cause;
    }
  }
}

// ============================================================
// SERVIÇO
// ============================================================

class YaneIntegrationService
  extends BaseService {

  constructor() {
    super();

    this.yaneApiUrl =
      normalizeUrl(
        process.env.YANE_API_URL ||
          DEFAULT_API_URL
      );

    this.timeoutMs =
      toPositiveNumber(
        process.env.YANE_API_TIMEOUT,
        DEFAULT_TIMEOUT_MS
      );

    this.serviceToken =
      clean(
        process.env.YANE_SERVICE_TOKEN
      ) ||
      clean(
        process.env.YANE_API_KEY
      ) ||
      null;

    if (!this.serviceToken) {
      console.warn(
        '[YANE] Nenhum token configurado. ' +
        'Defina YANE_SERVICE_TOKEN ou YANE_API_KEY.'
      );
    }

    console.log(
      '[YANE] Serviço inicializado.'
    );

    console.log(
      '[YANE] Base URL:',
      this.yaneApiUrl
    );
  }

  // ==========================================================
  // HTTP BASE
  // ==========================================================

  buildUrl(endpoint) {
    const path =
      `/${clean(endpoint).replace(
        /^\/+/,
        ''
      )}`;

    return `${this.yaneApiUrl}${path}`;
  }

  getHeaders({
    includeAuth = true,
    idempotencyKey = null,
    extra = {},
  } = {}) {
    const headers = {
      Accept:
        'application/json',

      'Content-Type':
        'application/json',

      ...extra,
    };

    if (
      includeAuth &&
      this.serviceToken
    ) {
      headers.Authorization =
        `Bearer ${this.serviceToken}`;
    }

    if (idempotencyKey) {
      headers[
        'Idempotency-Key'
      ] = String(
        idempotencyKey
      );
    }

    return headers;
  }

  async request(
    endpoint,
    {
      method = 'GET',
      body = undefined,
      includeAuth = true,
      timeoutMs = this.timeoutMs,
      idempotencyKey = null,
      logLabel = 'YANE',
    } = {}
  ) {
    const url =
      this.buildUrl(endpoint);

    const normalizedTimeout =
      toPositiveNumber(
        timeoutMs,
        this.timeoutMs
      );

    const options = {
      method,
      headers:
        this.getHeaders({
          includeAuth,
          idempotencyKey,
        }),

      signal:
        AbortSignal.timeout(
          normalizedTimeout
        ),
    };

    if (
      body !== undefined
    ) {
      options.body =
        JSON.stringify(body);
    }

    try {
      const response =
        await fetch(
          url,
          options
        );

      if (!response.ok) {
        throw await this.createHttpError(
          response,
          {
            endpoint,
            method,
          }
        );
      }

      return response;
    } catch (error) {
      const normalized =
        this.normalizeRequestError(
          error,
          {
            endpoint,
            method,
            timeoutMs:
              normalizedTimeout,
          }
        );

      console.error(
        `[${logLabel}] ${normalized.code}:`,
        normalized.message
      );

      throw normalized;
    }
  }

  normalizeRequestError(
    error,
    {
      endpoint,
      method,
      timeoutMs,
    }
  ) {
    // Já é nosso erro normalizado.
    if (
      error instanceof
      YaneIntegrationError
    ) {
      return error;
    }

    // Timeout / abort.
    if (
      isAbortError(error)
    ) {
      return new YaneIntegrationError(
        `Timeout após ${timeoutMs}ms`,
        {
          code:
            'YANE_TIMEOUT',
          retryable: true,
          endpoint,
          method,
          cause: error,
        }
      );
    }

    // Erro de rede:
    // DNS, ECONNREFUSED, socket etc.
    return new YaneIntegrationError(
      clean(
        error?.message
      ) ||
        'Falha de rede ao contactar a API Yane.',
      {
        code:
          'YANE_NETWORK_ERROR',
        retryable: true,
        endpoint,
        method,
        cause: error,
      }
    );
  }

  async createHttpError(
    response,
    {
      endpoint,
      method,
    } = {}
  ) {
    let detail = '';

    try {
      const contentType =
        response.headers?.get?.(
          'content-type'
        ) || '';

      if (
        isJsonContentType(
          contentType
        )
      ) {
        const data =
          await response.json();

        detail =
          data?.detail ||
          data?.message ||
          data?.error ||
          '';

        if (
          !detail &&
          data &&
          typeof data ===
            'object'
        ) {
          detail =
            JSON.stringify(data);
        }
      } else {
        detail =
          await response.text();
      }
    } catch (_) {
      // O corpo do erro não é obrigatório.
    }

    const status =
      response.status;

    const message =
      truncate(
        detail
      ) ||
      clean(
        response.statusText
      ) ||
      'Erro desconhecido na API Yane.';

    return new YaneIntegrationError(
      `HTTP ${status}: ${message}`,
      {
        status,
        code:
          `YANE_HTTP_${status}`,
        retryable:
          this.isRetryableStatus(
            status
          ),
        endpoint,
        method,
      }
    );
  }

  isRetryableStatus(status) {
    // 408 Request Timeout
    // 425 Too Early
    // 429 Too Many Requests
    // 5xx = problema temporário no servidor
    return (
      status === 408 ||
      status === 425 ||
      status === 429 ||
      status >= 500
    );
  }

  async requestJson(
    endpoint,
    options = {}
  ) {
    const response =
      await this.request(
        endpoint,
        options
      );

    return this.parseJson(
      response
    );
  }

  async parseJson(response) {
    const contentType =
      response.headers?.get?.(
        'content-type'
      ) || '';

    /*
     * Para APIs internas, JSON é o contrato esperado.
     *
     * Ainda assim, aceitamos text/plain para
     * diagnóstico e compatibilidade.
     */
    const text =
      await this.readResponseText(
        response
      );

    if (!clean(text)) {
      return {};
    }

    if (
      isJsonContentType(
        contentType
      )
    ) {
      try {
        return JSON.parse(text);
      } catch (error) {
        throw new YaneIntegrationError(
          'A API Yane respondeu JSON inválido.',
          {
            code:
              'YANE_INVALID_JSON',
            retryable: false,
            cause: error,
          }
        );
      }
    }

    // Compatibilidade com respostas textuais.
    try {
      return JSON.parse(text);
    } catch (_) {
      return {
        response: text,
      };
    }
  }

  async readResponseText(
    response
  ) {
    /*
     * fetch não nos dá um limite simples de
     * tamanho do body. Como esta é uma API interna,
     * colocamos um limite defensivo.
     */
    const contentLength =
      response.headers?.get?.(
        'content-length'
      );

    if (
      contentLength &&
      Number(contentLength) >
        MAX_RESPONSE_BODY_BYTES
    ) {
      throw new YaneIntegrationError(
        'Resposta da API Yane excedeu o tamanho permitido.',
        {
          code:
            'YANE_RESPONSE_TOO_LARGE',
          retryable: false,
        }
      );
    }

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
        'Resposta da API Yane excedeu o tamanho permitido.',
        {
          code:
            'YANE_RESPONSE_TOO_LARGE',
          retryable: false,
        }
      );
    }

    return text;
  }

  handleIntegrationError(
    error,
    context
  ) {
    console.error(
      `[YANE] ${context}:`,
      error.message
    );

    /*
     * Mantemos a compatibilidade com BaseService.
     *
     * Caso BaseService possua tratamento próprio,
     * deixamos que ele faça o trabalho. Se ele retornar
     * outro erro, não perdemos o original.
     */
    if (
      typeof this.handleError ===
      'function'
    ) {
      const handled =
        this.handleError(
          error,
          context
        );

      return handled || error;
    }

    return error;
  }

  // ==========================================================
  // TURNO DA ENTREVISTA
  // ==========================================================

  /**
   * Envia um turno do candidato ao backend Python.
   *
   * `turnId` é a identidade idempotente do turno.
   *
   * O mesmo ID deve representar exatamente a mesma operação
   * em todos os retries.
   */
  async sendInterviewTurn({
    interviewId,
    phone,
    message,
    turnId,
    messageId,
  }) {
    const normalizedInterviewId =
      clean(interviewId);

    const normalizedPhone =
      clean(phone);

    const normalizedMessage =
      clean(message);

    const normalizedTurnId =
      clean(turnId);

    if (
      !normalizedInterviewId
    ) {
      throw new YaneIntegrationError(
        'interviewId é obrigatório.',
        {
          code:
            'INVALID_INTERVIEW_ID',
          retryable: false,
        }
      );
    }

    if (
      !normalizedPhone ||
      !normalizedMessage
    ) {
      throw new YaneIntegrationError(
        'phone e message são obrigatórios.',
        {
          code:
            'INVALID_TURN_PAYLOAD',
          retryable: false,
        }
      );
    }

    const payload = {
      phone:
        normalizedPhone,

      message:
        normalizedMessage,

      turn_id:
        normalizedTurnId || null,
    };

    if (
      clean(messageId)
    ) {
      payload.message_id =
        clean(messageId);
    }

    return this.requestJson(
      ENDPOINTS.interviewTurn(
        normalizedInterviewId
      ),
      {
        method: 'POST',

        body: payload,

        timeoutMs:
          TURN_TIMEOUT_MS,

        // Fundamental para retries.
        idempotencyKey:
          normalizedTurnId ||
          null,

        logLabel:
          'YANE TURN',
      }
    );
  }

  // ==========================================================
  // LOOKUP POR TELEFONE
  // ==========================================================

  async findActiveInterviewByPhone(
    phone
  ) {
    const normalizedPhone =
      clean(phone);

    if (!normalizedPhone) {
      return null;
    }

    try {
      const result =
        await this.requestJson(
          ENDPOINTS.interviewByPhone(
            normalizedPhone
          ),
          {
            method: 'GET',

            timeoutMs:
              this.timeoutMs,

            logLabel:
              'YANE LOOKUP',
          }
        );

      return result || null;
    } catch (error) {
      /*
       * 404 = não existe entrevista ativa.
       *
       * Não fazemos:
       *   error.message.includes('404')
       *
       * porque mensagem é texto diagnóstico,
       * não contrato.
       */
      if (
        error instanceof
          YaneIntegrationError &&
        error.status === 404
      ) {
        return null;
      }

      throw this.handleIntegrationError(
        error,
        'Yane Interview Lookup'
      );
    }
  }

  // ==========================================================
  // STATUS DE MENSAGEM
  // ==========================================================

  async sendMessageStatus({
    phone,
    messageId,
    status = 'read',
  }) {
    const normalizedPhone =
      clean(phone);

    const normalizedMessageId =
      clean(messageId);

    if (
      !normalizedPhone ||
      !normalizedMessageId
    ) {
      return false;
    }

    try {
      const response =
        await this.request(
          ENDPOINTS.messageStatus,
          {
            method: 'POST',

            body: {
              phone:
                normalizedPhone,

              message_id:
                normalizedMessageId,

              status:
                clean(status) ||
                'read',

              timestamp:
                new Date()
                  .toISOString(),
            },

            timeoutMs:
              STATUS_TIMEOUT_MS,

            // Status pode ser repetido pelo transporte
            // sem representar um novo evento lógico.
            idempotencyKey:
              normalizedMessageId,

            logLabel:
              'YANE STATUS',
          }
        );

      return response.ok;
    } catch (error) {
      console.error(
        '[YANE] Erro ao enviar status:',
        error.message
      );

      return false;
    }
  }

  // ==========================================================
  // HEALTH CHECK
  // ==========================================================

  async healthCheck() {
    try {
      const response =
        await this.request(
          ENDPOINTS.health,
          {
            method: 'GET',

            includeAuth:
              false,

            timeoutMs:
              HEALTH_TIMEOUT_MS,

            logLabel:
              'YANE HEALTH',
          }
        );

      return response.ok;
    } catch (error) {
      console.error(
        '[YANE] Health check falhou:',
        error.message
      );

      return false;
    }
  }
}

module.exports =
  YaneIntegrationService;

module.exports.YaneIntegrationError =
  YaneIntegrationError;