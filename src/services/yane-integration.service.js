// src/services/yane-integration.service.js

const BaseService = require('./base.service');

// ============================================================
// CONFIGURAÇÃO
// ============================================================

const DEFAULT_API_URL = 'http://localhost:8000/api';
const DEFAULT_TIMEOUT_MS = 15000;

const ENDPOINTS = Object.freeze({
  interviewResult: '/interviews/webhooks/result',
  ai: '/ai/chat',
  messageStatus: '/webhooks/message-status',
  health: '/health',
});

const STATUS_TIMEOUT_MS = 5000;
const HEALTH_TIMEOUT_MS = 3000;

const FALLBACK_CANDIDATE_EMAIL =
  'nao_informado@exemplo.com';

// ============================================================
// HELPERS
// ============================================================

function clean(value) {
  return String(value ?? '').trim();
}

function normalizeUrl(url) {
  return clean(url).replace(/\/+$/, '');
}

function toPositiveNumber(value, fallback) {
  const number = Number(value);

  return Number.isFinite(number) && number > 0
    ? number
    : fallback;
}

// ============================================================
// SERVIÇO
// ============================================================

class YaneIntegrationService extends BaseService {
  constructor() {
    super();

    this.yaneApiUrl = normalizeUrl(
      process.env.YANE_API_URL || DEFAULT_API_URL
    );

    this.timeoutMs = toPositiveNumber(
      process.env.YANE_API_TIMEOUT,
      DEFAULT_TIMEOUT_MS
    );

    this.serviceToken =
      clean(process.env.YANE_SERVICE_TOKEN) ||
      clean(process.env.YANE_API_KEY) ||
      null;

    this.warnIfConfigurationIsIncomplete();

    console.log('[YANE] Serviço inicializado.');
    console.log(
      '[YANE] Base URL:',
      this.yaneApiUrl
    );
  }

  // ==========================================================
  // CONFIGURAÇÃO
  // ==========================================================

  warnIfConfigurationIsIncomplete() {
    if (!this.serviceToken) {
      console.warn(
        '[YANE] Nenhum token configurado. ' +
        'Defina YANE_SERVICE_TOKEN ou YANE_API_KEY.'
      );
    }
  }

  // ==========================================================
  // HTTP
  // ==========================================================

  buildUrl(endpoint) {
    const path = `/${clean(endpoint).replace(/^\/+/, '')}`;

    return `${this.yaneApiUrl}${path}`;
  }

  getHeaders(includeAuth = true) {
    const headers = {
      'Content-Type': 'application/json',
      Accept: 'application/json',
    };

    if (includeAuth && this.serviceToken) {
      headers.Authorization =
        `Bearer ${this.serviceToken}`;
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
      logLabel = 'YANE',
    } = {}
  ) {
    const url = this.buildUrl(endpoint);

    const options = {
      method,
      headers: this.getHeaders(includeAuth),
      signal: AbortSignal.timeout(timeoutMs),
    };

    if (body !== undefined) {
      options.body = JSON.stringify(body);
    }

    try {
      const response = await fetch(url, options);

      if (!response.ok) {
        throw await this.createHttpError(response);
      }

      return response;
    } catch (error) {
      console.error(
        `[${logLabel}] HTTP request failed:`,
        error.message
      );

      throw error;
    }
  }

  async requestJson(endpoint, options = {}) {
    const response = await this.request(
      endpoint,
      options
    );

    return this.parseJson(response);
  }

  async createHttpError(response) {
    let detail = '';

    try {
      const contentType =
        response.headers?.get?.('content-type') || '';

      if (
        contentType.toLowerCase().includes(
          'application/json'
        )
      ) {
        const data = await response.json();

        detail =
          data?.detail ||
          data?.message ||
          data?.error ||
          JSON.stringify(data);
      } else {
        detail = await response.text();
      }
    } catch (_) {
      // Algumas respostas HTTP podem não possuir corpo legível.
    }

    const message =
      clean(detail) ||
      clean(response.statusText) ||
      'Erro desconhecido na API Yane';

    return new Error(
      `HTTP ${response.status}: ${message}`
    );
  }

  async parseJson(response) {
    const contentType =
      response.headers?.get?.('content-type') || '';

    if (
      !contentType.toLowerCase().includes(
        'application/json'
      )
    ) {
      const text = await response.text();

      if (!clean(text)) {
        return {};
      }

      try {
        return JSON.parse(text);
      } catch (_) {
        return {
          response: text,
        };
      }
    }

    return response.json();
  }

  handleIntegrationError(error, context) {
    console.error(
      `[YANE] ${context}:`,
      error.message
    );

    return this.handleError(
      error,
      context
    );
  }

  // ==========================================================
  // RESULTADO DA ENTREVISTA
  // ==========================================================

  buildInterviewResultPayload(data = {}) {
    return {
      phone: data.phone,

      candidate_name:
        data.candidateName,

      expected_candidate_name:
        data.expectedCandidateName || null,

      is_identity_verified:
        Boolean(data.isIdentityVerified),

      candidate_email:
        clean(data.candidateEmail) ||
        FALLBACK_CANDIDATE_EMAIL,

      candidate_cv:
        data.candidateCv || null,

      job_title:
        data.jobTitle,

      job_requirements:
        Array.isArray(data.jobRequirements)
          ? data.jobRequirements
          : [],

      score:
        data.score,

      feedback:
        data.feedback,

      recommendation:
        data.recommendation,

      verified_claims:
        Array.isArray(data.verifiedClaims)
          ? data.verifiedClaims
          : [],

      identified_gaps:
        Array.isArray(data.identifiedGaps)
          ? data.identifiedGaps
          : [],

      transcript:
        Array.isArray(data.transcript)
          ? data.transcript
          : [],

      scores_breakdown:
        Array.isArray(data.scoresBreakdown)
          ? data.scoresBreakdown
          : [],

      interview_id:
        data.interviewId || null,

      hold_transaction_id:
        data.holdTransactionId || null,

      actual_cost:
        data.actualCost ??
        data.actual_cost ??
        null,
    };
  }

  async sendInterviewResult(data = {}) {
    try {
      const payload =
        this.buildInterviewResultPayload(data);

      console.log(
        '[YANE] A enviar resultado da entrevista:',
        this.buildSafeDebugPayload(payload)
      );

      const result =
        await this.requestJson(
          ENDPOINTS.interviewResult,
          {
            method: 'POST',
            body: payload,
            logLabel: 'YANE INTERVIEW',
          }
        );

      console.log(
        '[YANE] Resultado da entrevista enviado com sucesso.'
      );

      return result;
    } catch (error) {
      throw this.handleIntegrationError(
        error,
        'Yane Integration'
      );
    }
  }

  // ==========================================================
  // IA
  // ==========================================================

  buildAIPayload({
    messages,
    model,
    temperature,
    userId,
  }) {
    const payload = {
      messages,
      model,
      temperature,
      max_tokens: 1024,
    };

    if (
      userId !== null &&
      userId !== undefined &&
      clean(userId)
    ) {
      payload.user_id = userId;
    }

    return payload;
  }

  /**
   * Extrai conteúdo de diferentes formatos possíveis
   * devolvidos por APIs de LLM.
   *
   * Formatos suportados:
   *
   * { response: "..." }
   * { content: "..." }
   * { message: { content: "..." } }
   * { data: { response: "..." } }
   * { data: { content: "..." } }
   * { choices: [{ message: { content: "..." } }] }
   * { choices: [{ text: "..." }] }
   * "resposta directa"
   */
  extractAIContent(result) {
    if (typeof result === 'string') {
      return clean(result);
    }

    if (
      !result ||
      typeof result !== 'object'
    ) {
      return '';
    }

    const candidates = [
      result.response,
      result.content,
      result.message?.content,
      result.data?.response,
      result.data?.content,
      result.choices?.[0]?.message?.content,
      result.choices?.[0]?.text,
    ];

    for (const value of candidates) {
      if (
        typeof value === 'string' &&
        clean(value)
      ) {
        return clean(value);
      }
    }

    return '';
  }

  async callAI(
    messages,
    model = 'deepseek-chat',
    temperature = 0.7,
    userId = null
  ) {
    try {
      const safeMessages =
        Array.isArray(messages)
          ? messages
          : [];

      const safeTemperature = Number(
        temperature
      );

      const payload =
        this.buildAIPayload({
          messages: safeMessages,
          model:
            clean(model) ||
            'deepseek-chat',
          temperature:
            Number.isFinite(
              safeTemperature
            )
              ? safeTemperature
              : 0.7,
          userId,
        });

      console.log(
        '[YANE AI] Request:',
        {
          model: payload.model,

          userId:
            userId || 'Não informado',

          messages:
            safeMessages.length,

          temperature:
            payload.temperature,
        }
      );

      const result =
        await this.requestJson(
          ENDPOINTS.ai,
          {
            method: 'POST',
            body: payload,
            logLabel: 'YANE AI',
          }
        );

      const content =
        this.extractAIContent(result);

      if (!content) {
        console.warn(
          '[YANE AI] API respondeu sem conteúdo utilizável.',
          {
            responseType:
              typeof result,

            responseKeys:
              result &&
              typeof result === 'object'
                ? Object.keys(result)
                : [],
          }
        );
      }

      return content;
    } catch (error) {
      throw this.handleIntegrationError(
        error,
        'Yane AI'
      );
    }
  }

  // ==========================================================
  // STATUS DE MENSAGEM
  // ==========================================================

  buildMessageStatusPayload(data = {}) {
    return {
      phone: data.phone,

      message_id:
        data.message_id,

      status:
        data.status || 'read',

      timestamp:
        data.timestamp ||
        new Date().toISOString(),
    };
  }

  async sendMessageStatus(data = {}) {
    try {
      const payload =
        this.buildMessageStatusPayload(data);

      const response =
        await this.request(
          ENDPOINTS.messageStatus,
          {
            method: 'POST',
            body: payload,
            timeoutMs:
              STATUS_TIMEOUT_MS,
            logLabel: 'YANE STATUS',
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
            includeAuth: false,
            timeoutMs:
              HEALTH_TIMEOUT_MS,
            logLabel: 'YANE HEALTH',
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

  // ==========================================================
  // DEBUG SEGURO
  // ==========================================================

  buildSafeDebugPayload(payload = {}) {
    return {
      phone:
        payload.phone || null,

      candidate_name:
        payload.candidate_name ||
        null,

      job_title:
        payload.job_title ||
        null,

      score:
        payload.score ?? null,

      recommendation:
        payload.recommendation ||
        null,

      interview_id:
        payload.interview_id ||
        null,

      has_cv:
        Boolean(
          payload.candidate_cv
        ),

      transcript_size:
        Array.isArray(
          payload.transcript
        )
          ? payload.transcript.length
          : 0,

      scores_size:
        Array.isArray(
          payload.scores_breakdown
        )
          ? payload.scores_breakdown.length
          : 0,
    };
  }
}

module.exports = YaneIntegrationService;

