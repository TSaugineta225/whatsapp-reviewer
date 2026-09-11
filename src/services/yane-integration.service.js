// src/services/yane-integration.service.js
//
// Cliente HTTP minimalista para o backend Python.
// Só expõe os endpoints que o bot usa.

const BaseService = require('./base.service');

// ============================================================
// CONFIG
// ============================================================

const DEFAULT_API_URL = 'http://localhost:8000/api';
const DEFAULT_TIMEOUT_MS = 15000;

const TURN_TIMEOUT_MS = 45000;
const STATUS_TIMEOUT_MS = 5000;
const HEALTH_TIMEOUT_MS = 3000;

const ENDPOINTS = Object.freeze({
  interviewTurn: (interviewId) =>
    `/interviews/${interviewId}/turn`,
  interviewByPhone: (phone) =>
    `/interviews/by-phone/${encodeURIComponent(phone)}`,
  messageStatus: '/webhooks/message-status',
  health: '/health',
});

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
  return Number.isFinite(number) && number > 0 ? number : fallback;
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

    if (!this.serviceToken) {
      console.warn(
        '[YANE] Nenhum token configurado. ' +
          'Defina YANE_SERVICE_TOKEN ou YANE_API_KEY.'
      );
    }

    console.log('[YANE] Serviço inicializado.');
    console.log('[YANE] Base URL:', this.yaneApiUrl);
  }

  // ==========================================================
  // HTTP BASE
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
      headers.Authorization = `Bearer ${this.serviceToken}`;
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
      console.error(`[${logLabel}] HTTP request failed:`, error.message);
      throw error;
    }
  }

  async requestJson(endpoint, options = {}) {
    const response = await this.request(endpoint, options);
    return this.parseJson(response);
  }

  async createHttpError(response) {
    let detail = '';

    try {
      const contentType =
        response.headers?.get?.('content-type') || '';

      if (contentType.toLowerCase().includes('application/json')) {
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
      // Sem corpo legível — ignorar.
    }

    const message =
      clean(detail) ||
      clean(response.statusText) ||
      'Erro desconhecido na API Yane';

    return new Error(`HTTP ${response.status}: ${message}`);
  }

  async parseJson(response) {
    const contentType =
      response.headers?.get?.('content-type') || '';

    if (!contentType.toLowerCase().includes('application/json')) {
      const text = await response.text();
      if (!clean(text)) return {};

      try {
        return JSON.parse(text);
      } catch (_) {
        return { response: text };
      }
    }

    return response.json();
  }

  handleIntegrationError(error, context) {
    console.error(`[YANE] ${context}:`, error.message);
    return this.handleError(error, context);
  }

  // ==========================================================
  // TURNO DA ENTREVISTA
  // ==========================================================

  async sendInterviewTurn({ interviewId, phone, message, turnId }) {
    if (!interviewId) {
      throw new Error('interviewId é obrigatório.');
    }

    if (!phone || !message) {
      throw new Error('phone e message são obrigatórios.');
    }

    const payload = {
      phone,
      message,
      turn_id: turnId || null,
    };

    return this.requestJson(ENDPOINTS.interviewTurn(interviewId), {
      method: 'POST',
      body: payload,
      timeoutMs: TURN_TIMEOUT_MS,
      logLabel: 'YANE TURN',
    });
  }

  // ==========================================================
  // LOOKUP POR TELEFONE
  // ==========================================================

  /**
   * Procura a entrevista activa associada a um telefone.
   *
   * Usado pelo bot quando perde o mapeamento local em Redis
   * (ex.: restart, flush, TTL expirado).
   *
   * Devolve null se não existir entrevista activa.
   */
  async findActiveInterviewByPhone(phone) {
    if (!phone) return null;

    try {
      const result = await this.requestJson(
        ENDPOINTS.interviewByPhone(phone),
        {
          method: 'GET',
          timeoutMs: this.timeoutMs,
          logLabel: 'YANE LOOKUP',
        }
      );

      return result || null;
    } catch (error) {
      // 404 = sem entrevista activa → não é erro
      if (error.message?.includes('404')) {
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

  async sendMessageStatus({ phone, messageId, status = 'read' }) {
    try {
      const response = await this.request(ENDPOINTS.messageStatus, {
        method: 'POST',
        body: {
          phone,
          message_id: messageId,
          status,
          timestamp: new Date().toISOString(),
        },
        timeoutMs: STATUS_TIMEOUT_MS,
        logLabel: 'YANE STATUS',
      });

      return response.ok;
    } catch (error) {
      console.error('[YANE] Erro ao enviar status:', error.message);
      return false;
    }
  }

  // ==========================================================
  // HEALTH CHECK
  // ==========================================================

  async healthCheck() {
    try {
      const response = await this.request(ENDPOINTS.health, {
        method: 'GET',
        includeAuth: false,
        timeoutMs: HEALTH_TIMEOUT_MS,
        logLabel: 'YANE HEALTH',
      });

      return response.ok;
    } catch (error) {
      console.error('[YANE] Health check falhou:', error.message);
      return false;
    }
  }
}

module.exports = YaneIntegrationService;