// src/services/yane-integration.service.js

const BaseService = require('./base.service');

class YaneIntegrationService extends BaseService {
  /**
   * Serviço de integração com a API Yane.
   *
   * Responsabilidades:
   * - Enviar resultados das entrevistas.
   * - Comunicar com o serviço de IA.
   * - Enviar status de mensagens.
   * - Verificar disponibilidade da API Yane.
   */

  constructor() {
    super();

    // Garante o prefixo /api na URL base caso não informado no .env
    this.yaneApiUrl = (
      process.env.YANE_API_URL ||
      'http://localhost:8000/api'
    ).replace(/\/+$/, '');

    this.webhookEndpoint = '/interviews/webhooks/result';
    this.aiEndpoint = '/ai/chat'; // Aponta para /api/ai/chat
    this.statusEndpoint = '/webhooks/message-status';
    this.timeoutMs = Number(process.env.YANE_API_TIMEOUT) || 15000;

    console.log('[YANE DEBUG] Serviço inicializado.');
    console.log('[YANE DEBUG] Base URL configurada:', this.yaneApiUrl);
  }

  // ==========================================================================
  // CONFIGURAÇÃO / HELPERS INTERNOS
  // ==========================================================================

  /**
   * Constrói uma URL da API Yane garantindo a formatação correta das barras.
   */
  buildUrl(endpoint) {
    const cleanEndpoint = endpoint.startsWith('/') ? endpoint : `/${endpoint}`;
    const fullUrl = `${this.yaneApiUrl}${cleanEndpoint}`;
    return fullUrl;
  }

  /**
   * Retorna os headers padrão da integração.
   * Envia autenticação Bearer por padrão, aceitando YANE_SERVICE_TOKEN ou YANE_API_KEY.
   */
  getHeaders(includeAuth = true) {
    const headers = {
      'Content-Type': 'application/json',
    };

    const token = process.env.YANE_SERVICE_TOKEN || process.env.YANE_API_KEY;

    if (includeAuth && token) {
      headers.Authorization = `Bearer ${token}`;
    } else if (includeAuth && !token) {
      console.warn('[YANE DEBUG] AVISO: Nenhum token de autenticação (YANE_SERVICE_TOKEN ou YANE_API_KEY) foi encontrado no .env');
    }

    return headers;
  }

  /**
   * Converte uma resposta HTTP de erro em Error consistente,
   * extraindo os detalhes retornados pela exceção do FastAPI (campo `detail`).
   */
  async createHttpError(response) {
    let errorDetail = '';

    try {
      const data = await response.json();
      errorDetail = data.detail || data.message || JSON.stringify(data);
    } catch {
      try {
        errorDetail = await response.text();
      } catch {
        // Mantém o fallback caso o corpo não possa ser lido.
      }
    }

    const message =
      errorDetail ||
      response.statusText ||
      'Erro desconhecido na API Yane';

    return new Error(
      `HTTP ${response.status}: ${message}`
    );
  }

  /**
   * Valida uma resposta HTTP.
   */
  async ensureSuccessfulResponse(response) {
    if (!response.ok) {
      throw await this.createHttpError(response);
    }

    return response;
  }

  /**
   * Faz parsing seguro de JSON.
   */
  async parseJson(response) {
    return response.json();
  }

  // ==========================================================================
  // RESULTADO DA ENTREVISTA
  // ==========================================================================

  async sendInterviewResult(data) {
    try {
      const url = this.buildUrl(this.webhookEndpoint);

      console.log('[YANE DEBUG] Enviando resultado de entrevista para:', url);

      const payload = {
        phone: data.phone,
        candidate_name: data.candidateName,
        candidate_email:
          data.candidateEmail || 'nao_informado@exemplo.com',
        job_title: data.jobTitle,
        score: data.score,
        feedback: data.feedback,
        recommendation: data.recommendation,
        transcript: data.transcript,
        scores_breakdown: data.scoresBreakdown,
        interview_id: data.interviewId || null,
        hold_transaction_id: data.holdTransactionId || null,
        actual_cost: data.actualCost ?? data.actual_cost ?? null,
      };

      const response = await fetch(url, {
        method: 'POST',
        headers: this.getHeaders(true),
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(this.timeoutMs),
      });

      console.log(`[YANE DEBUG] Resposta de Resultado de Entrevista - Status: ${response.status}`);

      await this.ensureSuccessfulResponse(response);

      const result = await this.parseJson(response);

      console.log('[YANE] Resultados da entrevista enviados com sucesso');

      return result;
    } catch (error) {
      console.error(
        '[YANE] Erro ao enviar resultados:',
        error.message
      );

      throw this.handleError(
        error,
        'Yane Integration'
      );
    }
  }

  // ==========================================================================
  // IA
  // ==========================================================================

  async callAI(
    messages,
    model = 'deepseek-chat',
    temperature = 0.7,
    userId = null
  ) {
    try {
      const url = this.buildUrl(this.aiEndpoint);

      console.log('--------------------------------------------------');
      console.log('[YANE DEBUG] Chamando Endpoint da IA...');
      console.log('[YANE DEBUG] URL Final da Requisição:', url);
      console.log('[YANE DEBUG] Modelo:', model);
      console.log('[YANE DEBUG] User ID:', userId || 'Não informado');
      console.log('[YANE DEBUG] Total de Mensagens:', Array.isArray(messages) ? messages.length : 0);

      const payload = {
        messages,
        model,
        temperature,
        max_tokens: 1024,
      };

      if (userId) {
        payload.user_id = userId;
      }

      const headers = this.getHeaders(true);
      console.log('[YANE DEBUG] Headers enviados:', {
        'Content-Type': headers['Content-Type'],
        'Authorization': headers.Authorization ? 'Bearer [PRESENTE]' : '[AUSENTE]',
      });

      const response = await fetch(url, {
        method: 'POST',
        headers,
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(this.timeoutMs),
      });

      console.log(`[YANE DEBUG] Resposta HTTP da IA Received. Status: ${response.status} (${response.statusText})`);
      console.log('--------------------------------------------------');

      await this.ensureSuccessfulResponse(response);

      const result = await this.parseJson(response);

      return (
        result.response ||
        result.content ||
        ''
      );
    } catch (error) {
      console.error('--------------------------------------------------');
      console.error('[YANE DEBUG ERROR] Falha na chamada de IA');
      console.error('[YANE DEBUG ERROR] Mensagem de Erro:', error.message);
      console.error('--------------------------------------------------');

      throw this.handleError(
        error,
        'Yane AI'
      );
    }
  }

  // ==========================================================================
  // STATUS DE MENSAGEM
  // ==========================================================================

  async sendMessageStatus(data) {
    try {
      const url = this.buildUrl(this.statusEndpoint);

      console.log('[YANE DEBUG] Enviando status de mensagem para:', url);

      const payload = {
        phone: data.phone,
        message_id: data.message_id,
        status: data.status || 'read',
        timestamp:
          data.timestamp ||
          new Date().toISOString(),
      };

      const response = await fetch(url, {
        method: 'POST',
        headers: this.getHeaders(true),
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(5000),
      });

      console.log(`[YANE DEBUG] Resposta de Status de Mensagem - Status: ${response.status}`);

      return response.ok;
    } catch (error) {
      console.error(
        '[YANE] Erro ao enviar status:',
        error.message
      );

      return false;
    }
  }

  // ==========================================================================
  // HEALTH CHECK
  // ==========================================================================

  async healthCheck() {
    try {
      const url = this.buildUrl('/health');
      console.log('[YANE DEBUG] Testando Health Check em:', url);

      const response = await fetch(url, {
        headers: this.getHeaders(false),
        signal: AbortSignal.timeout(3000),
      });

      console.log(`[YANE DEBUG] Health Check Status: ${response.status}`);

      return response.ok;
    } catch (error) {
      console.error('[YANE DEBUG] Health Check falhou:', error.message);
      return false;
    }
  }
}

module.exports = YaneIntegrationService;