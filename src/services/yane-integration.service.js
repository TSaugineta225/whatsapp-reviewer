
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
   *
   * IMPORTANTE:
   * Os métodos públicos e os contratos existentes são mantidos
   * para evitar impacto nos consumidores deste serviço.
   */

  constructor() {
    super();

    this.yaneApiUrl = (
      process.env.YANE_API_URL ||
      'http://localhost:8000/api'
    ).replace(/\/+$/, '');

    this.webhookEndpoint = '/interviews/webhooks/result';
    this.aiEndpoint = '/ai/chat';
    this.statusEndpoint = '/webhooks/message-status';
  }

  // ==========================================================================
  // CONFIGURAÇÃO / HELPERS INTERNOS
  // ==========================================================================

  /**
   * Constrói uma URL da API Yane.
   */
  buildUrl(endpoint) {
    return `${this.yaneApiUrl}${endpoint}`;
  }

  /**
   * Retorna os headers padrão da integração.
   *
   * O token continua opcional, preservando o comportamento anterior.
   */
  getHeaders(includeAuth = false) {
    const headers = {
      'Content-Type': 'application/json',
    };

    if (includeAuth && process.env.YANE_SERVICE_TOKEN) {
      headers.Authorization = `Bearer ${process.env.YANE_SERVICE_TOKEN}`;
    }

    return headers;
  }

  /**
   * Converte uma resposta HTTP de erro em Error consistente.
   */
  async createHttpError(response) {
    let errorText = '';

    try {
      errorText = await response.text();
    } catch {
      // Mantém o fallback abaixo caso o corpo não possa ser lido.
    }

    const message =
      errorText ||
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
   *
   * Mantém o comportamento esperado para endpoints que retornam JSON.
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
      };

      const response = await fetch(url, {
        method: 'POST',
        headers: this.getHeaders(),
        body: JSON.stringify(payload),
      });

      await this.ensureSuccessfulResponse(response);

      const result = await this.parseJson(response);

      console.log(
        '[YANE] Resultados da entrevista enviados com sucesso'
      );

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
    temperature = 0.7
  ) {
    try {
      const url = this.buildUrl(this.aiEndpoint);

      const payload = {
        messages,
        model,
        temperature,
        max_tokens: 1024,
      };

      const response = await fetch(url, {
        method: 'POST',
        headers: this.getHeaders(true),
        body: JSON.stringify(payload),
      });

      await this.ensureSuccessfulResponse(response);

      const result = await this.parseJson(response);

      return (
        result.response ||
        result.content ||
        ''
      );
    } catch (error) {
      console.error(
        '[YANE] Erro ao chamar IA:',
        error.message
      );

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
        headers: this.getHeaders(),
        body: JSON.stringify(payload),
      });

      return response.ok;
    } catch (error) {
      console.error(
        '[YANE] Erro ao enviar status:',
        error.message
      );

      // Mantém o comportamento original:
      // falhas neste endpoint não interrompem o fluxo principal.
      return false;
    }
  }

  // ==========================================================================
  // HEALTH CHECK
  // ==========================================================================

  async healthCheck() {
    try {
      const response = await fetch(
        this.buildUrl('/health')
      );

      return response.ok;
    } catch {
      return false;
    }
  }
}

module.exports = YaneIntegrationService;

