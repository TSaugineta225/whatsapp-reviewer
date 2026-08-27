// src/services/yane-integration.service.js
const BaseService = require('./base.service');

class YaneIntegrationService extends BaseService {
  constructor() {
    super();
    this.yaneApiUrl = process.env.YANE_API_URL || 'http://localhost:8000/api';
    this.webhookEndpoint = '/interviews/webhooks/result';
    this.aiEndpoint = '/ai/chat';
    this.statusEndpoint = '/webhooks/message-status';
  }

  async sendInterviewResult(data) {
    try {
      const url = `${this.yaneApiUrl}${this.webhookEndpoint}`;
      const payload = {
        phone: data.phone,
        candidate_name: data.candidateName,
        candidate_email: data.candidateEmail || 'nao_informado@exemplo.com',
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
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP ${response.status}: ${errorText || response.statusText}`);
      }

      const result = await response.json();
      console.log('[YANE] Resultados enviados com sucesso');
      return result;
    } catch (error) {
      console.error('[YANE] Erro ao enviar resultados:', error.message);
      throw this.handleError(error, 'Yane Integration');
    }
  }

  async callAI(messages, model = 'deepseek-chat', temperature = 0.7) {
    try {
      const url = `${this.yaneApiUrl}${this.aiEndpoint}`;
      const headers = {
        'Content-Type': 'application/json',
      };
      if (process.env.YANE_SERVICE_TOKEN) {
        headers['Authorization'] = `Bearer ${process.env.YANE_SERVICE_TOKEN}`;
      }

      const response = await fetch(url, {
        method: 'POST',
        headers: headers,
        body: JSON.stringify({
          messages: messages,
          model: model,
          temperature: temperature,
          max_tokens: 1024,
        }),
      });

      if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`HTTP ${response.status}: ${errorText || response.statusText}`);
      }

      const result = await response.json();
      return result.response || result.content || '';
    } catch (error) {
      console.error('[YANE] Erro ao chamar IA:', error.message);
      throw this.handleError(error, 'Yane AI');
    }
  }

  // ============================================================
  // NOVO: Enviar status de leitura para a Yane
  // ============================================================
  async sendMessageStatus(data) {
    try {
      const url = `${this.yaneApiUrl}${this.statusEndpoint}`;
      const response = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          phone: data.phone,
          message_id: data.message_id,
          status: data.status || 'read',
          timestamp: data.timestamp || new Date().toISOString()
        })
      });
      return response.ok;
    } catch (error) {
      console.error('[YANE] Erro ao enviar status:', error.message);
      return false;
    }
  }

  async healthCheck() {
    try {
      const response = await fetch(`${this.yaneApiUrl}/health`);
      return response.ok;
    } catch {
      return false;
    }
  }
}

module.exports = YaneIntegrationService;