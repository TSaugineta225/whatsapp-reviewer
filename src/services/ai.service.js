// src/services/ai.service.js
const BaseService = require('./base.service');
const YaneIntegrationService = require('./yane-integration.service');

class AIService extends BaseService {
  constructor() {
    super();
    this.yane = new YaneIntegrationService();
    this.cache = new Map();
    this.defaultModel = 'deepseek-chat';
  }

  async generateResponse(prompt, systemPrompt) {
    try {
      const cacheKey = `${prompt}_${systemPrompt}`;
      if (this.cache.has(cacheKey)) {
        return this.cache.get(cacheKey);
      }

      const messages = [];
      if (systemPrompt) {
        messages.push({ role: 'system', content: systemPrompt });
      }
      messages.push({ role: 'user', content: prompt });

      const response = await this.yane.callAI(messages, this.defaultModel, 0.7);
      
      // Cache apenas se a resposta for válida
      if (response && response.length > 10) {
        this.cache.set(cacheKey, response);
        // Limitar cache a 100 entradas
        if (this.cache.size > 100) {
          const firstKey = this.cache.keys().next().value;
          this.cache.delete(firstKey);
        }
      }
      
      return response;
    } catch (error) {
      console.error('[AI] Erro ao gerar resposta:', error.message);
      throw this.handleError(error, 'AI Service');
    }
  }

  /**
   * Limpa o cache
   */
  clearCache() {
    this.cache.clear();
    console.log('[AI] Cache limpo');
  }
}

module.exports = AIService;