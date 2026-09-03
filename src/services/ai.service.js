const BaseService = require('./base.service');
const YaneIntegrationService = require('./yane-integration.service');

class AIService extends BaseService {
  constructor() {
    super();

    this.yane = new YaneIntegrationService();

    // Cache simples em memória.
    this.cache = new Map();

    // Se não estiver definido, o backend decide o modelo
    // de acordo com o usuário/plano.
    this.defaultModel = process.env.AI_MODEL || null;

    // Limite para evitar crescimento indefinido do cache.
    this.maxCacheSize = 100;

    // Temperatura padrão.
    this.defaultTemperature = 0.7;
  }

  /**
   * Gera uma resposta usando o serviço de IA.
   *
   * @param {string} prompt
   * @param {string|null} systemPrompt
   * @param {string|null} model
   * @returns {Promise<string>}
   */
  async generateResponse(prompt, systemPrompt = null, model = null) {
    try {
      this.validatePrompt(prompt);

      const selectedModel = model || this.defaultModel;
      const cacheKey = this.createCacheKey(
        prompt,
        systemPrompt,
        selectedModel
      );

      // Retorna imediatamente se já estiver em cache.
      const cachedResponse = this.cache.get(cacheKey);

      if (cachedResponse) {
        return cachedResponse;
      }

      const messages = this.buildMessages(prompt, systemPrompt);

      const response = await this.yane.callAI(
        messages,
        selectedModel,
        this.defaultTemperature
      );

      if (!this.isValidResponse(response)) {
        return response;
      }

      this.setCache(cacheKey, response);

      return response;
    } catch (error) {
      console.error('[AI] Erro ao gerar resposta:', error.message);

      throw this.handleError(error, 'AI Service');
    }
  }

  /**
   * Constrói o array de mensagens enviado para a IA.
   */
  buildMessages(prompt, systemPrompt) {
    const messages = [];

    if (systemPrompt?.trim()) {
      messages.push({
        role: 'system',
        content: systemPrompt.trim(),
      });
    }

    messages.push({
      role: 'user',
      content: prompt.trim(),
    });

    return messages;
  }

  /**
   * Cria uma chave determinística para o cache.
   */
  createCacheKey(prompt, systemPrompt, model) {
    return JSON.stringify({
      prompt: prompt.trim(),
      systemPrompt: systemPrompt?.trim() || null,
      model: model || null,
      temperature: this.defaultTemperature,
    });
  }

  /**
   * Guarda uma resposta no cache.
   */
  setCache(key, response) {
    // Se a chave já existir, remove primeiro para atualizar
    // sua posição no Map.
    if (this.cache.has(key)) {
      this.cache.delete(key);
    }

    this.cache.set(key, response);

    // Remove o item mais antigo quando atingir o limite.
    while (this.cache.size > this.maxCacheSize) {
      const oldestKey = this.cache.keys().next().value;

      if (oldestKey === undefined) {
        break;
      }

      this.cache.delete(oldestKey);
    }
  }

  /**
   * Valida o prompt antes de chamar a API.
   */
  validatePrompt(prompt) {
    if (typeof prompt !== 'string' || !prompt.trim()) {
      throw new Error('O prompt é obrigatório.');
    }
  }

  /**
   * Verifica se a resposta pode ser armazenada.
   */
  isValidResponse(response) {
    return (
      typeof response === 'string' &&
      response.trim().length > 10
    );
  }

  /**
   * Limpa todo o cache.
   */
  clearCache() {
    this.cache.clear();

    console.log('[AI] Cache limpo');
  }

  /**
   * Retorna informações básicas do cache.
   */
  getCacheStats() {
    return {
      size: this.cache.size,
      maxSize: this.maxCacheSize,
    };
  }
}

module.exports = AIService;
