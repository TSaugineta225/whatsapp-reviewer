// src/services/metrics.service.js
//
// Registry central de métricas Prometheus do bot.
//
// Exposto via GET /metrics pelo app.js.
//
// Métricas instrumentadas:
//   - Entrevistas iniciadas/terminadas/concluídas
//   - Turnos processados (por status)
//   - Latência do backend Python (histograma)
//   - Mensagens WhatsApp enviadas/recebidas
//   - Erros por subsistema
//   - Estado da conexão (WhatsApp, Redis)
//   - Contagem de entrevistas activas (actualizada a cada 30s)

const client = require('prom-client');

class MetricsService {
  constructor() {
    this.registry = new client.Registry();

    // Prefixo comum a todas as métricas.
    this.registry.setDefaultLabels({ app: 'yane-bot' });

    // Métricas padrão do Node (CPU, memória, event loop, GC).
    client.collectDefaultMetrics({
      register: this.registry,
      prefix: 'yane_node_',
    });

    this._defineMetrics();

    // Refresh periódico de métricas que exigem I/O (Redis).
    this._refreshTimer = null;
  }

  // ============================================================
  // DEFINIÇÃO DAS MÉTRICAS
  // ============================================================

  _defineMetrics() {
    const r = this.registry;

    // ----------------------------------------------------------
    // Entrevistas
    // ----------------------------------------------------------
    this.interviewsStarted = new client.Counter({
      name: 'yane_interviews_started_total',
      help: 'Total de entrevistas iniciadas',
      registers: [r],
    });

    this.interviewsFinished = new client.Counter({
      name: 'yane_interviews_finished_total',
      help: 'Total de entrevistas terminadas',
      labelNames: ['status'], // completed | cancelled | expired
      registers: [r],
    });

    this.interviewsActive = new client.Gauge({
      name: 'yane_interviews_active',
      help: 'Entrevistas com mapeamento activo em Redis',
      registers: [r],
    });

    // ----------------------------------------------------------
    // Turnos
    // ----------------------------------------------------------
    this.turnsTotal = new client.Counter({
      name: 'yane_turns_total',
      help: 'Total de turnos processados',
      labelNames: ['status'], // success | backend_error | duplicate | locked | no_interview
      registers: [r],
    });

    this.turnDuration = new client.Histogram({
      name: 'yane_turn_duration_seconds',
      help: 'Duração do processamento de um turno',
      labelNames: ['endpoint'],
      buckets: [0.5, 1, 2, 5, 10, 20, 30, 60],
      registers: [r],
    });

    // ----------------------------------------------------------
    // Mensagens WhatsApp
    // ----------------------------------------------------------
    this.messagesReceived = new client.Counter({
      name: 'yane_messages_received_total',
      help: 'Total de mensagens recebidas do WhatsApp',
      labelNames: ['type'], // text | button | other
      registers: [r],
    });

    this.messagesSent = new client.Counter({
      name: 'yane_messages_sent_total',
      help: 'Total de mensagens enviadas para o WhatsApp',
      labelNames: ['status'], // success | failed | rate_limited
      registers: [r],
    });

    this.messageSendDuration = new client.Histogram({
      name: 'yane_message_send_duration_seconds',
      help: 'Duração do envio de mensagem via Baileys',
      buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5],
      registers: [r],
    });

    // ----------------------------------------------------------
    // Erros por subsistema
    // ----------------------------------------------------------
    this.errorsTotal = new client.Counter({
      name: 'yane_errors_total',
      help: 'Total de erros por subsistema',
      labelNames: ['subsystem'], // backend | redis | whatsapp | ai
      registers: [r],
    });

    // ----------------------------------------------------------
    // Estado
    // ----------------------------------------------------------
    this.whatsappConnected = new client.Gauge({
      name: 'yane_whatsapp_connected',
      help: 'Estado da conexão WhatsApp (1=conectado, 0=desconectado)',
      registers: [r],
    });

    this.redisConnected = new client.Gauge({
      name: 'yane_redis_connected',
      help: 'Estado da conexão Redis (1=pronto, 0=indisponível)',
      registers: [r],
    });

    this.whatsappUptimeSeconds = new client.Gauge({
      name: 'yane_whatsapp_uptime_seconds',
      help: 'Segundos desde a última conexão WhatsApp bem-sucedida',
      registers: [r],
    });

    // ----------------------------------------------------------
    // Rate limit
    // ----------------------------------------------------------
    this.rateLimitHits = new client.Counter({
      name: 'yane_rate_limit_hits_total',
      help: 'Total de vezes que o rate limit foi atingido',
      labelNames: ['bucket'], // out
      registers: [r],
    });
  }

  // ============================================================
  // INICIALIZAÇÃO / SHUTDOWN
  // ============================================================

  /**
   * Inicia o refresh periódico de métricas que dependem de I/O.
   *
   * @param {Object} deps — { redis, whatsappService }
   */
  start(deps = {}) {
    this.redis = deps.redis || null;
    this.whatsappService = deps.whatsappService || null;

    // Actualização inicial
    this._refreshDerivedMetrics();

    // Refresh a cada 30s (mais rápido que o scrape do Prometheus — 15s — mas
    // suficientemente espaçado para não sobrecarregar o Redis).
    this._refreshTimer = setInterval(
      () => this._refreshDerivedMetrics(),
      30_000
    );

    // Não bloquear o shutdown do Node
    if (this._refreshTimer.unref) this._refreshTimer.unref();
  }

  stop() {
    if (this._refreshTimer) {
      clearInterval(this._refreshTimer);
      this._refreshTimer = null;
    }
  }

  async _refreshDerivedMetrics() {
    // Redis
    if (this.redis) {
      this.redisConnected.set(this.redis.isReady ? 1 : 0);

      if (this.redis.isReady) {
        try {
          const count = await this._countInterviewKeys();
          this.interviewsActive.set(count);
        } catch (error) {
          this.errorsTotal.inc({ subsystem: 'redis' });
        }
      } else {
        this.interviewsActive.set(0);
      }
    }

    // WhatsApp
    if (this.whatsappService) {
      const connected = this.whatsappService.isReady ? 1 : 0;
      this.whatsappConnected.set(connected);

      if (connected && this.whatsappService.connectedAt) {
        const uptime = Math.floor(
          (Date.now() - this.whatsappService.connectedAt) / 1000
        );
        this.whatsappUptimeSeconds.set(uptime);
      } else if (!connected) {
        this.whatsappUptimeSeconds.set(0);
      }
    }
  }

  async _countInterviewKeys() {
    // Usa SCAN por padrão — não bloqueia o Redis.
    // É aceitável a cada 30s para < 100k chaves.
    const pattern = `${this.redis.prefix}:phone:*`;
    let cursor = '0';
    let count = 0;

    do {
      const [next, keys] = await this.redis.client.scan(
        cursor,
        'MATCH',
        pattern,
        'COUNT',
        500
      );
      cursor = next;
      count += keys.length;
    } while (cursor !== '0');

    return count;
  }

  // ============================================================
  // HELPERS DE INSTRUMENTAÇÃO
  // ============================================================

  /**
   * Envolve uma função async, medindo a duração e registando sucesso/erro.
   */
  async time(histogram, labels, fn) {
    const end = histogram.startTimer(labels);
    try {
      return await fn();
    } finally {
      end();
    }
  }

  async getMetrics() {
    return this.registry.metrics();
  }

  getContentType() {
    return this.registry.contentType;
  }
}

// Singleton — importar em qualquer serviço.
module.exports = new MetricsService();