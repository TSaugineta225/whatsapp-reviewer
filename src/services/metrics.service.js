// src/services/metrics.service.js
//
// Registry central de métricas Prometheus do Yane Bot.
//
// Exposto via GET /metrics pelo app.js.
//
// Princípios:
//   - observabilidade nunca pode interromper o fluxo de negócio;
//   - refresh de métricas derivadas deve ser idempotente;
//   - falhas de Redis/WhatsApp devem ser isoladas;
//   - timers não devem acumular múltiplas execuções;
//   - labels declarados precisam corresponder aos usados pelos serviços.
//
// [FIX-QUEUE-1]
//   Declaradas as métricas de fila/dead-letter que o
//   interview.service.js já invocava:
//       queueEnqueued, queueRequeued, queueDeadLettered,
//       queueDepth, deadLetterDepth.
//
//   Antes desta revisão, as chamadas caiam no vazio por causa do
//   optional chaining em _metricIncrement/_metricSet do
//   InterviewService. A observabilidade da fila era silenciosamente
//   inexistente.
//
// [FIX-QUEUE-2]  <<< NOVO >>>
//   Declarada a métrica `workerBatches`, invocada pelo
//   InterviewService após cada lote de telefones processado pelo
//   worker de recuperação. Sem esta declaração, o counter ficava
//   silenciosamente em falta e não era possível observar pressão
//   da fila nem fracassos por lote.
//
//   Labels:
//       size     — número de telefones no lote (string, para bucket)
//       failures — número de falhas no lote (string, para bucket)
//
//   Para séries estáveis, os valores são convertidos para string
//   com limites discretos (batch_size <= 24, failures <= 24). Os
//   dashboards agregam por estes buckets discretos.
//
// [FIX-QUEUE-3]  <<< NOVO >>>
//   Declarada a métrica `aggregationDepth` — profundidade do buffer
//   de agregação por telefone. Permite ver quantas mensagens estão
//   acumuladas à espera de flush num dado momento.

const client = require('prom-client');

const APP_NAME = 'yane-bot';
const DEFAULT_METRICS_PREFIX = 'yane_node_';

const REFRESH_INTERVAL_MS = 30_000;
const REDIS_SCAN_COUNT = 500;

const TURN_DURATION_BUCKETS = [
  0.5,
  1,
  2,
  5,
  10,
  20,
  30,
  60,
];

const MESSAGE_SEND_DURATION_BUCKETS = [
  0.05,
  0.1,
  0.25,
  0.5,
  1,
  2,
  5,
];

class MetricsService {
  constructor() {
    this.registry = new client.Registry();

    this.registry.setDefaultLabels({
      app: APP_NAME,
    });

    client.collectDefaultMetrics({
      register: this.registry,
      prefix: DEFAULT_METRICS_PREFIX,
    });

    // Dependências externas.
    this.redis = null;
    this.whatsappService = null;

    // Lifecycle.
    this._refreshTimer = null;
    this._refreshInFlight = false;

    // Evita spam de logs sem esconder falhas de métricas diferentes.
    this._metricInstrumentationFailures = new Set();

    this._defineMetrics();
  }

  // ============================================================
  // DEFINIÇÃO DAS MÉTRICAS
  // ============================================================

  _defineMetrics() {
    const registers = [this.registry];

    // ----------------------------------------------------------
    // Entrevistas
    // ----------------------------------------------------------

    this.interviewsStarted = new client.Counter({
      name: 'yane_interviews_started_total',
      help: 'Total de entrevistas iniciadas (convite entregue)',
      registers,
    });

    this.interviewsQueued = new client.Counter({
      name: 'yane_interviews_queued_total',
      help: 'Total de entrevistas cujo convite ficou em fila para entrega',
      registers,
    });

    this.interviewsFinished = new client.Counter({
      name: 'yane_interviews_finished_total',
      help: 'Total de entrevistas terminadas',
      labelNames: ['status'],
      registers,
    });

    this.interviewsActive = new client.Gauge({
      name: 'yane_interviews_active',
      help: 'Entrevistas com mapeamento activo em Redis',
      registers,
    });

    // ----------------------------------------------------------
    // Turnos
    // ----------------------------------------------------------

    this.turnsTotal = new client.Counter({
      name: 'yane_turns_total',
      help: 'Total de turnos processados',
      labelNames: ['status'],
      registers,
    });

    this.turnDuration = new client.Histogram({
      name: 'yane_turn_duration_seconds',
      help: 'Duração do processamento de um turno',
      labelNames: ['endpoint'],
      buckets: TURN_DURATION_BUCKETS,
      registers,
    });

    // ----------------------------------------------------------
    // Mensagens WhatsApp
    // ----------------------------------------------------------

    this.messagesReceived = new client.Counter({
      name: 'yane_messages_received_total',
      help: 'Total de mensagens recebidas do WhatsApp',
      labelNames: ['type'],
      registers,
    });

    this.messagesSent = new client.Counter({
      name: 'yane_messages_sent_total',
      help: 'Total de mensagens enviadas para o WhatsApp',
      labelNames: ['status'],
      registers,
    });

    // IMPORTANTE:
    // whatsapp.service.js envia { attempt } para este histograma.
    // O label precisa existir no schema do prom-client para que
    // startTimer() não lance antes de socket.sendMessage().
    this.messageSendDuration = new client.Histogram({
      name: 'yane_message_send_duration_seconds',
      help: 'Duração do envio de mensagem via Baileys',
      labelNames: ['attempt'],
      buckets: MESSAGE_SEND_DURATION_BUCKETS,
      registers,
    });

    // ----------------------------------------------------------
    // Fila de pendentes e dead-letter
    // ----------------------------------------------------------
    //
    // [FIX-QUEUE-1] Declaradas as métricas que o InterviewService
    // invocava mas não existiam no registry. Sem isto, as chamadas
    // caiam no vazio pelo optional chaining.
    //
    // Convenções:
    //   - kind   : 'turn' | 'delivery' | 'unknown'
    //   - reason : 'lock_busy' | 'pending_queue' | 'backend_error'
    //              | 'lookup_failed' | 'max_attempts_reached'
    //              | 'non_retryable_error' | 'backend_terminal_error'
    //              | 'lookup_exhausted' | 'no_interview_resolvable'
    //              | 'invalid_json' | 'unknown'

    this.queueEnqueued = new client.Counter({
      name: 'yane_queue_enqueued_total',
      help: 'Itens enfileirados na fila de pendentes',
      labelNames: ['kind', 'reason'],
      registers,
    });

    this.queueRequeued = new client.Counter({
      name: 'yane_queue_requeued_total',
      help: 'Itens reagendados após falha',
      labelNames: ['kind'],
      registers,
    });

    this.queueDeadLettered = new client.Counter({
      name: 'yane_queue_dead_lettered_total',
      help: 'Itens enviados para dead-letter',
      labelNames: ['kind', 'reason'],
      registers,
    });

    this.queueDepth = new client.Gauge({
      name: 'yane_queue_depth',
      help: 'Profundidade da fila no lote corrente',
      labelNames: ['kind'],
      registers,
    });

    this.deadLetterDepth = new client.Gauge({
      name: 'yane_dead_letter_depth',
      help: 'Itens acumulados na dead-letter',
      registers,
    });

    // ----------------------------------------------------------
    // Worker de recuperação
    // ----------------------------------------------------------
    //
    // [FIX-QUEUE-2] Counter invocado pelo InterviewService após
    // cada lote de telefones processado.
    //
    // Labels são strings para caber em séries discretas. Os valores
    // são o tamanho do lote (0..workerBatchSize) e o número de
    // falhas nesse lote (0..workerBatchSize).
    //
    // Para o agregado global, um dashboard pode fazer:
    //   sum(rate(yane_worker_batches_total[5m])) by (failures)

    this.workerBatches = new client.Counter({
      name: 'yane_worker_batches_total',
      help: 'Lotes de telefones processados pelo worker de recuperação',
      labelNames: ['size', 'failures'],
      registers,
    });

    // ----------------------------------------------------------
    // Agregação de mensagens
    // ----------------------------------------------------------
    //
    // [FIX-QUEUE-3] Profundidade do buffer de agregação por telefone.
    // Fica em falta a contagem de telefones com buffer aberto — a
    // ser adicionada em revisão futura se valer a pena para
    // dashboards.

    this.aggregationDepth = new client.Gauge({
      name: 'yane_aggregation_depth',
      help: 'Mensagens em buffer de agregação por telefone',
      labelNames: ['kind'],
      registers,
    });

    // ----------------------------------------------------------
    // Erros
    // ----------------------------------------------------------

    this.errorsTotal = new client.Counter({
      name: 'yane_errors_total',
      help: 'Total de erros por subsistema',
      labelNames: ['subsystem'],
      registers,
    });

    // ----------------------------------------------------------
    // Estado
    // ----------------------------------------------------------

    this.whatsappConnected = new client.Gauge({
      name: 'yane_whatsapp_connected',
      help: 'Estado da conexão WhatsApp (1=conectado, 0=desconectado)',
      registers,
    });

    this.redisConnected = new client.Gauge({
      name: 'yane_redis_connected',
      help: 'Estado da conexão Redis (1=pronto, 0=indisponível)',
      registers,
    });

    this.whatsappUptimeSeconds = new client.Gauge({
      name: 'yane_whatsapp_uptime_seconds',
      help: 'Segundos desde a última conexão WhatsApp bem-sucedida',
      registers,
    });

    // ----------------------------------------------------------
    // Rate limit
    // ----------------------------------------------------------

    this.rateLimitHits = new client.Counter({
      name: 'yane_rate_limit_hits_total',
      help: 'Total de vezes que o rate limit foi atingido',
      labelNames: ['bucket'],
      registers,
    });
  }

  // ============================================================
  // INICIALIZAÇÃO / SHUTDOWN
  // ============================================================

  /**
   * Inicia o refresh periódico das métricas derivadas.
   *
   * É idempotente:
   * chamar start() duas vezes não cria dois intervalos.
   */
  start(deps = {}) {
    this.redis = deps.redis || null;
    this.whatsappService = deps.whatsappService || null;

    if (this._refreshTimer) {
      return;
    }

    // Primeira atualização imediata.
    void this._refreshDerivedMetrics();

    this._refreshTimer = setInterval(() => {
      void this._refreshDerivedMetrics();
    }, REFRESH_INTERVAL_MS);

    // O timer de métricas não deve impedir o processo de terminar.
    if (typeof this._refreshTimer.unref === 'function') {
      this._refreshTimer.unref();
    }
  }

  /**
   * Para o refresh periódico.
   */
  stop() {
    if (!this._refreshTimer) {
      return;
    }

    clearInterval(this._refreshTimer);
    this._refreshTimer = null;
  }

  // ============================================================
  // MÉTRICAS DERIVADAS
  // ============================================================

  /**
   * Atualiza métricas cujo valor vem de estado externo.
   *
   * Nunca propaga erros.
   *
   * Também impede refresh concorrente caso uma operação Redis
   * ultrapasse o intervalo configurado.
   */
  async _refreshDerivedMetrics() {
    if (this._refreshInFlight) {
      return;
    }

    this._refreshInFlight = true;

    try {
      await this._refreshRedisMetrics();
      this._refreshWhatsAppMetrics();
    } catch (error) {
      // Última barreira: observabilidade não deve quebrar o worker.
      this._recordMetricError('metrics_refresh', error);
    } finally {
      this._refreshInFlight = false;
    }
  }

  async _refreshRedisMetrics() {
    const redis = this.redis;

    if (!redis) {
      return;
    }

    const isReady = Boolean(redis.isReady);

    this._safeGaugeSet(this.redisConnected, isReady ? 1 : 0);

    if (!isReady) {
      // Não interpretamos "Redis indisponível" como
      // "zero entrevistas activas". Mantemos o último valor.
      return;
    }

    try {
      const count = await this._countInterviewKeys(redis);
      this._safeGaugeSet(this.interviewsActive, count);
    } catch (error) {
      this._recordMetricError('redis', error);
    }
  }

  _refreshWhatsAppMetrics() {
    const whatsappService = this.whatsappService;

    if (!whatsappService) {
      return;
    }

    const connected = Boolean(whatsappService.isReady);

    this._safeGaugeSet(
      this.whatsappConnected,
      connected ? 1 : 0
    );

    if (!connected) {
      this._safeGaugeSet(this.whatsappUptimeSeconds, 0);
      return;
    }

    const connectedAtMs = this._toTimestampMs(
      whatsappService.connectedAt
    );

    if (connectedAtMs == null) {
      this._safeGaugeSet(this.whatsappUptimeSeconds, 0);
      return;
    }

    const uptimeSeconds = Math.max(
      0,
      Math.floor((Date.now() - connectedAtMs) / 1000)
    );

    this._safeGaugeSet(
      this.whatsappUptimeSeconds,
      uptimeSeconds
    );
  }

  // ============================================================
  // REDIS
  // ============================================================

  /**
   * Conta entrevistas activas usando SCAN, evitando KEYS.
   */
  async _countInterviewKeys(redis = this.redis) {
    if (!redis?.client) {
      throw new Error('Redis client indisponível');
    }

    if (typeof redis.client.scan !== 'function') {
      throw new Error('Redis client não suporta SCAN');
    }

    const prefix = String(redis.prefix || 'yane');
    const pattern = `${prefix}:phone:*`;

    let cursor = '0';
    let count = 0;

    do {
      const result = await redis.client.scan(
        cursor,
        'MATCH',
        pattern,
        'COUNT',
        REDIS_SCAN_COUNT
      );

      const parsed = this._parseScanResult(result);

      cursor = parsed.cursor;
      count += parsed.keys.length;
    } while (cursor !== '0');

    return count;
  }

  /**
   * Suporta tanto respostas no formato:
   *
   *   [cursor, keys]
   *
   * como:
   *
   *   { cursor, keys }
   *
   * sem alterar a integração actual.
   */
  _parseScanResult(result) {
    if (Array.isArray(result)) {
      const [cursor, keys] = result;

      return {
        cursor: String(cursor ?? '0'),
        keys: Array.isArray(keys) ? keys : [],
      };
    }

    if (result && typeof result === 'object') {
      return {
        cursor: String(result.cursor ?? '0'),
        keys: Array.isArray(result.keys) ? result.keys : [],
      };
    }

    throw new Error('Resposta inválida do Redis SCAN');
  }

  // ============================================================
  // INSTRUMENTAÇÃO
  // ============================================================

  /**
   * Mede a execução de uma operação sem permitir que
   * a instrumentação interrompa o fluxo de negócio.
   *
   * Garantias:
   *   - startTimer() pode falhar sem impedir fn();
   *   - stopTimer() pode falhar sem substituir o erro de fn();
   *   - erros da própria fn() continuam sendo propagados.
   */
  async time(histogram, labels, fn) {
    let endTimer = null;

    try {
      if (!histogram || typeof histogram.startTimer !== 'function') {
        throw new TypeError(
          'Histograma inválido: startTimer() não disponível'
        );
      }

      endTimer = histogram.startTimer(labels);
    } catch (error) {
      this._recordInstrumentationFailure(histogram, error);
    }

    try {
      return await fn();
    } finally {
      if (typeof endTimer === 'function') {
        try {
          endTimer();
        } catch (error) {
          this._recordInstrumentationFailure(
            histogram,
            error,
            'stopTimer'
          );
        }
      }
    }
  }

  _recordInstrumentationFailure(
    histogram,
    error,
    phase = 'startTimer'
  ) {
    const metricName =
      histogram && typeof histogram.name === 'string'
        ? histogram.name
        : 'unknown_metric';

    const key = `${phase}:${metricName}`;

    if (this._metricInstrumentationFailures.has(key)) {
      return;
    }

    this._metricInstrumentationFailures.add(key);

    try {
      console.error(
        `[METRICS] ${phase} falhou para "${metricName}". ` +
          'A instrumentação foi ignorada para preservar o fluxo de negócio.',
        error?.message || error
      );
    } catch (_) {
      // Logging também é best-effort.
    }
  }

  // ============================================================
  // HELPERS DE SEGURANÇA
  // ============================================================

  _safeGaugeSet(gauge, value) {
    try {
      if (gauge && typeof gauge.set === 'function') {
        gauge.set(value);
      }
    } catch (error) {
      this._recordInstrumentationFailure(
        gauge,
        error,
        'gauge.set'
      );
    }
  }

  _recordMetricError(subsystem, error) {
    try {
      this.errorsTotal.inc({
        subsystem,
      });
    } catch (metricError) {
      this._recordInstrumentationFailure(
        this.errorsTotal,
        metricError,
        'errorsTotal.inc'
      );
    }

    try {
      console.error(
        `[METRICS] Falha no subsistema "${subsystem}".`,
        error?.message || error
      );
    } catch (_) {
      // Logging é best-effort.
    }
  }

  _toTimestampMs(value) {
    if (value == null) {
      return null;
    }

    if (value instanceof Date) {
      const timestamp = value.getTime();
      return Number.isFinite(timestamp) ? timestamp : null;
    }

    if (typeof value === 'number') {
      return Number.isFinite(value) ? value : null;
    }

    if (typeof value === 'string') {
      const numeric = Number(value);

      if (Number.isFinite(numeric)) {
        return numeric;
      }

      const parsed = Date.parse(value);

      return Number.isFinite(parsed) ? parsed : null;
    }

    return null;
  }

  // ============================================================
  // EXPOSIÇÃO PROMETHEUS
  // ============================================================

  async getMetrics() {
    return this.registry.metrics();
  }

  getContentType() {
    return this.registry.contentType;
  }
}

module.exports = new MetricsService();