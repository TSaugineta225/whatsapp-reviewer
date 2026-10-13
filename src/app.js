// src/app.js
//
// [ENV-REFACTOR]
//   - Removido `dotenv.config()` (passa a estar no `config/env.js`,
//     que corre antes de qualquer service importar `process.env`).
//   - `requireServiceToken` usa `env.yane.serviceToken`.
//   - `METRICS_TOKEN` vem de `env.metrics.token`.
//   - `console.log`/`console.error` substituídos por `logger` pino
//     para logs estruturados (mantendo o comportamento).

'use strict';

// IMPORTANTE: primeiro require — carrega o .env e valida-o.
const env = require('./config/env');

const express = require('express');
const pino = require('pino');

const errorHandler = require('./middleware/error.middleware');
const routes = require('./routes');

const RedisService = require('./services/redis.service');
const InterviewService = require('./services/interview.service');
const WhatsAppService = require('./services/whatsapp.service');
const YaneIntegrationService = require('./services/yane-integration.service');
const metrics = require('./services/metrics.service');

// Logger principal do bot. Nível controlado por LOG_LEVEL.
const logger = pino({
  level: env.logging.level,
  base: { service: 'bot' },
});

// ============================================================
// SERVIÇOS
// ============================================================

const redis = new RedisService();

const interviewService = new InterviewService(redis);
const whatsappService = new WhatsAppService(interviewService, redis);
const yaneIntegration = new YaneIntegrationService();

// Globais usados pelo InterviewService para enviar bolhas.
global.whatsappService = whatsappService;
global.interviewService = interviewService;
global.yaneIntegration = yaneIntegration;
global.redisService = redis;

// ============================================================
// MIDDLEWARE — autenticação de serviço
// ============================================================
//
// [SEC-PROD] Endpoints sensíveis protegidos por bearer token.
//
//   Authorization: Bearer <YANE_SERVICE_TOKEN>
//
// Se `env.yane.serviceToken` estiver vazio:
//   - em dev:     deixa passar (útil para testes manuais).
//   - em prod:    o `env.js` já abortou o arranque, portanto isto
//                 nunca chega a correr sem token em produção.

function requireServiceToken(req, res, next) {
  const expected = env.yane.serviceToken;

  if (!expected) {
    return next();
  }

  const auth = req.headers.authorization || '';
  const token = auth.startsWith('Bearer ') ? auth.slice(7) : null;

  if (!token || token !== expected) {
    return res.status(401).json({
      success: false,
      message: 'Unauthorized',
    });
  }

  return next();
}

// ============================================================
// APP
// ============================================================

const app = express();

app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true }));

app.use('/', routes);

// ============================================================
// START INTERVIEW — chamado pelo backend Python após invite
// ============================================================

app.post('/start-interview', requireServiceToken, async (req, res) => {
  try {
    const result = await interviewService.startInterview(req.body);

    if (!result.success) {
      return res.status(400).json(result);
    }

    return res.json(result);
  } catch (error) {
    logger.error({ err: error }, '/start-interview falhou');
    return res.status(500).json({
      success: false,
      message: 'Erro ao iniciar entrevista.',
      error: error.message,
    });
  }
});

// ============================================================
// SEND MESSAGE — notificações avulsas
// ============================================================

app.post('/send-message', requireServiceToken, async (req, res) => {
  try {
    const { to, message } = req.body;

    if (!to) {
      return res.status(400).json({
        success: false,
        message: 'O parâmetro "to" é obrigatório.',
      });
    }

    if (!message) {
      return res.status(400).json({
        success: false,
        message: 'O parâmetro "message" é obrigatório.',
      });
    }

    const preview =
      message.length > 50 ? `${message.substring(0, 50)}...` : message;

    logger.info({ to: `${to.slice(0, 6)}***`, preview }, 'A enviar mensagem');

    const sent = await whatsappService.sendMessage(to, message);

    if (!sent) {
      return res.status(500).json({
        success: false,
        message: 'Falha ao enviar mensagem.',
      });
    }

    return res.json({ success: true, to });
  } catch (error) {
    logger.error({ err: error }, '/send-message falhou');
    return res.status(500).json({
      success: false,
      message: 'Erro interno ao enviar mensagem.',
      error: error.message,
    });
  }
});

// ============================================================
// STATUS
// ============================================================
//
// [FIX-QR-DASHBOARD] — devolve status flat (chaves no topo) para o
// React + objecto `whatsapp` aninhado para compatibilidade.

app.get('/status', (req, res) => {
  try {
    const whatsappStatus = whatsappService.getStatus();

    res.json({
      connected: whatsappStatus.connected,
      ready: whatsappStatus.ready,
      connecting: whatsappStatus.connecting,
      qr_code: whatsappStatus.qr_code,
      session: whatsappStatus.session,
      connected_at: whatsappStatus.connected_at,
      retry_count: whatsappStatus.retry_count,
      recovery_scheduled: whatsappStatus.recovery_scheduled,
      message: whatsappStatus.message,
      whatsapp: whatsappStatus,
      redis: {
        ready: redis.isReady,
        url: redis.url,
      },
      uptime_seconds: Math.floor(process.uptime()),
    });
  } catch (error) {
    logger.error({ err: error }, '/status falhou');

    res.status(500).json({
      connected: false,
      ready: false,
      connecting: false,
      qr_code: null,
      session: null,
      message: 'Erro ao obter status do WhatsApp.',
      whatsapp: { connected: false, message: 'Erro ao obter status' },
      redis: { ready: false },
    });
  }
});

// ============================================================
// RESET WHATSAPP SESSION
// ============================================================

app.post('/reset', requireServiceToken, async (req, res) => {
  try {
    const result = await whatsappService.resetSession();
    res.json(result);
  } catch (error) {
    res.status(500).json({ success: false, message: error.message });
  }
});

// ============================================================
// HEALTH
// ============================================================

app.get('/health', (req, res) => {
  const healthy = whatsappService.isReady && redis.isReady;

  res.status(healthy ? 200 : 503).json({
    status: healthy ? 'ok' : 'degraded',
    whatsapp: whatsappService.isReady ? 'connected' : 'disconnected',
    redis: redis.isReady ? 'ready' : 'unavailable',
    timestamp: new Date().toISOString(),
  });
});

// ============================================================
// METRICS (Prometheus)
// ============================================================

app.get('/metrics', async (req, res) => {
  const expected = env.metrics.token;

  if (expected) {
    const auth = req.headers.authorization || '';
    const token = auth.startsWith('Bearer ') ? auth.slice(7) : null;

    if (token !== expected) {
      return res.status(401).send('Unauthorized');
    }
  }

  try {
    res.set('Content-Type', metrics.getContentType());
    res.send(await metrics.getMetrics());
  } catch (error) {
    logger.error({ err: error }, 'Falha ao serializar métricas');
    res.status(500).send('Metrics unavailable');
  }
});

// ============================================================
// ERROR HANDLER
// ============================================================

app.use(errorHandler);

module.exports = {
  app,
  redis,
  interviewService,
  whatsappService,
  yaneIntegration,
  metrics,
  logger,
};