// src/app.js
const express = require('express');
const dotenv = require('dotenv');

const errorHandler = require('./middleware/error.middleware');
const routes = require('./routes');

const RedisService = require('./services/redis.service');
const InterviewService = require('./services/interview.service');
const WhatsAppService = require('./services/whatsapp.service');
const YaneIntegrationService = require('./services/yane-integration.service');
const metrics = require('./services/metrics.service');

dotenv.config();

// ============================================================
// SERVIÇOS
// ============================================================
//
// Ordem importa:
//   1. Redis              → infraestrutura partilhada
//   2. InterviewService   → usa Redis
//   3. WhatsAppService    → usa InterviewService + Redis
//   4. YaneIntegration    → HTTP puro
//   5. Metrics            → instrumenta todos

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
// APP
// ============================================================

const app = express();

app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true }));

app.use('/', routes);

// ============================================================
// START INTERVIEW — chamado pelo backend Python após invite
// ============================================================

app.post('/start-interview', async (req, res) => {
  try {
    const result = await interviewService.startInterview(req.body);

    if (!result.success) {
      return res.status(400).json(result);
    }

    return res.json(result);
  } catch (error) {
    console.error('[BOT] /start-interview falhou:', error);
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

app.post('/send-message', async (req, res) => {
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

    console.log(`[BOT] A enviar mensagem para ${to}: "${preview}"`);

    const sent = await whatsappService.sendMessage(to, message);

    if (!sent) {
      return res.status(500).json({
        success: false,
        message: 'Falha ao enviar mensagem.',
      });
    }

    return res.json({ success: true, to });
  } catch (error) {
    console.error('[BOT] /send-message falhou:', error);
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

app.get('/status', (req, res) => {
  try {
    res.json({
      whatsapp: whatsappService.getStatus(),
      redis: {
        ready: redis.isReady,
        url: redis.url,
      },
      uptime_seconds: Math.floor(process.uptime()),
    });
  } catch (error) {
    res.status(500).json({
      whatsapp: { connected: false, message: 'Erro ao obter status' },
      redis: { ready: false },
    });
  }
});

// ============================================================
// RESET WHATSAPP SESSION
// ============================================================

app.post('/reset', async (req, res) => {
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
  const expected = process.env.METRICS_TOKEN;

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
    console.error('[METRICS] Falha ao serializar:', error);
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
};