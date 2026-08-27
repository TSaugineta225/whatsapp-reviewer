// src/app.js
const express = require("express");
const dotenv = require('dotenv');
const errorHandler = require('./middleware/error.middleware');
const routes = require('./routes');
const InterviewService = require('./services/interview.service');
const WhatsAppService = require('./services/whatsapp.service');
const YaneIntegrationService = require('./services/yane-integration.service');

dotenv.config();

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const interviewService = new InterviewService();
const whatsappService = new WhatsAppService(interviewService);
const yaneIntegration = new YaneIntegrationService();

global.whatsappService = whatsappService;
global.yaneIntegration = yaneIntegration;
global.interviewService = interviewService;

app.use('/', routes);

// ============================================================
// ENDPOINT PARA A YANE INICIAR A ENTREVISTA
// ============================================================
app.post('/start-interview', async (req, res) => {
  try {
    const { phone, jobTitle, candidateId, interviewId, candidateName, initialMessage, recruiterId, holdTransactionId, company, jobVacancy } = req.body;

    if (!phone) {
      return res.status(400).json({ success: false, message: 'Telefone nao fornecido' });
    }

    if (interviewService.getSession(phone)) {
      return res.status(400).json({
        success: false,
        message: 'Ja existe uma entrevista em andamento para este numero'
      });
    }

    const welcome = await interviewService.startInterview(phone, jobTitle, company || null, jobVacancy || null);

    const session = interviewService.getSession(phone);
    if (session) {
      session.interviewId = interviewId;
      session.holdTransactionId = holdTransactionId;
      session.recruiterId = recruiterId;
    }

    if (initialMessage) {
      await whatsappService.sendMessage(phone, initialMessage);
      await new Promise(resolve => setTimeout(resolve, 1500));
    }

    res.json({
      success: true,
      message: 'Entrevista iniciada com sucesso',
      phone
    });
  } catch (error) {
    console.error('Erro ao iniciar entrevista:', error);
    res.status(500).json({ success: false, message: 'Erro ao iniciar entrevista', error: error.message });
  }
});

// ============================================================
// ENDPOINT PARA A YANE ENVIAR MENSAGENS (NOTIFICAÇÕES)
// ============================================================
app.post('/send-message', async (req, res) => {
  try {
    const { to, message } = req.body;

    if (!to) {
      return res.status(400).json({ success: false, message: 'O parâmetro "to" (telefone) é obrigatório' });
    }

    if (!message) {
      return res.status(400).json({ success: false, message: 'O parâmetro "message" é obrigatório' });
    }

    console.log(`[BOT] A enviar mensagem para ${to}: "${message.substring(0, 50)}..."`);

    const sent = await whatsappService.sendMessage(to, message);

    if (sent) {
      res.json({ success: true, message: 'Mensagem enviada com sucesso', to: to });
    } else {
      res.status(500).json({ success: false, message: 'Falha ao enviar mensagem. Verifique se o número está correto.' });
    }
  } catch (error) {
    console.error('[BOT] Erro no endpoint /send-message:', error);
    res.status(500).json({ success: false, message: 'Erro interno ao enviar mensagem', error: error.message });
  }
});

// ============================================================
// WEBHOOK PARA STATUS DE MENSAGENS (leitura, entrega)
// ============================================================
app.post('/webhook/status', async (req, res) => {
  try {
    const { phone, message_id, status, timestamp } = req.body;
    console.log(`[WEBHOOK] Status da mensagem ${message_id} para ${phone}: ${status}`);

    if (global.yaneIntegration) {
      await global.yaneIntegration.sendMessageStatus({
        phone,
        message_id,
        status,
        timestamp
      });
    }

    res.json({ success: true });
  } catch (error) {
    console.error('[WEBHOOK] Erro:', error.message);
    res.status(500).json({ success: false, message: error.message });
  }
});

// ============================================================
// ENDPOINT PARA STATUS DO WHATSAPP
// ============================================================
app.get('/status', async (req, res) => {
  try {
    const status = whatsappService.getStatus();
    res.json(status);
  } catch (error) {
    res.status(500).json({ connected: false, qr_code: null, session: null, message: 'Erro ao obter status' });
  }
});

// ============================================================
// ENDPOINT PARA REINICIAR SESSÃO
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
// HEALTH CHECK
// ============================================================
app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    whatsapp: whatsappService.isReady ? 'connected' : 'disconnected',
    sessions: interviewService.sessions.size,
    timestamp: new Date().toISOString()
  });
});

app.use(errorHandler);

module.exports = { app, whatsappService };