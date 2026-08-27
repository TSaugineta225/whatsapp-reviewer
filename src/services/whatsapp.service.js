// src/services/whatsapp.service.js
const { default: makeWASocket, useMultiFileAuthState, DisconnectReason, fetchLatestBaileysVersion, makeInMemoryStore } = require('@whiskeysockets/baileys');
const { Boom } = require('@hapi/boom');
const qrcode = require('qrcode-terminal');
const pino = require('pino');
const fs = require('fs');
const path = require('path');
const BaseService = require('./base.service');

const AUTH_DIR = path.join(__dirname, '../../auth_info');

// ============================================================
// CLASSE PRINCIPAL
// ============================================================

class WhatsAppService extends BaseService {
  constructor(interviewService) {
    super();
    this.interviewService = interviewService;
    this.socket = null;
    this.isReady = false;
    this.qrCode = null;
    this.store = null;
    this.retryCount = 0;
    this.maxRetries = 5;
    this.isConnecting = false;
    this.yaneApiUrl = process.env.YANE_API_URL || 'http://localhost:8000/api';
    this.lastReadTimestamps = {};

    if (!fs.existsSync(AUTH_DIR)) {
      fs.mkdirSync(AUTH_DIR, { recursive: true });
    }
    this.logger = pino({ level: 'silent' });
  }

  // ============================================================
  // INICIALIZAÇÃO
  // ============================================================

  async initialize() {
    if (this.isConnecting) return;
    this.isConnecting = true;
    console.log('[BOT] Iniciando conexão com WhatsApp (Baileys)...');

    try {
      const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
      const { version } = await fetchLatestBaileysVersion();

      this.socket = makeWASocket({
        version,
        auth: state,
        logger: this.logger,
        browser: ['Yane ATS', 'Chrome', '120.0.0.0'],
        printQRInTerminal: false,
        syncFullHistory: false,
        markOnlineOnConnect: true,
        generateHighQualityLinkPreview: false,
        defaultQueryTimeoutMs: 60000,
      });

      this.store = makeInMemoryStore({ logger: this.logger });
      this.store.bind(this.socket.ev);
      this.setupEventHandlers(saveCreds);

      await this.socket.waitForConnectionUpdate(
        (update) => update.connection === 'open' || update.connection === 'close'
      );

      console.log('[BOT] Conexão inicializada com sucesso');
      this.isConnecting = false;
    } catch (error) {
      console.error('[BOT] Erro ao inicializar:', error.message);
      this.isConnecting = false;
      this.isReady = false;
      if (this.retryCount < this.maxRetries) {
        this.retryCount++;
        console.log(`[BOT] Tentativa ${this.retryCount}/${this.maxRetries} em 10s...`);
        setTimeout(() => this.initialize(), 10000);
      }
    }
  }

  // ============================================================
  // EVENTOS
  // ============================================================

  setupEventHandlers(saveCreds) {
    // 1. Connection updates (QR, open, close)
    this.socket.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect, qr } = update;

      if (qr) {
        console.log('[QR CODE] Digitalize com o seu WhatsApp:');
        qrcode.generate(qr, { small: true });
        this.qrCode = qr;
        global.qrCode = qr;
        this.isReady = false;
      }

      if (connection === 'open') {
        console.log('[OK] WhatsApp conectado e pronto a responder!');
        this.isReady = true;
        this.qrCode = null;
        global.qrCode = null;
        this.retryCount = 0;
        this.isConnecting = false;
        console.log(`[BOT] Conectado como: ${this.socket.user?.name} (${this.socket.user?.id})`);
      }

      if (connection === 'close') {
        const statusCode = (lastDisconnect?.error)?.output?.statusCode;
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        console.log(`[DESCONECTADO] Código: ${statusCode} - Reconectar: ${shouldReconnect}`);
        this.isReady = false;
        this.qrCode = null;
        global.qrCode = null;

        if (shouldReconnect && this.retryCount < this.maxRetries) {
          this.retryCount++;
          console.log(`[BOT] Tentativa ${this.retryCount}/${this.maxRetries} em 5s...`);
          setTimeout(() => this.initialize(), 5000);
        } else if (statusCode === DisconnectReason.loggedOut) {
          console.log('[BOT] Sessão expirada. Remova a pasta auth_info e reinicie.');
          if (fs.existsSync(AUTH_DIR)) fs.rmSync(AUTH_DIR, { recursive: true, force: true });
        }
      }
    });

    // 2. Credentials update
    this.socket.ev.on('creds.update', saveCreds);

    // ============================================================
    // 3. MENSAGENS (recebimento e interações com botões)
    // ============================================================
    this.socket.ev.on('messages.upsert', async (m) => {
      try {
        const msg = m.messages[0];
        if (!msg || !msg.message) return;
        if (msg.key.fromMe) return;
        if (msg.key.remoteJid === 'status@broadcast') return;

        const from = msg.key.remoteJid;
        let text = '';
        let isButtonClick = false;

        // 3A. Mensagem de texto normal
        if (msg.message.conversation) {
          text = msg.message.conversation;
        } else if (msg.message.extendedTextMessage?.text) {
          text = msg.message.extendedTextMessage.text;
        }
        // 3B. Resposta a botões interativos (Baileys)
        else if (msg.message.interactiveResponseMessage) {
          const response = msg.message.interactiveResponseMessage;
          if (response.nativeFlowResponseMessage) {
            text = response.nativeFlowResponseMessage?.text || '';
            isButtonClick = true;
          } else if (response.selectedButton) {
            text = response.selectedButton?.displayText || '';
            isButtonClick = true;
          }
        }
        // 3C. Resposta a botões (legado)
        else if (msg.message.buttonsResponseMessage) {
          const btn = msg.message.buttonsResponseMessage;
          text = btn.selectedButtonId || btn.displayText || '';
          isButtonClick = true;
        }

        if (!text && !isButtonClick) {
          return;
        }

        console.log(`[MENSAGEM] De [${from}]: "${text.substring(0, 100)}${text.length > 100 ? '...' : ''}"`);

        await this.handleMessage(from, text, isButtonClick);

      } catch (error) {
        console.error('[BOT] Erro ao processar mensagem:', error.message);
      }
    });

    // ============================================================
    // 4. STATUS DE LEITURA (Read Receipts)
    // ============================================================
    this.socket.ev.on('messages.update', async (updates) => {
      for (const update of updates) {
        if (update.status === 'read' && update.key) {
          const from = update.key.remoteJid;
          const messageId = update.key.id;
          
          // Evitar duplicados
          const now = Date.now();
          if (this.lastReadTimestamps[from] && (now - this.lastReadTimestamps[from] < 5000)) {
            continue;
          }
          this.lastReadTimestamps[from] = now;

          console.log(`[LEITURA] Mensagem ${messageId} lida por ${from}`);

          // Notificar a Yane (backend) sobre a leitura
          try {
            const response = await fetch(`${this.yaneApiUrl}/webhooks/message-status`, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                phone: from,
                message_id: messageId,
                status: 'read',
                timestamp: new Date().toISOString()
              })
            });
            if (!response.ok) {
              console.error('[LEITURA] Erro ao notificar backend:', response.status);
            }
          } catch (err) {
            console.error('[LEITURA] Erro ao enviar status:', err.message);
          }
        }
      }
    });

    // 5. Presença (opcional)
    this.socket.ev.on('presence.update', () => {});
  }

  // ============================================================
  // ENVIAR MENSAGEM COM BOTÕES INTERATIVOS (NOVO)
  // ============================================================

  async sendInteractiveMessage(to, title, body, buttons = []) {
    try {
      if (!this.isReady || !this.socket) {
        console.log('[BOT] WhatsApp não está pronto.');
        return false;
      }

      const chatId = to.includes('@') ? to : `${to}@s.whatsapp.net`;

      // Estrutura padrão para botões interativos (Baileys)
      const interactiveButtons = buttons.map((btn, idx) => ({
        id: btn.id || `btn_${idx}`,
        text: btn.text || btn.label || `Opção ${idx + 1}`,
        type: 'reply'
      }));

      await this.socket.sendMessage(chatId, {
        interactive: {
          header: title ? { title: title } : undefined,
          body: { text: body },
          footer: { text: 'Yane ATS - Recrutamento Inteligente' },
          action: {
            buttons: interactiveButtons
          }
        }
      });

      console.log(`[BOT] Mensagem com botões enviada para ${to}`);
      return true;
    } catch (error) {
      console.error(`[BOT] Erro ao enviar mensagem com botões para ${to}:`, error.message);
      return false;
    }
  }

  // ============================================================
  // ENVIAR MENSAGEM DE TEXTO SIMPLES
  // ============================================================

  async sendMessage(to, message) {
    try {
      if (!this.isReady || !this.socket) {
        console.log('[BOT] WhatsApp não está pronto.');
        return false;
      }
      const chatId = to.includes('@') ? to : `${to}@s.whatsapp.net`;
      await this.socket.sendMessage(chatId, { text: message });
      console.log(`[BOT] Mensagem enviada para ${to}`);
      return true;
    } catch (error) {
      console.error(`[BOT] Erro ao enviar mensagem para ${to}:`, error.message);
      return false;
    }
  }

  // ============================================================
  // PROCESSAMENTO DE MENSAGENS
  // ============================================================

  async handleMessage(from, text, isButton = false) {
    try {
      const userId = from;
      const hasActiveSession = !!this.interviewService.getSession(userId);

      // Comando de reset
      if (text.toLowerCase().trim() === '!reset' || text.toLowerCase().trim() === '!recomencar') {
        this.interviewService.endSession(userId);
        await this.sendMessage(from, "Sessão reiniciada! Envie START ou OLÁ para começar novamente.");
        return;
      }

      // Se for clique em botão "Iniciar" ou texto de trigger
      const isStart = this.isStartTrigger(text) || text === 'iniciar' || text === 'start' || isButton;

      if (!hasActiveSession && isStart) {
        console.log(`[SESSAO] A iniciar entrevista para ${userId}...`);
        const welcome = await this.interviewService.startInterview(userId);
        await this.sendMessage(from, welcome);
        return;
      }

      if (hasActiveSession) {
        console.log(`[SESSAO] A processar resposta para ${userId}...`);
        const response = await this.interviewService.handleResponse(userId, text);
        if (response) {
          await this.sendMessage(from, response);
        }
        return;
      }

      // Se não tiver sessão e não for trigger, ignorar
      console.log(`[BOT] Ignorando mensagem de ${userId} (sem sessão)`);

    } catch (error) {
      console.error('[BOT] Erro ao processar mensagem:', error.message);
      await this.sendMessage(from, "Desculpe, ocorreu um erro. Tente novamente dentro de momentos.");
    }
  }

  // ============================================================
  // UTILITÁRIOS
  // ============================================================

  isStartTrigger(text) {
    if (!text) return false;
    const cleanText = text.toLowerCase().trim();
    const triggers = [
      /^sim\b/i, /^oi\b/i, /^ol[aá]\b/i, /^iniciar\b/i, /^start\b/i,
      /^menu\b/i, /^entrevista\b/i, /^bom dia\b/i, /^boa tarde\b/i,
      /^boa noite\b/i, /^vaga\b/i, /^gostaria/i, /^quero/i,
      /^pode começar/i, /^começar/i,
    ];
    return triggers.some(regex => regex.test(cleanText));
  }

  getStatus() {
    return {
      connected: this.isReady && this.socket?.user?.id,
      qr_code: this.qrCode, // CORRIGIDO PARA snake_case
      session: this.socket?.user?.id?.split(':')[0] || null,
      message: this.isReady ? 'WhatsApp conectado e pronto' : this.qrCode ? 'Aguardando escaneamento do QR Code' : 'A iniciar sessão...'
    };
  }

  async resetSession() {
    try {
      this.isConnecting = true;
      if (this.socket) {
        try { await this.socket.ws.close(); } catch (_) {}
        this.socket = null;
      }
      this.isReady = false;
      this.qrCode = null;
      global.qrCode = null;
      this.retryCount = 0;
      if (fs.existsSync(AUTH_DIR)) {
        fs.rmSync(AUTH_DIR, { recursive: true, force: true });
        fs.mkdirSync(AUTH_DIR, { recursive: true });
      }
      await this.delay(2000);
      await this.initialize();
      this.isConnecting = false;
      return { success: true, message: 'Sessão reiniciada' };
    } catch (error) {
      console.error('[WHATSAPP] Erro ao reiniciar sessão:', error.message);
      this.isConnecting = false;
      return { success: false, message: error.message };
    }
  }

  delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
  getClient() { return this.socket; }
}

module.exports = WhatsAppService;