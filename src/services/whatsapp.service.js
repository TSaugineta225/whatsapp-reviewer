// src/services/whatsapp.service.js
//
// Ligação Baileys ao WhatsApp.
//
// SINGLE INSTANCE — o Baileys só permite uma ligação por número.
// A concorrência vem de processamento assíncrono + Redis + backend Python.

const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeInMemoryStore,
} = require('@whiskeysockets/baileys');

const qrcode = require('qrcode-terminal');
const pino = require('pino');
const fs = require('fs');
const path = require('path');

const BaseService = require('./base.service');
const RedisService = require('./redis.service');
const metrics = require('./metrics.service');

const AUTH_DIR = path.join(__dirname, '../../auth_info');

const DEFAULT_COUNTRY_CODE = '258';

const MAX_RETRIES = 5;
const INITIAL_RETRY_DELAY = 5000;
const CONNECTION_TIMEOUT = 60000;
const READ_STATUS_DEBOUNCE = 5000;

// Rate limit de saída (WhatsApp começa a bloquear acima de ~80 msg/s)
const OUTBOUND_RATE_LIMIT = 30;
const RATE_LIMIT_RETRY_MS = 200;
const RATE_LIMIT_MAX_WAIT_MS = 5000;

const STATUS_BROADCAST = 'status@broadcast';
const WHATSAPP_SUFFIX = '@s.whatsapp.net';
const GROUP_SUFFIX = '@g.us';
const LID_SUFFIX = '@lid';

class WhatsAppService extends BaseService {
  constructor(interviewService, redis = null) {
    super();

    if (!interviewService) {
      throw new Error(
        'WhatsAppService requer uma instância de interviewService.'
      );
    }

    this.interviewService = interviewService;
    this.redis = redis || interviewService.redis || new RedisService();

    // Estado da conexão
    this.socket = null;
    this.store = null;
    this.isReady = false;
    this.isConnecting = false;
    this.isShuttingDown = false;
    this.retryCount = 0;
    this.retryTimer = null;
    this.connectedAt = null;

    // QR Code
    this.qrCode = null;

    // Configuração
    this.defaultCountryCode =
      process.env.DEFAULT_COUNTRY_CODE || DEFAULT_COUNTRY_CODE;

    this.lastReadTimestamps = new Map();
    this.logger = pino({ level: 'silent' });

    this.ensureAuthDirectory();
  }

  // ============================================================
  // CONFIGURAÇÃO
  // ============================================================

  ensureAuthDirectory() {
    try {
      if (!fs.existsSync(AUTH_DIR)) {
        fs.mkdirSync(AUTH_DIR, { recursive: true });
      }
    } catch (error) {
      this.logError(
        'Não foi possível criar o diretório de autenticação.',
        error
      );
      throw error;
    }
  }

  // ============================================================
  // NORMALIZAÇÃO
  // ============================================================

  normalizePhone(phone) {
    if (!phone) return '';

    let value = String(phone)
      .split('@')[0]
      .split(':')[0]
      .replace(/\D/g, '');

    if (!value) return '';

    if (value.length === 9) {
      value = `${this.defaultCountryCode}${value}`;
    } else if (value.length === 10 && value.startsWith('0')) {
      value = `${this.defaultCountryCode}${value.slice(1)}`;
    }

    return value;
  }

  getChatId(to) {
    if (!to) return '';

    const value = String(to).trim();

    if (
      value.endsWith(GROUP_SUFFIX) ||
      value.endsWith(WHATSAPP_SUFFIX) ||
      value.endsWith(LID_SUFFIX)
    ) {
      return value;
    }

    const phone = this.normalizePhone(value);
    if (!phone) return '';

    return `${phone}${WHATSAPP_SUFFIX}`;
  }

  formatLogRecipient(chatId) {
    if (!chatId) return '';
    if (chatId.endsWith(WHATSAPP_SUFFIX)) {
      return `+${chatId.split('@')[0]}`;
    }
    return chatId;
  }

  // ============================================================
  // INICIALIZAÇÃO
  // ============================================================

  async initialize() {
    if (this.isConnecting || this.isShuttingDown) {
      this.log('Conexão já em curso ou a encerrar.');
      return;
    }

    this.clearRetryTimer();

    this.isConnecting = true;
    this.isReady = false;

    this.log('A iniciar conexão WhatsApp via Baileys...');

    try {
      this.ensureAuthDirectory();

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
        defaultQueryTimeoutMs: CONNECTION_TIMEOUT,
      });

      this.store = makeInMemoryStore({ logger: this.logger });
      this.store.bind(this.socket.ev);

      this.setupEventHandlers(saveCreds);

      this.log('Socket WhatsApp criado. Aguardando ligação...');
    } catch (error) {
      this.isConnecting = false;
      this.isReady = false;

      this.logError('Erro ao inicializar WhatsApp.', error);
      this.scheduleReconnect();
    }
  }

  setupEventHandlers(saveCreds) {
    if (!this.socket) {
      throw new Error('Socket WhatsApp não inicializado.');
    }

    this.socket.ev.on('connection.update', (update) =>
      this.handleConnectionUpdate(update)
    );

    this.socket.ev.on('creds.update', saveCreds);

    this.socket.ev.on('messages.upsert', (event) =>
      this.handleMessagesUpsert(event)
    );

    this.socket.ev.on('messages.update', (updates) =>
      this.handleMessagesUpdate(updates)
    );

    this.socket.ev.on('presence.update', () => {});
  }

  // ============================================================
  // CONNECTION UPDATE
  // ============================================================

  async handleConnectionUpdate(update) {
    const { connection, lastDisconnect, qr } = update;

    if (qr) this.handleQRCode(qr);

    if (connection === 'open') {
      this.handleConnectionOpen();
      return;
    }

    if (connection === 'close') {
      await this.handleConnectionClose(lastDisconnect);
    }
  }

  handleQRCode(qr) {
    this.qrCode = qr;
    this.isReady = false;

    console.log('\n[QR CODE] Digitalize com o seu WhatsApp:\n');
    qrcode.generate(qr, { small: true });
  }

  handleConnectionOpen() {
    this.isReady = true;
    this.isConnecting = false;
    this.qrCode = null;
    this.retryCount = 0;
    this.connectedAt = Date.now();

    this.clearRetryTimer();

    metrics.whatsappConnected.set(1);

    const user = this.socket?.user;
    this.log('WhatsApp conectado e pronto para responder.');

    if (user) {
      this.log(
        `Sessão: ${user.name || 'WhatsApp'} (${user.id || 'desconhecido'})`
      );
    }
  }

  async handleConnectionClose(lastDisconnect) {
    const statusCode = this.getDisconnectStatusCode(lastDisconnect);
    const loggedOut = statusCode === DisconnectReason.loggedOut;

    this.isReady = false;
    this.isConnecting = false;
    this.qrCode = null;
    this.connectedAt = null;

    metrics.whatsappConnected.set(0);

    this.log(
      `WhatsApp desconectado. Código: ${statusCode || 'desconhecido'}`
    );

    if (loggedOut) {
      this.log('Sessão encerrada pelo WhatsApp. Limpando credenciais.');
      this.clearAuthDirectory();
      return;
    }

    if (this.isShuttingDown) return;

    this.scheduleReconnect();
  }

  getDisconnectStatusCode(lastDisconnect) {
    return (
      lastDisconnect?.error?.output?.statusCode ||
      lastDisconnect?.error?.statusCode ||
      null
    );
  }

  // ============================================================
  // RECONEXÃO
  // ============================================================

  scheduleReconnect() {
    if (this.retryTimer || this.isShuttingDown) return;

    if (this.retryCount >= MAX_RETRIES) {
      this.logError(
        `Número máximo de tentativas atingido (${MAX_RETRIES}).`
      );
      return;
    }

    this.retryCount += 1;

    const delay =
      this.retryCount === 1
        ? INITIAL_RETRY_DELAY
        : INITIAL_RETRY_DELAY * this.retryCount;

    this.log(
      `Reconexão ${this.retryCount}/${MAX_RETRIES} em ${Math.round(
        delay / 1000
      )}s...`
    );

    this.retryTimer = setTimeout(async () => {
      this.retryTimer = null;

      try {
        await this.initialize();
      } catch (error) {
        this.logError('Falha na tentativa de reconexão.', error);
      }
    }, delay);
  }

  clearRetryTimer() {
    if (!this.retryTimer) return;
    clearTimeout(this.retryTimer);
    this.retryTimer = null;
  }

  // ============================================================
  // MENSAGENS RECEBIDAS
  // ============================================================

  async handleMessagesUpsert(event) {
    try {
      const messages = event?.messages || [];

      for (const message of messages) {
        // Cada mensagem processa-se independentemente.
        // Serialização por telefone é feita no InterviewService (Redis lock).
        this.processIncomingMessage(message).catch((error) => {
          this.logError('Erro no processamento assíncrono.', error);
        });
      }
    } catch (error) {
      this.logError('Erro ao iterar lote de mensagens.', error);
    }
  }

  async processIncomingMessage(msg) {
    if (!msg?.message) return;
    if (msg.key?.fromMe) return;

    const from = msg.key?.remoteJid;
    if (!from || from === STATUS_BROADCAST) return;

    const parsed = this.extractMessageContent(msg);
    if (!parsed.text && !parsed.isButtonClick) return;

    // Métrica de recepção
    const type = parsed.isButtonClick
      ? 'button'
      : parsed.text
        ? 'text'
        : 'other';
    metrics.messagesReceived.inc({ type });

    const preview =
      parsed.text.length > 100
        ? `${parsed.text.substring(0, 100)}...`
        : parsed.text;

    this.log(`Mensagem recebida de [${from}]: "${preview}"`);

    await this.handleMessage(
      from,
      parsed.text,
      parsed.isButtonClick,
      msg.key?.id || null
    );
  }

  // ============================================================
  // EXTRAÇÃO DO CONTEÚDO
  // ============================================================

  extractMessageContent(msg) {
    const message = msg.message;

    if (message.conversation) {
      return {
        text: message.conversation.trim(),
        isButtonClick: false,
      };
    }

    if (message.extendedTextMessage?.text) {
      return {
        text: message.extendedTextMessage.text.trim(),
        isButtonClick: false,
      };
    }

    if (message.interactiveResponseMessage) {
      return this.extractInteractiveResponse(
        message.interactiveResponseMessage
      );
    }

    if (message.buttonsResponseMessage) {
      const response = message.buttonsResponseMessage;

      return {
        text: (
          response.selectedButtonId ||
          response.displayText ||
          ''
        ).trim(),
        isButtonClick: true,
      };
    }

    return { text: '', isButtonClick: false };
  }

  extractInteractiveResponse(response) {
    const nativeFlow = response?.nativeFlowResponseMessage;

    if (nativeFlow) {
      return {
        text: (nativeFlow.text || nativeFlow.paramsJson || '').trim(),
        isButtonClick: true,
      };
    }

    const selectedButton = response?.selectedButton;

    if (selectedButton) {
      return {
        text: (
          selectedButton.displayText ||
          selectedButton.id ||
          ''
        ).trim(),
        isButtonClick: true,
      };
    }

    return { text: '', isButtonClick: false };
  }

  // ============================================================
  // PROCESSAMENTO
  // ============================================================

  async handleMessage(from, text, isButton = false, messageId = null) {
    if (!from) return;

    const normalizedText = String(text || '').trim();
    if (!normalizedText) return;

    if (this.isResetCommand(normalizedText)) {
      await this.interviewService.forgetInterview(from);

      await this.sendMessage(
        from,
        'Sessão reiniciada. Envie uma nova mensagem quando estiver pronto.'
      );

      return;
    }

    try {
      await this.interviewService.handleIncomingMessage(
        from,
        normalizedText,
        { messageId, isButton }
      );
    } catch (error) {
      this.logError(`Erro ao processar mensagem de ${from}.`, error);
    }
  }

  isResetCommand(text) {
    const normalized = String(text || '').toLowerCase().trim();

    return (
      normalized === '!reset' ||
      normalized === '!recomencar' ||
      normalized === '!recomeçar'
    );
  }

  // ============================================================
  // STATUS DE LEITURA
  // ============================================================

  async handleMessagesUpdate(updates = []) {
    for (const update of updates) {
      if (update?.status !== 'read') continue;

      const key = update.key;
      if (!key?.remoteJid || !key?.id) continue;

      await this.handleReadReceipt(key);
    }
  }

  async handleReadReceipt(key) {
    const from = key.remoteJid;
    const messageId = key.id;

    if (this.isReadReceiptDebounced(from)) return;

    this.lastReadTimestamps.set(from, Date.now());

    this.log(`Mensagem ${messageId} lida por ${from}`);

    await this.notifyBackendMessageRead({
      phone: from,
      messageId,
    });
  }

  isReadReceiptDebounced(phone) {
    const lastRead = this.lastReadTimestamps.get(phone);
    if (!lastRead) return false;
    return Date.now() - lastRead < READ_STATUS_DEBOUNCE;
  }

  async notifyBackendMessageRead({ phone, messageId }) {
    try {
      await this.interviewService.yane.sendMessageStatus({
        phone,
        messageId,
        status: 'read',
      });
    } catch (error) {
      this.logError('Erro ao enviar status de leitura.', error);
    }
  }

  // ============================================================
  // ENVIO DE MENSAGEM (com rate limit + métricas)
  // ============================================================

  async sendMessage(to, message) {
    if (!message) {
      this.logError('Tentativa de enviar mensagem vazia.');
      return false;
    }

    const chatId = this.prepareChatId(to);
    if (!chatId) return false;

    if (!this.ensureReady()) return false;

    const allowed = await this.waitForRateLimit();
    if (!allowed) {
      metrics.messagesSent.inc({ status: 'rate_limited' });
      metrics.rateLimitHits.inc({ bucket: 'out' });
      this.logError(
        'Rate limit excedido e tempo de espera máximo atingido.'
      );
      return false;
    }

    try {
      await metrics.time(
        metrics.messageSendDuration,
        {},
        () =>
          this.socket.sendMessage(chatId, {
            text: String(message),
          })
      );

      metrics.messagesSent.inc({ status: 'success' });

      this.log(`Mensagem enviada para ${this.formatLogRecipient(chatId)}`);
      return true;
    } catch (error) {
      metrics.messagesSent.inc({ status: 'failed' });
      metrics.errorsTotal.inc({ subsystem: 'whatsapp' });

      this.logError(`Erro ao enviar mensagem para ${to}.`, error);
      return false;
    }
  }

  async waitForRateLimit() {
    const start = Date.now();

    while (Date.now() - start < RATE_LIMIT_MAX_WAIT_MS) {
      const allowed = await this.redis.allowRate(
        'out',
        OUTBOUND_RATE_LIMIT
      );

      if (allowed) return true;

      await this.delay(RATE_LIMIT_RETRY_MS);
    }

    return false;
  }

  // ============================================================
  // MENSAGEM INTERATIVA
  // ============================================================

  async sendInteractiveMessage(to, title, body, buttons = []) {
    const chatId = this.prepareChatId(to);
    if (!chatId) return false;
    if (!this.ensureReady()) return false;
    if (!body) {
      this.logError('Mensagem interativa sem conteúdo.');
      return false;
    }

    const allowed = await this.waitForRateLimit();
    if (!allowed) return false;

    try {
      const interactiveButtons = this.buildInteractiveButtons(buttons);
      if (!interactiveButtons.length) {
        return this.sendMessage(to, body);
      }

      await this.socket.sendMessage(chatId, {
        text: title ? `*${title}*\n\n${body}` : String(body),
        footer: 'Yane ATS - Recrutamento Inteligente',
        buttons: interactiveButtons.map((btn) => ({
          buttonId: btn.id,
          buttonText: { displayText: btn.text },
          type: 1,
        })),
        headerType: 1,
      });

      this.log(
        `Mensagem interativa enviada para ${this.formatLogRecipient(chatId)}`
      );
      return true;
    } catch (error) {
      this.logError(
        `Erro na mensagem interativa para ${to}. Fallback em texto.`,
        error
      );

      const fallbackText = `${title ? `*${title}*\n\n` : ''}${body}`;
      return this.sendMessage(to, fallbackText);
    }
  }

  buildInteractiveButtons(buttons) {
    if (!Array.isArray(buttons)) return [];

    return buttons
      .filter(Boolean)
      .map((button, index) => ({
        id: button.id || `btn_${index + 1}`,
        text: button.label || button.text || `Opção ${index + 1}`,
      }));
  }

  // ============================================================
  // PRESENCE
  // ============================================================

  async sendPresenceUpdate(presence, to) {
    const chatId = this.prepareChatId(to);
    if (!chatId) return;

    if (!this.socket || !this.isReady) return;

    try {
      await this.socket.sendPresenceUpdate(presence, chatId);
    } catch (_) {
      // Presence é best-effort.
    }
  }

  // ============================================================
  // ESTADO
  // ============================================================

  getStatus() {
    const connected = Boolean(this.isReady && this.socket?.user?.id);

    return {
      connected,
      qr_code: this.qrCode,
      session: this.socket?.user?.id?.split(':')[0] || null,
      message: connected
        ? 'WhatsApp conectado e pronto'
        : this.qrCode
          ? 'Aguardando escaneamento do QR Code'
          : this.isConnecting
            ? 'A iniciar sessão...'
            : 'WhatsApp desconectado',
    };
  }

  ensureReady() {
    if (!this.isReady || !this.socket) {
      this.log('WhatsApp não está pronto para enviar mensagens.');
      return false;
    }
    return true;
  }

  prepareChatId(to) {
    const chatId = this.getChatId(to);
    if (!chatId) {
      this.logError(`Número/JID inválido: ${to}`);
      return '';
    }
    return chatId;
  }

  // ============================================================
  // RESET DA SESSÃO
  // ============================================================

  async resetSession() {
    if (this.isConnecting) {
      return {
        success: false,
        message: 'Já existe uma operação de conexão em andamento.',
      };
    }

    this.isConnecting = true;

    try {
      this.clearRetryTimer();
      await this.closeSocket();

      this.resetConnectionState();
      this.clearAuthDirectory();
      this.ensureAuthDirectory();

      await this.delay(2000);

      this.isConnecting = false;
      await this.initialize();

      return {
        success: true,
        message: 'Sessão reiniciada. Aguarde o novo QR Code.',
      };
    } catch (error) {
      this.isConnecting = false;
      this.logError('Erro ao reiniciar sessão do WhatsApp.', error);

      return { success: false, message: error.message };
    }
  }

  async closeSocket() {
    if (!this.socket) return;

    try {
      if (this.socket.ws) {
        this.socket.ws.close();
      }
    } catch (error) {
      this.logError('Erro ao fechar socket WhatsApp.', error);
    } finally {
      this.socket = null;
      this.store = null;
    }
  }

  resetConnectionState() {
    this.isReady = false;
    this.qrCode = null;
    this.retryCount = 0;
    this.connectedAt = null;
    this.lastReadTimestamps.clear();
  }

  clearAuthDirectory() {
    try {
      if (fs.existsSync(AUTH_DIR)) {
        fs.rmSync(AUTH_DIR, { recursive: true, force: true });
      }
    } catch (error) {
      this.logError('Erro ao limpar diretório de autenticação.', error);
    }
  }

  // ============================================================
  // GRACEFUL SHUTDOWN
  // ============================================================

  async shutdown() {
    if (this.isShuttingDown) return;

    this.isShuttingDown = true;
    this.log('Shutdown iniciado.');

    this.clearRetryTimer();

    try {
      if (this.socket) {
        await this.socket.end(undefined);
      }
    } catch (error) {
      this.logError('Erro ao fechar socket no shutdown.', error);
    }

    this.log('Shutdown do WhatsApp concluído.');
  }

  // ============================================================
  // UTILITÁRIOS
  // ============================================================

  delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  getClient() {
    return this.socket;
  }

  log(message) {
    console.log(`[WHATSAPP] ${message}`);
  }

  logError(message, error = null) {
    if (error) {
      console.error(
        `[WHATSAPP] ${message}`,
        error?.stack || error?.message || error
      );
      return;
    }
    console.error(`[WHATSAPP] ${message}`);
  }
}

module.exports = WhatsAppService;