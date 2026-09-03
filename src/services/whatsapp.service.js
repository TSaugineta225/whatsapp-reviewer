// src/services/whatsapp.service.js

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

const AUTH_DIR = path.join(__dirname, '../../auth_info');

const DEFAULT_API_URL = 'http://localhost:8000/api';
const DEFAULT_COUNTRY_CODE = '258';

const MAX_RETRIES = 5;
const INITIAL_RETRY_DELAY = 5000;
const CONNECTION_TIMEOUT = 60000;
const READ_STATUS_DEBOUNCE = 5000;

const STATUS_BROADCAST = 'status@broadcast';
const WHATSAPP_SUFFIX = '@s.whatsapp.net';
const GROUP_SUFFIX = '@g.us';
const LID_SUFFIX = '@lid';

class WhatsAppService extends BaseService {
  constructor(interviewService) {
    super();

    if (!interviewService) {
      throw new Error('WhatsAppService requer uma instância de interviewService.');
    }

    this.interviewService = interviewService;

    // Estado da conexão
    this.socket = null;
    this.store = null;
    this.isReady = false;
    this.isConnecting = false;
    this.retryCount = 0;
    this.retryTimer = null;

    // QR Code
    this.qrCode = null;

    // Configuração
    this.yaneApiUrl = (
      process.env.YANE_API_URL || DEFAULT_API_URL
    ).replace(/\/+$/, '');

    this.defaultCountryCode =
      process.env.DEFAULT_COUNTRY_CODE || DEFAULT_COUNTRY_CODE;

    // Controle de eventos
    this.lastReadTimestamps = new Map();

    // Logger silencioso para Baileys
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
      this.logError('Não foi possível criar o diretório de autenticação.', error);
      throw error;
    }
  }

  // ============================================================
  // NORMALIZAÇÃO DE TELEFONE E JID
  // ============================================================

  normalizePhone(phone) {
    if (!phone) {
      return '';
    }

    let clean = String(phone)
      .split('@')[0]
      .split(':')[0]
      .replace(/\D/g, '');

    if (!clean) {
      return '';
    }

    // Exemplo: 841234567 -> 258841234567
    if (clean.length === 9) {
      clean = `${this.defaultCountryCode}${clean}`;
    }
    // Exemplo: 0841234567 -> 258841234567
    else if (clean.length === 10 && clean.startsWith('0')) {
      clean = `${this.defaultCountryCode}${clean.slice(1)}`;
    }

    return clean;
  }

  getChatId(to) {
    if (!to) {
      return '';
    }

    const value = String(to).trim();

    // Preserva JIDs nativos do WhatsApp (@s.whatsapp.net, @g.us e @lid)
    if (
      value.endsWith(GROUP_SUFFIX) ||
      value.endsWith(WHATSAPP_SUFFIX) ||
      value.endsWith(LID_SUFFIX)
    ) {
      return value;
    }

    // Sanitiza e normaliza números de telefone convencionais
    const phone = this.normalizePhone(value);

    if (!phone) {
      return '';
    }

    return `${phone}${WHATSAPP_SUFFIX}`;
  }

  formatLogRecipient(chatId) {
    if (!chatId) {
      return '';
    }

    if (chatId.endsWith(WHATSAPP_SUFFIX)) {
      return `+${chatId.split('@')[0]}`;
    }

    return chatId;
  }

  // ============================================================
  // INICIALIZAÇÃO
  // ============================================================

  async initialize() {
    if (this.isConnecting) {
      this.log('Conexão já está em processo de inicialização.');
      return;
    }

    this.clearRetryTimer();

    this.isConnecting = true;
    this.isReady = false;

    this.log('Iniciando conexão com WhatsApp via Baileys...');

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

      this.store = makeInMemoryStore({
        logger: this.logger,
      });

      this.store.bind(this.socket.ev);

      this.setupEventHandlers(saveCreds);

      this.log('Socket WhatsApp criado. Aguardando conexão...');
    } catch (error) {
      this.isConnecting = false;
      this.isReady = false;

      this.logError('Erro ao inicializar WhatsApp.', error);
      this.scheduleReconnect();
    }
  }

  // ============================================================
  // EVENTOS
  // ============================================================

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

    if (qr) {
      this.handleQRCode(qr);
    }

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

    qrcode.generate(qr, {
      small: true,
    });
  }

  handleConnectionOpen() {
    this.isReady = true;
    this.isConnecting = false;
    this.qrCode = null;
    this.retryCount = 0;

    this.clearRetryTimer();

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

    this.log(`WhatsApp desconectado. Código: ${statusCode || 'desconhecido'}`);

    if (loggedOut) {
      this.log('Sessão encerrada pelo WhatsApp. Limpando credenciais locais.');
      this.clearAuthDirectory();
      return;
    }

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
    if (this.retryTimer) {
      return;
    }

    if (this.retryCount >= MAX_RETRIES) {
      this.logError(`Número máximo de tentativas atingido (${MAX_RETRIES}).`);
      return;
    }

    this.retryCount += 1;

    const delay =
      this.retryCount === 1
        ? INITIAL_RETRY_DELAY
        : INITIAL_RETRY_DELAY * this.retryCount;

    this.log(
      `Reconexão ${this.retryCount}/${MAX_RETRIES} em ${Math.round(delay / 1000)}s...`
    );

    this.retryTimer = setTimeout(async () => {
      this.retryTimer = null;

      try {
        await this.initialize();
      } catch (error) {
        this.logError('Falha durante tentativa de reconexão.', error);
      }
    }, delay);
  }

  clearRetryTimer() {
    if (!this.retryTimer) {
      return;
    }

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
        await this.processIncomingMessage(message);
      }
    } catch (error) {
      this.logError('Erro ao processar lote de mensagens.', error);
    }
  }

  async processIncomingMessage(msg) {
    if (!msg?.message) {
      return;
    }

    if (msg.key?.fromMe) {
      return;
    }

    const from = msg.key?.remoteJid;

    if (!from || from === STATUS_BROADCAST) {
      return;
    }

    const parsed = this.extractMessageContent(msg);

    if (!parsed.text && !parsed.isButtonClick) {
      return;
    }

    const preview =
      parsed.text.length > 100
        ? `${parsed.text.substring(0, 100)}...`
        : parsed.text;

    this.log(`Mensagem recebida de [${from}]: "${preview}"`);

    await this.handleMessage(from, parsed.text, parsed.isButtonClick);
  }

  // ============================================================
  // EXTRAÇÃO DO CONTEÚDO
  // ============================================================

  extractMessageContent(msg) {
    const message = msg.message;

    // Texto simples
    if (message.conversation) {
      return {
        text: message.conversation.trim(),
        isButtonClick: false,
      };
    }

    // Texto expandido
    if (message.extendedTextMessage?.text) {
      return {
        text: message.extendedTextMessage.text.trim(),
        isButtonClick: false,
      };
    }

    // Resposta interativa
    if (message.interactiveResponseMessage) {
      return this.extractInteractiveResponse(
        message.interactiveResponseMessage
      );
    }

    // Botões legados
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

    return {
      text: '',
      isButtonClick: false,
    };
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
        text: (selectedButton.displayText || selectedButton.id || '').trim(),
        isButtonClick: true,
      };
    }

    return {
      text: '',
      isButtonClick: false,
    };
  }

  // ============================================================
  // STATUS DE LEITURA
  // ============================================================

  async handleMessagesUpdate(updates = []) {
    for (const update of updates) {
      if (update?.status !== 'read') {
        continue;
      }

      const key = update.key;

      if (!key?.remoteJid || !key?.id) {
        continue;
      }

      await this.handleReadReceipt(key);
    }
  }

  async handleReadReceipt(key) {
    const from = key.remoteJid;
    const messageId = key.id;

    if (this.isReadReceiptDebounced(from)) {
      return;
    }

    this.lastReadTimestamps.set(from, Date.now());

    this.log(`Mensagem ${messageId} lida por ${from}`);

    await this.notifyBackendMessageRead({
      phone: from,
      messageId,
    });
  }

  isReadReceiptDebounced(phone) {
    const lastRead = this.lastReadTimestamps.get(phone);

    if (!lastRead) {
      return false;
    }

    return Date.now() - lastRead < READ_STATUS_DEBOUNCE;
  }

  async notifyBackendMessageRead({ phone, messageId }) {
    try {
      const response = await fetch(
        `${this.yaneApiUrl}/webhooks/message-status`,
        {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            phone,
            message_id: messageId,
            status: 'read',
            timestamp: new Date().toISOString(),
          }),
        }
      );

      if (!response.ok) {
        this.logError(
          `Backend rejeitou status de leitura. HTTP ${response.status}`
        );
      }
    } catch (error) {
      this.logError('Erro ao enviar status de leitura para o backend.', error);
    }
  }

  // ============================================================
  // ENVIO DE MENSAGEM
  // ============================================================

  async sendMessage(to, message) {
    if (!message) {
      this.logError('Tentativa de enviar mensagem vazia.');
      return false;
    }

    const chatId = this.prepareChatId(to);

    if (!chatId) {
      return false;
    }

    if (!this.ensureReady()) {
      return false;
    }

    try {
      await this.socket.sendMessage(chatId, {
        text: String(message),
      });

      this.log(`Mensagem enviada para ${this.formatLogRecipient(chatId)}`);

      return true;
    } catch (error) {
      this.logError(`Erro ao enviar mensagem para ${to}.`, error);
      return false;
    }
  }

  // ============================================================
  // MENSAGEM INTERATIVA
  // ============================================================

  async sendInteractiveMessage(to, title, body, buttons = []) {
    const chatId = this.prepareChatId(to);

    if (!chatId) {
      return false;
    }

    if (!this.ensureReady()) {
      return false;
    }

    if (!body) {
      this.logError('Mensagem interativa sem conteúdo.');
      return false;
    }

    try {
      const interactiveButtons = this.buildInteractiveButtons(buttons);

      if (!interactiveButtons.length) {
        return this.sendMessage(to, body);
      }

      // Envia os botões utilizando a estrutura limpa suportada pelo Baileys
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
        `Erro ao enviar mensagem interativa para ${to}. Enviando fallback em texto...`,
        error
      );

      // Fallback: envia mensagem formatada em texto caso os botões falhem
      const fallbackText = `${title ? `*${title}*\n\n` : ''}${body}`;
      return this.sendMessage(to, fallbackText);
    }
  }

  buildInteractiveButtons(buttons) {
    if (!Array.isArray(buttons)) {
      return [];
    }

    return buttons
      .filter(Boolean)
      .map((button, index) => ({
        id: button.id || `btn_${index + 1}`,
        text: button.text || button.label || `Opção ${index + 1}`,
      }));
  }

  // ============================================================
  // PROCESSAMENTO DA CONVERSA
  // ============================================================

  async handleMessage(from, text, isButton = false) {
    if (!from) {
      return;
    }

    const normalizedText = String(text || '').trim();

    try {
      const hasActiveSession = !!this.interviewService.getSession(from);

      // --------------------------------------------------------
      // RESET
      // --------------------------------------------------------

      if (this.isResetCommand(normalizedText)) {
        this.interviewService.endSession(from);

        await this.sendMessage(
          from,
          'Sessão reiniciada! Envie START ou OLÁ para começar novamente.'
        );

        return;
      }

      // --------------------------------------------------------
      // INÍCIO DA ENTREVISTA
      // --------------------------------------------------------

      if (!hasActiveSession && this.isStartTrigger(normalizedText, isButton)) {
        this.log(`[SESSAO] A iniciar entrevista para ${from}...`);

        const welcome = await this.interviewService.startInterview(from);

        // Apenas envia a mensagem se startInterview retornar uma string e não tiver enviado internamente
        if (typeof welcome === 'string' && welcome.trim().length > 0) {
          await this.sendMessage(from, welcome);
        }

        return;
      }

      // --------------------------------------------------------
      // SESSÃO ATIVA
      // --------------------------------------------------------

      if (hasActiveSession) {
        this.log(`[SESSAO] A processar resposta para ${from}...`);

        const response = await this.interviewService.handleResponse(
          from,
          normalizedText
        );

        if (response && typeof response === 'string' && response.trim()) {
          await this.sendMessage(from, response);
        }

        return;
      }

      // --------------------------------------------------------
      // SEM SESSÃO
      // --------------------------------------------------------

      this.log(`[BOT] Mensagem ignorada de ${from}: nenhuma sessão ativa.`);
    } catch (error) {
      this.logError(`Erro ao processar mensagem de ${from}.`, error);

      await this.sendMessage(
        from,
        'Desculpe, ocorreu um erro. Tente novamente dentro de momentos.'
      );
    }
  }

  isResetCommand(text) {
    const normalized = String(text || '')
      .toLowerCase()
      .trim();

    return (
      normalized === '!reset' ||
      normalized === '!recomencar' ||
      normalized === '!recomeçar'
    );
  }

  // ============================================================
  // TRIGGERS
  // ============================================================

  isStartTrigger(text, isButton = false) {
    if (isButton) {
      return this.isExplicitStartText(text);
    }

    if (!text) {
      return false;
    }

    const cleanText = text.toLowerCase().trim();

    const triggers = [
      /^sim\b/,
      /^oi\b/,
      /^ol[aá]\b/,
      /^iniciar\b/,
      /^start\b/,
      /^menu\b/,
      /^entrevista\b/,
      /^bom dia\b/,
      /^boa tarde\b/,
      /^boa noite\b/,
      /^vaga\b/,
      /^gostaria\b/,
      /^quero\b/,
      /^pode começar\b/,
      /^pode comecar\b/,
      /^começar\b/,
      /^comecar\b/,
    ];

    return triggers.some((regex) => regex.test(cleanText));
  }

  isExplicitStartText(text) {
    if (!text) {
      return false;
    }

    const cleanText = text.toLowerCase().trim();

    return [
      'iniciar',
      'start',
      'começar',
      'comecar',
      'iniciar entrevista',
      'começar entrevista',
      'comecar entrevista',
    ].includes(cleanText);
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

      return {
        success: false,
        message: error.message,
      };
    }
  }

  async closeSocket() {
    if (!this.socket) {
      return;
    }

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
    this.lastReadTimestamps.clear();
  }

  clearAuthDirectory() {
    try {
      if (fs.existsSync(AUTH_DIR)) {
        fs.rmSync(AUTH_DIR, {
          recursive: true,
          force: true,
        });
      }
    } catch (error) {
      this.logError('Erro ao limpar diretório de autenticação.', error);
    }
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