// src/services/whatsapp.service.js
//
// Integração Baileys ↔ WhatsApp do Yane.
//
// [FIXES]
// [FIX-1] PII (phone, remoteJid, preview) mascarada nos logs.
// [FIX-2] LID ↔ PN com persistência Redis. Quando enviamos uma
//         mensagem, o Baileys devolve o JID real associado. Se for
//         um LID, guardamos a associação no Redis. Nas respostas
//         seguintes, o LID é resolvido em O(1) sem depender do
//         signalRepository.
// [FIX-3] normalizePhone aceita números internacionais (8-15
//         dígitos) sem prefixar 258 por engano.
// [FIX-4] Mensagens com LID não resolvível são reportadas como
//         órfãs ao backend em vez de descartadas silenciosamente.
//
// PRINCÍPIOS
// - Uma única instância do socket por número.
// - O processamento de negócio continua no InterviewService.
// - Redis continua responsável pela coordenação/rate limit externo.
// - Nunca enviar para LID não resolvido.

const {
  default: makeWASocket,
  useMultiFileAuthState,
  DisconnectReason,
  WAMessageStatus,
  normalizeMessageContent,
  makeCacheableSignalKeyStore,
  fetchLatestBaileysVersion,
} = require('@whiskeysockets/baileys');

const qrcode = require('qrcode-terminal');
const pino = require('pino');
const fs = require('fs');
const path = require('path');

const BaseService = require('./base.service');
const RedisService = require('./redis.service');
const metrics = require('./metrics.service');

// =============================================================================
// CONFIGURAÇÃO
// =============================================================================

const AUTH_DIR = path.resolve(
  process.env.WHATSAPP_AUTH_DIR ||
    path.join(__dirname, '../../auth_info')
);

const DEFAULT_COUNTRY_CODE = '258';

const MAX_RETRIES = 5;
const INITIAL_RETRY_DELAY = 5_000;
const MAX_RETRY_DELAY = 60_000;

const CONNECTION_TIMEOUT = 60_000;

const READ_STATUS_DEBOUNCE = 5_000;
const READ_CACHE_MAX_SIZE = 5_000;

const OUTBOUND_RATE_LIMIT = 30;
const RATE_LIMIT_RETRY_MS = 200;
const RATE_LIMIT_MAX_WAIT_MS = 5_000;

const SEND_RETRY_ATTEMPTS = 2;
const SEND_RETRY_BASE_MS = 250;
const SEND_RETRY_MAX_MS = 1_500;

const LOGOUT_RESTART_DELAY_MS = 3_000;

const MAX_LID_CACHE_SIZE = 5_000;

const STATUS_BROADCAST = 'status@broadcast';
const WHATSAPP_SUFFIX = '@s.whatsapp.net';
const GROUP_SUFFIX = '@g.us';
const LID_SUFFIX = '@lid';
const NEWSLETTER_SUFFIX = '@newsletter';

const TRANSIENT_ERROR_CODES = new Set([
  'ECONNRESET',
  'ECONNREFUSED',
  'ETIMEDOUT',
  'EPIPE',
  'EHOSTUNREACH',
  'ENETUNREACH',
  'EAI_AGAIN',
  'UND_ERR_SOCKET',
]);

const TRANSIENT_STATUS_CODES = new Set([
  408, 425, 429, 500, 502, 503, 504,
]);

// =============================================================================
// HELPERS PUROS
// =============================================================================

function clean(value) {
  return String(value ?? '')
    .replace(/^["'`]+|["'`]+$/g, '')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{4,}/g, '\n\n')
    .trim();
}

function digitsOnly(value) {
  return String(value ?? '').replace(/\D/g, '');
}

function isAbortError(error) {
  return (
    error?.name === 'AbortError' ||
    error?.name === 'TimeoutError'
  );
}

function getErrorCode(error) {
  return String(
    error?.code ||
      error?.output?.statusCode ||
      error?.statusCode ||
      ''
  ).toUpperCase();
}

function getErrorStatusCode(error) {
  const raw =
    error?.output?.statusCode ?? error?.statusCode ?? null;

  const numeric = Number(raw);

  return Number.isFinite(numeric) ? numeric : null;
}

function isTransientSendError(error) {
  if (!error) return false;

  if (isAbortError(error)) return true;

  const code = getErrorCode(error);

  if (TRANSIENT_ERROR_CODES.has(code)) return true;

  const statusCode = getErrorStatusCode(error);

  if (
    statusCode !== null &&
    TRANSIENT_STATUS_CODES.has(statusCode)
  ) {
    return true;
  }

  const message = String(error?.message || '').toLowerCase();

  return (
    message.includes('socket closed') ||
    message.includes('socket error') ||
    message.includes('websocket') ||
    message.includes('connection reset') ||
    message.includes('connection closed') ||
    message.includes('connection lost') ||
    message.includes('disconnected') ||
    message.includes('timed out') ||
    message.includes('timeout')
  );
}

function isReadStatus(status) {
  if (
    typeof WAMessageStatus !== 'undefined' &&
    status === WAMessageStatus.READ
  ) {
    return true;
  }

  return String(status ?? '').toLowerCase() === 'read';
}

function maskPhone(phone) {
  const digits = digitsOnly(phone);

  if (digits.length < 6) return '***';

  return `${digits.slice(0, 3)}***${digits.slice(-3)}`;
}

function maskJid(jid) {
  const value = clean(jid);

  if (!value) return '';

  if (value.includes('@')) {
    const [local, suffix] = value.split('@');

    if (/\d/.test(local)) {
      return `${maskPhone(local)}@${suffix}`;
    }
  }

  return maskPhone(value);
}

// =============================================================================
// SERVIÇO
// =============================================================================

class WhatsAppService extends BaseService {
  constructor(interviewService, redis = null) {
    super();

    if (!interviewService) {
      throw new Error(
        'WhatsAppService requer uma instância de interviewService.'
      );
    }

    this.interviewService = interviewService;

    this.redis =
      redis ||
      interviewService.redis ||
      new RedisService();

    // -------------------------------------------------------------------------
    // Socket / lifecycle
    // -------------------------------------------------------------------------

    this.socket = null;

    this.isReady = false;
    this.isConnecting = false;
    this.isShuttingDown = false;

    this.retryCount = 0;
    this.retryTimer = null;
    this.logoutRestartTimer = null;

    this.connectedAt = null;

    this._initializePromise = null;
    this._lifecycleGeneration = 0;
    this._socketGeneration = 0;

    this.qrCode = null;

    // -------------------------------------------------------------------------
    // Config
    // -------------------------------------------------------------------------

    this.defaultCountryCode =
      digitsOnly(
        process.env.DEFAULT_COUNTRY_CODE || DEFAULT_COUNTRY_CODE
      ) || DEFAULT_COUNTRY_CODE;

    // -------------------------------------------------------------------------
    // Read receipts
    // -------------------------------------------------------------------------

    this.lastReadTimestamps = new Map();

    // -------------------------------------------------------------------------
    // [FIX-2] LID → PN cache em memória (write-through para Redis).
    // -------------------------------------------------------------------------

    this._lidToPhoneCache = new Map();

    // -------------------------------------------------------------------------
    // Logging
    // -------------------------------------------------------------------------

    this.logger = pino({
      level: process.env.LOG_LEVEL || 'info',
      base: { service: 'whatsapp' },
    });

    // -------------------------------------------------------------------------
    // Shutdown
    // -------------------------------------------------------------------------

    this._boundShutdown = this._handleProcessShutdown.bind(this);

    process.once('SIGTERM', this._boundShutdown);
    process.once('SIGINT', this._boundShutdown);

    this.ensureAuthDirectory();
  }

  // ===========================================================================
  // LOGGING
  // ===========================================================================

  _log(level, message, context = {}) {
    try {
      const loggerMethod = this.logger?.[level];

      if (typeof loggerMethod === 'function') {
        loggerMethod.call(this.logger, context, message);
      }
    } catch (_) {
      // Logging nunca deve derrubar o serviço.
    }
  }

  _safeLogContext(context = {}) {
    const safe = {};

    for (const [key, value] of Object.entries(context)) {
      if (value === undefined || value === null) continue;

      if (key === 'phone' || key === 'recipient') {
        safe[key] = maskPhone(value);
        continue;
      }

      if (key === 'remoteJid' || key === 'jid' || key === 'altJid') {
        safe[key] = maskJid(value);
        continue;
      }

      if (key === 'preview' || key === 'message' || key === 'body') {
        continue;
      }

      safe[key] = value;
    }

    return safe;
  }

  log(message, context = {}) {
    this._log('info', message, this._safeLogContext(context));
  }

  logWarn(message, context = {}) {
    this._log('warn', message, this._safeLogContext(context));
  }

  logError(message, error = null, context = {}) {
    this._log('error', message, {
      ...this._safeLogContext(context),
      ...(error
        ? {
            error: error?.message || String(error),
            code: error?.code || null,
            statusCode:
              error?.output?.statusCode ??
              error?.statusCode ??
              null,
          }
        : {}),
    });
  }

  // ===========================================================================
  // AUTH STORAGE
  // ===========================================================================

  ensureAuthDirectory() {
    try {
      fs.mkdirSync(AUTH_DIR, { recursive: true });
    } catch (error) {
      this.logError(
        'Não foi possível criar o diretório de autenticação.',
        error,
        { authDir: AUTH_DIR }
      );

      throw error;
    }
  }

  clearAuthDirectory() {
    try {
      fs.rmSync(AUTH_DIR, {
        recursive: true,
        force: true,
      });
    } catch (error) {
      this.logError(
        'Erro ao limpar diretório de autenticação.',
        error,
        { authDir: AUTH_DIR }
      );
    }
  }

  // ===========================================================================
  // PHONE / JID
  // ===========================================================================

  normalizePhone(phone) {
    if (!phone) return '';

    let value = String(phone).split('@')[0].split(':')[0];
    value = digitsOnly(value);

    if (!value) return '';

    if (value.startsWith('00258')) {
      value = value.slice(2);
    }

    if (value.startsWith(this.defaultCountryCode)) {
      return value;
    }

    if (value.length === 9) {
      return `${this.defaultCountryCode}${value}`;
    }

    if (value.length === 10 && value.startsWith('0')) {
      return `${this.defaultCountryCode}${value.slice(1)}`;
    }

    if (value.length >= 8 && value.length <= 15) {
      return value;
    }

    return '';
  }

  isValidPhone(phone) {
    const normalized = this.normalizePhone(phone);

    return (
      normalized.length >= 8 && normalized.length <= 15
    );
  }

  getChatId(to) {
    if (!to) return '';

    const value = String(to).trim();

    if (!value) return '';

    if (
      value === STATUS_BROADCAST ||
      value.endsWith(NEWSLETTER_SUFFIX)
    ) {
      return '';
    }

    if (value.endsWith(GROUP_SUFFIX)) {
      return value;
    }

    if (value.endsWith(LID_SUFFIX)) {
      // Nunca enviar diretamente para um LID não resolvido.
      return '';
    }

    if (value.endsWith(WHATSAPP_SUFFIX)) {
      const phone = this.normalizePhone(value.split('@')[0]);

      return this.isValidPhone(phone)
        ? `${phone}${WHATSAPP_SUFFIX}`
        : '';
    }

    const phone = this.normalizePhone(value);

    if (!this.isValidPhone(phone)) {
      return '';
    }

    return `${phone}${WHATSAPP_SUFFIX}`;
  }

  prepareChatId(to) {
    const chatId = this.getChatId(to);

    if (!chatId) {
      this.logError('Número/JID inválido para envio.', null, {
        recipient: String(to ?? ''),
      });

      return '';
    }

    return chatId;
  }

  formatLogRecipient(chatId) {
    if (!chatId) return '';
    return maskPhone(chatId);
  }

  // ===========================================================================
  // [FIX-2] LID → PHONE
  // ===========================================================================

  async resolveJidToPhone(primaryJid, altJid = null) {
    const primary = String(primaryJid || '').trim();
    const alt = String(altJid || '').trim();

    if (!primary) return '';

    if (
      primary === STATUS_BROADCAST ||
      primary.endsWith(GROUP_SUFFIX) ||
      primary.endsWith(NEWSLETTER_SUFFIX)
    ) {
      return '';
    }

    // 1. alt JID.
    if (alt) {
      if (alt.endsWith(WHATSAPP_SUFFIX)) {
        const phone = this.normalizePhone(alt.split('@')[0]);

        if (this.isValidPhone(phone)) {
          return phone;
        }
      }

      const altDigits = digitsOnly(alt);

      if (altDigits.length >= 8) {
        const phone = this.normalizePhone(altDigits);

        if (this.isValidPhone(phone)) return phone;
      }
    }

    // 2. remoteJid já é PN.
    if (primary.endsWith(WHATSAPP_SUFFIX)) {
      const phone = this.normalizePhone(primary.split('@')[0]);

      return this.isValidPhone(phone) ? phone : '';
    }

    // 3. LID — resolve via cache/Redis/signalRepository.
    if (primary.endsWith(LID_SUFFIX)) {
      return this._resolveLidToPhone(primary);
    }

    // 4. fallback defensivo.
    const digits = digitsOnly(primary);

    if (digits.length >= 8) {
      const phone = this.normalizePhone(digits);

      return this.isValidPhone(phone) ? phone : '';
    }

    return '';
  }

  normalizeLidJid(lid) {
    const digits = String(lid || '')
      .split('@')[0]
      .split(':')[0];

    return digits ? `${digits}${LID_SUFFIX}` : '';
  }

  async _resolveLidToPhone(lidJid) {
    const canonicalLid = this.normalizeLidJid(lidJid);

    if (!canonicalLid) return '';

    // 1. cache em memória.
    const cached = this._lidToPhoneCache.get(canonicalLid);
    if (cached) return cached;

    // 2. [FIX-2] Redis — mapping persistido quando enviamos o convite.
    try {
      const fromRedis = await this.redis.resolveLidMapping(canonicalLid);

      if (fromRedis) {
        const phone = this.normalizePhone(fromRedis);

        if (this.isValidPhone(phone)) {
          this._rememberLidMapping(canonicalLid, phone);
          return phone;
        }
      }
    } catch (error) {
      this.logWarn('Falha ao consultar LID no Redis.', {
        lid: canonicalLid,
        error: error?.message,
      });
    }

    // 3. signalRepository do Baileys (best-effort).
    try {
      const mapping = this.socket?.signalRepository?.lidMapping;

      if (
        mapping &&
        typeof mapping.getPNForLID === 'function'
      ) {
        const pn = await mapping.getPNForLID(canonicalLid);

        const phone = this.normalizePhone(String(pn || ''));

        if (this.isValidPhone(phone)) {
          // Persistir para futuras consultas.
          await this._persistLidMapping(canonicalLid, phone);
          this._rememberLidMapping(canonicalLid, phone);
          return phone;
        }
      }
    } catch (error) {
      this.logWarn('Falha ao resolver LID via signalRepository.', {
        lid: canonicalLid,
        error: error?.message,
      });
    }

    this.logWarn('LID não resolvível.', {
      lid: canonicalLid,
    });

    return '';
  }

  _rememberLidMapping(lidJid, phone) {
    if (!lidJid || !phone) return;

    try {
      if (this._lidToPhoneCache.size >= MAX_LID_CACHE_SIZE) {
        const oldestKey =
          this._lidToPhoneCache.keys().next().value;

        if (oldestKey) {
          this._lidToPhoneCache.delete(oldestKey);
        }
      }

      this._lidToPhoneCache.set(lidJid, phone);
    } catch (_) {
      // Cache é best-effort.
    }
  }

  async _persistLidMapping(lidJid, phone) {
    try {
      if (typeof this.redis.rememberLidMapping !== 'function') {
        return;
      }

      await this.redis.rememberLidMapping(lidJid, phone);
    } catch (error) {
      this.logWarn('Falha ao persistir LID → PN.', {
        lid: lidJid,
        error: error?.message,
      });
    }
  }

  _clearLidCache() {
    this._lidToPhoneCache.clear();
  }

  // ===========================================================================
  // [FIX-2] Captura do JID real associado à mensagem enviada
  // ===========================================================================

  /**
   * Extrai o JID devolvido pelo Baileys e, se for um LID, persiste
   * a associação LID → PN.
   *
   * É chamado imediatamente após um `socket.sendMessage` bem-sucedido.
   * O `chatId` de destino já é um PN (o candidato convidado); o JID
   * devolvido pode ser um LID que o WhatsApp associou.
   */
  async _captureLidMappingFromResponse(response, chatId) {
    try {
      const usedJid = response?.key?.remoteJid;

      if (!usedJid || typeof usedJid !== 'string') {
        return;
      }

      // Só nos interessa quando o WhatsApp devolve um LID.
      if (!usedJid.endsWith(LID_SUFFIX)) {
        return;
      }

      // O destino era um PN — extrair o número.
      const phone = this.normalizePhone(chatId);

      if (!this.isValidPhone(phone)) {
        return;
      }

      const canonicalLid = this.normalizeLidJid(usedJid);

      if (!canonicalLid) return;

      // Se já temos o mapping em cache, não vale a pena escrever.
      if (this._lidToPhoneCache.get(canonicalLid) === phone) {
        return;
      }

      this.log(
        'Mapping LID → PN capturado.',
        {
          lid: usedJid,
          phone,
        }
      );

      this._rememberLidMapping(canonicalLid, phone);

      await this._persistLidMapping(canonicalLid, phone);
    } catch (error) {
      this.logWarn('Falha ao capturar LID da resposta.', {
        error: error?.message,
      });
    }
  }

  // ===========================================================================
  // SOCKET LIFECYCLE
  // ===========================================================================

  async initialize() {
    if (this.isShuttingDown) return false;

    if (this._initializePromise) {
      return this._initializePromise;
    }

    if (this.isConnecting) return false;

    const lifecycleGeneration = this._lifecycleGeneration;

    this._initializePromise = this._initializeInternal(
      lifecycleGeneration
    ).finally(() => {
      this._initializePromise = null;
    });

    return this._initializePromise;
  }

  async _initializeInternal(lifecycleGeneration) {
    this.clearRetryTimer();

    this.isConnecting = true;
    this.isReady = false;
    this.qrCode = null;

    this.log('A iniciar conexão WhatsApp via Baileys.');

    try {
      this.ensureAuthDirectory();

      const { state, saveCreds } =
        await useMultiFileAuthState(AUTH_DIR);

      if (
        this.isShuttingDown ||
        lifecycleGeneration !== this._lifecycleGeneration
      ) {
        return false;
      }

      let version;

      try {
        const latest = await fetchLatestBaileysVersion();
        version = latest?.version || null;

        if (version) {
          this.log('Versão WhatsApp obtida.', {
            version: version.join('.'),
            isLatest: latest?.isLatest ?? null,
          });
        }
      } catch (error) {
        this.logWarn(
          'Não foi possível obter a versão atual do WhatsApp; '
          + 'usando a versão interna do Baileys.',
          { error: error?.message }
        );
      }

      if (
        this.isShuttingDown ||
        lifecycleGeneration !== this._lifecycleGeneration
      ) {
        return false;
      }

      const authKeys =
        typeof makeCacheableSignalKeyStore === 'function'
          ? makeCacheableSignalKeyStore(state.keys, this.logger)
          : state.keys;

      const socketConfig = {
        auth: {
          creds: state.creds,
          keys: authKeys,
        },
        logger: this.logger,
        browser: ['Yane ATS', 'Chrome', '120.0.0.0'],
        printQRInTerminal: false,
        syncFullHistory: false,
        markOnlineOnConnect: true,
        generateHighQualityLinkPreview: false,
        defaultQueryTimeoutMs: CONNECTION_TIMEOUT,
      };

      if (version) {
        socketConfig.version = version;
      }

      const socket = makeWASocket(socketConfig);

      if (
        this.isShuttingDown ||
        lifecycleGeneration !== this._lifecycleGeneration
      ) {
        try {
          if (typeof socket.end === 'function') {
            await socket.end(undefined);
          } else {
            socket.ws?.close?.();
          }
        } catch (_) {
          // Best-effort.
        }

        return false;
      }

      this.socket = socket;

      const socketGeneration = ++this._socketGeneration;

      this.setupEventHandlers(
        socket,
        socketGeneration,
        saveCreds
      );

      this.log('Socket WhatsApp criado. Aguardando ligação.');

      return true;
    } catch (error) {
      this.isConnecting = false;
      this.isReady = false;
      this.qrCode = null;

      this.logError('Erro ao inicializar WhatsApp.', error);

      if (!this.isShuttingDown) {
        this.scheduleReconnect();
      }

      return false;
    }
  }

  setupEventHandlers(socket, socketGeneration, saveCreds) {
    if (!socket) {
      throw new Error('Socket WhatsApp não inicializado.');
    }

    socket.ev.on('connection.update', (update) => {
      void this.handleConnectionUpdate(
        update,
        socket,
        socketGeneration
      ).catch((error) => {
        this.logError('Erro em connection.update.', error);
      });
    });

    socket.ev.on('creds.update', saveCreds);

    socket.ev.on('messages.upsert', (event) => {
      void this.handleMessagesUpsert(
        event,
        socket,
        socketGeneration
      ).catch((error) => {
        this.logError('Erro em messages.upsert.', error);
      });
    });

    socket.ev.on('messages.update', (updates) => {
      void this.handleMessagesUpdate(
        updates,
        socket,
        socketGeneration
      ).catch((error) => {
        this.logError('Erro em messages.update.', error);
      });
    });
  }

  isCurrentSocket(socket, socketGeneration) {
    return (
      !this.isShuttingDown &&
      socket === this.socket &&
      socketGeneration === this._socketGeneration
    );
  }

  // ===========================================================================
  // CONNECTION UPDATE
  // ===========================================================================

  async handleConnectionUpdate(
    update,
    socket = this.socket,
    socketGeneration = this._socketGeneration
  ) {
    if (!this.isCurrentSocket(socket, socketGeneration)) {
      return;
    }

    const { connection, lastDisconnect, qr } = update || {};

    if (qr) this.handleQRCode(qr);

    if (connection === 'open') {
      this.handleConnectionOpen(socket, socketGeneration);
      return;
    }

    if (connection === 'close') {
      await this.handleConnectionClose(
        lastDisconnect,
        socket,
        socketGeneration
      );
    }
  }

  handleQRCode(qr) {
    this.qrCode = qr;
    this.isReady = false;

    process.stdout.write(
      '\n[QR CODE] Digitalize com o seu WhatsApp:\n\n'
    );

    qrcode.generate(qr, { small: true });
  }

  handleConnectionOpen(
    socket = this.socket,
    socketGeneration = this._socketGeneration
  ) {
    if (!this.isCurrentSocket(socket, socketGeneration)) {
      return;
    }

    this.isReady = true;
    this.isConnecting = false;
    this.qrCode = null;
    this.retryCount = 0;
    this.connectedAt = Date.now();

    this.clearRetryTimer();
    this.clearLogoutRestartTimer();

    metrics.whatsappConnected.set(1);

    const user = socket?.user;

    this.log('WhatsApp conectado e pronto para responder.', {
      session: user?.id || null,
      name: user?.name || null,
    });
  }

  async handleConnectionClose(
    lastDisconnect,
    socket = this.socket,
    socketGeneration = this._socketGeneration
  ) {
    if (!this.isCurrentSocket(socket, socketGeneration)) {
      return;
    }

    const statusCode = this.getDisconnectStatusCode(lastDisconnect);
    const loggedOut =
      statusCode === DisconnectReason.loggedOut;

    this.socket = null;
    this.isReady = false;
    this.isConnecting = false;
    this.qrCode = null;
    this.connectedAt = null;

    metrics.whatsappConnected.set(0);

    this.log('WhatsApp desconectado.', {
      statusCode: statusCode || 'unknown',
    });

    if (loggedOut) {
      this.log(
        'Sessão encerrada pelo WhatsApp. Limpando credenciais '
        + 'e preparando novo login.'
      );

      this.clearAuthDirectory();
      this._clearLidCache();

      this._scheduleLogoutRestart();
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

  // ===========================================================================
  // RECONNECT
  // ===========================================================================

  scheduleReconnect() {
    if (
      this.retryTimer ||
      this.isShuttingDown ||
      this.isConnecting ||
      this.isReady
    ) {
      return;
    }

    if (this.retryCount >= MAX_RETRIES) {
      this.logError(
        `Número máximo de tentativas atingido (${MAX_RETRIES}).`
      );
      return;
    }

    this.retryCount += 1;

    const exponential = Math.min(
      INITIAL_RETRY_DELAY * Math.pow(2, this.retryCount - 1),
      MAX_RETRY_DELAY
    );

    const jitter = 0.8 + Math.random() * 0.4;
    const delay = Math.round(exponential * jitter);

    this.log(
      `Reconexão ${this.retryCount}/${MAX_RETRIES} agendada.`,
      { delayMs: delay }
    );

    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;

      void this.initialize().catch((error) => {
        this.logError('Falha na tentativa de reconexão.', error);
      });
    }, delay);

    this.unrefTimer(this.retryTimer);
  }

  clearRetryTimer() {
    if (!this.retryTimer) return;

    clearTimeout(this.retryTimer);
    this.retryTimer = null;
  }

  _scheduleLogoutRestart() {
    if (this.logoutRestartTimer || this.isShuttingDown) {
      return;
    }

    this.logoutRestartTimer = setTimeout(() => {
      this.logoutRestartTimer = null;

      if (this.isShuttingDown) return;

      this.retryCount = 0;

      void this.initialize()
        .then((started) => {
          if (!started) this.scheduleReconnect();
        })
        .catch((error) => {
          this.logError('Falha ao reiniciar após logout.', error);
          this.scheduleReconnect();
        });
    }, LOGOUT_RESTART_DELAY_MS);

    this.unrefTimer(this.logoutRestartTimer);
  }

  clearLogoutRestartTimer() {
    if (!this.logoutRestartTimer) return;

    clearTimeout(this.logoutRestartTimer);
    this.logoutRestartTimer = null;
  }

  unrefTimer(timer) {
    if (timer && typeof timer.unref === 'function') {
      timer.unref();
    }
  }

  // ===========================================================================
  // MENSAGENS RECEBIDAS
  // ===========================================================================

  async handleMessagesUpsert(
    event,
    socket = this.socket,
    socketGeneration = this._socketGeneration
  ) {
    if (!this.isCurrentSocket(socket, socketGeneration)) {
      return;
    }

    if (event?.type && event.type !== 'notify') {
      return;
    }

    const messages = Array.isArray(event?.messages)
      ? event.messages
      : [];

    for (const message of messages) {
      void this.processIncomingMessage(message).catch((error) => {
        this.logError(
          'Erro no processamento assíncrono da mensagem.',
          error,
          { msgId: message?.key?.id || null }
        );
      });
    }
  }

  async processIncomingMessage(msg) {
    if (!msg?.message) return;
    if (msg.key?.fromMe) return;

    const rawJid = msg.key?.remoteJid;

    if (!rawJid || rawJid === STATUS_BROADCAST) return;

    const altJid =
      msg.key?.remoteJidAlt ||
      msg.key?.participantAlt ||
      null;

    const phone = await this.resolveJidToPhone(rawJid, altJid);

    // [FIX-4] Mensagens com LID não resolvível são reportadas como
    // órfãs ao backend em vez de descartadas silenciosamente.
    if (!phone) {
      this.logWarn(
        'Mensagem ignorada: JID não resolvível para telefone.',
        {
          remoteJid: rawJid,
          altJid,
          msgId: msg.key?.id || null,
        }
      );

      metrics.errorsTotal.inc({ subsystem: 'whatsapp' });

      await this._reportUnresolvableIncoming({
        rawJid,
        altJid,
        message: msg,
      });

      return;
    }

    const parsed = this.extractMessageContent(msg);

    if (!parsed.text && !parsed.isButtonClick) {
      return;
    }

    const type = parsed.isButtonClick
      ? 'button'
      : parsed.text
        ? 'text'
        : 'other';

    metrics.messagesReceived.inc({ type });

    this.log('Mensagem recebida.', {
      phone,
      remoteJid: rawJid,
      msgId: msg.key?.id || null,
      type,
      textLength: parsed.text ? parsed.text.length : 0,
    });

    await this.handleMessage(
      phone,
      parsed.text,
      parsed.isButtonClick,
      msg.key?.id || null
    );
  }

  // [FIX-4] reportar mensagens não resolvíveis ao backend.
  async _reportUnresolvableIncoming({
    rawJid,
    altJid,
    message,
  }) {
    try {
      const reportUnmatched =
        this.interviewService?.yane?.reportUnmatchedIncoming;

      if (typeof reportUnmatched !== 'function') {
        return;
      }

      const parsed = this.extractMessageContent(message);

      // O backend aceita phone OU jid como identificador.
      await reportUnmatched({
        phone: null,
        jid: rawJid,
        altJid,
        message: parsed.text || '(mensagem sem texto)',
        messageId: message?.key?.id || null,
      });
    } catch (error) {
      // Best-effort.
      this.logWarn('Falha ao reportar mensagem órfã.', {
        remoteJid: rawJid,
        error: error?.message,
      });
    }
  }

  // ===========================================================================
  // EXTRAÇÃO
  // ===========================================================================

  extractMessageContent(msg) {
    if (!msg?.message) {
      return { text: '', isButtonClick: false };
    }

    const message =
      normalizeMessageContent(msg.message) || msg.message;

    if (message.conversation) {
      return {
        text: String(message.conversation).trim(),
        isButtonClick: false,
      };
    }

    if (message.extendedTextMessage?.text) {
      return {
        text: String(message.extendedTextMessage.text).trim(),
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
        text: String(
          response.selectedButtonId ||
            response.displayText ||
            ''
        ).trim(),
        isButtonClick: true,
      };
    }

    if (message.templateButtonReplyMessage) {
      const response = message.templateButtonReplyMessage;

      return {
        text: String(
          response.selectedId ||
            response.selectedDisplayText ||
            ''
        ).trim(),
        isButtonClick: true,
      };
    }

    if (message.listResponseMessage) {
      const response = message.listResponseMessage;

      return {
        text: String(
          response.singleSelectReply?.selectedRowId ||
            response.title ||
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
      const params = nativeFlow.paramsJson;

      if (params) {
        try {
          const parsed = JSON.parse(params);

          const selected =
            parsed?.id ||
            parsed?.selectedId ||
            parsed?.selected_id ||
            parsed?.buttonId ||
            parsed?.button_id ||
            parsed?.title ||
            parsed?.text;

          if (selected) {
            return {
              text: String(selected).trim(),
              isButtonClick: true,
            };
          }
        } catch (_) {
          // paramsJson pode não ser JSON válido.
        }

        return {
          text: String(nativeFlow.text || params).trim(),
          isButtonClick: true,
        };
      }

      return {
        text: String(nativeFlow.text || '').trim(),
        isButtonClick: true,
      };
    }

    const selectedButton = response?.selectedButton;

    if (selectedButton) {
      return {
        text: String(
          selectedButton.displayText ||
            selectedButton.id ||
            ''
        ).trim(),
        isButtonClick: true,
      };
    }

    return { text: '', isButtonClick: false };
  }

  // ===========================================================================
  // PROCESSAMENTO DE NEGÓCIO
  // ===========================================================================

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
      this.logError('Erro ao processar mensagem.', error, {
        phone: from,
        msgId: messageId,
      });
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

  // ===========================================================================
  // READ RECEIPTS
  // ===========================================================================

  async handleMessagesUpdate(
    updates = [],
    socket = this.socket,
    socketGeneration = this._socketGeneration
  ) {
    if (!this.isCurrentSocket(socket, socketGeneration)) {
      return;
    }

    if (!Array.isArray(updates)) return;

    for (const item of updates) {
      const key = item?.key;
      const status = item?.update?.status ?? item?.status;

      if (!isReadStatus(status)) continue;

      if (!key?.remoteJid || !key?.id) continue;

      await this.handleReadReceipt(key);
    }
  }

  async handleReadReceipt(key) {
    const rawJid = key?.remoteJid;
    const messageId = key?.id;

    if (!rawJid || !messageId) return;

    const altJid = key?.remoteJidAlt || null;

    const phone = await this.resolveJidToPhone(rawJid, altJid);

    if (!phone) return;

    if (this.isReadReceiptDebounced(phone)) return;

    this.lastReadTimestamps.set(phone, Date.now());

    this.trimReadCache();

    this.log('Mensagem lida pelo candidato.', {
      phone,
      msgId: messageId,
    });

    await this.notifyBackendMessageRead({ phone, messageId });
  }

  isReadReceiptDebounced(phone) {
    const lastRead = this.lastReadTimestamps.get(phone);

    if (!lastRead) return false;

    const elapsed = Date.now() - lastRead;

    if (elapsed >= READ_STATUS_DEBOUNCE) {
      this.lastReadTimestamps.delete(phone);
      return false;
    }

    return true;
  }

  trimReadCache() {
    if (
      this.lastReadTimestamps.size <= READ_CACHE_MAX_SIZE
    ) {
      return;
    }

    const now = Date.now();

    for (const [phone, timestamp] of this.lastReadTimestamps) {
      if (now - timestamp > READ_STATUS_DEBOUNCE * 2) {
        this.lastReadTimestamps.delete(phone);
      }

      if (this.lastReadTimestamps.size <= READ_CACHE_MAX_SIZE) {
        break;
      }
    }
  }

  async notifyBackendMessageRead({ phone, messageId }) {
    try {
      await this.interviewService.yane.sendMessageStatus({
        phone,
        messageId,
        status: 'read',
      });
    } catch (error) {
      this.logError('Erro ao enviar status de leitura.', error, {
        phone,
        msgId: messageId,
      });
    }
  }

  // ===========================================================================
  // ENVIO
  // ===========================================================================

  async sendMessage(to, message) {
    if (
      message === null ||
      message === undefined ||
      String(message).trim() === ''
    ) {
      this.logError('Tentativa de enviar mensagem vazia.');
      return false;
    }

    const chatId = this.prepareChatId(to);

    if (!chatId) return false;

    if (!this.ensureReady()) return false;

    const result = await this._sendPayload(
      chatId,
      { text: clean(message) },
      { rateLimit: true }
    );

    return result.ok;
  }

  async _sendPayload(
    chatId,
    payload,
    { rateLimit = true, operation = 'message' } = {}
  ) {
    if (!chatId) {
      return { ok: false, error: null, response: null };
    }

    if (rateLimit) {
      const allowed = await this.waitForRateLimit();

      if (!allowed) {
        metrics.messagesSent.inc({ status: 'rate_limited' });
        metrics.rateLimitHits.inc({ bucket: 'out' });

        this.logError(
          'Rate limit excedido e tempo máximo de espera atingido.',
          null,
          {
            recipient: this.formatLogRecipient(chatId),
            operation,
          }
        );

        return { ok: false, error: null, response: null };
      }
    }

    let lastError = null;

    const maxAttempts = SEND_RETRY_ATTEMPTS + 1;

    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      const socket = this.socket;

      if (!socket || !this.isReady) {
        this.logWarn(
          'Socket deixou de estar pronto durante o envio.',
          { recipient: this.formatLogRecipient(chatId) }
        );
        break;
      }

      try {
        const response = await metrics.time(
          metrics.messageSendDuration,
          { attempt: String(attempt) },
          () => socket.sendMessage(chatId, payload)
        );

        // [FIX-2] capturar o LID associado ao destino.
        // Best-effort: nunca bloqueia o envio.
        void this._captureLidMappingFromResponse(
          response,
          chatId
        );

        metrics.messagesSent.inc({ status: 'success' });

        this.log('Mensagem enviada.', {
          recipient: this.formatLogRecipient(chatId),
          operation,
          attempt,
        });

        return { ok: true, error: null, response };
      } catch (error) {
        lastError = error;

        const shouldRetry =
          attempt < SEND_RETRY_ATTEMPTS &&
          isTransientSendError(error);

        if (!shouldRetry) break;

        const backoff = Math.min(
          SEND_RETRY_BASE_MS * Math.pow(2, attempt),
          SEND_RETRY_MAX_MS
        );

        this.logWarn(
          'Falha transitória no envio; retry agendado.',
          {
            recipient: this.formatLogRecipient(chatId),
            operation,
            attempt: attempt + 1,
            maxAttempts,
            retryInMs: backoff,
            error: error?.message || String(error),
            code: error?.code || null,
          }
        );

        await this.delay(backoff);
      }
    }

    metrics.messagesSent.inc({ status: 'failed' });
    metrics.errorsTotal.inc({ subsystem: 'whatsapp' });

    this.logError('Falha definitiva ao enviar mensagem.', lastError, {
      recipient: this.formatLogRecipient(chatId),
      operation,
    });

    return { ok: false, error: lastError, response: null };
  }

  async waitForRateLimit() {
    const startedAt = Date.now();

    while (Date.now() - startedAt < RATE_LIMIT_MAX_WAIT_MS) {
      try {
        const allowed = await this.redis.allowRate(
          'out',
          OUTBOUND_RATE_LIMIT
        );

        if (allowed) return true;
      } catch (error) {
        this.logError('Falha no rate limiter Redis.', error, {
          bucket: 'out',
        });

        return false;
      }

      await this.delay(RATE_LIMIT_RETRY_MS);
    }

    return false;
  }

  // ===========================================================================
  // MENSAGEM INTERATIVA
  // ===========================================================================

  async sendInteractiveMessage(to, title, body, buttons = []) {
    if (
      body === null ||
      body === undefined ||
      String(body).trim() === ''
    ) {
      this.logError('Mensagem interativa sem conteúdo.');
      return false;
    }

    const chatId = this.prepareChatId(to);

    if (!chatId) return false;

    if (!this.ensureReady()) return false;

    const normalizedBody = clean(body);

    const formattedText = title
      ? `*${clean(title)}*\n\n${normalizedBody}`
      : normalizedBody;

    const interactiveButtons =
      this.buildInteractiveButtons(buttons);

    if (!interactiveButtons.length) {
      const result = await this._sendPayload(
        chatId,
        { text: formattedText },
        { rateLimit: true, operation: 'interactive-fallback' }
      );

      return result.ok;
    }

    const payload = {
      text: formattedText,
      footer: 'Yane ATS - Recrutamento Inteligente',
      buttons: interactiveButtons.map((button) => ({
        buttonId: button.id,
        buttonText: { displayText: button.text },
        type: 1,
      })),
      headerType: 1,
    };

    const result = await this._sendPayload(chatId, payload, {
      rateLimit: true,
      operation: 'interactive',
    });

    if (result.ok) return true;

    if (result.error && isTransientSendError(result.error)) {
      return false;
    }

    const fallback = await this._sendPayload(
      chatId,
      { text: formattedText },
      { rateLimit: false, operation: 'interactive-text-fallback' }
    );

    return fallback.ok;
  }

  buildInteractiveButtons(buttons) {
    if (!Array.isArray(buttons)) return [];

    return buttons
      .filter(Boolean)
      .map((button, index) => ({
        id: String(button.id || `btn_${index + 1}`).trim(),
        text: String(
          button.label ||
            button.text ||
            `Opção ${index + 1}`
        ).trim(),
      }))
      .filter((button) => button.id && button.text);
  }

  // ===========================================================================
  // PRESENCE
  // ===========================================================================

  async sendPresenceUpdate(presence, to) {
    const chatId = this.prepareChatId(to);

    if (!chatId) return;

    const socket = this.socket;

    if (!socket || !this.isReady) return;

    try {
      await socket.sendPresenceUpdate(presence, chatId);
    } catch (error) {
      this.logWarn('Falha ao atualizar presence.', {
        recipient: this.formatLogRecipient(chatId),
        presence,
        error: error?.message || String(error),
      });
    }
  }

  // ===========================================================================
  // STATUS
  // ===========================================================================

  getStatus() {
    const connected = Boolean(
      this.isReady && this.socket?.user?.id
    );

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

  // ===========================================================================
  // RESET DA SESSÃO
  // ===========================================================================

  async resetSession() {
    if (this.isConnecting || this._initializePromise) {
      return {
        success: false,
        message: 'Já existe uma operação de conexão em andamento.',
      };
    }

    this.isConnecting = true;

    this._lifecycleGeneration += 1;

    try {
      this.clearRetryTimer();
      this.clearLogoutRestartTimer();

      await this.closeSocket();

      this.resetConnectionState();

      this.clearAuthDirectory();
      this._clearLidCache();

      this.ensureAuthDirectory();

      await this.delay(2_000);

      if (this.isShuttingDown) {
        return {
          success: false,
          message: 'O serviço está a encerrar.',
        };
      }

      this.isConnecting = false;

      const started = await this.initialize();

      if (!started) {
        return {
          success: false,
          message:
            'Não foi possível iniciar a nova sessão do WhatsApp.',
        };
      }

      return {
        success: true,
        message: 'Sessão reiniciada. Aguarde o novo QR Code.',
      };
    } catch (error) {
      this.isConnecting = false;

      this.logError('Erro ao reiniciar sessão do WhatsApp.', error);

      return {
        success: false,
        message:
          error?.message || 'Falha ao reiniciar sessão.',
      };
    }
  }

  async closeSocket() {
    const socketToClose = this.socket;

    if (!socketToClose) return;

    this.socket = null;
    this._socketGeneration += 1;

    this.isReady = false;

    try {
      if (typeof socketToClose.end === 'function') {
        await socketToClose.end(undefined);
        return;
      }

      if (
        socketToClose.ws &&
        typeof socketToClose.ws.close === 'function'
      ) {
        socketToClose.ws.close();
      }
    } catch (error) {
      this.logError('Erro ao fechar socket WhatsApp.', error);
    }
  }

  resetConnectionState() {
    this.isReady = false;
    this.isConnecting = false;

    this.qrCode = null;
    this.retryCount = 0;
    this.connectedAt = null;

    this.lastReadTimestamps.clear();
  }

  // ===========================================================================
  // GRACEFUL SHUTDOWN
  // ===========================================================================

  async _handleProcessShutdown(signal) {
    this.log('Shutdown do processo.', { signal });

    try {
      await this.shutdown();
    } catch (error) {
      this.logError('Erro durante shutdown por sinal.', error);
    }
  }

  async shutdown() {
    if (this.isShuttingDown) return;

    this.isShuttingDown = true;

    this._lifecycleGeneration += 1;

    this.clearRetryTimer();
    this.clearLogoutRestartTimer();

    this.isReady = false;
    this.isConnecting = false;
    this.qrCode = null;
    this.connectedAt = null;

    metrics.whatsappConnected.set(0);

    this.log('Shutdown do WhatsApp iniciado.');

    try {
      process.removeListener('SIGTERM', this._boundShutdown);
      process.removeListener('SIGINT', this._boundShutdown);
    } catch (_) {
      // Best-effort.
    }

    const socket = this.socket;

    this.socket = null;
    this._socketGeneration += 1;

    try {
      if (socket) {
        if (typeof socket.end === 'function') {
          await socket.end(undefined);
        } else if (
          socket.ws &&
          typeof socket.ws.close === 'function'
        ) {
          socket.ws.close();
        }
      }
    } catch (error) {
      this.logError('Erro ao fechar socket no shutdown.', error);
    }

    this._clearLidCache();
    this.lastReadTimestamps.clear();

    this.log('Shutdown do WhatsApp concluído.');
  }

  // ===========================================================================
  // UTILITÁRIOS
  // ===========================================================================

  delay(ms) {
    const timeout = Math.max(0, Number(ms) || 0);

    return new Promise((resolve) =>
      setTimeout(resolve, timeout)
    );
  }

  getClient() {
    return this.socket;
  }
}

module.exports = WhatsAppService;