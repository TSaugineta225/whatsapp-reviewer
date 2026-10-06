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
// [FIX-2.5] handleConnectionClose distingue três casos:
//           - loggedOut (401): limpar credenciais, novo QR
//           - connectionReplaced (440): NÃO retentar. Outro
//             dispositivo assumiu a sessão. Retentar é inútil e
//             cria loop. Operador tem de agir.
//           - resto: backoff normal
//
// [FIX-2.6] extractMessageContent reconhece mensagens de mídia
//           (imagem, áudio, vídeo, documento, sticker). Antes eram
//           descartadas silenciosamente — o candidato ficava sem
//           resposta sem perceber porquê.
//
// [FIX-2.7] resetInterviewConversation resolve PRIMEIRO o
//           interviewId e só depois apaga o estado local. Sem isto,
//           uma falha de cancelamento backend deixava Redis e
//           backend divergentes.
//
// [FIX-2.8] Recuperação automática após esgotar MAX_RETRIES.
//           Antes, o serviço ficava permanentemente offline após
//           5 tentativas falhadas. Agora agenda uma tentativa
//           espaçada (5min) que se repete até a ligação voltar.
//
// [FIX-2.9] (NOVO) `errorMessage` foi reintroduzido como helper
//           puro no topo do ficheiro. Antes era referenciado em
//           `waitForRateLimit` mas não existia — o que rebentava
//           com `ReferenceError` precisamente no caminho de
//           degradação do Redis, quando mais se precisa do log.
//
// PRINCÍPIOS
// - Uma única instância do socket por número.
// - O processamento de negócio continua no InterviewService.
// - Redis continua responsável pela coordenação/rate limit externo.
// - Nunca enviar para LID não resolvido.
// - Observabilidade nunca pode interromper o fluxo de negócio.

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
const RETRY_RECOVERY_INTERVAL_MS = 5 * 60_000;

const CONNECTION_TIMEOUT = 60_000;

const READ_STATUS_DEBOUNCE = 5_000;
const READ_CACHE_MAX_SIZE = 5_000;

const OUTBOUND_RATE_LIMIT = 30;
const RATE_LIMIT_RETRY_MS = 200;
const RATE_LIMIT_MAX_WAIT_MS = 5_000;
const LOCAL_RATE_LIMIT_WINDOW_MS = 1_000;
const LOCAL_RATE_LIMIT_MAX = OUTBOUND_RATE_LIMIT;

const SEND_RETRY_ATTEMPTS = 2;
const SEND_RETRY_BASE_MS = 250;
const SEND_RETRY_MAX_MS = 1_500;

const LOGOUT_RESTART_DELAY_MS = 3_000;

const MAX_LID_CACHE_SIZE = 5_000;
const LID_CACHE_TTL_MS = 24 * 60 * 60_000;
const INCOMING_DEDUPE_TTL_MS = 10 * 60_000;
const INCOMING_DEDUPE_MAX_SIZE = 10_000;

const STATUS_BROADCAST = 'status@broadcast';
const WHATSAPP_SUFFIX = '@s.whatsapp.net';
const GROUP_SUFFIX = '@g.us';
const LID_SUFFIX = '@lid';
const NEWSLETTER_SUFFIX = '@newsletter';

// [FIX-2.5] código do WhatsApp para "sessão substituída por outro
// dispositivo". Não usar DisconnectReason.connectionReplaced
// directamente porque a constante pode não existir em versões mais
// antigas do Baileys.
const STATUS_CONNECTION_REPLACED = 440;

// [FIX-2.6] mensagens de mídia que reconhecemos mas não processamos.
const MEDIA_MESSAGE_TYPES = [
  'imageMessage',
  'videoMessage',
  'audioMessage',
  'documentMessage',
  'stickerMessage',
];

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

// [FIX-2.9] Helper reintroduzido. Era referenciado em `waitForRateLimit`
// sem estar definido, causando `ReferenceError` precisamente quando o
// Redis cai e o fallback local é acionado. Nunca falhar o logging.
function errorMessage(error) {
  return clean(
    error?.message || error?.error || 'unknown_error'
  ).slice(0, 500);
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

    this.socket = null;

    this.isReady = false;
    this.isConnecting = false;
    this.isShuttingDown = false;

    this.retryCount = 0;
    this.retryTimer = null;
    this.logoutRestartTimer = null;
    this.recoveryTimer = null;

    this.connectedAt = null;

    // Promises de lifecycle impedem initialize/reset concorrentes de criarem
    // sockets sobrepostos ou deixarem operações antigas mutarem o estado atual.
    this._initializePromise = null;
    this._resetPromise = null;
    this._lifecycleGeneration = 0;
    this._socketGeneration = 0;

    this.qrCode = null;

    this.defaultCountryCode =
      digitsOnly(
        process.env.DEFAULT_COUNTRY_CODE || DEFAULT_COUNTRY_CODE
      ) || DEFAULT_COUNTRY_CODE;

    this.lastReadTimestamps = new Map();
    this._readReceiptInFlight = new Set();

    // Evita duas operações de !reset simultâneas para o mesmo candidato.
    this._conversationResetLocks = new Map();

    this._lidToPhoneCache = new Map();
    this._incomingMessageCache = new Map();
    this._localRateLimit = [];

    if (typeof this.interviewService?.setWhatsAppService === 'function') {
      try {
        this.interviewService.setWhatsAppService(this);
      } catch (_) {
        // Ligação automática é best-effort.
      }
    }

    this.logger = pino({
      level: process.env.LOG_LEVEL || 'info',
      base: { service: 'whatsapp' },
    });

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

      if (
        key === 'remoteJid' ||
        key === 'jid' ||
        key === 'altJid' ||
        key === 'participant' ||
        key === 'participantAlt' ||
        key === 'session' ||
        key === 'lid'
      ) {
        safe[key] = this._maskIdentifier(value);
        continue;
      }

      if (key === 'preview' || key === 'message' || key === 'body') {
        continue;
      }

      safe[key] = value;
    }

    return safe;
  }

  _maskIdentifier(value) {
    const raw = clean(value);

    if (!raw) return '';

    if (raw.includes('@')) {
      return maskJid(raw);
    }

    if (/^\d[\d:\-\s]*$/.test(raw)) {
      return maskPhone(raw);
    }

    if (raw.length <= 4) {
      return '***';
    }

    return `${raw.slice(0, 2)}***${raw.slice(-2)}`;
  }

  _safeMetricInc(metric, labels = {}, metricName = 'unknown') {
    try {
      if (metric && typeof metric.inc === 'function') {
        metric.inc(labels);
      }
    } catch (error) {
      this._log(
        'warn',
        'Falha ao registar métrica; operação ignorada.',
        {
          metric: metricName,
          error: errorMessage(error),
        }
      );
    }
  }

  _safeMetricSet(metric, value, metricName = 'unknown') {
    try {
      if (metric && typeof metric.set === 'function') {
        metric.set(value);
      }
    } catch (error) {
      this._log(
        'warn',
        'Falha ao atualizar métrica; operação ignorada.',
        {
          metric: metricName,
          error: errorMessage(error),
        }
      );
    }
  }

  async _safeMetricTime(metric, labels, fn, metricName = 'unknown') {
    if (typeof fn !== 'function') return undefined;

    if (!metric || typeof metrics?.time !== 'function') {
      return fn();
    }

    let businessSettled = false;
    let businessResult;
    let businessError = null;

    const wrapped = async () => {
      try {
        businessResult = await fn();
        businessSettled = true;
        return businessResult;
      } catch (error) {
        businessError = error;
        businessSettled = true;
        throw error;
      }
    };

    try {
      const instrumentedResult = await metrics.time(
        metric,
        labels || {},
        wrapped
      );

      return businessSettled ? businessResult : instrumentedResult;
    } catch (error) {
      // Se o callback de negócio já foi executado, não transformamos uma
      // falha da instrumentação em falha do envio/processamento.
      if (businessError) {
        throw businessError;
      }

      if (businessSettled) {
        this._log(
          'warn',
          'Falha na instrumentação da métrica; resultado do negócio preservado.',
          {
            metric: metricName,
            error: errorMessage(error),
          }
        );

        return businessResult;
      }

      this._log(
        'warn',
        'Falha ao iniciar medição; executando operação sem instrumentação.',
        {
          metric: metricName,
          error: errorMessage(error),
        }
      );

      return fn();
    }
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
            error: errorMessage(error),
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
  // LID → PHONE
  // ===========================================================================

  async resolveJidToPhone(
    primaryJid,
    altJid = null,
    socket = this.socket
  ) {
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

    if (primary.endsWith(WHATSAPP_SUFFIX)) {
      const phone = this.normalizePhone(primary.split('@')[0]);

      return this.isValidPhone(phone) ? phone : '';
    }

    if (primary.endsWith(LID_SUFFIX)) {
      return this._resolveLidToPhone(primary, socket);
    }

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

  async _resolveLidToPhone(lidJid, socket = this.socket) {
    const canonicalLid = this.normalizeLidJid(lidJid);

    if (!canonicalLid) return '';

    const cached = this._lidToPhoneCache.get(canonicalLid);

    if (cached) {
      if (
        typeof cached === 'object' &&
        cached.expiresAt &&
        cached.expiresAt > Date.now()
      ) {
        return cached.phone || '';
      }

      // Compatibilidade com entradas antigas que ainda sejam strings.
      if (typeof cached === 'string') {
        return cached;
      }

      this._lidToPhoneCache.delete(canonicalLid);
    }

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
        error: errorMessage(error),
      });
    }

    try {
      // Use o socket associado ao evento. this.socket pode já apontar
      // para uma nova geração quando esta operação assíncrona terminar.
      const mapping = socket?.signalRepository?.lidMapping;

      if (
        mapping &&
        typeof mapping.getPNForLID === 'function'
      ) {
        const pn = await mapping.getPNForLID(canonicalLid);

        const phone = this.normalizePhone(String(pn || ''));

        if (this.isValidPhone(phone)) {
          await this._persistLidMapping(canonicalLid, phone);
          this._rememberLidMapping(canonicalLid, phone);
          return phone;
        }
      }
    } catch (error) {
      this.logWarn('Falha ao resolver LID via signalRepository.', {
        lid: canonicalLid,
        error: errorMessage(error),
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
      const now = Date.now();

      for (const [key, value] of this._lidToPhoneCache) {
        if (
          value &&
          typeof value === 'object' &&
          value.expiresAt &&
          value.expiresAt <= now
        ) {
          this._lidToPhoneCache.delete(key);
        }
      }

      if (this._lidToPhoneCache.size >= MAX_LID_CACHE_SIZE) {
        const oldestKey =
          this._lidToPhoneCache.keys().next().value;

        if (oldestKey) {
          this._lidToPhoneCache.delete(oldestKey);
        }
      }

      this._lidToPhoneCache.set(lidJid, {
        phone,
        expiresAt: now + LID_CACHE_TTL_MS,
      });
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
        error: errorMessage(error),
      });
    }
  }

  _clearLidCache() {
    this._lidToPhoneCache.clear();
  }

  async _captureLidMappingFromResponse(response, chatId) {
    try {
      const usedJid = response?.key?.remoteJid;

      if (!usedJid || typeof usedJid !== 'string') {
        return;
      }

      if (!usedJid.endsWith(LID_SUFFIX)) {
        return;
      }

      const phone = this.normalizePhone(chatId);

      if (!this.isValidPhone(phone)) {
        return;
      }

      const canonicalLid = this.normalizeLidJid(usedJid);

      if (!canonicalLid) return;

      const cached = this._lidToPhoneCache.get(canonicalLid);

      if (
        cached &&
        typeof cached === 'object' &&
        cached.phone === phone &&
        cached.expiresAt > Date.now()
      ) {
        return;
      }

      if (cached === phone) {
        // Compatibilidade com cache legado.
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
        error: errorMessage(error),
      });
    }
  }

  // ===========================================================================
  // SOCKET LIFECYCLE
  // ===========================================================================

  async initialize() {
    if (this.isShuttingDown) return false;

    // Um reset invalida a sessão anterior. Quem pedir initialize durante
    // esse período aguarda a operação e usa o socket que ficar vigente.
    if (this._resetPromise) {
      try {
        await this._resetPromise;
      } catch (error) {
        this.logError(
          'Erro numa operação de reset aguardada por initialize().',
          error
        );
      }

      if (this.isShuttingDown) return false;
    }

    return this._startInitialization();
  }

  _startInitialization() {
    if (this.isShuttingDown) return Promise.resolve(false);

    if (this.isReady && this.socket) {
      return Promise.resolve(true);
    }

    if (this._initializePromise) {
      return this._initializePromise;
    }

    if (this.isConnecting) {
      return Promise.resolve(false);
    }

    const lifecycleGeneration = this._lifecycleGeneration;

    let trackedPromise;

    trackedPromise = this._initializeInternal(
      lifecycleGeneration
    ).finally(() => {
      if (this._initializePromise === trackedPromise) {
        this._initializePromise = null;
      }
    });

    this._initializePromise = trackedPromise;

    return trackedPromise;
  }

  async _initializeInternal(lifecycleGeneration) {
    this.clearRetryTimer();
    this.clearRecoveryTimer();

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
          { error: errorMessage(error) }
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
      const ownsLifecycle =
        lifecycleGeneration === this._lifecycleGeneration;

      if (ownsLifecycle) {
        this.isConnecting = false;
        this.isReady = false;
        this.qrCode = null;
      }

      this.logError('Erro ao inicializar WhatsApp.', error);

      // Uma inicialização antiga pode terminar depois de um reset/shutdown.
      // Nunca deixe essa operação antiga agendar um reconnect por cima da
      // geração atual.
      if (
        ownsLifecycle &&
        !this.isShuttingDown
      ) {
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

    socket.ev.on('creds.update', (creds) => {
      Promise.resolve(saveCreds(creds)).catch((error) => {
        this.logError('Erro ao persistir credenciais do WhatsApp.', error);
      });
    });

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
    this.clearRecoveryTimer();
    this.clearLogoutRestartTimer();

    this._safeMetricSet(
      metrics.whatsappConnected,
      1,
      'whatsappConnected'
    );

    const user = socket?.user;

    this.log('WhatsApp conectado e pronto para responder.', {
      session: user?.id || null,
      name: user?.name || null,
    });
  }

  // [FIX-2.5] três casos distintos.
  async handleConnectionClose(
    lastDisconnect,
    socket = this.socket,
    socketGeneration = this._socketGeneration
  ) {
    if (!this.isCurrentSocket(socket, socketGeneration)) {
      return;
    }

    const statusCode = this.getDisconnectStatusCode(lastDisconnect);

    this.socket = null;
    this.isReady = false;
    this.isConnecting = false;
    this.qrCode = null;
    this.connectedAt = null;

    this._safeMetricSet(
      metrics.whatsappConnected,
      0,
      'whatsappConnected'
    );

    this.log('WhatsApp desconectado.', {
      statusCode: statusCode || 'unknown',
    });

    // Caso 1: sessão explicitamente encerrada pelo WhatsApp.
    if (statusCode === DisconnectReason.loggedOut) {
      this.log(
        'Sessão encerrada pelo WhatsApp. Limpando credenciais '
        + 'e preparando novo login.'
      );

      this.clearAuthDirectory();
      this._clearLidCache();

      this._scheduleLogoutRestart();
      return;
    }

    // [FIX-2.5] Caso 2: outro dispositivo assumiu a sessão.
    // Retentar é inútil — só re-autenticação manual resolve.
    if (statusCode === STATUS_CONNECTION_REPLACED) {
      this.logError(
        'Sessão substituída por outro dispositivo. '
        + 'Re-autenticação manual necessária — sem retry automático.',
        null,
        { statusCode }
      );

      this._safeMetricInc(
        metrics.errorsTotal,
        { subsystem: 'whatsapp' },
        'errorsTotal'
      );

      // Não chamar scheduleReconnect. Operador tem de agir.
      return;
    }

    // Caso 3: queda transitória — backoff normal.
    if (this.isShuttingDown) return;

    this.scheduleReconnect();
  }

  getDisconnectStatusCode(lastDisconnect) {
    const raw =
      lastDisconnect?.error?.output?.statusCode ??
      lastDisconnect?.error?.statusCode ??
      null;

    const numeric = Number(raw);

    return Number.isFinite(numeric) ? numeric : raw;
  }

  // ===========================================================================
  // RECONNECT
  // ===========================================================================

  scheduleReconnect() {
    if (
      this.retryTimer ||
      this.recoveryTimer ||
      this.isShuttingDown ||
      this.isConnecting ||
      this.isReady ||
      this._resetPromise
    ) {
      return;
    }

    if (this.retryCount >= MAX_RETRIES) {
      this.logError(
        `Número máximo de tentativas atingido (${MAX_RETRIES}). ` +
        `A tentar recuperação automática em ` +
        `${Math.round(RETRY_RECOVERY_INTERVAL_MS / 60_000)} minutos.`
      );

      if (!this.recoveryTimer) {
        const lifecycleGeneration = this._lifecycleGeneration;

        this.recoveryTimer = setTimeout(() => {
          this.recoveryTimer = null;

          if (
            this.isShuttingDown ||
            this._resetPromise ||
            lifecycleGeneration !== this._lifecycleGeneration
          ) {
            return;
          }

          this.retryCount = 0;

          void this.initialize().catch((error) => {
            this.logError(
              'Falha na recuperação automática da conexão.',
              error
            );
            this.scheduleReconnect();
          });
        }, RETRY_RECOVERY_INTERVAL_MS);

        this.unrefTimer(this.recoveryTimer);
      }

      return;
    }

    this.retryCount += 1;

    const exponential = Math.min(
      INITIAL_RETRY_DELAY * Math.pow(2, this.retryCount - 1),
      MAX_RETRY_DELAY
    );

    const jitter = 0.8 + Math.random() * 0.4;
    const delay = Math.round(exponential * jitter);
    const lifecycleGeneration = this._lifecycleGeneration;

    this.log(
      `Reconexão ${this.retryCount}/${MAX_RETRIES} agendada.`,
      { delayMs: delay }
    );

    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;

      if (
        this.isShuttingDown ||
        this._resetPromise ||
        lifecycleGeneration !== this._lifecycleGeneration
      ) {
        return;
      }

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

  clearRecoveryTimer() {
    if (!this.recoveryTimer) return;

    clearTimeout(this.recoveryTimer);
    this.recoveryTimer = null;
  }

  _scheduleLogoutRestart() {
    if (
      this.logoutRestartTimer ||
      this.isShuttingDown ||
      this._resetPromise
    ) {
      return;
    }

    const lifecycleGeneration = this._lifecycleGeneration;

    this.logoutRestartTimer = setTimeout(() => {
      this.logoutRestartTimer = null;

      if (
        this.isShuttingDown ||
        this._resetPromise ||
        lifecycleGeneration !== this._lifecycleGeneration
      ) {
        return;
      }

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

  _rememberIncomingMessage(messageId) {
    const id = clean(messageId);
    if (!id) return true;

    const now = Date.now();
    const previous = this._incomingMessageCache.get(id);

    if (previous && now - previous < INCOMING_DEDUPE_TTL_MS) {
      return false;
    }

    this._incomingMessageCache.set(id, now);

    if (this._incomingMessageCache.size > INCOMING_DEDUPE_MAX_SIZE) {
      for (const [key, timestamp] of this._incomingMessageCache) {
        if (now - timestamp > INCOMING_DEDUPE_TTL_MS) {
          this._incomingMessageCache.delete(key);
        }

        if (this._incomingMessageCache.size <= INCOMING_DEDUPE_MAX_SIZE) {
          break;
        }
      }
    }

    return true;
  }

  _clearIncomingMessageCache() {
    this._incomingMessageCache.clear();
  }

  _allowLocalRateLimit() {
    const now = Date.now();

    this._localRateLimit = this._localRateLimit.filter(
      (timestamp) =>
        now - timestamp < LOCAL_RATE_LIMIT_WINDOW_MS
    );

    if (this._localRateLimit.length >= LOCAL_RATE_LIMIT_MAX) {
      return false;
    }

    this._localRateLimit.push(now);
    return true;
  }

  _clearLocalRateLimit() {
    this._localRateLimit.length = 0;
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
      // O listener do Baileys permanece livre; cada mensagem é tratada
      // de forma independente e nunca bloqueia a entrega dos próximos eventos.
      void this.processIncomingMessage(
        message,
        socket,
        socketGeneration
      ).catch((error) => {
        this.logError(
          'Erro no processamento assíncrono da mensagem.',
          error,
          { msgId: message?.key?.id || null }
        );
      });
    }
  }

  async processIncomingMessage(
    msg,
    socket = this.socket,
    socketGeneration = this._socketGeneration
  ) {
    if (!this.isCurrentSocket(socket, socketGeneration)) {
      return;
    }

    if (!msg?.message) return;
    if (msg.key?.fromMe) return;

    const messageId = clean(msg.key?.id);

    if (messageId && !this._rememberIncomingMessage(messageId)) {
      this._safeMetricInc(
        metrics.messagesReceived,
        { type: 'duplicate' },
        'messagesReceived'
      );
      return;
    }

    const rawJid = msg.key?.remoteJid;

    if (!rawJid || rawJid === STATUS_BROADCAST) return;

    if (
      rawJid.endsWith(GROUP_SUFFIX) ||
      rawJid.endsWith(NEWSLETTER_SUFFIX)
    ) {
      return;
    }

    const altJid =
      msg.key?.remoteJidAlt ||
      msg.key?.participantAlt ||
      null;

    // Nunca use this.socket aqui: durante uma reconexão ele pode já ser
    // uma geração diferente da que originou o evento.
    const phone = await this.resolveJidToPhone(
      rawJid,
      altJid,
      socket
    );

    if (!phone) {
      this.logWarn(
        'Mensagem ignorada: JID não resolvível para telefone.',
        {
          remoteJid: rawJid,
          altJid,
          msgId: msg.key?.id || null,
        }
      );

      this._safeMetricInc(
        metrics.errorsTotal,
        { subsystem: 'whatsapp' },
        'errorsTotal'
      );

      await this._reportUnresolvableIncoming({
        rawJid,
        altJid,
        message: msg,
      });

      return;
    }

    const parsed = this.extractMessageContent(msg);

    if (
      !parsed.text &&
      !parsed.isButtonClick &&
      !parsed.isMedia
    ) {
      return;
    }

    const type = parsed.isButtonClick
      ? 'button'
      : parsed.isMedia
        ? 'media'
        : parsed.text
          ? 'text'
          : 'other';

    this._safeMetricInc(
      metrics.messagesReceived,
      { type },
      'messagesReceived'
    );

    this.log('Mensagem recebida.', {
      phone,
      remoteJid: rawJid,
      msgId: msg.key?.id || null,
      type,
      textLength: parsed.text ? parsed.text.length : 0,
      mediaType: parsed.mediaType || null,
    });

    await this.handleMessage(
      phone,
      parsed.text,
      parsed.isButtonClick,
      msg.key?.id || null,
      {
        isMedia: parsed.isMedia,
        mediaType: parsed.mediaType,
      }
    );
  }

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

      await reportUnmatched({
        phone: null,
        jid: rawJid,
        altJid,
        message: parsed.text || '(mensagem sem texto)',
        messageId: message?.key?.id || null,
      });
    } catch (error) {
      this.logWarn('Falha ao reportar mensagem órfã.', {
        remoteJid: rawJid,
        error: errorMessage(error),
      });
    }
  }

  // ===========================================================================
  // EXTRAÇÃO
  // ===========================================================================

  extractMessageContent(msg) {
    if (!msg?.message) {
      return { text: '', isButtonClick: false, isMedia: false };
    }

    const message =
      normalizeMessageContent(msg.message) || msg.message;

    if (message.conversation) {
      return {
        text: String(message.conversation).trim(),
        isButtonClick: false,
        isMedia: false,
      };
    }

    if (message.extendedTextMessage?.text) {
      return {
        text: String(message.extendedTextMessage.text).trim(),
        isButtonClick: false,
        isMedia: false,
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
        isMedia: false,
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
        isMedia: false,
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
        isMedia: false,
      };
    }

    // Mídia é sinalizada explicitamente. O texto fica vazio quando não
    // existe legenda, permitindo que handleMessage execute a resposta
    // específica para ficheiros em vez de tratar o placeholder como texto.
    for (const mediaType of MEDIA_MESSAGE_TYPES) {
      const media = message[mediaType];

      if (media) {
        return {
          text: clean(media.caption || ''),
          isButtonClick: false,
          isMedia: true,
          mediaType,
        };
      }
    }

    return { text: '', isButtonClick: false, isMedia: false };
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
              isMedia: false,
            };
          }
        } catch (_) {
          // paramsJson pode não ser JSON válido.
        }

        return {
          text: String(nativeFlow.text || params).trim(),
          isButtonClick: true,
          isMedia: false,
        };
      }

      return {
        text: String(nativeFlow.text || '').trim(),
        isButtonClick: true,
        isMedia: false,
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
        isMedia: false,
      };
    }

    return { text: '', isButtonClick: false, isMedia: false };
  }

  // ===========================================================================
  // PROCESSAMENTO DE NEGÓCIO
  // ===========================================================================

  async handleMessage(
    from,
    text,
    isButton = false,
    messageId = null,
    options = {}
  ) {
    if (!from) return;

    const normalizedText = String(text || '').trim();

    // Mídia sem legenda precisa chegar aqui com text vazio. Isso evita
    // confundir a indicação de mídia com uma mensagem textual.
    if (options.isMedia && !normalizedText) {
      await this.sendMessage(
        from,
        'Recebi o seu ficheiro. Nesta conversa só consigo ler texto. '
        + 'Pode escrever o que quiser partilhar?'
      );
      return;
    }

    if (!normalizedText) return;

    if (this.isResetCommand(normalizedText)) {
      const resetResult =
        await this.resetInterviewConversation(from);

      await this.sendMessage(
        from,
        resetResult
          ? 'A ligação a esta conversa foi limpa. Se quiser recomeçar '
            + 'o processo, contacte a equipa de RH.'
          : 'Não foi possível limpar completamente a conversa agora. '
            + 'Tente novamente em instantes ou contacte a equipa de RH.'
      );

      return;
    }

    try {
      await this.interviewService.handleIncomingMessage(
        from,
        normalizedText,
        {
          messageId,
          isButton,
          isMedia: Boolean(options.isMedia),
          mediaType: options.mediaType || null,
        }
      );
    } catch (error) {
      this.logError('Erro ao processar mensagem.', error, {
        phone: from,
        msgId: messageId,
      });
    }
  }

  async resetInterviewConversation(from) {
    const lockKey = this.normalizePhone(from) || String(from || '').trim();

    if (!lockKey) return false;

    const existing = this._conversationResetLocks.get(lockKey);

    if (existing) {
      return existing;
    }

    const promise = this._resetInterviewConversationInternal(from)
      .catch((error) => {
        this.logError(
          'Erro inesperado ao executar reset da conversa.',
          error,
          { phone: from }
        );

        return false;
      })
      .finally(() => {
        this._conversationResetLocks.delete(lockKey);
      });

    this._conversationResetLocks.set(lockKey, promise);

    return promise;
  }

  async _resetInterviewConversationInternal(from) {
    let interviewId = null;

    try {
      // [FIX-2.7] Resolver PRIMEIRO. O estado local não pode ser apagado
      // antes de descobrir qual entrevista deve ser cancelada no backend.
      if (
        typeof this.interviewService.resolveInterviewId === 'function'
      ) {
        interviewId =
          await this.interviewService.resolveInterviewId(from);
      }

      if (interviewId) {
        const cancelInterview =
          this.interviewService.yane?.cancelInterview;

        if (typeof cancelInterview !== 'function') {
          throw new Error(
            'cancelInterview indisponível no backend para reset de entrevista.'
          );
        }

        await cancelInterview.call(
          this.interviewService.yane,
          interviewId,
          'candidate_reset'
        );
      }

      // Só depois da sincronização com o backend limpamos o estado local.
      if (
        typeof this.interviewService.forgetInterview !== 'function'
      ) {
        throw new Error(
          'forgetInterview indisponível no InterviewService.'
        );
      }

      await this.interviewService.forgetInterview(from);

      this.log('Reset de conversa concluído.', {
        phone: from,
        session: interviewId || null,
      });

      return true;
    } catch (error) {
      this.logWarn('Falha ao sincronizar reset da entrevista.', {
        phone: from,
        session: interviewId || null,
        error: errorMessage(error),
      });

      // Importante: não apagamos o estado local se o cancelamento backend
      // de uma entrevista existente falhar. Isso evita deixar Redis e backend
      // em estados divergentes.
      return false;
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

      // Read receipt é best-effort. Não aguardamos o backend dentro da fila
      // de updates do Baileys, evitando que um request lento atrase os demais.
      void this.handleReadReceipt(
        key,
        socket,
        socketGeneration
      ).catch((error) => {
        this.logError('Erro ao processar read receipt.', error, {
          msgId: key?.id || null,
        });
      });
    }
  }

  async handleReadReceipt(
    key,
    socket = this.socket,
    socketGeneration = this._socketGeneration
  ) {
    if (!this.isCurrentSocket(socket, socketGeneration)) {
      return;
    }

    const rawJid = key?.remoteJid;
    const messageId = key?.id;

    if (!rawJid || !messageId) return;

    const inFlightKey = `${rawJid}|${messageId}`;

    if (this._readReceiptInFlight.has(inFlightKey)) {
      return;
    }

    this._readReceiptInFlight.add(inFlightKey);

    try {
      const altJid = key?.remoteJidAlt || key?.participantAlt || null;

      const phone = await this.resolveJidToPhone(
        rawJid,
        altJid,
        socket
      );

      if (!phone) return;

      if (this.isReadReceiptDebounced(phone)) return;

      this.lastReadTimestamps.set(phone, Date.now());
      this.trimReadCache();

      this.log('Mensagem lida pelo candidato.', {
        phone,
        msgId: messageId,
        participant: key?.participant || null,
      });

      void this.notifyBackendMessageRead({
        phone,
        messageId,
      }).catch((error) => {
        this.logError(
          'Erro assíncrono ao enviar status de leitura.',
          error,
          { phone, msgId: messageId }
        );
      });
    } finally {
      this._readReceiptInFlight.delete(inFlightKey);
    }
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
        this._safeMetricInc(
          metrics.messagesSent,
          { status: 'rate_limited' },
          'messagesSent'
        );

        this._safeMetricInc(
          metrics.rateLimitHits,
          { bucket: 'out' },
          'rateLimitHits'
        );

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
        const response = await this._safeMetricTime(
          metrics.messageSendDuration,
          { attempt: String(attempt) },
          () => socket.sendMessage(chatId, payload),
          'messageSendDuration'
        );

        void this._captureLidMappingFromResponse(
          response,
          chatId
        ).catch((error) => {
          this.logWarn(
            'Falha assíncrona ao guardar mapeamento LID → PN.',
            { error: errorMessage(error) }
          );
        });

        this._safeMetricInc(
          metrics.messagesSent,
          { status: 'success' },
          'messagesSent'
        );

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
            error: errorMessage(error),
            code: error?.code || null,
          }
        );

        await this.delay(backoff);
      }
    }

    this._safeMetricInc(
      metrics.messagesSent,
      { status: 'failed' },
      'messagesSent'
    );

    this._safeMetricInc(
      metrics.errorsTotal,
      { subsystem: 'whatsapp' },
      'errorsTotal'
    );

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

        if (allowed) {
          this._allowLocalRateLimit();
          return true;
        }
      } catch (error) {
        // Redis continua a ser o rate limiter distribuído principal.
        // Em degradação, usamos uma proteção local para não paralisar
        // completamente as entrevistas.
        //
        // [FIX-2.9] `errorMessage` era referenciado aqui sem estar
        // definido. Como este caminho corre precisamente quando o Redis
        // cai, o ReferenceError mascarava o warning. Corrigido com o
        // helper reintroduzido no topo do módulo.
        this.logWarn(
          'Rate limiter Redis indisponível; usando limite local.',
          {
            bucket: 'out',
            error: errorMessage(error),
          }
        );

        return this._allowLocalRateLimit();
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
        error: errorMessage(error),
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

    const sessionId = this.socket?.user?.id || null;

    return {
      connected,
      ready: connected,
      connecting: Boolean(this.isConnecting),
      qr_code: this.qrCode,

      session: sessionId?.split(':')[0] || null,

      connected_at: this.connectedAt
        ? new Date(this.connectedAt).toISOString()
        : null,

      retry_count: this.retryCount,
      recovery_scheduled: Boolean(this.recoveryTimer),

      message: connected
        ? 'WhatsApp conectado e pronto'
        : this.qrCode
          ? 'Aguardando escaneamento do QR Code'
          : this.isConnecting
            ? 'A iniciar sessão...'
            : this.recoveryTimer
              ? 'Ligação indisponível; recuperação automática agendada'
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
    if (this.isShuttingDown) {
      return {
        success: false,
        message: 'O serviço está a encerrar.',
      };
    }

    if (this._resetPromise) {
      return this._resetPromise;
    }

    let trackedResetPromise;

    trackedResetPromise = this._performSessionReset().finally(() => {
      if (this._resetPromise === trackedResetPromise) {
        this._resetPromise = null;
      }
    });

    this._resetPromise = trackedResetPromise;

    return trackedResetPromise;
  }

  async _performSessionReset() {
    // Invalida imediatamente listeners/socket da geração anterior.
    this._lifecycleGeneration += 1;
    const previousInitialize = this._initializePromise;

    this.isConnecting = true;
    this.isReady = false;

    try {
      this.clearRetryTimer();
      this.clearRecoveryTimer();
      this.clearLogoutRestartTimer();

      // Fechar primeiro impede que uma sessão antiga continue a receber
      // eventos enquanto a nova autenticação está a ser preparada.
      await this.closeSocket();

      // O initialize antigo pode ainda estar dentro de useMultiFileAuthState()
      // ou fetchLatestBaileysVersion(). Aguarde-o antes de criar outro socket.
      if (previousInitialize) {
        try {
          await previousInitialize;
        } catch (error) {
          this.logWarn(
            'Inicialização anterior terminou com erro durante reset.',
            { error: errorMessage(error) }
          );
        }
      }

      if (this.isShuttingDown) {
        return {
          success: false,
          message: 'O serviço está a encerrar.',
        };
      }

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

      // Não chamar initialize() aqui: _resetPromise ainda está ativo e
      // initialize() aguarda esse mesmo promise. Usamos a entrada privada
      // que já está protegida pelo reset atual.
      const started = await this._startInitialization();

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
          errorMessage(error) || 'Falha ao reiniciar sessão.',
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
    this._readReceiptInFlight.clear();
    this._clearIncomingMessageCache();
    this._clearLocalRateLimit();
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
    this.clearRecoveryTimer();
    this.clearLogoutRestartTimer();

    this.isReady = false;
    this.isConnecting = false;
    this.qrCode = null;
    this.connectedAt = null;

    this._safeMetricSet(
      metrics.whatsappConnected,
      0,
      'whatsappConnected'
    );

    this.log('Shutdown do WhatsApp iniciado.');

    try {
      process.removeListener('SIGTERM', this._boundShutdown);
      process.removeListener('SIGINT', this._boundShutdown);
    } catch (_) {
      // Best-effort.
    }

    // Invalida listeners imediatamente; operações antigas verão
    // isShuttingDown/lifecycleGeneration e deixam de criar novo socket.
    const socket = this.socket;

    this.socket = null;
    this._socketGeneration += 1;

    const inFlightLifecycle = [
      this._initializePromise,
      this._resetPromise,
    ].filter(Boolean);

    if (inFlightLifecycle.length) {
      await Promise.allSettled(inFlightLifecycle);
    }

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
    this._clearIncomingMessageCache();
    this._clearLocalRateLimit();
    this.lastReadTimestamps.clear();
    this._readReceiptInFlight.clear();
    this._conversationResetLocks.clear();

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