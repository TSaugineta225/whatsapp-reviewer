// src/services/redis.service.js
//
// Cliente Redis central do Yane Bot.
//
// [FIX-CRITICAL]
//   Removida a dependência de `this._scripts` (defineCommand).
//   O boot corria `_waitForReady` antes de `_defineScripts`, e se o
//   primeiro timeout batesse, os scripts nunca eram definidos e
//   `enqueuePendingItem` / `allowRate` / `releaseLock` falhavam em
//   silêncio com "this._scripts.X is not a function".
//
//   Todos os scripts Lua são agora passados inline ao `client.eval()`.
//   É ligeiramente mais caro em CPU, mas é atómico e nunca depende do
//   resultado de uma inicialização prévia. Para um bot que processa
//   algumas mensagens por segundo é irrelevante.
//
// [FIX-AVAILABILITY]
//   isAvailable() depende apenas de client.status === 'ready'.

'use strict';

const Redis = require('ioredis');
const crypto = require('crypto');
const pino = require('pino');

const DEFAULT_URL = 'redis://localhost:6379';
const DEFAULT_KEY_PREFIX = 'yane';

const PHONE_MAP_TTL = 7 * 24 * 60 * 60;
const LID_MAP_TTL = 30 * 24 * 60 * 60;
const DEDUPE_TTL = 24 * 60 * 60;

const LOCK_TTL = 300;
const LOCK_RENEW_INTERVAL_MS = 30_000;

const RATE_WINDOW = 1;
const CONNECT_TIMEOUT_MS = 10_000;
const ERROR_LOG_COOLDOWN_MS = 10_000;

// =============================================================================
// SCRIPTS LUA — inline por chamada, sem defineCommand
// =============================================================================

const RELEASE_LOCK_SCRIPT = `
  if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
  end
  return 0
`;

const REFRESH_LOCK_SCRIPT = `
  if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('EXPIRE', KEYS[1], ARGV[2])
  end
  return 0
`;

const ENQUEUE_PENDING_SCRIPT = `
  local len = redis.call('LLEN', KEYS[1])
  if len >= tonumber(ARGV[1]) then
    return 0
  end
  redis.call('LPUSH', KEYS[1], ARGV[2])
  redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
  return 1
`;

const RESCHEDULE_PENDING_SCRIPT = `
  if redis.call('LLEN', KEYS[1]) == 0 then
    return 0
  end
  redis.call('LSET', KEYS[1], -1, ARGV[1])
  redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
  return 1
`;

const DEAD_LETTER_SCRIPT = `
  local item = redis.call('RPOP', KEYS[1])
  if not item then
    return 0
  end
  local deadItem = ARGV[1]
  if not deadItem or deadItem == '' then
    deadItem = item
  end
  redis.call('LPUSH', KEYS[3], deadItem)
  if redis.call('LLEN', KEYS[1]) == 0 then
    redis.call('ZREM', KEYS[2], ARGV[2])
  end
  return 1
`;

const RATE_LIMIT_SCRIPT = `
  local current = redis.call('INCR', KEYS[1])
  if current == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
  end
  if current > tonumber(ARGV[1]) then
    return 0
  end
  return 1
`;

// =============================================================================
// SERVIÇO
// =============================================================================

class RedisService {
  constructor(options = {}) {
    this.prefix = String(
      options.prefix || process.env.REDIS_PREFIX || DEFAULT_KEY_PREFIX
    );

    this.url = String(
      options.url || process.env.REDIS_URL || DEFAULT_URL
    );

    this.logger = options.logger || pino({
      level: process.env.LOG_LEVEL || 'info',
      base: { service: 'redis' },
    });

    this.client = null;
    this.isReady = false;
    this.initializing = null;
    this._closing = false;

    this._lockRefreshInFlight = new Set();
    this._lastErrorLog = new Map();
  }

  // ===========================================================================
  // LOGGING
  // ===========================================================================

  _log(level, message, context = {}) {
    try {
      const method = this.logger?.[level];
      if (typeof method === 'function') {
        method.call(this.logger, context, message);
      }
    } catch (_) {}
  }

  log(message, context = {}) {
    this._log('info', message, context);
  }

  logWarn(message, context = {}) {
    this._log('warn', message, context);
  }

  logError(message, error = null, context = {}) {
    const safe = { ...context };
    if (error) {
      safe.error = error?.message || String(error);
      safe.code = error?.code || null;
    }
    this._log('error', message, safe);
  }

  _logThrottled(operation, prefix, error) {
    const message = error?.message || String(error);
    const now = Date.now();
    const last = this._lastErrorLog.get(operation) || 0;

    if (now - last < ERROR_LOG_COOLDOWN_MS) return;

    this._lastErrorLog.set(operation, now);

    try {
      console.error(prefix, message);
    } catch (_) {}
  }

  // ===========================================================================
  // LIFECYCLE
  // ===========================================================================

  async initialize() {
    if (this.isReady && this.client) return true;
    if (this.initializing) return this.initializing;
    if (this._closing) return false;

    this.initializing = this._initialize();
    try {
      return await this.initializing;
    } finally {
      this.initializing = null;
    }
  }

  async _initialize() {
    const previousClient = this.client;
    if (previousClient) {
      try { previousClient.disconnect(); } catch (_) {}
    }

    this.client = null;
    this.isReady = false;

    const client = new Redis(this.url, {
      maxRetriesPerRequest: 3,
      enableReadyCheck: true,
      lazyConnect: false,
      connectTimeout: CONNECT_TIMEOUT_MS,
      retryStrategy: (times) => Math.min(times * 200, 3_000),
    });

    this.client = client;
    this._attachClientEvents(client);

    try {
      await this._waitForReady(client, CONNECT_TIMEOUT_MS);
    } catch (error) {
      if (this.client === client) {
        this.client = null;
        this.isReady = false;
      }
      try { client.disconnect(); } catch (_) {}
      throw error;
    }

    this.log('RedisService inicializado.', {
      url: this._maskUrl(this.url),
      prefix: this.prefix,
    });

    return true;
  }

  _attachClientEvents(client) {
    client.on('ready', () => {
      if (this.client !== client) return;
      this.isReady = true;
      this.log('Redis pronto.');
    });

    client.on('error', (error) => {
      if (this.client !== client) return;
      this.isReady = false;
      this._logThrottled('connection', '[REDIS] Erro:', error);
    });

    client.on('close', () => {
      if (this.client !== client) return;
      this.isReady = false;
    });

    client.on('end', () => {
      if (this.client !== client) return;
      this.isReady = false;
    });

    client.on('reconnecting', () => {
      if (this.client !== client) return;
      this.isReady = false;
    });
  }

  _waitForReady(client, timeoutMs) {
    return new Promise((resolve, reject) => {
      let settled = false;

      const timer = setTimeout(() => {
        finish(() => reject(new Error('redis_ready_timeout')));
      }, timeoutMs);

      const cleanup = () => {
        clearTimeout(timer);
        client.removeListener('ready', onReady);
        client.removeListener('end', onEnd);
        client.removeListener('error', onError);
      };

      const finish = (fn) => {
        if (settled) return;
        settled = true;
        cleanup();
        fn();
      };

      const onReady = () => finish(resolve);
      const onEnd = () => finish(() => reject(new Error('redis_closed')));
      const onError = (err) => finish(() => reject(err));

      client.once('ready', onReady);
      client.once('end', onEnd);
      client.once('error', onError);

      if (this.client === client && client.status === 'ready') {
        finish(resolve);
      }
    });
  }

  async close() {
    if (this._closing) return;
    this._closing = true;

    const client = this.client;
    this.client = null;
    this.isReady = false;

    if (!client) return;

    try {
      await client.quit();
    } catch (error) {
      try { client.disconnect(); } catch (_) {}
    }

    this.log('RedisService encerrado.');
  }

  // ===========================================================================
  // ESTADO
  // ===========================================================================

  isAvailable() {
    return Boolean(
      this.client &&
      this.isReady &&
      this.client.status === 'ready'
    );
  }

  _maskUrl(url) {
    try {
      const parsed = new URL(url);
      if (parsed.password) parsed.password = '***';
      return parsed.toString();
    } catch (_) {
      return url;
    }
  }

  // ===========================================================================
  // CHAVES
  // ===========================================================================

  key(...parts) {
    return [this.prefix, ...parts]
      .filter((p) => p !== undefined && p !== null && String(p).length > 0)
      .map((p) => String(p))
      .join(':');
  }

  _safeKey(key) {
    const raw = String(key || '');
    if (raw.length <= 40) return raw;
    return `${raw.slice(0, 20)}...${raw.slice(-10)}`;
  }

  // ===========================================================================
  // OPERAÇÕES BÁSICAS
  // ===========================================================================

  async _execute(operation, fallback, fn) {
    if (!this.isAvailable()) return fallback;

    const client = this.client;

    try {
      return await fn(client);
    } catch (error) {
      this._logThrottled(operation, `[REDIS] ${operation} falhou:`, error);
      return fallback;
    }
  }

  async get(key) {
    return this._execute('GET', null, (client) => client.get(key));
  }

  async set(key, value, ttlSeconds = null) {
    return this._execute('SET', false, async (client) => {
      const ttl = this._normalizeTtl(ttlSeconds);
      if (ttl) {
        await client.set(key, value, 'EX', ttl);
      } else {
        await client.set(key, value);
      }
      return true;
    });
  }

  async del(key) {
    return this._execute('DEL', false, async (client) => {
      await client.del(key);
      return true;
    });
  }

  async setnx(key, value, ttlSeconds = null) {
    return this._execute('SETNX', false, async (client) => {
      const ttl = this._normalizeTtl(ttlSeconds);
      const result = ttl
        ? await client.set(key, value, 'EX', ttl, 'NX')
        : await client.set(key, value, 'NX');
      return result === 'OK';
    });
  }

  async incr(key, ttlSeconds = null) {
    return this._execute('INCR', 0, async (client) => {
      const value = await client.incr(key);
      const ttl = this._normalizeTtl(ttlSeconds);
      if (value === 1 && ttl) await client.expire(key, ttl);
      return value;
    });
  }

  _normalizeTtl(ttlSeconds) {
    if (ttlSeconds === null || ttlSeconds === undefined) return null;
    const ttl = Number(ttlSeconds);
    if (!Number.isFinite(ttl) || ttl <= 0) return null;
    return Math.max(1, Math.floor(ttl));
  }

  // ===========================================================================
  // LISTAS
  // ===========================================================================

  async lpush(key, value) {
    return this._execute('LPUSH', false, async (client) => {
      await client.lpush(key, value);
      return true;
    });
  }

  async rpop(key) {
    return this._execute('RPOP', null, (client) => client.rpop(key));
  }

  async lindex(key, index = 0) {
    return this._execute('LINDEX', null, (client) => client.lindex(key, index));
  }

  async llen(key) {
    return this._execute('LLEN', 0, (client) => client.llen(key));
  }

  // ===========================================================================
  // SORTED SETS
  // ===========================================================================

  async zrem(key, member) {
    return this._execute('ZREM', false, async (client) => {
      await client.zrem(key, member);
      return true;
    });
  }

  async zrangeByScore(key, min, max, offset = 0, count = 10) {
    return this._execute('ZRANGEBYSCORE', [], (client) =>
      client.zrangebyscore(key, min, max, 'LIMIT', offset, count)
    );
  }

  // ===========================================================================
  // MAPEAMENTO TELEFONE ↔ ENTREVISTA
  // ===========================================================================

  async rememberInterview(phone, interviewId, ttl = PHONE_MAP_TTL) {
    if (!phone || !interviewId) return false;
    return this.set(this.key('phone', phone), String(interviewId), ttl);
  }

  async resolveInterviewId(phone) {
    if (!phone) return null;
    return this.get(this.key('phone', phone));
  }

  async forgetInterview(phone) {
    if (!phone) return false;
    return this.del(this.key('phone', phone));
  }

  // ===========================================================================
  // MAPEAMENTO LID ↔ PHONE
  // ===========================================================================

  _normalizeLidKey(lidJid) {
    const value = String(lidJid || '').trim();
    if (!value) return '';
    return value.split('@')[0].split(':')[0].trim();
  }

  async rememberLidMapping(lidJid, phone, ttl = LID_MAP_TTL) {
    const local = this._normalizeLidKey(lidJid);
    if (!local || !phone) return false;
    return this.set(this.key('lid', local), String(phone), ttl);
  }

  async resolveLidMapping(lidJid) {
    const local = this._normalizeLidKey(lidJid);
    if (!local) return null;
    return this.get(this.key('lid', local));
  }

  async forgetLidMapping(lidJid) {
    const local = this._normalizeLidKey(lidJid);
    if (!local) return false;
    return this.del(this.key('lid', local));
  }

  // ===========================================================================
  // DEDUPE
  // ===========================================================================

  async markMessageSeen(messageId, ttl = DEDUPE_TTL) {
    if (!messageId) return true;
    return this.setnx(this.key('msg', messageId), '1', ttl);
  }

  // ===========================================================================
  // LOCKS DISTRIBUÍDOS — eval() inline
  // ===========================================================================

  _createLockToken() {
    return `${Date.now().toString(36)}-${crypto.randomBytes(16).toString('hex')}`;
  }

  async acquireLock(phone, ttl = LOCK_TTL) {
    if (!phone) return null;
    const key = this.key('lock', phone);
    const token = this._createLockToken();
    const acquired = await this.setnx(key, token, ttl);
    return acquired ? token : null;
  }

  async releaseLock(phone, token) {
    if (!phone || !token || !this.isAvailable()) return false;

    const client = this.client;
    const key = this.key('lock', phone);

    try {
      const result = await client.eval(RELEASE_LOCK_SCRIPT, 1, key, token);
      return Number(result) === 1;
    } catch (error) {
      this._logThrottled('release-lock', '[REDIS] release-lock falhou:', error);
      return false;
    }
  }

  async refreshLock(phone, token, ttl = LOCK_TTL) {
    if (!phone || !token || !this.isAvailable()) return false;

    const normalizedTtl = this._normalizeTtl(ttl);
    if (!normalizedTtl) return false;

    const client = this.client;
    const key = this.key('lock', phone);

    try {
      const result = await client.eval(
        REFRESH_LOCK_SCRIPT, 1, key, token, String(normalizedTtl)
      );
      return Number(result) === 1;
    } catch (error) {
      this._logThrottled('refresh-lock', '[REDIS] refresh-lock falhou:', error);
      return false;
    }
  }

  async withPhoneLock(phone, fn, options = {}) {
    const ttl = this._normalizeTtl(options.ttl ?? LOCK_TTL) || LOCK_TTL;
    const renewInterval = Math.max(
      1_000,
      Number(options.renewIntervalMs ?? LOCK_RENEW_INTERVAL_MS) || LOCK_RENEW_INTERVAL_MS
    );

    const token = await this.acquireLock(phone, ttl);
    if (!token) return null;

    let stopped = false;
    let refreshing = false;

    const heartbeat = setInterval(async () => {
      if (stopped || refreshing) return;
      refreshing = true;
      try {
        await this.refreshLock(phone, token, ttl);
      } finally {
        refreshing = false;
      }
    }, renewInterval);

    if (typeof heartbeat.unref === 'function') heartbeat.unref();

    try {
      return await fn();
    } finally {
      stopped = true;
      clearInterval(heartbeat);
      await this.releaseLock(phone, token);
    }
  }

  // ===========================================================================
  // RATE LIMIT — eval() inline
  // ===========================================================================

  async allowRate(bucket = 'out', maxPerSecond = 60) {
    if (!Number.isFinite(maxPerSecond) || maxPerSecond <= 0) return false;
    if (!this.isAvailable()) return true; // fail-open

    const second = Math.floor(Date.now() / 1000);
    const key = this.key('rate', bucket, second);
    const client = this.client;

    try {
      const result = await client.eval(
        RATE_LIMIT_SCRIPT, 1, key,
        String(maxPerSecond), String(RATE_WINDOW + 1)
      );
      return Number(result) === 1;
    } catch (error) {
      this._logThrottled('rate-limit', '[REDIS] rate-limit falhou:', error);
      return true; // fail-open: não bloquear envios por erro de Redis
    }
  }

  // ===========================================================================
  // FILA DE PENDENTES — eval() inline
  // ===========================================================================

  async enqueuePendingItem({ queueKey, phonesKey, phone, payload, score, maxItems = 20 }) {
    if (!this.isAvailable() || !queueKey || !phonesKey || !phone || maxItems <= 0) {
      return 0;
    }

    const client = this.client;

    try {
      const result = await client.eval(
        ENQUEUE_PENDING_SCRIPT, 2, queueKey, phonesKey,
        String(maxItems), String(payload ?? ''), String(score), String(phone)
      );
      return Number(result) || 0;
    } catch (error) {
      this._logThrottled('enqueue-pending', '[REDIS] enqueuePendingItem falhou:', error);
      return 0;
    }
  }

  async reschedulePendingItem({ queueKey, phonesKey, phone, payload, score }) {
    if (!this.isAvailable() || !queueKey || !phonesKey || !phone) return false;

    const client = this.client;

    try {
      const result = await client.eval(
        RESCHEDULE_PENDING_SCRIPT, 2, queueKey, phonesKey,
        String(payload ?? ''), String(score), String(phone)
      );
      return Number(result) === 1;
    } catch (error) {
      this._logThrottled('reschedule-pending', '[REDIS] reschedulePendingItem falhou:', error);
      return false;
    }
  }

  async movePendingToDeadLetter({ queueKey, phonesKey, deadKey, phone, payload }) {
    if (!this.isAvailable() || !queueKey || !phonesKey || !deadKey || !phone) {
      return false;
    }

    const client = this.client;

    try {
      const result = await client.eval(
        DEAD_LETTER_SCRIPT, 3, queueKey, phonesKey, deadKey,
        String(payload ?? ''), String(phone)
      );
      return Number(result) === 1;
    } catch (error) {
      this._logThrottled('dead-letter', '[REDIS] movePendingToDeadLetter falhou:', error);
      return false;
    }
  }
}

module.exports = RedisService;