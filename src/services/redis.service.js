// src/services/redis.service.js
//
// Camada de acesso ao Redis para coordenação do serviço WhatsApp.
//
// RESPONSABILIDADES
// -----------------
//   - conexão resiliente (reconnect com backoff e limite);
//   - namespacing consistente (`prefix:part:part`);
//   - mapeamento telefone ↔ entrevista (TTL longo);
//   - mapeamento LID ↔ PN (TTL longo);
//   - dedupe de mensagens recebidas (SET NX);
//   - locks distribuídos por telefone (SET NX PX + refresh);
//   - fila pendente por telefone (LIST + ZSET);
//   - dead-letter (LIST);
//   - rate limit por bucket (INCR + EXPIRE);
//   - cache de routing resolvido (TTL curto);
//   - estado de disambiguação (TTL curto).
//
// [FIX-REDIS-1]
//   Métodos de routing adicionados para suportar o
//   InterviewService v9.3:
//     - getDisambiguation / setDisambiguation / clearDisambiguation
//     - getResolvedInterview / rememberResolvedInterview /
//       clearResolvedInterview
//
// [FIX-REDIS-2]
//   Operações críticas (enqueue, reschedule, move-to-dead-letter,
//   release-lock, refresh-lock) executam via Lua para garantir
//   atomicidade entre LIST e ZSET.
//
// [FIX-REDIS-3]
//   isAvailable() baseado no estado real do cliente ioredis
//   ('ready'), não em flags manuais.
//
// PRINCÍPIOS
// - Nenhuma operação lança para o caller se o Redis estiver em baixo;
//   devolvem valores neutros e registam erro.
// - TTLs explícitos em todas as chaves efémeras.
// - Namespacing configurável via REDIS_PREFIX (default: 'yane').

'use strict';

const Redis = require('ioredis');
const pino = require('pino');

// =============================================================================
// CONFIGURAÇÃO
// =============================================================================

const DEFAULT_PREFIX = 'yane';
const DEFAULT_URL = 'redis://127.0.0.1:6379';

const DEFAULT_TTLS = Object.freeze({
  // Mapeamento telefone → entrevista activa. Longo porque não é
  // efémero — a entrevista dura dias.
  interviewMapping: 7 * 24 * 60 * 60,

  // Mapeamento LID → PN. Longo, para não perder histórico.
  lidMapping: 30 * 24 * 60 * 60,

  // Dedupe de mensagens recebidas. 24h cobre qualquer retry
  // razoável do WhatsApp/Baileys.
  messageSeen: 24 * 60 * 60,

  // Cache de routing resolvido. Curto para evitar stale após
  // uma nova entrevista ser criada para o mesmo telefone.
  resolvedInterview: 5 * 60,

  // Estado de disambiguação. Curto — a escolha do candidato
  // deve acontecer em minutos, não horas.
  disambiguation: 5 * 60,

  // Rate limit bucket. 1 segundo é o que o WhatsApp espera.
  rateWindow: 1,

  // Lock distribuído. Renovado pelo InterviewService a cada 30s.
  lock: 300,
});

const MAX_RETRY_ATTEMPTS = 30;
const RETRY_BASE_MS = 200;
const RETRY_MAX_MS = 3_000;

// =============================================================================
// LUA SCRIPTS
// =============================================================================

// Enfileira um item se a fila não estiver cheia.
// Retorna 1 em sucesso, 0 se cheia.
const ENQUEUE_SCRIPT = `
local queueKey = KEYS[1]
local phonesKey = KEYS[2]

local phone = ARGV[1]
local payload = ARGV[2]
local score = tonumber(ARGV[3])
local maxItems = tonumber(ARGV[4])

local len = redis.call('LLEN', queueKey)
if len >= maxItems then
  return 0
end

redis.call('LPUSH', queueKey, payload)
redis.call('ZADD', phonesKey, score, phone)
return 1
`;

// Substitui o item do topo da fila por uma versão atualizada
// (mesmo turno, nova tentativa) e reagenda o telefone.
// Retorna 1 em sucesso, 0 se a fila está vazia.
const RESCHEDULE_SCRIPT = `
local queueKey = KEYS[1]
local phonesKey = KEYS[2]

local phone = ARGV[1]
local newPayload = ARGV[2]
local newScore = tonumber(ARGV[3])

local len = redis.call('LLEN', queueKey)
if len == 0 then
  return 0
end

redis.call('LSET', queueKey, -1, newPayload)
redis.call('ZADD', phonesKey, newScore, phone)
return 1
`;

// Move o item do topo da fila para a dead-letter e limpa
// o telefone do schedule se ficar vazio.
const DEAD_LETTER_SCRIPT = `
local queueKey = KEYS[1]
local phonesKey = KEYS[2]
local deadKey = KEYS[3]

local phone = ARGV[1]
local deadPayload = ARGV[2]

local item = redis.call('RPOP', queueKey)
if not item then
  return 0
end

redis.call('LPUSH', deadKey, deadPayload)

if redis.call('LLEN', queueKey) == 0 then
  redis.call('ZREM', phonesKey, phone)
end

return 1
`;

// Release do lock — só se o token bater certo.
const RELEASE_LOCK_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`;

// Refresh do lock — só se o token bater certo.
const REFRESH_LOCK_SCRIPT = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`;

// Rate limit por janela fixa. INCR + EXPIRE atómico.
const RATE_LIMIT_SCRIPT = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local windowSeconds = tonumber(ARGV[2])

local current = redis.call('INCR', key)
if current == 1 then
  redis.call('EXPIRE', key, windowSeconds)
end

if current > limit then
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
      options.prefix || process.env.REDIS_PREFIX || DEFAULT_PREFIX
    );

    this.url = String(
      options.url || process.env.REDIS_URL || DEFAULT_URL
    );

    this.logger =
      options.logger ||
      pino({
        level: process.env.LOG_LEVEL || 'info',
        base: { service: 'redis' },
      });

    this.client = null;
    this._initialized = false;
    this._initializing = null;
    this._closing = false;

    // Scripts via defineCommand para aproveitar EVALSHA.
    this._scripts = null;
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
    } catch (_) {
      // Logging best-effort.
    }
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

  // ===========================================================================
  // LIFECYCLE
  // ===========================================================================

  async initialize() {
    if (this._initialized && !this._closing) return true;
    if (this._initializing) return this._initializing;

    this._closing = false;
    this._initializing = this._initializeInternal();

    try {
      return await this._initializing;
    } finally {
      this._initializing = null;
    }
  }

  async _initializeInternal() {
    if (this.client) return true;

    const client = new Redis(this.url, {
      lazyConnect: false,
      maxRetriesPerRequest: null,
      enableReadyCheck: true,
      enableOfflineQueue: true,
      keyPrefix: '',
      retryStrategy: (times) => {
        if (times > MAX_RETRY_ATTEMPTS) {
          this.logError(
            `Redis desistiu após ${MAX_RETRY_ATTEMPTS} tentativas.`
          );
          return null;
        }

        const delay = Math.min(
          RETRY_BASE_MS * Math.pow(1.5, times - 1),
          RETRY_MAX_MS
        );

        const jitter = 0.8 + Math.random() * 0.4;
        return Math.round(delay * jitter);
      },
      reconnectOnError: (error) => {
        const message = String(error?.message || '');
        return message.includes('READONLY');
      },
    });

    this.client = client;

    client.on('ready', () => {
      this.log('Redis pronto.');
    });

    client.on('error', (error) => {
      this.logError('Erro de Redis.', error);
    });

    client.on('close', () => {
      this.logWarn('Conexão Redis fechada.');
    });

    client.on('reconnecting', (delay) => {
      this.logWarn('Redis a reconectar.', { delayMs: delay });
    });

    client.on('end', () => {
      this.logWarn('Conexão Redis terminada.');
    });

    // Espera pela primeira ligação. Se não chegar em 10s, falha.
    try {
      await this._waitForReady(client, 10_000);
    } catch (error) {
      this.logError(
        'Redis não ficou pronto dentro do timeout inicial.',
        error
      );

      // Não fechamos o cliente — a reconexão continua em background.
      // O caller decide se continua ou não.
      throw error;
    }

    this._defineScripts(client);

    this._initialized = true;

    this.log('RedisService inicializado.', {
      url: this._maskUrl(this.url),
      prefix: this.prefix,
    });

    return true;
  }

  _waitForReady(client, timeoutMs) {
    return new Promise((resolve, reject) => {
      if (client.status === 'ready') {
        resolve();
        return;
      }

      const onReady = () => {
        cleanup();
        resolve();
      };

      const onError = (error) => {
        cleanup();
        reject(error);
      };

      const timer = setTimeout(() => {
        cleanup();
        reject(new Error('redis_ready_timeout'));
      }, timeoutMs);

      const cleanup = () => {
        clearTimeout(timer);
        client.removeListener('ready', onReady);
        client.removeListener('error', onError);
      };

      client.once('ready', onReady);
      client.once('error', onError);
    });
  }

  _defineScripts(client) {
    this._scripts = {
      enqueue: client.defineCommand('yaneEnqueue', {
        numberOfKeys: 2,
        lua: ENQUEUE_SCRIPT,
      }),
      reschedule: client.defineCommand('yaneReschedule', {
        numberOfKeys: 2,
        lua: RESCHEDULE_SCRIPT,
      }),
      deadLetter: client.defineCommand('yaneDeadLetter', {
        numberOfKeys: 3,
        lua: DEAD_LETTER_SCRIPT,
      }),
      releaseLock: client.defineCommand('yaneReleaseLock', {
        numberOfKeys: 1,
        lua: RELEASE_LOCK_SCRIPT,
      }),
      refreshLock: client.defineCommand('yaneRefreshLock', {
        numberOfKeys: 1,
        lua: REFRESH_LOCK_SCRIPT,
      }),
      rateLimit: client.defineCommand('yaneRateLimit', {
        numberOfKeys: 1,
        lua: RATE_LIMIT_SCRIPT,
      }),
    };
  }

  async close() {
    if (this._closing) return;
    this._closing = true;

    const client = this.client;
    this.client = null;
    this._initialized = false;
    this._scripts = null;

    if (!client) return;

    try {
      await client.quit();
    } catch (error) {
      this.logWarn('Falha no QUIT do Redis; forçando disconnect.', {
        error: error?.message,
      });

      try {
        client.disconnect();
      } catch (_) {
        // Best-effort.
      }
    }

    this.log('RedisService encerrado.');
  }

  // ===========================================================================
  // ESTADO
  // ===========================================================================

  isAvailable() {
    return Boolean(
      this.client && this.client.status === 'ready'
    );
  }

  // ===========================================================================
  // NAMESPACING
  // ===========================================================================

  /**
   * Junta as partes com `:` e prefixa com o namespace.
   *   key('msg', 'abc')          → 'yane:msg:abc'
   *   key('pending:turn', phone) → 'yane:pending:turn:258841234567'
   *   key('pending:phones')      → 'yane:pending:phones'
   */
  key(...parts) {
    const clean = parts
      .filter((p) => p !== undefined && p !== null && String(p).length > 0)
      .map((p) => String(p));

    if (!clean.length) return this.prefix;
    return `${this.prefix}:${clean.join(':')}`;
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
  // OPERAÇÕES GENÉRICAS
  // ===========================================================================

  async get(key) {
    try {
      return await this.client.get(key);
    } catch (error) {
      this.logError('Redis GET falhou.', error, { key: this._safeKey(key) });
      return null;
    }
  }

  async set(key, value, ttlSeconds = null) {
    try {
      if (ttlSeconds) {
        await this.client.set(key, value, 'EX', ttlSeconds);
      } else {
        await this.client.set(key, value);
      }
      return true;
    } catch (error) {
      this.logError('Redis SET falhou.', error, { key: this._safeKey(key) });
      return false;
    }
  }

  async del(key) {
    try {
      const removed = await this.client.del(key);
      return Number(removed) > 0;
    } catch (error) {
      this.logError('Redis DEL falhou.', error, { key: this._safeKey(key) });
      return false;
    }
  }

  async llen(key) {
    try {
      const value = await this.client.llen(key);
      return Number(value) || 0;
    } catch (error) {
      this.logError('Redis LLEN falhou.', error, { key: this._safeKey(key) });
      return 0;
    }
  }

  async lindex(key, index) {
    try {
      return await this.client.lindex(key, index);
    } catch (error) {
      this.logError('Redis LINDEX falhou.', error, { key: this._safeKey(key) });
      return null;
    }
  }

  async rpop(key) {
    try {
      return await this.client.rpop(key);
    } catch (error) {
      this.logError('Redis RPOP falhou.', error, { key: this._safeKey(key) });
      return null;
    }
  }

  async zrem(key, member) {
    try {
      const removed = await this.client.zrem(key, member);
      return Number(removed) > 0;
    } catch (error) {
      this.logError('Redis ZREM falhou.', error, { key: this._safeKey(key) });
      return false;
    }
  }

  async zrangeByScore(key, min, max, offset = 0, count = 20) {
    try {
      return await this.client.zrangebyscore(
        key,
        min,
        max,
        'LIMIT',
        offset,
        count
      );
    } catch (error) {
      this.logError(
        'Redis ZRANGEBYSCORE falhou.',
        error,
        { key: this._safeKey(key) }
      );
      return [];
    }
  }

  _safeKey(key) {
    // Não expor PII (telefones) nos logs de erro.
    const raw = String(key || '');
    if (raw.length <= 40) return raw;
    return `${raw.slice(0, 20)}...${raw.slice(-10)}`;
  }

  // ===========================================================================
  // MAPEAMENTO TELEFONE ↔ ENTREVISTA
  // ===========================================================================

  async rememberInterview(phone, interviewId) {
    if (!phone || !interviewId) return false;

    const key = this.key('interview', phone);
    return this.set(
      key,
      String(interviewId),
      DEFAULT_TTLS.interviewMapping
    );
  }

  async resolveInterviewId(phone) {
    if (!phone) return null;

    const key = this.key('interview', phone);
    return this.get(key);
  }

  async forgetInterview(phone) {
    if (!phone) return false;

    const key = this.key('interview', phone);
    return this.del(key);
  }

  // ===========================================================================
  // MAPEAMENTO LID ↔ PHONE
  // ===========================================================================

  async rememberLidMapping(lidJid, phone) {
    if (!lidJid || !phone) return false;

    const key = this.key('lid', lidJid);
    return this.set(key, String(phone), DEFAULT_TTLS.lidMapping);
  }

  async resolveLidMapping(lidJid) {
    if (!lidJid) return null;

    const key = this.key('lid', lidJid);
    return this.get(key);
  }

  async forgetLidMapping(lidJid) {
    if (!lidJid) return false;

    const key = this.key('lid', lidJid);
    return this.del(key);
  }

  // ===========================================================================
  // DEDUPE DE MENSAGENS
  // ===========================================================================

  /**
   * Marca uma mensagem como vista. Devolve `true` se for a primeira
   * vez (nova) e `false` se já tinha sido marcada (duplicada).
   */
  async markMessageSeen(messageId) {
    if (!messageId) return true;

    const key = this.key('msg', messageId);

    try {
      const result = await this.client.set(
        key,
        '1',
        'EX',
        DEFAULT_TTLS.messageSeen,
        'NX'
      );

      return result === 'OK';
    } catch (error) {
      this.logError('Redis SET NX falhou (dedupe).', error, {
        key: this._safeKey(key),
      });

      // Em caso de erro, assumimos que é nova. Preferimos processar
      // duplicado a descartar uma mensagem legítima.
      return true;
    }
  }

  // ===========================================================================
  // LOCKS DISTRIBUÍDOS
  // ===========================================================================

  /**
   * Adquire um lock para um recurso (por norma, telefone).
   * Devolve o token se sucesso, `null` se já está bloqueado.
   */
  async acquireLock(resource, ttlSeconds = DEFAULT_TTLS.lock) {
    if (!resource) return null;

    const token = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const key = this.key('lock', resource);

    try {
      const result = await this.client.set(
        key,
        token,
        'EX',
        ttlSeconds,
        'NX'
      );

      return result === 'OK' ? token : null;
    } catch (error) {
      this.logError('Redis SET NX falhou (lock).', error, {
        key: this._safeKey(key),
      });

      return null;
    }
  }

  async releaseLock(resource, token) {
    if (!resource || !token || !this._scripts) return false;

    const key = this.key('lock', resource);

    try {
      const result = await this._scripts.releaseLock(key, token);
      return Number(result) === 1;
    } catch (error) {
      this.logError('Redis release-lock falhou.', error, {
        key: this._safeKey(key),
      });

      return false;
    }
  }

  async refreshLock(resource, token, ttlSeconds = DEFAULT_TTLS.lock) {
    if (!resource || !token || !this._scripts) return false;

    const key = this.key('lock', resource);
    const ttlMs = Math.max(1_000, Number(ttlSeconds) * 1000);

    try {
      const result = await this._scripts.refreshLock(key, token, ttlMs);
      return Number(result) === 1;
    } catch (error) {
      this.logError('Redis refresh-lock falhou.', error, {
        key: this._safeKey(key),
      });

      return false;
    }
  }

  // ===========================================================================
  // FILA PENDENTE
  // ===========================================================================

  /**
   * Enfileira um item para um telefone.
   * Devolve 1 em sucesso, 0 se a fila estiver cheia, -1 em erro.
   */
  async enqueuePendingItem({
    queueKey,
    phonesKey,
    phone,
    payload,
    score,
    maxItems = 20,
  }) {
    if (!queueKey || !phonesKey || !phone || !payload || !this._scripts) {
      return -1;
    }

    try {
      const result = await this._scripts.enqueue(
        queueKey,
        phonesKey,
        phone,
        payload,
        String(score || Date.now()),
        String(maxItems)
      );

      return Number(result);
    } catch (error) {
      this.logError('Redis enqueue falhou.', error, {
        queueKey: this._safeKey(queueKey),
      });

      return -1;
    }
  }

  /**
   * Substitui o item do topo da fila por uma versão atualizada e
   * reagenda o telefone. Devolve `true` em sucesso.
   */
  async reschedulePendingItem({
    queueKey,
    phonesKey,
    phone,
    payload,
    score,
  }) {
    if (!queueKey || !phonesKey || !phone || !payload || !this._scripts) {
      return false;
    }

    try {
      const result = await this._scripts.reschedule(
        queueKey,
        phonesKey,
        phone,
        payload,
        String(score || Date.now())
      );

      return Number(result) === 1;
    } catch (error) {
      this.logError('Redis reschedule falhou.', error, {
        queueKey: this._safeKey(queueKey),
      });

      return false;
    }
  }

  /**
   * Move o item do topo da fila para a dead-letter. Devolve `true`
   * em sucesso.
   */
  async movePendingToDeadLetter({
    queueKey,
    phonesKey,
    deadKey,
    phone,
    payload,
  }) {
    if (!queueKey || !phonesKey || !deadKey || !phone || !this._scripts) {
      return false;
    }

    try {
      const result = await this._scripts.deadLetter(
        queueKey,
        phonesKey,
        deadKey,
        phone,
        payload
      );

      return Number(result) === 1;
    } catch (error) {
      this.logError('Redis dead-letter falhou.', error, {
        queueKey: this._safeKey(queueKey),
      });

      return false;
    }
  }

  // ===========================================================================
  // RATE LIMIT
  // ===========================================================================

  /**
   * Consome 1 do bucket. Devolve `true` se permitido, `false` se
   * excedeu o limite na janela actual.
   */
  async allowRate(bucket, limit, windowSeconds = DEFAULT_TTLS.rateWindow) {
    if (!bucket || !this._scripts) return false;

    const key = this.key('rate', bucket);

    try {
      const result = await this._scripts.rateLimit(
        key,
        String(limit),
        String(windowSeconds)
      );

      return Number(result) === 1;
    } catch (error) {
      this.logError('Redis rate-limit falhou.', error, {
        key: this._safeKey(key),
      });

      // Fail-open: se o rate limiter está em baixo, permitimos.
      // Bloquear envios por um erro de Redis seria pior.
      return true;
    }
  }

  // ===========================================================================
  // [FIX-REDIS-1] CACHE DE ROUTING RESOLVIDO
  // ===========================================================================

  async rememberResolvedInterview(
    phone,
    interviewId,
    ttlSeconds = DEFAULT_TTLS.resolvedInterview
  ) {
    if (!phone || !interviewId) return false;

    const key = this.key('resolved', phone);
    return this.set(key, String(interviewId), ttlSeconds);
  }

  async getResolvedInterview(phone) {
    if (!phone) return null;

    const key = this.key('resolved', phone);
    return this.get(key);
  }

  async clearResolvedInterview(phone) {
    if (!phone) return false;

    const key = this.key('resolved', phone);
    return this.del(key);
  }

  // ===========================================================================
  // [FIX-REDIS-1] ESTADO DE DISAMBIGUAÇÃO
  // ===========================================================================

  async setDisambiguation(
    phone,
    state,
    ttlSeconds = DEFAULT_TTLS.disambiguation
  ) {
    if (!phone || !state) return false;

    const key = this.key('disamb', phone);

    let serialized;
    try {
      serialized = JSON.stringify(state);
    } catch (error) {
      this.logError('Falha ao serializar estado de disambiguação.', error, {
        phone,
      });

      return false;
    }

    return this.set(key, serialized, ttlSeconds);
  }

  async getDisambiguation(phone) {
    if (!phone) return null;

    const key = this.key('disamb', phone);
    const raw = await this.get(key);

    if (!raw) return null;

    try {
      return JSON.parse(raw);
    } catch (error) {
      this.logError(
        'Falha ao deserializar estado de disambiguação.',
        error,
        { phone }
      );

      // Estado corrompido — limpamos para não ficar preso.
      await this.clearDisambiguation(phone);
      return null;
    }
  }

  async clearDisambiguation(phone) {
    if (!phone) return false;

    const key = this.key('disamb', phone);
    return this.del(key);
  }
}

module.exports = RedisService;