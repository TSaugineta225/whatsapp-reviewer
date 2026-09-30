// src/services/redis.service.js
//
// [FIXES]
// [FIX-1] Mapping persistente LID ↔ PN. Sem isto, um candidato que
//         responde por LID nunca é resolvido quando o
//         signalRepository do Baileys não tem o mapping.
//         TTL longo porque LID↔PN é estável.

const Redis = require('ioredis');
const crypto = require('crypto');

const DEFAULT_URL = 'redis://localhost:6379';
const DEFAULT_KEY_PREFIX = 'yane';

const PHONE_MAP_TTL = 2 * 60 * 60;              // 2 horas
const DEDUPE_TTL = 5 * 60;                      // 5 minutos
const LOCK_TTL = 90;                            // 90 segundos
const RATE_WINDOW = 1;                          // 1 segundo
const CONNECT_TIMEOUT = 5000;

// [FIX-1] 30 dias. LID↔PN é estável no WhatsApp; expirar cedo
// força lookups falhados. O Redis é a fonte de verdade.
const LID_MAP_TTL = 30 * 24 * 60 * 60;

const RELEASE_LOCK_SCRIPT = `
  if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
  end
  return 0
`;

class RedisService {
  constructor() {
    this.client = null;
    this.isReady = false;
    this.initializing = null;

    this.prefix = process.env.REDIS_PREFIX || DEFAULT_KEY_PREFIX;
    this.url = process.env.REDIS_URL || DEFAULT_URL;
  }

  // ============================================================
  // CICLO DE VIDA
  // ============================================================

  async initialize() {
    if (this.isReady && this.client) return;

    if (this.initializing) {
      return this.initializing;
    }

    this.initializing = this._initialize();

    try {
      await this.initializing;
    } finally {
      this.initializing = null;
    }
  }

  async _initialize() {
    if (this.client) {
      try {
        this.client.disconnect();
      } catch (_) {
        // Ignorar.
      }
    }

    this.isReady = false;

    const client = new Redis(this.url, {
      maxRetriesPerRequest: 3,
      enableReadyCheck: true,
      lazyConnect: false,
      retryStrategy: (times) => Math.min(times * 200, 3000),
    });

    this.client = client;

    client.on('ready', () => {
      this.isReady = true;
      console.log('[REDIS] Pronto.');
    });

    client.on('error', (error) => {
      this.isReady = false;
      console.error('[REDIS] Erro:', error.message);
    });

    client.on('close', () => {
      this.isReady = false;
    });

    try {
      await new Promise((resolve, reject) => {
        let timeout;

        const cleanup = () => {
          if (timeout) clearTimeout(timeout);
          client.removeListener('ready', onReady);
        };

        const onReady = () => {
          cleanup();
          resolve();
        };

        timeout = setTimeout(() => {
          cleanup();

          try {
            client.disconnect();
          } catch (_) {
            // Ignorar.
          }

          reject(
            new Error('Redis timeout na ligação inicial.')
          );
        }, CONNECT_TIMEOUT);

        client.once('ready', onReady);
      });
    } catch (error) {
      if (this.client === client) {
        this.client = null;
        this.isReady = false;
      }

      throw error;
    }
  }

  async close() {
    const client = this.client;

    if (!client) return;

    this.client = null;
    this.isReady = false;

    try {
      await client.quit();
    } catch (_) {
      try {
        client.disconnect();
      } catch (_) {
        // Ignorar.
      }
    }
  }

  // ============================================================
  // HELPERS INTERNOS
  // ============================================================

  _available() {
    return Boolean(this.client && this.isReady);
  }

  isAvailable() {
    return this._available();
  }

  async _execute(operation, fallback, fn) {
    if (!this._available()) {
      return fallback;
    }

    try {
      return await fn(this.client);
    } catch (error) {
      console.error(`[REDIS] ${operation} falhou:`, error.message);
      return fallback;
    }
  }

  // ============================================================
  // CHAVES
  // ============================================================

  key(...parts) {
    return [this.prefix, ...parts]
      .filter((part) => part !== undefined && part !== null && part !== '')
      .join(':');
  }

  // ============================================================
  // OPERAÇÕES BÁSICAS
  // ============================================================

  async get(key) {
    return this._execute('GET', null, (client) => client.get(key));
  }

  async set(key, value, ttlSeconds = null) {
    return this._execute('SET', false, async (client) => {
      if (Number.isFinite(ttlSeconds) && ttlSeconds > 0) {
        await client.set(key, value, 'EX', ttlSeconds);
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
      const result =
        Number.isFinite(ttlSeconds) && ttlSeconds > 0
          ? await client.set(key, value, 'EX', ttlSeconds, 'NX')
          : await client.set(key, value, 'NX');

      return result === 'OK';
    });
  }

  async incr(key, ttlSeconds = null) {
    return this._execute('INCR', 0, async (client) => {
      const value = await client.incr(key);

      if (value === 1 && Number.isFinite(ttlSeconds) && ttlSeconds > 0) {
        await client.expire(key, ttlSeconds);
      }

      return value;
    });
  }

  // ============================================================
  // LISTAS
  // ============================================================

  async lpush(key, value) {
    return this._execute('LPUSH', false, async (client) => {
      await client.lpush(key, value);
      return true;
    });
  }

  async lpop(key) {
    return this._execute('LPOP', null, (client) => client.lpop(key));
  }

  async rpop(key) {
    return this._execute('RPOP', null, (client) => client.rpop(key));
  }

  async lindex(key, index = 0) {
    return this._execute('LINDEX', null, (client) =>
      client.lindex(key, index)
    );
  }

  async llen(key) {
    return this._execute('LLEN', 0, (client) => client.llen(key));
  }

  async lset(key, index, value) {
    return this._execute('LSET', false, async (client) => {
      await client.lset(key, index, value);
      return true;
    });
  }

  // ============================================================
  // SORTED SETS
  // ============================================================

  async zadd(key, score, member) {
    return this._execute('ZADD', false, async (client) => {
      await client.zadd(key, score, member);
      return true;
    });
  }

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

  // ============================================================
  // PHONE ↔ INTERVIEW
  // ============================================================

  async rememberInterview(phone, interviewId, ttl = PHONE_MAP_TTL) {
    return this.set(
      this.key('phone', phone),
      String(interviewId),
      ttl
    );
  }

  async resolveInterviewId(phone) {
    return this.get(this.key('phone', phone));
  }

  async forgetInterview(phone) {
    return this.del(this.key('phone', phone));
  }

  // ============================================================
  // [FIX-1] LID ↔ PHONE
  //
  // O mapping LID→PN é persistido quando enviamos uma mensagem e o
  // Baileys devolve o JID real que o WhatsApp associou. Nas
  // respostas do candidato, o LID é resolvido sem depender do
  // signalRepository (que falha se o mapping não estiver carregado).
  // ============================================================

  _normalizeLidKey(lidJid) {
    const value = String(lidJid || '').trim();
    if (!value) return '';

    // Strip do sufixo @lid e parâmetros (:1).
    const local = value.split('@')[0].split(':')[0];
    if (!local) return '';

    return local;
  }

  async rememberLidMapping(lidJid, phone, ttl = LID_MAP_TTL) {
    const local = this._normalizeLidKey(lidJid);

    if (!local || !phone) {
      return false;
    }

    return this.set(
      this.key('lid', local),
      String(phone),
      ttl
    );
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

  // ============================================================
  // DEDUPE
  // ============================================================

  async markMessageSeen(messageId, ttl = DEDUPE_TTL) {
    if (!messageId) return true;

    return this.setnx(this.key('msg', messageId), '1', ttl);
  }

  // ============================================================
  // LOCK POR TELEFONE
  // ============================================================

  async acquireLock(phone, ttl = LOCK_TTL) {
    const key = this.key('lock', phone);

    const token = `${Date.now()}-${crypto
      .randomBytes(16)
      .toString('hex')}`;

    const acquired = await this.setnx(key, token, ttl);

    return acquired ? token : null;
  }

  async releaseLock(phone, token) {
    if (!token || !this._available()) return;

    const key = this.key('lock', phone);

    try {
      await this.client.eval(RELEASE_LOCK_SCRIPT, 1, key, token);
    } catch (error) {
      console.error('[REDIS] RELEASE LOCK falhou:', error.message);
    }
  }

  async refreshLock(phone, token, ttl = LOCK_TTL) {
    if (!token || !this._available()) return false;

    const key = this.key('lock', phone);

    try {
      const current = await this.client.get(key);

      if (current !== token) {
        return false;
      }

      await this.client.expire(key, ttl);
      return true;
    } catch (error) {
      console.error('[REDIS] REFRESH LOCK falhou:', error.message);
      return false;
    }
  }

  // ============================================================
  // RATE LIMIT
  // ============================================================

  async allowRate(bucket = 'out', maxPerSecond = 60) {
    const second = Math.floor(Date.now() / 1000);

    const key = this.key('rate', bucket, second);

    const count = await this.incr(key, RATE_WINDOW + 1);

    return count <= maxPerSecond;
  }

  // ============================================================
  // FILA DE TURNOS PENDENTES
  // ============================================================

  /**
   * Enfileira item de forma atómica.
   *
   * Devolve 1 em sucesso, 0 se a fila está cheia ou erro.
   */
  async enqueuePendingItem({
    queueKey,
    phonesKey,
    phone,
    payload,
    score,
    maxItems,
  }) {
    if (!this._available()) {
      return 0;
    }

    const script = `
      local len = redis.call('LLEN', KEYS[1])
      if len >= tonumber(ARGV[1]) then
        return 0
      end
      redis.call('LPUSH', KEYS[1], ARGV[2])
      redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
      return 1
    `;

    try {
      const result = await this.client.eval(
        script,
        2,
        queueKey,
        phonesKey,
        String(maxItems),
        payload,
        String(score),
        phone
      );

      return Number(result) || 0;
    } catch (error) {
      console.error('[REDIS] enqueuePendingItem falhou:', error.message);
      return 0;
    }
  }

  async reschedulePendingItem({
    queueKey,
    phonesKey,
    phone,
    payload,
    score,
  }) {
    if (!this._available()) {
      return false;
    }

    const script = `
      redis.call('LSET', KEYS[1], -1, ARGV[1])
      redis.call('ZADD', KEYS[2], ARGV[2], ARGV[3])
      return 1
    `;

    try {
      await this.client.eval(
        script,
        2,
        queueKey,
        phonesKey,
        payload,
        String(score),
        phone
      );

      return true;
    } catch (error) {
      console.error(
        '[REDIS] reschedulePendingItem falhou:',
        error.message
      );
      return false;
    }
  }

  async movePendingToDeadLetter({
    queueKey,
    phonesKey,
    deadKey,
    phone,
    payload,
  }) {
    if (!this._available()) {
      return false;
    }

    const script = `
      local item = redis.call('RPOP', KEYS[1])
      if not item then
        return 0
      end
      redis.call('LPUSH', KEYS[3], ARGV[1])
      local remaining = redis.call('LLEN', KEYS[1])
      if remaining == 0 then
        redis.call('ZREM', KEYS[2], ARGV[2])
      end
      return 1
    `;

    try {
      const result = await this.client.eval(
        script,
        3,
        queueKey,
        phonesKey,
        deadKey,
        payload,
        phone
      );

      return Boolean(result);
    } catch (error) {
      console.error(
        '[REDIS] movePendingToDeadLetter falhou:',
        error.message
      );
      return false;
    }
  }

  // ============================================================
  // HELPERS
  // ============================================================

  async withPhoneLock(phone, fn) {
    const token = await this.acquireLock(phone);

    if (!token) {
      return null;
    }

    try {
      return await fn();
    } finally {
      await this.releaseLock(phone, token);
    }
  }
}

module.exports = RedisService;