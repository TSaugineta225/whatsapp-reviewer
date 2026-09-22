// src/services/redis.service.js
const Redis = require('ioredis');
const crypto = require('crypto');

const DEFAULT_URL = 'redis://localhost:6379';
const DEFAULT_KEY_PREFIX = 'yane';

const PHONE_MAP_TTL = 2 * 60 * 60; // 2 horas
const DEDUPE_TTL = 5 * 60;          // 5 minutos
const LOCK_TTL = 90;                // 90 segundos
const RATE_WINDOW = 1;              // 1 segundo
const CONNECT_TIMEOUT = 5000;

// Libera o lock somente se o token ainda pertencer ao proprietário.
// GET + DEL separados podem sofrer race condition.
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
    // Redis já está pronto.
    if (this.isReady && this.client) return;

    // Outra chamada já está inicializando.
    // Evita múltiplas conexões quando initialize() é chamado
    // simultaneamente por diferentes partes da aplicação.
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
        // Ignorar: estamos apenas limpando uma conexão anterior.
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
            // Ignorar erro durante cleanup.
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
      // Em caso de conexão já perdida, não há nada mais a fazer.
      try {
        client.disconnect();
      } catch (_) {
        // Ignorar erro durante cleanup.
      }
    }
  }

  // ============================================================
  // HELPERS INTERNOS
  // ============================================================

  _available() {
    return Boolean(this.client && this.isReady);
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
    return this._execute(
      'GET',
      null,
      (client) => client.get(key)
    );
  }

  async set(key, value, ttlSeconds = null) {
    return this._execute(
      'SET',
      false,
      async (client) => {
        if (Number.isFinite(ttlSeconds) && ttlSeconds > 0) {
          await client.set(key, value, 'EX', ttlSeconds);
        } else {
          await client.set(key, value);
        }

        return true;
      }
    );
  }

  async del(key) {
    return this._execute(
      'DEL',
      false,
      async (client) => {
        await client.del(key);
        return true;
      }
    );
  }

  async setnx(key, value, ttlSeconds = null) {
    return this._execute(
      'SETNX',
      false,
      async (client) => {
        const result =
          Number.isFinite(ttlSeconds) && ttlSeconds > 0
            ? await client.set(key, value, 'EX', ttlSeconds, 'NX')
            : await client.set(key, value, 'NX');

        return result === 'OK';
      }
    );
  }

  async incr(key, ttlSeconds = null) {
    return this._execute(
      'INCR',
      0,
      async (client) => {
        const value = await client.incr(key);

        // TTL só é aplicado quando a chave acabou de ser criada.
        // Isso mantém o comportamento de janela atual.
        if (value === 1 && Number.isFinite(ttlSeconds) && ttlSeconds > 0) {
          await client.expire(key, ttlSeconds);
        }

        return value;
      }
    );
  }

  // ============================================================
  // LISTAS (fila de turnos pendentes)
  // ============================================================

  async lpush(key, value) {
    return this._execute(
      'LPUSH',
      false,
      async (client) => {
        await client.lpush(key, value);
        return true;
      }
    );
  }

  async lpop(key) {
    return this._execute(
      'LPOP',
      null,
      (client) => client.lpop(key)
    );
  }

  async lindex(key, index = 0) {
    return this._execute(
      'LINDEX',
      null,
      (client) => client.lindex(key, index)
    );
  }

  async llen(key) {
    return this._execute(
      'LLEN',
      0,
      (client) => client.llen(key)
    );
  }

  async lset(key, index, value) {
    return this._execute(
      'LSET',
      false,
      async (client) => {
        await client.lset(key, index, value);
        return true;
      }
    );
  }

  // ============================================================
  // SORTED SETS (índice de telefones pendentes)
  // ============================================================

  async zadd(key, score, member) {
    return this._execute(
      'ZADD',
      false,
      async (client) => {
        await client.zadd(key, score, member);
        return true;
      }
    );
  }

  async zrem(key, member) {
    return this._execute(
      'ZREM',
      false,
      async (client) => {
        await client.zrem(key, member);
        return true;
      }
    );
  }

  async zrangeByScore(
    key,
    min,
    max,
    offset = 0,
    count = 10
  ) {
    return this._execute(
      'ZRANGEBYSCORE',
      [],
      (client) =>
        client.zrangebyscore(
          key,
          min,
          max,
          'LIMIT',
          offset,
          count
        )
    );
  }

  // ============================================================
  // PHONE ↔ INTERVIEW
  // ============================================================

  async rememberInterview(
    phone,
    interviewId,
    ttl = PHONE_MAP_TTL
  ) {
    return this.set(
      this.key('phone', phone),
      String(interviewId),
      ttl
    );
  }

  async resolveInterviewId(phone) {
    return this.get(
      this.key('phone', phone)
    );
  }

  async forgetInterview(phone) {
    return this.del(
      this.key('phone', phone)
    );
  }

  // ============================================================
  // DEDUPE
  // ============================================================

  async markMessageSeen(
    messageId,
    ttl = DEDUPE_TTL
  ) {
    // Mantém a semântica original:
    // sem ID não há nada para deduplicar.
    if (!messageId) return true;

    return this.setnx(
      this.key('msg', messageId),
      '1',
      ttl
    );
  }

  // ============================================================
  // LOCK POR TELEFONE
  // ============================================================

  async acquireLock(
    phone,
    ttl = LOCK_TTL
  ) {
    const key = this.key('lock', phone);

    const token = `${Date.now()}-${crypto.randomBytes(16).toString('hex')}`;

    const acquired = await this.setnx(
      key,
      token,
      ttl
    );

    return acquired ? token : null;
  }

  async releaseLock(phone, token) {
    if (!token || !this._available()) return;

    const key = this.key('lock', phone);

    try {
      await this.client.eval(
        RELEASE_LOCK_SCRIPT,
        1,
        key,
        token
      );
    } catch (error) {
      console.error(
        '[REDIS] RELEASE LOCK falhou:',
        error.message
      );
    }
  }

  // ============================================================
  // RATE LIMIT
  // ============================================================

  async allowRate(
    bucket = 'out',
    maxPerSecond = 60
  ) {
    const second = Math.floor(Date.now() / 1000);

    const key = this.key(
      'rate',
      bucket,
      second
    );

    const count = await this.incr(
      key,
      RATE_WINDOW + 1
    );

    return count <= maxPerSecond;
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