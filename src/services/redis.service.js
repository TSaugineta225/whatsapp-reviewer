// src/services/redis.service.js
//
// Wrapper minimalista sobre ioredis.
// Expõe apenas o que o bot precisa:
//   - GET / SET / DEL / EXPIRE
//   - SETNX (locks)
//   - INCR (rate limit e contadores)
//   - dedupe(mensagemId)  → atómico
//   - rateLimit(bucket)   → atómico
//   - withLock(chave)     → executa um bloco com lock

const Redis = require('ioredis');

const DEFAULT_URL = 'redis://localhost:6379';
const DEFAULT_KEY_PREFIX = 'yane';

// Tempos padrão (em segundos)
const PHONE_MAP_TTL = 2 * 60 * 60;    // 2 horas
const DEDUPE_TTL = 5 * 60;            // 5 minutos
const LOCK_TTL = 30;                  // 30 segundos (turno longo)
const RATE_WINDOW = 1;                // 1 segundo

class RedisService {
  constructor() {
    this.client = null;
    this.isReady = false;
    this.prefix = process.env.REDIS_PREFIX || DEFAULT_KEY_PREFIX;

    this.url = process.env.REDIS_URL || DEFAULT_URL;
  }

  // ============================================================
  // CICLO DE VIDA
  // ============================================================

  async initialize() {
    if (this.client) return;

    this.client = new Redis(this.url, {
      maxRetriesPerRequest: 3,
      enableReadyCheck: true,
      lazyConnect: false,
      retryStrategy: (times) => Math.min(times * 200, 3000),
    });

    this.client.on('ready', () => {
      this.isReady = true;
      console.log('[REDIS] Pronto.');
    });

    this.client.on('error', (error) => {
      this.isReady = false;
      console.error('[REDIS] Erro:', error.message);
    });

    this.client.on('close', () => {
      this.isReady = false;
    });

    // Aguarda primeira ligação
    await new Promise((resolve, reject) => {
      const timeout = setTimeout(
        () => reject(new Error('Redis timeout na ligação inicial.')),
        5000
      );

      this.client.once('ready', () => {
        clearTimeout(timeout);
        resolve();
      });

      this.client.once('error', (err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  }

  async close() {
    if (!this.client) return;

    try {
      await this.client.quit();
    } catch (_) {
      // Ignorar erro no shutdown.
    } finally {
      this.client = null;
      this.isReady = false;
    }
  }

  // ============================================================
  // CHAVES
  // ============================================================

  key(...parts) {
    return [this.prefix, ...parts].filter(Boolean).join(':');
  }

  // ============================================================
  // OPERAÇÕES BÁSICAS
  // ============================================================

  async get(key) {
    if (!this.isReady) return null;
    try {
      return await this.client.get(key);
    } catch (error) {
      console.error('[REDIS] GET falhou:', error.message);
      return null;
    }
  }

  async set(key, value, ttlSeconds = null) {
    if (!this.isReady) return false;
    try {
      if (ttlSeconds) {
        await this.client.set(key, value, 'EX', ttlSeconds);
      } else {
        await this.client.set(key, value);
      }
      return true;
    } catch (error) {
      console.error('[REDIS] SET falhou:', error.message);
      return false;
    }
  }

  async del(key) {
    if (!this.isReady) return false;
    try {
      await this.client.del(key);
      return true;
    } catch (error) {
      console.error('[REDIS] DEL falhou:', error.message);
      return false;
    }
  }

  async setnx(key, value, ttlSeconds = null) {
    if (!this.isReady) return false;
    try {
      const result = ttlSeconds
        ? await this.client.set(key, value, 'EX', ttlSeconds, 'NX')
        : await this.client.set(key, value, 'NX');
      return result === 'OK';
    } catch (error) {
      console.error('[REDIS] SETNX falhou:', error.message);
      return false;
    }
  }

  async incr(key, ttlSeconds = null) {
    if (!this.isReady) return 0;
    try {
      const value = await this.client.incr(key);
      if (value === 1 && ttlSeconds) {
        await this.client.expire(key, ttlSeconds);
      }
      return value;
    } catch (error) {
      console.error('[REDIS] INCR falhou:', error.message);
      return 0;
    }
  }

  // ============================================================
  // PHONE ↔ INTERVIEW
  // ============================================================

  async rememberInterview(phone, interviewId, ttl = PHONE_MAP_TTL) {
    const key = this.key('phone', phone);
    return this.set(key, String(interviewId), ttl);
  }

  async resolveInterviewId(phone) {
    const key = this.key('phone', phone);
    return this.get(key);
  }

  async forgetInterview(phone) {
    const key = this.key('phone', phone);
    return this.del(key);
  }

  // ============================================================
  // DEDUPE DE MENSAGENS
  // ============================================================

  /**
   * Devolve `true` se a mensagem é nova (nunca vista) e marca-a.
   * Devolve `false` se já foi processada (duplicada).
   *
   * Usa SET NX atómico. Não há race condition.
   */
  async markMessageSeen(messageId, ttl = DEDUPE_TTL) {
    if (!messageId) return true;

    const key = this.key('msg', messageId);
    return this.setnx(key, '1', ttl);
  }

  // ============================================================
  // LOCK POR TELEFONE
  // ============================================================

  /**
   * Adquire um lock temporário por telefone.
   *
   * Devolve um token se conseguiu, ou null se já está locked.
   * O caller deve chamar releaseLock() no finally.
   */
  async acquireLock(phone, ttl = LOCK_TTL) {
    const key = this.key('lock', phone);
    const token = `${Date.now()}-${Math.random().toString(36).slice(2)}`;

    const acquired = await this.setnx(key, token, ttl);
    return acquired ? token : null;
  }

  /**
   * Liberta o lock apenas se ainda formos os donos.
   * Evita apagar o lock de outro processo que o adquiriu após o TTL.
   */
  async releaseLock(phone, token) {
    if (!token) return;

    const key = this.key('lock', phone);

    try {
      const current = await this.client.get(key);
      if (current === token) {
        await this.client.del(key);
      }
    } catch (_) {
      // Ignorar.
    }
  }

  // ============================================================
  // RATE LIMIT (token bucket simplificado)
  // ============================================================

  /**
   * Conta quantas operações foram feitas nesta janela (segundo).
   * Devolve `true` se ainda estamos abaixo do limite.
   */
  async allowRate(bucket = 'out', maxPerSecond = 60) {
    const key = this.key('rate', bucket, Math.floor(Date.now() / 1000));
    const count = await this.incr(key, RATE_WINDOW + 1);
    return count <= maxPerSecond;
  }

  // ============================================================
  // HELPERS
  // ============================================================

  /**
   * Executa `fn` com um lock por telefone.
   * Se não conseguir o lock, devolve `null` sem executar.
   */
  async withPhoneLock(phone, fn) {
    const token = await this.acquireLock(phone);
    if (!token) return null;

    try {
      return await fn();
    } finally {
      await this.releaseLock(phone, token);
    }
  }
}

module.exports = RedisService;