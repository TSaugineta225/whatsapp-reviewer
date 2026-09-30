// src/services/redis.service.js
//
// Cliente Redis central do Yane Bot.
//
// Responsabilidades:
//   - lifecycle e reconexão do Redis;
//   - operações básicas;
//   - mapping telefone ↔ entrevista;
//   - mapping persistente LID ↔ PN;
//   - deduplicação de mensagens;
//   - locks distribuídos por telefone;
//   - rate limiting;
//   - filas de turnos;
//   - dead-letter.
//
// Princípios:
//   - Redis pode falhar sem derrubar o processo;
//   - locks são validados e renovados atomicamente;
//   - eventos de clientes antigos nunca podem corromper o estado
//     do cliente actualmente activo;
//   - operações críticas usam scripts Lua quando necessário;
//   - logs Redis são informativos, mas não devem inundar stdout.
//
// [FIX-1] Mapping persistente LID ↔ PN.
//         TTL longo porque o mapping é relativamente estável.
//
// [FIX-2] LOCK_TTL = 300s.
//         O processamento de um turno pode ultrapassar 90s em
//         cenários normais. O lock é renovado periodicamente.
//
// [FIX-3] refreshLock() usa Lua para fazer GET + EXPIRE
//         atomicamente. Evita race conditions.
//
// [FIX-4] Eventos do Redis são associados ao client correcto.
//         Um client antigo em processo de shutdown nunca consegue
//         marcar o novo client como indisponível.
//
// [FIX-5] withPhoneLock() renova automaticamente o lock.
//         O consumidor não precisa implementar heartbeat manual.
//

const Redis = require('ioredis');
const crypto = require('crypto');

// ============================================================
// CONFIGURAÇÃO
// ============================================================

const DEFAULT_URL = 'redis://localhost:6379';
const DEFAULT_KEY_PREFIX = 'yane';

const PHONE_MAP_TTL = 2 * 60 * 60; // 2 horas
const DEDUPE_TTL = 5 * 60; // 5 minutos

const LOCK_TTL = 300; // 5 minutos

// Renovação suficientemente frequente para manter uma margem
// confortável dentro do TTL de 300s.
const LOCK_RENEW_INTERVAL_MS = 30_000;

const RATE_WINDOW = 1; // 1 segundo
const CONNECT_TIMEOUT = 5000;

const LID_MAP_TTL = 30 * 24 * 60 * 60; // 30 dias

const ERROR_LOG_COOLDOWN_MS = 10_000;

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

class RedisService {
  constructor() {
    // Client Redis actualmente activo.
    this.client = null;

    // Estado exposto ao restante da aplicação.
    this.isReady = false;

    // Evita inicializações concorrentes.
    this.initializing = null;

    // Evita múltiplos refresh simultâneos do mesmo lock.
    this._lockRefreshInFlight = new Set();

    // Rate-limit de logs de erros.
    this._lastErrorLog = new Map();

    this.prefix =
      process.env.REDIS_PREFIX || DEFAULT_KEY_PREFIX;

    this.url =
      process.env.REDIS_URL || DEFAULT_URL;
  }

  // ============================================================
  // CICLO DE VIDA
  // ============================================================

  async initialize() {
    if (this.isReady && this.client) {
      return;
    }

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
    const previousClient = this.client;

    if (previousClient) {
      try {
        previousClient.disconnect();
      } catch (_) {
        // Best effort.
      }
    }

    this.client = null;
    this.isReady = false;

    const client = new Redis(this.url, {
      maxRetriesPerRequest: 3,
      enableReadyCheck: true,
      lazyConnect: false,
      connectTimeout: CONNECT_TIMEOUT,

      retryStrategy: (times) =>
        Math.min(times * 200, 3000),
    });

    this.client = client;

    this._attachClientEvents(client);

    try {
      await this._waitForReady(client);
    } catch (error) {
      if (this.client === client) {
        this.client = null;
        this.isReady = false;
      }

      try {
        client.disconnect();
      } catch (_) {
        // Best effort.
      }

      throw error;
    }
  }

  _attachClientEvents(client) {
    client.on('ready', () => {
      // Um client antigo nunca pode alterar o estado
      // do client que o substituiu.
      if (this.client !== client) {
        return;
      }

      this.isReady = true;

      console.log('[REDIS] Pronto.');
    });

    client.on('error', (error) => {
      if (this.client !== client) {
        return;
      }

      this.isReady = false;

      this._logError(
        'connection',
        '[REDIS] Erro:',
        error
      );
    });

    client.on('close', () => {
      if (this.client !== client) {
        return;
      }

      this.isReady = false;
    });

    client.on('end', () => {
      if (this.client !== client) {
        return;
      }

      this.isReady = false;
    });

    client.on('reconnecting', () => {
      if (this.client !== client) {
        return;
      }

      this.isReady = false;
    });
  }

  async _waitForReady(client) {
    return new Promise((resolve, reject) => {
      let settled = false;

      const timeout = setTimeout(() => {
        finishReject(
          new Error('Redis timeout na ligação inicial.')
        );
      }, CONNECT_TIMEOUT);

      const cleanup = () => {
        clearTimeout(timeout);

        client.removeListener(
          'ready',
          onReady
        );

        client.removeListener(
          'end',
          onEnd
        );
      };

      const finishResolve = () => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();
        resolve();
      };

      const finishReject = (error) => {
        if (settled) {
          return;
        }

        settled = true;
        cleanup();
        reject(error);
      };

      const onReady = () => {
        finishResolve();
      };

      const onEnd = () => {
        finishReject(
          new Error('Redis encerrou a ligação inicial.')
        );
      };

      client.once('ready', onReady);
      client.once('end', onEnd);

      // Pode já ter atingido ready entre a criação do
      // client e a instalação dos listeners.
      if (
        this.client === client &&
        client.status === 'ready'
      ) {
        finishResolve();
      }
    });
  }

  async close() {
    const client = this.client;

    if (!client) {
      this.isReady = false;
      return;
    }

    // Invalida imediatamente o client para que eventos tardios
    // não alterem o estado da aplicação.
    this.client = null;
    this.isReady = false;

    try {
      await client.quit();
    } catch (error) {
      try {
        client.disconnect();
      } catch (_) {
        // Best effort.
      }
    }
  }

  // ============================================================
  // HELPERS INTERNOS
  // ============================================================

  _available() {
    return Boolean(
      this.client &&
      this.isReady &&
      this.client.status === 'ready'
    );
  }

  isAvailable() {
    return this._available();
  }

  async _execute(operation, fallback, fn) {
    if (!this._available()) {
      return fallback;
    }

    const client = this.client;

    try {
      return await fn(client);
    } catch (error) {
      this._logError(
        operation,
        `[REDIS] ${operation} falhou:`,
        error
      );

      return fallback;
    }
  }

  _logError(operation, prefix, error) {
    const message =
      error?.message ||
      String(error);

    const now = Date.now();
    const last = this._lastErrorLog.get(operation) || 0;

    // Evita inundação dos logs durante uma indisponibilidade
    // prolongada do Redis.
    if (
      now - last <
      ERROR_LOG_COOLDOWN_MS
    ) {
      return;
    }

    this._lastErrorLog.set(
      operation,
      now
    );

    try {
      console.error(
        prefix,
        message
      );
    } catch (_) {
      // Logging é best effort.
    }
  }

  _normalizeTtl(ttlSeconds) {
    if (
      ttlSeconds === null ||
      ttlSeconds === undefined
    ) {
      return null;
    }

    const ttl = Number(ttlSeconds);

    if (!Number.isFinite(ttl) || ttl <= 0) {
      return null;
    }

    return Math.max(
      1,
      Math.floor(ttl)
    );
  }

  // ============================================================
  // CHAVES
  // ============================================================

  key(...parts) {
    return [
      this.prefix,
      ...parts,
    ]
      .filter(
        (part) =>
          part !== undefined &&
          part !== null &&
          part !== ''
      )
      .map((part) => String(part))
      .join(':');
  }

  // ============================================================
  // OPERAÇÕES BÁSICAS
  // ============================================================

  async get(key) {
    return this._execute(
      'GET',
      null,
      (client) =>
        client.get(key)
    );
  }

  async set(
    key,
    value,
    ttlSeconds = null
  ) {
    return this._execute(
      'SET',
      false,
      async (client) => {
        const ttl =
          this._normalizeTtl(
            ttlSeconds
          );

        if (ttl !== null) {
          await client.set(
            key,
            value,
            'EX',
            ttl
          );
        } else {
          await client.set(
            key,
            value
          );
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

  async setnx(
    key,
    value,
    ttlSeconds = null
  ) {
    return this._execute(
      'SETNX',
      false,
      async (client) => {
        const ttl =
          this._normalizeTtl(
            ttlSeconds
          );

        const result =
          ttl !== null
            ? await client.set(
                key,
                value,
                'EX',
                ttl,
                'NX'
              )
            : await client.set(
                key,
                value,
                'NX'
              );

        return result === 'OK';
      }
    );
  }

  async incr(
    key,
    ttlSeconds = null
  ) {
    return this._execute(
      'INCR',
      0,
      async (client) => {
        const value =
          await client.incr(key);

        const ttl =
          this._normalizeTtl(
            ttlSeconds
          );

        if (
          value === 1 &&
          ttl !== null
        ) {
          await client.expire(
            key,
            ttl
          );
        }

        return value;
      }
    );
  }

  // ============================================================
  // LISTAS
  // ============================================================

  async lpush(key, value) {
    return this._execute(
      'LPUSH',
      false,
      async (client) => {
        await client.lpush(
          key,
          value
        );

        return true;
      }
    );
  }

  async lpop(key) {
    return this._execute(
      'LPOP',
      null,
      (client) =>
        client.lpop(key)
    );
  }

  async rpop(key) {
    return this._execute(
      'RPOP',
      null,
      (client) =>
        client.rpop(key)
    );
  }

  async lindex(
    key,
    index = 0
  ) {
    return this._execute(
      'LINDEX',
      null,
      (client) =>
        client.lindex(
          key,
          index
        )
    );
  }

  async llen(key) {
    return this._execute(
      'LLEN',
      0,
      (client) =>
        client.llen(key)
    );
  }

  async lset(
    key,
    index,
    value
  ) {
    return this._execute(
      'LSET',
      false,
      async (client) => {
        await client.lset(
          key,
          index,
          value
        );

        return true;
      }
    );
  }

  // ============================================================
  // SORTED SETS
  // ============================================================

  async zadd(
    key,
    score,
    member
  ) {
    return this._execute(
      'ZADD',
      false,
      async (client) => {
        await client.zadd(
          key,
          score,
          member
        );

        return true;
      }
    );
  }

  async zrem(
    key,
    member
  ) {
    return this._execute(
      'ZREM',
      false,
      async (client) => {
        await client.zrem(
          key,
          member
        );

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
    if (!phone || !interviewId) {
      return false;
    }

    return this.set(
      this.key(
        'phone',
        phone
      ),
      String(interviewId),
      ttl
    );
  }

  async resolveInterviewId(
    phone
  ) {
    if (!phone) {
      return null;
    }

    return this.get(
      this.key(
        'phone',
        phone
      )
    );
  }

  async forgetInterview(
    phone
  ) {
    if (!phone) {
      return false;
    }

    return this.del(
      this.key(
        'phone',
        phone
      )
    );
  }

  // ============================================================
  // LID ↔ PHONE
  // ============================================================

  _normalizeLidKey(lidJid) {
    const value =
      String(lidJid || '').trim();

    if (!value) {
      return '';
    }

    const local =
      value
        .split('@')[0]
        .split(':')[0]
        .trim();

    return local;
  }

  async rememberLidMapping(
    lidJid,
    phone,
    ttl = LID_MAP_TTL
  ) {
    const local =
      this._normalizeLidKey(
        lidJid
      );

    if (!local || !phone) {
      return false;
    }

    return this.set(
      this.key(
        'lid',
        local
      ),
      String(phone),
      ttl
    );
  }

  async resolveLidMapping(
    lidJid
  ) {
    const local =
      this._normalizeLidKey(
        lidJid
      );

    if (!local) {
      return null;
    }

    return this.get(
      this.key(
        'lid',
        local
      )
    );
  }

  async forgetLidMapping(
    lidJid
  ) {
    const local =
      this._normalizeLidKey(
        lidJid
      );

    if (!local) {
      return false;
    }

    return this.del(
      this.key(
        'lid',
        local
      )
    );
  }

  // ============================================================
  // DEDUPE
  // ============================================================

  async markMessageSeen(
    messageId,
    ttl = DEDUPE_TTL
  ) {
    if (!messageId) {
      return true;
    }

    // true = mensagem nova marcada;
    // false = já existia ou Redis indisponível.
    return this.setnx(
      this.key(
        'msg',
        messageId
      ),
      '1',
      ttl
    );
  }

  // ============================================================
  // LOCK POR TELEFONE
  // ============================================================

  _createLockToken() {
    return [
      Date.now().toString(36),
      crypto
        .randomBytes(16)
        .toString('hex'),
    ].join('-');
  }

  async acquireLock(
    phone,
    ttl = LOCK_TTL
  ) {
    if (!phone) {
      return null;
    }

    const key =
      this.key(
        'lock',
        phone
      );

    const token =
      this._createLockToken();

    const acquired =
      await this.setnx(
        key,
        token,
        ttl
      );

    return acquired
      ? token
      : null;
  }

  async releaseLock(
    phone,
    token
  ) {
    if (
      !phone ||
      !token ||
      !this._available()
    ) {
      return false;
    }

    const client =
      this.client;

    const key =
      this.key(
        'lock',
        phone
      );

    try {
      const result =
        await client.eval(
          RELEASE_LOCK_SCRIPT,
          1,
          key,
          token
        );

      return Number(result) === 1;
    } catch (error) {
      this._logError(
        'release-lock',
        '[REDIS] RELEASE LOCK falhou:',
        error
      );

      return false;
    }
  }

  async refreshLock(
    phone,
    token,
    ttl = LOCK_TTL
  ) {
    if (
      !phone ||
      !token ||
      !this._available()
    ) {
      return false;
    }

    const normalizedTtl =
      this._normalizeTtl(ttl);

    if (normalizedTtl === null) {
      return false;
    }

    const key =
      this.key(
        'lock',
        phone
      );

    const client =
      this.client;

    try {
      // GET + EXPIRE acontece atomicamente.
      const result =
        await client.eval(
          REFRESH_LOCK_SCRIPT,
          1,
          key,
          token,
          normalizedTtl
        );

      return Number(result) === 1;
    } catch (error) {
      this._logError(
        'refresh-lock',
        '[REDIS] REFRESH LOCK falhou:',
        error
      );

      return false;
    }
  }

  // ============================================================
  // LOCK COM HEARTBEAT
  // ============================================================

  async withPhoneLock(
    phone,
    fn,
    options = {}
  ) {
    const ttl =
      this._normalizeTtl(
        options.ttl ??
          LOCK_TTL
      ) || LOCK_TTL;

    const renewInterval =
      Math.max(
        1000,
        Number(
          options.renewIntervalMs ??
            LOCK_RENEW_INTERVAL_MS
        ) || LOCK_RENEW_INTERVAL_MS
      );

    const token =
      await this.acquireLock(
        phone,
        ttl
      );

    if (!token) {
      return null;
    }

    let stopped = false;
    let refreshRunning = false;

    const heartbeat = setInterval(
      async () => {
        if (
          stopped ||
          refreshRunning
        ) {
          return;
        }

        refreshRunning = true;

        try {
          const refreshed =
            await this.refreshLock(
              phone,
              token,
              ttl
            );

          if (!refreshed) {
            this._logError(
              `lock-lost:${phone}`,
              `[REDIS] Lock perdido para ${phone}:`,
              new Error(
                'refreshLock() não confirmou a posse do lock'
              )
            );
          }
        } finally {
          refreshRunning = false;
        }
      },
      renewInterval
    );

    if (
      typeof heartbeat.unref ===
      'function'
    ) {
      heartbeat.unref();
    }

    try {
      return await fn();
    } finally {
      stopped = true;

      clearInterval(
        heartbeat
      );

      await this.releaseLock(
        phone,
        token
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
    if (
      !Number.isFinite(
        maxPerSecond
      ) ||
      maxPerSecond <= 0
    ) {
      return false;
    }

    const second =
      Math.floor(
        Date.now() / 1000
      );

    const key =
      this.key(
        'rate',
        bucket,
        second
      );

    const count =
      await this.incr(
        key,
        RATE_WINDOW + 1
      );

    return count <= maxPerSecond;
  }

  // ============================================================
  // FILA DE TURNOS PENDENTES
  // ============================================================

  async enqueuePendingItem({
    queueKey,
    phonesKey,
    phone,
    payload,
    score,
    maxItems,
  }) {
    if (
      !this._available() ||
      !queueKey ||
      !phonesKey ||
      !phone ||
      maxItems <= 0
    ) {
      return 0;
    }

    const script = `
      local len =
        redis.call('LLEN', KEYS[1])

      if len >= tonumber(ARGV[1]) then
        return 0
      end

      redis.call(
        'LPUSH',
        KEYS[1],
        ARGV[2]
      )

      redis.call(
        'ZADD',
        KEYS[2],
        ARGV[3],
        ARGV[4]
      )

      return 1
    `;

    try {
      const result =
        await this.client.eval(
          script,
          2,
          queueKey,
          phonesKey,
          String(maxItems),
          String(payload ?? ''),
          String(score),
          String(phone)
        );

      return Number(result) || 0;
    } catch (error) {
      this._logError(
        'enqueue-pending',
        '[REDIS] enqueuePendingItem falhou:',
        error
      );

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
    if (
      !this._available() ||
      !queueKey ||
      !phonesKey ||
      !phone
    ) {
      return false;
    }

    const script = `
      if redis.call('LLEN', KEYS[1]) == 0 then
        return 0
      end

      redis.call(
        'LSET',
        KEYS[1],
        -1,
        ARGV[1]
      )

      redis.call(
        'ZADD',
        KEYS[2],
        ARGV[2],
        ARGV[3]
      )

      return 1
    `;

    try {
      const result =
        await this.client.eval(
          script,
          2,
          queueKey,
          phonesKey,
          String(payload ?? ''),
          String(score),
          String(phone)
        );

      return Number(result) === 1;
    } catch (error) {
      this._logError(
        'reschedule-pending',
        '[REDIS] reschedulePendingItem falhou:',
        error
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
    if (
      !this._available() ||
      !queueKey ||
      !phonesKey ||
      !deadKey ||
      !phone
    ) {
      return false;
    }

    const script = `
      local item =
        redis.call(
          'RPOP',
          KEYS[1]
        )

      if not item then
        return 0
      end

      local deadItem =
        ARGV[1]

      if not deadItem or deadItem == '' then
        deadItem = item
      end

      redis.call(
        'LPUSH',
        KEYS[3],
        deadItem
      )

      local remaining =
        redis.call(
          'LLEN',
          KEYS[1]
        )

      if remaining == 0 then
        redis.call(
          'ZREM',
          KEYS[2],
          ARGV[2]
        )
      end

      return 1
    `;

    try {
      const result =
        await this.client.eval(
          script,
          3,
          queueKey,
          phonesKey,
          deadKey,
          String(payload ?? ''),
          String(phone)
        );

      return Number(result) === 1;
    } catch (error) {
      this._logError(
        'dead-letter',
        '[REDIS] movePendingToDeadLetter falhou:',
        error
      );

      return false;
    }
  }
}

module.exports = RedisService;