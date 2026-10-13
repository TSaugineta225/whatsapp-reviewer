// src/config/env.js
//
// Configuração central do bot Node.
//
// Este módulo é o ÚNICO sítio onde se lê process.env. Todos os
// serviços importam `env` daqui, garantindo:
//
//   1. O `.env` é carregado ANTES de qualquer outro require correr.
//   2. As variáveis obrigatórias são validadas conforme o ambiente.
//   3. Em produção, a aplicação falha no arranque se faltar algo
//      crítico — em vez de cair silenciosamente em defaults.
//   4. Os valores são congelados: ninguém os modifica em runtime.
//
// Uso:
//     const env = require('../config/env');
//     console.log(env.yane.apiUrl);
//     env.logStartupStatus();
//
// Ordem dos requires no `server.js`:
//     require('./src/config/env');   // ← PRIMEIRO
//     const { app } = require('./src/app');

'use strict';

const path = require('path');
const dotenv = require('dotenv');

// =============================================================================
// 1. CARREGAR .ENV (ANTES DE TUDO)
// =============================================================================

// Carrega o `.env` que está na raiz do projecto (mesma pasta do
// docker-compose.yml / package.json). Não falha se não existir —
// em Docker/Cloud Run, as variáveis são injectadas directamente.
dotenv.config({
  path: path.resolve(__dirname, '..', '..', '.env'),
});

// =============================================================================
// 2. HELPERS
// =============================================================================

function clean(value) {
  return String(value ?? '').trim();
}

function asString(value, fallback = '') {
  const v = clean(value);
  return v || fallback;
}

function asInt(value, fallback) {
  const n = Number(clean(value));
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
}

function asBool(value, fallback = false) {
  const v = clean(value).toLowerCase();
  if (['1', 'true', 'yes', 'on'].includes(v)) return true;
  if (['0', 'false', 'no', 'off'].includes(v)) return false;
  return fallback;
}

function asUrl(value, fallback = '') {
  const v = clean(value).replace(/\/+$/, '');
  return v || fallback;
}

// =============================================================================
// 3. AMBIENTE
// =============================================================================

const NODE_ENV = asString(process.env.NODE_ENV, 'development').toLowerCase();

const ALLOWED_ENVS = ['development', 'staging', 'production'];
if (!ALLOWED_ENVS.includes(NODE_ENV)) {
  console.warn(
    `[env] NODE_ENV inválido: "${NODE_ENV}". ` +
      `A usar "development". Valores aceites: ${ALLOWED_ENVS.join(', ')}.`
  );
}

const effectiveEnv = ALLOWED_ENVS.includes(NODE_ENV) ? NODE_ENV : 'development';

const IS_PRODUCTION = effectiveEnv === 'production';
const IS_STAGING = effectiveEnv === 'staging';
const IS_DEVELOPMENT = effectiveEnv === 'development';

// =============================================================================
// 4. CONFIGURAÇÃO
// =============================================================================

const env = {
  // -------------------------------------------------------------------------
  // AMBIENTE
  // -------------------------------------------------------------------------
  nodeEnv: effectiveEnv,
  isProduction: IS_PRODUCTION,
  isStaging: IS_STAGING,
  isDevelopment: IS_DEVELOPMENT,
  debug: asBool(process.env.DEBUG, IS_DEVELOPMENT),

  // -------------------------------------------------------------------------
  // HTTP SERVER
  // -------------------------------------------------------------------------
  port: asInt(process.env.PORT, 3000),
  bindHost: asString(process.env.BOT_BIND_HOST, '127.0.0.1'),

  // -------------------------------------------------------------------------
  // REDIS
  // -------------------------------------------------------------------------
  redis: {
    url: asString(process.env.REDIS_URL, 'redis://localhost:6379/0'),
    prefix: asString(process.env.REDIS_PREFIX, 'yane'),
  },

  // -------------------------------------------------------------------------
  // YANE API (backend Python)
  // -------------------------------------------------------------------------
  yane: {
    // ⚠️  O valor DEVE incluir o prefixo `/api` (ou `/api/v1`),
    //     porque o serviço anexa `/interviews/...` e `/health`
    //     directamente ao baseUrl.
    apiUrl: asUrl(process.env.YANE_API_URL, 'http://localhost:8000/api'),

    serviceToken: asString(process.env.YANE_SERVICE_TOKEN, ''),
    apiKey: asString(process.env.YANE_API_KEY, ''),

    apiTimeoutMs: asInt(process.env.YANE_API_TIMEOUT, 15_000),
    turnTimeoutMs: asInt(process.env.YANE_TURN_TIMEOUT, 300_000),
    lookupTimeoutMs: asInt(process.env.YANE_LOOKUP_TIMEOUT, 10_000),
    statusTimeoutMs: asInt(process.env.YANE_STATUS_TIMEOUT, 5_000),
    healthTimeoutMs: asInt(process.env.YANE_HEALTH_TIMEOUT, 3_000),
  },

  // -------------------------------------------------------------------------
  // BOT / WHATSAPP
  // -------------------------------------------------------------------------
  whatsapp: {
    botSecret: asString(process.env.WHATSAPP_BOT_SECRET, ''),
    defaultCountryCode: asString(process.env.DEFAULT_COUNTRY_CODE, '258'),
    typingMsPerChar: asInt(process.env.WHATSAPP_TYPING_MS_PER_CHAR, 22),
    typingMaxMs: asInt(process.env.WHATSAPP_TYPING_MAX_MS, 2_600),
  },

  // -------------------------------------------------------------------------
  // MÉTRICAS / OBSERVABILIDADE
  // -------------------------------------------------------------------------
  metrics: {
    token: asString(process.env.METRICS_TOKEN, ''),
  },

  // -------------------------------------------------------------------------
  // LOGGING
  // -------------------------------------------------------------------------
  logging: {
    level: asString(
      process.env.LOG_LEVEL,
      IS_PRODUCTION ? 'info' : 'debug'
    ).toLowerCase(),
  },
};

// =============================================================================
// 5. VALIDAÇÃO POR AMBIENTE
// =============================================================================

const errors = [];
const warnings = [];

// -------------------------------------------------------------------------
// REDIS_URL
// -------------------------------------------------------------------------
if (!env.redis.url) {
  errors.push('REDIS_URL é obrigatório.');
} else if (!/^rediss?:\/\//.test(env.redis.url)) {
  errors.push('REDIS_URL deve começar por redis:// ou rediss://.');
}

// -------------------------------------------------------------------------
// YANE_API_URL
// -------------------------------------------------------------------------
if (!env.yane.apiUrl) {
  errors.push('YANE_API_URL é obrigatório.');
} else if (!/^https?:\/\//.test(env.yane.apiUrl)) {
  errors.push('YANE_API_URL deve ser uma URL HTTP(S).');
} else if (IS_PRODUCTION && /localhost|127\.0\.0\.1/.test(env.yane.apiUrl)) {
  errors.push('YANE_API_URL não pode apontar para localhost em produção.');
} else if (IS_PRODUCTION && !env.yane.apiUrl.startsWith('https://')) {
  errors.push('YANE_API_URL deve usar HTTPS em produção.');
}

// -------------------------------------------------------------------------
// TOKENS
// -------------------------------------------------------------------------
if (!env.yane.serviceToken && !env.whatsapp.botSecret) {
  if (IS_PRODUCTION || IS_STAGING) {
    errors.push(
      'YANE_SERVICE_TOKEN (ou WHATSAPP_BOT_SECRET) é obrigatório em ' +
        'staging/produção. Sem isto, o backend Python recebe 401.'
    );
  } else {
    warnings.push(
      'YANE_SERVICE_TOKEN não definido. O bot vai aceitar todos os ' +
        'pedidos sem autenticação (aceitável em dev, inseguro em produção).'
    );
  }
} else if (IS_PRODUCTION) {
  const token = env.yane.serviceToken || env.whatsapp.botSecret;
  if (token.length < 32) {
    errors.push(
      'O token do bot deve ter pelo menos 32 caracteres em produção.'
    );
  }
  if (token.toLowerCase().includes('changeme')) {
    errors.push('O token do bot não pode conter "changeme".');
  }
}

// -------------------------------------------------------------------------
// LOG_LEVEL
// -------------------------------------------------------------------------
const allowedLogLevels = ['debug', 'info', 'warn', 'error'];
if (!allowedLogLevels.includes(env.logging.level)) {
  warnings.push(
    `LOG_LEVEL inválido: "${env.logging.level}". ` +
      `A usar "info". Valores aceites: ${allowedLogLevels.join(', ')}.`
  );
  env.logging.level = 'info';
}

// -------------------------------------------------------------------------
// Abortar ou avisar
// -------------------------------------------------------------------------
if (errors.length > 0) {
  const header = `[env] Configuração inválida para ${effectiveEnv.toUpperCase()}:`;
  console.error(header);
  errors.forEach((e) => console.error(`  - ${e}`));
  throw new Error(
    'Configuração inválida. Corrige o .env antes de arrancar.'
  );
}

if (warnings.length > 0) {
  console.warn(`[env] Avisos em ${effectiveEnv.toUpperCase()}:`);
  warnings.forEach((w) => console.warn(`  - ${w}`));
}

// =============================================================================
// 6. LOG DE ARRANQUE
// =============================================================================
//
// Definido ANTES do Object.freeze(env), para podermos anexá-lo ao
// objecto `env` sem violar o freeze.
//
// Assim o API fica:
//     const env = require('./src/config/env');
//     env.logStartupStatus();
//
// e não polui o namespace com um export separado.

/**
 * Registra o estado da configuração sem revelar segredos.
 *
 * Não é chamado automaticamente — o `server.js` invoca-o uma única
 * vez, no arranque. Assim evitamos poluir testes, migrações e CLIs.
 */
function logStartupStatus() {
  const prefix = `[${env.nodeEnv.toUpperCase()}]`;

  console.log(`${prefix} Configuração carregada.`);

  // Redis
  console.log(
    `${prefix} Redis: ${env.redis.url.replace(/:\/\/.*@/, '://***@')} ` +
      `(prefix="${env.redis.prefix}")`
  );

  // Yane API
  console.log(
    `${prefix} Yane API: ${env.yane.apiUrl} ` +
      `(auth=${env.yane.serviceToken ? 'yes' : 'no'}, ` +
      `apiKey=${env.yane.apiKey ? 'yes' : 'no'})`
  );

  // Bot
  console.log(
    `${prefix} WhatsApp: botSecret=${
      env.whatsapp.botSecret ? 'configured' : 'missing'
    }, countryCode=${env.whatsapp.defaultCountryCode}`
  );

  // Métricas
  console.log(
    `${prefix} Métricas: ${
      env.metrics.token ? 'protegidas por token' : 'públicas'
    }`
  );

  // Servidor
  console.log(`${prefix} HTTP: http://${env.bindHost}:${env.port}`);
}

// Anexar a função ao env ANTES do freeze.
env.logStartupStatus = logStartupStatus;

// =============================================================================
// 7. CONGELAR
// =============================================================================

Object.freeze(env.redis);
Object.freeze(env.yane);
Object.freeze(env.whatsapp);
Object.freeze(env.metrics);
Object.freeze(env.logging);
Object.freeze(env);

// =============================================================================
// 8. EXPORTS
// =============================================================================

module.exports = env;