// server.js
//
// [ENV-REFACTOR]
//   - Carrega `config/env` no topo (antes de `./src/app`).
//   - Chama `logStartupStatus()` uma única vez, no arranque.
//   - Usa `env.port` e `env.bindHost`.

'use strict';

// ============================================================
// CONFIG — primeiro de todos
// ============================================================
//
// Este require:
//   1. Carrega o `.env` (se existir).
//   2. Valida as variáveis conforme NODE_ENV.
//   3. Aborta se algo crítico faltar em produção.
//
// Tem de vir ANTES de qualquer outro require que dependa de
// `process.env` — incluindo os services em `./src/app`.
//
const env = require('./src/config/env');
const { logStartupStatus } = env;

// ============================================================
// APP
// ============================================================

const {
  app,
  redis,
  interviewService,
  whatsappService,
  logger,
} = require('./src/app');

const PORT = env.port;

// ============================================================
// BOOTSTRAP
// ============================================================

async function bootstrap() {
  try {
    // Registo da configuração (uma única vez).
    logStartupStatus();

    // 1. Redis primeiro
    await redis.initialize();
    logger.info('Redis pronto.');

    // 2. InterviewService (usa Redis)
    await interviewService.initialize();
    logger.info('InterviewService pronto.');

    // 3. HTTP server
    const server = app.listen(PORT, () => {
      logger.info({ port: PORT, env: env.nodeEnv }, 'Servidor HTTP pronto');
    });

    // 4. WhatsApp (último — depende de tudo)
    await whatsappService.initialize();
    logger.info('WhatsApp a inicializar.');

    return server;
  } catch (error) {
    logger.fatal({ err: error }, 'Falha no arranque');
    process.exit(1);
  }
}

// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

let isShuttingDown = false;

async function shutdown(signal, server) {
  if (isShuttingDown) return;
  isShuttingDown = true;

  logger.info({ signal }, 'A encerrar...');

  // 1. Parar de aceitar novos HTTP requests
  if (server) {
    await new Promise((resolve) => server.close(resolve));
    logger.info('HTTP fechado.');
  }

  // 2. Fechar WhatsApp
  try {
    await whatsappService.shutdown();
    logger.info('WhatsApp fechado.');
  } catch (error) {
    logger.error({ err: error }, 'Erro ao fechar WhatsApp');
  }

  // 3. Fechar Redis
  try {
    await redis.close();
    logger.info('Redis fechado.');
  } catch (error) {
    logger.error({ err: error }, 'Erro ao fechar Redis');
  }

  logger.info('Concluído.');
  process.exit(0);
}

// ============================================================
// ERROR HANDLERS GLOBAIS
// ============================================================

process.on('uncaughtException', (error) => {
  logger.fatal({ err: error }, 'Uncaught Exception');
  shutdown('uncaughtException', null);
});

process.on('unhandledRejection', (reason) => {
  logger.fatal({ err: reason }, 'Unhandled Rejection');
  shutdown('unhandledRejection', null);
});

// ============================================================
// RUN
// ============================================================

bootstrap().then((server) => {
  process.on('SIGTERM', () => shutdown('SIGTERM', server));
  process.on('SIGINT', () => shutdown('SIGINT', server));
});