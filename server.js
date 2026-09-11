// server.js
const {
  app,
  redis,
  interviewService,
  whatsappService,
} = require('./src/app');

const PORT = Number(process.env.PORT) || 3000;

// ============================================================
// BOOTSTRAP
// ============================================================

async function bootstrap() {
  try {
    // 1. Redis primeiro
    await redis.initialize();
    console.log('[BOOT] Redis pronto.');

    // 2. InterviewService (usa Redis)
    await interviewService.initialize();
    console.log('[BOOT] InterviewService pronto.');

    // 3. HTTP server
    const server = app.listen(PORT, () => {
      console.log(`[BOOT] Servidor HTTP a ouvir em :${PORT}`);
    });

    // 4. WhatsApp (último — depende de tudo)
    await whatsappService.initialize();
    console.log('[BOOT] WhatsApp a inicializar.');

    return server;
  } catch (error) {
    console.error('[BOOT] Falha no arranque:', error);
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
  console.log(`[SHUTDOWN] Recebido ${signal}. A encerrar...`);

  // 1. Parar de aceitar novos HTTP requests
  if (server) {
    await new Promise((resolve) => server.close(resolve));
    console.log('[SHUTDOWN] HTTP fechado.');
  }

  // 2. Fechar WhatsApp (para de processar mensagens)
  try {
    await whatsappService.shutdown();
    console.log('[SHUTDOWN] WhatsApp fechado.');
  } catch (error) {
    console.error('[SHUTDOWN] Erro ao fechar WhatsApp:', error.message);
  }

  // 3. Fechar Redis
  try {
    await redis.close();
    console.log('[SHUTDOWN] Redis fechado.');
  } catch (error) {
    console.error('[SHUTDOWN] Erro ao fechar Redis:', error.message);
  }

  console.log('[SHUTDOWN] Concluído.');
  process.exit(0);
}

// ============================================================
// ERROR HANDLERS GLOBAIS
// ============================================================

process.on('uncaughtException', (error) => {
  console.error('[FATAL] Uncaught Exception:', error);
  // Não sair imediatamente: deixa o shutdown limpar.
  shutdown('uncaughtException', null);
});

process.on('unhandledRejection', (reason) => {
  console.error('[FATAL] Unhandled Rejection:', reason);
  // Rejeições não tratadas são bugs — deixar o processo limpar.
  shutdown('unhandledRejection', null);
});

// ============================================================
// RUN
// ============================================================

bootstrap().then((server) => {
  process.on('SIGTERM', () => shutdown('SIGTERM', server));
  process.on('SIGINT', () => shutdown('SIGINT', server));
});