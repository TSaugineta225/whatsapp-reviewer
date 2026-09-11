// src/services/interview.service.js
//
// Camada de transporte entre o WhatsApp e o backend Python.

const { randomUUID } = require('crypto');

const RedisService = require('./redis.service');
const YaneIntegrationService = require('./yane-integration.service');
const metrics = require('./metrics.service');

// ============================================================
// CONFIG
// ============================================================

const DEFAULT_TYPING_MS_PER_CHAR = 22;
const DEFAULT_TYPING_MAX_MS = 2600;

// ============================================================
// HELPERS PUROS
// ============================================================

function clean(value) {
  return String(value ?? '')
    .replace(/^["'`]+|["'`]+$/g, '')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function normalizePhone(phone) {
  if (!phone) return '';
  const digits = String(phone).replace(/\D/g, '');
  if (!digits) return '';
  if (digits.startsWith('258')) return digits;
  if (digits.startsWith('0')) return `258${digits.slice(1)}`;
  return `258${digits}`;
}

// ============================================================
// SERVIÇO
// ============================================================

class InterviewService {
  constructor(redis = null) {
    this.redis = redis || new RedisService();
    this.yane = new YaneIntegrationService();

    this.typingMsPerChar =
      Number(process.env.WHATSAPP_TYPING_MS_PER_CHAR) ||
      DEFAULT_TYPING_MS_PER_CHAR;

    this.typingMax =
      Number(process.env.WHATSAPP_TYPING_MAX_MS) ||
      DEFAULT_TYPING_MAX_MS;
  }

  async initialize() {
    await this.redis.initialize();
  }

  async close() {
    await this.redis.close();
  }

  // ============================================================
  // MAPEAMENTO PHONE ↔ INTERVIEW
  // ============================================================

  async rememberInterview(phone, interviewId) {
    const key = normalizePhone(phone);
    if (!key || !interviewId) return;

    await this.redis.rememberInterview(key, interviewId);
  }

  async resolveInterviewId(phone) {
    const key = normalizePhone(phone);
    if (!key) return null;

    return this.redis.resolveInterviewId(key);
  }

  async forgetInterview(phone) {
    const key = normalizePhone(phone);
    if (key) await this.redis.forgetInterview(key);
  }

  // ============================================================
  // START — chamado pelo backend após criar a entrevista
  // ============================================================

  async startInterview(payload = {}) {
    const phone = normalizePhone(payload.phone);
    const interviewId = payload.interviewId || payload.interview_id;
    const initialMessage = clean(
      payload.initialMessage || payload.initial_message
    );

    if (!phone || !interviewId || !initialMessage) {
      console.warn(
        '[INTERVIEW] startInterview ignorado: payload inválido.',
        { phone, interviewId, hasMessage: Boolean(initialMessage) }
      );
      return { success: false, reason: 'invalid_payload' };
    }

    await this.rememberInterview(phone, interviewId);
    await this.sendHuman(phone, initialMessage);

    metrics.interviewsStarted.inc();

    console.log(
      `[INTERVIEW] Convite enviado | interview=${interviewId} | phone=${phone}`
    );

    return { success: true, interviewId, phone };
  }

  // ============================================================
  // MENSAGEM RECEBIDA
  // ============================================================

  async handleIncomingMessage(from, text, options = {}) {
    const phone = normalizePhone(from);
    const message = clean(text);
    const messageId = options.messageId || null;

    if (!phone || !message) {
      return { handled: false, reason: 'empty' };
    }

    // 1. Dedupe (Baileys às vezes reenvia)
    if (messageId) {
      const isNew = await this.redis.markMessageSeen(messageId);
      if (!isNew) {
        metrics.turnsTotal.inc({ status: 'duplicate' });
        console.log(
          `[INTERVIEW] Mensagem duplicada ignorada | msgId=${messageId}`
        );
        return { handled: false, reason: 'duplicate' };
      }
    }

    // 2. Lock por telefone
    const result = await this.redis.withPhoneLock(phone, async () => {
      const interviewId = await this.resolveInterviewId(phone);

      if (!interviewId) {
        metrics.turnsTotal.inc({ status: 'no_interview' });
        return { handled: false, reason: 'no_active_interview' };
      }

      const turnId = randomUUID();

      let turn;

      try {
        turn = await metrics.time(
          metrics.turnDuration,
          { endpoint: 'turn' },
          () =>
            this.yane.sendInterviewTurn({
              interviewId,
              phone,
              message,
              turnId,
            })
        );
      } catch (error) {
        metrics.turnsTotal.inc({ status: 'backend_error' });
        metrics.errorsTotal.inc({ subsystem: 'backend' });

        console.error(
          `[INTERVIEW] Falha ao contactar backend | interview=${interviewId}`,
          error.message
        );
        return { handled: true, error: true };
      }

      metrics.turnsTotal.inc({ status: 'success' });

      const bubbles = Array.isArray(turn?.bubbles)
        ? turn.bubbles.map(clean).filter(Boolean)
        : [];

      if (bubbles.length) {
        await this.sendBubbles(phone, bubbles);
      }

      if (turn?.finished) {
        await this.forgetInterview(phone);

        metrics.interviewsFinished.inc({
          status: turn.interview_status || 'unknown',
        });

        console.log(
          `[INTERVIEW] Terminada | interview=${interviewId} | ` +
            `status=${turn.interview_status} | ` +
            `credits=${turn.credits_charged}`
        );
      }

      return {
        handled: true,
        finished: Boolean(turn?.finished),
        status: turn?.interview_status || 'in_progress',
      };
    });

    if (result === null) {
      metrics.turnsTotal.inc({ status: 'locked' });
      console.log(
        `[INTERVIEW] Turno em processamento para ${phone}, ignorando duplicado.`
      );
      return { handled: false, reason: 'locked' };
    }

    return result;
  }

  // ============================================================
  // ENVIO HUMANIZADO
  // ============================================================

  async sendBubbles(phone, bubbles) {
    const client = global.whatsappService;
    if (!client) return;

    for (const bubble of bubbles) {
      const text = clean(bubble);
      if (!text) continue;
      await this.sendHuman(phone, text, client);
    }
  }

  async sendHuman(phone, text, client = global.whatsappService) {
    if (!text || !client) return;

    const bubbles = this.splitBubbles(text);

    for (const bubble of bubbles) {
      try {
        if (typeof client.sendPresenceUpdate === 'function') {
          await client.sendPresenceUpdate('composing', phone);
        }
      } catch (_) {
        // Presence é best-effort.
      }

      const typingDelay = Math.min(
        bubble.length * this.typingMsPerChar,
        this.typingMax
      );

      await this.delay(typingDelay);

      try {
        await client.sendMessage(phone, bubble);
      } catch (error) {
        console.error(
          `[WHATSAPP] Falha ao enviar mensagem | phone=${phone}`,
          error.message
        );
        return;
      }

      await this.delay(300);
    }
  }

  // ============================================================
  // DIVISÃO DE BOLHAS
  // ============================================================

  splitBubbles(text) {
    const parts = String(text || '')
      .split(/\n{2,}/)
      .map(clean)
      .filter(Boolean);

    if (!parts.length) return [];

    const merged = this.mergeSmallBubbles(parts);

    if (merged.length <= 3) return merged;

    return [
      merged[0],
      merged.slice(1, -1).join('\n\n'),
      merged[merged.length - 1],
    ];
  }

  mergeSmallBubbles(parts) {
    const result = [];

    for (const part of parts) {
      const previous = result[result.length - 1];

      if (previous && (previous.length < 60 || part.length < 40)) {
        result[result.length - 1] = `${previous}\n\n${part}`;
      } else {
        result.push(part);
      }
    }

    return result;
  }

  delay(ms) {
    return new Promise((resolve) =>
      setTimeout(resolve, Math.max(0, Number(ms) || 0))
    );
  }
}

module.exports = InterviewService;