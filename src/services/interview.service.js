// src/services/interview.service.js

const {
  STAGES,
  INTERVIEW_CONFIG,
  AGENTS,
  COMPANY,
  JOB_VACANCY,
} = require('../config/interview');

const BaseService = require('./base.service');
const AIService = require('./ai.service');
const CalendarService = require('./calendar.service');
const InterviewSession = require('../models/interview-session.model');
const YaneIntegrationService = require('./yane-integration.service');

// ============================================================
// CONSTANTES
// ============================================================

const DEFAULT_STAGE_ID = 'initial';
const DEFAULT_HISTORY_LIMIT = 10;
const DEFAULT_QUESTIONS_PER_STAGE = 3;

const DEFAULT_TYPING_MS_PER_CHAR = 22;
const DEFAULT_TYPING_MAX_MS = 2600;

const MAX_USED_OPENERS = 40;
const MAX_RECENT_QUESTIONS = 12;
const MAX_RECORDED_EVIDENCE = 30;
const MAX_RECORDED_GAPS = 30;

const OVERUSED_WORD_THRESHOLD = 3;
const MAX_IDENTITY_ATTEMPTS = 3;

const FALLBACK_ANSWER =
  'Essa informação preciso de confirmar com a equipa antes de lhe responder com segurança.';

const FALLBACK_ACKNOWLEDGEMENTS = [
  'Esse detalhe ajuda a perceber melhor a sua experiência.',
  'Isso ajuda a colocar essa experiência em contexto.',
  'Fica mais claro como lidou com essa situação.',
  'Esse exemplo ajuda a perceber como trabalha na prática.',
  'Esse contexto é útil para compreender melhor o seu percurso.',
];

const FALLBACK_SHORT_ANSWER_PROMPTS = [
  'Pode dar-me um exemplo concreto disso?',
  'Como foi essa situação na prática?',
  'O que fez exactamente nessa situação?',
  'Pode explicar um pouco melhor essa experiência?',
];

const FALLBACK_REPHRASES = [
  'Deixe-me perguntar de outra forma:',
  'Voltando ao que lhe perguntei:',
  'Queria perceber melhor esta parte:',
];

const FALLBACK_RESUME_LINES = [
  'Voltando ao que estávamos a falar:',
  'Continuando a partir disso:',
  'Pegando nesse ponto:',
];

const FALLBACK_ALTERNATIVE_QUESTIONS = [
  'Pode dar-me um exemplo concreto disso no seu percurso?',
  'Qual foi exactamente o seu papel e responsabilidade nessa situação?',
  'O que fez a seguir para garantir o resultado?',
  'Qual foi o impacto directo dessa acção na equipa ou no projecto?',
];

const BANNED_OPENERS = [
  'entendo',
  'entendi',
  'percebo',
  'percebi',
  'compreendo',
  'certo',
  'ok',
  'okay',
  'interessante',
  'muito interessante',
  'boa',
  'bom',
  'ótimo',
  'óptimo',
  'perfeito',
  'excelente',
  'claro',
  'exacto',
  'exato',
  'faz sentido',
  'que bom',
  'obrigado pela resposta',
  'obrigado por partilhar',
  'agradeço a partilha',
];

const BANNED_RE = new RegExp(
  '^\\s*(' +
    BANNED_OPENERS.map(escapeRegex).join('|') +
    ')\\b[\\s,.:;!—-]*',
  'i'
);

const STOPWORDS = new Set(
  (
    'a o as os um uma de do da dos das em no na nos nas e ou que se por para com sem sobre ' +
    'ao aos à às pelo pela isso isto esse essa este esta seu sua seus suas meu minha meus minhas ' +
    'muito mais já lhe me te nos vos ser estar tem ter é são foi era como quando onde qual quais ' +
    'não sim dele dela'
  ).split(/\s+/)
);

const SIMPLE_TONE = Object.freeze({
  simple:
    'Usa palavras simples, frases curtas e naturais. Evita jargão desnecessário. Uma pergunta de cada vez.',
  medium:
    'Usa linguagem profissional, clara e acessível. Mantém naturalidade e evita formalidade excessiva.',
  advanced:
    'Usa linguagem profissional mais elaborada, mas continua natural, humana e fácil de acompanhar.',
});

// ============================================================
// HELPERS PUROS
// ============================================================

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function pick(items = []) {
  if (!Array.isArray(items) || !items.length) return '';
  return items[Math.floor(Math.random() * items.length)] || '';
}

function clean(value) {
  return String(value ?? '')
    .replace(/^["'`]+|["'`]+$/g, '')
    .replace(/[ \t]+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function extractWords(value) {
  return String(value ?? '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z\s]/g, ' ')
    .split(/\s+/)
    .filter(
      (word) => word.length > 3 && !STOPWORDS.has(word)
    );
}

function clamp(value, min, max) {
  const number = Number(value);

  if (!Number.isFinite(number)) {
    return min;
  }

  return Math.max(min, Math.min(max, number));
}

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function uniqueStrings(items) {
  const seen = new Set();
  const result = [];

  for (const item of asArray(items)) {
    const value = clean(item);

    if (!value || seen.has(value)) {
      continue;
    }

    seen.add(value);
    result.push(value);
  }

  return result;
}

// ============================================================
// SERVIÇO
// ============================================================

class InterviewService extends BaseService {
  constructor() {
    super();

    this.sessions = new Map();

    this.questionCollector = new Map();
    this.collectorTimers = new Map();

    this.aiService = new AIService();
    this.calendarService = new CalendarService();
    this.yaneIntegration = new YaneIntegrationService();

    this.stages = Array.isArray(STAGES) ? STAGES : [];

    this.typingMsPerChar =
      Number(INTERVIEW_CONFIG.typingMsPerChar) ||
      DEFAULT_TYPING_MS_PER_CHAR;

    this.typingMax =
      Number(INTERVIEW_CONFIG.typingMaxMs) ||
      DEFAULT_TYPING_MAX_MS;

    this.questionsPerStage =
      Number(INTERVIEW_CONFIG.questionsPerStage) ||
      DEFAULT_QUESTIONS_PER_STAGE;
  }

  // ============================================================
  // SESSÃO
  // ============================================================

  getSession(userId) {
    return this.sessions.get(userId);
  }

  endSession(userId) {
    const session = this.sessions.get(userId);

    if (session) {
      try {
        session.cancelTimeout?.();
      } catch (error) {
        console.warn(
          '[INTERVIEW] Falha ao cancelar timeout:',
          error.message
        );
      }
    }

    this.sessions.delete(userId);
    this.clearQuestionCollection(userId);
  }

  resetInterviewState(session) {
    if (!session) return;

    const preserved = {
      userId: session.userId,
      expectedCandidateName: session.expectedCandidateName || null,
      candidateName: session.candidateName || null,
      candidateEmail: session.candidateEmail || null,
      candidateCv: session.candidateCv || null,
      jobTitle: session.jobTitle || null,
      jobDescription: session.jobDescription || null,
      jobRequirements: session.jobRequirements || [],
      company: session.company || null,
      jobVacancy: session.jobVacancy || null,
      interviewId: session.interviewId || null,
      holdTransactionId: session.holdTransactionId || null,
      recruiterId: session.recruiterId || null,
      languageLevel: session.languageLevel || 'simple',
    };

    if (typeof session.resetState === 'function') {
      session.resetState();
      Object.assign(session, preserved);
      session.languageLevel = preserved.languageLevel || 'simple';
      return;
    }

    this.resetSessionFields(session);
    Object.assign(session, preserved);
  }

  resetSessionFields(session) {
    session.stageIndex = 0;
    session.stage = DEFAULT_STAGE_ID;

    session.currentQuestion = 0;
    session.questionCounter = 0;
    session.lastQuestion = null;

    session.askedQuestions = new Set();
    session.topicsCovered = [];
    session.verifiedClaims = [];
    session.identifiedGaps = [];

    session.isIdentityVerified = false;
    session.identityAttempts = 0;

    session.inQASection = false;
    session.followUpPending = false;
    session.pendingCancel = false;

    session.offTopicAttempts = 0;
    session.shortAnswerStreak = 0;
    session.qaOffered = false;

    session.scores = [];
    session.conversationHistory = [];

    session.usedOpeners = [];
    session.usedWords = {};
    session.ackStreak = 0;
    session.turnCount = 0;
  }

  scheduleSessionTimeout(userId, session) {
    if (!session) return;

    try {
      session.cancelTimeout?.();
    } catch (_) {}

    if (typeof session.scheduleTimeout !== 'function') {
      return;
    }

    session.scheduleTimeout(async (expiredSession) => {
      const currentSession = this.sessions.get(userId);

      if (
        !currentSession ||
        currentSession !== expiredSession ||
        !this.isSessionExpired(currentSession)
      ) {
        return;
      }

      try {
        await this.sendMessage(
          userId,
          this.generateTimeoutMessage(currentSession)
        );
      } finally {
        this.endSession(userId);
      }

      console.log(
        `[TIMEOUT] Sessão encerrada por inactividade: ${userId}`
      );
    });
  }

  isSessionExpired(session) {
    if (!session) return true;

    if (typeof session.isExpired === 'function') {
      return session.isExpired();
    }

    const timeoutMinutes =
      Number(INTERVIEW_CONFIG.timeoutMinutes) || 20;

    const lastInteraction = Number(session.lastInteraction || 0);

    if (!lastInteraction) {
      return false;
    }

    return (
      Date.now() - lastInteraction >
      timeoutMinutes * 60 * 1000
    );
  }

  // ============================================================
  // INÍCIO DA ENTREVISTA
  // ============================================================

  async startInterview(
    userId,
    payload = {},
    legacyCompany = null,
    legacyJobVacancy = null
  ) {
    try {
      const config = this.normalizeInterviewConfig(
        payload,
        legacyCompany,
        legacyJobVacancy
      );

      const session = new InterviewSession(userId);

      this.applyInterviewConfig(session, config);
      this.resetInterviewState(session);

      this.sessions.set(userId, session);

      session.updateLastInteraction?.();
      this.scheduleSessionTimeout(userId, session);

      const welcomeMessage =
        this.generateWelcomeMessage(session);

      const whatsapp = global.whatsappService;

      if (
        whatsapp &&
        typeof whatsapp.sendInteractiveMessage === 'function'
      ) {
        await whatsapp.sendInteractiveMessage(
          userId,
          `Entrevista - ${session.getCompanyName()}`,
          welcomeMessage,
          [
            {
              id: 'start_interview',
              text: '✅ Iniciar Entrevista',
            },
            {
              id: 'later',
              text: '⏰ Responder Depois',
            },
          ]
        );

        console.log(
          `[SESSÃO] Convite interativo enviado para ${userId}`
        );
      } else {
        await this.sendMessage(userId, welcomeMessage);
      }

      return welcomeMessage;
    } catch (error) {
      throw this.handleError(error, 'Start Interview');
    }
  }

  normalizeInterviewConfig(
    payload,
    legacyCompany,
    legacyJobVacancy
  ) {
    if (typeof payload === 'string') {
      return {
        jobTitle: payload,
        company: legacyCompany,
        jobVacancy: legacyJobVacancy,
      };
    }

    return payload && typeof payload === 'object'
      ? payload
      : {};
  }

  applyInterviewConfig(session, config) {
    const vacancy = config.jobVacancy || JOB_VACANCY;

    session.expectedCandidateName =
      config.candidateName ||
      config.expectedCandidateName ||
      null;

    session.candidateName = null;

    session.candidateEmail =
      config.candidateEmail || null;

    session.candidateCv =
      config.candidateCv ||
      config.cvText ||
      null;

    session.jobTitle =
      config.jobTitle ||
      vacancy?.title ||
      JOB_VACANCY.title ||
      'Vaga de Emprego';

    session.jobDescription =
      config.jobDescription ||
      vacancy?.description ||
      JOB_VACANCY.description ||
      '';

    session.jobRequirements =
      config.jobRequirements ||
      vacancy?.requirements ||
      JOB_VACANCY.requirements ||
      [];

    session.company =
      config.company ||
      COMPANY ||
      null;

    session.jobVacancy = vacancy || null;

    session.interviewId =
      config.interviewId || null;

    session.holdTransactionId =
      config.holdTransactionId || null;

    session.recruiterId =
      config.recruiterId || null;
  }

  generateWelcomeMessage(session) {
    const companyName = session.getCompanyName();
    const jobTitle = session.getJobTitle();
    const industry = clean(session.company?.industry);

    const roleText = industry
      ? `Estamos a recrutar para a posição de ${jobTitle}, na área de ${industry}.`
      : `Estamos a recrutar para a posição de ${jobTitle}.`;

    return [
      `Olá! Aqui é da equipa de selecção da ${companyName}.`,
      roleText,
      '',
      'A ideia é termos uma conversa dinâmica sobre o seu percurso, o seu CV e como as suas experiências se alinham a esta oportunidade.',
      '',
      'Para garantir a segurança do processo e confirmarmos a sua candidatura, por favor diga-me o seu nome completo.',
    ].join('\n');
  }

  generateTimeoutMessage(session) {
    const name = this.firstName(session);

    return [
      `Olá, ${name}.`,
      '',
      'Ficámos algum tempo sem receber resposta e a entrevista foi encerrada por inactividade.',
      '',
      `Pode iniciar novamente quando estiver disponível. Obrigado pelo seu interesse na ${session.getCompanyName()}.`,
    ].join('\n');
  }

  async sendMessage(userId, text) {
    const message = clean(text);

    if (!message || !global.whatsappService) {
      return;
    }

    try {
      await global.whatsappService.sendMessage(
        userId,
        message
      );
    } catch (error) {
      console.error(
        '[WHATSAPP] Erro ao enviar mensagem:',
        error.message
      );
    }
  }

  // ============================================================
  // ENVIO HUMANIZADO
  // ============================================================

  async sendHuman(
    userId,
    text,
    client = global.whatsappService
  ) {
    if (!text || !client) {
      return;
    }

    const bubbles = this.splitBubbles(text);

    for (const bubble of bubbles) {
      try {
        if (
          typeof client.sendPresenceUpdate === 'function'
        ) {
          await client.sendPresenceUpdate(
            'composing',
            userId
          );
        }
      } catch (_) {}

      const typingDelay = Math.min(
        bubble.length * this.typingMsPerChar,
        this.typingMax
      );

      await this.delay(typingDelay);

      await client.sendMessage(
        userId,
        bubble
      );

      await this.delay(300);
    }
  }

  splitBubbles(text) {
    const parts = String(text || '')
      .split(/\n{2,}/)
      .map(clean)
      .filter(Boolean);

    if (!parts.length) {
      return [];
    }

    const merged = this.mergeSmallBubbles(parts);

    if (merged.length <= 3) {
      return merged;
    }

    return [
      merged[0],
      merged.slice(1, -1).join('\n\n'),
      merged[merged.length - 1],
    ];
  }

  mergeSmallBubbles(parts) {
    const result = [];

    for (const part of parts) {
      const previous =
        result[result.length - 1];

      if (
        previous &&
        (previous.length < 60 ||
          part.length < 40)
      ) {
        result[result.length - 1] =
          `${previous}\n\n${part}`;
      } else {
        result.push(part);
      }
    }

    return result;
  }

  delay(ms) {
    return new Promise((resolve) => {
      setTimeout(resolve, Math.max(0, Number(ms) || 0));
    });
  }

  // ============================================================
  // IDENTIDADE
  // ============================================================

  async validateCandidateIdentity(
    session,
    inputName
  ) {
    const normalizedInput =
      this.normalizeName(inputName);

    if (!normalizedInput) {
      return {
        valid: false,
        reason: 'NAME_INVALID',
        message:
          'Não consegui identificar um nome válido. Pode escrever o seu nome completo, por favor?',
      };
    }

    if (!session.expectedCandidateName) {
      session.candidateName = normalizedInput;
      session.isIdentityVerified = true;

      return { valid: true };
    }

    const normalizedExpected =
      this.normalizeName(
        session.expectedCandidateName
      );

    if (!normalizedExpected) {
      session.candidateName = normalizedInput;
      session.isIdentityVerified = true;

      return { valid: true };
    }

    const inputTokens = new Set(
      extractWords(normalizedInput)
    );

    const expectedTokens = new Set(
      extractWords(normalizedExpected)
    );

    const commonTokens = [...inputTokens].filter(
      (token) => expectedTokens.has(token)
    );

    const directMatch =
      normalizedInput.includes(normalizedExpected) ||
      normalizedExpected.includes(normalizedInput);

    if (
      directMatch ||
      commonTokens.length >= 1
    ) {
      session.candidateName =
        session.expectedCandidateName ||
        normalizedInput;

      session.isIdentityVerified = true;

      return { valid: true };
    }

    const aiValidationPrompt = `
Compara se os dois nomes pertencem plausivelmente à mesma pessoa.

Considera:
- abreviações;
- nomes compostos;
- ordem ligeiramente diferente;
- variações de sobrenome;
- diferenças culturais de escrita.

Não assumes que são a mesma pessoa apenas porque partilham uma palavra comum.

Nome registado:
"${session.expectedCandidateName}"

Nome respondido:
"${normalizedInput}"

Responde estritamente em JSON:

{
  "isSamePerson": true,
  "confidence": 0.0
}
`;

    try {
      const rawAi = await this.generateAI(
        aiValidationPrompt,
        AGENTS.recruiter.systemPrompt
      );

      const parsedAi =
        this.parseJsonObject(rawAi);

      if (
        parsedAi?.isSamePerson === true &&
        clamp(parsedAi.confidence, 0, 1) >= 0.6
      ) {
        session.candidateName =
          session.expectedCandidateName;

        session.isIdentityVerified = true;

        return { valid: true };
      }
    } catch (error) {
      console.warn(
        '[IDENTITY] Erro na validação AI:',
        error.message
      );
    }

    session.identityAttempts =
      Number(session.identityAttempts || 0) + 1;

    if (
      session.identityAttempts >=
      MAX_IDENTITY_ATTEMPTS
    ) {
      return {
        valid: false,
        reason: 'MAX_ATTEMPTS_EXCEEDED',
        message:
          `O nome fornecido não corresponde ao registo de candidatura (${session.expectedCandidateName}). ` +
          'Por motivos de segurança, a entrevista não pode prosseguir. Por favor, entre em contacto com o RH.',
      };
    }

    return {
      valid: false,
      reason: 'MISMATCH',
      message:
        `O nome informado (${normalizedInput}) difere do nome registado para esta vaga (${session.expectedCandidateName}).\n\n` +
        'Por favor, confirme se é o próprio candidato ou digite o seu nome completo conforme consta da candidatura.',
    };
  }

  // ============================================================
  // FLUXO PRINCIPAL
  // ============================================================

  async handleResponse(userId, message) {
    const session = this.getSession(userId);

    if (!session) {
      return null;
    }

    const text = clean(message);

    if (!text) {
      return 'Pode enviar a sua resposta quando estiver pronto.';
    }

    const cancellation =
      await this.handleCancellationIntent(
        userId,
        session,
        text
      );

    if (cancellation !== null) {
      return cancellation;
    }

    if (this.isSessionExpired(session)) {
      this.endSession(userId);

      return (
        'Ficámos algum tempo sem falar e a sessão foi encerrada. ' +
        'Quando quiser retomar, escreva "OLÁ" e começamos novamente.'
      );
    }

    session.updateLastInteraction?.();
    session.turnCount =
      Number(session.turnCount || 0) + 1;

    this.scheduleSessionTimeout(
      userId,
      session
    );

    session.updateLanguageLevel?.(text);

    // ----------------------------------------------------------
    // 1. IDENTIDADE
    // ----------------------------------------------------------

    if (!session.isIdentityVerified) {
      const identityCheck =
        await this.validateCandidateIdentity(
          session,
          text
        );

      if (!identityCheck.valid) {
        if (
          identityCheck.reason ===
          'MAX_ATTEMPTS_EXCEEDED'
        ) {
          this.endSession(userId);
        }

        return identityCheck.message;
      }

      const question =
        await this.askFirstQuestion(session);

      this.rememberQuestion(
        session,
        question
      );

      return [
        `Confirmação efectuada com sucesso! Muito prazer, ${this.firstName(session)}.`,
        '',
        question,
      ].join('\n');
    }

    // ----------------------------------------------------------
    // 2. Q&A
    // ----------------------------------------------------------

    if (this.isCandidateQuestion(text)) {
      const answer =
        await this.answerCandidate(
          session,
          text
        );

      return [
        answer,
        '',
        this.resumeLine(session),
      ].join('\n');
    }

    // ----------------------------------------------------------
    // 3. RESPOSTA CURTA
    // ----------------------------------------------------------

    if (this.isTooShort(text)) {
      session.shortAnswerStreak =
        Number(session.shortAnswerStreak || 0) + 1;

      const maxNudges =
        Number(
          INTERVIEW_CONFIG.maxShortAnswerNudges
        ) || 1;

      if (
        session.shortAnswerStreak <= maxNudges
      ) {
        return this.varied(
          session,
          FALLBACK_SHORT_ANSWER_PROMPTS
        );
      }
    } else {
      session.shortAnswerStreak = 0;
    }

    // ----------------------------------------------------------
    // 4. ANÁLISE IA
    // ----------------------------------------------------------

    const turn =
      await this.analyzeTurn(
        session,
        text
      );

    // ----------------------------------------------------------
    // 5. OFF-TOPIC
    // ----------------------------------------------------------

    if (turn.off_topic) {
      session.offTopicAttempts =
        Number(session.offTopicAttempts || 0) + 1;

      const tolerance =
        Number(
          INTERVIEW_CONFIG.offTopicToleranceBeforeSkip
        ) || 2;

      if (
        session.offTopicAttempts >=
        tolerance
      ) {
        return this.advanceFocus(
          session,
          turn.acknowledgement
        );
      }

      return this.rephrase(session);
    }

    session.offTopicAttempts = 0;

    // ----------------------------------------------------------
    // 6. MEMÓRIA
    // ----------------------------------------------------------

    this.recordTurn(
      session,
      text,
      turn
    );

    // ----------------------------------------------------------
    // 7. CONCLUSÃO
    // ----------------------------------------------------------

    if (turn.interview_complete) {
      return this.concludeInterview(session);
    }

    session.questionCounter =
      Number(session.questionCounter || 0) + 1;

    const currentStage =
      this.getStageForSession(session);

    const stageLimit =
      Number(
        currentStage?.questionsPerStage
      ) || this.questionsPerStage;

    if (
      turn.should_transition ||
      session.questionCounter >= stageLimit
    ) {
      return this.advanceFocus(
        session,
        turn.acknowledgement
      );
    }

    // ----------------------------------------------------------
    // 8. PRÓXIMA PERGUNTA
    // ----------------------------------------------------------

    if (turn.next_question) {
      const question =
        this.dedupeQuestion(
          session,
          turn.next_question
        );

      this.rememberQuestion(
        session,
        question
      );

      return this.compose(
        session,
        turn.acknowledgement,
        question
      );
    }

    return this.advanceFocus(
      session,
      turn.acknowledgement
    );
  }

  // ============================================================
  // CANCELAMENTO
  // ============================================================

  async handleCancellationIntent(
    userId,
    session,
    text
  ) {
    if (
      this.isCancelIntent(text) &&
      !session.pendingCancel
    ) {
      session.pendingCancel = true;

      return [
        'Sem problema. Posso encerrar a entrevista.',
        '',
        'Só quero confirmar: quer mesmo terminar agora?',
        '',
        'Escreva SIM para terminar ou CONTINUAR para voltar à entrevista.',
      ].join('\n');
    }

    if (!session.pendingCancel) {
      return null;
    }

    if (
      /^(sim|s|confirmo|quero|terminar)$/i.test(text)
    ) {
      const companyName =
        session.getCompanyName();

      this.endSession(userId);

      return [
        'Tudo bem, entrevista encerrada.',
        `Obrigado pelo seu interesse na ${companyName}.`,
      ].join('\n\n');
    }

    session.pendingCancel = false;

    return [
      'Vamos continuar.',
      '',
      session.lastQuestion ||
        'Conte-me um pouco mais sobre o seu percurso.',
    ].join('\n');
  }

  // ============================================================
  // ANÁLISE DO TURNO
  // ============================================================

  async analyzeTurn(
    session,
    answer
  ) {
    const stage =
      this.getStageForSession(session);

    const agent =
      this.getStageAgent(stage);

    const context =
      this.buildInterviewContext(session);

    const tone =
      this.getTone(session);

    const prompt = `
Analisa a resposta de um candidato a uma vaga de emprego.

CONTEXTO DA VAGA
Título: ${context.jobTitle}
Empresa: ${context.companyName}

Descrição:
${context.jobDescription || 'Não especificada.'}

Requisitos:
${context.jobRequirementsText}

CV:
${context.candidateCv || 'Nenhum CV anexado.'}

ETAPA ATUAL
${stage?.name || 'Entrevista'}

Objectivo:
${stage?.objective || 'Recolher evidências sobre as competências exigidas.'}

CANDIDATO
Nome: ${context.candidateName}
Nível de linguagem: ${context.languageLevel}

${tone}

MEMÓRIA
Tópicos cobertos:
${context.coveredTopics}

Evidências comprovadas:
${context.verifiedClaims}

Lacunas:
${context.identifiedGaps}

PALAVRAS FREQUENTES:
${context.overusedWords || 'Nenhuma'}

PERGUNTAS RECENTES:
${context.askedQuestions}

HISTÓRICO:
${context.history || '(Início da conversa)'}

ÚLTIMA PERGUNTA:
"${context.lastQuestion || '(Nenhuma)'}"

RESPOSTA DO CANDIDATO:
"${answer}"

INSTRUÇÕES
1. Avalia a resposta contra os requisitos da vaga.
2. Compara a resposta com o CV quando houver CV.
3. Detecta contradições ou informações novas.
4. Não dês notas altas a respostas vagas, genéricas ou puramente teóricas.
5. Prioriza evidências práticas: acções, responsabilidade, contexto, resultados e impacto.
6. Gera uma próxima pergunta natural baseada no CV, vaga ou lacunas.
7. Evita repetir perguntas já feitas.
8. A reacção deve ser específica e curta.
9. Não inventes factos sobre o candidato.

RESPONDE ESTRITAMENTE EM JSON:

{
  "acknowledgement": "",
  "score": 0,
  "clarity": 0,
  "relevance": 0,
  "depth": 0,
  "off_topic": false,
  "generic": false,
  "emotion": "neutro",
  "topic": "",
  "evidence": "",
  "gap": "",
  "next_question": null,
  "should_transition": false,
  "interview_complete": false,
  "feedback": ""
}
`;

    try {
      const raw =
        await this.generateAI(
          prompt,
          agent.systemPrompt
        );

      const parsed =
        this.parseJsonObject(raw);

      if (!parsed) {
        throw new Error(
          'Resposta da IA sem JSON válido.'
        );
      }

      return this.normalizeTurn(parsed);
    } catch (error) {
      console.error(
        '[INTERVIEW] Falha na análise do turno:',
        error.message
      );

      return this.createTurnFallback(
        session
      );
    }
  }

  normalizeTurn(turn = {}) {
    const emotion =
      clean(turn.emotion).toLowerCase();

    return {
      acknowledgement:
        this.cleanAcknowledgement(
          turn.acknowledgement
        ),

      score: clamp(
        turn.score,
        0,
        10
      ),

      clarity: clamp(
        turn.clarity,
        0,
        4
      ),

      relevance: clamp(
        turn.relevance,
        0,
        3
      ),

      depth: clamp(
        turn.depth,
        0,
        3
      ),

      off_topic:
        turn.off_topic === true,

      generic:
        turn.generic === true,

      emotion: [
        'confiante',
        'neutro',
        'inseguro',
        'entusiasmado',
        'frustrado',
      ].includes(emotion)
        ? emotion
        : 'neutro',

      topic: clean(turn.topic),

      evidence: clean(
        turn.evidence
      ),

      gap: clean(
        turn.gap
      ),

      next_question:
        turn.next_question
          ? clean(turn.next_question)
          : null,

      should_transition:
        turn.should_transition === true,

      interview_complete:
        turn.interview_complete === true,

      feedback: clean(
        turn.feedback
      ),
    };
  }

  createTurnFallback(session) {
    return {
      acknowledgement:
        this.varied(
          session,
          FALLBACK_ACKNOWLEDGEMENTS
        ),

      score: 5,
      clarity: 2,
      relevance: 2,
      depth: 1,

      off_topic: false,
      generic: false,

      emotion: 'neutro',

      topic: '',
      evidence: '',
      gap: '',
      next_question: null,

      should_transition: true,
      interview_complete: false,

      feedback:
        'Avaliação automática indisponível.',
    };
  }

  // ============================================================
  // MEMÓRIA
  // ============================================================

  recordTurn(
    session,
    answer,
    turn
  ) {
    const score =
      clamp(turn.score, 0, 10);

    const effectiveScore =
      turn.generic
        ? Math.max(0, score - 0.5)
        : score;

    if (
      !Array.isArray(session.scores)
    ) {
      session.scores = [];
    }

    session.scores.push({
      score: effectiveScore,

      clarity: clamp(
        turn.clarity,
        0,
        4
      ),

      relevance: clamp(
        turn.relevance,
        0,
        3
      ),

      depth: clamp(
        turn.depth,
        0,
        3
      ),

      feedback:
        turn.feedback || '',

      question:
        session.lastQuestion || '',

      answer,

      emotion:
        turn.emotion || 'neutro',

      artificial:
        turn.generic === true,

      topic:
        turn.topic || '',

      evidence:
        turn.evidence || '',

      gap:
        turn.gap || '',
    });

    this.addUniqueMemory(
      session,
      'topicsCovered',
      turn.topic
    );

    this.addUniqueMemory(
      session,
      'verifiedClaims',
      turn.evidence,
      MAX_RECORDED_EVIDENCE
    );

    this.addUniqueMemory(
      session,
      'identifiedGaps',
      turn.gap,
      MAX_RECORDED_GAPS
    );

    this.addConversationMessage(
      session,
      'assistant',
      session.lastQuestion
    );

    this.addConversationMessage(
      session,
      'user',
      answer
    );
  }

  addUniqueMemory(
    session,
    field,
    value,
    maxItems = null
  ) {
    const item = clean(value);

    if (!item) return;

    if (!Array.isArray(session[field])) {
      session[field] = [];
    }

    if (!session[field].includes(item)) {
      session[field].push(item);
    }

    if (
      maxItems &&
      session[field].length > maxItems
    ) {
      session[field] =
        session[field].slice(-maxItems);
    }
  }

  addConversationMessage(
    session,
    role,
    content
  ) {
    const value = clean(content);

    if (!value) return;

    if (
      !Array.isArray(
        session.conversationHistory
      )
    ) {
      session.conversationHistory = [];
    }

    session.conversationHistory.push({
      role,
      content: value,
      timestamp:
        new Date().toISOString(),
    });
  }

  // ============================================================
  // PRIMEIRA PERGUNTA
  // ============================================================

  async askFirstQuestion(session) {
    const stage =
      this.getStageForSession(session);

    const agent =
      this.getStageAgent(stage);

    const tone =
      this.getTone(session);

    const context =
      this.buildInterviewContext(session);

    const prompt = `
Vais iniciar uma entrevista profissional por WhatsApp.

CANDIDATO
Nome: ${context.candidateName}

CV:
${context.candidateCv || 'Sem CV anexado.'}

VAGA
Título: ${context.jobTitle}
Empresa: ${context.companyName}

Descrição:
${context.jobDescription || 'Geral'}

Requisitos:
${context.jobRequirementsText}

${tone}

Cria a primeira pergunta.

Regras:
- Liga directamente algo do CV aos requisitos da vaga.
- Procura uma experiência prática.
- Prioriza acções e resultados.
- Mantém um tom profissional e acolhedor.
- Faz apenas uma pergunta.
- Máximo de duas frases.
- Sem emojis.
- Não comeces com "Agora vamos falar de".
- Responde APENAS com a pergunta.
`;

    try {
      const response =
        await this.generateAI(
          prompt,
          agent.systemPrompt
        );

      const question =
        clean(response);

      if (question) {
        return this.dedupeQuestion(
          session,
          question
        );
      }
    } catch (error) {
      console.error(
        '[INTERVIEW] Falha na primeira pergunta:',
        error.message
      );
    }

    return this.fallbackQuestion(stage);
  }

  fallbackQuestion(stage) {
    return clean(
      stage?.openingHint ||
        'Conte-me um pouco sobre o seu percurso profissional e a experiência mais relevante para esta vaga.'
    );
  }

  // ============================================================
  // TRANSIÇÃO DE ETAPA
  // ============================================================

  async advanceFocus(
    session,
    acknowledgement = ''
  ) {
    const currentIndex =
      Math.max(
        0,
        Number(session.stageIndex || 0)
      );

    if (
      currentIndex >=
      this.stages.length - 1
    ) {
      return this.concludeInterview(
        session
      );
    }

    const previousStage =
      this.stages[currentIndex];

    const nextIndex =
      currentIndex + 1;

    session.stageIndex = nextIndex;
    session.stage =
      this.stages[nextIndex]?.id ||
      DEFAULT_STAGE_ID;

    session.questionCounter = 0;
    session.offTopicAttempts = 0;

    const nextStage =
      this.getStageForSession(session);

    const agent =
      this.getStageAgent(nextStage);

    const context =
      this.buildInterviewContext(session);

    const prompt = `
Continua a entrevista profissional por WhatsApp.

CANDIDATO:
${context.candidateName}

VAGA:
${context.jobTitle}

REQUISITOS:
${context.jobRequirementsText}

CV:
${context.candidateCv || 'Não disponível.'}

EVIDÊNCIAS JÁ DEMONSTRADAS:
${context.verifiedClaims}

TEMA ANTERIOR:
${previousStage?.objective || ''}

NOVO TEMA:
${nextStage?.objective || ''}

ÁREAS DE FOCO:
${this.formatList(nextStage?.focus)}

Cria UMA pergunta natural para o novo tema.

Regras:
- Usa o CV ou evidências já obtidas quando possível.
- Faz uma transição natural.
- Não uses "Agora vamos falar de...".
- Não repitas perguntas já feitas.
- Máximo de duas frases.
- Sem emojis.
- Responde APENAS com a pergunta.
`;

    let question = '';

    try {
      question = clean(
        await this.generateAI(
          prompt,
          agent.systemPrompt
        )
      );
    } catch (error) {
      console.error(
        '[INTERVIEW] Falha na transição:',
        error.message
      );
    }

    if (!question) {
      question =
        this.fallbackQuestion(
          nextStage
        );
    }

    question =
      this.dedupeQuestion(
        session,
        question
      );

    this.rememberQuestion(
      session,
      question
    );

    let response =
      this.compose(
        session,
        acknowledgement,
        question
      );

    if (!session.qaOffered) {
      session.qaOffered = true;

      response +=
        '\n\nSe quiser saber alguma coisa sobre a empresa ou sobre a vaga, pode perguntar a qualquer momento.';
    }

    return response;
  }

  // ============================================================
  // Q&A
  // ============================================================

  isCandidateQuestion(text) {
    const value =
      clean(text).toLowerCase();

    if (!value) {
      return false;
    }

    const directQuestionPattern =
      /(qual [ée] o sal[áa]rio|como funciona|onde fica|quais [sãa]o os benef[íi]cios|qual [ée] o hor[áa]rio|qual [ée] o regime|voc[êe]s pagam|quanto [ée] o sal[áa]rio|quando come[çc]a)/i;

    const interrogativePattern =
      /^(como|quanto|qual|quais|onde|quando|o que|porque|por que|posso|gostaria de saber|voc[êe]s|tem)\b/i;

    return (
      directQuestionPattern.test(value) ||
      (
        /[?؟]$/.test(value) &&
        interrogativePattern.test(value)
      )
    );
  }

  async answerCandidate(
    session,
    question
  ) {
    return this.generateBatchAnswers(
      [question],
      session
    );
  }

  async generateBatchAnswers(
    questions,
    session
  ) {
    const validQuestions =
      asArray(questions)
        .map(clean)
        .filter(Boolean);

    if (!validQuestions.length) {
      return FALLBACK_ANSWER;
    }

    const questionsText =
      validQuestions
        .map(
          (question, index) =>
            `${index + 1}. ${question}`
        )
        .join('\n');

    const tone =
      this.getTone(session);

    const company =
      session.company || COMPANY;

    const jobVacancy =
      session.jobVacancy ||
      JOB_VACANCY;

    const prompt = `
Responde às perguntas do candidato sobre esta oportunidade de trabalho.

PERGUNTAS:
${questionsText}

DADOS DA EMPRESA:
${JSON.stringify(company)}

DADOS DA VAGA:
${JSON.stringify(jobVacancy)}

${tone}

REGRAS:
- Usa apenas os dados fornecidos.
- Não inventes salário, benefícios, localização, horário ou condições.
- Quando a informação não existir, informa que a equipa de RH precisa confirmar.
- Sê breve e transparente.
- Não uses emojis.
`;

    try {
      const response =
        await this.generateAI(
          prompt,
          AGENTS.recruiter.systemPrompt
        );

      return clean(response) ||
        FALLBACK_ANSWER;
    } catch (error) {
      console.error(
        '[INTERVIEW] Erro no Q&A:',
        error.message
      );

      return FALLBACK_ANSWER;
    }
  }

  // ============================================================
  // REPETIÇÃO / LINGUAGEM
  // ============================================================

  openerKey(text) {
    return extractWords(text)
      .slice(0, 3)
      .join(' ');
  }

  registerLanguage(
    session,
    text
  ) {
    if (!session || !text) return;

    if (!session.usedWords) {
      session.usedWords = {};
    }

    for (const word of extractWords(text)) {
      session.usedWords[word] =
        Number(session.usedWords[word] || 0) +
        1;
    }

    const key =
      this.openerKey(text);

    if (!key) return;

    if (!Array.isArray(session.usedOpeners)) {
      session.usedOpeners = [];
    }

    if (
      !session.usedOpeners.includes(key)
    ) {
      session.usedOpeners.push(key);
    }

    if (
      session.usedOpeners.length >
      MAX_USED_OPENERS
    ) {
      session.usedOpeners =
        session.usedOpeners.slice(
          -MAX_USED_OPENERS
        );
    }
  }

  getOverusedWords(session) {
    return Object.entries(
      session?.usedWords || {}
    )
      .filter(
        ([, count]) =>
          count >= 2
      )
      .sort(
        (a, b) => b[1] - a[1]
      )
      .slice(0, 15)
      .map(([word]) => word)
      .join(', ');
  }

  cleanAcknowledgement(
    acknowledgement
  ) {
    let value =
      clean(acknowledgement);

    if (!value) return '';

    let iterations = 0;

    while (
      BANNED_RE.test(value) &&
      iterations < 3
    ) {
      value = clean(
        value.replace(
          BANNED_RE,
          ''
        )
      );

      iterations++;
    }

    return value;
  }

  cleanAck(
    session,
    acknowledgement
  ) {
    const value =
      this.cleanAcknowledgement(
        acknowledgement
      );

    if (!value) {
      return '';
    }

    const key =
      this.openerKey(value);

    if (
      key &&
      asArray(
        session.usedOpeners
      ).includes(key)
    ) {
      return '';
    }

    const repeatedWord =
      extractWords(value).some(
        (word) =>
          Number(
            session.usedWords?.[word] || 0
          ) >=
          OVERUSED_WORD_THRESHOLD
      );

    if (repeatedWord) {
      return '';
    }

    return (
      value.charAt(0).toUpperCase() +
      value.slice(1)
    );
  }

  varied(
    session,
    options
  ) {
    const validOptions =
      asArray(options)
        .map(clean)
        .filter(Boolean);

    if (!validOptions.length) {
      return '';
    }

    const usedOpeners =
      asArray(
        session.usedOpeners
      );

    const fresh =
      validOptions.filter(
        (option) =>
          !usedOpeners.includes(
            this.openerKey(option)
          )
      );

    const selected = pick(
      fresh.length
        ? fresh
        : validOptions
    );

    this.registerLanguage(
      session,
      selected
    );

    return selected;
  }

  compose(
    session,
    acknowledgement,
    question
  ) {
    const cleanQuestion =
      clean(question);

    if (!cleanQuestion) {
      return '';
    }

    let reaction =
      this.cleanAck(
        session,
        acknowledgement
      );

    if (
      reaction &&
      Number(session.ackStreak || 0) >= 1
    ) {
      reaction = '';
    }

    this.registerLanguage(
      session,
      cleanQuestion
    );

    if (!reaction) {
      session.ackStreak = 0;
      return cleanQuestion;
    }

    session.ackStreak =
      Number(session.ackStreak || 0) + 1;

    this.registerLanguage(
      session,
      reaction
    );

    return `${reaction}\n\n${cleanQuestion}`;
  }

  // ============================================================
  // PERGUNTAS
  // ============================================================

  rememberQuestion(
    session,
    question
  ) {
    const value = clean(question);

    if (!value) return;

    if (
      !(session.askedQuestions instanceof Set)
    ) {
      session.askedQuestions =
        new Set(
          asArray(
            session.askedQuestions
          )
        );
    }

    session.lastQuestion = value;
    session.askedQuestions.add(value);
  }

  dedupeQuestion(
    session,
    question
  ) {
    const candidate =
      clean(question);

    if (!candidate) {
      return this.fallbackQuestion(
        this.getStageForSession(session)
      );
    }

    const asked =
      Array.from(
        session.askedQuestions || []
      ).slice(
        -MAX_RECENT_QUESTIONS
      );

    if (
      !this.isQuestionTooSimilar(
        candidate,
        asked
      )
    ) {
      return candidate;
    }

    return this.getAlternativeQuestion(
      session
    );
  }

  getAlternativeQuestion(
    session
  ) {
    const asked =
      Array.from(
        session.askedQuestions || []
      );

    const fresh =
      FALLBACK_ALTERNATIVE_QUESTIONS.filter(
        (question) =>
          !asked.some(
            (oldQuestion) =>
              this.isQuestionTooSimilar(
                question,
                [oldQuestion]
              )
          )
      );

    return (
      pick(
        fresh.length
          ? fresh
          : FALLBACK_ALTERNATIVE_QUESTIONS
      ) ||
      this.fallbackQuestion(
        this.getStageForSession(
          session
        )
      )
    );
  }

  isQuestionTooSimilar(
    question,
    askedQuestions
  ) {
    const currentWords =
      new Set(
        extractWords(question)
      );

    if (!currentWords.size) {
      return false;
    }

    return asArray(
      askedQuestions
    ).some((oldQuestion) => {
      const oldWords =
        new Set(
          extractWords(oldQuestion)
        );

      if (!oldWords.size) {
        return false;
      }

      let intersection = 0;

      for (const word of currentWords) {
        if (oldWords.has(word)) {
          intersection++;
        }
      }

      const similarity =
        intersection /
        Math.max(
          currentWords.size,
          oldWords.size
        );

      return similarity >= 0.7;
    });
  }

  rephrase(session) {
    const question =
      session.lastQuestion ||
      'Conte-me um pouco mais sobre a sua experiência.';

    const prefix =
      this.varied(
        session,
        FALLBACK_REPHRASES
      );

    return prefix
      ? `${prefix} ${question}`
      : question;
  }

  resumeLine(session) {
    const question =
      session.lastQuestion ||
      'Conte-me um pouco mais sobre o seu percurso.';

    const prefix =
      this.varied(
        session,
        FALLBACK_RESUME_LINES
      );

    return prefix
      ? `${prefix} ${question}`
      : question;
  }

  // ============================================================
  // CONCLUSÃO
  // ============================================================

  async concludeInterview(
    session
  ) {
    const averageScore =
      this.calculateFinalScore(
        session
      );

    const result =
      this.determineResult(
        averageScore
      );

    const summary =
      await this.generatePersonalizedSummary(
        session
      );

    const lines = [
      `Chegámos ao fim, ${this.firstName(session)}. Obrigado pelo tempo e pela conversa.`,
      '',
      summary,
    ];

    const genericCount =
      asArray(
        session.scores
      ).filter(
        (score) =>
          score.artificial
      ).length;

    if (genericCount >= 2) {
      lines.push(
        '',
        'Exemplos concretos das experiências que já teve ajudam a demonstrar melhor o seu perfil.'
      );
    }

    const finalMessage =
      await this.processInterviewOutcome(
        session,
        averageScore,
        result,
        lines
      );

    console.log(
      '[INTERVIEW] Resultado Final:',
      {
        candidato:
          session.candidateName,

        esperado:
          session.expectedCandidateName,

        identidadeVerificada:
          session.isIdentityVerified,

        media:
          Number(
            averageScore.toFixed(1)
          ),

        classificacao:
          result,

        respostasAvaliadas:
          asArray(
            session.scores
          ).length,

        languageLevel:
          session.languageLevel,
      }
    );

    this.endSession(
      session.userId
    );

    return finalMessage;
  }

  async processInterviewOutcome(
    session,
    averageScore,
    result,
    lines
  ) {
    const passingScore =
      Number(
        INTERVIEW_CONFIG.minScoreToPass
      ) || 6;

    const passed =
      averageScore >= passingScore;

    await this.sendInterviewResult(
      session,
      averageScore,
      result,
      passed
    );

    if (passed) {
      lines.push(
        '',
        'Boas notícias: o seu perfil segue para a próxima fase.'
      );

      try {
        const email =
          this.isValidEmail(
            session.candidateEmail
          )
            ? clean(
                session.candidateEmail
              )
            : null;

        if (email) {
          const meetingLink =
            await this.calendarService
              .scheduleInterview(
                email,
                session.candidateName
              );

          if (meetingLink) {
            lines.push(
              '',
              `Os detalhes foram enviados para ${email}.`,
              `Link da reunião: ${meetingLink}`
            );
          } else {
            lines.push(
              '',
              `Os detalhes foram enviados para ${email}.`
            );
          }
        } else {
          lines.push(
            '',
            'A nossa equipa de RH entrará em contacto para combinar os próximos passos.'
          );
        }
      } catch (error) {
        console.error(
          '[INTERVIEW] Falha ao agendar entrevista:',
          error.message
        );

        lines.push(
          '',
          'A nossa equipa de RH entrará em contacto para combinar os próximos passos.'
        );
      }
    } else {
      lines.push(
        '',
        'Neste momento, o perfil não corresponde totalmente aos requisitos desta vaga. Ficamos com a candidatura registada para futuras oportunidades.'
      );
    }

    lines.push(
      '',
      'Foi um gosto conversar consigo. Muita sorte no seu percurso.'
    );

    return lines.join('\n');
  }

  async sendInterviewResult(
    session,
    averageScore,
    result,
    passed
  ) {
    try {
      await this.yaneIntegration.sendInterviewResult(
        {
          phone: session.userId,

          candidateName:
            session.candidateName,

          expectedCandidateName:
            session.expectedCandidateName,

          isIdentityVerified:
            session.isIdentityVerified,

          candidateEmail:
            session.candidateEmail ||
            null,

          candidateCv:
            session.candidateCv ||
            null,

          jobTitle:
            session.getJobTitle(),

          jobRequirements:
            asArray(
              session.jobRequirements
            ),

          score:
            Number(
              averageScore.toFixed(2)
            ),

          feedback:
            result,

          recommendation:
            passed
              ? 'approve'
              : 'hold',

          verifiedClaims:
            asArray(
              session.verifiedClaims
            ),

          identifiedGaps:
            asArray(
              session.identifiedGaps
            ),

          transcript:
            asArray(
              session.conversationHistory
            ),

          scoresBreakdown:
            asArray(
              session.scores
            ),

          interviewId:
            session.interviewId ||
            null,

          holdTransactionId:
            session.holdTransactionId ||
            null,
        }
      );
    } catch (error) {
      console.error(
        '[YANE] Erro ao enviar resultado ao backend:',
        error.message
      );
    }
  }

  async generatePersonalizedSummary(
    session
  ) {
    const history =
      asArray(
        session.scores
      )
        .map((score) =>
          [
            `Pergunta: ${score.question}`,
            `Resposta: ${score.answer}`,
            score.evidence
              ? `Evidência: ${score.evidence}`
              : '',
          ]
            .filter(Boolean)
            .join('\n')
        )
        .join('\n\n');

    if (!history) {
      return 'Obrigado por partilhar o seu percurso connosco.';
    }

    const prompt = `
Escreve um resumo personalizado da entrevista de 2 a 3 frases para o candidato.

BASEIA-TE APENAS NAS EVIDÊNCIAS:
${history}

Não uses:
- notas;
- scores;
- pontuações;
- emojis;
- elogios genéricos vazios.

Não inventes características ou experiências.
`;

    try {
      const summary =
        await this.generateAI(
          prompt,
          AGENTS.recruiter.systemPrompt
        );

      return (
        clean(summary) ||
        'Obrigado por partilhar a sua experiência e o seu percurso connosco.'
      );
    } catch (error) {
      console.error(
        '[INTERVIEW] Erro no resumo:',
        error.message
      );

      return 'Obrigado por partilhar a sua experiência e o seu percurso connosco.';
    }
  }

  // ============================================================
  // IA
  // ============================================================

  async generateAI(
    prompt,
    systemPrompt
  ) {
    return this.aiService.generateResponse(
      prompt,
      systemPrompt
    );
  }

  // ============================================================
  // JSON SEGURO
  // ============================================================

  parseJsonObject(response) {
    const text =
      clean(response);

    if (!text) {
      return null;
    }

    const candidates = [
      text,
      text
        .replace(
          /^```json\s*/i,
          ''
        )
        .replace(
          /^```\s*/i,
          ''
        )
        .replace(
          /\s*```$/i,
          ''
        )
        .trim(),
    ];

    for (const candidate of candidates) {
      try {
        const parsed =
          JSON.parse(candidate);

        if (
          parsed &&
          typeof parsed === 'object' &&
          !Array.isArray(parsed)
        ) {
          return parsed;
        }
      } catch (_) {}
    }

    const objectText =
      this.extractBalancedJson(
        text
      );

    if (!objectText) {
      return null;
    }

    try {
      const parsed =
        JSON.parse(objectText);

      return parsed &&
        typeof parsed === 'object' &&
        !Array.isArray(parsed)
        ? parsed
        : null;
    } catch (_) {
      return null;
    }
  }

  extractBalancedJson(text) {
    const start =
      text.indexOf('{');

    if (start === -1) {
      return null;
    }

    let depth = 0;
    let inString = false;
    let escaped = false;

    for (
      let index = start;
      index < text.length;
      index++
    ) {
      const char =
        text[index];

      if (inString) {
        if (escaped) {
          escaped = false;
          continue;
        }

        if (char === '\\') {
          escaped = true;
          continue;
        }

        if (char === '"') {
          inString = false;
        }

        continue;
      }

      if (char === '"') {
        inString = true;
        continue;
      }

      if (char === '{') {
        depth++;
        continue;
      }

      if (char === '}') {
        depth--;

        if (depth === 0) {
          return text.slice(
            start,
            index + 1
          );
        }
      }
    }

    return null;
  }

  // ============================================================
  // CONTEXTO
  // ============================================================

  getStageForSession(session) {
    const index =
      Math.max(
        0,
        Number(
          session?.stageIndex || 0
        )
      );

    if (!this.stages.length) {
      return null;
    }

    return (
      this.stages[index] ||
      this.stages[
        this.stages.length - 1
      ] ||
      null
    );
  }

  getStageAgent(stage) {
    return (
      AGENTS[stage?.agent] ||
      AGENTS.recruiter
    );
  }

  getTone(session) {
    return (
      SIMPLE_TONE[
        session?.languageLevel
      ] ||
      SIMPLE_TONE.simple
    );
  }

  buildInterviewContext(session) {
    const stage =
      this.getStageForSession(
        session
      );

    const history =
      this.getRecentHistoryText(
        session,
        DEFAULT_HISTORY_LIMIT
      );

    const coveredTopics =
      uniqueStrings(
        session?.topicsCovered
      );

    const verifiedClaims =
      uniqueStrings(
        session?.verifiedClaims
      );

    const identifiedGaps =
      uniqueStrings(
        session?.identifiedGaps
      );

    const askedQuestions =
      Array.from(
        session?.askedQuestions || []
      )
        .slice(
          -MAX_RECENT_QUESTIONS
        )
        .join(' | ');

    const jobRequirements =
      asArray(
        session?.jobRequirements
      );

    return {
      stage,

      companyName:
        session?.getCompanyName?.() ||
        clean(
          session?.company?.name
        ) ||
        'Empresa',

      jobTitle:
        session?.getJobTitle?.() ||
        clean(
          session?.jobTitle
        ) ||
        'Vaga de Emprego',

      jobDescription:
        clean(
          session?.jobDescription
        ),

      jobRequirements,

      jobRequirementsText:
        this.formatList(
          jobRequirements
        ),

      candidateName:
        clean(
          session?.candidateName ||
            session?.expectedCandidateName
        ) || 'Candidato',

      expectedCandidateName:
        clean(
          session?.expectedCandidateName
        ),

      candidateCv:
        session?.candidateCv ||
        'Sem CV cadastrado',

      languageLevel:
        session?.languageLevel ||
        'simple',

      lastQuestion:
        clean(
          session?.lastQuestion
        ),

      coveredTopics:
        coveredTopics.join('; ') ||
        'Nenhum ainda',

      verifiedClaims:
        verifiedClaims.join('; ') ||
        'Nenhuma até ao momento',

      identifiedGaps:
        identifiedGaps.join('; ') ||
        'Nenhuma lacuna crítica',

      askedQuestions:
        askedQuestions ||
        'Nenhuma',

      history,

      overusedWords:
        this.getOverusedWords(
          session
        ),
    };
  }

  formatList(items) {
    const validItems =
      asArray(items)
        .map((item) => {
          if (
            item &&
            typeof item === 'object'
          ) {
            return clean(
              item.name ||
              item.title ||
              item.description ||
              JSON.stringify(item)
            );
          }

          return clean(item);
        })
        .filter(Boolean);

    if (!validItems.length) {
      return 'Não especificados.';
    }

    return validItems
      .map(
        (item) => `- ${item}`
      )
      .join('\n');
  }

  getRecentHistoryText(
    session,
    limit = DEFAULT_HISTORY_LIMIT
  ) {
    return asArray(
      session?.conversationHistory
    )
      .slice(-limit)
      .map((message) => {
        const speaker =
          message.role ===
          'assistant'
            ? 'Recrutador'
            : 'Candidato';

        return `${speaker}: ${clean(
          message.content
        )}`;
      })
      .filter(Boolean)
      .join('\n');
  }

  // ============================================================
  // UTILITÁRIOS
  // ============================================================

  firstName(session) {
    return (
      clean(
        session?.candidateName ||
          session?.expectedCandidateName
      )
        .split(/\s+/)[0] ||
      'candidato'
    );
  }

  isCancelIntent(text) {
    const value =
      clean(text).toLowerCase();

    if (value.length > 80) {
      return false;
    }

    return (
      /quero (cancelar|sair|desistir|parar)/.test(
        value
      ) ||
      /^(cancelar|desistir|sair|encerrar|abortar)$/.test(
        value
      ) ||
      /não quero continuar/.test(
        value
      ) ||
      /^desisto$/.test(value)
    );
  }

  isTooShort(text) {
    const value =
      clean(text);

    const tokens =
      value
        .split(/\s+/)
        .filter(Boolean);

    return (
      tokens.length <= 3 &&
      !/[?؟]$/.test(value)
    );
  }

  normalizeName(text) {
    let value =
      clean(text)
        .replace(
          /^(o\s+meu\s+nome\s+[ée]|me\s+chamo|chamo-me|sou\s+o|sou\s+a|sou)\s+/i,
          ''
        )
        .replace(
          /[^\p{L}\s'-]/gu,
          ''
        )
        .trim();

    if (
      value.length < 2 ||
      value.length > 80
    ) {
      return null;
    }

    const parts =
      value
        .split(/\s+/)
        .filter(Boolean);

    if (!parts.length) {
      return null;
    }

    return parts
      .map((word) => {
        const normalized =
          word.toLowerCase();

        return (
          normalized.charAt(0).toUpperCase() +
          normalized.slice(1)
        );
      })
      .join(' ');
  }

  calculateFinalScore(
    session
  ) {
    const scores =
      asArray(
        session?.scores
      );

    if (!scores.length) {
      return 0;
    }

    const validScores =
      scores.map((score) =>
        clamp(
          score?.score,
          0,
          10
        )
      );

    const total =
      validScores.reduce(
        (sum, score) =>
          sum + score,
        0
      );

    return (
      total /
      validScores.length
    );
  }

  determineResult(
    averageScore
  ) {
    if (averageScore >= 8) {
      return 'Excelente';
    }

    if (averageScore >= 6) {
      return 'Bom';
    }

    return 'A desenvolver';
  }

  isValidEmail(email) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(
      clean(email)
    );
  }

  updateLanguageLevel(
    session,
    text
  ) {
    session?.updateLanguageLevel?.(
      text
    );
  }

  clearQuestionCollection(
    userId
  ) {
    this.questionCollector.delete(
      userId
    );

    const timer =
      this.collectorTimers.get(
        userId
      );

    if (timer) {
      clearTimeout(timer);
      this.collectorTimers.delete(
        userId
      );
    }
  }
}

module.exports = InterviewService;
