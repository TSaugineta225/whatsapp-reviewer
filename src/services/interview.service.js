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
const DEFAULT_HISTORY_LIMIT = 6;
const DEFAULT_QUESTIONS_PER_STAGE = 3;

const DEFAULT_TYPING_MS_PER_CHAR = 22;
const DEFAULT_TYPING_MAX_MS = 2600;

const MAX_USED_OPENERS = 40;
const MAX_RECENT_QUESTIONS = 8;
const OVERUSED_WORD_THRESHOLD = 3;

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
    BANNED_OPENERS
      .map(escapeRegex)
      .join('|') +
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

// ============================================================
// HELPERS PUROS
// ============================================================

function escapeRegex(value) {
  return String(value).replace(
    /[.*+?^${}()|[\]\\]/g,
    '\\$&'
  );
}

function pick(items = []) {
  if (!Array.isArray(items) || items.length === 0) {
    return '';
  }

  return items[Math.floor(Math.random() * items.length)];
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
      (word) =>
        word.length > 3 &&
        !STOPWORDS.has(word)
    );
}

function clamp(value, min, max) {
  const number = Number(value);

  if (!Number.isFinite(number)) {
    return min;
  }

  return Math.max(
    min,
    Math.min(max, number)
  );
}

// ============================================================
// TOM
// ============================================================

const SIMPLE_TONE = {
  simple:
    'Usa palavras simples, frases curtas e naturais. Evita jargão desnecessário. Uma pergunta de cada vez.',

  medium:
    'Usa linguagem profissional, clara e acessível. Mantém naturalidade e evita formalidade excessiva.',

  advanced:
    'Usa linguagem profissional mais elaborada, mas continua natural, humana e fácil de acompanhar.',
};

// ============================================================
// SERVICE
// ============================================================

class InterviewService extends BaseService {
  constructor() {
    super();

    this.sessions = new Map();

    // Mantidos para compatibilidade com o fluxo actual.
    this.questionCollector = new Map();
    this.collectorTimers = new Map();

    this.aiService = new AIService();
    this.calendarService = new CalendarService();
    this.yaneIntegration = new YaneIntegrationService();

    this.stages = Array.isArray(STAGES)
      ? STAGES
      : [];

    this.typingMsPerChar =
      Number(
        INTERVIEW_CONFIG.typingMsPerChar
      ) || DEFAULT_TYPING_MS_PER_CHAR;

    this.typingMax =
      Number(
        INTERVIEW_CONFIG.typingMaxMs
      ) || DEFAULT_TYPING_MAX_MS;

    this.questionsPerStage =
      Number(
        INTERVIEW_CONFIG.questionsPerStage
      ) || DEFAULT_QUESTIONS_PER_STAGE;
  }

  // ============================================================
  // SESSÕES
  // ============================================================

  getSession(userId) {
    return this.sessions.get(userId);
  }

  endSession(userId) {
    const session =
      this.sessions.get(userId);

    if (session) {
      try {
        session.cancelTimeout();
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
    if (!session) {
      return;
    }

    const preserved = {
      userId: session.userId,
      candidateName: session.candidateName,
      candidateEmail: session.candidateEmail,
      jobTitle: session.jobTitle,
      company: session.company,
      jobVacancy: session.jobVacancy,
      interviewId: session.interviewId,
      holdTransactionId:
        session.holdTransactionId,
      recruiterId: session.recruiterId,
      languageLevel:
        session.languageLevel,
    };

    if (
      typeof session.resetState ===
      'function'
    ) {
      session.resetState();

      session.userId =
        preserved.userId;
      session.candidateName =
        preserved.candidateName;
      session.candidateEmail =
        preserved.candidateEmail;
      session.jobTitle =
        preserved.jobTitle;
      session.company =
        preserved.company;
      session.jobVacancy =
        preserved.jobVacancy;
      session.interviewId =
        preserved.interviewId;
      session.holdTransactionId =
        preserved.holdTransactionId;
      session.recruiterId =
        preserved.recruiterId;

      session.languageLevel =
        preserved.languageLevel || 'simple';

      return;
    }

    // Compatibilidade com versões antigas
    // do InterviewSession.
    session.stageIndex = 0;
    session.stage = DEFAULT_STAGE_ID;
    session.currentQuestion = 0;
    session.questionCounter = 0;
    session.lastQuestion = null;

    session.askedQuestions = new Set();
    session.topicsCovered = [];

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

    session.languageLevel =
      preserved.languageLevel || 'simple';

    session.candidateName =
      preserved.candidateName;
    session.candidateEmail =
      preserved.candidateEmail;
    session.jobTitle =
      preserved.jobTitle;
    session.company =
      preserved.company;
    session.jobVacancy =
      preserved.jobVacancy;
    session.interviewId =
      preserved.interviewId;
    session.holdTransactionId =
      preserved.holdTransactionId;
    session.recruiterId =
      preserved.recruiterId;
    session.userId =
      preserved.userId;
  }

  scheduleSessionTimeout(
    userId,
    session
  ) {
    if (!session) {
      return;
    }

    try {
      session.cancelTimeout();
    } catch (_) {}

    session.scheduleTimeout(
      async (expiredSession) => {
        const currentSession =
          this.sessions.get(userId);

        if (
          !currentSession ||
          currentSession !==
            expiredSession
        ) {
          return;
        }

        if (
          !this.isSessionExpired(
            currentSession
          )
        ) {
          return;
        }

        try {
          await this.sendMessage(
            userId,
            this.generateTimeoutMessage(
              currentSession
            )
          );
        } finally {
          this.endSession(userId);
        }

        console.log(
          `[TIMEOUT] Sessão encerrada por inatividade: ${userId}`
        );
      }
    );
  }

  isSessionExpired(session) {
    if (!session) {
      return true;
    }

    if (
      typeof session.isExpired ===
      'function'
    ) {
      return session.isExpired();
    }

    const timeoutMinutes =
      Number(
        INTERVIEW_CONFIG.timeoutMinutes
      ) || 20;

    return (
      Date.now() -
        Number(
          session.lastInteraction || 0
        ) >
      timeoutMinutes * 60 * 1000
    );
  }

  // ============================================================
  // INÍCIO
  // ============================================================

  async startInterview(
    userId,
    jobTitle = null,
    company = null,
    jobVacancy = null
  ) {
    try {
      const session =
        new InterviewSession(userId);

      session.jobTitle =
        jobTitle ||
        JOB_VACANCY.title ||
        null;

      session.company =
        company ||
        COMPANY ||
        null;

      session.jobVacancy =
        jobVacancy ||
        JOB_VACANCY ||
        null;

      this.resetInterviewState(
        session
      );

      this.sessions.set(
        userId,
        session
      );

      session.updateLastInteraction();

      this.scheduleSessionTimeout(
        userId,
        session
      );

      const welcomeMessage =
        this.generateWelcomeMessage(
          session
        );

      const whatsapp =
        global.whatsappService;

      if (
        whatsapp &&
        typeof whatsapp.sendInteractiveMessage ===
          'function'
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
          `[SESSAO] Convite interativo enviado para ${userId}`
        );
      } else {
        await this.sendMessage(
          userId,
          welcomeMessage
        );
      }

      return welcomeMessage;
    } catch (error) {
      throw this.handleError(
        error,
        'Start Interview'
      );
    }
  }

  generateWelcomeMessage(session) {
    const companyName =
      session.getCompanyName();

    const jobTitle =
      session.getJobTitle();

    const industry =
      session.company?.industry ||
      '';

    const roleText = industry
      ? `Estamos a recrutar para a posição de ${jobTitle}, na área de ${industry}.`
      : `Estamos a recrutar para a posição de ${jobTitle}.`;

    return [
      `Olá! Aqui é do processo de selecção da ${companyName}.`,
      roleText,
      '',
      'A ideia é termos uma conversa tranquila sobre o seu percurso, a sua experiência e algumas situações relacionadas com a função.',
      '',
      'Para começarmos, pode dizer-me o seu nome completo?',
    ].join('\n');
  }

  generateTimeoutMessage(session) {
    const name =
      this.firstName(session);

    return [
      `Olá, ${name}.`,
      '',
      'Ficámos algum tempo sem receber resposta e a entrevista foi encerrada por inactividade.',
      '',
      `Pode iniciar novamente quando estiver disponível. Obrigado pelo seu interesse na ${session.getCompanyName()}.`,
    ].join('\n');
  }

  async sendMessage(
    userId,
    text
  ) {
    if (
      !text ||
      !global.whatsappService
    ) {
      return;
    }

    try {
      await global.whatsappService.sendMessage(
        userId,
        text
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

    const bubbles =
      this.splitBubbles(text);

    for (const bubble of bubbles) {
      try {
        if (
          typeof client.sendPresenceUpdate ===
          'function'
        ) {
          await client.sendPresenceUpdate(
            'composing',
            userId
          );
        }
      } catch (_) {
        // Presença é apenas cosmética.
      }

      const typingDelay =
        Math.min(
          bubble.length *
            this.typingMsPerChar,
          this.typingMax
        );

      await this.delay(
        typingDelay
      );

      await client.sendMessage(
        userId,
        bubble
      );

      await this.delay(300);
    }
  }

  splitBubbles(text) {
    const parts =
      String(text || '')
        .split(/\n{2,}/)
        .map((part) =>
          part.trim()
        )
        .filter(Boolean);

    if (!parts.length) {
      return [];
    }

    if (parts.length <= 3) {
      return this.mergeSmallBubbles(
        parts
      );
    }

    const merged =
      this.mergeSmallBubbles(parts);

    if (merged.length <= 3) {
      return merged;
    }

    return [
      merged[0],
      merged
        .slice(1, -1)
        .join('\n\n'),
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
        (
          previous.length < 60 ||
          part.length < 40
        )
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
    return new Promise((resolve) =>
      setTimeout(resolve, ms)
    );
  }

  // ============================================================
  // FLUXO PRINCIPAL
  // ============================================================

  async handleResponse(
    userId,
    message
  ) {
    const session =
      this.getSession(userId);

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

    if (
      this.isSessionExpired(session)
    ) {
      this.endSession(userId);

      return (
        'Ficámos algum tempo sem falar e a sessão foi encerrada. ' +
        'Quando quiser retomar, escreva "OLÁ" e começamos novamente.'
      );
    }

    session.updateLastInteraction();

    session.turnCount =
      (session.turnCount || 0) + 1;

    this.scheduleSessionTimeout(
      userId,
      session
    );

    // ----------------------------------------------------------
    // IDENTIFICAÇÃO
    // ----------------------------------------------------------

    if (!session.candidateName) {
      return this.handleCandidateName(
        session,
        text
      );
    }

    session.updateLanguageLevel(
      text
    );

    // ----------------------------------------------------------
    // PERGUNTA DO CANDIDATO
    // ----------------------------------------------------------

    if (
      this.isCandidateQuestion(text)
    ) {
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
    // RESPOSTA MUITO CURTA
    // ----------------------------------------------------------

    if (this.isTooShort(text)) {
      session.shortAnswerStreak =
        (session.shortAnswerStreak || 0) +
        1;

      if (
        session.shortAnswerStreak <=
        (INTERVIEW_CONFIG.maxShortAnswerNudges || 1)
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
    // ANÁLISE
    // ----------------------------------------------------------

    const turn =
      await this.analyzeTurn(
        session,
        text
      );

    // ----------------------------------------------------------
    // OFF TOPIC
    // ----------------------------------------------------------

    if (turn.off_topic) {
      session.offTopicAttempts =
        (session.offTopicAttempts || 0) +
        1;

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

      return this.rephrase(
        session
      );
    }

    session.offTopicAttempts = 0;

    // ----------------------------------------------------------
    // REGISTO
    // ----------------------------------------------------------

    this.recordTurn(
      session,
      text,
      turn
    );

    // ----------------------------------------------------------
    // CONCLUSÃO
    // ----------------------------------------------------------

    if (turn.interview_complete) {
      return this.concludeInterview(
        session
      );
    }

    session.questionCounter =
      (session.questionCounter || 0) +
      1;

    const stageQuestionLimit =
      Number(
        this.getCurrentStage()
          ?.questionsPerStage
      ) ||
      this.questionsPerStage;

    if (
      turn.should_transition ||
      session.questionCounter >=
        stageQuestionLimit
    ) {
      return this.advanceFocus(
        session,
        turn.acknowledgement
      );
    }

    // ----------------------------------------------------------
    // PRÓXIMA PERGUNTA
    // ----------------------------------------------------------

    if (turn.next_question) {
      const question =
        this.dedupeQuestion(
          session,
          turn.next_question
        );

      session.lastQuestion =
        question;

      session.askedQuestions.add(
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
      /^(sim|s|confirmo|quero|terminar)$/i.test(
        text
      )
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
  // NOME
  // ============================================================

  async handleCandidateName(
    session,
    text
  ) {
    const name =
      this.normalizeName(text);

    if (!name) {
      return 'Não consegui perceber o nome. Pode escrever o seu nome, por favor?';
    }

    session.candidateName =
      name;

    session.updateLanguageLevel(
      text
    );

    const question =
      await this.askFirstQuestion(
        session
      );

    session.lastQuestion =
      question;

    session.askedQuestions.add(
      question
    );

    return [
      `Muito prazer, ${this.firstName(session)}.`,
      '',
      question,
    ].join('\n');
  }

  // ============================================================
  // ANÁLISE PRINCIPAL
  // ============================================================

  async analyzeTurn(
    session,
    answer
  ) {
    const stage =
      this.getCurrentStage();

    const agent =
      this.getStageAgent(stage);

    const context =
      this.buildInterviewContext(
        session
      );

    const tone =
      this.getTone(
        session
      );

    const prompt = `
Analisa a última resposta do candidato durante uma entrevista profissional.

Esta análise é INTERNA.
Nunca reveles ao candidato os critérios, notas ou estrutura interna.

CONTEXTO DA VAGA
Título: ${context.jobTitle}
Empresa: ${context.companyName}

TEMA ACTUAL
${stage?.name || 'Entrevista'}
Objectivo:
${stage?.objective || 'Recolher evidência relevante para a vaga.'}

ÁREAS DE FOCO
${this.formatList(
  stage?.focus
)}

CANDIDATO
Nome: ${context.candidateName}
Nível de linguagem: ${context.languageLevel}

${tone}

TEMAS JÁ COBERTOS
${context.coveredTopics}

PERGUNTAS JÁ FEITAS
${context.askedQuestions}

HISTÓRICO RECENTE
${context.history || '(início da entrevista)'}

ÚLTIMA PERGUNTA
${context.lastQuestion || '(nenhuma)'}

RESPOSTA ACTUAL
"${answer}"

AVALIAÇÃO INTERNA

1. acknowledgement
Uma reacção humana curta, no máximo 15 palavras.
Quando possível, refere um detalhe concreto da resposta.
Não elogies automaticamente.
Pode ser "".

2. score
Nota geral de 0 a 10 sobre a qualidade da evidência apresentada
nesta resposta.

3. clarity
Nota de 0 a 4.

4. relevance
Nota de 0 a 3.

5. depth
Nota de 0 a 3.

Importante:
Avalia conteúdo profissional.
Não penalizes ortografia, sotaque, erros de escrita, mistura de idiomas,
nível socioeconómico, idade, género, aparência ou estilo de comunicação.

6. off_topic
true apenas se a resposta estiver realmente desconectada da pergunta.

7. generic
true se a resposta for vaga, puramente declarativa ou sem evidência suficiente.

8. emotion
Estado aparente na resposta.
Exemplos: confiante, neutro, inseguro, entusiasmado, frustrado.

9. topic
Resume o principal assunto abordado em 2 a 4 palavras.

10. evidence
Descreve em poucas palavras o que a resposta efectivamente demonstrou.
Não inventes competências.

11. gap
Indica a principal informação que ainda falta para avaliar esta parte da vaga.
Pode ser "" quando não existir uma lacuna relevante.

12. next_question
Uma única pergunta natural.

A próxima pergunta deve, por ordem de preferência:
- aprofundar uma evidência concreta apresentada;
- esclarecer uma lacuna relevante;
- verificar uma competência da vaga ainda não suficientemente demonstrada;
- explorar uma consequência ou resultado da experiência mencionada.

Evita repetir perguntas já feitas.
Não inventes experiências.
Máximo de duas frases.

Usa null quando for melhor mudar de foco.

13. should_transition
true quando esta área já produziu evidência suficiente
ou quando outra área da entrevista passou a ser mais importante.

14. interview_complete
true SOMENTE quando todas as áreas essenciais da entrevista
já tiverem sido suficientemente exploradas.

15. feedback
Nota interna curta, até 20 palavras.

REGRAS:

- Não transformes todas as respostas em elogios.
- Não uses linguagem robótica.
- Não trates o CV como prova definitiva.
- Não assumes que uma competência existe só porque apareceu no CV.
- Usa a vaga como referência do que realmente importa.
- Procura comportamento e evidência observável.
- Uma resposta curta não significa necessariamente uma resposta fraca.
- Uma resposta longa não significa necessariamente uma resposta boa.
- Não reveles notas ou critérios.

JSON:

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

      return this.normalizeTurn(
        parsed
      );
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
    const acknowledgement =
      clean(
        turn.acknowledgement
      );

    const nextQuestion =
      turn.next_question
        ? clean(
            turn.next_question
          )
        : null;

    return {
      acknowledgement:
        this.cleanAcknowledgement(
          acknowledgement
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

      emotion:
        clean(turn.emotion).toLowerCase() ||
        'neutro',

      topic:
        clean(turn.topic),

      evidence:
        clean(turn.evidence),

      gap:
        clean(turn.gap),

      next_question:
        nextQuestion || null,

      should_transition:
        turn.should_transition === true,

      interview_complete:
        turn.interview_complete === true,

      feedback:
        clean(turn.feedback),
    };
  }

  createTurnFallback(
    session
  ) {
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
  // REGISTO
  // ============================================================

  recordTurn(
    session,
    answer,
    turn
  ) {
    const score =
      clamp(
        turn.score,
        0,
        10
      );

    const effectiveScore =
      turn.generic
        ? Math.max(
            0,
            score - 0.5
          )
        : score;

    if (
      Array.isArray(session.scores)
    ) {
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
          turn.generic,

        topic:
          turn.topic || '',

        evidence:
          turn.evidence || '',

        gap:
          turn.gap || '',
      });
    }

    if (turn.topic) {
      session.topicsCovered =
        session.topicsCovered || [];

      if (
        !session.topicsCovered.includes(
          turn.topic
        )
      ) {
        session.topicsCovered.push(
          turn.topic
        );
      }
    }

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

  addConversationMessage(
    session,
    role,
    content
  ) {
    if (
      !content ||
      !Array.isArray(
        session.conversationHistory
      )
    ) {
      return;
    }

    session.conversationHistory.push({
      role,
      content: String(content),
    });
  }

  // ============================================================
  // PRIMEIRA PERGUNTA
  // ============================================================

  async askFirstQuestion(
    session
  ) {
    const stage =
      this.getCurrentStage();

    const agent =
      this.getStageAgent(stage);

    const tone =
      this.getTone(
        session
      );

    const jobContext =
      this.getJobContext(
        session
      );

    const prompt = `
Vais iniciar uma entrevista profissional por WhatsApp.

CANDIDATO
Nome: ${session.candidateName}

VAGA
Título: ${jobContext.jobTitle}
Empresa: ${jobContext.companyName}

TEMA INICIAL
${stage?.objective || ''}

${tone}

Cria a primeira pergunta da entrevista.

A pergunta deve:
- ser natural;
- criar espaço para o candidato apresentar o seu percurso;
- ser relevante para a vaga;
- permitir encontrar uma experiência que possa ser aprofundada;
- não presumir experiência formal;
- não repetir informação já disponível no contexto.

Uma abertura equivalente a "fala-me um pouco sobre o teu percurso"
é aceitável quando realmente ainda falta contexto.

Evita perguntas vazias ou excessivamente genéricas.

Máximo de duas frases.
Uma pergunta principal.
Sem emojis.

Responde apenas com a pergunta.
`;

    try {
      const question =
        clean(
          await this.generateAI(
            prompt,
            agent.systemPrompt
          )
        );

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

    return this.fallbackQuestion(
      stage
    );
  }

  fallbackQuestion(stage) {
    const hint =
      stage?.openingHint ||
      'Conte-me um pouco sobre o seu percurso profissional e a experiência mais relevante para esta vaga.';

    return clean(hint);
  }

  // ============================================================
  // TRANSIÇÃO
  // ============================================================

  async advanceFocus(
    session,
    acknowledgement = ''
  ) {
    const currentIndex =
      Number(
        session.stageIndex || 0
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

    session.stageIndex =
      currentIndex + 1;

    session.stage =
      this.stages[
        session.stageIndex
      ]?.id ||
      DEFAULT_STAGE_ID;

    session.questionCounter = 0;
    session.offTopicAttempts = 0;

    const nextStage =
      this.getCurrentStage();

    const agent =
      this.getStageAgent(
        nextStage
      );

    const context =
      this.buildInterviewContext(
        session
      );

    const previousAnswers =
      session.conversationHistory
        .filter(
          (message) =>
            message.role === 'user'
        )
        .slice(-2)
        .map(
          (message) =>
            message.content
        )
        .join(' ');

    const prompt = `
Continua uma entrevista profissional por WhatsApp.

CANDIDATO
${context.candidateName}

VAGA
${context.jobTitle}

ÚLTIMA PARTE DA CONVERSA
${previousAnswers || '(sem resposta disponível)'}

TEMA ANTERIOR
${previousStage?.objective || ''}

NOVO TEMA
${nextStage?.objective || ''}

ÁREAS DE FOCO
${this.formatList(
  nextStage?.focus
)}

Cria UMA pergunta natural que faça a transição sem parecer que começou
um novo questionário.

Sempre que possível, usa algo que o candidato acabou de mencionar como ponte.

Não digas:
- "agora vamos falar de";
- "mudando de assunto";
- "passando para";
- "na próxima etapa";
- "vamos avaliar";
- "agora vou avaliar".

Evita repetir perguntas já feitas.

Máximo de duas frases.
Sem emojis.
Responde apenas com a pergunta.
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

    session.lastQuestion =
      question;

    session.askedQuestions.add(
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
      directQuestionPattern.test(
        value
      ) ||
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
      Array.isArray(questions)
        ? questions.filter(Boolean)
        : [];

    if (!validQuestions.length) {
      return 'Essa informação preciso de confirmar com a equipa antes de lhe responder com segurança.';
    }

    const questionsText =
      validQuestions
        .map(
          (question, index) =>
            `${index + 1}. ${question}`
        )
        .join('\n');

    const tone =
      this.getTone(
        session
      );

    const company =
      session.company ||
      COMPANY;

    const jobVacancy =
      session.jobVacancy ||
      JOB_VACANCY;

    const prompt = `
Responde às perguntas de um candidato durante uma entrevista profissional.

PERGUNTAS
${questionsText}

EMPRESA
${JSON.stringify(company)}

VAGA
${JSON.stringify(jobVacancy)}

${tone}

REGRAS:
- responde directamente;
- sê breve;
- não inventes informação;
- usa apenas os dados fornecidos;
- não inventes salário, benefícios, horários, políticas ou condições;
- quando faltar informação, diz claramente que precisa de confirmação;
- questões de remuneração pessoal devem ser encaminhadas para o RH;
- não reveles critérios internos da entrevista;
- sem emojis;
- mantém tom humano.

Responde apenas ao que foi perguntado.
`;

    try {
      const response =
        await this.generateAI(
          prompt,
          AGENTS.recruiter.systemPrompt
        );

      return (
        clean(response) ||
        'Essa informação preciso de confirmar com a equipa antes de lhe responder com segurança.'
      );
    } catch (error) {
      console.error(
        '[INTERVIEW] Erro no Q&A:',
        error.message
      );

      return 'Essa informação preciso de confirmar com a equipa antes de lhe responder com segurança.';
    }
  }

  // ============================================================
  // OFF TOPIC
  // ============================================================

  async detectOffTopic(
    session,
    message
  ) {
    const lastQuestion =
      session.lastQuestion || '';

    if (!lastQuestion) {
      return false;
    }

    const prompt = `
PERGUNTA
"${lastQuestion}"

RESPOSTA
"${message}"

A resposta está TOTALMENTE fora do assunto da pergunta?

Responde apenas:
SIM
ou
NÃO
`;

    try {
      const answer =
        await this.generateAI(
          prompt,
          AGENTS.recruiter.systemPrompt
        );

      return /^sim\b/i.test(
        clean(answer)
      );
    } catch (error) {
      return false;
    }
  }

  handleOffTopic(
    session
  ) {
    const question =
      session.lastQuestion ||
      'Conte-me um pouco mais sobre a sua experiência.';

    return `${this.varied(
      session,
      FALLBACK_REPHRASES
    )} ${question}`;
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
    if (!session || !text) {
      return;
    }

    session.usedWords =
      session.usedWords || {};

    for (const word of extractWords(
      text
    )) {
      session.usedWords[word] =
        (session.usedWords[word] || 0) +
        1;
    }

    const key =
      this.openerKey(text);

    if (!key) {
      return;
    }

    session.usedOpeners =
      session.usedOpeners || [];

    if (
      !session.usedOpeners.includes(
        key
      )
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

  getOverusedWords(
    session
  ) {
    return Object.entries(
      session.usedWords || {}
    )
      .filter(
        ([, count]) =>
          count >= 2
      )
      .sort(
        (a, b) => b[1] - a[1]
      )
      .slice(0, 15)
      .map(
        ([word]) => word
      )
      .join(', ');
  }

  cleanAcknowledgement(
    acknowledgement
  ) {
    let value =
      clean(acknowledgement);

    if (!value) {
      return '';
    }

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
      (
        session.usedOpeners || []
      ).includes(key)
    ) {
      return '';
    }

    const repeatedWord =
      extractWords(value).some(
        (word) =>
          (
            session.usedWords || {}
          )[word] >=
          OVERUSED_WORD_THRESHOLD
      );

    if (repeatedWord) {
      return '';
    }

    return value.charAt(0).toUpperCase() +
      value.slice(1);
  }

  varied(
    session,
    options
  ) {
    const validOptions =
      Array.isArray(options)
        ? options.filter(Boolean)
        : [];

    if (!validOptions.length) {
      return '';
    }

    const fresh =
      validOptions.filter(
        (option) =>
          !(
            session.usedOpeners || []
          ).includes(
            this.openerKey(
              option
            )
          )
      );

    const selected =
      pick(
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

    // Evita várias validações seguidas,
    // porque começam a parecer uma fórmula.
    if (
      reaction &&
      (session.ackStreak || 0) >= 1
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
      (session.ackStreak || 0) + 1;

    this.registerLanguage(
      session,
      reaction
    );

    return `${reaction}\n\n${cleanQuestion}`;
  }

  // ============================================================
  // DEDUPLICAÇÃO
  // ============================================================

  dedupeQuestion(
    session,
    question
  ) {
    const candidate =
      clean(question);

    if (!candidate) {
      return this.fallbackQuestion(
        this.getCurrentStage()
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
    const alternatives = [
      'Pode dar-me um exemplo concreto disso?',
      'E qual foi exactamente o seu papel nessa situação?',
      'O que fez a seguir?',
      'Qual foi o resultado dessa experiência?',
    ];

    const asked =
      Array.from(
        session.askedQuestions || []
      );

    const fresh =
      alternatives.filter(
        (question) =>
          !asked.some(
            (old) =>
              this.isQuestionTooSimilar(
                question,
                [old]
              )
          )
      );

    return pick(
      fresh.length
        ? fresh
        : alternatives
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

    return askedQuestions.some(
      (oldQuestion) => {
        const oldWords =
          new Set(
            extractWords(
              oldQuestion
            )
          );

        if (!oldWords.size) {
          return false;
        }

        let intersection = 0;

        for (
          const word of currentWords
        ) {
          if (
            oldWords.has(word)
          ) {
            intersection++;
          }
        }

        const similarity =
          intersection /
          Math.max(
            currentWords.size,
            oldWords.size
          );

        return similarity >= 0.70;
      }
    );
  }

  rephrase(session) {
    const question =
      session.lastQuestion ||
      'Conte-me um pouco mais sobre a sua experiência.';

    return `${this.varied(
      session,
      FALLBACK_REPHRASES
    )} ${question}`;
  }

  resumeLine(session) {
    const question =
      session.lastQuestion ||
      'Conte-me um pouco mais sobre o seu percurso.';

    return `${this.varied(
      session,
      FALLBACK_RESUME_LINES
    )} ${question}`;
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
      session.scores.filter(
        (score) =>
          score.artificial
      ).length;

    if (genericCount >= 2) {
      lines.push(
        '',
        'Exemplos concretos das experiências que já teve ajudam a mostrar melhor o seu percurso.'
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
      '[INTERVIEW] Resultado',
      {
        candidato:
          session.candidateName,
        media:
          Number(
            averageScore.toFixed(1)
          ),
        classificacao:
          result,
        respostas:
          session.scores.length,
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
      averageScore >=
      passingScore;

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
            ? session.candidateEmail
            : null;

        if (email) {
          const meetingLink =
            await this.calendarService.scheduleInterview(
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
            'A nossa equipa de RH entra em contacto para combinar os próximos passos.'
          );
        }
      } catch (error) {
        console.error(
          '[INTERVIEW] Falha ao agendar entrevista:',
          error.message
        );

        lines.push(
          '',
          'A nossa equipa de RH entra em contacto para combinar os próximos passos.'
        );
      }
    } else {
      lines.push(
        '',
        'Neste momento, o perfil não corresponde totalmente ao que esta vaga exige. Ainda assim, ficamos com o registo da candidatura para futuras oportunidades.'
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
          candidateEmail:
            session.candidateEmail ||
            null,
          jobTitle:
            session.getJobTitle(),
          score:
            Number(
              averageScore.toFixed(2)
            ),
          feedback: result,
          recommendation:
            passed
              ? 'approve'
              : 'hold',
          transcript:
            session.conversationHistory,
          scoresBreakdown:
            session.scores,
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
        '[YANE] Erro ao enviar resultado:',
        error.message
      );
    }
  }

  async generatePersonalizedSummary(
    session
  ) {
    const history =
      session.scores
        .map(
          (score) =>
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
Escreve um resumo curto da entrevista para o candidato.

O texto deve ter 2 ou 3 frases.

BASEIA-TE APENAS NO QUE FOI REALMENTE DEMONSTRADO
NAS RESPOSTAS.

Destaca:
- experiências concretas;
- competências demonstradas;
- formas de trabalhar observáveis.

Não inventes características.
Não confundas aquilo que o candidato afirmou saber com aquilo que demonstrou.

Não uses:
- notas;
- pontuações;
- critérios;
- emojis;
- clichés;
- elogios genéricos;
- "foi uma excelente entrevista".

ENTREVISTA

${history}
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
  // AI
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
  // JSON
  // ============================================================

  parseJsonObject(
    response
  ) {
    const text =
      clean(response);

    if (!text) {
      return null;
    }

    // JSON directo.
    try {
      const parsed =
        JSON.parse(text);

      if (
        parsed &&
        typeof parsed === 'object' &&
        !Array.isArray(parsed)
      ) {
        return parsed;
      }
    } catch (_) {}

    // Markdown fence.
    const fenced =
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
        .trim();

    try {
      const parsed =
        JSON.parse(fenced);

      if (
        parsed &&
        typeof parsed === 'object' &&
        !Array.isArray(parsed)
      ) {
        return parsed;
      }
    } catch (_) {}

    // JSON embutido.
    const objectText =
      this.extractBalancedJson(
        fenced
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

  extractBalancedJson(
    text
  ) {
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
      const character =
        text[index];

      if (inString) {
        if (escaped) {
          escaped = false;
          continue;
        }

        if (
          character === '\\'
        ) {
          escaped = true;
          continue;
        }

        if (
          character === '"'
        ) {
          inString = false;
        }

        continue;
      }

      if (
        character === '"'
      ) {
        inString = true;
        continue;
      }

      if (
        character === '{'
      ) {
        depth++;
        continue;
      }

      if (
        character === '}'
      ) {
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

  getCurrentStage() {
    const index =
      this.sessions.size >= 0
        ? undefined
        : undefined;

    // O índice é obtido pela sessão nos
    // métodos que chamam esta função.
    return null;
  }

  getStageForSession(
    session
  ) {
    return (
      this.stages[
        Number(
          session?.stageIndex || 0
        )
      ] ||
      this.stages[
        this.stages.length - 1
      ]
    );
  }

  getStageAgent(
    stage
  ) {
    return (
      AGENTS[
        stage?.agent
      ] ||
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

  getJobContext(session) {
    return {
      companyName:
        session.getCompanyName(),
      jobTitle:
        session.getJobTitle(),
    };
  }

  buildInterviewContext(
    session
  ) {
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
      (
        session.topicsCovered ||
        []
      )
        .filter(Boolean)
        .join('; ') ||
      'nenhum ainda';

    const askedQuestions =
      Array.from(
        session.askedQuestions ||
          []
      )
        .slice(-MAX_RECENT_QUESTIONS)
        .join(' | ') ||
      'nenhuma';

    return {
      stage,
      companyName:
        session.getCompanyName(),
      jobTitle:
        session.getJobTitle(),
      candidateName:
        session.candidateName ||
        'Candidato',
      languageLevel:
        session.languageLevel ||
        'simple',
      lastQuestion:
        session.lastQuestion ||
        '',
      coveredTopics,
      askedQuestions,
      history,
      overusedWords:
        this.getOverusedWords(
          session
        ),
    };
  }

  // Compatibilidade: vários métodos
  // chamam getCurrentStage(session).
  get currentStage() {
    return null;
  }

  formatList(items) {
    if (!Array.isArray(items) || !items.length) {
      return 'Não especificadas.';
    }

    return items
      .filter(Boolean)
      .map(
        (item) =>
          `- ${item}`
      )
      .join('\n');
  }

  getRecentHistoryText(
    session,
    limit = DEFAULT_HISTORY_LIMIT
  ) {
    return (
      session.conversationHistory || []
    )
      .slice(-limit)
      .map(
        (message) =>
          `${
            message.role === 'assistant'
              ? 'Recrutador'
              : 'Candidato'
          }: ${message.content}`
      )
      .join('\n');
  }

  // ============================================================
  // UTILITÁRIOS
  // ============================================================

  firstName(session) {
    return (
      clean(
        session.candidateName
      )
        .split(/\s+/)[0] ||
      'candidato'
    );
  }

  isCancelIntent(text) {
    const value =
      clean(text).toLowerCase();

    if (
      value.length > 80
    ) {
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
    const tokens =
      clean(text)
        .split(/\s+/)
        .filter(Boolean);

    return (
      tokens.length <= 3 &&
      !/[?؟]$/.test(
        clean(text)
      )
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
      .map(
        (word) => {
          const normalized =
            word.toLowerCase();

          return (
            normalized.charAt(0)
              .toUpperCase() +
            normalized.slice(1)
          );
        }
      )
      .join(' ');
  }

  calculateFinalScore(
    session
  ) {
    if (
      !Array.isArray(
        session.scores
      ) ||
      !session.scores.length
    ) {
      return 0;
    }

    const total =
      session.scores.reduce(
        (
          sum,
          score
        ) =>
          sum +
          clamp(
            score?.score,
            0,
            10
          ),
        0
      );

    return (
      total /
      session.scores.length
    );
  }

  determineResult(
    averageScore
  ) {
    if (
      averageScore >= 8
    ) {
      return 'Excelente';
    }

    if (
      averageScore >= 6
    ) {
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
    if (
      session &&
      typeof session.updateLanguageLevel ===
        'function'
    ) {
      session.updateLanguageLevel(
        text
      );
    }
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

