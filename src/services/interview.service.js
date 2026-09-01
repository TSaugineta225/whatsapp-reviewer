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
// HELPERS
// ============================================================

const pick = (items = []) => {
  if (!Array.isArray(items) || !items.length) return '';
  return items[Math.floor(Math.random() * items.length)];
};

const CLEAN = (value) =>
  String(value || '')
    .replace(/^["'`]+|["'`]+$/g, '')
    .replace(/\s+\n/g, '\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();

const SIMPLE_TONE = {
  simple:
    'Usa palavras simples, frases curtas e naturais. Evita jargão. Faz uma pergunta de cada vez.',
  medium:
    'Usa linguagem profissional, clara e acessível. Mantém a conversa natural e sem formalidade excessiva.',
  advanced:
    'Usa linguagem mais elaborada, mas continua natural, humana e fácil de acompanhar.',
};

const VALIDATION_PHRASES = [
  'Percebi.',
  'Certo.',
  'Faz sentido.',
  'Já fiquei com uma ideia melhor.',
  'Isso ajuda a perceber melhor.',
  'Estou a acompanhar.',
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
      .map((word) =>
        word.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      )
      .join('|') +
    ')\\b[\\s,.:;!—-]*',
  'i'
);

const ACK_FALLBACK = [
  'Esse detalhe ajuda-me a perceber melhor a experiência que teve.',
  'Fico com uma ideia mais clara de como lidou com essa situação.',
  'Esse contexto é útil para perceber como trabalha na prática.',
  'Consigo perceber melhor o tipo de situação que já enfrentou.',
  'Isso ajuda a colocar a experiência em contexto.',
];

const STOPWORDS = new Set(
  (
    'a o as os um uma de do da dos das em no na nos nas e ou que se por para com sem sobre ' +
    'ao aos à às pelo pela isso isto esse essa este esta seu sua seus suas meu minha meus minhas ' +
    'muito mais já lhe me te nos vos ser estar tem ter é são foi era como quando onde qual quais ' +
    'não sim para por uma uns umas dele dela'
  ).split(' ')
);

const words = (value) =>
  String(value || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z\s]/g, ' ')
    .split(/\s+/)
    .filter(
      (word) => word.length > 3 && !STOPWORDS.has(word)
    );

// ============================================================
// SERVICE
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

    this.stages = STAGES;

    this.typingMsPerChar = 25;
    this.typingMax = 3000;
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
        session.cancelTimeout();
      } catch (_) {}
    }

    this.sessions.delete(userId);
    this.clearQuestionCollection(userId);
  }

  resetInterviewState(session) {
    const preservedHistory = Array.isArray(
      session.conversationHistory
    )
      ? session.conversationHistory
      : [];

    session.stageIndex = 0;
    session.conversationHistory = preservedHistory;
    session.scores = [];
    session.lastQuestion = null;

    session.inQASection = false;
    session.questionCounter = 0;

    session.askedQuestions = new Set();

    session.pendingCancel = false;
    session.offTopicAttempts = 0;

    session.topicsCovered = [];
    session.turnCount = 0;

    session.usedOpeners = [];
    session.usedWords = {};

    session.ackStreak = 0;
    session.qaOffered = false;

    session.shortAnswerStreak = 0;

    session.candidateEmail = session.candidateEmail || null;

    session.languageLevel = 'simple';
  }

  scheduleSessionTimeout(userId, session) {
    try {
      session.cancelTimeout();
    } catch (_) {}

    session.scheduleTimeout(async (expiredSession) => {
      const currentSession = this.sessions.get(userId);

      if (
        !currentSession ||
        currentSession !== expiredSession
      ) {
        return;
      }

      if (!this.isSessionExpired(currentSession)) {
        return;
      }

      const message =
        this.generateTimeoutMessage(currentSession);

      await this.sendMessage(userId, message);

      this.endSession(userId);

      console.log(
        `[TIMEOUT] Sessão encerrada por inatividade: ${userId}`
      );
    });
  }

  isSessionExpired(session) {
    const timeoutMinutes = Number(
      INTERVIEW_CONFIG.timeoutMinutes || 5
    );

    const timeoutMs =
      timeoutMinutes * 60 * 1000;

    return (
      Date.now() - Number(session.lastInteraction || 0) >
      timeoutMs
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
      const session = new InterviewSession(userId);

      session.jobTitle =
        jobTitle || JOB_VACANCY.title;

      session.company =
        company || COMPANY;

      session.jobVacancy =
        jobVacancy || JOB_VACANCY;

      this.resetInterviewState(session);

      this.sessions.set(userId, session);

      session.updateLastInteraction();

      this.scheduleSessionTimeout(
        userId,
        session
      );

      const welcomeMessage =
        this.generateWelcomeMessage(session);

      const whatsappService =
        global.whatsappService;

      if (
        whatsappService &&
        typeof whatsappService.sendInteractiveMessage ===
          'function'
      ) {
        await whatsappService.sendInteractiveMessage(
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
      COMPANY.industry ||
      '';

    return [
      `Olá! Aqui é do processo de selecção da ${companyName}.`,
      `Estamos a recrutar para a posição de ${jobTitle}${
        industry
          ? `, na área de ${industry}`
          : ''
      }.`,
      '',
      'A ideia é termos uma conversa tranquila sobre a sua experiência, a forma como trabalha e algumas situações reais que já enfrentou.',
      '',
      'Para começarmos, pode dizer-me o seu nome completo?',
    ].join('\n');
  }

  generateTimeoutMessage(session) {
    const name =
      this.firstName(session) ||
      'candidato';

    return [
      `Olá, ${name}. Ficámos algum tempo sem receber resposta e a entrevista foi encerrada por inactividade.`,
      '',
      'Pode iniciar novamente quando estiver disponível.',
      '',
      `Obrigado pelo seu interesse na ${session.getCompanyName()}.`,
    ].join('\n');
  }

  async sendMessage(userId, text) {
    if (
      !global.whatsappService ||
      !text
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
  // MENSAGENS HUMANIZADAS
  // ============================================================

  async sendHuman(
    userId,
    text,
    client = global.whatsappService
  ) {
    if (!text || !client) return;

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
      } catch (_) {}

      await this.delay(
        Math.min(
          bubble.length *
            this.typingMsPerChar,
          this.typingMax
        )
      );

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
      .map((part) => part.trim())
      .filter(Boolean);

    if (!parts.length) return [];

    const merged = [];

    for (const part of parts) {
      const last =
        merged[merged.length - 1];

      if (
        last &&
        (last.length < 60 ||
          part.length < 40)
      ) {
        merged[merged.length - 1] =
          `${last}\n\n${part}`;
      } else {
        merged.push(part);
      }
    }

    if (merged.length <= 3) {
      return merged;
    }

    return [
      merged[0],
      merged.slice(1, -1).join('\n\n'),
      merged[merged.length - 1],
    ];
  }

  delay(ms) {
    return new Promise((resolve) =>
      setTimeout(resolve, ms)
    );
  }

  // ============================================================
  // PROCESSAMENTO PRINCIPAL
  // ============================================================

  async handleResponse(userId, message) {
    const session =
      this.getSession(userId);

    if (!session) {
      return null;
    }

    const text = CLEAN(message);

    if (!text) {
      return 'Pode enviar a sua resposta quando estiver pronto.';
    }

    // ----------------------------------------------------------
    // CANCELAMENTO
    // ----------------------------------------------------------

    const cancellationResponse =
      await this.handleCancellationIntent(
        userId,
        session,
        text
      );

    if (cancellationResponse !== null) {
      return cancellationResponse;
    }

    // ----------------------------------------------------------
    // TIMEOUT / INTERAÇÃO
    // ----------------------------------------------------------

    if (this.isSessionExpired(session)) {
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
    // NOME
    // ----------------------------------------------------------

    if (!session.candidateName) {
      return await this.handleCandidateName(
        session,
        text
      );
    }

    session.updateLanguageLevel(text);

    // ----------------------------------------------------------
    // PERGUNTA DO CANDIDATO
    // ----------------------------------------------------------

    if (this.isCandidateQuestion(text)) {
      const reply =
        await this.answerCandidate(
          session,
          text
        );

      return [
        reply,
        '',
        this.resumeLine(session),
      ].join('\n');
    }

    // ----------------------------------------------------------
    // RESPOSTAS CURTAS
    // ----------------------------------------------------------

    if (this.isTooShort(text)) {
      session.shortAnswerStreak =
        (session.shortAnswerStreak || 0) + 1;

      if (session.shortAnswerStreak < 2) {
        return this.varied(session, [
          `Conte-me um pouco mais sobre isso — um caso concreto ajuda-me a perceber melhor.`,
          'Como foi essa situação na prática?',
          'Lembra-se de uma situação real em que isso aconteceu?',
          'E o que fez nessa situação?',
        ]);
      }
    } else {
      session.shortAnswerStreak = 0;
    }

    // ----------------------------------------------------------
    // ANÁLISE PRINCIPAL
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
        (session.offTopicAttempts || 0) + 1;

      if (
        session.offTopicAttempts >= 2
      ) {
        return await this.advanceFocus(
          session,
          turn.acknowledgement
        );
      }

      return this.rephrase(session);
    }

    session.offTopicAttempts = 0;

    // ----------------------------------------------------------
    // REGISTAR TURNO
    // ----------------------------------------------------------

    this.recordTurn(
      session,
      text,
      turn
    );

    // ----------------------------------------------------------
    // FIM DA ENTREVISTA
    // ----------------------------------------------------------

    if (turn.interview_complete) {
      return await this.concludeInterview(
        session
      );
    }

    session.questionCounter =
      (session.questionCounter || 0) + 1;

    const questionsPerStage =
      Number(
        INTERVIEW_CONFIG.questionsPerStage || 3
      );

    if (
      turn.should_transition ||
      session.questionCounter >=
        questionsPerStage
    ) {
      return await this.advanceFocus(
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

      session.lastQuestion = question;
      session.askedQuestions.add(question);

      return this.compose(
        session,
        turn.acknowledgement,
        question
      );
    }

    return await this.advanceFocus(
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
      return 'Não consegui perceber o nome. Pode escrever apenas o seu nome, por favor?';
    }

    session.candidateName = name;

    session.updateLanguageLevel(text);

    const firstQuestion =
      await this.askFirstQuestion(
        session
      );

    session.lastQuestion =
      firstQuestion;

    session.askedQuestions.add(
      firstQuestion
    );

    return [
      `Muito prazer, ${this.firstName(session)}.`,
      '',
      firstQuestion,
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
      this.stages[session.stageIndex] ||
      this.stages[this.stages.length - 1];

    const agent =
      AGENTS[stage.agent] ||
      AGENTS.recruiter;

    const tone =
      SIMPLE_TONE[
        session.languageLevel
      ] ||
      SIMPLE_TONE.simple;

    const historyText =
      this.getRecentHistoryText(
        session,
        6
      );

    const covered =
      (session.topicsCovered || [])
        .filter(Boolean)
        .join('; ') ||
      'nenhum ainda';

    const asked =
      Array.from(
        session.askedQuestions || []
      )
        .slice(-5)
        .join(' | ') ||
      'nenhuma';

    const overused =
      this.getOverusedWords(
        session
      );

    const prompt = `
És um recrutador experiente. Estás a conduzir uma entrevista profissional por WhatsApp.

A conversa deve parecer uma conversa real, não um questionário automático.

OBJETIVO INTERNO DESTA PARTE:
${stage.name} — ${stage.objective}

NÃO reveles este objetivo ao candidato.

Candidato:
${session.candidateName}

Empresa:
${session.getCompanyName()}

Vaga:
${session.getJobTitle()}

Nível de linguagem:
${session.languageLevel}

${tone}

TEMAS JÁ COBERTOS:
${covered}

PERGUNTAS JÁ FEITAS:
${asked}

HISTÓRICO RECENTE:
${historyText || '(início da conversa)'}

ÚLTIMA PERGUNTA:
${session.lastQuestion || '(nenhuma)'}

RESPOSTA DO CANDIDATO:
"${answer}"

ANALISA A RESPOSTA E DEVOLVE APENAS JSON.

1. acknowledgement
Uma reação humana curta, no máximo 15 palavras.
Deve referir um detalhe concreto da resposta.
Pode ser "" quando não existir nada relevante para comentar.

Não comeces com:
${BANNED_OPENERS.join(', ')}.

Também evita estas palavras já usadas frequentemente:
${overused || '(nenhuma)'}

2. score
Nota geral de 0 a 10 para o conteúdo da resposta.

3. clarity
0 a 4.

4. relevance
0 a 3.

5. depth
0 a 3.

Avalia apenas o conteúdo da resposta, nunca ortografia, sotaque, género,
idade, aparência, nacionalidade ou forma de escrever.

6. off_topic
true somente quando a resposta estiver totalmente desconectada da pergunta.

7. generic
true quando a resposta não apresentar substância suficiente ou parecer
decorada, vaga ou sem exemplo concreto.

8. emotion
Uma palavra curta descrevendo o estado aparente na resposta.
Exemplos: confiante, neutro, inseguro, entusiasmado, frustrado.

9. topic
Duas a quatro palavras resumindo o assunto da resposta.

10. next_question
Uma única pergunta aberta que nasça naturalmente da resposta.
Usa detalhes apresentados pelo candidato.
Se a resposta for vaga, pede um exemplo concreto.
Máximo de duas frases.

Usa null quando for melhor mudar de foco.

11. should_transition
true quando já existir informação suficiente nesta etapa.

12. interview_complete
true SOMENTE quando esta for realmente a última etapa
e existir informação suficiente.

13. feedback
Nota interna curta, máximo 20 palavras.
Não será mostrada ao candidato.

REGRAS DA CONVERSA:

- uma pergunta de cada vez;
- não repetir perguntas;
- não elogiar automaticamente;
- não usar emojis;
- não dizer que o candidato está a ser avaliado;
- não mencionar score;
- não mencionar etapas ou fases;
- não fazer comentários sobre a forma de escrever;
- preferir perguntas que aprofundem algo que a pessoa acabou de dizer;
- quando possível, pedir situações concretas;
- não inventar informação.

JSON EXATO:

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
          'Resposta da IA sem JSON válido'
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
    const number = (
      value,
      max
    ) => {
      const parsed =
        Number(value);

      if (!Number.isFinite(parsed)) {
        return 0;
      }

      return Math.max(
        0,
        Math.min(max, parsed)
      );
    };

    const acknowledgement =
      CLEAN(
        turn.acknowledgement
      );

    const nextQuestion =
      turn.next_question
        ? CLEAN(turn.next_question)
        : null;

    return {
      acknowledgement,
      score: number(
        turn.score,
        10
      ),
      clarity: number(
        turn.clarity,
        4
      ),
      relevance: number(
        turn.relevance,
        3
      ),
      depth: number(
        turn.depth,
        3
      ),
      off_topic:
        turn.off_topic === true,
      generic:
        turn.generic === true,
      emotion:
        CLEAN(turn.emotion).toLowerCase() ||
        'neutro',
      topic: CLEAN(turn.topic),
      next_question:
        nextQuestion || null,
      should_transition:
        turn.should_transition === true,
      interview_complete:
        turn.interview_complete === true,
      feedback: CLEAN(turn.feedback),
    };
  }

  createTurnFallback(session) {
    return {
      acknowledgement:
        this.varied(
          session,
          ACK_FALLBACK
        ),
      score: 5,
      clarity: 2,
      relevance: 2,
      depth: 1,
      off_topic: false,
      generic: false,
      emotion: 'neutro',
      topic: '',
      next_question: null,
      should_transition: true,
      interview_complete: false,
      feedback:
        'Avaliação automática indisponível.',
    };
  }

  // ============================================================
  // REGISTO DA CONVERSA
  // ============================================================

  recordTurn(
    session,
    answer,
    turn
  ) {
    const effectiveScore =
      turn.generic
        ? Math.max(
            0,
            turn.score - 0.5
          )
        : turn.score;

    session.scores.push({
      score: effectiveScore,
      clarity: turn.clarity,
      relevance: turn.relevance,
      depth: turn.depth,
      feedback: turn.feedback,
      question:
        session.lastQuestion || '',
      answer,
      emotion: turn.emotion,
      artificial: turn.generic,
      topic: turn.topic || '',
    });

    if (turn.topic) {
      session.topicsCovered.push(
        turn.topic
      );
    }

    if (session.lastQuestion) {
      session.conversationHistory.push({
        role: 'assistant',
        content:
          session.lastQuestion,
      });
    }

    session.conversationHistory.push({
      role: 'user',
      content: answer,
    });
  }

  // ============================================================
  // PRIMEIRA PERGUNTA
  // ============================================================

  async askFirstQuestion(session) {
    const stage =
      this.stages[session.stageIndex] ||
      this.stages[0];

    const agent =
      AGENTS[stage.agent] ||
      AGENTS.recruiter;

    const tone =
      SIMPLE_TONE[
        session.languageLevel
      ] ||
      SIMPLE_TONE.simple;

    const prompt = `
Vais iniciar uma entrevista por WhatsApp com ${session.candidateName},
candidato à vaga de ${session.getJobTitle()}.

Objetivo interno:
${stage.objective}

Não reveles esse objetivo.

${tone}

Escreve UMA pergunta de abertura natural.
A pessoa deve conseguir contar uma experiência ou situação concreta.

Não uses:
- "fale-me um pouco sobre si";
- linguagem de questionário;
- "etapa";
- "fase";
- "avaliação";
- frases muito formais.

Máximo de duas frases.
Sem emojis.

Responde apenas com a pergunta.
`;

    try {
      const question =
        await this.generateAI(
          prompt,
          agent.systemPrompt
        );

      return (
        CLEAN(question) ||
        this.fallbackQuestion(stage)
      );
    } catch (error) {
      console.error(
        '[INTERVIEW] Falha na primeira pergunta:',
        error.message
      );

      return this.fallbackQuestion(
        stage
      );
    }
  }

  fallbackQuestion(stage) {
    const objective =
      String(
        stage?.objective ||
          'o seu percurso profissional'
      ).toLowerCase();

    return `Conte-me uma situação recente do seu trabalho que tenha relação com ${objective}.`;
  }

  // ============================================================
  // TRANSIÇÃO ENTRE TEMAS
  // ============================================================

  async advanceFocus(
    session,
    acknowledgement = ''
  ) {
    if (
      session.stageIndex >=
      this.stages.length - 1
    ) {
      return await this.concludeInterview(
        session
      );
    }

    const previousStage =
      this.stages[session.stageIndex];

    session.stageIndex += 1;
    session.questionCounter = 0;
    session.offTopicAttempts = 0;

    const nextStage =
      this.stages[session.stageIndex];

    const agent =
      AGENTS[nextStage.agent] ||
      AGENTS.recruiter;

    const tone =
      SIMPLE_TONE[
        session.languageLevel
      ] ||
      SIMPLE_TONE.simple;

    const recentAnswers =
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

    const overused =
      this.getOverusedWords(
        session
      );

    const prompt = `
Estás a conduzir uma entrevista profissional por WhatsApp.

Candidato:
${session.candidateName}

Vaga:
${session.getJobTitle()}

A pessoa acabou de dizer:
"${recentAnswers}"

Tema anterior:
${previousStage.objective}

Novo tema interno:
${nextStage.objective}

Cria UMA pergunta natural que faça uma ponte entre os dois assuntos.

Não digas:
- "agora vamos falar de";
- "mudando de assunto";
- "passando para";
- "na próxima etapa";
- "vamos avaliar".

Parte de algo que a pessoa acabou de dizer sempre que possível.

Evita estas palavras muito utilizadas:
${overused || '(nenhuma)'}

${tone}

Máximo de duas frases.
Sem emojis.
Responde apenas com a pergunta.
`;

    let question = '';

    try {
      question = CLEAN(
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
  // Q&A DO CANDIDATO
  // ============================================================

  isCandidateQuestion(text) {
    const value =
      String(text || '')
        .trim()
        .toLowerCase();

    if (!value) return false;

    const hasQuestionMark =
      /[?؟]$/.test(value);

    const candidateQuestionTerms =
      /(voc[êe]s|empresa|vaga|sal[áa]rio|hor[áa]rio|equipa|contrato|remunera|benef[íi]cio|processo|quando|onde fica|como funciona|quanto|regime|local de trabalho|entrada)/i;

    return (
      hasQuestionMark ||
      candidateQuestionTerms.test(
        value
      )
    );
  }

  async answerCandidate(
    session,
    question
  ) {
    return await this.generateBatchAnswers(
      [question],
      session
    );
  }

  async generateBatchAnswers(
    questions,
    session
  ) {
    try {
      const questionsText =
        questions
          .map(
            (question, index) =>
              `${index + 1}. ${question}`
          )
          .join('\n');

      const tone =
        SIMPLE_TONE[
          session.languageLevel
        ] ||
        SIMPLE_TONE.simple;

      const company =
        session.company ||
        COMPANY;

      const jobVacancy =
        session.jobVacancy ||
        JOB_VACANCY;

      const prompt = `
Responde às perguntas de um candidato durante uma entrevista profissional em português de Moçambique.

PERGUNTAS:
${questionsText}

EMPRESA:
${JSON.stringify(company)}

VAGA:
${JSON.stringify(jobVacancy)}

${tone}

REGRAS:

- responde directamente;
- não uses uma introdução longa;
- responde cada pergunta separadamente quando houver mais de uma;
- usa apenas informação fornecida sobre a empresa e vaga;
- não inventes salário, benefícios, horários ou políticas;
- se não existir informação suficiente, diz que será necessário confirmar com a equipa;
- assuntos de remuneração pessoal devem ser encaminhados para o RH;
- sem emojis;
- mantém tom humano.

`;

      const response =
        await this.generateAI(
          prompt,
          AGENTS.recruiter.systemPrompt
        );

      return (
        CLEAN(response) ||
        'Essa informação preciso de confirmar com a equipa antes de lhe responder com segurança.'
      );
    } catch (error) {
      console.error(
        '[INTERVIEW] Erro no Q&A:',
        error.message
      );

      return (
        'Essa informação preciso de confirmar com a equipa antes de lhe responder com segurança.'
      );
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

    const prompt = `
Pergunta feita:
"${lastQuestion}"

Resposta:
"${message}"

A resposta está totalmente fora do assunto da pergunta?

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
        CLEAN(answer)
      );
    } catch (error) {
      return false;
    }
  }

  handleOffTopic(session) {
    const question =
      session.lastQuestion ||
      'Conte-me um pouco mais sobre a sua experiência.';

    const lead =
      this.varied(session, [
        'Deixe-me perguntar de outra forma:',
        'Voltando ao que lhe perguntei:',
        'O que queria perceber era isto:',
      ]);

    return `${lead} ${question}`;
  }

  // ============================================================
  // CONTROLO DE REPETIÇÃO
  // ============================================================

  openerKey(text) {
    return words(text)
      .slice(0, 2)
      .join(' ');
  }

  registerLanguage(
    session,
    text
  ) {
    session.usedWords =
      session.usedWords || {};

    for (const word of words(text)) {
      session.usedWords[word] =
        (session.usedWords[word] || 0) +
        1;
    }

    const key =
      this.openerKey(text);

    if (key) {
      session.usedOpeners =
        session.usedOpeners || [];

      if (
        !session.usedOpeners.includes(
          key
        )
      ) {
        session.usedOpeners.push(
          key
        );
      }

      // Mantém apenas histórico recente.
      if (
        session.usedOpeners.length > 40
      ) {
        session.usedOpeners =
          session.usedOpeners.slice(-40);
      }
    }
  }

  getOverusedWords(session) {
    return Object.entries(
      session.usedWords || {}
    )
      .filter(
        ([, count]) => count >= 2
      )
      .sort(
        (a, b) => b[1] - a[1]
      )
      .slice(0, 15)
      .map(([word]) => word)
      .join(', ');
  }

  cleanAck(
    session,
    acknowledgement
  ) {
    let value =
      CLEAN(acknowledgement);

    if (!value) return '';

    let attempts = 0;
    let trimmed = false;

    while (
      BANNED_RE.test(value) &&
      attempts < 3
    ) {
      value = CLEAN(
        value.replace(
          BANNED_RE,
          ''
        )
      );

      trimmed = true;
      attempts += 1;
    }

    if (!value) {
      return '';
    }

    if (
      trimmed &&
      (value.length < 30 ||
        /^(que|o que|isso|isto|e |mas |faz|para mim)\b/i.test(
          value
        ))
    ) {
      return '';
    }

    value =
      value.charAt(0).toUpperCase() +
      value.slice(1);

    const key =
      this.openerKey(value);

    if (
      key &&
      (session.usedOpeners || [])
        .includes(key)
    ) {
      return '';
    }

    const overused =
      words(value).some(
        (word) =>
          (session.usedWords || {})[
            word
          ] >= 3
      );

    if (overused) {
      return '';
    }

    return value;
  }

  varied(
    session,
    options
  ) {
    const fresh =
      options.filter(
        (option) =>
          !(session.usedOpeners || [])
            .includes(
              this.openerKey(option)
            )
      );

    const selected =
      pick(
        fresh.length
          ? fresh
          : options
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
      CLEAN(question);

    if (!cleanQuestion) {
      return '';
    }

    let reaction =
      this.cleanAck(
        session,
        acknowledgement
      );

    // Evita cadeias de validações automáticas.
    if (
      reaction &&
      (session.ackStreak || 0) >= 1
    ) {
      reaction = '';
    }

    if (!reaction) {
      session.ackStreak = 0;

      this.registerLanguage(
        session,
        cleanQuestion
      );

      return cleanQuestion;
    }

    session.ackStreak =
      (session.ackStreak || 0) + 1;

    this.registerLanguage(
      session,
      reaction
    );

    this.registerLanguage(
      session,
      cleanQuestion
    );

    const validation =
      pick(
        VALIDATION_PHRASES
      );

    return `${validation} ${reaction}\n\n${cleanQuestion}`;
  }

  // ============================================================
  // PERGUNTAS DUPLICADAS
  // ============================================================

  dedupeQuestion(
    session,
    question
  ) {
    const cleanQuestion =
      CLEAN(question);

    if (!cleanQuestion) {
      return this.fallbackQuestion(
        this.stages[
          session.stageIndex
        ]
      );
    }

    const asked =
      Array.from(
        session.askedQuestions || []
      );

    if (
      !this.isQuestionTooSimilar(
        cleanQuestion,
        asked
      )
    ) {
      return cleanQuestion;
    }

    // Em vez de apenas colocar:
    // "Deixe-me perguntar de outra forma..."
    //
    // tentamos uma variação curta.
    return `Pode dar-me um exemplo concreto disso?`;
  }

  isQuestionTooSimilar(
    question,
    askedQuestions
  ) {
    const currentWords =
      new Set(words(question));

    if (!currentWords.size) {
      return false;
    }

    return askedQuestions.some(
      (oldQuestion) => {
        const oldWords =
          new Set(words(oldQuestion));

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

        return similarity >= 0.70;
      }
    );
  }

  rephrase(session) {
    const question =
      session.lastQuestion ||
      'Conte-me um pouco mais sobre a sua experiência.';

    const lead =
      this.varied(session, [
        'Deixe-me perguntar de outra forma:',
        'Voltando ao que lhe perguntei:',
        'O que queria perceber era isto:',
      ]);

    return `${lead} ${question}`;
  }

  resumeLine(session) {
    const question =
      session.lastQuestion ||
      'Conte-me um pouco mais sobre o seu percurso.';

    const lead =
      this.varied(session, [
        'Voltando ao que estávamos a falar:',
        'Continuando de onde ficámos:',
        'E pegando nisso:',
      ]);

    return `${lead} ${question}`;
  }

  // ============================================================
  // CONCLUSÃO
  // ============================================================

  async concludeInterview(session) {
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
        (score) => score.artificial
      ).length;

    if (genericCount >= 2) {
      lines.push(
        '',
        'Nas próximas conversas, exemplos concretos do que já fez podem ajudar a mostrar melhor a sua experiência.'
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
        classificacao: result,
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
    const passed =
      averageScore >=
      Number(
        INTERVIEW_CONFIG.minScoreToPass ||
          6
      );

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

          lines.push(
            '',
            `Os detalhes foram enviados para ${email}.`,
            `Link da reunião: ${meetingLink}`
          );
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
          score: Number(
            averageScore.toFixed(2)
          ),
          feedback: result,
          recommendation: passed
            ? 'approve'
            : 'hold',
          transcript:
            session.conversationHistory,
          scoresBreakdown:
            session.scores,
          interviewId:
            session.interviewId || null,
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
            `Pergunta: ${score.question}\nResposta: ${score.answer}`
        )
        .join('\n\n');

    if (!history) {
      return 'Obrigado por partilhar o seu percurso connosco.';
    }

    const prompt = `
Com base nesta entrevista, escreve um resumo curto para o candidato.

O texto deve ter 2 ou 3 frases.

Destaca pontos fortes CONCRETOS que a pessoa realmente demonstrou.
Não inventes características.
Não atribuas competências que não aparecem nas respostas.

Não uses:
- emojis;
- notas;
- pontuações;
- clichés;
- "foi uma excelente entrevista";
- elogios genéricos.

Entrevista:

${history}
`;

    try {
      const summary =
        await this.generateAI(
          prompt,
          AGENTS.recruiter.systemPrompt
        );

      return (
        CLEAN(summary) ||
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
  // AI WRAPPER
  // ============================================================

  async generateAI(
    prompt,
    systemPrompt
  ) {
    const messages = [
      {
        role: 'system',
        content:
          systemPrompt ||
          AGENTS.recruiter.systemPrompt,
      },
      {
        role: 'user',
        content: prompt,
      },
    ];

    // Mantém a compatibilidade com o AIService atual.
    //
    // O serviço atual expõe generateResponse().
    // Centralizar aqui permite trocar o provider futuramente
    // sem alterar toda a entrevista.
    return await this.aiService.generateResponse(
      prompt,
      systemPrompt
    );
  }

  // ============================================================
  // JSON
  // ============================================================

  parseJsonObject(response) {
    const text =
      CLEAN(response);

    if (!text) {
      return null;
    }

    // Primeiro tenta resposta JSON pura.
    try {
      const direct =
        JSON.parse(text);

      if (
        direct &&
        typeof direct === 'object'
      ) {
        return direct;
      }
    } catch (_) {}

    // Remove possíveis fences.
    const clean =
      text
        .replace(/^```json\s*/i, '')
        .replace(/^```\s*/i, '')
        .replace(/\s*```$/i, '')
        .trim();

    try {
      const direct =
        JSON.parse(clean);

      if (
        direct &&
        typeof direct === 'object'
      ) {
        return direct;
      }
    } catch (_) {}

    // Procura um objeto JSON balanceando chaves.
    const objectText =
      this.extractBalancedJson(
        clean
      );

    if (!objectText) {
      return null;
    }

    try {
      const parsed =
        JSON.parse(objectText);

      return parsed &&
        typeof parsed === 'object'
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
  // HISTÓRICO
  // ============================================================

  getRecentHistoryText(
    session,
    limit = 6
  ) {
    return session.conversationHistory
      .slice(-limit)
      .map(
        (message) =>
          `${
            message.role ===
            'assistant'
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
      String(
        session.candidateName || ''
      )
        .split(/\s+/)[0] ||
      'colega'
    );
  }

  isCancelIntent(text) {
    const value =
      String(text || '')
        .toLowerCase()
        .trim();

    if (value.length > 80) {
      return false;
    }

    return (
      /(quero (cancelar|sair|desistir|parar))/.test(
        value
      ) ||
      /^(cancelar|desistir|sair|encerrar|abortar)$/.test(
        value
      ) ||
      /n[ãa]o quero continuar/.test(
        value
      ) ||
      /^desisto$/.test(value)
    );
  }

  isTooShort(text) {
    const tokenCount =
      String(text || '')
        .trim()
        .split(/\s+/)
        .filter(Boolean)
        .length;

    return (
      tokenCount <= 3 &&
      !/\?/.test(text)
    );
  }

  normalizeName(text) {
    const cleaned =
      String(text || '')
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
      cleaned.length < 2 ||
      cleaned.length > 60
    ) {
      return null;
    }

    const parts =
      cleaned
        .split(/\s+/)
        .filter(Boolean);

    if (!parts.length) {
      return null;
    }

    return parts
      .map(
        (word) =>
          word.charAt(0).toUpperCase() +
          word
            .slice(1)
            .toLowerCase()
      )
      .join(' ');
  }

  calculateFinalScore(session) {
    if (
      !Array.isArray(session.scores) ||
      !session.scores.length
    ) {
      return 0;
    }

    const total =
      session.scores.reduce(
        (sum, score) =>
          sum + Number(score.score || 0),
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
      String(email || '').trim()
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

  clearQuestionCollection(userId) {
    this.questionCollector.delete(
      userId
    );

    if (
      this.collectorTimers.has(userId)
    ) {
      clearTimeout(
        this.collectorTimers.get(userId)
      );

      this.collectorTimers.delete(
        userId
      );
    }
  }
}

module.exports = InterviewService;

