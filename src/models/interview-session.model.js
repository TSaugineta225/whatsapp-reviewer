// src/models/interview-session.model.js

const { INTERVIEW_CONFIG } = require('../config/interview');

// ============================================================
// CONSTANTES
// ============================================================

const DEFAULT_TIMEOUT_MINUTES =
  Number(INTERVIEW_CONFIG.timeoutMinutes) || 20;

const DEFAULT_HISTORY_LIMIT = 6;
const DEFAULT_LANGUAGE_LEVEL = 'simple';
const DEFAULT_STAGE = 'initial';

const DEFAULT_TIMEOUT_MS =
  DEFAULT_TIMEOUT_MINUTES * 60 * 1000;

const DEFAULT_SCORE = 0;
const MAX_SCORE = 10;

const LANGUAGE_LEVELS = Object.freeze({
  SIMPLE: 'simple',
  MEDIUM: 'medium',
  ADVANCED: 'advanced',
});

const COMPLEX_WORDS = [
  'especificamente',
  'adicionalmente',
  'particularmente',
  'consideravelmente',
  'significativamente',
  'simultaneamente',
  'consequentemente',
  'alternativamente',
  'fundamentalmente',
  'substantivamente',
];

const COMPLEX_WORDS_PATTERN = new RegExp(
  `\\b(?:${COMPLEX_WORDS.join('|')})\\b`,
  'i'
);

// ============================================================
// MODEL
// ============================================================

class InterviewSession {
  constructor(userId) {
    this.userId = userId;

    this._initializeIdentity();
    this._initializeInterviewState();
    this._initializeConversationState();
    this._initializeTimingState();
  }

  // ==========================================================
  // INITIALIZATION
  // ==========================================================

  _initializeIdentity() {
    this.candidateName = null;
    this.candidateEmail = null;

    this.jobTitle = null;
    this.company = null;
    this.jobVacancy = null;

    this.interviewId = null;
    this.holdTransactionId = null;
    this.recruiterId = null;
  }

  _initializeInterviewState() {
    this.stageIndex = 0;
    this.stage = DEFAULT_STAGE;

    this.currentQuestion = 0;
    this.questionCounter = 0;

    this.lastQuestion = null;

    this.askedQuestions = new Set();
    this.topicsCovered = [];

    this.inQASection = false;
    this.followUpPending = false;
    this.pendingCancel = false;

    this.offTopicAttempts = 0;
    this.shortAnswerStreak = 0;

    this.qaOffered = false;
  }

  _initializeConversationState() {
    this.scores = [];
    this.conversationHistory = [];

    this.usedOpeners = [];
    this.usedWords = Object.create(null);

    this.ackStreak = 0;
    this.turnCount = 0;

    this.languageLevel =
      DEFAULT_LANGUAGE_LEVEL;
  }

  _initializeTimingState() {
    const now = Date.now();

    this.startTime = now;
    this.lastInteraction = now;

    this.timeoutDuration =
      DEFAULT_TIMEOUT_MS;

    this.timeoutTimer = null;

    this.timeoutWarningSent = false;
    this.timeoutWarningTime = null;
  }

  // ==========================================================
  // TIMING
  // ==========================================================

  updateLastInteraction() {
    this.lastInteraction = Date.now();

    return this.lastInteraction;
  }

  isExpired(now = Date.now()) {
    if (!this.lastInteraction) {
      return false;
    }

    return (
      now - this.lastInteraction >
      this.timeoutDuration
    );
  }

  getTimeSinceLastInteraction() {
    return Math.max(
      0,
      Date.now() - this.lastInteraction
    );
  }

  getDuration() {
    return Math.max(
      0,
      Math.floor(
        (Date.now() - this.startTime) /
          60000
      )
    );
  }

  scheduleTimeout(callback) {
    if (typeof callback !== 'function') {
      throw new TypeError(
        'scheduleTimeout requer uma função callback.'
      );
    }

    this.cancelTimeout();

    this.timeoutTimer = setTimeout(() => {
      this.timeoutTimer = null;

      try {
        const result = callback(this);

        // Permite callbacks assíncronos sem gerar
        // uma Promise rejeitada não tratada.
        if (
          result &&
          typeof result.catch === 'function'
        ) {
          result.catch(() => {});
        }
      } catch (error) {
        // O callback é responsabilidade do serviço.
        // Aqui apenas evitamos quebrar o timer.
      }
    }, this.timeoutDuration);

    return this.timeoutTimer;
  }

  resetTimeout(callback) {
    return this.scheduleTimeout(callback);
  }

  cancelTimeout() {
    if (!this.timeoutTimer) {
      return false;
    }

    clearTimeout(this.timeoutTimer);
    this.timeoutTimer = null;

    return true;
  }

  // ==========================================================
  // CANDIDATO
  // ==========================================================

  hasBasicInfo() {
    return Boolean(
      this.candidateName?.trim() &&
      this.candidateEmail?.trim()
    );
  }

  getCandidateInfo() {
    return {
      email: this.candidateEmail,
      name: this.candidateName,
      whatsappId: this.userId,
    };
  }

  setCandidateInfo({
    name = null,
    email = null,
  } = {}) {
    this.candidateName =
      this._cleanNullable(name);

    this.candidateEmail =
      this._cleanNullable(email);

    return this;
  }

  // ==========================================================
  // CONTEXTO DA VAGA / EMPRESA
  // ==========================================================

  getCompanyName() {
    return (
      this.company?.name ||
      'Empresa'
    );
  }

  getJobTitle() {
    return (
      this.jobVacancy?.title ||
      this.jobTitle ||
      'Vaga'
    );
  }

  setJobContext({
    jobTitle = null,
    company = null,
    jobVacancy = null,
  } = {}) {
    if (jobTitle != null) {
      this.jobTitle = jobTitle;
    }

    if (company != null) {
      this.company = company;
    }

    if (jobVacancy != null) {
      this.jobVacancy = jobVacancy;
    }

    return this;
  }

  // ==========================================================
  // ESTÁGIO
  // ==========================================================

  getCurrentStageIndex() {
    return this.stageIndex;
  }

  setStage(index, stageId = null) {
    const normalizedIndex = Math.max(
      0,
      Number(index) || 0
    );

    this.stageIndex = normalizedIndex;

    if (stageId) {
      this.stage = String(stageId);
    }

    this.questionCounter = 0;
    this.currentQuestion = 0;
    this.offTopicAttempts = 0;
    this.shortAnswerStreak = 0;

    return this;
  }

  advanceStage(stageId = null) {
    this.stageIndex += 1;

    this.stage =
      stageId ||
      this.stage ||
      DEFAULT_STAGE;

    this.questionCounter = 0;
    this.currentQuestion = 0;
    this.offTopicAttempts = 0;
    this.shortAnswerStreak = 0;

    return this;
  }

  // ==========================================================
  // PERGUNTAS
  // ==========================================================

  getQuestionCount() {
    return this.askedQuestions.size;
  }

  registerQuestion(question) {
    const normalizedQuestion =
      this._cleanNullable(question);

    if (!normalizedQuestion) {
      return false;
    }

    this.lastQuestion =
      normalizedQuestion;

    this.askedQuestions.add(
      normalizedQuestion
    );

    this.currentQuestion += 1;
    this.questionCounter += 1;

    return true;
  }

  hasAskedQuestion(question) {
    if (!question) {
      return false;
    }

    return this.askedQuestions.has(
      String(question).trim()
    );
  }

  getLastQuestion() {
    return this.lastQuestion;
  }

  setLastQuestion(question) {
    this.lastQuestion =
      this._cleanNullable(question);

    return this.lastQuestion;
  }

  // ==========================================================
  // PROGRESSO
  // ==========================================================

  getProgress() {
    return {
      stage: this.stage,
      stageIndex: this.stageIndex,

      questionNumber:
        this.questionCounter + 1,

      currentQuestion:
        this.currentQuestion,

      totalQuestions:
        this.askedQuestions.size,

      scores: [...this.scores],

      duration: this.getDuration(),

      inQASection:
        this.inQASection,

      turnCount:
        this.turnCount,
    };
  }

  // ==========================================================
  // SCORES
  // ==========================================================

  getAverageScore() {
    if (!this.scores.length) {
      return 0;
    }

    const total =
      this.scores.reduce(
        (sum, item) =>
          sum +
          this._normalizeScore(
            item.score
          ),
        0
      );

    return (
      total / this.scores.length
    );
  }

  getLatestScore() {
    return (
      this.scores.length
        ? this.scores[
            this.scores.length - 1
          ]
        : null
    );
  }

  addScore(score = {}) {
    const normalizedScore = {
      score: this._normalizeScore(
        score.score
      ),

      clarity: this._normalizeScore(
        score.clarity
      ),

      relevance: this._normalizeScore(
        score.relevance
      ),

      depth: this._normalizeScore(
        score.depth
      ),

      feedback:
        this._cleanString(
          score.feedback
        ),

      question:
        this._cleanString(
          score.question
        ),

      answer:
        this._cleanString(
          score.answer
        ),

      emotion:
        this._cleanString(
          score.emotion
        ) || 'neutro',

      artificial:
        Boolean(score.artificial),

      topic:
        this._cleanString(
          score.topic
        ),

      evidence:
        this._cleanString(
          score.evidence
        ),

      gap:
        this._cleanString(
          score.gap
        ),
    };

    this.scores.push(
      normalizedScore
    );

    return normalizedScore;
  }

  _normalizeScore(value) {
    const numericValue =
      Number(value);

    if (
      !Number.isFinite(
        numericValue
      )
    ) {
      return DEFAULT_SCORE;
    }

    return Math.max(
      DEFAULT_SCORE,
      Math.min(
        MAX_SCORE,
        numericValue
      )
    );
  }

  // ==========================================================
  // HISTÓRICO
  // ==========================================================

  addToHistory(
    role,
    content
  ) {
    if (
      !role ||
      content == null
    ) {
      return false;
    }

    const normalizedContent =
      this._cleanString(
        content
      );

    if (!normalizedContent) {
      return false;
    }

    this.conversationHistory.push({
      role: String(role),
      content:
        normalizedContent,
    });

    return true;
  }

  getRecentHistory(
    limit = DEFAULT_HISTORY_LIMIT
  ) {
    const parsedLimit =
      Number(limit);

    const normalizedLimit =
      Number.isFinite(parsedLimit)
        ? Math.max(
            0,
            Math.floor(
              parsedLimit
            )
          )
        : DEFAULT_HISTORY_LIMIT;

    if (
      normalizedLimit === 0
    ) {
      return [];
    }

    return this.conversationHistory.slice(
      -normalizedLimit
    );
  }

  clearHistory() {
    this.conversationHistory = [];

    return this;
  }

  // ==========================================================
  // TÓPICOS
  // ==========================================================

  addTopic(topic) {
    const normalizedTopic =
      this._cleanString(topic);

    if (
      !normalizedTopic
    ) {
      return false;
    }

    if (
      !this.topicsCovered.includes(
        normalizedTopic
      )
    ) {
      this.topicsCovered.push(
        normalizedTopic
      );
    }

    return true;
  }

  hasCoveredTopic(topic) {
    if (!topic) {
      return false;
    }

    return this.topicsCovered.includes(
      String(topic).trim()
    );
  }

  // ==========================================================
  // VOCABULÁRIO / REPETIÇÃO
  // ==========================================================

  registerWords(text) {
    const extracted =
      this._extractMeaningfulWords(
        text
      );

    for (const word of extracted) {
      this.usedWords[word] =
        (this.usedWords[word] || 0) +
        1;
    }

    return extracted.length;
  }

  isWordOverused(
    word,
    threshold = 3
  ) {
    if (!word) {
      return false;
    }

    const normalizedWord =
      String(word)
        .trim()
        .toLowerCase();

    const normalizedThreshold =
      Math.max(
        1,
        Number(threshold) || 1
      );

    return (
      (this.usedWords[
        normalizedWord
      ] || 0) >=
      normalizedThreshold
    );
  }

  getWordUsage(word) {
    if (!word) {
      return 0;
    }

    const normalizedWord =
      String(word)
        .trim()
        .toLowerCase();

    return (
      this.usedWords[
        normalizedWord
      ] || 0
    );
  }

  _extractMeaningfulWords(text) {
    return String(text || '')
      .toLowerCase()
      .normalize('NFD')
      .replace(
        /[\u0300-\u036f]/g,
        ''
      )
      .replace(
        /[^a-z\s]/g,
        ' '
      )
      .split(/\s+/)
      .filter(
        (word) =>
          word.length > 3
      );
  }

  // ==========================================================
  // LINGUAGEM
  // ==========================================================

  updateLanguageLevel(text) {
    const normalizedText =
      this._cleanString(text);

    if (!normalizedText) {
      return this.languageLevel;
    }

    const tokens =
      normalizedText
        .split(/\s+/)
        .filter(
          (word) =>
            word.length > 2
        );

    if (!tokens.length) {
      return this.languageLevel;
    }

    const wordCount =
      tokens.length;

    const sentenceCount =
      normalizedText
        .split(/[.!?]+/)
        .filter(
          (sentence) =>
            sentence.trim()
              .length > 0
        )
        .length;

    const averageWordsPerSentence =
      wordCount /
      Math.max(
        sentenceCount,
        1
      );

    const hasComplexWords =
      COMPLEX_WORDS_PATTERN.test(
        normalizedText
      );

    if (
      averageWordsPerSentence > 12 ||
      hasComplexWords
    ) {
      this.languageLevel =
        LANGUAGE_LEVELS.ADVANCED;
    } else if (
      averageWordsPerSentence > 7
    ) {
      this.languageLevel =
        LANGUAGE_LEVELS.MEDIUM;
    } else {
      this.languageLevel =
        LANGUAGE_LEVELS.SIMPLE;
    }

    return this.languageLevel;
  }

  getLanguageLevel() {
    return this.languageLevel;
  }

  // ==========================================================
  // QA / CONTROLO
  // ==========================================================

  resetShortAnswerState() {
    this.shortAnswerStreak = 0;
    return this;
  }

  resetOffTopicState() {
    this.offTopicAttempts = 0;
    return this;
  }

  // ==========================================================
  // RESET
  // ==========================================================

  resetState({
    preserveIdentity = true,
    preserveLanguageLevel = true,
    preserveInterviewContext = true,
  } = {}) {
    this.cancelTimeout();

    const identity = preserveIdentity
      ? {
          userId:
            this.userId,

          candidateName:
            this.candidateName,

          candidateEmail:
            this.candidateEmail,

          interviewId:
            this.interviewId,

          holdTransactionId:
            this.holdTransactionId,

          recruiterId:
            this.recruiterId,
        }
      : {
          userId: this.userId,
        };

    const context =
      preserveInterviewContext
        ? {
            jobTitle:
              this.jobTitle,

            company:
              this.company,

            jobVacancy:
              this.jobVacancy,
          }
        : {
            jobTitle: null,
            company: null,
            jobVacancy: null,
          };

    const languageLevel =
      preserveLanguageLevel
        ? this.languageLevel
        : DEFAULT_LANGUAGE_LEVEL;

    this._initializeInterviewState();
    this._initializeConversationState();
    this._initializeTimingState();

    // Restaurar dados persistentes.
    this.userId =
      identity.userId;

    this.candidateName =
      identity.candidateName ??
      null;

    this.candidateEmail =
      identity.candidateEmail ??
      null;

    this.interviewId =
      identity.interviewId ??
      null;

    this.holdTransactionId =
      identity.holdTransactionId ??
      null;

    this.recruiterId =
      identity.recruiterId ??
      null;

    this.jobTitle =
      context.jobTitle;

    this.company =
      context.company;

    this.jobVacancy =
      context.jobVacancy;

    this.languageLevel =
      languageLevel;

    return this;
  }

  // ==========================================================
  // SERIALIZAÇÃO
  // ==========================================================

  toJSON() {
    return {
      userId: this.userId,

      candidateName:
        this.candidateName,

      candidateEmail:
        this.candidateEmail,

      jobTitle:
        this.jobTitle,

      company:
        this.company,

      jobVacancy:
        this.jobVacancy,

      stageIndex:
        this.stageIndex,

      stage:
        this.stage,

      currentQuestion:
        this.currentQuestion,

      questionCounter:
        this.questionCounter,

      lastQuestion:
        this.lastQuestion,

      askedQuestions:
        Array.from(
          this.askedQuestions
        ),

      topicsCovered:
        [...this.topicsCovered],

      inQASection:
        this.inQASection,

      followUpPending:
        this.followUpPending,

      pendingCancel:
        this.pendingCancel,

      offTopicAttempts:
        this.offTopicAttempts,

      shortAnswerStreak:
        this.shortAnswerStreak,

      qaOffered:
        this.qaOffered,

      scores:
        [...this.scores],

      conversationHistory:
        [...this.conversationHistory],

      usedOpeners:
        [...this.usedOpeners],

      usedWords:
        { ...this.usedWords },

      ackStreak:
        this.ackStreak,

      turnCount:
        this.turnCount,

      startTime:
        this.startTime,

      lastInteraction:
        this.lastInteraction,

      timeoutWarningSent:
        this.timeoutWarningSent,

      timeoutWarningTime:
        this.timeoutWarningTime,

      interviewId:
        this.interviewId,

      holdTransactionId:
        this.holdTransactionId,

      recruiterId:
        this.recruiterId,

      languageLevel:
        this.languageLevel,
    };
  }

  // ==========================================================
  // HELPERS INTERNOS
  // ==========================================================

  _cleanString(value) {
    return String(value ?? '')
      .trim();
  }

  _cleanNullable(value) {
    const normalized =
      this._cleanString(value);

    return normalized
      ? normalized
      : null;
  }
}

module.exports = InterviewSession;

