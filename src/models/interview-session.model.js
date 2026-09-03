// src/models/interview-session.model.js

const { INTERVIEW_CONFIG } = require('../config/interview');

// ============================================================
// DEFAULTS
// ============================================================

const DEFAULT_TIMEOUT_MINUTES = INTERVIEW_CONFIG.timeoutMinutes || 20;
const DEFAULT_HISTORY_LIMIT = 6;
const DEFAULT_LANGUAGE_LEVEL = 'simple';
const DEFAULT_STAGE = 'initial';

const DEFAULT_TIMEOUT_MS = DEFAULT_TIMEOUT_MINUTES * 60 * 1000;

// ============================================================
// MODEL
// ============================================================

class InterviewSession {
  constructor(userId) {
    this.userId = userId;

    this._initializeIdentity();
    this._initializeInterviewState();
    this._initializeConversationState();
    this._initializeEvaluationState();
    this._initializeTimingState();
    this._initializeIntegrationState();
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
    this.usedWords = {};

    this.ackStreak = 0;
    this.turnCount = 0;

    this.languageLevel = DEFAULT_LANGUAGE_LEVEL;
  }

  _initializeEvaluationState() {
    // Reservado para evolução futura da avaliação.
    // Mantido separado para evitar misturar estado de conversa
    // com estado de entrevista.
  }

  _initializeTimingState() {
    const now = Date.now();

    this.startTime = now;
    this.lastInteraction = now;

    this.timeoutTimer = null;
    this.timeoutDuration = DEFAULT_TIMEOUT_MS;

    this.timeoutWarningSent = false;
    this.timeoutWarningTime = null;
  }

  _initializeIntegrationState() {
    // Os valores são definidos em _initializeIdentity().
    // Este método existe apenas para deixar explícita a separação
    // entre estado da entrevista e integrações externas.
  }

  // ==========================================================
  // TIMING
  // ==========================================================

  updateLastInteraction() {
    this.lastInteraction = Date.now();
  }

  isExpired() {
    return Date.now() - this.lastInteraction > this.timeoutDuration;
  }

  getDuration() {
    return Math.floor(
      (Date.now() - this.startTime) / 60000
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
      callback(this);
    }, this.timeoutDuration);
  }

  resetTimeout(callback) {
    this.scheduleTimeout(callback);
  }

  cancelTimeout() {
    if (!this.timeoutTimer) {
      return;
    }

    clearTimeout(this.timeoutTimer);
    this.timeoutTimer = null;
  }

  // ==========================================================
  // CANDIDATO
  // ==========================================================

  hasBasicInfo() {
    return Boolean(
      this.candidateEmail &&
      this.candidateName
    );
  }

  getCandidateInfo() {
    return {
      email: this.candidateEmail,
      name: this.candidateName,
      whatsappId: this.userId,
    };
  }

  // ==========================================================
  // CONTEXTO DA VAGA / EMPRESA
  // ==========================================================

  getCompanyName() {
    return this.company?.name || 'Empresa';
  }

  getJobTitle() {
    return (
      this.jobVacancy?.title ||
      this.jobTitle ||
      'Vaga'
    );
  }

  // ==========================================================
  // PROGRESSO
  // ==========================================================

  getProgress() {
    return {
      stage: this.stage,
      stageIndex: this.stageIndex,

      // Mantido como índice humano para compatibilidade.
      questionNumber: this.currentQuestion + 1,

      // Número de perguntas já registadas.
      totalQuestions: this.askedQuestions.size,

      scores: this.scores,
      duration: this.getDuration(),
      inQASection: this.inQASection,
    };
  }

  // ==========================================================
  // SCORES
  // ==========================================================

  getAverageScore() {
    if (!this.scores.length) {
      return 0;
    }

    const total = this.scores.reduce(
      (sum, item) => sum + this._normalizeScore(item.score),
      0
    );

    return total / this.scores.length;
  }

  addScore(score = {}) {
    this.scores.push({
      score: this._normalizeScore(score.score),
      clarity: this._normalizeScore(score.clarity),
      relevance: this._normalizeScore(score.relevance),
      depth: this._normalizeScore(score.depth),

      feedback: score.feedback || '',
      question: score.question || '',
      answer: score.answer || '',
      emotion: score.emotion || 'neutro',

      artificial: Boolean(score.artificial),
    });
  }

  _normalizeScore(value) {
    const numericValue = Number(value);

    if (!Number.isFinite(numericValue)) {
      return 0;
    }

    return Math.max(0, Math.min(10, numericValue));
  }

  // ==========================================================
  // PERGUNTAS
  // ==========================================================

  getLastQuestion() {
    return this.lastQuestion;
  }

  setLastQuestion(question) {
    this.lastQuestion =
      question == null
        ? null
        : String(question).trim();
  }

  // ==========================================================
  // HISTÓRICO
  // ==========================================================

  addToHistory(role, content) {
    if (!role || content == null) {
      return;
    }

    this.conversationHistory.push({
      role,
      content: String(content),
    });
  }

  getRecentHistory(limit = DEFAULT_HISTORY_LIMIT) {
    const normalizedLimit = Math.max(
      0,
      Number(limit) || DEFAULT_HISTORY_LIMIT
    );

    if (normalizedLimit === 0) {
      return [];
    }

    return this.conversationHistory.slice(-normalizedLimit);
  }

  // ==========================================================
  // VOCABULÁRIO / REPETIÇÃO
  // ==========================================================

  registerWords(text) {
    const words = this._extractMeaningfulWords(text);

    for (const word of words) {
      this.usedWords[word] =
        (this.usedWords[word] || 0) + 1;
    }
  }

  isWordOverused(word, threshold = 3) {
    if (!word) {
      return false;
    }

    const normalizedWord = String(word)
      .trim()
      .toLowerCase();

    const normalizedThreshold = Math.max(
      1,
      Number(threshold) || 1
    );

    return (
      (this.usedWords[normalizedWord] || 0) >=
      normalizedThreshold
    );
  }

  _extractMeaningfulWords(text) {
    return String(text || '')
      .toLowerCase()
      .replace(/[^a-zà-ÿ\s]/gi, ' ')
      .split(/\s+/)
      .filter((word) => word.length > 3);
  }

  // ==========================================================
  // LINGUAGEM
  // ==========================================================

  updateLanguageLevel(text) {
    const normalizedText = String(text || '').trim();

    if (!normalizedText) {
      this.languageLevel = DEFAULT_LANGUAGE_LEVEL;
      return;
    }

    const words = normalizedText
      .split(/\s+/)
      .filter((word) => word.length > 2);

    const wordCount = words.length;

    const hasComplexWords = /(
      especificamente|
      adicionalmente|
      particularmente|
      consideravelmente|
      significativamente|
      simultaneamente|
      consequentemente|
      alternativamente|
      fundamentalmente|
      substantivamente
    )/ix.test(normalizedText);

    const sentenceCount =
      normalizedText
        .split(/[.!?]+/)
        .filter((sentence) => sentence.trim().length > 0)
        .length;

    const averageWordsPerSentence =
      wordCount / Math.max(sentenceCount, 1);

    if (
      averageWordsPerSentence > 12 ||
      hasComplexWords
    ) {
      this.languageLevel = 'advanced';
      return;
    }

    if (averageWordsPerSentence > 7) {
      this.languageLevel = 'medium';
      return;
    }

    this.languageLevel = 'simple';
  }

  // ==========================================================
  // RESET
  // ==========================================================

  resetState() {
    // Nunca deixa um timer antigo continuar activo
    // depois de reiniciar a sessão.
    this.cancelTimeout();

    const persistentState = {
      userId: this.userId,

      candidateName: this.candidateName,
      candidateEmail: this.candidateEmail,
      jobTitle: this.jobTitle,

      company: this.company,
      jobVacancy: this.jobVacancy,

      interviewId: this.interviewId,
      holdTransactionId: this.holdTransactionId,
      recruiterId: this.recruiterId,

      languageLevel: this.languageLevel,
    };

    this._initializeInterviewState();
    this._initializeConversationState();

    // Reinicia o relógio da nova sessão.
    this._initializeTimingState();

    // Mantém os dados que identificam a entrevista/candidato.
    this.userId = persistentState.userId;

    this.candidateName = persistentState.candidateName;
    this.candidateEmail = persistentState.candidateEmail;
    this.jobTitle = persistentState.jobTitle;

    this.company = persistentState.company;
    this.jobVacancy = persistentState.jobVacancy;

    this.interviewId = persistentState.interviewId;
    this.holdTransactionId =
      persistentState.holdTransactionId;
    this.recruiterId = persistentState.recruiterId;

    this.languageLevel =
      persistentState.languageLevel;

    return this;
  }
}

module.exports = InterviewSession;

