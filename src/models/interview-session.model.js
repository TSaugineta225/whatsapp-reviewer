// src/models/interview-session.model.js
const { INTERVIEW_CONFIG } = require('../config');

class InterviewSession {
  constructor(userId) {
    this.userId = userId;
    
    this.candidateName = null;
    this.candidateEmail = null;
    this.jobTitle = null;
    
    this.company = null;
    this.jobVacancy = null;
    
    this.stageIndex = 0;
    this.stage = 'initial';
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
    
    this.scores = [];
    this.conversationHistory = [];
    
    this.usedOpeners = [];
    this.usedWords = {};
    this.ackStreak = 0;
    this.turnCount = 0;
    
    this.startTime = Date.now();
    this.lastInteraction = Date.now();
    
    this.timeoutTimer = null;
    this.timeoutDuration = 4 * 60 * 60 * 1000;
    this.timeoutWarningSent = false;
    this.timeoutWarningTime = null;
    
    this.interviewId = null;
    this.holdTransactionId = null;
    this.recruiterId = null;
    
    this.languageLevel = 'simple';
  }

  updateLastInteraction() {
    this.lastInteraction = Date.now();
  }

  isExpired() {
    const timeoutMs = (INTERVIEW_CONFIG.timeoutMinutes || 5) * 60 * 1000;
    return Date.now() - this.lastInteraction > timeoutMs;
  }

  getDuration() {
    return Math.floor((Date.now() - this.startTime) / (60 * 1000));
  }

  scheduleTimeout(callback) {
    this.cancelTimeout();
    this.timeoutTimer = setTimeout(() => {
      callback(this);
    }, this.timeoutDuration);
  }

  cancelTimeout() {
    if (this.timeoutTimer) {
      clearTimeout(this.timeoutTimer);
      this.timeoutTimer = null;
    }
  }

  resetTimeout(callback) {
    this.cancelTimeout();
    this.scheduleTimeout(callback);
  }

  hasBasicInfo() {
    return this.candidateEmail !== null && this.candidateName !== null;
  }

  getCandidateInfo() {
    return {
      email: this.candidateEmail,
      name: this.candidateName,
      whatsappId: this.userId,
    };
  }

  getCompanyName() {
    return this.company?.name || "Empresa";
  }

  getJobTitle() {
    return this.jobVacancy?.title || this.jobTitle || "Vaga";
  }

  getProgress() {
    return {
      stage: this.stage,
      stageIndex: this.stageIndex,
      questionNumber: this.currentQuestion + 1,
      totalQuestions: this.askedQuestions.size,
      scores: this.scores,
      duration: this.getDuration(),
      inQASection: this.inQASection,
    };
  }

  getAverageScore() {
    if (this.scores.length === 0) return 0;
    const sum = this.scores.reduce((acc, s) => acc + (s.score || 0), 0);
    return sum / this.scores.length;
  }

  getLastQuestion() {
    return this.lastQuestion;
  }

  setLastQuestion(question) {
    this.lastQuestion = question;
  }

  addToHistory(role, content) {
    this.conversationHistory.push({ role, content });
  }

  getRecentHistory(n = 6) {
    return this.conversationHistory.slice(-n);
  }

  addScore(score) {
    this.scores.push({
      score: score.score || 0,
      clarity: score.clarity || 0,
      relevance: score.relevance || 0,
      depth: score.depth || 0,
      feedback: score.feedback || '',
      question: score.question || '',
      answer: score.answer || '',
      emotion: score.emotion || 'neutro',
      artificial: score.artificial || false,
    });
  }

  registerWords(text) {
    const words = String(text || '')
      .toLowerCase()
      .replace(/[^a-z\s]/g, ' ')
      .split(/\s+/)
      .filter(w => w.length > 3);
    
    for (const w of words) {
      this.usedWords[w] = (this.usedWords[w] || 0) + 1;
    }
  }

  isWordOverused(word, threshold = 3) {
    return (this.usedWords[word] || 0) >= threshold;
  }

  updateLanguageLevel(text) {
    const wordCount = text.split(/\s+/).filter(w => w.length > 2).length;
    const hasComplexWords = /(especificamente|adicionalmente|particularmente|consideravelmente|significativamente|simultaneamente|consequentemente|alternativamente|fundamentalmente|substantivamente)/i.test(text);
    const sentenceCount = text.split(/[.!?]+/).filter(s => s.trim().length > 0).length;
    const avgWordsPerSentence = wordCount / (sentenceCount || 1);

    if (avgWordsPerSentence > 12 || hasComplexWords) {
      this.languageLevel = 'advanced';
    } else if (avgWordsPerSentence > 7) {
      this.languageLevel = 'medium';
    } else {
      this.languageLevel = 'simple';
    }
  }

  resetState() {
    const savedName = this.candidateName;
    const savedEmail = this.candidateEmail;
    const savedJobTitle = this.jobTitle;
    const savedUserId = this.userId;
    const savedCompany = this.company;
    const savedJobVacancy = this.jobVacancy;
    const savedInterviewId = this.interviewId;
    const savedHoldTransactionId = this.holdTransactionId;
    const savedRecruiterId = this.recruiterId;
    const savedLanguageLevel = this.languageLevel;

    this.stageIndex = 0;
    this.stage = 'initial';
    this.currentQuestion = 0;
    this.questionCounter = 0;
    this.lastQuestion = null;
    this.askedQuestions = new Set();
    this.inQASection = false;
    this.followUpPending = false;
    this.pendingCancel = false;
    this.offTopicAttempts = 0;
    this.shortAnswerStreak = 0;
    this.qaOffered = false;
    this.scores = [];
    this.conversationHistory = [];
    this.topicsCovered = [];
    this.usedOpeners = [];
    this.usedWords = {};
    this.ackStreak = 0;
    this.turnCount = 0;
    this.timeoutWarningSent = false;
    this.timeoutWarningTime = null;

    this.candidateName = savedName;
    this.candidateEmail = savedEmail;
    this.jobTitle = savedJobTitle;
    this.userId = savedUserId;
    this.company = savedCompany;
    this.jobVacancy = savedJobVacancy;
    this.interviewId = savedInterviewId;
    this.holdTransactionId = savedHoldTransactionId;
    this.recruiterId = savedRecruiterId;
    this.languageLevel = savedLanguageLevel;
  }
}

module.exports = InterviewSession;