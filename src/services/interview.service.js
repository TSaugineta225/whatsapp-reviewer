// src/services/interview.service.js
const { STAGES, INTERVIEW_CONFIG, AGENTS, COMPANY, JOB_VACANCY } = require('../config/interview');
const BaseService = require('./base.service');
const AIService = require('./ai.service');
const CalendarService = require('./calendar.service');
const InterviewSession = require('../models/interview-session.model');
const YaneIntegrationService = require('./yane-integration.service');

// ============================================================
// HELPERS
// ============================================================

const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];
const CLEAN = (s) =>
  String(s || '')
    .replace(/^["'`]+|["'`]+$/g, '')
    .replace(/\s+\n/g, '\n')
    .trim();

const VALIDATION_PHRASES = [
  'Entendi.', 'Percebi.', 'Compreendo.', 'Já vi.', 'Certo.',
  'Ah, sim.', 'Claro.', 'Faz sentido.', 'Bom saber.', 'Interessante.',
  'Isso é útil.', 'Obrigado por partilhar.', 'Percebo o que diz.',
  'Compreendo perfeitamente.', 'Já fico com uma ideia.',
];

const SIMPLE_TONE = {
  simple: 'Usa palavras simples e frases curtas. Perguntas diretas. Evita jargão. Dá tempo para a pessoa responder.',
  medium: 'Usa linguagem equilibrada, profissional mas acessível. Frases claras.',
  advanced: 'Usa linguagem mais rica, com profundidade, mas sem ser técnico.',
};

const BANNED_OPENERS = [
  'entendo', 'entendi', 'percebo', 'percebi', 'compreendo', 'certo', 'ok', 'okay',
  'interessante', 'muito interessante', 'boa', 'bom', 'óptimo', 'otimo', 'ótimo',
  'perfeito', 'excelente', 'claro', 'exacto', 'exato', 'faz sentido', 'que bom',
  'obrigado pela resposta', 'obrigado por partilhar', 'agradeço a partilha',
];

const BANNED_RE = new RegExp(
  '^\\s*(' + BANNED_OPENERS.map((w) => w.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|') + ')\\b[\\s,.:;!—-]*',
  'i'
);

const ACK_FALLBACK = [
  'Fico com uma ideia clara do terreno onde trabalhou.',
  'Isso diz muito sobre a forma como lida com o dia a dia.',
  'Consigo imaginar bem essa situação.',
  'Há aí experiência de rua, não só de escritório.',
  'Nota-se que já passou por cenários pouco fáceis.',
  'Esse detalhe ajuda-me bastante.',
];

const STOPWORDS = new Set(
  ('a o as os um uma de do da dos das em no na nos nas e ou que se por para com sem sobre ' +
   'ao aos à às pelo pela isso isto esse essa este esta seu sua meu minha muito mais já ' +
   'lhe me te nos vos ser estar tem ter é são foi era como quando onde qual quais não sim')
    .split(' ')
);

const words = (s) =>
  String(s || '')
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z\s]/g, ' ')
    .split(/\s+/)
    .filter((w) => w.length > 3 && !STOPWORDS.has(w));

// ============================================================
// CLASSE PRINCIPAL
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

  getSession(userId) {
    return this.sessions.get(userId);
  }

  endSession(userId) {
    const session = this.sessions.get(userId);
    if (session) {
      session.cancelTimeout();
    }
    this.sessions.delete(userId);
    this.clearQuestionCollection(userId);
  }

  // ============================================================
  // INÍCIO DA ENTREVISTA (com botões)
  // ============================================================

  async startInterview(userId, jobTitle = null, company = null, jobVacancy = null) {
    try {
      const session = new InterviewSession(userId);
      session.jobTitle = jobTitle || JOB_VACANCY.title;
      session.company = company || COMPANY;
      session.jobVacancy = jobVacancy || JOB_VACANCY;

      this.resetInterviewState(session);
      this.sessions.set(userId, session);

      // Agendar timeout (4 horas)
      session.scheduleTimeout(async (expiredSession) => {
        const currentSession = this.sessions.get(userId);
        if (!currentSession || currentSession !== expiredSession) return;
        if (Date.now() - currentSession.lastInteraction < 5 * 60 * 1000) return;
        const message = this.generateTimeoutMessage(currentSession);
        await this.sendMessage(userId, message);
        this.endSession(userId);
        console.log(`[TIMEOUT] Sessão encerrada por inatividade: ${userId}`);
      });

      // ============================================================
      // ENVIAR CONVITE COM BOTÕES (em vez de texto simples)
      // ============================================================
      const welcomeMessage = this.generateWelcomeMessage(session);
      
      // Verificar se o serviço WhatsApp suporta botões
      const whatsappService = global.whatsappService;
      if (whatsappService && typeof whatsappService.sendInteractiveMessage === 'function') {
        // Enviar com botões interativos
        const companyName = session.getCompanyName();
        await whatsappService.sendInteractiveMessage(
          userId,
          `Entrevista - ${companyName}`,
          welcomeMessage,
          [
            { id: 'start_interview', text: '✅ Iniciar Entrevista' },
            { id: 'later', text: '⏰ Responder Depois' }
          ]
        );
        console.log(`[SESSAO] Convite com botões enviado para ${userId}`);
      } else {
        // Fallback: enviar como texto normal
        await this.sendMessage(userId, welcomeMessage);
      }

      return welcomeMessage;
    } catch (error) {
      throw this.handleError(error, 'Start Interview');
    }
  }

  generateWelcomeMessage(session) {
    const companyName = session.getCompanyName();
    const jobTitle = session.getJobTitle();
    const industry = session.company?.industry || COMPANY.industry;

    return `Olá! Aqui é do processo de selecção da ${companyName}. Fico contente por conversar consigo.

Estamos à procura de alguém para a posição de ${jobTitle}, na nossa equipa em Moçambique (${industry}).

Isto é uma conversa descontraída, sem respostas certas ou erradas. Vamos falar sobre a sua experiência e o que o motiva.

Para começar, pode dizer-me o seu nome completo?`;
  }

  generateTimeoutMessage(session) {
    const name = this.firstName(session) || 'candidato';
    return `Olá, ${name}. Notamos que não recebemos a sua resposta a tempo. Infelizmente, teremos que encerrar esta entrevista por enquanto.

Fique à vontade para se candidatar novamente no futuro.

Agradecemos o seu interesse.`;
  }

  async sendMessage(userId, text) {
    if (global.whatsappService) {
      try {
        await global.whatsappService.sendMessage(userId, text);
      } catch (e) {
        console.error('[TIMEOUT] Erro ao enviar mensagem:', e.message);
      }
    }
  }

  resetInterviewState(session) {
    session.stageIndex = 0;
    session.conversationHistory = session.conversationHistory || [];
    session.scores = session.scores || [];
    session.lastQuestion = null;
    session.inQASection = false;
    session.questionCounter = 0;
    session.askedQuestions = new Set();
    session.pendingCancel = false;
    session.offTopicAttempts = 0;
    session.topicsCovered = [];
    session.turnCount = 0;
    session.usedOpeners = session.usedOpeners || [];
    session.usedWords = session.usedWords || {};
    session.ackStreak = 0;
    session.qaOffered = false;
    session.shortAnswerStreak = 0;
    session.candidateEmail = null;
    session.languageLevel = 'simple';
  }

  // ============================================================
  // ENVIO HUMANIZADO
  // ============================================================

  async sendHuman(userId, text, client = global.whatsappService) {
    if (!text || !client) return;
    const bubbles = this.splitBubbles(text);
    for (const bubble of bubbles) {
      try {
        if (client && typeof client.sendPresenceUpdate === 'function') {
          await client.sendPresenceUpdate('composing', userId);
        }
      } catch (_) {}
      await this.delay(Math.min(bubble.length * this.typingMsPerChar, this.typingMax));
      await client.sendMessage(userId, bubble);
      await this.delay(300);
    }
  }

  splitBubbles(text) {
    const parts = String(text)
      .split(/\n{2,}/)
      .map((p) => p.trim())
      .filter(Boolean);
    const merged = [];
    for (const part of parts) {
      const last = merged[merged.length - 1];
      if (last && (last.length < 60 || part.length < 40)) {
        merged[merged.length - 1] = `${last}\n\n${part}`;
      } else {
        merged.push(part);
      }
    }
    if (merged.length <= 3) return merged;
    return [merged[0], merged.slice(1, -1).join('\n\n'), merged[merged.length - 1]];
  }

  delay(ms) {
    return new Promise((r) => setTimeout(r, ms));
  }

  // ============================================================
  // PROCESSAMENTO PRINCIPAL
  // ============================================================

  async handleResponse(userId, message) {
    const session = this.getSession(userId);
    if (!session) return null;

    const text = String(message || '').trim();

    // --- CANCELAMENTO ---
    if (this.isCancelIntent(text) && !session.pendingCancel) {
      session.pendingCancel = true;
      return `Sem problema, posso encerrar aqui. Só para confirmar: quer mesmo terminar a entrevista? O progresso não é recuperado depois.

Se sim, escreva SIM. Se preferir continuar, escreva CONTINUAR.`;
    }

    if (session.pendingCancel) {
      if (/^(sim|s|confirmo|quero)$/i.test(text)) {
        this.endSession(userId);
        return `Tudo bem, entrevista encerrada. Obrigado pelo seu interesse na ${session.getCompanyName()}.

Se mudar de ideias, é só escrever "OLÁ" e recomeçamos do início.`;
      }
      session.pendingCancel = false;
      return `Óptimo, vamos continuar então.

${session.lastQuestion || 'Conte-me um pouco mais sobre si.'}`;
    }

    // Resetar timeout a cada interação
    session.cancelTimeout();
    session.scheduleTimeout(async (expiredSession) => {
      const currentSession = this.sessions.get(userId);
      if (!currentSession || currentSession !== expiredSession) return;
      if (Date.now() - currentSession.lastInteraction < 5 * 60 * 1000) return;
      const msg = this.generateTimeoutMessage(currentSession);
      await this.sendMessage(userId, msg);
      this.endSession(userId);
      console.log(`[TIMEOUT] Sessão encerrada por inatividade: ${userId}`);
    });

    if (this.isSessionExpired(session)) {
      this.endSession(userId);
      return 'Ficámos um tempo sem falar e a sessão fechou. Quando quiser retomar, escreva "OLÁ" e começamos de novo.';
    }

    session.updateLastInteraction();
    session.turnCount = (session.turnCount || 0) + 1;

    // --- ETAPA 1: NOME ---
    if (!session.candidateName) {
      const name = this.normalizeName(text);
      if (!name) {
        return 'Não consegui perceber o nome. Pode escrever apenas o seu nome, por favor?';
      }
      session.candidateName = name;
      const savedName = session.candidateName;
      this.resetInterviewState(session);
      session.candidateName = savedName;

      session.updateLanguageLevel(text);

      const firstQuestion = await this.askFirstQuestion(session);
      session.lastQuestion = firstQuestion;
      session.askedQuestions.add(firstQuestion);

      return `Muito prazer, ${this.firstName(session)}. Vamos com calma.

${firstQuestion}`;
    }

    session.updateLanguageLevel(text);

    // --- PERGUNTAS DO CANDIDATO ---
    if (this.isCandidateQuestion(text)) {
      const reply = await this.answerCandidate(session, text);
      return `${reply}

${this.resumeLine(session)}`;
    }

    // --- RESPOSTAS MUITO CURTAS ---
    if (this.isTooShort(text)) {
      session.shortAnswerStreak = (session.shortAnswerStreak || 0) + 1;
      if (session.shortAnswerStreak < 2) {
        return this.varied(session, [
          `Conte-me como foi na prática, ${this.firstName(session)} — um caso concreto ajuda-me a ver melhor.`,
          'Dê-me o contexto à volta disso: quem estava envolvido?',
          'Puxe pela memória de uma situação real. Como correu?',
          'E na altura, o que fez a diferença? Descreva-me o momento.',
        ]);
      }
    } else {
      session.shortAnswerStreak = 0;
    }

    // --- TURNO PRINCIPAL ---
    const turn = await this.analyzeTurn(session, text);

    if (turn.off_topic) {
      session.offTopicAttempts = (session.offTopicAttempts || 0) + 1;
      if (session.offTopicAttempts >= 2) {
        return await this.advanceFocus(session);
      }
      return this.rephrase(session);
    }
    session.offTopicAttempts = 0;

    this.recordTurn(session, text, turn);

    if (turn.interview_complete) {
      return await this.concludeInterview(session);
    }

    session.questionCounter = (session.questionCounter || 0) + 1;

    if (turn.should_transition || session.questionCounter >= (INTERVIEW_CONFIG.questionsPerStage || 3)) {
      return await this.advanceFocus(session, turn.acknowledgement);
    }

    if (turn.next_question) {
      const question = this.dedupeQuestion(session, turn.next_question);
      session.lastQuestion = question;
      session.askedQuestions.add(question);
      return this.compose(session, turn.acknowledgement, question);
    }

    return await this.advanceFocus(session, turn.acknowledgement);
  }

  // ============================================================
  // ANÁLISE DO TURNO (IA)
  // ============================================================

  async analyzeTurn(session, answer) {
    const stage = this.stages[session.stageIndex];
    const agent = AGENTS[stage.agent] || AGENTS.recruiter;
    const tone = SIMPLE_TONE[session.languageLevel] || SIMPLE_TONE.simple;
    const jobTitle = session.getJobTitle();
    const companyName = session.getCompanyName();

    const historyText = session.conversationHistory
      .slice(-6)
      .map((m) => `${m.role === 'assistant' ? 'Recrutador' : 'Candidato'}: ${m.content}`)
      .join('\n');

    const covered = (session.topicsCovered || []).join('; ') || 'nenhum ainda';
    const asked = Array.from(session.askedQuestions || []).slice(-5).join(' | ');
    const overused = Object.entries(session.usedWords || {})
      .filter(([, n]) => n >= 2)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 15)
      .map(([w]) => w)
      .join(', ');

    const prompt = `És um recrutador moçambicano experiente, calmo e atencioso. A conversa deve ser natural e fluida, como uma conversa real.

Foco interno desta parte da conversa (NUNCA menciones isto ao candidato): "${stage.name}" — ${stage.objective}
Candidato: ${session.candidateName}
Empresa: ${companyName}
Vaga: ${jobTitle}
Nível de linguagem do candidato: ${session.languageLevel} (${tone})

Temas já cobertos: ${covered}
Perguntas já feitas (não repetir): ${asked || 'nenhuma'}

Histórico recente:
${historyText || '(início da etapa)'}

Última pergunta: ${session.lastQuestion}
Resposta do candidato: "${answer}"

Faz tudo isto de uma só vez e devolve JSON:

1. "acknowledgement": uma reacção humana curta (1 frase, máx. 15 palavras) que mostre que ouviste — refere um detalhe CONCRETO que a pessoa acabou de dizer. Pode ser vazio ("") se não houver nada de novo a comentar.
   PROIBIDO usar: entendo, entendi, percebo, percebi, compreendo, certo, ok, interessante, boa, óptimo, perfeito, excelente, claro, faz sentido, obrigado por partilhar.
   PROIBIDO reutilizar palavras já muito usadas: ${overused || '(nenhuma ainda)'}.
2. Avaliação: score 0-10 = clarity (0-4) + relevance (0-3) + depth (0-3). Sê justo: avalia o conteúdo e a experiência, nunca a forma.
3. "off_topic": true apenas se a resposta não tiver qualquer relação com a pergunta.
4. "generic": true se a resposta parecer decorada/genérica, sem exemplo concreto.
5. "emotion": uma palavra (entusiasmado, confiante, inseguro, neutro, frustrado...).
6. "topic": 2-4 palavras a resumir o tema desta resposta.
7. "next_question": a pergunta seguinte, natural, que nasce do que a pessoa acabou de dizer (usa as palavras dela), aberta. Se a resposta foi genérica, pede um exemplo concreto. Máx. 2 frases. null se for altura de mudar de etapa.
8. "should_transition": true se já tens material suficiente nesta etapa.
9. "interview_complete": true apenas se esta for a última etapa e já houver material suficiente.
10. "feedback": nota interna curta (máx. 20 palavras), não mostrada ao candidato.

Regras de voz: sem emojis; sem elogios automáticos; sem repetir palavras que já usaste; uma pergunta de cada vez; nunca dizer ao candidato que está a ser avaliado, pontuado, ou que mudou de fase.

Responde SÓ com JSON válido:
{"acknowledgement":"","score":0,"clarity":0,"relevance":0,"depth":0,"off_topic":false,"generic":false,"emotion":"","topic":"","next_question":null,"should_transition":false,"interview_complete":false,"feedback":""}`;

    try {
      const raw = await this.aiService.generateResponse(prompt, agent.systemPrompt);
      const match = String(raw).match(/\{[\s\S]*\}/);
      if (!match) throw new Error('sem JSON');
      const parsed = JSON.parse(match[0]);
      return this.normalizeTurn(parsed);
    } catch (error) {
      console.error('[interview] falha na análise do turno:', error.message);
      return {
        acknowledgement: this.varied(session, ACK_FALLBACK),
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
        feedback: 'Avaliação automática indisponível.',
      };
    }
  }

  normalizeTurn(t) {
    const num = (v, max) => Math.max(0, Math.min(max, Number(v) || 0));
    return {
      acknowledgement: CLEAN(t.acknowledgement),
      score: num(t.score, 10),
      clarity: num(t.clarity, 4),
      relevance: num(t.relevance, 3),
      depth: num(t.depth, 3),
      off_topic: !!t.off_topic,
      generic: !!t.generic,
      emotion: CLEAN(t.emotion).toLowerCase() || 'neutro',
      topic: CLEAN(t.topic),
      next_question: t.next_question ? CLEAN(t.next_question) : null,
      should_transition: !!t.should_transition,
      interview_complete: !!t.interview_complete,
      feedback: CLEAN(t.feedback),
    };
  }

  recordTurn(session, answer, turn) {
    session.scores.push({
      score: turn.generic ? Math.max(0, turn.score - 0.5) : turn.score,
      clarity: turn.clarity,
      relevance: turn.relevance,
      depth: turn.depth,
      feedback: turn.feedback,
      question: session.lastQuestion,
      answer,
      emotion: turn.emotion,
      artificial: turn.generic,
    });

    if (turn.topic) session.topicsCovered.push(turn.topic);
    session.conversationHistory.push({ role: 'assistant', content: session.lastQuestion });
    session.conversationHistory.push({ role: 'user', content: answer });
  }

  // ============================================================
  // PREVENÇÃO DE REPETIÇÃO
  // ============================================================

  openerKey(text) {
    return words(text).slice(0, 2).join(' ');
  }

  registerLanguage(session, text) {
    session.usedWords = session.usedWords || {};
    for (const w of words(text)) {
      session.usedWords[w] = (session.usedWords[w] || 0) + 1;
    }
    const key = this.openerKey(text);
    if (key) {
      session.usedOpeners = session.usedOpeners || [];
      session.usedOpeners.push(key);
    }
  }

  cleanAck(session, ack) {
    let a = CLEAN(ack);
    if (!a) return '';
    let guard = 0;
    let trimmed = false;
    while (BANNED_RE.test(a) && guard++ < 3) {
      a = CLEAN(a.replace(BANNED_RE, ''));
      trimmed = true;
    }
    if (!a) return '';
    if (trimmed && (a.length < 30 || /^(que|o que|isso|isto|e |mas |faz|para mim)\b/i.test(a))) return '';
    a = a.charAt(0).toUpperCase() + a.slice(1);

    const key = this.openerKey(a);
    if (key && (session.usedOpeners || []).includes(key)) return '';

    const heavy = words(a).some((w) => (session.usedWords || {})[w] >= 3);
    if (heavy) return '';

    return a;
  }

  varied(session, options) {
    const fresh = options.filter((o) => !(session.usedOpeners || []).includes(this.openerKey(o)));
    const chosen = pick(fresh.length ? fresh : options);
    this.registerLanguage(session, chosen);
    return chosen;
  }

  compose(session, ack, question) {
    const q = CLEAN(question);
    let reaction = this.cleanAck(session, ack);

    if (reaction && (session.ackStreak || 0) >= 1) reaction = '';

    if (!reaction) {
      session.ackStreak = 0;
      this.registerLanguage(session, q);
      return q;
    }

    session.ackStreak = (session.ackStreak || 0) + 1;
    this.registerLanguage(session, reaction);
    this.registerLanguage(session, q);

    const validation = pick(VALIDATION_PHRASES);
    return `${validation} ${reaction}\n\n${q}`;
  }

  // ============================================================
  // PRIMEIRA PERGUNTA DA ETAPA
  // ============================================================

  async askFirstQuestion(session) {
    const stage = this.stages[session.stageIndex];
    const agent = AGENTS[stage.agent] || AGENTS.recruiter;
    const tone = SIMPLE_TONE[session.languageLevel] || SIMPLE_TONE.simple;
    const jobTitle = session.getJobTitle();

    const prompt = `Vais iniciar a conversa com ${session.candidateName}, candidato à vaga de ${jobTitle} em Moçambique.
Foco interno (não mencionar): ${stage.objective}.

${tone}

Escreve UMA pergunta de abertura, aberta e natural, como quem começa uma conversa real. Deve convidar a pessoa a contar uma história concreta. Proibido: "fale-me um pouco sobre si", falar em etapas, fases ou avaliação. Máx. 2 frases. Sem emojis.
Responde apenas com a pergunta.`;

    try {
      const question = await this.aiService.generateResponse(prompt, agent.systemPrompt);
      return CLEAN(question) || this.fallbackQuestion(stage);
    } catch (error) {
      console.error('[interview] falha na primeira pergunta:', error.message);
      return this.fallbackQuestion(stage);
    }
  }

  fallbackQuestion(stage) {
    const focus = (stage && stage.objective) || 'o seu percurso';
    return `Conte-me uma situação recente do seu trabalho que tenha a ver com ${String(focus).toLowerCase()}.`;
  }

  // ============================================================
  // MUDANÇA DE FOCO (TRANSIÇÃO)
  // ============================================================

  async advanceFocus(session, acknowledgement) {
    if (session.stageIndex >= this.stages.length - 1) {
      return await this.concludeInterview(session);
    }

    const previous = this.stages[session.stageIndex];
    session.stageIndex++;
    session.questionCounter = 0;
    session.offTopicAttempts = 0;

    const nextStage = this.stages[session.stageIndex];
    const agent = AGENTS[nextStage.agent] || AGENTS.recruiter;
    const tone = SIMPLE_TONE[session.languageLevel] || SIMPLE_TONE.simple;

    const said = session.conversationHistory
      .filter((m) => m.role === 'user')
      .slice(-2)
      .map((m) => m.content)
      .join(' ');

    const overused = Object.entries(session.usedWords || {})
      .filter(([, n]) => n >= 2)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 15)
      .map(([w]) => w)
      .join(', ');

    let question;
    try {
      question = CLEAN(
        await this.aiService.generateResponse(
          `Conversa por WhatsApp com ${session.candidateName}, candidato a ${session.getJobTitle()} em Moçambique.

O que ele disse há pouco: "${said}"
Tema que estávamos a explorar (interno): ${previous.objective}
Tema que quero explorar agora (interno, NUNCA mencionar): ${nextStage.objective}

Escreve UMA pergunta que faça a ponte de forma completamente natural: parte de um detalhe concreto do que ele disse e desliza para o novo tema, como acontece numa conversa real.
Proibido: anunciar mudança de assunto, falar em etapas/fases/avaliação, usar "agora vamos falar de", "passando a", "mudando de assunto".
Proibido repetir estas palavras já gastas: ${overused || '(nenhuma)'}.
${tone} Máx. 2 frases. Sem emojis. Responde apenas com a pergunta.`,
          agent.systemPrompt,
        ),
      );
    } catch (_) {
      question = '';
    }

    if (!question) question = this.fallbackQuestion(nextStage);

    session.lastQuestion = question;
    session.askedQuestions.add(question);

    let out = this.compose(session, acknowledgement, question);

    if (!session.qaOffered) {
      session.qaOffered = true;
      out += `\n\n(E se lhe surgir alguma dúvida sobre a empresa ou a vaga, pode perguntar a qualquer momento.)`;
    }

    return out;
  }

  // ============================================================
  // PERGUNTAS DO CANDIDATO (Q&A)
  // ============================================================

  isCandidateQuestion(text) {
    const t = String(text || '').trim();
    if (!t.endsWith('?')) return false;
    return /(voc[êe]s|empresa|vaga|sal[áa]rio|hor[áa]rio|equipa|contrato|remunera|benef[íi]cio|processo|quando|onde fica|como funciona|quanto)/i.test(t);
  }

  async answerCandidate(session, question) {
    return await this.generateBatchAnswers([question], session);
  }

  async generateBatchAnswers(questions, session) {
    try {
      const questionsText = questions.map((q, i) => `${i + 1}. ${q}`).join('\n');
      const tone = SIMPLE_TONE[session.languageLevel] || SIMPLE_TONE.simple;
      const company = session.company || COMPANY;
      const jobVacancy = session.jobVacancy || JOB_VACANCY;

      const prompt = `És o recrutador desta entrevista. Responde às perguntas do candidato em português de Moçambique, de forma directa e humana.

Perguntas:
${questionsText}

Empresa: ${JSON.stringify(company)}
Vaga: ${JSON.stringify(jobVacancy)}

${tone}

Regras:
- Vai directo ao assunto, sem saudação longa nem preâmbulos.
- Responde a cada pergunta em 1 a 3 frases, com informação real da empresa/vaga.
- Se não souberes algo, diz honestamente que confirmas com a equipa.
- Salário e condições pessoais: encaminha com simpatia para a conversa com o RH.
- Sem emojis.`;

      const response = await this.aiService.generateResponse(prompt, AGENTS.recruiter.systemPrompt);
      return CLEAN(response);
    } catch (error) {
      console.error('[interview] erro no Q&A:', error.message);
      return 'Essa não lhe sei responder de cabeça com rigor — confirmo com a equipa e digo-lhe.';
    }
  }

  // ============================================================
  // DETECÇÃO DE OFF-TOPIC
  // ============================================================

  async detectOffTopic(session, message) {
    const lastQuestion = session.lastQuestion || '';
    const prompt = `A pergunta feita foi: "${lastQuestion}". A resposta do candidato foi: "${message}".
Essa resposta é totalmente fora de tópico (não responde à pergunta de forma alguma) ou está relacionada?
Responda apenas "SIM" se for totalmente fora de tópico, ou "NÃO" se for relacionada.`;
    const answer = await this.aiService.generateResponse(prompt, AGENTS.recruiter.systemPrompt);
    return answer.toLowerCase().trim().startsWith('sim');
  }

  handleOffTopic(session) {
    const q = session.lastQuestion || 'Conte-me um pouco mais sobre a sua experiência.';
    const lead = this.varied(session, [
      'Deixe-me perguntar de outra forma:',
      'Voltando à pergunta:',
      'O que eu queria saber é:',
    ]);
    return `${lead} ${q}`;
  }

  dedupeQuestion(session, question) {
    const asked = Array.from(session.askedQuestions || []);
    if (!asked.includes(question)) return question;
    return `Deixe-me perguntar de outra forma: ${question}`;
  }

  rephrase(session) {
    const q = session.lastQuestion || 'Conte-me um pouco mais sobre a sua experiência.';
    const lead = this.varied(session, [
      'Deixe-me perguntar de outra forma:',
      'Voltando à pergunta:',
      'O que eu queria saber é:',
    ]);
    return `${lead} ${q}`;
  }

  resumeLine(session) {
    const q = session.lastQuestion || 'Conte-me um pouco mais sobre o seu percurso.';
    const lead = this.varied(session, [
      'Voltando a si:',
      'Continuando de onde estávamos:',
      'E diga-me:',
    ]);
    return `${lead} ${q}`;
  }

  // ============================================================
  // CONCLUSÃO
  // ============================================================

  async concludeInterview(session) {
    const averageScore = this.calculateFinalScore(session);
    const result = this.determineResult(averageScore);
    const summary = await this.generatePersonalizedSummary(session);

    let finalMessage = `Chegámos ao fim, ${this.firstName(session)}. Obrigado pelo tempo e pela conversa.

${summary}`;

    const genericCount = session.scores.filter((s) => s.artificial).length;
    if (genericCount >= 2) {
      finalMessage += `\n\nUma dica: em próximas conversas, traga exemplos concretos do que já fez. Histórias reais valem mais do que respostas genéricas.`;
    }

    // Enviar resultados para a Yane
    try {
      await this.yaneIntegration.sendInterviewResult({
        phone: session.userId,
        candidateName: session.candidateName,
        candidateEmail: session.candidateEmail || 'nao_informado@exemplo.com',
        jobTitle: session.getJobTitle(),
        score: averageScore,
        feedback: result,
        recommendation: averageScore >= 6 ? 'approve' : 'hold',
        transcript: session.conversationHistory,
        scoresBreakdown: session.scores,
        interviewId: session.interviewId || null,
        holdTransactionId: session.holdTransactionId || null
      });
    } catch (error) {
      console.error('[YANE] Erro ao enviar resultados:', error.message);
    }

    if (averageScore >= INTERVIEW_CONFIG.minScoreToPass) {
      try {
        const meetingLink = await this.calendarService.scheduleInterview(
          session.candidateEmail || 'nao_informado@exemplo.com',
          session.candidateName,
        );
        finalMessage += `\n\nBoas notícias: passa à fase seguinte. Enviei os detalhes para ${session.candidateEmail || 'o seu contacto'}.\nLink da reunião: ${meetingLink}`;
      } catch (error) {
        console.error('[interview] falha ao agendar:', error.message);
        finalMessage += `\n\nBoas notícias: passa à fase seguinte. A nossa equipa de RH entra em contacto em até 2 dias úteis.`;
      }
    } else {
      finalMessage += `\n\nNeste momento o perfil não encaixa totalmente no que a vaga exige, mas gostámos de o conhecer. Ficamos com a sua candidatura para futuras oportunidades.`;
    }

    finalMessage += `\n\nFoi um gosto conversar consigo. Muita sorte no caminho.`;

    console.log('[interview] resultado', {
      candidato: session.candidateName,
      media: Number(averageScore.toFixed(1)),
      classificacao: result,
      respostas: session.scores.length,
      languageLevel: session.languageLevel,
    });

    this.endSession(session.userId);
    return finalMessage;
  }

  async generatePersonalizedSummary(session) {
    const history = session.scores
      .map((s) => `P: ${s.question}\nR: ${s.answer}`)
      .join('\n');

    try {
      const summary = await this.aiService.generateResponse(
        `Com base nesta conversa, escreve 3 frases calorosas e pessoais dirigidas ao candidato, destacando pontos fortes CONCRETOS que ele demonstrou (usa o que ele realmente disse). Sem emojis, sem notas, sem clichés.

${history}`,
        AGENTS.recruiter.systemPrompt,
      );
      return CLEAN(summary);
    } catch (error) {
      console.error('[interview] erro no resumo:', error.message);
      return 'Gostei de ouvir a sua experiência e a forma como explicou o seu percurso.';
    }
  }

  // ============================================================
  // UTILITÁRIOS
  // ============================================================

  firstName(session) {
    return String(session.candidateName || '').split(/\s+/)[0] || 'colega';
  }

  isCancelIntent(text) {
    const t = text.toLowerCase().trim();
    if (t.length > 60) return false;
    return /(quero (cancelar|sair|desistir|parar))|^(cancelar|desistir|sair|encerrar|abortar)$|n[ãa]o quero continuar|desisto/i.test(t);
  }

  isTooShort(text) {
    const words = text.split(/\s+/).filter(Boolean);
    return words.length <= 3 && !/\?/.test(text);
  }

  normalizeName(text) {
    const cleaned = String(text)
      .replace(/^(o\s+meu\s+nome\s+[ée]|me\s+chamo|chamo-me|sou\s+o|sou\s+a|sou)\s+/i, '')
      .replace(/[^\p{L}\s'-]/gu, '')
      .trim();
    if (cleaned.length < 2 || cleaned.length > 60) return null;
    return cleaned
      .split(/\s+/)
      .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
      .join(' ');
  }

  calculateFinalScore(session) {
    if (!session.scores.length) return 0;
    return session.scores.reduce((acc, s) => acc + s.score, 0) / session.scores.length;
  }

  determineResult(averageScore) {
    if (averageScore >= 8) return 'Excelente';
    if (averageScore >= 6) return 'Bom';
    return 'A desenvolver';
  }

  isSessionExpired(session) {
    const timeoutMs = (INTERVIEW_CONFIG.timeoutMinutes || 5) * 60 * 1000;
    return Date.now() - session.lastInteraction > timeoutMs;
  }

  isValidEmail(email) {
    return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(String(email).trim());
  }

  clearQuestionCollection(userId) {
    this.questionCollector.delete(userId);
    if (this.collectorTimers.has(userId)) {
      clearTimeout(this.collectorTimers.get(userId));
      this.collectorTimers.delete(userId);
    }
  }
}

module.exports = InterviewService;