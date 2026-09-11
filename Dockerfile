# Dockerfile

# ============================================================
# STAGE 1 — dependências de produção
# ============================================================
FROM node:20-alpine AS deps

WORKDIR /app

# Instalar apenas prod deps
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev --no-audit --no-fund && \
    npm cache clean --force

# ============================================================
# STAGE 2 — runtime
# ============================================================
FROM node:20-alpine AS runtime

# tini para reencaminhamento correcto de sinais (PID 1).
# curl para o healthcheck.
RUN apk add --no-cache tini curl

# Utilizador não-root
RUN addgroup -g 1001 -S nodejs && \
    adduser -S -u 1001 -G nodejs nodejs

WORKDIR /app

# Copiar deps do stage anterior
COPY --from=deps /app/node_modules ./node_modules

# Copiar código com owner correcto
COPY --chown=nodejs:nodejs . .

# Diretórios persistentes (serão volumes)
RUN mkdir -p /app/auth_info /app/logs && \
    chown -R nodejs:nodejs /app/auth_info /app/logs

USER nodejs

# Variáveis base
ENV NODE_ENV=production \
    PORT=3000 \
    NODE_OPTIONS="--max-old-space-size=768"

EXPOSE 3000

# Healthcheck nativo
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
  CMD curl -fsS http://localhost:3000/health || exit 1

# tini como PID 1
ENTRYPOINT ["/sbin/tini", "--"]

CMD ["node", "server.js"]