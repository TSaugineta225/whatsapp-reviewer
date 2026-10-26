# syntax=docker/dockerfile:1.6

# ============================================================
# STAGE 1 — dependências de produção
# ============================================================
FROM node:20.18-alpine AS deps

WORKDIR /app

COPY package.json package-lock.json* ./
RUN --mount=type=cache,target=/root/.npm \
    npm ci --omit=dev --no-audit --no-fund

# ============================================================
# STAGE 2 — runtime
# ============================================================
FROM node:20.18-alpine AS runtime

# tini       → PID 1 correto
# curl       → healthcheck (remova junto com o HEALTHCHECK se quiser mínima)
RUN apk add --no-cache tini curl

RUN addgroup -g 1001 -S nodejs && \
    adduser  -S -u 1001 -G nodejs nodejs

WORKDIR /app

COPY --from=deps --chown=nodejs:nodejs /app/node_modules ./node_modules
COPY --chown=nodejs:nodejs . .

RUN mkdir -p /app/auth_info /app/logs && \
    chown -R nodejs:nodejs /app/auth_info /app/logs

USER nodejs

ENV NODE_ENV=production \
    PORT=3000 \
    NODE_OPTIONS="--max-old-space-size=640"

EXPOSE 3000

# Usado apenas em `docker run` puro; em compose, o healthcheck do compose prevalece.
HEALTHCHECK --interval=30s --timeout=5s --start-period=90s --retries=3 \
  CMD curl -fsS http://localhost:3000/health || exit 1

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["node", "server.js"]