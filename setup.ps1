<#
.SYNOPSIS
    Setup completo do Yane Bot no Windows (Docker Desktop).
.DESCRIPTION
    - Valida pré-requisitos (Docker Desktop + Compose v2)
    - Cria estrutura de pastas
    - Gera .env a partir de .env.example
    - Gera package-lock.json (obrigatório para `npm ci` no Dockerfile)
    - Valida docker-compose + Caddyfile
    - Faz build e sobe tudo
.NOTES
    Executar na raiz do projecto, numa PowerShell como Administrador.
#>

#Requires -RunAsAdministrator

$ErrorActionPreference = 'Stop'
Set-Location -Path $PSScriptRoot

Write-Host "`n=== Yane Bot — Setup Windows ===" -ForegroundColor Cyan

# ─────────────────────────────────────────────────────────────
# 1. Pré-requisitos
# ─────────────────────────────────────────────────────────────
Write-Host "`n[1/7] A verificar pré-requisitos..." -ForegroundColor Yellow

try {
    $dockerVersion = docker --version
    Write-Host "  ✓ $dockerVersion" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Docker não encontrado. Instala Docker Desktop: https://docs.docker.com/desktop/install/windows-install/" -ForegroundColor Red
    exit 1
}

try {
    $composeVersion = docker compose version
    Write-Host "  ✓ $composeVersion" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Docker Compose v2 não encontrado. Vem com o Docker Desktop." -ForegroundColor Red
    exit 1
}

# Verificar daemon activo
try {
    docker info 2>&1 | Out-Null
    Write-Host "  ✓ Docker daemon activo" -ForegroundColor Green
} catch {
    Write-Host "  ✗ Docker daemon não está a correr. Abre o Docker Desktop e espera ficar verde." -ForegroundColor Red
    exit 1
}

try {
    $nodeVersion = node --version
    Write-Host "  ✓ Node $nodeVersion (para gerar lockfile local)" -ForegroundColor Green
} catch {
    Write-Host "  ⚠ Node.js não encontrado no PATH." -ForegroundColor Yellow
    Write-Host "    Necessário para gerar package-lock.json. Instala: https://nodejs.org/" -ForegroundColor Yellow
    $continue = Read-Host "    Continuar sem gerar lockfile? (s/n)"
    if ($continue -ne 's') { exit 1 }
}

# ─────────────────────────────────────────────────────────────
# 2. Estrutura de pastas
# ─────────────────────────────────────────────────────────────
Write-Host "`n[2/7] A criar estrutura de pastas..." -ForegroundColor Yellow

$folders = @(
    ".\auth_info",
    ".\logs\bot",
    ".\logs\caddy",
    ".\monitoring\prometheus",
    ".\monitoring\alertmanager",
    ".\monitoring\grafana\provisioning\datasources",
    ".\monitoring\grafana\provisioning\dashboards",
    ".\monitoring\grafana\dashboards",
    ".\src"
)

foreach ($f in $folders) {
    if (-not (Test-Path $f)) {
        New-Item -ItemType Directory -Force -Path $f | Out-Null
        Write-Host "  + $f" -ForegroundColor Gray
    } else {
        Write-Host "  = $f (já existe)" -ForegroundColor DarkGray
    }
}

# ─────────────────────────────────────────────────────────────
# 3. .env
# ─────────────────────────────────────────────────────────────
Write-Host "`n[3/7] A preparar .env..." -ForegroundColor Yellow

if (-not (Test-Path ".env.example")) {
    Write-Host "  ✗ .env.example não existe. Cria primeiro esse ficheiro." -ForegroundColor Red
    exit 1
}

if (Test-Path ".env") {
    Write-Host "  = .env já existe (não sobrescrevo)" -ForegroundColor DarkGray
} else {
    Copy-Item ".env.example" ".env"
    Write-Host "  + .env criado a partir de .env.example" -ForegroundColor Green
    Write-Host "  ⚠ EDITA .env e preenche REDIS_PASSWORD, YANE_API_URL," -ForegroundColor Yellow
    Write-Host "    YANE_SERVICE_TOKEN e GRAFANA_ADMIN_PASSWORD antes de continuar." -ForegroundColor Yellow

    $editNow = Read-Host "  Abrir .env no Notepad agora? (s/n)"
    if ($editNow -eq 's') {
        notepad .env
    }

    Read-Host "  Pressiona ENTER quando tiveres o .env pronto"
}

# ─────────────────────────────────────────────────────────────
# 4. package-lock.json
# ─────────────────────────────────────────────────────────────
Write-Host "`n[4/7] A preparar package-lock.json..." -ForegroundColor Yellow

if (-not (Test-Path "package-lock.json")) {
    try {
        Write-Host "  A correr 'npm install' (pode demorar 1-2 min)..." -ForegroundColor Gray
        npm install --no-audit --no-fund
        Write-Host "  + package-lock.json gerado" -ForegroundColor Green
    } catch {
        Write-Host "  ⚠ Falha ao gerar lockfile. O Dockerfile usa `npm ci` e vai falhar." -ForegroundColor Yellow
    }
} else {
    Write-Host "  = package-lock.json já existe" -ForegroundColor DarkGray
}

# ─────────────────────────────────────────────────────────────
# 5. Validar configs
# ─────────────────────────────────────────────────────────────
Write-Host "`n[5/7] A validar configurações..." -ForegroundColor Yellow

# docker-compose.yml
Write-Host "  → docker compose config" -ForegroundColor Gray
docker compose config 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  ✓ docker-compose.yml válido" -ForegroundColor Green
} else {
    Write-Host "  ✗ docker-compose.yml tem erros:" -ForegroundColor Red
    docker compose config
    exit 1
}

# Caddyfile
Write-Host "  → caddy validate" -ForegroundColor Gray
$caddyPath = (Resolve-Path ".\Caddyfile").Path -replace '\\','/'
docker run --rm `
    -v "${caddyPath}:/etc/caddy/Caddyfile:ro" `
    caddy:2-alpine caddy validate --config /etc/caddy/Caddyfile 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "  ✓ Caddyfile válido" -ForegroundColor Green
} else {
    Write-Host "  ✗ Caddyfile tem erros:" -ForegroundColor Red
    docker run --rm -v "${caddyPath}:/etc/caddy/Caddyfile:ro" `
        caddy:2-alpine caddy validate --config /etc/caddy/Caddyfile
    exit 1
}

# ─────────────────────────────────────────────────────────────
# 6. Build
# ─────────────────────────────────────────────────────────────
Write-Host "`n[6/7] A fazer build da imagem do bot..." -ForegroundColor Yellow
Write-Host "  (primeira vez: 2-5 min)" -ForegroundColor Gray

docker compose build --no-cache bot
if ($LASTEXITCODE -ne 0) {
    Write-Host "  ✗ Build falhou." -ForegroundColor Red
    exit 1
}
Write-Host "  ✓ Imagem construída" -ForegroundColor Green

# ─────────────────────────────────────────────────────────────
# 7. Subir
# ─────────────────────────────────────────────────────────────
Write-Host "`n[7/7] A subir containers..." -ForegroundColor Yellow

docker compose up -d
if ($LASTEXITCODE -ne 0) {
    Write-Host "  ✗ Falha no `up`." -ForegroundColor Red
    exit 1
}

Write-Host "`n  A aguardar 20s para os serviços arrancarem..." -ForegroundColor Gray
Start-Sleep -Seconds 20

docker compose ps

# ─────────────────────────────────────────────────────────────
# Resumo
# ─────────────────────────────────────────────────────────────
Write-Host "`n=== Setup concluído ===" -ForegroundColor Cyan

Write-Host "`nPróximos passos:" -ForegroundColor White
Write-Host "  1. Ver logs do bot (QR code aparece aqui):" -ForegroundColor Gray
Write-Host "       docker compose logs -f bot" -ForegroundColor White
Write-Host "  2. Testar healthcheck local:" -ForegroundColor Gray
Write-Host "       curl.exe http://127.0.0.1:3000/health" -ForegroundColor White
Write-Host "  3. Abrir Grafana:" -ForegroundColor Gray
Write-Host "       http://127.0.0.1:3001" -ForegroundColor White
Write-Host "  4. Ver estado geral:" -ForegroundColor Gray
Write-Host "       docker compose ps" -ForegroundColor White