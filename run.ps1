# V-Lake: Full deployment for Windows
# Usage: cd vlake-final; powershell -ExecutionPolicy Bypass -File run.ps1

$ErrorActionPreference = "Continue"

Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  V-LAKE: Full Stack Deployment" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""

# [0] Check Docker
Write-Host "[0/5] Checking Docker..." -ForegroundColor Yellow
$dv = docker version --format "{{.Server.Version}}" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "  ERROR: Docker not running. Start Docker Desktop first." -ForegroundColor Red
    exit 1
}
Write-Host "  Docker $dv" -ForegroundColor Green

# [1] Besu uses dev mode - no key/genesis setup needed
Write-Host "[1/5] Besu dev mode - no setup needed" -ForegroundColor Green

# [2] Build + Start
Write-Host "[2/5] Building and starting Docker..." -ForegroundColor Yellow
docker compose build --no-cache 2>&1
if ($LASTEXITCODE -ne 0) { docker-compose build --no-cache 2>&1 }
docker compose up -d 2>&1
if ($LASTEXITCODE -ne 0) { docker-compose up -d 2>&1 }

# [3] Wait
Write-Host "[3/5] Waiting 60s for services..." -ForegroundColor Yellow
Start-Sleep -Seconds 60

$services = @("vlake-postgres","vlake-minio","vlake-kafka","vlake-mongodb","vlake-besu-node1","vlake-backend")
foreach ($svc in $services) {
    try {
        $r = docker inspect --format "{{.State.Running}}" $svc 2>$null
        if ($r -eq "true") { Write-Host "  [OK] $svc" -ForegroundColor Green }
        else { Write-Host "  [--] $svc" -ForegroundColor Yellow }
    } catch { Write-Host "  [--] $svc" -ForegroundColor Yellow }
}

# [4] Deploy contract
Write-Host "[4/6] Installing deps + deploying contract..." -ForegroundColor Yellow
pip install py-solc-x web3 eth-account minio kafka-python-ng pymongo psycopg2-binary cryptography --quiet 2>$null
python scripts\deploy_contract.py

# [5] Recreate backend so it picks up the new CONTRACT_ADDRESS and ABI
Write-Host "[5/6] Restarting backend with new contract address..." -ForegroundColor Yellow
docker compose up -d backend --force-recreate 2>&1

# [6] Seed
Write-Host "[6/6] Seeding data..." -ForegroundColor Yellow
python scripts\seed_all.py

Write-Host ""
Write-Host "================================================" -ForegroundColor Green
Write-Host "  V-LAKE is running!" -ForegroundColor Green
Write-Host "  Frontend: http://localhost:3000" -ForegroundColor Green
Write-Host "  Backend:  http://localhost:5000" -ForegroundColor Green
Write-Host "  MinIO:    http://localhost:9001" -ForegroundColor Green
Write-Host "================================================" -ForegroundColor Green
Write-Host ""
Write-Host "Stop: docker compose down" -ForegroundColor Yellow
Write-Host "Stop + delete: docker compose down -v" -ForegroundColor Yellow
