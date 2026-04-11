# V-Lake: Generate Besu QBFT genesis and validator keys
# This is now also inlined in run.ps1, but kept as standalone for manual use.

Write-Host "V-LAKE: Setting up Besu QBFT network" -ForegroundColor Cyan

$projectRoot = (Get-Location).Path
$configDir = "$projectRoot\config\besu"
$keysDir = "$configDir\keys"

Write-Host "  Project root: $projectRoot"

if (-not (Test-Path $keysDir)) { New-Item -ItemType Directory -Force -Path $keysDir | Out-Null }

for ($i = 1; $i -le 4; $i++) {
    $kf = "$keysDir\node$i.key"
    if (-not (Test-Path $kf)) {
        $bytes = New-Object byte[] 32
        [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
        $hex = -join ($bytes | ForEach-Object { $_.ToString("x2") })
        [System.IO.File]::WriteAllText($kf, $hex)
        Write-Host "  Generated node$i.key" -ForegroundColor Green
    } else {
        Write-Host "  node$i.key exists" -ForegroundColor Yellow
    }
}

$genesisJson = '{"config":{"chainId":1337,"berlinBlock":0,"qbft":{"blockperiodseconds":2,"epochlength":30000,"requesttimeoutseconds":4}},"nonce":"0x0","timestamp":"0x0","gasLimit":"0x1fffffffffffff","difficulty":"0x1","alloc":{},"extraData":"0xf87aa00000000000000000000000000000000000000000000000000000000000000000f854940000000000000000000000000000000000000001940000000000000000000000000000000000000002940000000000000000000000000000000000000003940000000000000000000000000000000000000004c080c0"}'
$genesisPath = "$configDir\genesis.json"
[System.IO.File]::WriteAllText($genesisPath, $genesisJson)
Write-Host "  genesis.json created" -ForegroundColor Green
