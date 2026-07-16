[CmdletBinding()]
param(
    [switch]$SkipBuild,
    [switch]$OpenPowerBI
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $ProjectRoot

function Invoke-Native {
    param([Parameter(Mandatory)][scriptblock]$Command)
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "El comando externo finalizó con código $LASTEXITCODE"
    }
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw 'Docker no está disponible en PATH.'
}

$freeGiB = [math]::Round((Get-PSDrive -Name C).Free / 1GB, 2)
Write-Host "Espacio libre en C: $freeGiB GiB"
if ($freeGiB -lt 45) {
    Write-Warning 'El procesamiento completo puede requerir 45 GiB o más.'
}

if (-not $SkipBuild) {
    Invoke-Native { docker compose build spark }
}
Invoke-Native { docker compose up -d mongo }
Invoke-Native { docker compose run --rm spark tlc-pipeline catalog }
Invoke-Native {
    docker compose run --rm spark tlc-pipeline full `
        --catalog-input /workspace/data/bronze/_catalog.json
}

Write-Host 'Pipeline y verificación final completados.'
if ($OpenPowerBI) {
    Start-Process -FilePath (Join-Path $ProjectRoot 'powerbi\TLC_BigData.pbip')
}
