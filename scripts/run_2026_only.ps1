param(
    [ValidateSet("Demo", "Execute")]
    [string]$Mode = "Demo",
    [switch]$SkipModels
)

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot
Set-Location $repo

$catalog = "/workspace/data/bronze/_catalog_2026.json"
$commands = @(
    "tlc-pipeline catalog --years 2026 --catalog-output $catalog",
    "tlc-pipeline ingest --years 2026 --catalog-input $catalog",
    "tlc-pipeline silver --years 2026",
    # Gold se reconstruye desde todo Silver: así no se borran 2023-2025 de los dashboards.
    "tlc-pipeline gold"
)

if (-not $SkipModels) {
    $commands += "tlc-pipeline models"
}
$commands += @(
    "tlc-pipeline audit-export",
    "tlc-pipeline powerbi",
    "tlc-pipeline verify --powerbi-path /workspace/powerbi"
)

Write-Host "Pipeline de exposición TLC: ingesta y Silver limitados al año 2026."
Write-Host "Gold y Power BI conservan el histórico completo 2023-2026."
$commands | ForEach-Object { Write-Host "  docker compose exec spark $_" }

if ($Mode -eq "Demo") {
    Write-Host "Modo Demo: no se modificó ningún dato. Use -Mode Execute durante la exposición."
    exit 0
}

docker compose up -d
foreach ($command in $commands) {
    Write-Host "`nEjecutando: $command"
    docker compose exec spark bash -lc $command
    if ($LASTEXITCODE -ne 0) {
        throw "Falló el comando: $command"
    }
}

Write-Host "`nEjecución 2026 terminada y verificada."
