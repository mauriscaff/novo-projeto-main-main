param(
    [switch]$ShowLogs
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$ArtifactsDir = Join-Path $Root "artifacts"
$PidFile = Join-Path $ArtifactsDir "uvicorn.pid"
$OutLog = Join-Path $ArtifactsDir "uvicorn-app.out.log"
$ErrLog = Join-Path $ArtifactsDir "uvicorn-app.err.log"

if (-not (Test-Path $PidFile)) {
    Write-Host "Nenhum PID file encontrado em $PidFile."
    exit 0
}

$pidRaw = Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not ($pidRaw -match "^\d+$")) {
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
    Write-Host "PID file invalido removido."
    exit 0
}

$pidValue = [int]$pidRaw
$proc = Get-Process -Id $pidValue -ErrorAction SilentlyContinue

if ($proc) {
    Stop-Process -Id $pidValue -Force -ErrorAction SilentlyContinue
    Write-Host "Processo parado (PID $pidValue)."
} else {
    Write-Host "Processo PID $pidValue nao estava em execucao."
}

Remove-Item $PidFile -Force -ErrorAction SilentlyContinue

if ($ShowLogs) {
    Write-Host "=== STDERR (tail 80) ==="
    if (Test-Path $ErrLog) { Get-Content $ErrLog -Tail 80 }
    Write-Host "=== STDOUT (tail 80) ==="
    if (Test-Path $OutLog) { Get-Content $OutLog -Tail 80 }
}
