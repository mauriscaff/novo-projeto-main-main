param(
    [string]$HostAddress = "127.0.0.1",
    [int]$Port = 8000,
    [int]$StartupTimeoutSec = 25,
    [switch]$Foreground
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$ArtifactsDir = Join-Path $Root "artifacts"
$PidFile = Join-Path $ArtifactsDir "uvicorn.pid"
$OutLog = Join-Path $ArtifactsDir "uvicorn-app.out.log"
$ErrLog = Join-Path $ArtifactsDir "uvicorn-app.err.log"

if (-not (Test-Path $ArtifactsDir)) {
    New-Item -ItemType Directory -Path $ArtifactsDir | Out-Null
}

function Resolve-PythonCommand {
    if ($env:PYTHON_EXE -and (Test-Path $env:PYTHON_EXE)) {
        return [pscustomobject]@{
            FilePath = $env:PYTHON_EXE
            PrefixArgs = @()
        }
    }

    $known312 = Join-Path $env:LOCALAPPDATA "Programs\\Python\\Python312\\python.exe"
    if (Test-Path $known312) {
        return [pscustomobject]@{
            FilePath = $known312
            PrefixArgs = @()
        }
    }

    $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCmd -and $pythonCmd.Source -and ($pythonCmd.Source -notmatch "WindowsApps")) {
        return [pscustomobject]@{
            FilePath = $pythonCmd.Source
            PrefixArgs = @()
        }
    }

    $pyLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($pyLauncher -and $pyLauncher.Source) {
        return [pscustomobject]@{
            FilePath = $pyLauncher.Source
            PrefixArgs = @("-3")
        }
    }

    throw "Python nao encontrado. Defina `$env:PYTHON_EXE ou instale Python 3 no PATH."
}

function Get-ListeningPid([int]$PortNumber) {
    $lines = netstat -ano | Select-String ":$PortNumber "
    foreach ($line in $lines) {
        $text = ($line.ToString() -replace "\s+", " ").Trim()
        $parts = $text.Split(" ")
        # Formato esperado (TCP): Proto LocalAddr ForeignAddr State PID
        if ($parts.Length -lt 5) { continue }
        $state = $parts[-2]
        if ($state -ne "LISTENING") { continue }
        $pidText = $parts[-1]
        if ($pidText -match "^\d+$") {
            return [int]$pidText
        }
    }
    return $null
}

function Normalize-ProcessPathVariable {
    $processPath = [System.Environment]::GetEnvironmentVariable("Path", "Process")
    $processPATH = [System.Environment]::GetEnvironmentVariable("PATH", "Process")

    if (-not $processPath -and -not $processPATH) {
        return
    }

    $normalizedPath = $processPath
    if (-not $normalizedPath) {
        $normalizedPath = $processPATH
    }

    [System.Environment]::SetEnvironmentVariable("Path", $normalizedPath, "Process")
    [System.Environment]::SetEnvironmentVariable("PATH", $null, "Process")
}

if (Test-Path $PidFile) {
    $existingPidRaw = Get-Content $PidFile -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($existingPidRaw -match "^\d+$") {
        $existingProc = Get-Process -Id ([int]$existingPidRaw) -ErrorAction SilentlyContinue
        if ($existingProc) {
            Write-Host "Aplicacao ja esta em execucao (PID $existingPidRaw)."
            Write-Host "URL: http://$HostAddress`:$Port"
            exit 0
        }
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

$portPid = Get-ListeningPid -PortNumber $Port
if ($portPid) {
    throw "Porta $Port ja esta em uso pelo PID $portPid. Pare esse processo antes de iniciar."
}

if (Test-Path $OutLog) { Remove-Item $OutLog -Force -ErrorAction SilentlyContinue }
if (Test-Path $ErrLog) { Remove-Item $ErrLog -Force -ErrorAction SilentlyContinue }

$pythonCmd = Resolve-PythonCommand
$checkArgs = @()
$checkArgs += $pythonCmd.PrefixArgs
$checkArgs += @("-c", "import uvicorn")
& $pythonCmd.FilePath @checkArgs 1>$null 2>$null
if ($LASTEXITCODE -ne 0) {
    throw "Modulo 'uvicorn' nao encontrado no Python selecionado. Instale dependencias com: pip install -r requirements.txt"
}

$args = @()
$args += $pythonCmd.PrefixArgs
$args += @("-m", "uvicorn", "main:app", "--host", $HostAddress, "--port", "$Port")

if ($Foreground) {
    Write-Host "Executando em foreground. Use Ctrl+C para parar."
    Write-Host "URL: http://$HostAddress`:$Port"
    & $pythonCmd.FilePath @args
    exit $LASTEXITCODE
}

Normalize-ProcessPathVariable
$process = Start-Process `
    -FilePath $pythonCmd.FilePath `
    -ArgumentList $args `
    -WorkingDirectory $Root `
    -RedirectStandardOutput $OutLog `
    -RedirectStandardError $ErrLog `
    -PassThru

$process.Id | Set-Content -Path $PidFile -Encoding ASCII

$started = $false
for ($i = 0; $i -lt $StartupTimeoutSec; $i++) {
    Start-Sleep -Seconds 1

    $probe = $null
    try {
        $probe = Invoke-WebRequest -Uri "http://$HostAddress`:$Port/health" -UseBasicParsing -TimeoutSec 2
    } catch {
        $probe = $null
    }

    if ($probe -and $probe.StatusCode -eq 200) {
        $started = $true
        break
    }

    $alive = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
    if (-not $alive) {
        break
    }
}

if (-not $started) {
    $alive = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
    if ($alive) {
        Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
    Write-Host "Falha ao iniciar a aplicacao. Ultimas linhas do erro:"
    if (Test-Path $ErrLog) {
        Get-Content $ErrLog -Tail 80
    }
    exit 1
}

Write-Host "Aplicacao iniciada com sucesso."
Write-Host "PID: $($process.Id)"
Write-Host "URL: http://$HostAddress`:$Port"
Write-Host "Health: http://$HostAddress`:$Port/health"
