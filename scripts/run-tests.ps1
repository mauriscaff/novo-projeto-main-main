param(
    [string]$PythonExe = "python",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$PytestArgs
)

$ErrorActionPreference = "Stop"

$env:DATABASE_URL = "sqlite+aiosqlite:///./test_zombiehunter.db"
$env:SCHEDULER_ENABLED = "false"
$env:READONLY_MODE = "true"

function Resolve-PythonCommand {
    param([string]$Preferred)

    if ($env:PYTHON_EXE -and (Test-Path $env:PYTHON_EXE)) {
        return [pscustomobject]@{
            FilePath = $env:PYTHON_EXE
            PrefixArgs = @()
        }
    }

    if ($Preferred -and $Preferred -ne "python") {
        $cmd = Get-Command $Preferred -ErrorAction SilentlyContinue
        if ($cmd -and $cmd.Source) {
            return [pscustomobject]@{
                FilePath = $cmd.Source
                PrefixArgs = @()
            }
        }
        if (Test-Path $Preferred) {
            return [pscustomobject]@{
                FilePath = $Preferred
                PrefixArgs = @()
            }
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

    throw "Python nao encontrado. Defina `$env:PYTHON_EXE ou instale Python 3."
}

$python = Resolve-PythonCommand -Preferred $PythonExe

if (-not $PytestArgs -or $PytestArgs.Count -eq 0) {
    & $python.FilePath @($python.PrefixArgs + @("-m", "pytest"))
    exit $LASTEXITCODE
}

& $python.FilePath @($python.PrefixArgs + @("-m", "pytest") + $PytestArgs)
exit $LASTEXITCODE
