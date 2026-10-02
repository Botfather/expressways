[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("start", "stop", "restart", "status", "start-all", "stop-all", "restart-all", "status-all")]
    [string]$Action,

    [Parameter(Position = 1)]
    [ValidateSet("expressways-server", "expressways-http-gateway", "expressways-orchestrator", "nanobot-runtime")]
    [string]$Service
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $Root

$PidDir = if ($env:PID_DIR) { $env:PID_DIR } else { Join-Path $Root "var\agent\service-control" }
$LogDir = if ($env:LOG_DIR) { $env:LOG_DIR } else { Join-Path $PidDir "logs" }
$ConfigPath = if ($env:CONFIG_PATH) { $env:CONFIG_PATH } else { Join-Path $Root "configs\expressways.example.toml" }
$BrokerAddress = if ($env:BROKER_ADDRESS) { $env:BROKER_ADDRESS } else { "127.0.0.1:7766" }
$HttpListen = if ($env:HTTP_LISTEN) { $env:HTTP_LISTEN } else { "127.0.0.1:8790" }
$TokenFile = if ($env:TOKEN_FILE) { $env:TOKEN_FILE } else { Join-Path $Root "var\auth\developer.token" }
$Services = @("expressways-server", "expressways-http-gateway", "expressways-orchestrator", "nanobot-runtime")

New-Item -ItemType Directory -Force -Path $PidDir, $LogDir | Out-Null

function Get-ServiceSpec([string]$Name) {
    switch ($Name) {
        "expressways-server" {
            return @{ File = Join-Path $Root "bin\expressways-server.exe"; Args = @("--config", $ConfigPath) }
        }
        "expressways-http-gateway" {
            return @{ File = Join-Path $Root "bin\expressways-http-gateway.exe"; Args = @("--listen", $HttpListen, "--broker-address", $BrokerAddress) }
        }
        "expressways-orchestrator" {
            return @{ File = Join-Path $Root "bin\expressways-orchestrator.exe"; Args = @("--transport", "tcp", "--address", $BrokerAddress, "supervise", "--token-file", $TokenFile) }
        }
        "nanobot-runtime" {
            return @{ File = Join-Path $Root "bin\expressways-nanobot-system.exe"; Args = @("--transport", "tcp", "--address", $BrokerAddress, "run-runtime", "--token-file", $TokenFile, "--agent-id", "nanobot-runtime", "--state-dir", (Join-Path $Root "var\agent\nanobot-runtime"), "--ensure-topics", "true") }
        }
        default { throw "Unsupported service: $Name" }
    }
}

function Get-PidPath([string]$Name) { Join-Path $PidDir "$Name.pid" }
function Get-LogPath([string]$Name) { Join-Path $LogDir "$Name.log" }

function Get-ManagedProcess([string]$Name) {
    $PidPath = Get-PidPath $Name
    if (-not (Test-Path -LiteralPath $PidPath -PathType Leaf)) { return $null }
    $ManagedPid = (Get-Content -LiteralPath $PidPath -Raw).Trim()
    if ($ManagedPid -notmatch '^\d+$') { Remove-Item -LiteralPath $PidPath -Force; return $null }
    $Process = Get-Process -Id ([int]$ManagedPid) -ErrorAction SilentlyContinue
    if (-not $Process) { return $null }
    $ExpectedPath = (Get-ServiceSpec $Name).File
    try {
        if (-not [System.StringComparer]::OrdinalIgnoreCase.Equals($Process.Path, $ExpectedPath)) {
            Write-Warning "Ignoring stale pid file for $Name because pid $ManagedPid belongs to another executable."
            return $null
        }
    } catch {
        Write-Warning "Unable to verify executable ownership for pid $ManagedPid; refusing to manage it."
        return $null
    }
    return $Process
}

function Start-ManagedService([string]$Name) {
    if (Get-ManagedProcess $Name) { Write-Host "$Name is already running."; return }
    $Spec = Get-ServiceSpec $Name
    if (-not (Test-Path -LiteralPath $Spec.File -PathType Leaf)) { throw "Packaged binary not found: $($Spec.File)" }
    $LogPath = Get-LogPath $Name
    $Process = Start-Process -FilePath $Spec.File -ArgumentList $Spec.Args -WorkingDirectory $Root -RedirectStandardOutput $LogPath -RedirectStandardError "$LogPath.err" -PassThru
    Set-Content -LiteralPath (Get-PidPath $Name) -Value $Process.Id -NoNewline
    Start-Sleep -Milliseconds 750
    if ($Process.HasExited) { throw "$Name exited during startup. See $LogPath and $LogPath.err" }
    Write-Host "$Name started (pid $($Process.Id))."
}

function Stop-ManagedService([string]$Name) {
    $Process = Get-ManagedProcess $Name
    if ($Process) {
        Stop-Process -Id $Process.Id
        try { Wait-Process -Id $Process.Id -Timeout 10 -ErrorAction Stop } catch { Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue }
    }
    Remove-Item -LiteralPath (Get-PidPath $Name) -Force -ErrorAction SilentlyContinue
    Write-Host "$Name stopped."
}

function Get-ManagedStatus([string]$Name) {
    $Process = Get-ManagedProcess $Name
    if ($Process) { Write-Host "$Name is running (pid $($Process.Id))."; return $true }
    Write-Host "$Name is stopped."
    return $false
}

function Invoke-One([string]$Verb, [string]$Name) {
    switch ($Verb) {
        "start" { Start-ManagedService $Name }
        "stop" { Stop-ManagedService $Name }
        "restart" { Stop-ManagedService $Name; Start-ManagedService $Name }
        "status" { if (-not (Get-ManagedStatus $Name)) { $script:StatusFailures++ } }
    }
}

$StatusFailures = 0
if ($Action.EndsWith("-all")) {
    $Verb = $Action.Substring(0, $Action.Length - 4)
    $Ordered = if ($Verb -eq "stop") { @($Services[3], $Services[2], $Services[1], $Services[0]) } else { $Services }
    foreach ($Name in $Ordered) { Invoke-One $Verb $Name }
} else {
    if (-not $Service) { throw "A service name is required for action '$Action'." }
    Invoke-One $Action $Service
}

if ($StatusFailures -gt 0) { exit 1 }
