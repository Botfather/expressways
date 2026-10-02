[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("verify", "apply")]
    [string]$Action,

    [Parameter(Mandatory = $true, Position = 1)]
    [string]$SourceDirectory
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Source = (Resolve-Path $SourceDirectory).Path
$TokenFile = if ($env:TOKEN_FILE) { $env:TOKEN_FILE } else { Join-Path $Root "var\auth\developer.token" }
$BrokerAddress = if ($env:BROKER_ADDRESS) { $env:BROKER_ADDRESS } else { "127.0.0.1:7766" }
$HealthAttempts = if ($env:HEALTH_ATTEMPTS) { [int]$env:HEALTH_ATTEMPTS } else { 40 }
$HealthDelayMs = if ($env:HEALTH_DELAY_MS) { [int]$env:HEALTH_DELAY_MS } else { 250 }
$UpgradeRoot = Join-Path $Root "var\agent\upgrades"
$LockPath = Join-Path $Root "var\agent\upgrade.lock"

function Test-UpgradeSource {
    if ([System.StringComparer]::OrdinalIgnoreCase.Equals($Source, $Root)) {
        throw "Upgrade source must differ from the installed bundle."
    }
    $Required = @(
        "bin\expressways-server.exe", "bin\expresswaysctl.exe",
        "bin\expressways-http-gateway.exe", "bin\expressways-orchestrator.exe",
        "bin\expressways-nanobot-system.exe", "bin\expressways-interop-bridge.exe",
        "scripts\expressways-service.ps1", "scripts\install-user-service.ps1",
        "scripts\upgrade-bundle.ps1", "configs\expressways.example.toml", "checksums.txt"
    )
    foreach ($Relative in $Required) {
        if (-not (Test-Path -LiteralPath (Join-Path $Source $Relative) -PathType Leaf)) {
            throw "Upgrade bundle is missing required path: $Relative"
        }
    }
    foreach ($Line in Get-Content -LiteralPath (Join-Path $Source "checksums.txt")) {
        if (-not $Line) { continue }
        if ($Line -notmatch '^([0-9a-fA-F]{64})  (.+)$') { throw "Invalid checksum entry: $Line" }
        $Expected = $Matches[1].ToLowerInvariant()
        $Relative = $Matches[2]
        if ([IO.Path]::IsPathRooted($Relative) -or (($Relative -split '[\\/]') -contains '..')) {
            throw "Unsafe checksum path: $Relative"
        }
        $Path = Join-Path $Source $Relative
        if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) { throw "Checksummed file is missing: $Relative" }
        $Actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant()
        if ($Actual -ne $Expected) { throw "Checksum mismatch: $Relative" }
    }
}

function Test-BrokerHealth {
    if (-not (Test-Path -LiteralPath $TokenFile -PathType Leaf)) { return $false }
    $Ctl = Join-Path $Root "bin\expresswaysctl.exe"
    foreach ($Attempt in 1..$HealthAttempts) {
        & $Ctl --transport tcp --address $BrokerAddress health --token-file $TokenFile *> $null
        if ($LASTEXITCODE -eq 0) { return $true }
        Start-Sleep -Milliseconds $HealthDelayMs
    }
    return $false
}

function Restore-Previous([string]$Backup) {
    $Lifecycle = Join-Path $Root "scripts\expressways-service.ps1"
    & $Lifecycle stop-all *> $null
    $Failed = Join-Path $Backup "failed"
    New-Item -ItemType Directory -Force -Path $Failed | Out-Null
    Move-Item -LiteralPath (Join-Path $Root "bin") -Destination (Join-Path $Failed "bin")
    Move-Item -LiteralPath (Join-Path $Root "scripts") -Destination (Join-Path $Failed "scripts")
    Move-Item -LiteralPath (Join-Path $Backup "previous\bin") -Destination (Join-Path $Root "bin")
    Move-Item -LiteralPath (Join-Path $Backup "previous\scripts") -Destination (Join-Path $Root "scripts")
    & (Join-Path $Root "scripts\expressways-service.ps1") start-all
}

Test-UpgradeSource
if ($Action -eq "verify") {
    Write-Host "Upgrade bundle verification passed: $Source"
    exit 0
}
if (-not (Test-Path -LiteralPath $TokenFile -PathType Leaf)) {
    throw "Refusing upgrade without health token: $TokenFile"
}

New-Item -ItemType Directory -Force -Path $UpgradeRoot | Out-Null
$Lock = $null
try {
    $Lock = [IO.File]::Open($LockPath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
} catch {
    throw "Another upgrade transaction is active: $LockPath"
}

try {
    $Transaction = "{0}-{1}" -f (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ"), $PID
    $Backup = Join-Path $UpgradeRoot $Transaction
    $Staging = Join-Path $Backup "staging"
    New-Item -ItemType Directory -Force -Path $Staging, (Join-Path $Backup "previous") | Out-Null
    Copy-Item -Recurse -LiteralPath (Join-Path $Source "bin") -Destination (Join-Path $Staging "bin")
    Copy-Item -Recurse -LiteralPath (Join-Path $Source "scripts") -Destination (Join-Path $Staging "scripts")
    Copy-Item -LiteralPath (Join-Path $Source "checksums.txt") -Destination $Staging
    Copy-Item -LiteralPath (Join-Path $Source "configs\expressways.example.toml") -Destination (Join-Path $Staging "expressways.example.toml")
    if (Test-Path -LiteralPath (Join-Path $Source "release-notes.md")) {
        Copy-Item -LiteralPath (Join-Path $Source "release-notes.md") -Destination $Staging
    }

    & (Join-Path $Root "scripts\expressways-service.ps1") stop-all
    Move-Item -LiteralPath (Join-Path $Root "bin") -Destination (Join-Path $Backup "previous\bin")
    Move-Item -LiteralPath (Join-Path $Root "scripts") -Destination (Join-Path $Backup "previous\scripts")
    Move-Item -LiteralPath (Join-Path $Staging "bin") -Destination (Join-Path $Root "bin")
    Move-Item -LiteralPath (Join-Path $Staging "scripts") -Destination (Join-Path $Root "scripts")

    $Started = $true
    try { & (Join-Path $Root "scripts\expressways-service.ps1") start-all } catch { $Started = $false }
    if (-not $Started -or -not (Test-BrokerHealth)) {
        Write-Warning "Upgrade validation failed; restoring previous release."
        Restore-Previous $Backup
        if (-not (Test-BrokerHealth)) { throw "Rollback restored files but broker health is still failing." }
        Set-Content -LiteralPath (Join-Path $Backup "status") -Value "rolled_back" -NoNewline
        throw "Upgrade rolled back after failed validation."
    }

    Copy-Item -LiteralPath (Join-Path $Staging "checksums.txt") -Destination (Join-Path $Root "checksums.txt") -Force
    Copy-Item -LiteralPath (Join-Path $Staging "expressways.example.toml") -Destination (Join-Path $Root "configs\expressways.example.toml.dist") -Force
    if (Test-Path -LiteralPath (Join-Path $Staging "release-notes.md")) {
        Copy-Item -LiteralPath (Join-Path $Staging "release-notes.md") -Destination (Join-Path $Root "release-notes.md") -Force
    }
    Set-Content -LiteralPath (Join-Path $Backup "status") -Value "committed" -NoNewline
    Set-Content -LiteralPath (Join-Path $UpgradeRoot "current") -Value $Transaction -NoNewline
    Write-Host "Upgrade committed. Previous managed payload: $(Join-Path $Backup 'previous')"
} finally {
    if ($Lock) { $Lock.Dispose() }
    Remove-Item -LiteralPath $LockPath -Force -ErrorAction SilentlyContinue
}
