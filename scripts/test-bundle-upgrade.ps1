$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$TestRoot = Join-Path ([IO.Path]::GetTempPath()) ("expressways-bundle-upgrade-" + [Guid]::NewGuid().ToString("N"))

function New-TestExe([string]$Path, [int]$ExitCode) {
    $ClassName = "Program" + [Guid]::NewGuid().ToString("N")
    $Source = "public static class $ClassName { public static int Main(string[] args) { return $ExitCode; } }"
    Add-Type -TypeDefinition $Source -Language CSharp -OutputAssembly $Path -OutputType ConsoleApplication
}

function New-Bundle([string]$Path, [string]$Version, [bool]$Healthy) {
    New-Item -ItemType Directory -Force -Path (Join-Path $Path "bin"), (Join-Path $Path "scripts"), (Join-Path $Path "configs"), (Join-Path $Path "var\auth") | Out-Null
    $ExitCode = if ($Healthy) { 0 } else { 1 }
    foreach ($Binary in @("expressways-server", "expresswaysctl", "expressways-http-gateway", "expressways-orchestrator", "expressways-nanobot-system", "expressways-interop-bridge")) {
        New-TestExe (Join-Path $Path "bin\$Binary.exe") $ExitCode
    }
    Set-Content -LiteralPath (Join-Path $Path "bin\version") -Value $Version -NoNewline
    Set-Content -LiteralPath (Join-Path $Path "scripts\expressways-service.ps1") -Value 'param([string]$Action) exit 0'
    Set-Content -LiteralPath (Join-Path $Path "scripts\install-user-service.ps1") -Value 'param([string]$Action) exit 0'
    Copy-Item -LiteralPath (Join-Path $RepoRoot "scripts\upgrade-bundle.ps1") -Destination (Join-Path $Path "scripts\upgrade-bundle.ps1")
    Set-Content -LiteralPath (Join-Path $Path "configs\expressways.example.toml") -Value "version = `"$Version`""
    Set-Content -LiteralPath (Join-Path $Path "release-notes.md") -Value "notes for $Version"
    $Lines = Get-ChildItem -LiteralPath (Join-Path $Path "bin"), (Join-Path $Path "configs"), (Join-Path $Path "scripts") -File -Recurse |
        Sort-Object FullName |
        ForEach-Object {
            $Relative = [IO.Path]::GetRelativePath($Path, $_.FullName).Replace('\', '/')
            "{0}  {1}" -f (Get-FileHash -Algorithm SHA256 -LiteralPath $_.FullName).Hash.ToLowerInvariant(), $Relative
        }
    Set-Content -LiteralPath (Join-Path $Path "checksums.txt") -Value $Lines
}

try {
    $Current = Join-Path $TestRoot "current"
    $Good = Join-Path $TestRoot "good"
    $Bad = Join-Path $TestRoot "bad"
    New-Bundle $Current "old" $true
    New-Bundle $Good "new" $true
    New-Bundle $Bad "broken" $false
    Set-Content -LiteralPath (Join-Path $Current "var\auth\developer.token") -Value "token"
    Set-Content -LiteralPath (Join-Path $Current "configs\expressways.example.toml") -Value "operator-owned = true"
    $env:TOKEN_FILE = Join-Path $Current "var\auth\developer.token"
    $env:HEALTH_ATTEMPTS = "1"
    $env:HEALTH_DELAY_MS = "1"

    & (Join-Path $Current "scripts\upgrade-bundle.ps1") verify $Good *> $null
    & (Join-Path $Current "scripts\upgrade-bundle.ps1") apply $Good *> $null
    if ((Get-Content -LiteralPath (Join-Path $Current "bin\version") -Raw) -ne "new") { throw "healthy upgrade did not commit" }
    if ((Get-Content -LiteralPath (Join-Path $Current "configs\expressways.example.toml") -Raw).Trim() -ne "operator-owned = true") { throw "operator config changed" }

    $Failed = $false
    try { & (Join-Path $Current "scripts\upgrade-bundle.ps1") apply $Bad *> $null } catch { $Failed = $true }
    if (-not $Failed) { throw "unhealthy upgrade unexpectedly succeeded" }
    if ((Get-Content -LiteralPath (Join-Path $Current "bin\version") -Raw) -ne "new") { throw "rollback did not restore previous release" }

    Add-Content -LiteralPath (Join-Path $Good "bin\expressways-server.exe") -Value "tampered"
    $TamperAccepted = $true
    try { & (Join-Path $Current "scripts\upgrade-bundle.ps1") verify $Good *> $null } catch { $TamperAccepted = $false }
    if ($TamperAccepted) { throw "tampered bundle unexpectedly passed verification" }
    Write-Host "Windows bundle upgrade transaction passed: checksum gate, state/config preservation, commit, and automatic rollback."
} finally {
    Remove-Item -LiteralPath $TestRoot -Recurse -Force -ErrorAction SilentlyContinue
}
