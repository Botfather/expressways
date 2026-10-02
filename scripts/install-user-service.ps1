[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [ValidateSet("install", "uninstall", "status", "render")]
    [string]$Action,

    [Parameter(Position = 1)]
    [string]$OutputPath
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$LifecycleScript = Join-Path $Root "scripts\expressways-service.ps1"
$TaskName = "Expressways Local Backbone"

if ($Root.Contains("`n") -or $Root.Contains("`r")) {
    throw "Bundle path must not contain line breaks."
}
if (-not (Test-Path -LiteralPath $LifecycleScript -PathType Leaf)) {
    throw "Missing lifecycle helper: $LifecycleScript"
}

function ConvertTo-XmlText([string]$Value) {
    return [System.Security.SecurityElement]::Escape($Value)
}

function New-TaskXml {
    $EscapedScript = ConvertTo-XmlText $LifecycleScript
    $EscapedRoot = ConvertTo-XmlText $Root
    $UserId = ConvertTo-XmlText ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name)
    return @"
<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <Triggers><LogonTrigger><Enabled>true</Enabled><UserId>$UserId</UserId></LogonTrigger></Triggers>
  <Principals><Principal id="Author"><UserId>$UserId</UserId><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>
  <Settings><MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy><DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries><StopIfGoingOnBatteries>false</StopIfGoingOnBatteries><AllowHardTerminate>true</AllowHardTerminate><StartWhenAvailable>true</StartWhenAvailable><ExecutionTimeLimit>PT0S</ExecutionTimeLimit><RestartOnFailure><Interval>PT1M</Interval><Count>3</Count></RestartOnFailure><Enabled>true</Enabled></Settings>
  <Actions Context="Author"><Exec><Command>powershell.exe</Command><Arguments>-NoProfile -NonInteractive -ExecutionPolicy Bypass -File &quot;$EscapedScript&quot; supervise</Arguments><WorkingDirectory>$EscapedRoot</WorkingDirectory></Exec></Actions>
</Task>
"@
}

switch ($Action) {
    "render" {
        if (-not $OutputPath) { throw "render requires an output path" }
        $Parent = Split-Path -Parent $OutputPath
        if ($Parent) { New-Item -ItemType Directory -Force -Path $Parent | Out-Null }
        New-TaskXml | Set-Content -LiteralPath $OutputPath -Encoding Unicode
        Write-Host "Rendered Windows per-user scheduled task: $OutputPath"
    }
    "install" {
        $Xml = New-TaskXml
        Register-ScheduledTask -TaskName $TaskName -Xml $Xml -Force | Out-Null
        Start-ScheduledTask -TaskName $TaskName
        Write-Host "Installed Expressways per-user startup task from $Root."
    }
    "uninstall" {
        Stop-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
        & $LifecycleScript stop-all
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
        Write-Host "Removed Expressways per-user startup task. Runtime data was preserved."
    }
    "status" {
        Get-ScheduledTask -TaskName $TaskName
        & $LifecycleScript status-all
    }
}
