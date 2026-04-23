# Registers the PJM short-term ICE ticker-data Task Scheduler job.
# Fires every 15 minutes, 05:00–16:00 daily. The orchestration module's
# weekday + trading-hours gate (backend.orchestration.ice_python._policies)
# is the authoritative window — Sat/Sun and off-hours fires exit 0.
#
# Requires: Administrator. ICE XL + conda env `helioscta-pjm-da-dev` on the host.

$condaPath   = "$env:USERPROFILE\miniconda3\Scripts\activate.bat"
$condaEnv    = "helioscta-pjm-da-dev"
$repoRoot    = (Resolve-Path "$PSScriptRoot\..\..\..\..").Path
$moduleName  = "backend.orchestration.ice_python.ticker_data"

$cmdArgs = "/c `"call `"$condaPath`" $condaEnv && cd /d `"$repoRoot`" && python -m $moduleName`""

$action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument $cmdArgs

# Every 15 min from 05:00 for 11 hours (last fire at 15:45 MT). Weekday
# gating lives in Python so the trigger stays simple.
# Repetition is copied from a throwaway -Once trigger because Windows
# PowerShell 5.1 won't let you set .Repetition.Interval directly on a -Daily.
$trigger = New-ScheduledTaskTrigger -Daily -At "05:00"
$trigger.Repetition = (New-ScheduledTaskTrigger -Once -At "05:00" `
    -RepetitionInterval (New-TimeSpan -Minutes 15) `
    -RepetitionDuration (New-TimeSpan -Hours 11)).Repetition

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

Register-ScheduledTask `
    -TaskName "Ticker Data" `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -TaskPath "\PJM DA\ICE Python\" `
    -Force
