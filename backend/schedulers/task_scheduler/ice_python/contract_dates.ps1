# Registers the PJM short-term ICE contract-dates Task Scheduler job.
# Fires hourly from 05:00 to 10:00 MT (last fire 09:00), daily. The Python
# orchestration's weekday gate backstops weekend misfires.
#
# Requires: Administrator. ICE XL + conda env `helioscta-pjm-da-dev` on the host.

$condaPath   = "$env:USERPROFILE\miniconda3\Scripts\activate.bat"
$condaEnv    = "helioscta-pjm-da-dev"
$repoRoot    = (Resolve-Path "$PSScriptRoot\..\..\..\..").Path
$moduleName  = "backend.orchestration.ice_python.contract_dates"

$cmdArgs = "/c `"call `"$condaPath`" $condaEnv && cd /d `"$repoRoot`" && python -m $moduleName`""

$action = New-ScheduledTaskAction `
    -Execute "cmd.exe" `
    -Argument $cmdArgs

# Every hour from 05:00 for 5 hours (fires at 05, 06, 07, 08, 09 MT).
# Weekday gating lives in Python.
# Repetition is copied from a throwaway -Once trigger because Windows
# PowerShell 5.1 won't let you set .Repetition.Interval directly on a -Daily.
$trigger = New-ScheduledTaskTrigger -Daily -At "05:00"
$trigger.Repetition = (New-ScheduledTaskTrigger -Once -At "05:00" `
    -RepetitionInterval (New-TimeSpan -Hours 1) `
    -RepetitionDuration (New-TimeSpan -Hours 5)).Repetition

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 30)

Register-ScheduledTask `
    -TaskName "Contract Dates" `
    -Action $action `
    -Trigger $trigger `
    -Settings $settings `
    -TaskPath "\PJM DA\ICE Python\" `
    -Force
