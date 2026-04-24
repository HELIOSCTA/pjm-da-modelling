# Registers the PJM short-term ICE ticker-data Task Scheduler job.
# Two triggers:
#   1. Coarse: every 15 min, 05:00–15:45 MT (intraday polling).
#   2. Close sprint: every 1 min, 15:50–15:59 MT (capture last ticks
#      into the 16:00 MT exchange close).
# The orchestration module's weekday + trading-hours gate
# (backend.orchestration.ice_python._policies) is the authoritative
# window — Sat/Sun and off-hours fires exit 0.
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

# Trigger 1 — coarse intraday cadence. Every 15 min from 05:00 for 11
# hours (last fire at 15:45 MT). Weekday gating lives in Python so the
# trigger stays simple. Repetition is copied from a throwaway -Once
# trigger because Windows PowerShell 5.1 won't let you set
# .Repetition.Interval directly on a -Daily.
$triggerCoarse = New-ScheduledTaskTrigger -Daily -At "05:00"
$triggerCoarse.Repetition = (New-ScheduledTaskTrigger -Once -At "05:00" `
    -RepetitionInterval (New-TimeSpan -Minutes 15) `
    -RepetitionDuration (New-TimeSpan -Hours 11)).Repetition

# Trigger 2 — close sprint. Every 1 min from 15:50 for 10 min (fires
# 15:50, 15:51, …, 15:59 MT). The 16:00 MT fire is gated off by
# _policies.TRADING_END_HOUR since the exchange is closed.
$triggerSprint = New-ScheduledTaskTrigger -Daily -At "15:50"
$triggerSprint.Repetition = (New-ScheduledTaskTrigger -Once -At "15:50" `
    -RepetitionInterval (New-TimeSpan -Minutes 1) `
    -RepetitionDuration (New-TimeSpan -Minutes 10)).Repetition

$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

Register-ScheduledTask `
    -TaskName "Ticker Data" `
    -Action $action `
    -Trigger $triggerCoarse, $triggerSprint `
    -Settings $settings `
    -TaskPath "\PJM DA\ICE Python\" `
    -Force
