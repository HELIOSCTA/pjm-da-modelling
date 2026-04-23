# Unregister All Scheduled Tasks
# This script removes all scheduled tasks under the \PJM DA task path
# Run this script as Administrator to unregister the scheduled tasks

$taskPath = "\PJM DA\"
$allTasks = Get-ScheduledTask -TaskPath "$taskPath*" -ErrorAction SilentlyContinue

if ($null -eq $allTasks -or $allTasks.Count -eq 0) {
    Write-Host "No scheduled tasks found under $taskPath" -ForegroundColor Yellow
    exit
}

Write-Host "Found $($allTasks.Count) scheduled tasks to unregister:" -ForegroundColor Cyan
Write-Host ""

foreach ($task in $allTasks) {
    Write-Host "  $($task.TaskPath)$($task.TaskName)" -ForegroundColor Yellow
}

Write-Host ""
$confirm = Read-Host "Are you sure you want to unregister all tasks? (y/n)"
if ($confirm -ne 'y') {
    Write-Host "Cancelled." -ForegroundColor Yellow
    exit
}

Write-Host ""

$successful = 0
$failed = 0

foreach ($task in $allTasks) {
    $fullPath = "$($task.TaskPath)$($task.TaskName)"
    Write-Host "Unregistering: $fullPath" -ForegroundColor Yellow

    try {
        Unregister-ScheduledTask -TaskName $task.TaskName -TaskPath $task.TaskPath -Confirm:$false
        Write-Host "  Success" -ForegroundColor Green
        $successful++
    }
    catch {
        Write-Host "  Failed: $_" -ForegroundColor Red
        $failed++
    }
    Write-Host ""
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Unregistration Complete" -ForegroundColor Cyan
Write-Host "  Successful: $successful" -ForegroundColor Green
Write-Host "  Failed: $failed" -ForegroundColor $(if ($failed -gt 0) { "Red" } else { "Green" })
Write-Host "========================================" -ForegroundColor Cyan

# Remove empty subfolders from Task Scheduler
Write-Host ""
Write-Host "Cleaning up empty task folders..." -ForegroundColor Cyan

$scheduleService = New-Object -ComObject "Schedule.Service"
$scheduleService.Connect()
$rootFolder = $scheduleService.GetFolder("\")

function Remove-EmptyTaskFolders($folder) {
    foreach ($subfolder in $folder.GetFolders(0)) {
        Remove-EmptyTaskFolders $subfolder
    }
    if ($folder.Path -like "\PJM DA*" -and $folder.GetTasks(0).Count -eq 0 -and $folder.GetFolders(0).Count -eq 0) {
        try {
            $parentPath = $folder.Path.Substring(0, $folder.Path.LastIndexOf("\"))
            if ($parentPath -eq "") { $parentPath = "\" }
            $parentFolder = $scheduleService.GetFolder($parentPath)
            $parentFolder.DeleteFolder($folder.Name, 0)
            Write-Host "  Removed empty folder: $($folder.Path)" -ForegroundColor Green
        }
        catch {
            Write-Host "  Failed to remove folder: $($folder.Path) - $_" -ForegroundColor Red
        }
    }
}

try {
    $pjmDaFolder = $scheduleService.GetFolder("\PJM DA")
    Remove-EmptyTaskFolders $pjmDaFolder
    # Remove the root PJM DA folder itself if empty
    $pjmDaFolder = $scheduleService.GetFolder("\PJM DA")
    if ($pjmDaFolder.GetTasks(0).Count -eq 0 -and $pjmDaFolder.GetFolders(0).Count -eq 0) {
        $rootFolder.DeleteFolder("PJM DA", 0)
        Write-Host "  Removed empty folder: \PJM DA" -ForegroundColor Green
    }
}
catch {
    Write-Host "  No folders to clean up or already removed." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Cleanup complete." -ForegroundColor Cyan
