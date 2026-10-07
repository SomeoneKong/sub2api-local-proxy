# 在 Windows 上安装：程序、配置、目录缓存与日志都在 ~/.sub2api-local-proxy/，注册一个登录时启动的计划任务。
# 重复执行即升级（换程序、重启任务，配置不动）。不需要管理员权限。
# 新程序先暂存并用它校验配置，通过了才停旧实例、替换、启动；校验不过时正在运行的实例不受影响。
#
# 用法：
#   .\install-windows.ps1 -Upstream http://sub2api.example.com:8080 [-Binary 程序路径]
#   .\install-windows.ps1 -Uninstall      # 停止并删除计划任务，保留 ~/.sub2api-local-proxy/
param(
    [string]$Upstream = '',
    [string]$Binary = '',
    [switch]$Uninstall
)
$ErrorActionPreference = 'Stop'

$name = 'sub2api-local-proxy'
$data = Join-Path $HOME '.sub2api-local-proxy'
$exe = Join-Path $data "bin\$name.exe"
$staged = Join-Path $data "bin\$name.new.exe" # 扩展名必须是 .exe 才能运行
$config = Join-Path $data 'config.json'
$logFile = Join-Path $data 'proxy.log'
$user = "$env:USERDOMAIN\$env:USERNAME"

function Stop-Proxy {
    if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) {
        Stop-ScheduledTask -TaskName $name
    }
    # 等进程真正退出：退出前 exe 仍被占用，替换会失败。
    $procs = @(Get-Process -Name $name -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq $exe })
    $procs | Stop-Process -Force -ErrorAction SilentlyContinue
    $procs | Wait-Process -Timeout 10 -ErrorAction SilentlyContinue
}

if ($Uninstall) {
    Stop-Proxy
    if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $name -Confirm:$false
    }
    Write-Output "已删除计划任务；配置、缓存与日志保留在 $data"
    return
}

if (-not $Binary) { $Binary = Join-Path $PSScriptRoot "$name-windows-amd64.exe" }
if (-not (Test-Path $Binary)) { throw "找不到程序 $Binary" }
if (-not (Test-Path $config) -and -not $Upstream) { throw '首次安装要用 -Upstream 指定网关地址' }

New-Item -ItemType Directory -Force (Join-Path $data 'bin') | Out-Null
Copy-Item $Binary $staged -Force
# Windows 版是无控制台窗口的程序，日志只能写文件。
try {
    & $staged -init -config $config -upstream $Upstream -log-file '~/.sub2api-local-proxy/proxy.log' 2>&1 |
        Out-String | Write-Output
    $initOk = $LASTEXITCODE -eq 0
} catch {
    Write-Output $_.ToString()
    $initOk = $false
}
if (-not $initOk) {
    Remove-Item $staged -Force -ErrorAction SilentlyContinue
    throw '新程序不能运行或配置有误，未改动正在运行的实例'
}

Stop-Proxy
# 杀毒软件扫描等也可能短暂占住 exe，替换失败时稍等重试。
$moveError = $null
for ($i = 0; $i -lt 10; $i++) {
    try { Move-Item $staged $exe -Force; $moveError = $null; break }
    catch { $moveError = $_; Start-Sleep -Milliseconds 500 }
}
if ($moveError) {
    Remove-Item $staged -Force -ErrorAction SilentlyContinue
    if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) { Start-ScheduledTask -TaskName $name }
    throw "替换程序失败，已重新启动原程序: $moveError"
}

$action = New-ScheduledTaskAction -Execute $exe -Argument "-config `"$config`""
$trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew `
    -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1)
$principal = New-ScheduledTaskPrincipal -UserId $user -LogonType Interactive -RunLevel Limited
Register-ScheduledTask -TaskName $name -Action $action -Trigger $trigger -Settings $settings `
    -Principal $principal -Force | Out-Null
Start-ScheduledTask -TaskName $name
Start-Sleep -Seconds 2
$proc = Get-Process -Name $name -ErrorAction SilentlyContinue | Where-Object { $_.Path -eq $exe }
if (-not $proc) {
    if (Test-Path $logFile) { Get-Content $logFile -Tail 10 | Write-Output }
    throw "启动失败，见上面的日志（$logFile）"
}
Write-Output "已启动（PID $($proc.Id)）。日志: $logFile；改 $config 后自动生效"
