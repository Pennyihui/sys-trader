# 检查 ProxyPoolService 启动失败原因
Write-Host "=== 服务状态 ==="
sc.exe query ProxyPoolService | Select-String "STATE|EXIT"

Write-Host "`n=== 最近服务相关事件日志 ==="
Get-WinEvent -LogName 'System' -MaxEvents 50 -ErrorAction SilentlyContinue |
    Where-Object { $_.Message -match 'ProxyPool|nssm' } |
    Select-Object -First 5 TimeCreated, Id, @{N='Msg';E={$_.Message.Substring(0,[Math]::Min(200,$_.Message.Length))}} |
    Format-List

Write-Host "`n=== nssm 服务状态 ==="
cd "D:\Documents\z_python_data_analy\Quent\Sys_trader\tools\proxy_pool"
& .\nssm.exe status ProxyPoolService

Write-Host "`n=== 尝试直接启动并查看输出 ==="
& .\nssm.exe start ProxyPoolService 2>&1
Start-Sleep -Seconds 3
sc.exe query ProxyPoolService | Select-String "STATE|EXIT"