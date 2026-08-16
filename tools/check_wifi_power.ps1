# 检查无线网卡电源管理设置
Write-Host "=== 1. 无线网卡信息 ==="
Get-NetAdapter WLAN | Select-Object Name, InterfaceDescription, Status | Format-List

Write-Host "=== 2. 网卡 PnPCapabilities (0x100=禁止节能, 0或缺失=允许节能) ==="
$base = "HKLM:\SYSTEM\CurrentControlSet\Control\Class\{4d36e972-e325-11ce-bfc1-08002be10318}"
Get-ChildItem $base -ErrorAction SilentlyContinue | ForEach-Object {
    try {
        $props = Get-ItemProperty $_.PSPath -ErrorAction Stop
        if ($props.DriverDesc -match 'Intel|Wireless|WLAN|Wi-Fi|9560') {
            Write-Host ("{0}`n  PnPCapabilities = {1} (0x{1:X})" -f $props.DriverDesc, $props.PnPCapabilities)
            if ($props.PnPCapabilities -eq 0x100) {
                Write-Host "  => 节能已禁用（好）"
            } elseif ($null -eq $props.PnPCapabilities) {
                Write-Host "  => 节能开启（问题！需要设置为0x100）"
            } else {
                Write-Host "  => 节能可能开启"
            }
        }
    } catch {}
}

Write-Host "`n=== 3. 电源计划中的无线适配器设置 ==="
powercfg /query SCHEME_CURRENT 2>&1 | Select-String -Pattern "无线|Wireless|Power Saving" -Context 0,6

Write-Host "`n=== 4. Intel 网卡高级电源设置 (驱动级) ==="
Get-NetAdapterAdvancedProperty WLAN -ErrorAction SilentlyContinue | Where-Object { $_.DisplayName -match "Power|节能|U-APSD|Sleep" } | Select-Object DisplayName, DisplayValue | Format-Table -AutoSize