# 禁用无线网卡节能：设置 PnPCapabilities = 0x100
# 需要以管理员身份运行

Write-Host "=== 禁用无线网卡节能 ==="
$base = "HKLM:\SYSTEM\CurrentControlSet\Control\Class\{4d36e972-e325-11ce-bfc1-08002be10318}"

# 找到 Intel 无线网卡的注册表路径
$found = $false
Get-ChildItem $base -ErrorAction SilentlyContinue | ForEach-Object {
    try {
        $props = Get-ItemProperty $_.PSPath -ErrorAction Stop
        if ($props.DriverDesc -match 'Intel.*Wireless|9560|Wireless-AC') {
            $path = $_.PSPath
            $old = $props.PnPCapabilities
            Write-Host ("找到网卡: {0}" -f $props.DriverDesc)
            Write-Host ("路径: {0}" -f $_.Name)
            Write-Host ("当前 PnPCapabilities: {0} (0x{0:X})" -f $old)

            # 设置 PnPCapabilities = 0x100 (256) 表示禁止系统关闭设备节能
            Set-ItemProperty $path -Name "PnPCapabilities" -Value 0x100 -Type DWord

            # 验证
            $new = (Get-ItemProperty $path).PnPCapabilities
            Write-Host ("设置后 PnPCapabilities: {0} (0x{0:X})" -f $new)
            if ($new -eq 0x100) {
                Write-Host "`n[OK] 节能已禁用，重启网卡后生效"
                Write-Host "提示: 可以禁用再启用网卡，或重启电脑生效"
            } else {
                Write-Host "`n[WARN] 设置可能未生效（可能需要管理员权限）"
            }
            $found = $true
        }
    } catch {}
}

if (-not $found) {
    Write-Host "[ERR] 未找到 Intel 无线网卡"
}

Write-Host "`n=== 附加: 禁用 MIMO 节能 (SMPS) ==="
# MIMO Power Save: 通过 Set-NetAdapterAdvancedProperty 设置
# 2 = No SMPS (禁用 MIMO 节能)
try {
    Set-NetAdapterAdvancedProperty -Name "WLAN" -RegistryKeyword "MIMOPowerSaveMode" -RegistryValue 2 -ErrorAction Stop
    Write-Host "[OK] MIMO 电源模式已改为 无 SMPS (禁用节能)"
} catch {
    Write-Host "[WARN] 设置 MIMO 节能失败: $($_.Exception.Message)"
    Write-Host "       请在设备管理器手动设置: 网络适配器 -> Intel Wireless-AC 9560 -> 高级 -> MIMO电源模式 -> 无 SMPS"
}