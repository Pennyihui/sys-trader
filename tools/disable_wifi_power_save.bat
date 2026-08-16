@echo off
chcp 65001 >nul
echo ============================================
echo   禁用无线网卡节能 - 一键修复
echo   (解决周期性断连 / ping超时问题)
echo ============================================
echo.

:: 检查管理员权限
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERR] 请右键此文件，选择"以管理员身份运行"
    pause
    exit /b 1
)

echo [1/3] 禁用系统关闭网卡节能 (PnPCapabilities=0x100)...
reg add "HKLM\SYSTEM\CurrentControlSet\Control\Class\{4d36e972-e325-11ce-bfc1-08002be10318}" /f >nul 2>&1

:: 找到 Intel 无线网卡的子键并设置
set "FOUND=0"
for /f "delims=" %%i in ('reg query "HKLM\SYSTEM\CurrentControlSet\Control\Class\{4d36e972-e325-11ce-bfc1-08002be10318}" /s /f "Intel(R) Wireless" /d 2^>nul ^| findstr /i "HKLM"') do (
    reg add "%%i" /v PnPCapabilities /t REG_DWORD /d 0x100 /f
    echo   [OK] 已设置: %%i
    set "FOUND=1"
)
if "%FOUND%"=="0" (
    echo   [WARN] 未通过搜索找到，尝试已知路径...
    for /d %%d in ("HKLM\SYSTEM\CurrentControlSet\Control\Class\{4d36e972-e325-11ce-bfc1-08002be10318}\00*") do (
        reg query "%%d" /v DriverDesc 2>nul | findstr /i "Intel" >nul && (
            reg add "%%d" /v PnPCapabilities /t REG_DWORD /d 0x100 /f
            echo   [OK] 已设置: %%d
        )
    )
)

echo.
echo [2/3] 禁用 MIMO 节能 (SMPS -> No SMPS)...
powershell -ExecutionPolicy Bypass -Command "Set-NetAdapterAdvancedProperty -Name 'WLAN' -RegistryKeyword 'MIMOPowerSaveMode' -RegistryValue 2 -ErrorAction SilentlyContinue"
if %errorlevel% equ 0 (
    echo   [OK] MIMO 电源模式已改为 无SMPS
) else (
    echo   [INFO] 若失败请在设备管理器手动改: 高级 - MIMO电源模式 - 无SMPS
)

echo.
echo [3/3] 重启无线网卡使设置生效...
powershell -ExecutionPolicy Bypass -Command "Restart-NetAdapter -Name 'WLAN' -ErrorAction SilentlyContinue; Write-Host '  网卡已重启'"

echo.
echo ============================================
echo   完成! 节能已禁用
echo   下一步建议: 更新网卡驱动(当前为2020年旧版)
echo ============================================
pause