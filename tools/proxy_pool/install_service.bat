@echo off
chcp 65001 >nul
echo ===== Proxy Pool Service 安装脚本 =====
echo.

set "SCRIPT_DIR=%~dp0"
set "NSSM_URL=https://nssm.cc/release/nssm-2.24-101-g897c7ad.zip"
set "NSSM_ZIP=%TEMP%\nssm.zip"
set "NSSM_DIR=%TEMP%\nssm"
set "NSSM_EXE=%SCRIPT_DIR%nssm.exe"
set "PYTHON_EXE=python"
set "SERVICE_NAME=ProxyPoolService"

:: 检查是否已安装
sc query %SERVICE_NAME% >nul 2>&1
if %errorlevel% equ 0 (
    echo [*] 服务 %SERVICE_NAME% 已存在，正在卸载...
    nssm stop %SERVICE_NAME%
    nssm remove %SERVICE_NAME% confirm
)

:: 检查 nssm
if not exist "%NSSM_EXE%" (
    echo [ERR] nssm.exe 未找到，请先下载 nssm
    pause
    exit /b 1
) else (
    echo [OK] nssm 已就绪
)

:: 注册服务
echo [*] 注册 Windows 服务...
"%NSSM_EXE%" install %SERVICE_NAME% "%PYTHON_EXE%" "%SCRIPT_DIR%proxy_pool.py --service"
"%NSSM_EXE%" set %SERVICE_NAME% AppDirectory "%SCRIPT_DIR%"
"%NSSM_EXE%" set %SERVICE_NAME% Start SERVICE_AUTO_START
"%NSSM_EXE%" set %SERVICE_NAME% AppStdout "%SCRIPT_DIR%logs\service.log"
"%NSSM_EXE%" set %SERVICE_NAME% AppStderr "%SCRIPT_DIR%logs\service.err"
"%NSSM_EXE%" set %SERVICE_NAME% AppRotateFiles 1
"%NSSM_EXE%" set %SERVICE_NAME% AppRotateOnline 1
"%NSSM_EXE%" set %SERVICE_NAME% AppRotateSeconds 86400
"%NSSM_EXE%" set %SERVICE_NAME% AppRestartDelay 5000

:: 启动服务
echo [*] 启动服务...
"%NSSM_EXE%" start %SERVICE_NAME%

echo.
echo ===== 安装完成 =====
echo 服务名称: %SERVICE_NAME%
echo 管理命令:
echo   nssm start %SERVICE_NAME%     启动
echo   nssm stop %SERVICE_NAME%      停止
echo   nssm restart %SERVICE_NAME%   重启
echo   nssm status %SERVICE_NAME%    状态
echo.
pause