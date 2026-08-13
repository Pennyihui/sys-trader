@echo off
chcp 65001 >nul
echo ============================================
echo   Network Monitor 服务安装
echo   (右键 -> 以管理员身份运行)
echo ============================================
echo.

net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERR] 请右键此文件，选择"以管理员身份运行"
    pause
    exit /b 1
)

set "SCRIPT_DIR=%~dp0"
set "NSSM_EXE=%SCRIPT_DIR%..\proxy_pool\nssm.exe"
set "PYTHON_EXE=E:\Anaconda3\python.exe"
set "SCRIPT=%SCRIPT_DIR%network_monitor.py"
set "SERVICE_NAME=NetworkMonitorService"

if not exist "%NSSM_EXE%" (
    echo [ERR] nssm.exe 未找到: %NSSM_EXE%
    pause
    exit /b 1
)

echo [*] 卸载旧服务（如果存在）...
sc query %SERVICE_NAME% >nul 2>&1
if %errorlevel% equ 0 (
    "%NSSM_EXE%" stop %SERVICE_NAME%
    "%NSSM_EXE%" remove %SERVICE_NAME% confirm
)

echo [*] 注册服务...
"%NSSM_EXE%" install %SERVICE_NAME% "%PYTHON_EXE%" "%SCRIPT%"
"%NSSM_EXE%" set %SERVICE_NAME% AppDirectory "%SCRIPT_DIR%"
"%NSSM_EXE%" set %SERVICE_NAME% Start SERVICE_AUTO_START
"%NSSM_EXE%" set %SERVICE_NAME% AppStdout "%SCRIPT_DIR%logs\service.log"
"%NSSM_EXE%" set %SERVICE_NAME% AppStderr "%SCRIPT_DIR%logs\service.err"
"%NSSM_EXE%" set %SERVICE_NAME% AppRestartDelay 5000

echo [*] 启动服务...
"%NSSM_EXE%" start %SERVICE_NAME%

echo.
echo ============================================
echo   完成! 服务: %SERVICE_NAME%
echo   API:  http://127.0.0.1:8766
echo ============================================
pause