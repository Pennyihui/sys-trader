@echo off
chcp 65001 >nul
echo ===== SystraderService 安装脚本 =====
echo.

:: ==================== 管理员权限检查 ====================
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERR] 需要管理员权限！请右键本脚本，选择"以管理员身份运行"。
    pause
    exit /b 1
)

:: ==================== 路径与常量 ====================
set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%SCRIPT_DIR%.."
set "NSSM_EXE=%SCRIPT_DIR%proxy_pool\nssm.exe"
set "PYTHON_EXE=python"
set "SERVICE_NAME=SystraderService"
set "SERVICE_CMD=-m shared.runner --execution-mode live --instance live"

:: ==================== nssm 检查 ====================
if not exist "%NSSM_EXE%" (
    echo [ERR] nssm.exe 未找到: %NSSM_EXE%
    echo       可复用 tools\proxy_pool\nssm.exe，或从 https://nssm.cc 下载后放到该位置
    pause
    exit /b 1
) else (
    echo [OK] nssm 已就绪
)

:: ==================== 已安装检查 ====================
sc query %SERVICE_NAME% >nul 2>&1
if %errorlevel% equ 0 (
    echo [WARN] 服务 %SERVICE_NAME% 已存在。
    choice /c YN /m "是否先停止并卸载旧服务，再重新安装？(Y/N)"
    if errorlevel 2 (
        echo 已取消，退出。
        pause
        exit /b 0
    )
    echo [*] 停止并卸载旧服务...
    "%NSSM_EXE%" stop %SERVICE_NAME%
    "%NSSM_EXE%" remove %SERVICE_NAME% confirm
)

:: ==================== 安装确认 ====================
echo.
echo 将安装以下配置:
echo   服务名称 : %SERVICE_NAME%
echo   命令     : %PYTHON_EXE% %SERVICE_CMD%
echo   工作目录 : %ROOT_DIR%
echo   启动类型 : 自动 (SERVICE_AUTO_START)
echo   日志     : %ROOT_DIR%logs\systrader-service.log / systrader-service.err
echo   崩溃策略 : 自动重启 (AppExit Default Restart，延迟 5 秒)
echo   日志轮转 : 每日轮转 (AppRotateFiles)
echo.
choice /c YN /m "确认安装？(Y/N)"
if errorlevel 2 (
    echo 已取消，退出。
    pause
    exit /b 0
)

:: ==================== 注册服务 ====================
echo [*] 注册 Windows 服务...
"%NSSM_EXE%" install %SERVICE_NAME% "%PYTHON_EXE%" "%SERVICE_CMD%"
"%NSSM_EXE%" set %SERVICE_NAME% AppDirectory "%ROOT_DIR%"
"%NSSM_EXE%" set %SERVICE_NAME% Start SERVICE_AUTO_START
"%NSSM_EXE%" set %SERVICE_NAME% AppStdout "%ROOT_DIR%logs\systrader-service.log"
"%NSSM_EXE%" set %SERVICE_NAME% AppStderr "%ROOT_DIR%logs\systrader-service.err"
"%NSSM_EXE%" set %SERVICE_NAME% AppRotateFiles 1
"%NSSM_EXE%" set %SERVICE_NAME% AppRotateOnline 1
"%NSSM_EXE%" set %SERVICE_NAME% AppRotateSeconds 86400
"%NSSM_EXE%" set %SERVICE_NAME% AppRestartDelay 5000
"%NSSM_EXE%" set %SERVICE_NAME% AppExit Default Restart

:: ==================== 启动服务 ====================
echo [*] 启动服务...
"%NSSM_EXE%" start %SERVICE_NAME%
if %errorlevel% neq 0 (
    echo [ERR] 服务启动失败，请查看日志: %ROOT_DIR%logs\systrader-service.err
)

echo.
echo ===== 安装完成 =====
echo 服务名称: %SERVICE_NAME%
echo 管理命令:
echo   "%NSSM_EXE%" start %SERVICE_NAME%      启动
echo   "%NSSM_EXE%" stop %SERVICE_NAME%       停止
echo   "%NSSM_EXE%" restart %SERVICE_NAME%    重启
echo   "%NSSM_EXE%" status %SERVICE_NAME%     状态
echo   "%NSSM_EXE%" remove %SERVICE_NAME% confirm   卸载
echo   "%NSSM_EXE%" edit %SERVICE_NAME%       图形化配置
echo.
echo 查看日志:
echo   tail -f "%ROOT_DIR%logs\systrader-service.log"
echo   tail -f "%ROOT_DIR%logs\systrader-service.err"
echo   或 PowerShell: Get-Content "%ROOT_DIR%logs\systrader-service.log" -Wait
echo.
echo 注意: 若 python 不在系统 PATH，请将上方 %PYTHON_EXE% 替换为
echo       绝对路径（如 C:\Users\Evan\anaconda3\python.exe）后重装。
echo.
pause
