@echo off
rem ===== SystraderService installer (nssm) =====
rem ASCII-only: cmd parses batch files in the active codepage (GBK on
rem Chinese Windows); UTF-8 comments break execution. Keep this file ASCII.
echo ===== SystraderService installer =====
echo.

rem ===== Admin check =====
net session >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERR] Administrator privileges required.
    echo       Right-click this script and select "Run as administrator".
    pause
    exit /b 1
)

rem ===== Paths & constants =====
set "SCRIPT_DIR=%~dp0"
set "ROOT_DIR=%~dp0.."
set "NSSM_EXE=%SCRIPT_DIR%proxy_pool\nssm.exe"
rem Prefer absolute python (SYSTEM account lacks user PATH); fall back to PATH lookup
set "PYTHON_EXE=python"
if exist "C:\Users\Evan\anaconda3\python.exe" set "PYTHON_EXE=C:\Users\Evan\anaconda3\python.exe"
set "SERVICE_NAME=SystraderService"
set "SERVICE_CMD=-m shared.runner --execution-mode live --instance live"

rem ===== nssm check =====
if not exist "%NSSM_EXE%" (
    echo [ERR] nssm.exe not found: %NSSM_EXE%
    echo       Reuse tools\proxy_pool\nssm.exe or download from https://nssm.cc
    pause
    exit /b 1
) else (
    echo [OK] nssm ready
)

rem ===== Existing service check =====
sc query %SERVICE_NAME% >nul 2>&1
if %errorlevel% equ 0 (
    echo [WARN] Service %SERVICE_NAME% already exists.
    choice /c YN /m "Stop and reinstall it? (Y/N)"
    if errorlevel 2 (
        echo Cancelled.
        pause
        exit /b 0
    )
    echo [*] Stopping and removing old service...
    "%NSSM_EXE%" stop %SERVICE_NAME% >nul 2>&1
    "%NSSM_EXE%" remove %SERVICE_NAME% confirm
)

rem ===== Install confirmation =====
echo.
echo Will install:
echo   Service  : %SERVICE_NAME%
echo   Command  : %PYTHON_EXE% %SERVICE_CMD%
echo   WorkDir  : %ROOT_DIR%
echo   Start    : AUTO
echo   Logs     : %ROOT_DIR%\logs\systrader-service.log / .err
echo   Restart  : on crash (AppExit Default Restart, 5s delay)
echo   Rotate   : daily
echo.
choice /c YN /m "Confirm install? (Y/N)"
if errorlevel 2 (
    echo Cancelled.
    pause
    exit /b 0
)

rem ===== Register service =====
echo [*] Registering Windows service...
"%NSSM_EXE%" install %SERVICE_NAME% "%PYTHON_EXE%" "%SERVICE_CMD%"
"%NSSM_EXE%" set %SERVICE_NAME% AppDirectory "%ROOT_DIR%"
"%NSSM_EXE%" set %SERVICE_NAME% Start SERVICE_AUTO_START
"%NSSM_EXE%" set %SERVICE_NAME% AppStdout "%ROOT_DIR%\logs\systrader-service.log"
"%NSSM_EXE%" set %SERVICE_NAME% AppStderr "%ROOT_DIR%\logs\systrader-service.err"
"%NSSM_EXE%" set %SERVICE_NAME% AppRotateFiles 1
"%NSSM_EXE%" set %SERVICE_NAME% AppRotateOnline 1
"%NSSM_EXE%" set %SERVICE_NAME% AppRotateSeconds 86400
"%NSSM_EXE%" set %SERVICE_NAME% AppRestartDelay 5000
"%NSSM_EXE%" set %SERVICE_NAME% AppExit Default Restart

rem ===== Start service =====
echo [*] Starting service...
"%NSSM_EXE%" start %SERVICE_NAME%
if %errorlevel% neq 0 (
    echo [ERR] Service failed to start. Check: %ROOT_DIR%\logs\systrader-service.err
)

echo.
echo ===== Install complete =====
echo Service : %SERVICE_NAME%
echo Manage  :
echo   "%NSSM_EXE%" start %SERVICE_NAME%
echo   "%NSSM_EXE%" stop %SERVICE_NAME%
echo   "%NSSM_EXE%" restart %SERVICE_NAME%
echo   "%NSSM_EXE%" status %SERVICE_NAME%
echo   "%NSSM_EXE%" remove %SERVICE_NAME% confirm
echo   "%NSSM_EXE%" edit %SERVICE_NAME%
echo Logs    :
echo   Get-Content "%ROOT_DIR%\logs\systrader-service.log" -Wait
echo   Get-Content "%ROOT_DIR%\logs\systrader-service.err" -Wait
echo.
echo NOTE: if the python above is wrong, edit PYTHON_EXE in this script and rerun.
echo.
pause
