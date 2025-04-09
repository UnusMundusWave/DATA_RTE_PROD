title RTE Monitoring - Suivi de Production Électrique
color 1E
@echo off
setlocal enabledelayedexpansion

:: Configuration
set PYTHON_SCRIPTS=_1_getTransparencyAPI.py _2_parser_csv.py _3_import_csv.py _4_ProductionReporting_Telegram_bot.py
set TARGET_MINUTE=28
set LAST_RUN_HOUR=-1

:: Get the directory of the batch script
set WORKING_DIR=%~dp0

:: Remove trailing backslash from WORKING_DIR
if "%WORKING_DIR:~-1%"=="\" set "WORKING_DIR=%WORKING_DIR:~0,-1%"

echo ======================================================
echo   RTE Monitoring - Suivi de Production Électrique
echo   Monitoring horaire à XX:%TARGET_MINUTE%
echo ======================================================
echo.

:: Activate the virtual environment
call "%WORKING_DIR%\Scripts\activate.bat"

if errorlevel 1 (
    echo Erreur lors de l'activation de l'environnement virtuel.
    pause
    exit /b 1
)

:main_loop
cls
:: Get current hour to prevent multiple executions in the same hour
for /f "tokens=1-3 delims=:." %%a in ("%TIME%") do (
    set /a CURRENT_HOUR=%%a
)

:: Skip execution if we've already run in this hour
if %CURRENT_HOUR% EQU %LAST_RUN_HOUR% (
    echo Cycle déjà exécuté pour cette heure. Attente du prochain cycle...
    goto calculate_wait
)

echo ======================================================
echo   RTE Monitoring - Cycle de traitement
echo   Heure de démarrage: %TIME%
echo ======================================================
echo.

:: Execute Python scripts
set SCRIPT_COUNT=0
for %%S in (%PYTHON_SCRIPTS%) do (
    set /a SCRIPT_COUNT+=1
    echo Exécution de %%S...
    python "%WORKING_DIR%\%%S"
    if errorlevel 1 (
        echo Erreur lors de l'exécution de %%S
        pause
        exit /b 1
    )
    
    :: Add 60-second pause after the 4th Python script
    if !SCRIPT_COUNT! EQU 4 (
        echo.
        echo Pause de 1 secondes après l'exécution du dernier script...
        timeout /t 1 /nobreak >nul
    )
)

:: Record the hour of this execution
set LAST_RUN_HOUR=%CURRENT_HOUR%

echo.
echo Cycle terminé avec succès à %TIME%
echo Prochain cycle programmé à XX:%TARGET_MINUTE%
echo.

:: Calculate time until next hour's target minute
:calculate_wait
    :: Récupération de l'heure avec gestion des espaces
    set TIME_STRING=%TIME: =%
    for /f "tokens=1-3 delims=:." %%a in ("%TIME_STRING%") do (
        set /a CURRENT_HOUR=1%%a %% 100
        set /a CURRENT_MINUTE=1%%b %% 100
        set /a CURRENT_SECOND=1%%c %% 100
    )
    
    :: If we're already at or past the target minute, wait until the next hour
    if %CURRENT_MINUTE% GEQ %TARGET_MINUTE% (
        set /a MINUTES_TO_WAIT=60 - %CURRENT_MINUTE% + %TARGET_MINUTE%
    ) else (
        set /a MINUTES_TO_WAIT=%TARGET_MINUTE% - %CURRENT_MINUTE%
    )
    
    set /a SECONDS_TO_WAIT=(%MINUTES_TO_WAIT% * 60) - %CURRENT_SECOND%
    
    echo Attente de %SECONDS_TO_WAIT% secondes jusqu'au prochain cycle (XX:%TARGET_MINUTE%)
    echo Heure actuelle : %TIME%
    echo.

:wait_loop
    :: Check if it's time to run the scripts again
    set TIME_STRING=%TIME: =%
    for /f "tokens=1-3 delims=:." %%a in ("%TIME_STRING%") do (
        set /a CHECK_HOUR=1%%a %% 100
        set /a CHECK_MINUTE=1%%b %% 100
    )
    
    :: Only run if we're at the target minute AND we haven't run in this hour yet
    if %CHECK_MINUTE% EQU %TARGET_MINUTE% if %CHECK_HOUR% NEQ %LAST_RUN_HOUR% (
        echo Heure cible atteinte! Démarrage du nouveau cycle...
        goto main_loop
    )
    
    if %SECONDS_TO_WAIT% LEQ 0 (
        echo Temps d'attente écoulé, vérification de l'heure...
        goto check_time
    )
    
    :: Wait in smaller increments and show progress
    set /a WAIT_INCREMENT=1
    if %SECONDS_TO_WAIT% LSS %WAIT_INCREMENT% set /a WAIT_INCREMENT=%SECONDS_TO_WAIT%
    
    :: echo Prochain cycle dans %SECONDS_TO_WAIT% secondes...
    title %SECONDS_TO_WAIT% - RTE Monitoring - Suivi de Production Électrique


    timeout /t %WAIT_INCREMENT% /nobreak >nul
    set /a SECONDS_TO_WAIT-=%WAIT_INCREMENT%
    goto wait_loop

:check_time
    :: Double check that we're at the right minute AND we haven't run in this hour yet
    set TIME_STRING=%TIME: =%
    for /f "tokens=1-3 delims=:." %%a in ("%TIME_STRING%") do (
        set /a CHECK_HOUR=1%%a %% 100
        set /a CHECK_MINUTE=1%%b %% 100
    )
    
    :: Allow a +/-1 minute tolerance
    set /a MIN_TARGET=%TARGET_MINUTE% - 1
    set /a MAX_TARGET=%TARGET_MINUTE% + 1
    
    if %CHECK_MINUTE% GEQ %MIN_TARGET% if %CHECK_MINUTE% LEQ %MAX_TARGET% if %CHECK_HOUR% NEQ %LAST_RUN_HOUR% (
        echo L'heure cible est atteinte. Lancement du nouveau cycle...
        goto main_loop
    ) else (
        echo L'heure cible n'est pas encore atteinte ou le cycle a déjà été exécuté cette heure.
        echo Recalcul du temps d'attente...
        goto calculate_wait
    )

goto main_loop

:end
deactivate
endlocal
exit /b 0