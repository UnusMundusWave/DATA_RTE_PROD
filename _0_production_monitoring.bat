@echo off
setlocal enabledelayedexpansion

:: Configuration
set PYTHON_SCRIPTS=_1_getTransparencyAPI.py _2_parser_csv.py _3_import_csv.py _4_ProductionReporting_Telegram_bot.py
set TARGET_MINUTE=50  :: Exécution à XX:50

:main_loop
cls
echo Démarrage du cycle de traitement...

:: Exécution des scripts Python
for %%S in (%PYTHON_SCRIPTS%) do (
    echo Exécution de %%S...
    python %%S
    if errorlevel 1 (
        echo Erreur lors de l'exécution de %%S
        pause
        exit /b 1
    )
)

echo Cycle terminé. Prochain cycle à XX:%TARGET_MINUTE%...
:: Calcul du temps d'attente
set CURRENT_HOUR=%TIME:~0,2%
set CURRENT_MINUTE=%TIME:~3,2%
set CURRENT_SECOND=%TIME:~6,2%

:: Conversion en nombres décimaux (supprime les zéros non significatifs)
set /a "CURRENT_HOUR=10%CURRENT_HOUR% %% 100, CURRENT_MINUTE=10%CURRENT_MINUTE% %% 100, CURRENT_SECOND=10%CURRENT_SECOND% %% 100"

set /a "WAIT_TIME=(TARGET_MINUTE - CURRENT_MINUTE) * 60 - CURRENT_SECOND"

:: Ajustement si l'heure est déjà passée
if %WAIT_TIME% lss 0 set /a WAIT_TIME+=3600

set /a COUNTDOWN=%WAIT_TIME%
if %COUNTDOWN% equ 0 (
    echo Démarrage immédiat!
    timeout /t 2 >nul
    goto main_loop
)

:countdown
set /a MINUTES=COUNTDOWN/60
set /a SECONDS=COUNTDOWN%%60

:: Effacer la ligne précédente
for /l %%i in (1,1,80) do <nul set /p=.

:: Afficher le nouveau compte à rebours
<nul set /p="Prochain cycle dans : !MINUTES! minutes !SECONDS! secondes   "

timeout /t 1 /nobreak >nul
set /a COUNTDOWN-=1
if !COUNTDOWN! geq 0 goto countdown

echo.
goto main_loop 