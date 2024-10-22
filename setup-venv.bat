@echo off
:: Sprawdzenie, czy Python jest zainstalowany
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Python nie jest zainstalowany lub nie jest dodany do zmiennej PATH.
    exit /b 1
)

:: Tworzenie środowiska wirtualnego o nazwie .venv
echo Tworzenie środowiska wirtualnego .venv...
python -m venv .venv

:: Sprawdzenie, czy środowisko zostało utworzone
if exist ".venv\Scripts\activate.bat" (
    echo Środowisko wirtualne utworzone pomyślnie.
) else (
    echo Nie udalo sie utworzyć środowiska wirtualnego.
    exit /b 1
)

:: Aktywowanie środowiska wirtualnego
echo Aktywowanie środowiska wirtualnego...
call .venv\Scripts\activate.bat

:: Informacja o aktywacji środowiska
echo Środowisko wirtualne .venv jest aktywne.
