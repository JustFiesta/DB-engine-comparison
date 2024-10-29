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

:: Instalacja zależności
echo Instalacja zależności...
pip install --upgrade pip
pip install faker pandas random

:: Sprawdzenie instalacji zależności
if %errorlevel% neq 0 (
    echo Nie udalo sie zainstalować zależności.
    exit /b 1
) else (
    echo Zależności zostały zainstalowane pomyślnie.
)

:: Uruchomienie skryptu Pythona
echo Uruchamianie skryptu do generowania danych...
python generate_data.py

:: Dezaktywacja środowiska wirtualnego
deactivate

echo Skrypt zakończył działanie.
