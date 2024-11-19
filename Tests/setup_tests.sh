#!/usr/bin/env bash
#------------------
# Script for setting venv and running tests

# Sprawdzenie, czy Python jest zainstalowany
if ! command -v python3 &> /dev/null; then
    echo "Python3 nie jest zainstalowany lub nie jest dodany do zmiennej PATH."
    exit 1
fi

# Tworzenie środowiska wirtualnego o nazwie .venv
echo "Tworzenie środowiska wirtualnego .venv..."
python3 -m venv .venv

# Sprawdzenie, czy środowisko zostało utworzone
if [ -d ".venv" ]; then
    echo "Środowisko wirtualne utworzone pomyślnie."
else
    echo "Nie udało się utworzyć środowiska wirtualnego."
    exit 1
fi

# Aktywowanie środowiska wirtualnego
echo "Aktywowanie środowiska wirtualnego..."
source .venv/bin/activate

# Instalacja zależności
echo "Instalacja zależności..."
pip install --upgrade pip
pip install pymongo psutil mysql mysql-connector-python

# Sprawdzenie instalacji zależności
if [ $? -eq 0 ]; then
    echo "Zależności zostały zainstalowane pomyślnie."
else
    echo "Nie udało się zainstalować zależności."
    exit 1
fi

# Uruchomienie skryptu Pythona
echo "Uruchamianie skryptu do testowania..."
python3 ./run_tests.py

# Dezaktywacja środowiska wirtualnego
deactivate

# Przenieś wynik do Home
mv ./system_stats.xlsx "$HOME"

echo "Skrypt zakończył działanie."