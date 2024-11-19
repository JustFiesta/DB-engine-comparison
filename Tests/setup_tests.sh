#!/usr/bin/env bash
#------------------
# Script for setting up venv and running tests

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
pip install pymongo psutil mysql-connector-python

# Sprawdzenie instalacji zależności
if [ $? -eq 0 ]; then
    echo "Zależności zostały zainstalowane pomyślnie."
else
    echo "Nie udało się zainstalować zależności."
    deactivate
    exit 1
fi

# Wyszukanie i uruchomienie trzech skryptów testowych Python
echo "Wyszukiwanie testów Python w bieżącym katalogu..."
test_files=( $(find . -maxdepth 1 -type f -name "test_*.py" | sort | grep -v "$(basename "$0")") )

if [ ${#test_files[@]} -eq 0 ]; then
    echo "Nie znaleziono żadnych skryptów testowych w bieżącym katalogu."
    deactivate
    exit 1
fi

echo "Znaleziono następujące testy: ${test_files[*]}"
for test_file in "${test_files[@]}"; do
    echo "Uruchamianie testu: $test_file"
    python3 "$test_file"

    # Obsługa błędów
    if [ $? -ne 0 ]; then
        echo "Błąd podczas uruchamiania testu: $test_file"
        deactivate
        exit 1
    fi
done

# Dezaktywacja środowiska wirtualnego
deactivate

# Przeniesienie wyników do katalogu domowego
if [ -f "./system_stats.xlsx" ]; then
    echo "Przenoszenie pliku wynikowego do katalogu domowego..."
    mv ./system_stats.xlsx "$HOME"
else
    echo "Brak pliku wynikowego do przeniesienia."
fi

echo "Skrypt zakończył działanie."
