#!/bin/bash

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

# Informacja o aktywacji środowiska
echo "Środowisko wirtualne .venv jest aktywne."
