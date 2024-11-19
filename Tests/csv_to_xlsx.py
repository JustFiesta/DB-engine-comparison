import pandas as pd

# Stałe ścieżki dla plików
CSV_FILE_PATH = "db_tests/system_stats.csv"
XLSX_FILE_PATH = "./system_stats.xlsx"

def csv_to_xlsx():
    """
    Konwertuje plik CSV na format XLSX.
    """

    try:
        # Wczytanie pliku CSV
        print(f"Przetwarzanie pliku: {CSV_FILE_PATH}")
        data = pd.read_csv(CSV_FILE_PATH)

        # Zapisanie pliku jako XLSX
        print(f"Zapisywanie pliku jako: {XLSX_FILE_PATH}")
        data.to_excel(XLSX_FILE_PATH, index=False, engine='openpyxl')
        print("Konwersja zakończona sukcesem!")

    except FileNotFoundError:
        print(f"Błąd: Plik {CSV_FILE_PATH} nie został znaleziony.")
    except Exception as e:
        print(f"Wystąpił nieoczekiwany błąd: {e}")

if __name__ == "__main__":
    csv_to_xlsx()
