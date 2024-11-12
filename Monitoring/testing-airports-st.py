import threading
import time
import psutil
from pymongo import MongoClient
import mysql.connector
from concurrent.futures import ThreadPoolExecutor

# Połączenie z MongoDB
client_mongo = MongoClient("mongodb://localhost:27017/")
db_mongo = client_mongo['Airports']

# Połączenie z MariaDB
conn_mariadb = mysql.connector.connect(
    host="localhost",
    user="mariadb",
    password="P@ssw0rd",
    database="Airports"
)
cursor_mariadb = conn_mariadb.cursor()


# Funkcja monitorująca zasoby podczas zapytania
def monitoruj_zasoby(pid, monitoring_aktywny):
    proces = psutil.Process(pid)
    metryki = []

    while monitoring_aktywny[0]:  # Użycie listy, aby zaktualizować wartość z funkcji wywołującej
        cpu_usage = proces.cpu_percent(interval=1)
        ram_usage = proces.memory_info().rss / (1024 * 1024)  # W MB
        io_counters = proces.io_counters()
        open_files = len(proces.open_files())
        metryki.append({
            "cpu": cpu_usage,
            "ram": ram_usage,
            "read_bytes": io_counters.read_bytes,
            "write_bytes": io_counters.write_bytes,
            "open_files": open_files
        })
        time.sleep(1)
    return metryki


# Funkcja zbierająca końcowe metryki po zakończeniu zapytania
def zbierz_metryki_koncowe(proces):
    metryki_koncowe = {
        "cpu": proces.cpu_percent(),
        "ram": proces.memory_info().rss / (1024 * 1024),  # W MB
        "open_files": len(proces.open_files()),
        "io_counters": proces.io_counters()
    }
    return metryki_koncowe


# Funkcja wykonująca zapytanie w MongoDB
def wykonaj_zapytanie_mongo(collection, query):
    monitoring_aktywny_mongo = [True]  # Lista do przekazania jako referencja
    pid = client_mongo.admin.command('serverStatus')['pid']  # Pobranie PID MongoDB
    proces = psutil.Process(pid)

    # Uruchomienie monitoringu zasobów w osobnym wątku
    monitoring_thread = threading.Thread(target=monitoruj_zasoby, args=(pid, monitoring_aktywny_mongo))
    monitoring_thread.start()

    # Wykonanie zapytania
    start_time = time.time()
    result = db_mongo[collection].find(query).explain("executionStats")
    end_time = time.time()

    # Zakończenie monitoringu
    monitoring_aktywny_mongo[0] = False  # Zatrzymanie pętli w monitorującym wątku
    monitoring_thread.join()

    # Zbieranie końcowych metryk
    metryki_koncowe = zbierz_metryki_koncowe(proces)
    czas_wykonania = end_time - start_time

    print("\n--- MongoDB ---")
    print("Czas wykonania zapytania:", czas_wykonania, "s")
    print("Wynik explain:", result)
    print("Końcowe metryki:", metryki_koncowe)
    return czas_wykonania, metryki_koncowe


# Funkcja wykonująca zapytanie w MariaDB
def wykonaj_zapytanie_mariadb(query):
    monitoring_aktywny_mariadb = [True]  # Lista do przekazania jako referencja
    pid = conn_mariadb.connection_id  # Pobiera PID MariaDB
    proces = psutil.Process(pid)

    # Uruchomienie monitoringu zasobów w osobnym wątku
    with ThreadPoolExecutor() as executor:
        monitor_task = executor.submit(monitoruj_zasoby, pid, monitoring_aktywny_mariadb)
        start_time = time.time()

        # Wykonanie zapytania
        cursor_mariadb.execute(query)
        result = cursor_mariadb.fetchall()

        end_time = time.time()
        czas_wykonania = end_time - start_time

        # Zakończenie monitoringu
        monitoring_aktywny_mariadb[0] = False  # Zatrzymanie pętli w monitorującym wątku
        metryki_w_trakcie = monitor_task.result()
        metryki_koncowe = zbierz_metryki_koncowe(proces)

    print("\n--- MariaDB ---")
    print("Czas wykonania zapytania:", czas_wykonania, "s")
    print("Wynik zapytania:", result)
    print("Zebrane metryki podczas zapytania:", metryki_w_trakcie)
    print("Końcowe metryki:", metryki_koncowe)

    return czas_wykonania, metryki_koncowe


# Funkcja główna
def main():
    # Przykładowe zapytanie do MongoDB
    mongo_query = {"ARRIVAL_DELAY": {"$gt": 60}}
    mongo_czas, mongo_metryki = wykonaj_zapytanie_mongo("flights", mongo_query)

    # Przykładowe zapytanie do MariaDB
    mariadb_query = "SELECT * FROM flights WHERE ARRIVAL_DELAY > 60;"  
    mariadb_czas, mariadb_metryki = wykonaj_zapytanie_mariadb(mariadb_query)

    # Porównanie wyników
    print("\n--- Porównanie wyników ---")
    print(f"Czas wykonania MongoDB: {mongo_czas:.2f} s")
    print(f"Czas wykonania MariaDB: {mariadb_czas:.2f} s")

    if mongo_czas < mariadb_czas:
        print("MongoDB było szybsze.")
    elif mariadb_czas < mongo_czas:
        print("MariaDB było szybsze.")
    else:
        print("Oba zapytania miały taki sam czas wykonania.")

    print("\n--- Porównanie końcowych metryk ---")
    print("MongoDB metryki:", mongo_metryki)
    print("MariaDB metryki:", mariadb_metryki)


# Uruchomienie funkcji głównej
if __name__ == "__main__":
    main()
