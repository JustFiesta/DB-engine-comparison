import time
import psutil
import mysql.connector
from pymongo import MongoClient
import csv
import statistics

def collect_system_stats():
    """Funkcja zbierająca statystyki systemowe, w tym użycie dysku"""
    process = psutil.Process()
    stats = {
        'cpu_percent': psutil.cpu_percent(interval=1),
        'memory_percent': psutil.virtual_memory().percent,
        'read_bytes': process.io_counters().read_bytes,
        'write_bytes': process.io_counters().write_bytes,
        'open_files': len(process.open_files()),
        'disk_usage_percent': psutil.disk_usage('/').percent, 
        'disk_total': psutil.disk_usage('/').total,             
        'disk_used': psutil.disk_usage('/').used,             
        'disk_free': psutil.disk_usage('/').free                
    }
    return stats

def test_mariadb_query(query_type):
    """Funkcja do testowania zapytań w MariaDB"""
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='mariadb',
            password='P@ssw0rd',
            database='Airports'
        )
        cursor = conn.cursor()
        
        # Wybór zapytania na podstawie parametru
        if query_type == 'flights':
            query = "SELECT * FROM Flights WHERE ARRIVAL_DELAY > 60;"
        elif query_type == 'airlines':
            query = "SELECT AIRLINE FROM Airlines WHERE IATA_CODE = 'AA';"
        elif query_type == 'airports':
            query = "SELECT AIRPORT, CITY FROM Airports WHERE STATE = 'CA';"
        else:
            raise ValueError("Nieznany typ zapytania")

        start_time = time.time()

        cursor.execute(query)
        
        result = cursor.fetchmany(100)  
        while result:
            result = cursor.fetchmany(100)

        end_time = time.time()
        query_time = end_time - start_time
        
        cursor.close()
        conn.close()

        return query_time
        
    except Exception as e:
        print(f"MariaDB Error: {e}")
        return None

def test_mongodb_query(query_type):
    """Funkcja do testowania zapytań w MongoDB"""
    try:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        db = client['Airports']
        
        # Wybór kolekcji i zapytania na podstawie parametru
        if query_type == 'flights':
            collection = db['Flights']
            query = {"ARRIVAL_DELAY": {"$gt": 60}}
            projection = None
        elif query_type == 'airlines':
            collection = db['Airlines']
            query = { "IATA_CODE": "AA" }
            projection = { "AIRLINE": 1, "_id": 0 }
        elif query_type == 'airports':
            collection = db['Airports']
            query = { "STATE": "CA" } 
            projection = { "AIRPORT": 1, "CITY": 1, "_id": 0 }
        else:
            raise ValueError("Nieznany typ zapytania")

        start_time = time.time()

        cursor = collection.find(query, projection)
        all_results = list(cursor)

        end_time = time.time()
        query_time = end_time - start_time

        client.close()

        return query_time

    except Exception as e:
        print(f"MongoDB Error: {e}")
        return None

def save_to_csv(data, filename="system_stats.csv"):
    """Funkcja zapisująca wyniki do pliku CSV"""
    with open(filename, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data.keys())
        if file.tell() == 0:
            writer.writeheader()
        writer.writerow(data)

def test_database_performance():
    """
    Funkcja do testowania wydajności bazy danych.
    Wykonuje zapytania do baz danych, zbiera statystyki systemowe
    i zapisuje wynik w pliku CSV.
    """
    # Lista typów zapytań
    query_types = ['flights', 'airlines', 'airports']
    
    # Wykonanie testów dla każdego typu zapytania
    for query_type in query_types:
        print(f"\nTesting query type: {query_type}")
        
        # Lista czasów wykonania zapytań
        mariadb_query_times = []
        mongodb_query_times = []

        # Wielokrotne wykonanie testów
        for run in range(3):
            print(f"\nRun {run + 1}:")
            
            # Testowanie MariaDB
            mariadb_query_time = test_mariadb_query(query_type)
            if mariadb_query_time is not None:
                mariadb_query_times.append(mariadb_query_time)
                print(f"MariaDB {query_type} query time: {mariadb_query_time:.4f} seconds")

            # Testowanie MongoDB
            mongodb_query_time = test_mongodb_query(query_type)
            if mongodb_query_time is not None:
                mongodb_query_times.append(mongodb_query_time)
                print(f"MongoDB {query_type} query time: {mongodb_query_time:.4f} seconds")

        # Obliczenie statystyk
        if mariadb_query_times:
            mariadb_avg_time = statistics.mean(mariadb_query_times)
            mariadb_std_time = statistics.stdev(mariadb_query_times)
            print(f"\nMariaDB {query_type} - Average query time: {mariadb_avg_time:.4f} ± {mariadb_std_time:.4f} seconds")

        if mongodb_query_times:
            mongodb_avg_time = statistics.mean(mongodb_query_times)
            mongodb_std_time = statistics.stdev(mongodb_query_times)
            print(f"MongoDB {query_type} - Average query time: {mongodb_avg_time:.4f} ± {mongodb_std_time:.4f} seconds")

        # Zbieranie statystyk systemowych
        system_stats = collect_system_stats()
        system_stats['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
        system_stats['query_type'] = query_type

        # Zapis wyników dla MariaDB
        if mariadb_query_times:
            mariadb_stats = system_stats.copy()
            mariadb_stats['database'] = 'MariaDB'
            mariadb_stats['avg_query_time'] = mariadb_avg_time
            mariadb_stats['query_std_dev'] = mariadb_std_time
            save_to_csv(mariadb_stats)

        # Zapis wyników dla MongoDB
        if mongodb_query_times:
            mongodb_stats = system_stats.copy()
            mongodb_stats['database'] = 'MongoDB'
            mongodb_stats['avg_query_time'] = mongodb_avg_time
            mongodb_stats['query_std_dev'] = mongodb_std_time
            save_to_csv(mongodb_stats)

if __name__ == '__main__':
    test_database_performance()