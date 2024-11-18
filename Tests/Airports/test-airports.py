import time
import psutil
import mysql.connector
from pymongo import MongoClient
import csv


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


def test_mariadb_query(query):
    """Funkcja do testowania zapytań w MariaDB"""
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='mariadb',
            password='P@ssw0rd',
            database='Airports'
        )
        cursor = conn.cursor()

        start_time = time.time()
        print(f"MariaDB: Executing query: {query}")
        cursor.execute(query)

        result = cursor.fetchmany(100)
        while result:
            result = cursor.fetchmany(100)

        end_time = time.time()
        query_time = end_time - start_time
        print(f"Query executed in {query_time} seconds.")

        cursor.close()
        conn.close()

        return query_time

    except mysql.connector.Error as err:
        print(f"MariaDB Error: {err}")
        return None

    except Exception as e:
        print(f"General error: {e}")
        return None


def test_mongodb_query(collection_name, query, projection=None):
    """Funkcja do testowania zapytań w MongoDB"""
    try:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        db = client['Airports']
        collection = db[collection_name]

        start_time = time.time()
        print(f"MongoDB: Executing query on collection '{collection_name}': {query}")
        
        cursor = collection.find(query, projection) if projection else collection.find(query)
        all_results = [doc for doc in cursor]

        end_time = time.time()
        query_time = end_time - start_time
        print(f"Query executed in {query_time} seconds. Total fetched: {len(all_results)}")

        client.close()

        return query_time

    except Exception as e:
        print(f"Error: {e}")
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
    Funkcja do jednorazowego testowania wydajności bazy danych.
    Wykonuje zapytania do baz danych, zbiera statystyki systemowe
    i zapisuje wynik w pliku CSV.
    """
    queries = {
        'MariaDB': [
            "SELECT * FROM Flights WHERE ARRIVAL_DELAY > 60;",
            "SELECT AIRLINE FROM Airlines WHERE IATA_CODE = 'AA';",
            "SELECT AIRPORT, CITY FROM Airports WHERE STATE = 'CA';" 
        ],
        'MongoDB': [
            {
                'collection': 'Flights',
                'query': {"ARRIVAL_DELAY": {"$gt": 60}},
                'projection': None
            },
            {
                'collection': 'Airlines',
                'query': {"IATA_CODE": "AA"},
                'projection': {"AIRLINE": 1, "_id": 0}
            },
            { 
                'collection': 'Airports',
                'query': { "STATE": "CA" },
                'projection': { "AIRPORT": 1, "CITY": 1, "_id": 0 }  
            }
        ]
    }

    for query in queries['MariaDB']:
        mariadb_query_time = test_mariadb_query(query)
        system_stats = collect_system_stats()
        system_stats['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
        system_stats['database'] = 'MariaDB'
        system_stats['query_time'] = mariadb_query_time
        save_to_csv(system_stats)

    for query_set in queries['MongoDB']:
        mongodb_query_time = test_mongodb_query(
            query_set['collection'], query_set['query'], query_set['projection']
        )
        system_stats = collect_system_stats()
        system_stats['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
        system_stats['database'] = 'MongoDB'
        system_stats['query_time'] = mongodb_query_time
        save_to_csv(system_stats)


if __name__ == '__main__':
    for i in range(3):
        test_database_performance()
