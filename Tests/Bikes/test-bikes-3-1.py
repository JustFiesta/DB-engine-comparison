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

def test_mariadb_query():
    """Funkcja do testowania zapytań w MariaDB"""
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='mariadb',
            password='P@ssw0rd',
            database='bikes'
        )
        cursor = conn.cursor()
        query = "SELECT t.trip_id t.tripduration, s.station_name AS start_stat_name, e.station_name AS end_stat_name FROM TripUsers t JOIN Stations s ON t.start_station_id = s.station_id JOIN Stations e ON t.end_station_id = e.station_id WHERE t.tripduration > 20;"  
        start_time = time.time()

        print("MariaDB: Executing query...")
        cursor.execute(query)
        
        total_results = 0
        result = cursor.fetchmany(100)  
        while result:
            total_results += len(result)  
            result = cursor.fetchmany(100)

        end_time = time.time()
        query_time = end_time - start_time
        print(f"Query executed in {query_time} seconds. Total fetched: {total_results} rows.")
        
        cursor.close()
        conn.close()

        return query_time
        
    except mysql.connector.Error as err:
        print(f"MariaDB Error: {err}")
        return None

    except Exception as e:
        print(f"General error: {e}")
        return None

def test_mongodb_query():
    """Funkcja do testowania zapytań w MongoDB"""
    try:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        db = client['Bikes']
        collection = db['TripUsers']
        
        pipeline = [
            {
                "$match": {
                    "tripduration": {"$gt": 20}  
                }
            },
            {
                "$lookup": {
                    "from": "Stations",
                    "localField": "start_station_id",
                    "foreignField": "station_id",
                    "as": "start_station"
                }
            },
            {
                "$lookup": {
                    "from": "Stations",
                    "localField": "end_station_id",
                    "foreignField": "station_id",
                    "as": "end_station"
                }
            },
            {
                "$unwind": "$start_station"
            },
            {
                "$unwind": "$end_station"
            },
            {
                "$project": {
                    "trip_id": 1,
                    "tripduration": 1,
                    "start_station_name": "$start_station.station_name",
                    "end_station_name": "$end_station.station_name",
                    "_id": 0
                }
            },
            {
                "$sort": {"trip_id": 1}
            },
        ]

        start_time = time.time()
        print("MongoDB: Executing query...")

        cursor = collection.aggregate(pipeline)
        all_results = list(cursor)

        end_time = time.time()
        query_time = end_time - start_time
        print(f"Query executed in {query_time} seconds. Total results: {len(all_results)}")
        
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
    # Testowanie MariaDB
    mariadb_query_time = test_mariadb_query()  

    # Testowanie MongoDB
    mongodb_query_time = test_mongodb_query() 

    # Zbieranie statystyk systemowych
    system_stats = collect_system_stats()
    
    # Dodanie danych do statystyk
    system_stats['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')

    if mariadb_query_time is not None:
        system_stats['database'] = 'MariaDB'
        system_stats['query_time'] = mariadb_query_time
        save_to_csv(system_stats)

    if mongodb_query_time is not None:
        system_stats['database'] = 'MongoDB'
        system_stats['query_time'] = mongodb_query_time
        save_to_csv(system_stats)

if __name__ == '__main__':
    test_database_performance()