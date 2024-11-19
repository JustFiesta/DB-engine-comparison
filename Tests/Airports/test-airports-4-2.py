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
            user='bot',
            password='P@ssw0rd',
            database='Airports'
        )
        cursor = conn.cursor()
        query = """SELECT 
            DISTINCT a.AIRPORT,
            a.CITY,
            a.STATE
        FROM Airports a
        JOIN Flights f ON a.IATA_CODE = f.ORIGIN_AIRPORT
        WHERE f.DISTANCE > (
            SELECT AVG(DISTANCE) 
            FROM Flights
        )
        ORDER BY a.STATE, a.CITY; """

        start_time = time.time()

        print("MariaDB: Executing query...")
        cursor.execute(query)
        
        
        result = cursor.fetchmany(100)  
        while result:
            print(f"Fetched {len(result)} rows")
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

def test_mongodb_query():
    """Funkcja do testowania zapytań w MongoDB"""
    try:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        db = client['Airports']
        collection = db['Flights']
        print("MongoDB: Executing query...")

        avg_pipeline = [
            {
                "$group": {
                    "_id": None,
                    "avgDistance": { "$avg": "$DISTANCE" }
                }
            }
        ]
        
        avg_result = list(collection.aggregate(avg_pipeline))
        avg_distance = avg_result[0]['avgDistance']

        main_pipeline = [
            {
                "$match": {
                    "DISTANCE": { "$gt": avg_distance }
                }
            },
            {
                "$lookup": {
                    "from": "Airports",
                    "localField": "ORIGIN_AIRPORT",
                    "foreignField": "IATA_CODE",
                    "as": "airport_info"
                }
            },
            {
                "$unwind": "$airport_info"
            },
            {
                "$group": {
                    "_id": {
                        "airport": "$airport_info.AIRPORT",
                        "city": "$airport_info.CITY",
                        "state": "$airport_info.STATE"
                    }
                }
            },
            {
                "$sort": {
                    "_id.state": 1,
                    "_id.city": 1
                }
            }
        ]

        start_time = time.time()
        
        cursor = collection.aggregate(main_pipeline, allowDiskUse=True)
        all_results = []
        
        batch_size = 1000
        current_batch = []
        
        for doc in cursor:
            current_batch.append(doc)
            if len(current_batch) >= batch_size:
                all_results.extend(current_batch)
                current_batch = []
        
        if current_batch:
            all_results.extend(current_batch)

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
    mariadb_query_time = test_mariadb_query() 

    mongodb_query_time = test_mongodb_query()  

    system_stats = collect_system_stats()
    
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
    for i in range(3):
        test_database_performance()