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
            database='Airports'
        )
        cursor = conn.cursor()
        query = "SELECT Flights.FLIGHT_ID, Airlines.AIRLINE AS airline_name, origin_airports.AIRPORT AS origin_airport, destination_airports.AIRPORT AS destination_airport, flights.ARRIVAL_DELAY FROM flights JOIN Airlines ON flights.AIRLINE = airlines.IATA_CODE JOIN Airports AS origin_airports ON Flights.ORIGIN_AIRPORT = origin_airports.IATA_CODE JOIN Airports AS destination_airports ON Flights.DESTINATION_AIRPORT = destination_airports.IATA_CODE WHERE Flights.ARRIVAL_DELAY > 100;"

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

        pipeline = [
            {
                "$match": { "ARRIVAL_DELAY": { "$gt": 100 } }  
            },
            {
                "$lookup": {
                    "from": "airlines",  
                    "localField": "AIRLINE",  
                    "foreignField": "IATA_CODE",  
                    "as": "airline_info"  
                }
            },
            {
                "$unwind": "$airline_info" 
            },
            {
                "$lookup": {
                    "from": "airports",  
                    "localField": "ORIGIN_AIRPORT",  
                    "foreignField": "IATA_CODE",  
                    "as": "origin_airport"  
                }
            },
            {
                "$unwind": "$origin_airport"  
            },
            {
                "$lookup": {
                    "from": "airports",  
                    "localField": "DESTINATION_AIRPORT",  
                    "foreignField": "IATA_CODE", 
                    "as": "destination_airport"  
                }
            },
            {
                "$unwind": "$destination_airport"  
            },
            {
                "$project": {  
                    "airline": "$airline_info.AIRLINE", 
                    "origin_airport": "$origin_airport.AIRPORT",  
                    "destination_airport": "$destination_airport.AIRPORT",  
                    "ARRIVAL_DELAY": 1,  
                    "_id": 0  
                }
            }
        ]

        start_time = time.time()

        cursor = collection.aggregate(pipeline)
        all_results = []

        for doc in cursor:
            all_results.append(doc)  

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
    # Testowanie MariaDB
    mariadb_query_time = test_mariadb_query() 

    # Testowanie MongoDB
    mongodb_query_time = test_mongodb_query()  

    # Zbieranie statystyk systemowych
    system_stats = collect_system_stats()
    
    # Dodanie danych do statystyk
    system_stats['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')

    # Dodanie nazw silników baz danych do wyników
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