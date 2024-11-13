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
        query = "SELECT Flights.FLIGHT_ID, Airlines.AIRLINE AS airline_name, origin_airports.AIRPORT AS origin_airport, destination_airports.AIRPORT AS destination_airport, Flights.ARRIVAL_DELAY FROM Flights JOIN Airlines ON Flights.AIRLINE = Airlines.IATA_CODE JOIN Airports AS origin_airports ON Flights.ORIGIN_AIRPORT = origin_airports.IATA_CODE JOIN Airports AS destination_airports ON Flights.DESTINATION_AIRPORT = destination_airports.IATA_CODE WHERE Flights.ARRIVAL_DELAY > 100;"

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
    """Funkcja do testowania zapytań w MongoDB z dodatkowym debugowaniem"""
    try:
        # Zwiększamy timeout i dodajemy więcej opcji połączenia
        client = MongoClient(
            'mongodb://localhost:27017/',
            serverSelectionTimeoutMS=10000,
            connectTimeoutMS=20000,
            socketTimeoutMS=45000
        )
        
        # Sprawdzamy połączenie
        client.admin.command('ping')
        print("MongoDB: Successfully connected to the database")
        
        db = client['Airports']
        
        # Sprawdzamy dostępne kolekcje
        collections = db.list_collection_names()
        print(f"Available collections: {collections}")
        
        collection = db['Flights']
        
        # Sprawdzamy czy kolekcje istnieją przed wykonaniem zapytania
        required_collections = ['Flights', 'airlines', 'airports']
        missing_collections = [coll for coll in required_collections if coll not in collections]
        
        if missing_collections:
            raise Exception(f"Missing required collections: {missing_collections}")
            
        # Sprawdzamy przykładowy dokument z każdej kolekcji
        print("\nSample documents from collections:")
        for coll_name in required_collections:
            sample_doc = db[coll_name].find_one()
            print(f"\n{coll_name} sample document:")
            print(sample_doc)

        print("\nMongoDB: Executing query...")
        
        # Modyfikujemy pipeline, aby wykonywać operacje etapami
        initial_pipeline = [
            {
                "$match": { "ARRIVAL_DELAY": { "$gt": 100 } }
            }
        ]
        
        # Najpierw sprawdzamy ile dokumentów spełnia warunek ARRIVAL_DELAY
        matching_count = len(list(collection.aggregate(initial_pipeline)))
        print(f"Documents matching ARRIVAL_DELAY > 100: {matching_count}")

        # Pełny pipeline
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
                "$unwind": {
                    "path": "$airline_info",
                    "preserveNullAndEmptyArrays": True
                }
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
                "$unwind": {
                    "path": "$origin_airport",
                    "preserveNullAndEmptyArrays": True
                }
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
                "$unwind": {
                    "path": "$destination_airport",
                    "preserveNullAndEmptyArrays": True
                }
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
        
        # Wykonujemy zapytanie z limitem
        cursor = collection.aggregate(pipeline, allowDiskUse=True)
        all_results = []
        
        for doc in cursor:
            all_results.append(doc)
            if len(all_results) % 100 == 0:
                print(f"Processed {len(all_results)} documents...")

        end_time = time.time()
        query_time = end_time - start_time
        print(f"Query executed in {query_time} seconds. Total fetched: {len(all_results)}")

        client.close()
        return query_time

    except Exception as e:
        print(f"Error: {e}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Full traceback:\n{traceback.format_exc()}")
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