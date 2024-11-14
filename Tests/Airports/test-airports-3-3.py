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
        query = "SELECT ap.AIRPORT AS destination_airport, a.AIRLINE AS airline_name FROM Flights f JOIN Airlines a ON f.AIRLINE = a.IATA_CODE JOIN Airports ap ON f.DESTINATION_AIRPORT = ap.IATA_CODE WHERE f.ARRIVAL_DELAY > 120;"

        start_time = time.time()

        print("MariaDB: Executing query...")
        cursor.execute(query)
        
        
        result = cursor.fetchmany(100)  
        while result:
            #print(f"Fetched {len(result)} rows")
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
        client = MongoClient(
            'mongodb://localhost:27017/',
            serverSelectionTimeoutMS=120000,
            connectTimeoutMS=120000,
            socketTimeoutMS=120000,
            maxPoolSize=10,
            waitQueueTimeoutMS=120000,
            retryWrites=True,
            w='majority'
        )
        
        print("MongoDB: Connected to database")
        db = client['Airports']
        
        print("Checking and creating indexes...")
        db.Flights.create_index([("AIRLINE", 1)])
        db.Flights.create_index([("ORIGIN_AIRPORT", 1)])
        db.Flights.create_index([("DESTINATION_AIRPORT", 1)])
        db.Flights.create_index([("ARRVAL_DELAY", 1)])  # Zmienione z ARRIVAL_DELAY
        
        collection = db['Flights']

        pipeline = [
            {
                "$match": {
                    "ARRIVAL_DELAY": {"$gt": 100}  # Zmienione z ARRIVAL_DELAY
                }
            },
            {
                "$lookup": {
                    "from": "Airlines",
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
                    "from": "Airports",
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
                    "from": "Airports",
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
                    "airline_name": "$airline_info.AIRLINE",
                    "origin_airport": "$origin_airport.AIRPORT",
                    "destination_airport": "$destination_airport.AIRPORT",
                    "arrival_delay": "$ARRIVAL_DELAY",  # Zmienione z arrival_delay
                    "_id": 0
                }
            },
            {
                "$sort": {"arrival_delay": -1}  # Zmienione z arrival_delay
            }
        ]

        start_time = time.time()
        print("\nMongoDB: Executing query...")
        
        try:
            # Dodane logowanie przykładowych wyników
            cursor = collection.aggregate(
                pipeline,
                allowDiskUse=True,
                batchSize=500,
                maxTimeMS=1800000
            )
            
            results_count = 0
            sample_results = []  # Lista na przykładowe wyniki
            
            for doc in cursor:
                results_count += 1
                if results_count <= 5:  # Zapisz pierwsze 5 wyników
                    sample_results.append(doc)
                if results_count % 5000 == 0:
                    print(f"Processed {results_count} documents...")
                    
            end_time = time.time()
            query_time = end_time - start_time
            
            print(f"\nQuery statistics:")
            print(f"- Total time: {query_time:.2f} seconds")
            print(f"- Documents processed: {results_count}")
            
            # Wyświetl przykładowe wyniki
            if sample_results:
                print("\nSample results (first 5 documents):")
                for idx, doc in enumerate(sample_results, 1):
                    print(f"\nDocument {idx}:")
                    print(f"Airline: {doc.get('airline_name')}")
                    print(f"Origin: {doc.get('origin_airport')}")
                    print(f"Destination: {doc.get('destination_airport')}")
                    print(f"Delay: {doc.get('airline_delay')}")
            
            client.close()
            return query_time
            
        except Exception as e:
            print(f"\nError during query execution:")
            print(f"Error type: {type(e)}")
            print(f"Error message: {str(e)}")
            raise
            
    except Exception as e:
        print(f"\nGeneral error:")
        print(f"Error type: {type(e)}")
        print(f"Error message: {str(e)}")
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