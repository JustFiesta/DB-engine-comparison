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
        query = "SELECT a.AIRLINE, COUNT(f.FLIGHT_NUMBER) AS flight_count FROM Flights f JOIN Airlines a ON f.AIRLINE = a.IATA_CODE GROUP BY a.AIRLINE;"

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
    """Funkcja do testowania zapytań w MongoDB z optymalizacjami dla dużych zbiorów danych"""
    try:
        # Zwiększamy jeszcze bardziej timeouty
        client = MongoClient(
            'mongodb://localhost:27017/',
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=60000,
            socketTimeoutMS=90000,
            maxPoolSize=1,
            waitQueueTimeoutMS=90000
        )
        
        print("MongoDB: Connected to database")
        db = client['Airports']
        collection = db['Flights']

        # Najpierw sprawdźmy ile dokumentów spełnia podstawowy warunek
        matching_count = collection.count_documents({"ARRIVAL_DELAY": {"$gt": 100}})
        print(f"Documents matching ARRIVAL_DELAY > 100: {matching_count}")

        # Dodajemy limit do zapytania i wykonujemy je w mniejszych porcjach
        batch_size = 1000
        pipeline = [
            {
                "$match": {
                    "ARRIVAL_DELAY": {"$gt": 100}
                }
            },
            {
                # Limit dodany na początku pipeline'u
                "$limit": batch_size
            },
            {
                # Optymalizacja: pobieramy tylko potrzebne pola
                "$project": {
                    "AIRLINE": 1,
                    "ORIGIN_AIRPORT": 1,
                    "DESTINATION_AIRPORT": 1,
                    "ARRIVAL_DELAY": 1
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
                "$unwind": {
                    "path": "$airline_info",
                    "preserveNullAndEmptyArrays": true
                }
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
                "$unwind": {
                    "path": "$origin_airport",
                    "preserveNullAndEmptyArrays": true
                }
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
        
        print("\nMongoDB: Executing optimized query...")
        
        try:
            # Wykonujemy zapytanie z większymi opcjami wydajności
            cursor = collection.aggregate(
                pipeline,
                allowDiskUse=True,
                batchSize=100,  # Mniejszy batch size dla lepszej kontroli pamięci
                maxTimeMS=300000  # 5 minut maksymalnego czasu wykonania
            )
            
            all_results = []
            processed = 0
            
            for doc in cursor:
                all_results.append(doc)
                processed += 1
                if processed % 100 == 0:
                    print(f"Processed {processed} documents...")
                    
            end_time = time.time()
            query_time = end_time - start_time
            
            print(f"\nQuery statistics:")
            print(f"- Total time: {query_time:.2f} seconds")
            print(f"- Documents processed: {len(all_results)}")
            print(f"- Average processing time per document: {query_time/len(all_results):.4f} seconds")
            
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