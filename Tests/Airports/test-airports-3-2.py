import time
import psutil
import mysql.connector
from pymongo import MongoClient
import csv

def collect_system_stats():
    """Funkcja zbierająca statystyki systemowe"""
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
        
        # Zapytanie z JOIN-ami do znalezienia opóźnionych lotów
        query = """
        SELECT 
            a.AIRLINE as airline_name,
            orig.AIRPORT as origin_airport,
            dest.AIRPORT as destination_airport,
            f.ARRIVAL_DELAY
        FROM Flights f
        JOIN Airlines a ON f.AIRLINE = a.IATA_CODE
        JOIN Airports orig ON f.ORIGIN_AIRPORT = orig.IATA_CODE
        JOIN Airports dest ON f.DESTINATION_AIRPORT = dest.IATA_CODE
        WHERE f.ARRIVAL_DELAY > 100
        ORDER BY f.ARRIVAL_DELAY DESC;
        """

        start_time = time.time()

        print("MariaDB: Executing query...")
        cursor.execute(query)
        
        results_count = 0
        # Pobieramy wyniki w paczkach po 1000
        while True:
            results = cursor.fetchmany(1000)
            if not results:
                break
            results_count += len(results)
            if results_count % 10000 == 0:
                print(f"Fetched {results_count} rows...")
        
        end_time = time.time()
        query_time = end_time - start_time
        print(f"Query executed in {query_time:.2f} seconds.")
        print(f"Total rows fetched: {results_count}")
        
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
            serverSelectionTimeoutMS=30000,
            connectTimeoutMS=60000,
            socketTimeoutMS=90000,
            maxPoolSize=1,
            waitQueueTimeoutMS=90000
        )
        
        print("MongoDB: Connected to database")
        db = client['Airports']
        collection = db['Flights']

        pipeline = [
            {
                "$match": {
                    "ARRIVAL_DELAY": {"$gt": 100}
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
                    "preserveNullAndEmptyArrays": True
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
                    "preserveNullAndEmptyArrays": True
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
                    "airline_name": "$airline_info.AIRLINE",
                    "origin_airport": "$origin_airport.AIRPORT",
                    "destination_airport": "$destination_airport.AIRPORT",
                    "arrival_delay": "$ARRIVAL_DELAY",
                    "_id": 0
                }
            },
            {
                "$sort": {"arrival_delay": -1}
            }
        ]

        start_time = time.time()
        print("\nMongoDB: Executing query...")
        
        try:
            cursor = collection.aggregate(
                pipeline,
                allowDiskUse=True,
                batchSize=1000,
                maxTimeMS=600000  # 10 minut
            )
            
            results_count = 0
            for _ in cursor:
                results_count += 1
                if results_count % 10000 == 0:
                    print(f"Processed {results_count} documents...")
                    
            end_time = time.time()
            query_time = end_time - start_time
            
            print(f"\nQuery statistics:")
            print(f"- Total time: {query_time:.2f} seconds")
            print(f"- Documents processed: {results_count}")
            
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
    """Funkcja do testowania wydajności bazy danych"""
    # Testowanie MariaDB
    mariadb_query_time = test_mariadb_query() 

    # Testowanie MongoDB
    mongodb_query_time = test_mongodb_query()  

    # Zbieranie statystyk systemowych
    system_stats = collect_system_stats()
    
    # Dodanie timestampu
    system_stats['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')

    # Zapisanie wyników dla MariaDB
    if mariadb_query_time is not None:
        system_stats['database'] = 'MariaDB'
        system_stats['query_time'] = mariadb_query_time
        save_to_csv(system_stats)

    # Zapisanie wyników dla MongoDB
    if mongodb_query_time is not None:
        system_stats['database'] = 'MongoDB'
        system_stats['query_time'] = mongodb_query_time
        save_to_csv(system_stats)

if __name__ == '__main__':
    test_database_performance()