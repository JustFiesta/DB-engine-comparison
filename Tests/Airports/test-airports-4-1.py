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
        query = """SELECT 
            f.AIRLINE,
            a.AIRLINE as AIRLINE_NAME,
            ROUND(AVG(f.ARRIVAL_DELAY), 2) as AVG_DELAY
        FROM Flights f
        JOIN Airlines a ON f.AIRLINE = a.IATA_CODE
        GROUP BY f.AIRLINE
        HAVING AVG_DELAY > (
            SELECT AVG(ARRIVAL_DELAY) 
            FROM Flights
        )
        ORDER BY AVG_DELAY DESC;
        """

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
        client = MongoClient(
            'mongodb://localhost:27017/',
            serverSelectionTimeoutMS=300000,  
            connectTimeoutMS=300000,
            socketTimeoutMS=300000,
            maxPoolSize=50,                   
            waitQueueTimeoutMS=300000,
            retryReads=True
        )
        
        print("MongoDB: Connected to database")
        db = client['Airports']
        collection = db['Flights']

        pipeline = [
            {
                "$facet": {
                    "totalAvgDelay": [
                        {
                            "$group": {
                                "_id": 1,
                                "avg": { "$avg": "$ARRIVAL_DELAY" }
                            }
                        }
                    ],
                    "airlineDelays": [
                        {
                            "$group": {
                                "_id": "$AIRLINE",
                                "avgDelay": { "$avg": "$ARRIVAL_DELAY" }
                            }
                        },
                        {
                            "$lookup": {
                                "from": "Airlines",
                                "localField": "_id",
                                "foreignField": "IATA_CODE",
                                "as": "airline_info"
                            }
                        },
                        {
                            "$unwind": "$airline_info"
                        }
                    ]
                }
            },
            {
                "$unwind": "$totalAvgDelay"
            },
            {
                "$project": {
                    "results": {
                        "$filter": {
                            "input": "$airlineDelays",
                            "as": "airline",
                            "cond": { "$gt": ["$$airline.avgDelay", "$totalAvgDelay.avg"] }
                        }
                    }
                }
            },
            {
                "$unwind": "$results"
            },
            {
                "$project": {
                    "_id": "$results._id",
                    "airlineName": "$results.airline_info.AIRLINE",
                    "avgDelay": { "$round": ["$results.avgDelay", 2] }
                }
            },
            {
                "$sort": { "avgDelay": -1 }
            }
        ]

        start_time = time.time()
        print("\nMongoDB: Executing query...")
        
        try:
            cursor = collection.aggregate(
                pipeline,
                allowDiskUse=True,
                batchSize=500,
                maxTimeMS=1800000
            )
            
            results = []
            for doc in cursor:
                results.append(doc)
            
            end_time = time.time()
            query_time = end_time - start_time
            
            print(f"\nQuery statistics:")
            print(f"- Total time: {query_time:.2f} seconds")
            print(f"- Records: {len(results)}")
            
            client.close()
            return query_time, results
            
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
        return None, None

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