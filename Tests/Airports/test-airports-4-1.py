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
        query = "SELECT * FROM Flights f1 WHERE ARRIVAL_DELAY = ( SELECT MAX(ARRIVAL_DELAY) FROM Flights f2 WHERE f1.YEAR = f2.YEAR AND f1.MONTH = f2.MONTH AND f1.DAY = f2.DAY );"

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
            serverSelectionTimeoutMS=120000,
            connectTimeoutMS=120000,
            socketTimeoutMS=120000,
            maxPoolSize=10,
            waitQueueTimeoutMS=120000
        )
        
        print("MongoDB: Connected to database")
        db = client['Airports']
        collection = db['Flights']

        # Tworzenie indeksów dla optymalizacji
        print("Creating indexes for date fields...")
        collection.create_index([("YEAR", 1)])
        collection.create_index([("MONTH", 1)])
        collection.create_index([("DAY", 1)])
        collection.create_index([("ARRIVAL_DELAY", 1)])

        # Pipeline dla znalezienia maksymalnych opóźnień
        pipeline = [
            {
                # Grupowanie po dacie i znalezienie maksymalnego opóźnienia
                "$group": {
                    "_id": {
                        "year": "$YEAR",
                        "month": "$MONTH",
                        "day": "$DAY"
                    },
                    "maxDelay": {"$max": "$ARRIVAL_DELAY"},
                    "count": {"$sum": 1}
                }
            },
            {
                # Połączenie z oryginalną kolekcją aby znaleźć pełne informacje o locie
                "$lookup": {
                    "from": "Flights",
                    "let": {
                        "year": "$_id.year",
                        "month": "$_id.month",
                        "day": "$_id.day",
                        "maxDelay": "$maxDelay"
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$and": [
                                        {"$eq": ["$YEAR", "$$year"]},
                                        {"$eq": ["$MONTH", "$$month"]},
                                        {"$eq": ["$DAY", "$$day"]},
                                        {"$eq": ["$ARRIVAL_DELAY", "$$maxDelay"]}
                                    ]
                                }
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
                        }
                    ],
                    "as": "flight_details"
                }
            },
            {
                "$unwind": "$flight_details"
            },
            {
                "$project": {
                    "_id": 0,
                    "date": {
                        "$concat": [
                            {"$toString": "$_id.year"}, "-",
                            {"$toString": "$_id.month"}, "-",
                            {"$toString": "$_id.day"}
                        ]
                    },
                    "maxDelay": "$maxDelay",
                    "flightNumber": "$flight_details.FLIGHT_NUMBER",
                    "airline": "$flight_details.airline_info.AIRLINE",
                    "originAirport": "$flight_details.origin_airport.AIRPORT",
                    "destinationAirport": "$flight_details.destination_airport.AIRPORT",
                    "scheduledDeparture": "$flight_details.SCHEDULED_DEPARTURE",
                    "scheduledArrival": "$flight_details.SCHEDULED_ARRIVAL",
                    "actualDeparture": "$flight_details.DEPARTURE_TIME",
                    "actualArrival": "$flight_details.ARRIVAL_TIME"
                }
            },
            {
                "$sort": {
                    "date": 1,
                    "maxDelay": -1
                }
            }
        ]

        start_time = time.time()
        print("\nMongoDB: Executing max delays query...")
        
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
            print(f"- Days processed: {len(results)}")
            
            # Wyświetl pierwsze 5 wyników
            print("\nSample results (first 5 days):")
            for idx, doc in enumerate(results[:5], 1):
                print(f"\nDay {idx}:")
                print(f"Date: {doc['date']}")
                print(f"Max Delay: {doc['maxDelay']} minutes")
                print(f"Airline: {doc['airline']}")
                print(f"Flight: {doc['flightNumber']}")
                print(f"Route: {doc['originAirport']} -> {doc['destinationAirport']}")
                print(f"Scheduled: {doc['scheduledDeparture']} -> {doc['scheduledArrival']}")
                print(f"Actual: {doc['actualDeparture']} -> {doc['actualArrival']}")
            
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