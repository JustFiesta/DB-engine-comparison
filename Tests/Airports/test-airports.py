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


def test_mongodb_query(collection_name, query=None, pipeline=None, projection=None):
    """
    Funkcja do testowania zapytań w MongoDB (z obsługą agregacji i klasycznych zapytań).
    """
    try:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        db = client['Airports']
        collection = db[collection_name]

        start_time = time.time()

        if pipeline:
            print(f"MongoDB: Executing aggregation pipeline on collection '{collection_name}'")
            cursor = collection.aggregate(pipeline)
        elif query:
            print(f"MongoDB: Executing query on collection '{collection_name}': {query}")
            cursor = collection.find(query, projection) if projection else collection.find(query)
        else:
            raise ValueError("Either 'query' or 'pipeline' must be provided.")

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
            # zapytania
            "SELECT * FROM Flights WHERE ARRIVAL_DELAY > 60;",
            "SELECT AIRLINE FROM Airlines WHERE IATA_CODE = 'AA';",
            "SELECT AIRPORT, CITY FROM Airports WHERE STATE = 'CA';",
            # grupowanie
            "SELECT DAY_OF_WEEK, AVG(ARRIVAL_DELAY) AS avg_arrival_delay FROM Flights GROUP BY DAY_OF_WEEK;",
            "SELECT CANCELLATION_REASON, COUNT(*) AS cancel_count FROM Flights WHERE CANCELLED = 1 GROUP BY CANCELLATION_REASON;", 
            "SELECT YEAR, MONTH, DAY, COUNT(*) AS flight_count FROM Flights WHERE AIRLINE = 'UA' GROUP BY YEAR, MONTH, DAY;",
            # joiny
            "SELECT a.AIRLINE, COUNT(f.FLIGHT_NUMBER) AS flight_count FROM Flights f JOIN Airlines a ON f.AIRLINE = a.IATA_CODE GROUP BY a.AIRLINE;",
            """SELECT 
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
            """,
            "SELECT ap.AIRPORT AS destination_airport, a.AIRLINE AS airline_name FROM Flights f JOIN Airlines a ON f.AIRLINE = a.IATA_CODE JOIN Airports ap ON f.DESTINATION_AIRPORT = ap.IATA_CODE WHERE f.ARRIVAL_DELAY > 120;",
            # podzapytania
            """SELECT 
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
            """,
            """SELECT 
            DISTINCT a.AIRPORT,
            a.CITY,
            a.STATE
            FROM Airports a
            JOIN Flights f ON a.IATA_CODE = f.ORIGIN_AIRPORT
            WHERE f.DISTANCE > (
                SELECT AVG(DISTANCE) 
                FROM Flights
            )
            ORDER BY a.STATE, a.CITY; """,
            """SELECT 
            f.MONTH,
            a.AIRLINE as AIRLINE_NAME,
            COUNT(*) as DELAYED_FLIGHTS
            FROM Flights f
            JOIN Airlines a ON f.AIRLINE = a.IATA_CODE
            WHERE f.ARRIVAL_DELAY > 0
            GROUP BY f.MONTH, f.AIRLINE
            HAVING DELAYED_FLIGHTS = (
                SELECT COUNT(*) 
                FROM Flights f2 
                WHERE f2.MONTH = f.MONTH 
                AND f2.ARRIVAL_DELAY > 0 
                GROUP BY f2.AIRLINE 
                ORDER BY COUNT(*) DESC 
                LIMIT 1
            )
            ORDER BY f.MONTH; """
        ],
        'MongoDB': [
            # zapytania
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
            },
            # grupowanie
            {
                'collection': 'Flights',
                'query': None,
                'pipeline': [
                    {
                        "$group": {
                            "_id": "$DAY_OF_WEEK",  
                            "avg_arrival_delay": { "$avg": "$ARRIVAL_DELAY" }
                        }
                    }
                ],
                'projection': None
            },
            {
                'collection': 'Flights',
                'query': None,
                'pipeline': [
                    {
                        "$match": {
                            "CANCELLED": 1  
                        }
                    },
                    {
                        "$group": {
                            "_id": "$CANCELLATION_REASON", 
                            "cancel_count": { "$sum": 1 }  
                        }
                    }
                ]
            },
            {
                'collection': 'Flights',
                'query': None,
                'pipeline': [
                    {
                        "$match": {
                            "AIRLINE": "UA"  
                        }
                    },
                    {
                        "$group": {
                            "_id": {
                                "year": "$YEAR",  
                                "month": "$MONTH",  
                                "day": "$DAY"  
                            },
                            "flight_count": { "$sum": 1 } 
                        }
                    }
                ]
            },
            # joiny
            {
                'collection': 'Flights',
                'query': None,
                'pipeline': [
                    {
                        "$group": {
                            "_id": "$AIRLINE",
                            "flight_count": {"$sum": 1}
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
                        "$unwind": {
                            "path": "$airline_info",
                            "preserveNullAndEmptyArrays": True
                        }
                    },
                    {
                        "$project": {
                            "airline": "$airline_info.AIRLINE",
                            "flight_count": 1,
                            "_id": 0
                        }
                    }
                ]
            },
            {
                'collection': 'Flights',
                'query': None,
                'pipeline': [
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
                            "arrival_delay": "$ARRIVAL_DELAY",
                            "_id": 0
                        }
                    },
                    {
                        "$sort": {"arrival_delay": -1}
                    }
                ]
            },
            {
                'collection': 'Flights',
                'query': None,
                'pipeline': [
                    {
                        "$match": {
                            "ARRIVAL_DELAY": {"$gt": 120}
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
                            "arrival_delay": "$ARRIVAL_DELAY",
                            "_id": 0
                        }
                    },
                    {
                        "$sort": {"arrival_delay": -1}
                    }
                ]
            },
            # podzapytania
            {
                'collection': 'Flights',
                'query': None,
                'pipeline': [
                    {
                        "$group": {
                            "_id": "$DAY_OF_WEEK",  
                            "avg_arrival_delay": { "$avg": "$ARRIVAL_DELAY" }
                        }
                    }
                ]
            },
            {
                'collection': 'Flights',
                'query': None,
                'pipeline': [
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
            },
            {
                'collection': 'Flights',
                'query': None,
                'pipeline': [
                    {
                        "$facet": {
                            "avgDistance": [
                                {
                                    "$group": {
                                        "_id": None,
                                        "avg": { "$avg": "$DISTANCE" }
                                    }
                                }
                            ]
                        }
                    },
                    {
                        "$unwind": "$avgDistance"
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
                        "$match": {
                            "DISTANCE": { "$gt": "$avgDistance.avg" }
                        }
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
            },
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
        test_database_performance()
