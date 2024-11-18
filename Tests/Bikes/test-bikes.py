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
            database='bikes'
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
        all_results = [doc for doc in cursor]

        print(f"Query executed in {query_time} seconds. Total fetched: {len(all_results)}")

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
        db = client['Bikes']
        collection = db[collection_name]

        start_time = time.time()

        if pipeline:
            print(f"MongoDB: Executing aggregation pipeline on collection '{collection_name}'")
            cursor = collection.aggregate(pipeline)
        elif query:
            print(f"MongoDB: Executing query on collection '{collection_name}': {query}")
            cursor = collection.find(query, projection) if projection else collection.find(query)
        else:
            raise ValueError("Either 'query' or 'pipeline' must be provided")

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
    Funkcja do testowania wydajności bazy danych.
    Wykonuje zapytania do baz danych, zbiera statystyki systemowe
    i zapisuje wynik w pliku CSV.
    """
    queries = {
        'MariaDB': [
            # zapytania
            "SELECT * FROM TripUsers WHERE tripduration > 30 * 60;",
            "SELECT * FROM TripUsers WHERE end_station_name = 'Newport Pkwy';",
            "SELECT DISTINCT usertype FROM TripUsers;",
            # grupowanie
            "SELECT usertype, COUNT(*) AS trip_count FROM TripUsers GROUP BY usertype;",
            "SELECT birth_year, SUM(tripduration) AS total_tripduration FROM TripUsers GROUP BY birth_year;", 
            "SELECT gender, AVG(tripduration) AS average_tripduration FROM TripUsers GROUP BY gender;",
            # joiny
            """SELECT 
            t.trip_id, t.tripduration, s.station_name AS start_stat_name, e.station_name AS end_stat_name 
            FROM TripUsers t JOIN Stations s ON t.start_station_id = s.station_id 
            JOIN Stations e ON t.end_station_id = e.station_id 
            WHERE t.tripduration > 20;""",
            """SELECT 
            t.trip_id, t.tripduration, start_stations.station_name AS start_station_name, end_stations.station_name AS end_station_name 
            FROM TripUsers t JOIN  Stations AS start_stations ON t.start_station_id = start_stations.station_id 
            JOIN Stations AS end_stations ON t.end_station_id = end_stations.station_id 
            WHERE t.tripduration > 20;""",
            """SELECT 
            t.trip_id, t.tripduration, t.starttime, start_station.station_name AS start_station, end_station.station_name AS end_station 
            FROM TripUsers t JOIN Stations start_station ON t.start_station_id = start_station.station_id 
            JOIN Stations end_station ON t.end_station_id = end_station.station_id;""",
            # podzapytania
            "SELECT * FROM TripUsers WHERE tripduration > (SELECT AVG(tripduration) FROM TripUsers);",
            "SELECT * FROM Stations WHERE station_id IN (SELECT DISTINCT end_station_id FROM TripUsers);",
            """SELECT * 
            FROM TripUsers
            WHERE birth_year < 1980 AND tripduration > (SELECT AVG(tripduration) FROM TripUsers WHERE birth_year < 1980);"""
        ],
        'MongoDB': [
            # zapytania
            {
                'collection': 'TripUsers',
                'query': {"tripduration": { "$gt": 1800 } },
                'projection': None 
            },
            {
                'collection': 'TripUsers',
                'query': {"end_station_name": "Newport Pkwy" },
                'projection': None
            },
            { 
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    {
                        "$group": {"_id": "$usertype"}
                    },  
                    {
                        "$project": {"usertype": "$_id", "_id": 0}
                    }  
                ],
                'projection': None
            },
            # grupowanie
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    {
                        "$group": {
                            "_id": "$usertype",
                            "trip_count": {"$sum": 1}  
                        }
                    },
                    {
                        "$project": {
                            "usertype": "$_id",
                            "trip_count": 1,
                            "_id": 0
                        }
                    },
                    {
                        "$sort": {"usertype": 1} 
                    }
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    {
                        "$group": {
                            "_id": "$birth_year",
                            "total_tripduration": {"$sum": "$tripduration"}
                        }
                    },
                    {
                        "$project": {
                            "birth_year": "$_id",
                            "total_tripduration": 1,
                            "_id": 0
                        }
                    },
                    {
                        "$sort": {"birth_year": 1}  
                    },
                    {
                        "$match": {  
                            "birth_year": {"$ne": None}
                        }
                    }
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    {
                        "$group": {
                            "_id": "$gender",
                            "average_tripduration": {"$avg": "$tripduration"},
                            "count": {"$sum": 1}
                        }
                    },
                    {
                        "$project": {
                            "gender": "$_id",
                            "average_tripduration": 1,
                            "count": 1,
                            "_id": 0
                        }
                    },
                    {
                        "$sort": {"gender": 1}
                    },
                    {
                        "$match": {
                            "gender": {"$ne": None}
                        }
                    }
                ],
                'projection': None
            },
            # joiny
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
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
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    {
                        '$match': {
                            'tripduration': {'$gt': 20}
                    }
                    },
                    {
                        '$lookup': {
                            'from': 'stations',
                            'localField': 'start_station_id',
                            'foreignField': 'station_id',
                            'as': 'start_station'
                        }
                    },
                    {
                        '$lookup': {
                            'from': 'stations',
                            'localField': 'end_station_id',
                            'foreignField': 'station_id',
                            'as': 'end_station'
                        }
                    },
                    {
                        '$project': {
                            'trip_id': 1,
                            'tripduration': 1,
                            'start_station_name': {'$arrayElemAt': ['$start_station.station_name', 0]},
                            'end_station_name': {'$arrayElemAt': ['$end_station.station_name', 0]}
                        }
                    }
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    {
                        '$lookup': {
                            'from': 'stations',
                            'localField': 'start_station_id',
                            'foreignField': 'station_id',
                            'as': 'start_station'
                        }
                    },
                    {
                        '$lookup': {
                            'from': 'stations',
                            'localField': 'end_station_id',
                            'foreignField': 'station_id',
                            'as': 'end_station'
                        }
                    },
                    {
                        '$project': {
                            'trip_id': 1,
                            'tripduration': 1,
                            'starttime': 1,
                            'start_station': {'$arrayElemAt': ['$start_station.station_name', 0]},
                            'end_station': {'$arrayElemAt': ['$end_station.station_name', 0]}
                        }
                    }
                    
                ],
                'projection': None
            },
            # podzapytania
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    {
                        '$facet': {
                            'avgDuration': [
                                {
                                    '$group': {
                                        '_id': None,
                                        'avg_tripduration': {'$avg': '$tripduration'}
                                    }
                                }
                            ],
                            'allTrips': [
                                {
                                    '$match': {}  
                                }
                            ]
                        }
                    },
                    {
                        '$unwind': '$avgDuration'
                    },
                    {
                        '$unwind': '$allTrips'
                    },
                    {
                        '$match': {
                            '$expr': {
                                '$gt': ['$allTrips.tripduration', '$avgDuration.avg_tripduration']
                            }
                        }
                    },
                    {
                        '$replaceRoot': { 'newRoot': '$allTrips' }
                    },
                    {
                        '$project': {
                            '_id': 0,
                            'trip_id': 1,
                            'tripduration': 1,
                            'starttime': 1
                        }
                    }
                ],
                'projection': None
            },
            {
                'collection': 'Stations',  
                'query': None,
                'pipeline': [
                    {
                        '$lookup': {
                            'from': 'TripUsers',
                            'pipeline': [
                                {
                                    '$group': {
                                        '_id': None,
                                        'unique_end_stations': {'$addToSet': '$end_station_id'}
                                    }
                                }
                            ],
                            'as': 'end_stations_info'
                        }
                    },
                    {
                        '$unwind': '$end_stations_info'
                    },
                    {
                        '$match': {
                            '$expr': {
                                '$in': ['$station_id', '$end_stations_info.unique_end_stations']
                            }
                        }
                    },
                    {
                        '$project': {
                            '_id': 0,
                            'station_id': 1,
                            'station_name': 1
                        }
                    }
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    {
                        '$facet': {
                            'avgDurationYoung': [
                                {
                                    '$match': {
                                        'birth_year': {'$lt': 1980}
                                    }
                                },
                                {
                                    '$group': {
                                        '_id': None,
                                        'avg_tripduration': {'$avg': '$tripduration'}
                                    }
                                }
                            ],
                            'allTripsYoung': [
                                {
                                    '$match': {
                                        'birth_year': {'$lt': 1980}
                                    }
                                }
                            ]
                        }
                    },
                    {
                        '$unwind': '$avgDurationYoung'
                    },
                    {
                        '$unwind': '$allTripsYoung'
                    },
                    {
                        '$match': {
                            '$expr': {
                                '$gt': ['$allTripsYoung.tripduration', '$avgDurationYoung.avg_tripduration']
                            }
                        }
                    },
                    {
                        '$replaceRoot': { 'newRoot': '$allTripsYoung' }
                    },
                    {
                        '$project': {
                            '_id': 0,
                            'trip_id': 1,
                            'tripduration': 1,
                            'birth_year': 1
                        }
                    }
                ],
                'projection': None
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
            collection_name=query_set['collection'],
            query=query_set.get('query'),
            pipeline=query_set.get('pipeline'),
            projection=query_set.get('projection')
        )
        system_stats = collect_system_stats()
        system_stats['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
        system_stats['database'] = 'MongoDB'
        system_stats['query_time'] = mongodb_query_time
        save_to_csv(system_stats)

if __name__ == '__main__':
        test_database_performance()
