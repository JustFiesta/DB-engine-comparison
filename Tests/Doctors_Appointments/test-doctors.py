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
            database='Bikes'
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
            "",
            "",
            "",
            # grupowanie
            "",
            "", 
            "",
            # joiny
            "",
            "",
            "",
            # podzapytania
            "",
            "",
            ""
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
                    
                ],
                'projection': None
            },
            # grupowanie
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    
                ],
                'projection': None
            },
            # joiny
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    
                ],
                'projection': None
            },
            # podzapytania
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                   
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                     
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
            query_set['collection'], query_set['query'], query_set['projection']
        )
        system_stats = collect_system_stats()
        system_stats['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
        system_stats['database'] = 'MongoDB'
        system_stats['query_time'] = mongodb_query_time
        save_to_csv(system_stats)


if __name__ == '__main__':
        test_database_performance()
