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
            database='Doctors_Appointments'
        )
        cursor = conn.cursor()

        start_time = time.time()
        print(f"MariaDB: Executing query: {query}")
        cursor.execute(query)

        total_rows = 0
        result = cursor.fetchmany(100)
        while result:
            total_rows += len(result)
            result = cursor.fetchmany(100)

        end_time = time.time()
        query_time = end_time - start_time
        print(f"Query executed in {query_time} seconds. Total rows returned: {total_rows}")

        cursor.close()
        conn.close()

        return query_time, total_rows

    except mysql.connector.Error as err:
        print(f"MariaDB Error: {err}")
        return None, 0

    except Exception as e:
        print(f"General error: {e}")
        return None, 0


def test_mongodb_query(collection_name, query=None, pipeline=None, projection=None):
    """
    Funkcja do testowania zapytań w MongoDB (z obsługą agregacji i klasycznych zapytań).
    """
    try:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        db = client['Doctors_Appointments']
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

        ],
        'MongoDB': [
            # zapytania
            {
                'collection': 'Appointments',
                'pipeline': [
                    {'$lookup': {
                        'from': 'doctors',
                        'localField': 'doctor_id',
                        'foreignField': 'doctor_id',
                        'as': 'doctor'
                    }},
                    {'$lookup': {
                        'from': 'patients',
                        'localField': 'patient_id',
                        'foreignField': 'patient_id',
                        'as': 'patient'
                    }},
                    {'$unwind': '$doctor'},
                    {'$unwind': '$patient'},
                    {'$project': {
                        'appointment_date': 1,
                        'doctor_first_name': '$doctor.first_name',
                        'doctor_last_name': '$doctor.last_name',
                        'patient_first_name': '$patient.first_name',
                        'patient_last_name': '$patient.last_name',
                        'diagnosis': 1
                    }},
                    {'$limit': 10}
                ],
                'projection': None
            },
            {
                'collection': 'Appointments',
                'query': None,
                'pipeline': [
                    {
                        '$group': {
                            '_id': '$patient_id',
                            'total_appointments': {'$sum': 1}
                        }
                    },
                    {   
                        '$match': {
                            'total_appointments': {'$gt': 5}
                        }
                    },
                    {
                        '$lookup': {
                            'from': 'Patients',
                            'localField': '_id',
                            'foreignField': 'patient_id',
                            'as': 'patient_info'
                        }
                    },
                    {
                        '$unwind': '$patient_info'
                    },
                    {
                        '$project': {
                            'first_name': '$patient_info.first_name',
                            'last_name': '$patient_info.last_name',
                            'total_appointments': 1
                        }
                    },
                ],
                'projection': None
            },
            # podzapytania
            {
                'collection': 'Appointments',
                'query': None,
                'pipeline': [
                    {
                        '$group': {
                        '_id': '$patient_id',
                        'appointment_count': {'$sum': 1}
                        }
                    },
                    {
                        '$match': {'appointment_count': {'$gt': 1}}},
                    {
                        '$lookup': {
                            'from': 'Appointments',
                            'let': {'patient_id': '$_id'},
                            'pipeline': [
                                {
                                    '$match': {
                                        '$expr': {'$eq': ['$patient_id', '$$patient_id']}
                                    }
                                },
                                {   
                                    '$lookup': {
                                        'from': 'Patients',
                                        'localField': 'patient_id',
                                        'foreignField': 'patient_id',
                                        'as': 'patient'
                                    }
                                },
                                {
                                    '$unwind': '$patient'
                                },
                                {
                                    '$project': {
                                        'appointment_id': 1,
                                        'appointment_date': 1,
                                        'first_name': '$patient.first_name',
                                        'last_name': '$patient.last_name',
                                        'diagnosis': 1
                                    }
                                }
                        ],
                        'as': 'appointments'
                        }
                    },
                    {
                        '$unwind': '$appointments'
                    },
                    {
                        '$replaceRoot': {'newRoot': '$appointments'}
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
