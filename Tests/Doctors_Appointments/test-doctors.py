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
            # zapytania
            "SELECT * FROM Doctors WHERE specialization = 'Cardiology';",
            "SELECT * FROM Patients WHERE birthdate < '1980-01-01';",
            "SELECT * FROM Appointments WHERE diagnosis = 'Hypertension';",
            # grupowanie
            "SELECT YEAR(birthdate) AS birth_year, COUNT(*) AS patient_count FROM Patients GROUP BY birth_year;",
            "SELECT patient_id, COUNT(DISTINCT doctor_id) AS doctor_count FROM Appointments GROUP BY patient_id HAVING doctor_count > 1;", 
            "SELECT diagnosis, COUNT(*) AS diagnosis_count FROM Appointments GROUP BY diagnosis;",
            # joiny
            """SELECT 
                a.appointment_id,
                CONCAT(d.first_name, ' ', d.last_name) AS doctor_name,
                CONCAT(p.first_name, ' ', p.last_name) AS patient_name,
                a.diagnosis,
                a.treatment
            FROM 
                Appointments a
            JOIN 
                Doctors d ON a.doctor_id = d.doctor_id
            JOIN 
                Patients p ON a.patient_id = p.patient_id
            WHERE 
                d.doctor_id = 6970;
            """,
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
                'collection': 'Doctors',
                'query': { "specialization": 'Cardiology' },
                'projection': None 
            },
            {
                'collection': 'Patients',
                'query': { 'birthdate': {'$lt': '1980-01-01'} },
                'projection': None
            },
            { 
                'collection': 'Appointments',
                'query': None,
                'pipeline': [
                    {
                        '$match': 
                        {'diagnosis': 'Hypertension'}
                    },
                    {
                        '$project': {'_id': 0}
                    }
                ],
                'projection': None
            },
            # grupowanie
            {
                'collection': 'Patients',
                'query': None,
                'pipeline': [
                    {
                        '$addFields': {
                            'converted_date': {
                                '$cond': {
                                    'if': {'$type': '$birthdate'}, 
                                    'then': {
                                        '$cond': {
                                            'if': {'$eq': [{'$type': '$birthdate'}, 'string']},
                                            'then': {'$dateFromString': {'dateString': '$birthdate'}},
                                            'else': '$birthdate'
                                        }
                                    },
                                    'else': None
                                }
                            }
                        }
                    },
                    {
                        '$group': {
                            '_id': {'$year': '$converted_date'},
                            'patient_count': {'$sum': 1}
                        }
                    },
                    {
                        '$match': {
                            '_id': {'$ne': None}
                        }
                    },
                    {
                        '$project': {
                            '_id': 0,
                            'birth_year': '$_id',
                            'patient_count': 1
                        }
                    }
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
                            'unique_doctors': {'$addToSet': '$doctor_id'}
                        }
                    },
                    {
                        '$project': {
                            'patient_id': '$_id',
                            'doctor_count': {'$size': '$unique_doctors'}
                        }
                    },
                    {
                        '$match': {
                            'doctor_count': {'$gt': 1}
                        }
                    },
                    {
                        '$project': {
                            '_id': 0,
                            'patient_id': 1,
                            'doctor_count': 1
                        }
                    }
                ],
                'projection': None
            },
            {
                'collection': 'Appointments',
                'query': None,
                'pipeline': [
                    {
                        '$group': {
                            '_id': '$diagnosis',
                            'diagnosis_count': {'$count': {}}
                        }
                    },
                    {
                        '$project': {
                            '_id': 0,
                            'diagnosis': '$_id',
                            'diagnosis_count': 1
                        }
                    }
                ],
                'projection': None
            },
            # joiny
            {
                'collection': 'Appointments',
                'query': None,
                'pipeline': [
                    {
                        '$match': {
                            'doctor_id' : 6970
                        }  
                    },
                    {
                        '$lookup': {
                            'from': 'Doctors',
                            'localField': 'doctor_id',
                            'foreignField': 'doctor_id',
                            'as': 'doctor'
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
                        '$unwind': '$doctor'
                    },
                    {
                        '$unwind': '$patient'
                    },
                    {
                        '$project': {
                            '_id': 0,
                            'appointment_id': 1,
                            'doctor_name': {
                                '$concat': [
                                    '$doctor.first_name', 
                                    ' ', 
                                    '$doctor.last_name'
                                ]
                            },
                            'patient_name': {
                                '$concat': [
                                    '$patient.first_name',
                                    ' ',
                                    '$patient.last_name'
                                ]
                            },
                            'diagnosis': 1,
                            'treatment': 1
                        }
                    }
                ],
                'projection': None
            },
            {
                'collection': '',
                'query': None,
                'pipeline': [
                    
                ],
                'projection': None
            },
            {
                'collection': '',
                'query': None,
                'pipeline': [
                    
                ],
                'projection': None
            },
            # podzapytania
            {
                'collection': '',
                'query': None,
                'pipeline': [
                   
                ],
                'projection': None
            },
            {
                'collection': '',
                'query': None,
                'pipeline': [
                    
                ],
                'projection': None
            },
            {
                'collection': '',
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
