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
            database='Doctors_Appointments'
        )
        cursor = conn.cursor()
        query = """SELECT 
            p.patient_id,
            CONCAT(p.first_name, ' ', p.last_name) as patient_name,
            COUNT(a.appointment_id) as total_appointments,
            GROUP_CONCAT(DISTINCT d.specialization) as visited_specializations
        FROM 
            Patients p
        LEFT JOIN 
            Appointments a ON p.patient_id = a.patient_id
        LEFT JOIN 
            Doctors d ON a.doctor_id = d.doctor_id
        GROUP BY 
            p.patient_id
        HAVING 
            total_appointments > 1;
        """  
        start_time = time.time()

        print("MariaDB: Executing query...")
        cursor.execute(query)
        
        total_results = 0
        result = cursor.fetchmany(100)  

        end_time = time.time()
        query_time = end_time - start_time
        print(f"Query executed in {query_time} seconds. Total fetched: {total_results} rows.")

        cursor.close()
        conn.close()

        return query_time, total_results  

    except mysql.connector.Error as err:
        print(f"MariaDB Error: {err}")
        return None, 0

    except Exception as e:
        print(f"General error: {e}")
        return None, 0


def test_mongodb_query():
    """Funkcja do testowania zapytań w MongoDB"""
    try:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        db = client['Doctors_Appointments']
        doctors_collection = db['Appointments']
        
        pipeline = [
             {'$group': {
                        '_id': '$doctor_id',
                        'patient_count': {'$sum': 1}
                    }},
                    {'$sort': {'patient_count': -1}},
                    {'$lookup': {
                        'from': 'appointments',
                        'let': {'doctor_id': '$_id'},
                        'pipeline': [
                            {'$match': {
                                '$expr': {'$eq': ['$doctor_id', '$$doctor_id']}
                            }},
                            {'$lookup': {
                                'from': 'Patients',
                                'localField': 'patient_id',
                                'foreignField': 'patient_id',
                                'as': 'patient'
                            }},
                            {'$unwind': '$patient'},
                            {'$group': {
                                '_id': {
                                    'patient_id': '$patient_id',
                                    'first_name': '$patient.first_name',
                                    'last_name': '$patient.last_name'
                                }
                            }},
                            {'$project': {
                                'first_name': '$_id.first_name',
                                'last_name': '$_id.last_name',
                                '_id': 0
                            }}
                        ],
                        'as': 'Patients'
                    }},
                    {'$unwind': '$Patients'},
                    {'$replaceRoot': {'newRoot': '$Patients'}}
        ]

        start_time = time.time()
        print("MongoDB: Executing query...")

        cursor = doctors_collection.aggregate(pipeline)
        all_results = list(cursor)

        end_time = time.time()
        query_time = end_time - start_time
        print(f"Query executed in {query_time} seconds. Total results: {len(all_results)}")
        
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
    #mariadb_query_time = test_mariadb_query()  

    mongodb_query_time = test_mongodb_query() 

    system_stats = collect_system_stats()
    
    system_stats['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')

    # if mariadb_query_time is not None:
    #     system_stats['database'] = 'MariaDB'
    #     system_stats['query_time'] = mariadb_query_time
    #     save_to_csv(system_stats)

    if mongodb_query_time is not None:
        system_stats['database'] = 'MongoDB'
        system_stats['query_time'] = mongodb_query_time
        save_to_csv(system_stats)

if __name__ == '__main__':
    test_database_performance()
