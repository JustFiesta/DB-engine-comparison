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
        query = """SELECT first_name, last_name
            FROM Patients
            WHERE patient_id IN (
                SELECT patient_id
                FROM Appointments
                GROUP BY patient_id, diagnosis
                HAVING COUNT(appointment_id) >= 2
            );
        """  
        start_time = time.time()

        print("MariaDB: Executing query...")
        cursor.execute(query)
        
        total_results = 0
        while True:
            result = cursor.fetchmany(100)
            if not result:
                break
            total_results += len(result)

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
        client = MongoClient('mongodb://localhost:27017/', 
                           serverSelectionTimeoutMS=5000,
                           maxPoolSize=50)
        db = client['Doctors_Appointments']
        appointments_collection = db['Appointments']
        
        appointments_collection.create_index([("doctor_id", 1)])
        appointments_collection.create_index([("patient_id", 1)])
        db['Doctors'].create_index([("specialization", 1)])
        
        pipeline = [
            {
                '$lookup': {
                    'from': 'Doctors',
                    'localField': 'doctor_id',
                    'foreignField': 'doctor_id',
                    'as': 'doctor'
                }
            },
            {
                '$unwind': '$doctor'
            },
            {
                '$match': {
                    'doctor.specialization': 'Cardiology'
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
                    'first_name': '$patient.first_name',
                    'last_name': '$patient.last_name',
                    '_id': 0
                }
            },
            {
                '$group': {
                    '_id': {
                        'first_name': '$first_name',
                        'last_name': '$last_name'
                    },
                    'first_name': {'$first': '$first_name'},
                    'last_name': {'$first': '$last_name'}
                }
            }
        ]

        start_time = time.time()
        print("MongoDB: Executing query...")

        cursor = appointments_collection.aggregate(
            pipeline,
            allowDiskUse=True,
            batchSize=1000  
        )

        total_results = 0
        for _ in cursor:  
            total_results += 1
                
        end_time = time.time()
        query_time = end_time - start_time
        print(f"Query executed in {query_time} seconds. Total results: {total_results}")
        
        client.close()

        return query_time, total_results

    except Exception as e:
        print(f"Error: {e}")
        return None, 0
    
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