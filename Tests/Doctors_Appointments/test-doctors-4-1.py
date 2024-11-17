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
                FROM Appointments a
                JOIN Doctors d ON a.doctor_id = d.doctor_id
                WHERE d.specialization = 'Cardiology'
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

def test_mongodb_query_debug():
    """Funkcja do debugowania zapytania MongoDB"""
    try:
        client = MongoClient('mongodb://localhost:27017/', 
                           serverSelectionTimeoutMS=5000)
        db = client['Doctors_Appointments']
        appointments_collection = db['Appointments']
        
        # Test 1: Znajdź kardiologów
        print("Test 1: Szukam kardiologów...")
        cardio_docs = list(db['Doctors'].find({'specialization': 'Cardiology'}))
        print(f"Znaleziono {len(cardio_docs)} kardiologów")
        
        # Test 2: Znajdź wizyty u kardiologów
        print("\nTest 2: Szukam wizyt u kardiologów...")
        cardio_ids = [doc['doctor_id'] for doc in cardio_docs]
        appointments = list(appointments_collection.find({'doctor_id': {'$in': cardio_ids}}).limit(5))
        print(f"Przykładowe wizyty (limit 5): {len(appointments)}")
        
        # Test 3: Znajdź pacjentów z tych wizyt
        if appointments:
            print("\nTest 3: Szukam pacjentów...")
            patient_ids = [app['patient_id'] for app in appointments]
            patients = list(db['Patients'].find(
                {'patient_id': {'$in': patient_ids}},
                {'first_name': 1, 'last_name': 1}
            ).limit(5))
            print(f"Przykładowi pacjenci (limit 5): {len(patients)}")
        
        # Test 4: Proste agregacje
        print("\nTest 4: Testuje prostą agregację...")
        pipeline_simple = [
            {
                '$lookup': {
                    'from': 'Doctors',
                    'localField': 'doctor_id',
                    'foreignField': 'doctor_id',
                    'as': 'doctor'
                }
            },
            {
                '$limit': 5
            }
        ]
        
        simple_results = list(appointments_collection.aggregate(pipeline_simple))
        print(f"Prosta agregacja (limit 5): {len(simple_results)}")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"Błąd podczas debugowania: {e}")
        return False

def test_mongodb_query_optimized():
    """Zoptymalizowana wersja oryginalnego zapytania"""
    try:
        client = MongoClient('mongodb://localhost:27017/', 
                           serverSelectionTimeoutMS=5000)
        db = client['Doctors_Appointments']
        
        # Najpierw znajdź ID kardiologów
        cardio_ids = [doc['doctor_id'] for doc in db['Doctors'].find(
            {'specialization': 'Cardiology'},
            {'doctor_id': 1}
        )]
        
        if not cardio_ids:
            print("Nie znaleziono kardiologów!")
            return None, 0
            
        # Następnie znajdź wizyty tylko dla tych lekarzy
        pipeline = [
            {
                '$match': {
                    'doctor_id': {'$in': cardio_ids}
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
                '$group': {
                    '_id': {
                        'patient_id': '$patient_id'
                    },
                    'first_name': {'$first': '$patient.first_name'},
                    'last_name': {'$first': '$patient.last_name'}
                }
            }
        ]
        
        start_time = time.time()
        print("MongoDB: Executing query...")
        
        cursor = db['Appointments'].aggregate(
            pipeline,
            allowDiskUse=True,
            batchSize=100
        )
        
        total_results = 0
        for _ in cursor:
            total_results += 1
            if total_results % 100 == 0:  # Logowanie postępu
                print(f"Przetworzono {total_results} wyników...")
                
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
