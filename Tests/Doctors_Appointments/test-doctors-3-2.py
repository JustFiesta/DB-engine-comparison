import time
import psutil
import mysql.connector
from pymongo import MongoClient
import csv
import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(),  
    ]
)

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
            d.first_name, 
            d.last_name
        FROM 
            Appointments a
        JOIN 
            Doctors d
        ON 
            a.doctor_id = d.doctor_id
        WHERE 
            a.diagnosis = 'Cold'
        GROUP BY 
            d.first_name, 
            d.last_name;   
        """  
        start_time = time.time()

        print("MariaDB: Executing query...")
        cursor.execute(query)
        
        total_results = 0
        result = cursor.fetchmany(100)  
        while result:
            total_results += len(result)  
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
    try:
        logging.info("Rozpoczynam połączenie z MongoDB")
        client = MongoClient('mongodb://localhost:27017/', 
            serverSelectionTimeoutMS=300000,
            connectTimeoutMS=300000,
            socketTimeoutMS=300000
        )
        
        logging.info("Sprawdzam dostępność bazy danych")
        client.admin.command('ismaster')
        
        logging.info("Wybieranie bazy danych")
        db = client['Doctors_Appointments']
        
        logging.info("Wybieranie kolekcji")
        doctors_collection = db['Appointments']
        
        logging.info("Tworzenie indeksów")
        doctors_collection.create_index('diagnosis')
        doctors_collection.create_index('doctor_id')
        
        logging.info("Przygotowywanie pipeline agregacji")
        pipeline = [
            {'$match': {'diagnosis': 'Cold'}},
            {'$lookup': {
                'from': 'Doctors',
                'localField': 'doctor_id',
                'foreignField': 'doctor_id',
                'as': 'doctor'
            }},
            {'$unwind': '$doctor'},
            {'$group': {
                '_id': {
                    'first_name': '$doctor.first_name',
                    'last_name': '$doctor.last_name'
                },
                'count': {'$sum': 1} 
            }},
            {'$project': {
                'first_name': '$_id.first_name',
                'last_name': '$_id.last_name',
                'count': 1,
                '_id': 0
            }}
        ]

        logging.info("Wykonywanie zapytania agregacji")
        start_time = time.time()
        cursor = doctors_collection.aggregate(pipeline, maxTimeMS=300000)  
        
        logging.info("Przetwarzanie wyników")
        all_results = list(cursor)

        end_time = time.time()
        query_time = end_time - start_time
        
        logging.info(f"Zapytanie wykonane. Czas: {query_time} sekund. Wyniki: {len(all_results)}")
        
        client.close()
        return query_time

    except Exception as e:
        logging.error(f"Błąd MongoDB: {e}", exc_info=True)
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
    test_database_performance()
