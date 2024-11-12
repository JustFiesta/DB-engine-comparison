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
        query = "SELECT * FROM Flights WHERE AIRLINE_DELAY > 60;"  
        start_time = time.time()

        cursor.execute(query)
        result = cursor.fetchall()

        end_time = time.time()
        query_time = end_time - start_time
        
        cursor.close()
        conn.close()

        return query_time
        
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def test_mongodb_query():
    """Funkcja do testowania zapytań w MongoDB"""
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client['Airports']
        collection = db['Flights']
        query = {"ARRIVAL_DELAY": {"$gt": 60}}  
        start_time = time.time()

        result = list(collection.find(query))
        
        end_time = time.time()
        query_time = end_time - start_time
        
        client.close()

        return query_time

    except Exception as e:
        print(f"Error: {e}")
        return None

def save_to_csv(data, filename="system_stats.csv"):
    """Funkcja zapisująca wyniki do pliku CSV"""
    with open(filename, mode='a', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=data.keys())
        # Jeśli plik jest pusty, dodaj nagłówki
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
    system_stats['mariadb_query_time'] = mariadb_query_time
    system_stats['mongodb_query_time'] = mongodb_query_time
    system_stats['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')

    # Zapisanie danych do pliku CSV
    save_to_csv(system_stats)

if __name__ == '__main__':
    # Jednorazowe testowanie wydajności
    test_database_performance()
