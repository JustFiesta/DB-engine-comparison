import time
import psutil
import mysql.connector
from pymongo import MongoClient
import csv

def test_mongodb_query():
    """Funkcja do testowania zapytań w MongoDB z dodatkową diagnostyką"""
    try:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        db = client['Doctors_Appointments']
        collection = db['appointments']
        
        # Sprawdzenie całkowitej liczby dokumentów w kolekcji
        total_docs = collection.count_documents({})
        print(f"\nDiagnostyka MongoDB:")
        print(f"Całkowita liczba dokumentów w kolekcji: {total_docs}")
        
        # Pipeline z diagnostyką po każdym etapie
        pipeline_stages = [
            # Etap 1: Grupowanie
            [
                {
                    '$group': {
                        '_id': '$patient_id',
                        'unique_doctors': {'$addToSet': '$doctor_id'}
                    }
                }
            ],
            # Etap 2: Grupowanie + Liczenie doktorów
            [
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
                }
            ],
            # Pełny pipeline
            [
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
            ]
        ]

        # Wykonanie każdego etapu pipeline'u z diagnostyką
        for i, pipeline in enumerate(pipeline_stages, 1):
            results = list(collection.aggregate(pipeline))
            print(f"\nEtap {i} pipeline - liczba wyników: {len(results)}")
            if len(results) > 0:
                print(f"Przykładowy wynik: {results[0]}")
            
        # Właściwe wykonanie zapytania z pomiarem czasu
        start_time = time.time()
        print("\nMongoDB: Executing final query...")

        final_results = list(collection.aggregate(pipeline_stages[-1]))
        
        end_time = time.time()
        query_time = end_time - start_time
        total_results = len(final_results)
        
        print(f"\nWyniki końcowe:")
        print(f"Czas wykonania: {query_time} sekund")
        print(f"Liczba wyników: {total_results}")
        
        # Sprawdźmy strukturę pierwszych kilku dokumentów w kolekcji
        print("\nStruktura przykładowych dokumentów:")
        for doc in collection.find().limit(2):
            print(doc)
        
        client.close()
        return query_time, total_results

    except Exception as e:
        print(f"\nMongoDB Error: {e}")
        return None, 0

def test_database_performance():
    """
    Funkcja do jednorazowego testowania wydajności bazy danych.
    Wykonuje zapytania do baz danych, zbiera statystyki systemowe
    i zapisuje wynik w pliku CSV.
    """
    # Testowanie MongoDB
    mongodb_time, mongodb_results = test_mongodb_query()
    
    if mongodb_time is not None:
        # Zbieranie statystyk systemowych
        system_stats = collect_system_stats()
        system_stats['timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S')
        system_stats['database'] = 'MongoDB'
        system_stats['query_time'] = mongodb_time
        system_stats['total_results'] = mongodb_results
        save_to_csv(system_stats)

if __name__ == '__main__':
    test_database_performance()