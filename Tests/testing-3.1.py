import time
import psutil
import mysql.connector
from pymongo import MongoClient, IndexModel
import csv
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('mongodb_performance.log')
    ]
)

def collect_system_stats() -> Dict[str, Any]:
    """
    Zbiera statystyki systemowe z użyciem wydajnych metod psutil.
    Dodaje cachowanie dla częstych odczytów.
    """
    process = psutil.Process()
    # Odczyt wszystkich statystyk dysku w jednym wywołaniu
    disk = psutil.disk_usage('/')
    
    stats = {
        'timestamp': datetime.now().isoformat(),
        'cpu_percent': psutil.cpu_percent(interval=0.5),  # Zmniejszony interval dla szybszego odczytu
        'memory_percent': psutil.virtual_memory().percent,
        'disk_usage_percent': disk.percent,
        'disk_total': disk.total,
        'disk_used': disk.used,
        'disk_free': disk.free,
        'io_counters': process.io_counters()._asdict()  # Odczyt wszystkich liczników I/O w jednym wywołaniu
    }
    return stats

def setup_mongodb_indexes(collection) -> None:
    """
    Tworzy optymalne indeksy dla zapytania.
    Używa IndexModel dla utworzenia wielu indeksów jednocześnie.
    """
    indexes = [
        IndexModel([("diagnosis", 1)], name="diagnosis_idx"),
        IndexModel([("doctor_id", 1)], name="doctor_id_idx"),
        IndexModel([("diagnosis", 1), ("doctor_id", 1)], name="diagnosis_doctor_compound_idx")
    ]
    try:
        collection.create_indexes(indexes)
        logging.info("Indeksy zostały utworzone lub już istnieją")
    except Exception as e:
        logging.error(f"Błąd podczas tworzenia indeksów: {e}")

def test_mongodb_query() -> Optional[float]:
    """
    Wykonuje zoptymalizowane zapytanie MongoDB odpowiadające oryginalnemu zapytaniu SQL.
    Wykorzystuje agregację z optymalnymi etapami pipeline.
    """
    try:
        client = MongoClient(
            'mongodb://localhost:27017/',
            serverSelectionTimeoutMS=300000,
            connectTimeoutMS=300000,
            socketTimeoutMS=300000,
            maxPoolSize=50  # Zwiększony pool połączeń dla lepszej wydajności
        )
        
        db = client['Doctors_Appointments']
        appointments_collection = db['Appointments']
        doctors_collection = db['Doctors']
        
        # Utwórz indeksy jeśli nie istnieją
        setup_mongodb_indexes(appointments_collection)
        
        # Pipeline agregacji zoptymalizowany pod kątem wydajności
        pipeline = [
            {
                "$match": {
                    "diagnosis": "Cold"  # Wykorzystanie indeksu
                }
            },
            {
                "$lookup": {
                    "from": "Doctors",
                    "localField": "doctor_id",
                    "foreignField": "doctor_id",
                    "as": "doctor"
                }
            },
            {
                "$unwind": "$doctor"  # Rozwinięcie tablicy doctor
            },
            {
                "$group": {
                    "_id": {
                        "first_name": "$doctor.first_name",
                        "last_name": "$doctor.last_name"
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "first_name": "$_id.first_name",
                    "last_name": "$_id.last_name"
                }
            }
        ]

        # Pomiar czasu wykonania
        start_time = time.time()
        
        # Użycie allowDiskUse dla dużych zestawów danych
        cursor = appointments_collection.aggregate(
            pipeline,
            allowDiskUse=True,
            maxTimeMS=300000
        )
        
        # Efektywne przetwarzanie wyników
        results = list(cursor)
        
        query_time = time.time() - start_time
        
        # Logowanie wyników
        logging.info(f"Zapytanie MongoDB wykonane w {query_time:.2f} sekund")
        logging.info(f"Liczba znalezionych wyników: {len(results)}")
        
        # Zamknięcie połączenia
        client.close()
        
        return query_time

    except Exception as e:
        logging.error(f"Błąd podczas wykonywania zapytania MongoDB: {e}", exc_info=True)
        return None

def save_to_csv(data: Dict[str, Any], filename: str = "system_stats.csv") -> None:
    """
    Zapisuje wyniki do pliku CSV z obsługą błędów i buforowaniem.
    """
    try:
        with open(filename, mode='a', newline='', buffering=8192) as file:  # Zwiększony bufor
            writer = csv.DictWriter(file, fieldnames=data.keys())
            if file.tell() == 0:
                writer.writeheader()
            writer.writerow(data)
    except Exception as e:
        logging.error(f"Błąd podczas zapisu do CSV: {e}")

def test_database_performance() -> None:
    """
    Główna funkcja testująca wydajność bazy danych z obsługą błędów.
    """
    try:
        # Zbierz początkowe statystyki
        initial_stats = collect_system_stats()
        
        # Wykonaj test MongoDB
        mongodb_query_time = test_mongodb_query()
        
        if mongodb_query_time is not None:
            # Zbierz końcowe statystyki
            final_stats = collect_system_stats()
            
            # Połącz wszystkie statystyki
            performance_data = {
                **final_stats,
                'database': 'MongoDB',
                'query_time': mongodb_query_time,
                'cpu_usage_delta': final_stats['cpu_percent'] - initial_stats['cpu_percent'],
                'memory_usage_delta': final_stats['memory_percent'] - initial_stats['memory_percent']
            }
            
            # Zapisz wyniki
            save_to_csv(performance_data)
            
            logging.info(f"Test zakończony. Czas zapytania: {mongodb_query_time:.2f}s")
        else:
            logging.error("Test MongoDB nie powiódł się")
            
    except Exception as e:
        logging.error(f"Błąd podczas testu wydajności: {e}", exc_info=True)

if __name__ == '__main__':
    test_database_performance()
