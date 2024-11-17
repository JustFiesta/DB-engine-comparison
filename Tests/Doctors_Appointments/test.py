import time
from pymongo import MongoClient

def test_mongodb_connection():
    """Test połączenia z bazą MongoDB"""
    try:
        client = MongoClient('mongodb://localhost:27017/', 
                           serverSelectionTimeoutMS=5000)
        db = client['Doctors_Appointments']
        print("MongoDB version:", client.server_info()['version'])
        print("Dostępne kolekcje:", db.list_collection_names())
        
        # Sprawdź liczę dokumentów w każdej kolekcji
        for collection in db.list_collection_names():
            count = db[collection].count_documents({})
            print(f"Kolekcja {collection}: {count} dokumentów")
            
        client.close()
        return True
    except Exception as e:
        print(f"Błąd połączenia: {e}")
        return False

def test_mongodb_query_debug():
    """Funkcja do debugowania zapytania MongoDB"""
    try:
        client = MongoClient('mongodb://localhost:27017/', 
                           serverSelectionTimeoutMS=5000)
        db = client['Doctors_Appointments']
        appointments_collection = db['Appointments']
        
        # Test 1: Znajdź kardiologów
        print("\nTest 1: Szukam kardiologów...")
        cardio_docs = list(db['Doctors'].find({'specialization': 'Cardiology'}))
        print(f"Znaleziono {len(cardio_docs)} kardiologów")
        if cardio_docs:
            print("Przykładowy kardiolog:", cardio_docs[0])
        
        # Test 2: Znajdź wizyty u kardiologów
        print("\nTest 2: Szukam wizyt u kardiologów...")
        cardio_ids = [doc['doctor_id'] for doc in cardio_docs]
        appointments = list(appointments_collection.find({'doctor_id': {'$in': cardio_ids}}).limit(5))
        print(f"Przykładowe wizyty (limit 5): {len(appointments)}")
        if appointments:
            print("Przykładowa wizyta:", appointments[0])
        
        # Test 3: Znajdź pacjentów z tych wizyt
        if appointments:
            print("\nTest 3: Szukam pacjentów...")
            patient_ids = [app['patient_id'] for app in appointments]
            patients = list(db['Patients'].find(
                {'patient_id': {'$in': patient_ids}},
                {'first_name': 1, 'last_name': 1}
            ).limit(5))
            print(f"Przykładowi pacjenci (limit 5): {len(patients)}")
            if patients:
                print("Przykładowy pacjent:", patients[0])
        
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
        if simple_results:
            print("Przykładowy wynik agregacji:", simple_results[0])
        
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
        print("\nZnajdowanie kardiologów...")
        cardio_ids = [doc['doctor_id'] for doc in db['Doctors'].find(
            {'specialization': 'Cardiology'},
            {'doctor_id': 1}
        )]
        
        if not cardio_ids:
            print("Nie znaleziono kardiologów!")
            return None, 0
            
        print(f"Znaleziono {len(cardio_ids)} kardiologów")
            
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
        print("\nMongoDB: Executing query...")
        
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
        
        print(f"\nQuery executed in {query_time} seconds. Total results: {total_results}")
        
        client.close()
        return query_time, total_results
        
    except Exception as e:
        print(f"Error: {e}")
        return None, 0

if __name__ == '__main__':
    print("=== Test połączenia z MongoDB ===")
    if test_mongodb_connection():
        print("\n=== Debug zapytania ===")
        test_mongodb_query_debug()
        print("\n=== Test zoptymalizowanego zapytania ===")
        test_mongodb_query_optimized()
    else:
        print("Nie można kontynuować testów z powodu błędu połączenia")