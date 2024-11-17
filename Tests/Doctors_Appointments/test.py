import time
from pymongo import MongoClient

def test_step_by_step():
    try:
        client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
        db = client['Doctors_Appointments']
        
        # Krok 1: Znajdź kardiologów
        print("\nKrok 1: Wyszukiwanie kardiologów...")
        cardio_docs = list(db['Doctors'].find({'specialization': 'Cardiology'}))
        print(f"Znaleziono {len(cardio_docs)} kardiologów")
        
        cardio_ids = [doc['doctor_id'] for doc in cardio_docs]
        
        # Krok 2: Sprawdź liczbę wizyt dla kardiologów
        print("\nKrok 2: Liczenie wizyt dla kardiologów...")
        appointments_count = db['Appointments'].count_documents({
            'doctor_id': {'$in': cardio_ids}
        })
        print(f"Liczba wizyt dla kardiologów: {appointments_count}")
        
        # Krok 3: Znajdź wizyty i pacjentów etapami
        print("\nKrok 3: Wykonywanie zapytania etapami...")
        
        # Etap 1: Tylko match
        print("Etap 1: Filtrowanie wizyt...")
        pipeline1 = [
            {
                '$match': {
                    'doctor_id': {'$in': cardio_ids}
                }
            },
            {
                '$limit': 5  # Limit dla testu
            }
        ]
        
        results1 = list(db['Appointments'].aggregate(pipeline1))
        print(f"Przykładowe wizyty (5): {len(results1)}")
        
        # Etap 2: Match + Lookup
        print("\nEtap 2: Dołączanie danych pacjentów...")
        pipeline2 = [
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
                '$limit': 5  # Limit dla testu
            }
        ]
        
        results2 = list(db['Appointments'].aggregate(pipeline2))
        print(f"Przykładowe wizyty z pacjentami (5): {len(results2)}")
        
        # Etap 3: Pełne zapytanie z limitem
        # print("\nEtap 3: Testowanie pełnego pipeline z limitem...")
        # pipeline3 = [
        #     {
        #         '$match': {
        #             'doctor_id': {'$in': cardio_ids},
        #         }
        #     },
        #     {
        #         '$lookup': {
        #             'from': 'Patients',
        #             'localField': 'patient_id',
        #             'foreignField': 'patient_id',
        #             'as': 'patient'
        #         }
        #     },
        #     {
        #         '$unwind': {
        #             'path': '$patient',
        #             'preserveNullAndEmptyArrays': True
        #         }
        #     },
        #     {
        #         '$project': {
        #             'patient_id': 1,
        #             'patient_first_name': '$patient.first_name',
        #             'patient_last_name': '$patient.last_name',
        #             'doctor_id': 1,
        #             '_id': 0
        #         }
        #     },
        #     {
        #         '$limit': 5
        #     }
        # ]

        # results3 = list(db['Appointments'].aggregate(pipeline3))
        # print(f"Wyniki: {results3}")

        
        # results3 = list(db['Appointments'].aggregate(pipeline3))
        # print(f"Wyniki pełnego pipeline z limitem (5): {len(results3)}")
        
        # Etap 4: Pełne zapytanie porcjami
        print("\nEtap 4: Wykonywanie pełnego zapytania porcjami...")
        batch_size = 100  # Ustaw rozmiar batcha
        total_processed = 0
        pipeline_final = [
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
        cursor = db['Appointments'].aggregate(
            pipeline_final,
            allowDiskUse=True,
            batchSize=batch_size
        )
        
        print("Rozpoczynam przetwarzanie wyników...")
        try:
            for doc in cursor:
                total_processed += 1
                
                # Wyświetlaj co 10 wyników dla debugowania
                if total_processed % 10 == 0:
                    elapsed_time = time.time() - start_time
                    print(f"Przetworzono {total_processed} wyników w {elapsed_time:.2f} sekund")
                
                # Opcjonalnie wyświetl pierwsze 5 wyników
                if total_processed <= 5:
                    print(doc)

        except Exception as e:
            print(f"Błąd podczas przetwarzania wyników: {e}")
        
        end_time = time.time()
        total_time = end_time - start_time
        print(f"\nCałkowity czas: {total_time:.2f} sekund")
        print(f"Całkowita liczba wyników: {total_processed}")
        
        client.close()
        
    except Exception as e:
        print(f"Błąd podczas wykonywania testu: {e}")

if __name__ == "__main__":
    test_step_by_step()
