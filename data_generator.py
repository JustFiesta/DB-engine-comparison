from faker import Faker
import pandas as pd
import random

fake = Faker()

# Lista przykładowych domen
domains = ['example.com', 'hospital.com', 'medclinic.org', 'healthcare.net']


# Generowanie lekarzy z emailami opartymi o imię i nazwisko
def generate_doctors(n):
    doctors = []
    specializations = ['Kardiologia', 'Neurologia', 'Ortopedia', 'Pediatria', 'Dermatologia']

    for i in range(1, n + 1):
        first_name = fake.first_name()
        last_name = fake.last_name()

        # Tworzenie emaila na podstawie imienia, nazwiska i losowej domeny
        email = f"{first_name.lower()}.{last_name.lower()}@{random.choice(domains)}"

        doctors.append({
            'doctor_id': i,
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'specialization': random.choice(specializations)
        })

    return doctors


# Generowanie pacjentów
def generate_patients(n):
    patients = []
    for i in range(1, n + 1):
        patients.append({
            'patient_id': i,
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'birthdate': fake.date_of_birth(minimum_age=0, maximum_age=90),
            'phone_number': fake.phone_number()
        })
    return patients


# Generowanie wizyt pacjentów u lekarzy
def generate_appointments(n, doctor_ids, patient_ids):
    diagnoses = [
        'Nadciśnienie tętnicze', 'Cukrzyca typu 2', 'Przeziębienie', 'Zapalenie płuc',
        'Astma', 'Migrena', 'Infekcja dróg moczowych', 'Depresja', 'Reumatoidalne zapalenie stawów',
        'Zaburzenia snu', 'Niewydolność serca', 'Choroba Alzheimera', 'Zapalenie oskrzeli'
    ]

    treatments = [
        'Leczenie farmakologiczne', 'Fizjoterapia', 'Konsultacja psychologiczna',
        'Zabieg chirurgiczny', 'Antybiotyki', 'Leki przeciwzapalne', 'Ćwiczenia oddechowe',
        'Dieta niskosodowa', 'Leki przeciwbólowe', 'Leki przeciwhistaminowe'
    ]

    appointments = []
    for i in range(1, n + 1):
        doctor_id = random.choice(doctor_ids)
        patient_id = random.choice(patient_ids)
        appointments.append({
            'appointment_id': i,
            'doctor_id': doctor_id,
            'patient_id': patient_id,
            'appointment_date': fake.date_this_year(),
            'diagnosis': random.choice(diagnoses),
            'treatment': random.choice(treatments)
        })
    return appointments


# Zapis danych do plików CSV
def save_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)


# Główna funkcja do generowania danych
def generate_database(num_doctors, num_patients, num_appointments):
    doctors = generate_doctors(num_doctors)
    patients = generate_patients(num_patients)
    appointments = generate_appointments(num_appointments, [d['doctor_id'] for d in doctors],
                                         [p['patient_id'] for p in patients])

    # Zapisanie danych do plików CSV
    save_to_csv(doctors, 'doktorzy10.csv')
    save_to_csv(patients, 'pacjenci10.csv')
    save_to_csv(appointments, 'wizyty10.csv')


if __name__ == "__main__":
    num_doctors = int(input("Ile lekarzy wygenerować? "))
    num_patients = int(input("Ile pacjentów wygenerować? "))
    num_appointments = int(input("Ile wizyt wygenerować? "))

    generate_database(num_doctors, num_patients, num_appointments)
    print("Dane zostały wygenerowane i zapisane do plików CSV.")
