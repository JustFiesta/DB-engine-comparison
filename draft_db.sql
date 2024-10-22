-- Tworzenie tabeli dla lekarzy
CREATE TABLE doctors (
    doctor_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    specialization VARCHAR(50)
);

-- Tworzenie tabeli dla pacjentów
CREATE TABLE patients (
    patient_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    birthdate DATE,
    phone_number VARCHAR(20)
);

-- Tworzenie tabeli dla wizyt (appointments) z kluczami obcymi do lekarzy i pacjentów
CREATE TABLE appointments (
    appointment_id INT PRIMARY KEY,
    doctor_id INT,
    patient_id INT,
    appointment_date DATE,
    diagnosis VARCHAR(255),
    treatment VARCHAR(255),
    FOREIGN KEY (doctor_id) REFERENCES doctors(doctor_id),
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
);
