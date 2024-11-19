--
-- Baza lekarzy
--
CREATE DATABASE Doctors_Appointments;

USE Doctors_Appointments;

-- Tworzenie tabeli dla lekarzy
CREATE TABLE Doctors (
    doctor_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    specialization VARCHAR(50) NOT NULL,

    -- Dodajemy indeks dla wyszukiwania po nazwisku
    INDEX idx_doctor_name (last_name, first_name),
    -- Dodajemy indeks dla wyszukiwania po specjalizacji
    INDEX idx_specialization (specialization)
);

-- Tworzenie tabeli dla pacjentów
CREATE TABLE Patients (
    patient_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    birthdate DATE NOT NULL,
    phone_number VARCHAR(20) NOT NULL,

    -- Dodajemy indeks dla wyszukiwania po nazwisku
    INDEX idx_patient_name (last_name, first_name),
    -- Dodajemy indeks dla wyszukiwania po dacie urodzenia
    INDEX idx_birthdate (birthdate),
    -- Dodajemy unikalny indeks dla numeru telefonu
    UNIQUE INDEX idx_phone (phone_number)
);

-- Tworzenie tabeli dla wizyt
CREATE TABLE Appointments (
    appointment_id INT AUTO_INCREMENT PRIMARY KEY,
    doctor_id INT NOT NULL,
    patient_id INT NOT NULL,
    appointment_date DATETIME NOT NULL,
    appointment_status ENUM('scheduled', 'completed', 'cancelled', 'no_show') NOT NULL DEFAULT 'scheduled',
    diagnosis VARCHAR(100),
    treatment VARCHAR(100),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Klucze obce
    FOREIGN KEY (doctor_id) 
        REFERENCES Doctors(doctor_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
        
    FOREIGN KEY (patient_id) 
        REFERENCES Patients(patient_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
        
    -- Indeksy
    INDEX idx_appointment_date (appointment_date),
    INDEX idx_appointment_status (appointment_status),
    
    -- Dodajemy ograniczenie uniemożliwiające duplikaty wizyt
    UNIQUE INDEX idx_doctor_patient_date (doctor_id, patient_id, appointment_date)
);
