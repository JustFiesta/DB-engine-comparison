-- Załaduj dane lekarzy z doctors.csv
LOAD DATA INFILE '/path/to/doctors.csv'
INTO TABLE doctors
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES
(doctor_id, first_name, last_name, email, specialization);

-- Załaduj dane pacjentów z patients.csv
LOAD DATA INFILE '/path/to/patients.csv'
INTO TABLE patients
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES
(patient_id, first_name, last_name, birthdate, phone_number);

-- Załaduj dane wizyt z appointments.csv
LOAD DATA INFILE '/path/to/appointments.csv'
INTO TABLE appointments
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES
(appointment_id, doctor_id, patient_id, appointment_date, diagnosis, treatment);

-- Wyświetl pierwszych 10 lekarzy
SELECT * FROM doctors LIMIT 10;

-- Wyświetl pierwszych 10 pacjentów
SELECT * FROM patients LIMIT 10;

-- Wyświetl pierwszych 10 wizyt
SELECT * FROM appointments LIMIT 10;

-- Wyświetl lekarzy, którzy mieli wizyty z pacjentami
SELECT d.first_name AS doctor_name, p.first_name AS patient_name, a.appointment_date
FROM appointments a
JOIN doctors d ON a.doctor_id = d.doctor_id
JOIN patients p ON a.patient_id = p.patient_id
ORDER BY a.appointment_date;
