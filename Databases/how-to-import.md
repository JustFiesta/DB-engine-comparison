# Importing data into MongoDB and MariaDB

These are steps for correct import of given datasets.

Every database structure is present in its own subfolder.

## MariaDB

0. Connect to MariaDB server via `mysql`/`mariadb`
1. Copy structure file into cli
2. Load data

    ```sql
    LOAD DATA LOCAL INFILE 'path/to/file.csv' INTO TABLE Table_name FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
    ```

3. Check DBs with simple query

    ```sql
    USE bikes;
    SELECT * FROM Stations;

    USE Doctor_Appointments;
    SELECT * FROM Doctors;

    USE Airports;
    SELECT * FROM Flights;
    ```

## MongoDB

0. Connect to MongoDB server via `monogsh`
1. Create database
2. Use `mongoimport` to import database

    ```shell
    mongoimport --db your_database_name --collection doctors --type csv --headerline --file /path/to/doctors.csv
    mongoimport --db your_database_name --collection patients --type csv --headerline --file /path/to/patients.csv
    mongoimport --db your_database_name --collection appointments --type csv --headerline --file /path/to/appointments.csv
    ```

3. Create "references" (relations)

    ```shell
    db.appointments.find().forEach(function(appointment) {
        var doctor = db.doctors.findOne({ doctor_id: appointment.doctor_id });
        var patient = db.patients.findOne({ patient_id: appointment.patient_id });
        
        if (doctor && patient) {
            db.appointments.update(
                { appointment_id: appointment.appointment_id },
                { $set: { doctor: doctor, patient: patient } }
            );
        }
    });
    ```

4. Check DB simple query
