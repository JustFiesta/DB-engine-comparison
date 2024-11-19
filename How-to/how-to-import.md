# Importing data into MongoDB and MariaDB

These are steps for correct import of given datasets.

Every database structure is present in its own subfolder.

## MariaDB

0. Connect to MariaDB server via `mysql`/`mariadb`
1. Copy structure file into cli
2. Set mysqld options to speed up import process

    ```shell
    sudo vim /etc/mysql/mariadb.cnf

        # add this in .cnf file
        [mysqld]
        innodb_buffer_pool_size = 15G
        innodb_log_buffer_size = 1G

    sudo systemctl daemon-reload
    sudo systemctl restart mariadb
    ```

3. Disable foregin key checks to improve import time

    ```sql
    set global net_buffer_length=1000000;
    set global max_allowed_packet=1000000000;

    SET foreign_key_checks = 0;
    ```

4. Load data

    ```sql
    LOAD DATA LOCAL INFILE 'path/to/file.csv' INTO TABLE Table_name FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;
    (set, correct, dataset, columns, for, corresponding, db, structure)
    ```

5. Enable foreign key checks when procedure is complete

    ```sql
    SET foreign_key_checks = 1;
    ```

6. Check DBs with simple query

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

    ```shell
    use Doctors_Appointments
    ```

2. Use `mongoimport` to import database

    ```shell
    mongoimport --db Doctors_Appointments --collection Doctors --type csv --headerline --file /path/to/doctors.csv
    mongoimport --db Doctors_Appointments --collection Patients --type csv --headerline --file /path/to/patients.csv
    mongoimport --db Doctors_Appointments --collection Appointments --type csv --headerline --file /path/to/appointments.csv
    ```

3. Create "references" (relations)

    ```shell
    db.Appointments.find().forEach(function(appointment) {
        var doctor = db.doctors.findOne({ doctor_id: appointment.doctor_id });
        var patient = db.patients.findOne({ patient_id: appointment.patient_id });
        
        if (doctor && patient) {
            db.Appointments.update(
                { appointment_id: appointment.appointment_id },
                { $set: { doctor: doctor, patient: patient } }
            );
        }
    });
    ```

4. Check DB simple query
