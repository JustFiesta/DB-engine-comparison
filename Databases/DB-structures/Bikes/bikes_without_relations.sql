--
-- Baza rowery
--
CREATE DATABASE bikes;

USE bikes;

CREATE TABLE Stations (
    station_id INT AUTO_INCREMENT PRIMARY KEY,
    station_name VARCHAR(255),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8)
);

CREATE TABLE TripUsers (
    trip_id INT AUTO_INCREMENT PRIMARY KEY,
    tripduration INT,
    starttime DATETIME,
    stoptime DATETIME,
    start_station_id INT,
    start_station_name VARCHAR(255),
    end_station_id INT,
    end_station_name VARCHAR(255),
    bikeid INT,
    birth_year INT,
    gender INT,
    usertype VARCHAR(255),
);