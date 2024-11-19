--
-- Baza rowery
--
CREATE DATABASE Bikes;

USE Bikes;

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
    start_station_name VARCHAR(255),
    end_station_name VARCHAR(255),
    bikeid INT,
    birth_year INT NULL,
    gender INT,
    usertype VARCHAR(255),

    -- Klucze obce
    FOREIGN KEY (start_station_id) REFERENCES Stations(station_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,

    FOREIGN KEY (end_station_id) REFERENCES Stations(station_id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

-- Indeksy dla poprawy wydajności
CREATE INDEX idx_start_station ON TripUsers(start_station_id);
CREATE INDEX idx_end_station ON TripUsers(end_station_id);
CREATE INDEX idx_starttime ON TripUsers(starttime);
CREATE INDEX idx_bikeid ON TripUsers(bikeid);
