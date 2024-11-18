
--
-- Baza loty
--
CREATE DATABASE Airports;

USE Airports;

CREATE TABLE Airlines (
    IATA_CODE CHAR(2) PRIMARY KEY,
    AIRLINE VARCHAR(100)
);

CREATE TABLE Airports (
    IATA_CODE CHAR(3) PRIMARY KEY,
    AIRPORT VARCHAR(100),
    CITY VARCHAR(50),
    STATE VARCHAR(2),
    COUNTRY VARCHAR(3),
    LATITUDE DECIMAL(8,5),
    LONGITUDE DECIMAL(8,5)
);

CREATE TABLE Cancellation_codes (
    CANCELLATION_REASON CHAR(1)  PRIMARY KEY,
    CANCELLATION_DESCRIPTION VARCHAR(100)
);

CREATE TABLE Flights (
    FLIGHT_ID BIGINT AUTO_INCREMENT PRIMARY KEY,
    YEAR INT,
    MONTH INT,
    DAY INT,
    DAY_OF_WEEK INT,
    AIRLINE CHAR(2),
    FLIGHT_NUMBER INT,
    TAIL_NUMBER VARCHAR(10),
    ORIGIN_AIRPORT CHAR(3),
    DESTINATION_AIRPORT CHAR(3),
    SCHEDULED_DEPARTURE INT,
    DEPARTURE_TIME INT,
    DEPARTURE_DELAY INT,
    TAXI_OUT INT,
    WHEELS_OFF INT,
    SCHEDULED_TIME INT,
    ELAPSED_TIME INT,
    AIR_TIME INT,
    DISTANCE INT,
    WHEELS_ON INT,
    TAXI_IN INT,
    SCHEDULED_ARRIVAL INT,
    ARRIVAL_TIME INT,
    ARRIVAL_DELAY INT,
    DIVERTED TINYINT(1),
    CANCELLED TINYINT(1),
    CANCELLATION_REASON CHAR(1),
    AIR_SYSTEM_DELAY INT,
    SECURITY_DELAY INT,
    AIRLINE_DELAY INT,
    LATE_AIRCRAFT_DELAY INT,
    WEATHER_DELAY INT,
   
    -- Klucze obce
    FOREIGN KEY (AIRLINE) 
        REFERENCES Airlines(IATA_CODE)
        ON DELETE SET NULL
        ON UPDATE CASCADE,
        
    FOREIGN KEY (ORIGIN_AIRPORT) 
        REFERENCES Airports(IATA_CODE)
        ON DELETE SET NULL
        ON UPDATE CASCADE,
        
    FOREIGN KEY (DESTINATION_AIRPORT) 
        REFERENCES Airports(IATA_CODE)
        ON DELETE SET NULL
        ON UPDATE CASCADE,
        
    FOREIGN KEY (CANCELLATION_REASON) 
        REFERENCES Cancellation_codes(CANCELLATION_REASON)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

-- Indeksy dla poprawy wydajności
CREATE INDEX idx_flight_date ON Flights(YEAR, MONTH, DAY);
CREATE INDEX idx_airline ON Flights(AIRLINE);
CREATE INDEX idx_origin ON Flights(ORIGIN_AIRPORT);
CREATE INDEX idx_destination ON Flights(DESTINATION_AIRPORT);
CREATE INDEX idx_flight_number ON Flights(FLIGHT_NUMBER);
CREATE INDEX idx_cancelled ON Flights(CANCELLED);
