# QUERIES

These are queries for testing databases.

## Database Airports

1. Simple queries

Retrieves the flight ID, flight number, and arrival delay for flights with delays exceeding 60 minutes.

MariaDB:

  ```sql
    SELECT FLIGHT_ID, FLIGHT_NUMBER, ARRIVAL_DELAY 
    FROM Flights 
    WHERE ARRIVAL_DELAY > 60;
  ```

MongoDB:

  ```shell
    {   'collection': 'Flights',
        'query': {"ARRIVAL_DELAY": {"$gt": 60}},
        'projection': {"_id": 0, "FLIGHT_NUMBER": 1, "ARRIVAL_DELAY": 1}
    }
  ```

Returns the name of the airline with the IATA code 'AA'.

MariaDB:

  ```sql
    SELECT AIRLINE 
    FROM Airlines 
    WHERE IATA_CODE = 'AA';
  ```

MongoDB:

  ```shell
    {
        'collection': 'Airlines',
        'query': {"IATA_CODE": "AA"},
        'projection': {"_id": 0, "AIRLINE": 1}
    }
  ```

Displays the names of airports and their corresponding cities in California (state code 'CA').

MariaDB:

  ```sql
    SELECT AIRPORT, CITY 
    FROM Airports 
    WHERE STATE = 'CA';
  ```

MongoDB:

  ```shell
    {
        'collection': 'Airports',
        'query': { "STATE": "CA" },
        'projection': {"_id": 0, "AIRPORT": 1, "CITY": 1}
    }
  ```

2. Aggregation

Calculates the average arrival delay for flights, grouped by the day of the week.

MariaDB:

  ```sql
    SELECT DAY_OF_WEEK, AVG(ARRIVAL_DELAY) AS avg_arrival_delay 
    FROM Flights 
    GROUP BY DAY_OF_WEEK;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Flights',
        'pipeline': [
            {
                "$group": {
                    "_id": "$DAY_OF_WEEK",  
                    "avg_arrival_delay": { "$avg": "$ARRIVAL_DELAY" }
                }
            }
        ]
    },
  ```

Displays the number of canceled flights, grouped by cancellation reason.

MariaDB:

  ```sql
    SELECT CANCELLATION_REASON, COUNT(*) AS cancel_count 
    FROM Flights 
    WHERE CANCELLED = 1 
    GROUP BY CANCELLATION_REASON;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Flights',
        'pipeline': [
            {
                "$match": {
                    "CANCELLED": 1,
                    "CANCELLATION_REASON": { "$exists": True }
                }
            },
            {
                "$group": {
                    "_id": "$CANCELLATION_REASON", 
                    "cancel_count": { "$sum": 1 }  
                }
            }
        ]
    },
  ```

Shows the number of flights operated by airline UA (United Airlines) for each day.

MariaDB:

  ```sql
    SELECT YEAR, MONTH, DAY, COUNT(*) AS flight_count 
    FROM Flights 
    WHERE AIRLINE = 'UA' 
    GROUP BY YEAR, MONTH, DAY;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Flights',
        'pipeline': [
                                    {
                "$match": {
                    "AIRLINE": "UA",
                    "YEAR": { "$exists": True },
                    "MONTH": { "$exists": True },
                    "DAY": { "$exists": True }
                }
            },
            {
                "$group": {
                    "_id": {
                        "year": "$YEAR",  
                        "month": "$MONTH",  
                        "day": "$DAY"  
                    },
                    "flight_count": { "$sum": 1 } 
                }
            }
        ]
    },
  ```

3. Join

Displays the number of flights grouped by airline.

MariaDB:

  ```sql
    SELECT a.AIRLINE, COUNT(f.FLIGHT_NUMBER) AS flight_count 
    FROM Flights f 
    JOIN Airlines a ON f.AIRLINE = a.IATA_CODE 
    GROUP BY a.AIRLINE;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Flights',
        'pipeline': [
            {
                "$group": {
                    "_id": "$AIRLINE",
                    "flight_count": {"$sum": 1}
                }
            },
            {
                "$lookup": {
                    "from": "Airlines",
                    "localField": "_id",
                    "foreignField": "IATA_CODE",
                    "as": "airline_info",
                    "pipeline": [
                        { "$project": { "AIRLINE": 1, "_id": 0 } }
                    ]
                }
            },
            {
                "$set": {
                    "airline_info": { "$arrayElemAt": ["$airline_info", 0] }
                }
            },
            {
                "$project": {
                    "airline": "$airline_info.AIRLINE",
                    "flight_count": 1,
                    "_id": 0
                }
            }
        ]
    },
  ```

Lists flights with delays greater than 100 minutes, including the airline name, origin airport, destination airport, and delay time. Results are sorted by delay in descending order.

MariaDB:

  ```sql
    SELECT 
        a.AIRLINE AS airline_name,
        orig.AIRPORT AS origin_airport,
        dest.AIRPORT AS destination_airport,
        f.ARRIVAL_DELAY
    FROM Flights f
    JOIN Airlines a ON f.AIRLINE = a.IATA_CODE
    JOIN Airports orig ON f.ORIGIN_AIRPORT = orig.IATA_CODE
    JOIN Airports dest ON f.DESTINATION_AIRPORT = dest.IATA_CODE
    WHERE f.ARRIVAL_DELAY > 100
    ORDER BY f.ARRIVAL_DELAY DESC;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Flights',
        'pipeline': [
            {
                "$match": {
                    "ARRIVAL_DELAY": {"$gt": 100}
                }
            },
            {
                "$lookup": {
                    "from": "Airlines",
                    "localField": "AIRLINE",
                    "foreignField": "IATA_CODE",
                    "as": "airline_info",
                    "pipeline": [
                        { "$project": { "AIRLINE": 1, "_id": 0 } }
                    ]
                }
            },
            {
                "$set": {
                    "airline_info": { "$arrayElemAt": ["$airline_info", 0] }
                }
            },
            {
                "$lookup": {
                    "from": "Airports",
                    "localField": "ORIGIN_AIRPORT",
                    "foreignField": "IATA_CODE",
                    "as": "origin_airport",
                    "pipeline": [
                        { "$project": { "AIRPORT": 1, "_id": 0 } }
                    ]
                }
            },
            {
                "$set": {
                    "origin_airport": { "$arrayElemAt": ["$origin_airport", 0] }
                }
            },
            {
                "$lookup": {
                    "from": "Airports",
                    "localField": "DESTINATION_AIRPORT",
                    "foreignField": "IATA_CODE",
                    "as": "destination_airport",
                    "pipeline": [
                        { "$project": { "AIRPORT": 1, "_id": 0 } }
                    ]
                }
            },
            {
                "$set": {
                    "destination_airport": { "$arrayElemAt": ["$destination_airport", 0] }
                }
            },
            {
                "$project": {
                    "airline_name": "$airline_info.AIRLINE",
                    "origin_airport": "$origin_airport.AIRPORT",
                    "destination_airport": "$destination_airport.AIRPORT",
                    "arrival_delay": "$ARRIVAL_DELAY",
                    "_id": 0
                }
            },
            {
                "$sort": {"arrival_delay": -1}
            }
        ]
    }
  ```

Displays destination airports and airline names for flights delayed by more than 120 minutes.

MariaDB:

  ```sql
    SELECT ap.AIRPORT AS destination_airport, a.AIRLINE AS airline_name 
    FROM Flights f 
    JOIN Airlines a ON f.AIRLINE = a.IATA_CODE 
    JOIN Airports ap ON f.DESTINATION_AIRPORT = ap.IATA_CODE 
    WHERE f.ARRIVAL_DELAY > 120;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Flights',
        'pipeline': [
            {
                "$match": {
                    "ARRIVAL_DELAY": {"$gt": 120}
                }
            },
            {
                "$lookup": {
                    "from": "Airlines",
                    "localField": "AIRLINE",
                    "foreignField": "IATA_CODE",
                    "as": "airline_info",
                    "pipeline": [
                        { "$project": { "AIRLINE": 1, "_id": 0 } }
                    ]
                }
            },
            {
                "$set": {
                    "airline_info": { "$arrayElemAt": ["$airline_info", 0] }
                }
            },
            {
                "$lookup": {
                    "from": "Airports",
                    "localField": "ORIGIN_AIRPORT",
                    "foreignField": "IATA_CODE",
                    "as": "origin_airport",
                    "pipeline": [
                        { "$project": { "AIRPORT": 1, "_id": 0 } }
                    ]
                }
            },
            {
                "$set": {
                    "origin_airport": { "$arrayElemAt": ["$origin_airport", 0] }
                }
            },
            {
                "$lookup": {
                    "from": "Airports",
                    "localField": "DESTINATION_AIRPORT",
                    "foreignField": "IATA_CODE",
                    "as": "destination_airport",
                    "pipeline": [
                        { "$project": { "AIRPORT": 1, "_id": 0 } }
                    ]
                }
            },
            {
                "$set": {
                    "destination_airport": { "$arrayElemAt": ["$destination_airport", 0] }
                }
            },
            {
                "$project": {
                    "airline_name": "$airline_info.AIRLINE",
                    "origin_airport": "$origin_airport.AIRPORT",
                    "destination_airport": "$destination_airport.AIRPORT",
                    "arrival_delay": "$ARRIVAL_DELAY",
                    "_id": 0
                }
            },
            {
                "$sort": {"arrival_delay": -1}
            }
        ]
    }
  ```

4. Subqueries

Lists airlines with their average delay times, where the delay exceeds the global average delay.

MariaDB:

  ```sql
    SELECT 
        f.AIRLINE,
        a.AIRLINE AS AIRLINE_NAME,
        ROUND(AVG(f.ARRIVAL_DELAY), 2) AS AVG_DELAY
    FROM Flights f
    JOIN Airlines a ON f.AIRLINE = a.IATA_CODE
    GROUP BY f.AIRLINE
    HAVING AVG_DELAY > (
        SELECT AVG(ARRIVAL_DELAY) 
        FROM Flights
    )
    ORDER BY AVG_DELAY DESC;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Flights',
        'pipeline': [
            {
                "$group": {
                    "_id": "$DAY_OF_WEEK",  
                    "avg_arrival_delay": { "$avg": "$ARRIVAL_DELAY" }
                }
            }
        ]
    }
  ```

Lists unique airports (including their city and state) that serve flights traveling longer than the average distance.

MariaDB:

  ```sql
    SELECT 
        DISTINCT a.AIRPORT,
        a.CITY,
        a.STATE
    FROM Airports a
    JOIN Flights f ON a.IATA_CODE = f.ORIGIN_AIRPORT
    WHERE f.DISTANCE > (
        SELECT AVG(DISTANCE) 
        FROM Flights
    )
    ORDER BY a.STATE, a.CITY;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Flights',
        'pipeline': [
            {
                "$facet": {
                    "totalAvgDelay": [
                        {
                            "$group": {
                                "_id": 1,
                                "avg": { "$avg": "$ARRIVAL_DELAY" }
                            }
                        }
                    ],
                    "airlineDelays": [
                        {
                            "$group": {
                                "_id": "$AIRLINE",
                                "avgDelay": { "$avg": "$ARRIVAL_DELAY" }
                            }
                        },
                        {
                            "$lookup": {
                                "from": "Airlines",
                                "localField": "_id",
                                "foreignField": "IATA_CODE",
                                "as": "airline_info"
                            }
                        },
                        {
                            "$unwind": "$airline_info"
                        }
                    ]
                }
            },
            {
                "$unwind": "$totalAvgDelay"
            },
            {
                "$project": {
                    "results": {
                        "$filter": {
                            "input": "$airlineDelays",
                            "as": "airline",
                            "cond": { "$gt": ["$$airline.avgDelay", "$totalAvgDelay.avg"] }
                        }
                    }
                }
            },
            {
                "$unwind": "$results"
            },
            {
                "$project": {
                    "_id": "$results._id",
                    "airlineName": "$results.airline_info.AIRLINE",
                    "avgDelay": { "$round": ["$results.avgDelay", 2] }
                }
            },
            {
                "$sort": { "avgDelay": -1 }
            }
        ],
    }
  ```

For each month, displays the airline with the highest number of delayed flights.

MariaDB:

  ```sql
    SELECT 
        f.MONTH,
        a.AIRLINE AS AIRLINE_NAME,
        COUNT(*) AS DELAYED_FLIGHTS
    FROM Flights f
    JOIN Airlines a ON f.AIRLINE = a.IATA_CODE
    WHERE f.ARRIVAL_DELAY > 0
    GROUP BY f.MONTH, f.AIRLINE
    HAVING DELAYED_FLIGHTS = (
        SELECT COUNT(*) 
        FROM Flights f2 
        WHERE f2.MONTH = f.MONTH 
        AND f2.ARRIVAL_DELAY > 0 
        GROUP BY f2.AIRLINE 
        ORDER BY COUNT(*) DESC 
        LIMIT 1
    )
    ORDER BY f.MONTH;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Flights',
        'pipeline': [
            {
                "$match": {
                    "ARRIVAL_DELAY": { "$gt": 0 }
                }
            },
            {
                "$group": {
                    "_id": {
                        "month": "$MONTH",
                        "airline": "$AIRLINE"
                    },
                    "delayed_flights": { "$sum": 1 }
                }
            },
            {
                "$group": {
                    "_id": "$_id.month",
                    "maxDelays": { "$max": "$delayed_flights" },
                    "allData": {
                        "$push": {
                            "airline": "$_id.airline",
                            "delayed_flights": "$delayed_flights"
                        }
                    }
                }
            },
            {
                "$project": {
                    "airline_data": {
                        "$filter": {
                            "input": "$allData",
                            "as": "item",
                            "cond": { "$eq": ["$$item.delayed_flights", "$maxDelays"] }
                        }
                    }
                }
            },
            {
                "$unwind": "$airline_data"
            },
            {
                "$lookup": {
                    "from": "Airlines",
                    "localField": "airline_data.airline",
                    "foreignField": "IATA_CODE",
                    "as": "airline_info"
                }
            },
            {
                "$unwind": "$airline_info"
            },
            {
                "$project": {
                    "_id": 0,
                    "month": "$_id",
                    "airline_name": "$airline_info.AIRLINE",
                    "delayed_flights": "$airline_data.delayed_flights"
                }
            },
            {
                "$sort": { "month": 1 }
            }
        ]
    }
  ```

## Database Bikes

1. Simple queries

Retrieves all records for trips where the duration exceeds 30 minutes (converted to seconds).

MariaDB:

  ```sql
    SELECT * FROM TripUsers WHERE tripduration > 30 * 60;
  ```

MongoDB:

  ```shell
    {
        'collection': 'TripUsers',
        'query': {"tripduration": { "$gt": 1800 } },
        'projection': None 
    }
  ```

Retrieves all records of trips that end at the station named 'Newport Pkwy'.

MariaDB:

  ```sql
    SELECT * FROM TripUsers WHERE end_station_name = 'Newport Pkwy';
  ```

MongoDB:

  ```shell
    {
        'collection': 'TripUsers',
        'query': {"end_station_name": "Newport Pkwy" },
        'projection': None
    }
  ```

Displays the unique types of users (e.g., subscriber, customer) in the TripUsers table.

MariaDB:

  ```sql
    SELECT DISTINCT usertype FROM TripUsers;
  ```

MongoDB:

  ```shell
    { 
        'collection': 'TripUsers',
        'pipeline': [
            {
                "$group": {"_id": "$usertype"}
            },  
            {
                "$project": {"usertype": "$_id", "_id": 0}
            }  
        ]
    }
  ```

2. Aggregation

Groups the records by user type and counts the number of trips for each type.

MariaDB:

  ```sql
    SELECT usertype, COUNT(*) AS trip_count FROM TripUsers GROUP BY usertype;
  ```

MongoDB:

  ```shell
    {
        'collection': 'TripUsers',
        'pipeline': [
            {
                "$group": {
                    "_id": "$usertype",
                    "trip_count": {"$sum": 1}  
                }
            },
            {
                "$project": {
                    "usertype": "$_id",
                    "trip_count": 1,
                    "_id": 0
                }
            },
            {
                "$sort": {"usertype": 1} 
            }
        ]
    }
  ```


Groups the records by birth year and calculates the total trip duration for each year.

MariaDB:

  ```sql
    SELECT birth_year, SUM(tripduration) AS total_tripduration FROM TripUsers GROUP BY birth_year;
  ```

MongoDB:

  ```shell
    {
        'collection': 'TripUsers',
        'pipeline': [
            {
                "$group": {
                    "_id": "$birth_year",
                    "total_tripduration": {"$sum": "$tripduration"}
                }
            },
            {
                "$project": {
                    "birth_year": "$_id",
                    "total_tripduration": 1,
                    "_id": 0
                }
            },
            {
                "$sort": {"birth_year": 1}  
            },
            {
                "$match": {  
                    "birth_year": {"$ne": None}
                }
            }
        ]
    }   
  ```

 Groups the records by gender and calculates the average trip duration for each gender.

MariaDB:

  ```sql
    SELECT gender, AVG(tripduration) AS average_tripduration FROM TripUsers GROUP BY gender;
  ```

MongoDB:

  ```shell
    {
        'collection': 'TripUsers',
        'pipeline': [
            {
                "$group": {
                    "_id": "$gender",
                    "average_tripduration": {"$avg": "$tripduration"},
                    "count": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "gender": "$_id",
                    "average_tripduration": 1,
                    "count": 1,
                    "_id": 0
                }
            },
            {
                "$sort": {"gender": 1}
            },
            {
                "$match": {
                    "gender": {"$ne": None}
                }
            }
        ]
    }
  ```

3. Join

Retrieves trip details for trips longer than 20 minutes, including the names of the start and end stations by joining the TripUsers table with the Stations table

MariaDB:

  ```sql
    SELECT 
        t.trip_id, t.tripduration, s.station_name AS start_stat_name, e.station_name AS end_stat_name 
    FROM TripUsers t 
    JOIN Stations s ON t.start_station_id = s.station_id 
    JOIN Stations e ON t.end_station_id = e.station_id 
    WHERE t.tripduration > 20;
  ```

MongoDB:

  ```shell
    {
        'collection': 'TripUsers',
        'pipeline': [
            {
                "$match": {
                    "tripduration": {"$gt": 20}  
                }
            },
            {
                "$lookup": {
                    "from": "Stations",
                    "localField": "start_station_id",
                    "foreignField": "station_id",
                    "as": "start_station"
                }
            },
            {
                "$lookup": {
                    "from": "Stations",
                    "localField": "end_station_id",
                    "foreignField": "station_id",
                    "as": "end_station"
                }
            },
            {
                "$unwind": "$start_station"
            },
            {
                "$unwind": "$end_station"
            },
            {
                "$project": {
                    "trip_id": 1,
                    "tripduration": 1,
                    "start_station_name": "$start_station.station_name",
                    "end_station_name": "$end_station.station_name",
                    "_id": 0
                }
            },
            {
                "$sort": {"trip_id": 1}
            },
        ]
    }
  ```

Similar to the previous query, but uses aliases for the Stations table for start and end stations.

MariaDB:

  ```sql
    SELECT 
        t.trip_id, t.tripduration, start_stations.station_name AS start_station_name, end_stations.station_name AS end_station_name 
    FROM TripUsers t 
    JOIN  Stations AS start_stations ON t.start_station_id = start_stations.station_id 
    JOIN Stations AS end_stations ON t.end_station_id = end_stations.station_id 
    WHERE t.tripduration > 20;
  ```

MongoDB:

  ```shell
    {
        'collection': 'TripUsers',
        'pipeline': [
            {
                '$match': {
                    'tripduration': {'$gt': 20}
            }
            },
            {
                '$lookup': {
                    'from': 'stations',
                    'localField': 'start_station_id',
                    'foreignField': 'station_id',
                    'as': 'start_station'
                }
            },
            {
                '$lookup': {
                    'from': 'stations',
                    'localField': 'end_station_id',
                    'foreignField': 'station_id',
                    'as': 'end_station'
                }
            },
            {
                '$project': {
                    'trip_id': 1,
                    'tripduration': 1,
                    'start_station_name': {'$arrayElemAt': ['$start_station.station_name', 0]},
                    'end_station_name': {'$arrayElemAt': ['$end_station.station_name', 0]}
                }
            }
        ]
    }
  ```

Retrieves detailed trip information, including start and end station names, by joining the TripUsers table with the Stations table.

MariaDB:

  ```sql
    SELECT 
        t.trip_id, t.tripduration, t.starttime, start_station.station_name AS start_station, end_station.station_name AS end_station 
    FROM TripUsers t 
    JOIN Stations start_station ON t.start_station_id = start_station.station_id 
    JOIN Stations end_station ON t.end_station_id = end_station.station_id;
  ```

MongoDB:

  ```shell
    {
        'collection': 'TripUsers',
        'pipeline': [
            {
                '$lookup': {
                    'from': 'stations',
                    'localField': 'start_station_id',
                    'foreignField': 'station_id',
                    'as': 'start_station'
                }
            },
            {
                '$lookup': {
                    'from': 'stations',
                    'localField': 'end_station_id',
                    'foreignField': 'station_id',
                    'as': 'end_station'
                }
            },
            {
                '$project': {
                    'trip_id': 1,
                    'tripduration': 1,
                    'starttime': 1,
                    'start_station': {'$arrayElemAt': ['$start_station.station_name', 0]},
                    'end_station': {'$arrayElemAt': ['$end_station.station_name', 0]}
                }
            }
            
        ]
    },
  ```

4. Subqueries

 Retrieves all records for trips whose duration is greater than the average trip duration.

MariaDB:

  ```sql
    SELECT * FROM TripUsers WHERE tripduration > (SELECT AVG(tripduration) FROM TripUsers);
  ```

MongoDB:

  ```shell
    
  ```


Retrieves station details for stations that have been used as the end station in any trip.

MariaDB:

  ```sql
    SELECT * FROM Stations WHERE station_id IN (SELECT DISTINCT end_station_id FROM TripUsers);
  ```

MongoDB:

  ```shell
    {
        'collection': 'Stations',
        'pipeline': [
            {
                '$lookup': {
                    'from': 'TripUsers',
                    'pipeline': [
                        {
                            '$group': {
                                '_id': None,
                                'unique_end_stations': {'$addToSet': '$end_station_id'}
                            }
                        },
                        {
                            '$project': {
                                '_id': 0,
                                'unique_end_stations': 1
                            }
                        }
                    ],
                    'as': 'end_stations_info'
                }
            },
            {
                '$set': {
                    'end_stations_list': {
                        '$arrayElemAt': ['$end_stations_info.unique_end_stations', 0]
                    }
                }
            },
            {
                '$match': {
                    '$expr': {
                        '$in': ['$station_id', '$end_stations_list']
                    }
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'station_id': 1,
                    'station_name': 1
                }
            }
        ]
    }
  ```

Retrieves trips for users born before 1980, where the trip duration is longer than the average duration for this group.

MariaDB:

  ```sql
    SELECT * 
    FROM TripUsers
    WHERE birth_year < 1980 AND tripduration > (SELECT AVG(tripduration) FROM TripUsers WHERE birth_year < 1980);
  ```

MongoDB:

  ```shell
  {
        'collection': 'TripUsers',
        'pipeline': [
            {
                '$facet': {
                    'avgDurationYoung': [
                        {
                            '$match': {
                                'birth_year': {'$lt': 1980}
                            }
                        },
                        {
                            '$group': {
                                '_id': None,
                                'avg_tripduration': {'$avg': '$tripduration'}
                            }
                        }
                    ],
                    'allTripsYoung': [
                        {
                            '$match': {
                                'birth_year': {'$lt': 1980}
                            }
                        }
                    ]
                }
            },
            {
                '$unwind': '$avgDurationYoung'
            },
            {
                '$unwind': '$allTripsYoung'
            },
            {
                '$match': {
                    '$expr': {
                        '$gt': ['$allTripsYoung.tripduration', '$avgDurationYoung.avg_tripduration']
                    }
                }
            },
            {
                '$replaceRoot': { 'newRoot': '$allTripsYoung' }
            },
            {
                '$project': {
                    '_id': 0,
                    'trip_id': 1,
                    'tripduration': 1,
                    'birth_year': 1
                }
            }
        ]
    }
  ```

## Database Doctors_Appointments

1. Simple queries

Retrieves all records from the Doctors table where the doctor specializes in cardiology.

MariaDB:

  ```sql
    SELECT * FROM Doctors WHERE specialization = 'Cardiology';
  ```

MongoDB:

  ```shell
    {
        'collection': 'Doctors',
        'query': { "specialization": 'Cardiology' },
        'projection': None 
    }
  ```

Retrieves all records of patients who were born before January 1, 1980.

MariaDB:

  ```sql
    SELECT * FROM Patients WHERE birthdate < '1980-01-01';
  ```

MongoDB:

  ```shell
    {
        'collection': 'Patients',
        'query': { 'birthdate': {'$lt': '1980-01-01'} },
        'projection': None
    }
  ```

Retrieves all appointments from the Appointments table where the diagnosis is 'Hypertension'.

MariaDB:

  ```sql
    SELECT * FROM Appointments WHERE diagnosis = 'Hypertension';
  ```

MongoDB:

  ```shell
    {
        'collection': 'Appointments',
            'query': None,
        'pipeline': [
            {
                '$match': 
                {'diagnosis': 'Hypertension'}
            },
            {
                '$project': {'_id': 0}
            }
        ]
    }
  ```

2. Aggregation

Groups the records of patients by their birth year and counts the number of patients for each year.

MariaDB:

  ```sql
    SELECT YEAR(birthdate) AS birth_year, COUNT(*) AS patient_count FROM Patients GROUP BY birth_year;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Patients',
        'pipeline': [
            {
                '$addFields': {
                    'converted_date': {
                        '$cond': {
                            'if': {'$type': '$birthdate'}, 
                            'then': {
                                '$cond': {
                                    'if': {'$eq': [{'$type': '$birthdate'}, 'string']},
                                    'then': {'$dateFromString': {'dateString': '$birthdate'}},
                                    'else': '$birthdate'
                                }
                            },
                            'else': None
                        }
                    }
                }
            },
            {
                '$group': {
                    '_id': {'$year': '$converted_date'},
                    'patient_count': {'$sum': 1}
                }
            },
            {
                '$match': {
                    '_id': {'$ne': None}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'birth_year': '$_id',
                    'patient_count': 1
                }
            }
        ]
    }
  ```

Groups the records by patient ID and counts the number of distinct doctors that have treated each patient, returning only patients who have seen more than one doctor.

MariaDB:

  ```sql
    SELECT patient_id, COUNT(DISTINCT doctor_id) AS doctor_count FROM Appointments GROUP BY patient_id  HAVING doctor_count > 1;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Appointments',
        'pipeline': [
            {
                '$group': {
                    '_id': '$patient_id',
                    'unique_doctors': {'$addToSet': '$doctor_id'}
                }
            },
            {
                '$project': {
                    'patient_id': '$_id',
                    'doctor_count': {'$size': '$unique_doctors'}
                }
            },
            {
                '$match': {
                    'doctor_count': {'$gt': 1}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'patient_id': 1,
                    'doctor_count': 1
                }
            }
        ]
    }
  ```


Groups the records of appointments by diagnosis and counts the number of appointments for each diagnosis.

MariaDB:

  ```sql
    SELECT diagnosis, COUNT(*) AS diagnosis_count FROM Appointments GROUP BY diagnosis;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Appointments',
        'pipeline': [
            {
                '$group': {
                    '_id': '$diagnosis',
                    'diagnosis_count': {'$count': {}}
                }
            },
            {
                '$project': {
                    '_id': 0,
                    'diagnosis': '$_id',
                    'diagnosis_count': 1
                }
            }
        ]
    }
  ```

3. Join

Retrieves appointment details (including doctor and patient names, diagnosis, and treatment) for a specific doctor identified by doctor_id = 6970.

MariaDB:

  ```sql
    SELECT 
        a.appointment_id,
        CONCAT(d.first_name, ' ', d.last_name) AS doctor_name,
        CONCAT(p.first_name, ' ', p.last_name) AS patient_name,
        a.diagnosis,
        a.treatment
    FROM 
        Appointments a
    JOIN 
        Doctors d ON a.doctor_id = d.doctor_id
    JOIN 
        Patients p ON a.patient_id = p.patient_id
    WHERE 
        d.doctor_id = 6970;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Appointments',
        'pipeline': [
            {
                '$match': {
                    'doctor_id' : 6970
                }  
            },
            {
                '$lookup': {
                    'from': 'Doctors',
                    'localField': 'doctor_id',
                    'foreignField': 'doctor_id',
                    'as': 'doctor'
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
                '$unwind': '$doctor'
            },
            {
                '$unwind': '$patient'
            },
            {
                '$project': {
                    '_id': 0,
                    'appointment_id': 1,
                    'doctor_name': {
                        '$concat': [
                            '$doctor.first_name', 
                            ' ', 
                            '$doctor.last_name'
                        ]
                    },
                    'patient_name': {
                        '$concat': [
                            '$patient.first_name',
                            ' ',
                            '$patient.last_name'
                        ]
                    },
                    'diagnosis': 1,
                    'treatment': 1
                }
            }
        ]
    }
  ```

Retrieves details of the most recent 10 appointments, including doctor and patient names and the diagnosis.

MariaDB:

  ```sql
    SELECT 
        a.appointment_date,
        d.first_name as doctor_first_name,
        d.last_name as doctor_last_name,
        p.first_name as patient_first_name,
        p.last_name as patient_last_name,
        a.diagnosis
    FROM Appointments a
    JOIN Doctors d ON a.doctor_id = d.doctor_id
    JOIN Patients p ON a.patient_id = p.patient_id
    LIMIT 10;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Appointments',
        'pipeline': [
            {
                '$lookup': {
                    'from': 'Doctors',
                    'localField': 'doctor_id',
                    'foreignField': 'doctor_id',
                    'as': 'doctor'
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
                '$unwind': '$doctor'
            },
            {
                '$unwind': '$patient'
            },
            {
                '$project': {
                    'appointment_date': 1,
                    'doctor_first_name': '$doctor.first_name',
                    'doctor_last_name': '$doctor.last_name',
                    'patient_first_name': '$patient.first_name',
                    'patient_last_name': '$patient.last_name',
                    'diagnosis': 1
                }
            },
            {
                '$limit': 10
            }
        ]
    }
  ```

Retrieves the details of patients who have had more than 7 appointments, showing the patient's name and total number of appointments.

MariaDB:

  ```sql
    SELECT 
        p.first_name,
        p.last_name,
        COUNT(*) as total_appointments
    FROM Appointments a
    JOIN Patients p ON a.patient_id = p.patient_id
    GROUP BY a.patient_id, p.first_name, p.last_name
    HAVING COUNT(*) > 7
    LIMIT 10;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Appointments',
        'pipeline': [
            {
                '$group': {
                    '_id': '$patient_id',
                    'total_appointments': {'$sum': 1}
                }
            },
            {
                '$match': {'total_appointments': {'$gt': 5}}
            },
            {
                '$lookup': {
                    'from': 'Patients',
                    'localField': '_id',
                    'foreignField': 'patient_id',
                    'as': 'patient_info'
                }
            },
            {
                '$unwind': '$patient_info'
            },
            {
                '$project': {
                    'first_name': '$patient_info.first_name',
                    'last_name': '$patient_info.last_name',
                    'total_appointments': 1
                }
            },
            {
                '$limit': 10
            }
        ]
    }
  ```

4. Subqueries

Retrieves the details of patients who have had more than 7 appointments, showing the patient's name and total number of appointments.

MariaDB:

  ```sql
    WITH DoctorWithMostPatients AS (
        SELECT 
            doctor_id,
            COUNT(*) as patient_count
        FROM Appointments
        GROUP BY doctor_id
        ORDER BY COUNT(*) DESC
        LIMIT 1
    )
    SELECT DISTINCT
        p.first_name,
        p.last_name
    FROM Appointments a
    JOIN Patients p ON a.patient_id = p.patient_id
    JOIN DoctorWithMostPatients d ON a.doctor_id = d.doctor_id
    LIMIT 10;
  ```

MongoDB:

  ```shell
    {
        'collection': 'Appointments',
        'pipeline': [
            {
                '$group': {
                    '_id': '$doctor_id',
                    'patient_count': {'$sum': 1}
                }
            },
            {
                '$sort': {'patient_count': -1}
            },
            {
                '$limit': 1
            },
            {
                '$lookup': {
                    'from': 'Appointments',
                    'let': {'doctor_id': '$_id'},
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {'$eq': ['$doctor_id', '$$doctor_id']}
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
                                    'patient_id': '$patient_id',
                                    'first_name': '$patient.first_name',
                                    'last_name': '$patient.last_name'
                                }
                            }
                        },
                        {
                            '$project': {
                                'first_name': '$_id.first_name',
                                'last_name': '$_id.last_name',
                                '_id': 0
                            }
                        },
                        {
                            '$limit': 10
                        }
                    ],
                    'as': 'patients'
                }
            },
            {
                '$unwind': '$patients'
            },
            {
                '$replaceRoot': {'newRoot': '$patients'}
            },
            {
                '$limit': 10
            }
        ]
    }
  ```

Retrieves the names of patients who have had at least 2 appointments for the same diagnosis.

MariaDB:

  ```sql
    SELECT first_name, last_name
    FROM Patients
    WHERE patient_id IN (
        SELECT patient_id
        FROM Appointments
        GROUP BY patient_id, diagnosis
        HAVING COUNT(appointment_id) >= 2
    );
  ```

MongoDB:

  ```shell
    {
        'collection': 'Appointments',
        'pipeline': [
            {
                '$group': {
                    '_id': {
                        'patient_id': '$patient_id',
                        'diagnosis': '$diagnosis'
                    },
                    'count': {'$sum': 1}
                }
            },
            {
                '$match': {
                    'count': {'$gte': 2}
                }
            },
            {
                '$lookup': {
                    'from': 'Patients',
                    'localField': '_id.patient_id',
                    'foreignField': 'patient_id',
                    'as': 'patient_info'
                }
            },
            {
                '$unwind': '$patient_info'
            },
            {
                '$project': {
                    '_id': 0,
                    'first_name': '$patient_info.first_name',
                    'last_name': '$patient_info.last_name'
                }
            },
            {
                '$limit': 10
            }
        ]
    }
  ```

Retrieves the names of patients who have been treated by cardiologists, limiting the results to the first 10 patients

MariaDB:

  ```sql
    SELECT DISTINCT 
        first_name, 
        last_name
    FROM Patients
    WHERE patient_id IN (
        SELECT a.patient_id
        FROM Appointments a
        JOIN Doctors d ON a.doctor_id = d.doctor_id
        WHERE d.specialization = 'Cardiology'
    )
    LIMIT 10;
  ```
