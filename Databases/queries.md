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