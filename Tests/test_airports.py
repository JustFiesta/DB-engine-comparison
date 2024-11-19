"""
chuchujariport
"""

from testing_functions import test_database_performance

def main():
    db_name = "Airports"
    queries = {
        'MariaDB': [
            # zapytania
            "SELECT * FROM Flights WHERE ARRIVAL_DELAY > 60;",
            "SELECT AIRLINE FROM Airlines WHERE IATA_CODE = 'AA';",
            "SELECT AIRPORT, CITY FROM Airports WHERE STATE = 'CA';",
            # grupowanie
            "SELECT DAY_OF_WEEK, AVG(ARRIVAL_DELAY) AS avg_arrival_delay FROM Flights GROUP BY DAY_OF_WEEK;",
            "SELECT CANCELLATION_REASON, COUNT(*) AS cancel_count FROM Flights WHERE CANCELLED = 1 GROUP BY CANCELLATION_REASON;", 
            "SELECT YEAR, MONTH, DAY, COUNT(*) AS flight_count FROM Flights WHERE AIRLINE = 'UA' GROUP BY YEAR, MONTH, DAY;",
            # joiny
            "SELECT a.AIRLINE, COUNT(f.FLIGHT_NUMBER) AS flight_count FROM Flights f JOIN Airlines a ON f.AIRLINE = a.IATA_CODE GROUP BY a.AIRLINE;",
            """SELECT 
                a.AIRLINE as airline_name,
                orig.AIRPORT as origin_airport,
                dest.AIRPORT as destination_airport,
                f.ARRIVAL_DELAY
            FROM Flights f
            JOIN Airlines a ON f.AIRLINE = a.IATA_CODE
            JOIN Airports orig ON f.ORIGIN_AIRPORT = orig.IATA_CODE
            JOIN Airports dest ON f.DESTINATION_AIRPORT = dest.IATA_CODE
            WHERE f.ARRIVAL_DELAY > 100
            ORDER BY f.ARRIVAL_DELAY DESC;
            """,
            "SELECT ap.AIRPORT AS destination_airport, a.AIRLINE AS airline_name FROM Flights f JOIN Airlines a ON f.AIRLINE = a.IATA_CODE JOIN Airports ap ON f.DESTINATION_AIRPORT = ap.IATA_CODE WHERE f.ARRIVAL_DELAY > 120;",
            # podzapytania
            """SELECT 
            f.AIRLINE,
            a.AIRLINE as AIRLINE_NAME,
            ROUND(AVG(f.ARRIVAL_DELAY), 2) as AVG_DELAY
            FROM Flights f
            JOIN Airlines a ON f.AIRLINE = a.IATA_CODE
            GROUP BY f.AIRLINE
            HAVING AVG_DELAY > (
                SELECT AVG(ARRIVAL_DELAY) 
                FROM Flights
            )
            ORDER BY AVG_DELAY DESC;
            """,
            """SELECT 
            DISTINCT a.AIRPORT,
            a.CITY,
            a.STATE
            FROM Airports a
            JOIN Flights f ON a.IATA_CODE = f.ORIGIN_AIRPORT
            WHERE f.DISTANCE > (
                SELECT AVG(DISTANCE) 
                FROM Flights
            )
            ORDER BY a.STATE, a.CITY; """,
            """SELECT 
            f.MONTH,
            a.AIRLINE as AIRLINE_NAME,
            COUNT(*) as DELAYED_FLIGHTS
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
            ORDER BY f.MONTH; """
        ],
        'MongoDB': [
                # zapytania
                {
                    'collection': 'Flights',
                    'query': {"ARRIVAL_DELAY": {"$gt": 60}},
                    'projection': None
                },
                {
                    'collection': 'Airlines',
                    'query': {"IATA_CODE": "AA"},
                    'projection': None
                },
                { 
                    'collection': 'Airports',
                    'query': { "STATE": "CA" },
                    'projection': None  
                },
                # grupowanie
                {
                    'collection': 'Flights',
                    'query': None,
                    'pipeline': [
                        {
                            "$group": {
                                "_id": "$DAY_OF_WEEK",  
                                "avg_arrival_delay": { "$avg": "$ARRIVAL_DELAY" }
                            }
                        }
                    ],
                    'projection': None
                },
                {
                    'collection': 'Flights',
                    'query': None,
                    'pipeline': [
                        {
                            "$match": {
                                "CANCELLED": 1  
                            }
                        },
                        {
                            "$group": {
                                "_id": "$CANCELLATION_REASON", 
                                "cancel_count": { "$sum": 1 }  
                            }
                        }
                    ],
                    'projection': None
                },
                {
                    'collection': 'Flights',
                    'query': None,
                    'pipeline': [
                        {
                            "$match": {
                                "AIRLINE": "UA"  
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
                    ],
                    'projection': None
                },
                # joiny
                {
                    'collection': 'Flights',
                    'query': None,
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
                                "as": "airline_info"
                            }
                        },
                        {
                            "$unwind": {
                                "path": "$airline_info",
                                "preserveNullAndEmptyArrays": True
                            }
                        },
                        {
                            "$project": {
                                "airline": "$airline_info.AIRLINE",
                                "flight_count": 1,
                                "_id": 0
                            }
                        }
                    ],
                    'projection': None
                },
                {
                    'collection': 'Flights',
                    'query': None,
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
                                "as": "airline_info"
                            }
                        },
                        {
                            "$unwind": "$airline_info"
                        },
                        {
                            "$lookup": {
                                "from": "Airports",
                                "localField": "ORIGIN_AIRPORT",
                                "foreignField": "IATA_CODE",
                                "as": "origin_airport"
                            }
                        },
                        {
                            "$unwind": "$origin_airport"
                        },
                        {
                            "$lookup": {
                                "from": "Airports",
                                "localField": "DESTINATION_AIRPORT",
                                "foreignField": "IATA_CODE",
                                "as": "destination_airport"
                            }
                        },
                        {
                            "$unwind": "$destination_airport"
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
                    ],
                    'projection': None
                },
                {
                    'collection': 'Flights',
                    'query': None,
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
                                "as": "airline_info"
                            }
                        },
                        {
                            "$unwind": "$airline_info"
                        },
                        {
                            "$lookup": {
                                "from": "Airports",
                                "localField": "ORIGIN_AIRPORT",
                                "foreignField": "IATA_CODE",
                                "as": "origin_airport"
                            }
                        },
                        {
                            "$unwind": "$origin_airport"
                        },
                        {
                            "$lookup": {
                                "from": "Airports",
                                "localField": "DESTINATION_AIRPORT",
                                "foreignField": "IATA_CODE",
                                "as": "destination_airport"
                            }
                        },
                        {
                            "$unwind": "$destination_airport"
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
                    ],
                    'projection': None
                },
                # podzapytania
                {
                    'collection': 'Flights',
                    'query': None,
                    'pipeline': [
                        {
                            "$group": {
                                "_id": "$DAY_OF_WEEK",  
                                "avg_arrival_delay": { "$avg": "$ARRIVAL_DELAY" }
                            }
                        }
                    ]
                },
                {
                    'collection': 'Flights',
                    'query': None,
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
                    'projection': None
                },
                {
                    'collection': 'Flights',
                    'query': None,
                    'pipeline': [
                        {
                            "$match": {
                                "ARRIVAL_DELAY": {"$gt": 0}  
                            }
                        },
                        {
                            "$group": {
                                "_id": {
                                    "month": "$MONTH",
                                    "airline": "$AIRLINE"
                                },
                                "delayed_flights": {"$sum": 1}
                            }
                        },
                        {
                            "$group": {
                                "_id": "$_id.month",
                                "maxDelays": {"$max": "$delayed_flights"},
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
                                        "cond": {"$eq": ["$$item.delayed_flights", "$maxDelays"]}
                                    }
                                }
                            }
                        },
                        {"$unwind": "$airline_data"},
                        {
                            "$lookup": {
                                "from": "Airlines",
                                "localField": "airline_data.airline",
                                "foreignField": "IATA_CODE",
                                "as": "airline_info"
                            }
                        },
                        {"$unwind": "$airline_info"},
                        {
                            "$project": {
                                "_id": 0,
                                "month": "$_id",
                                "airline_name": "$airline_info.AIRLINE",
                                "delayed_flights": "$airline_data.delayed_flights"
                            }
                        },
                        {"$sort": {"month": 1}}
                    ],
                    'projection': None
                },
            ]
    }

    test_database_performance(queries, db_name)

if __name__ == "__main__":
    main()
