"""
Moduł testujący dla bazy Bikes
"""

from Tests.db_tests.testing_functions import test_database_performance

def main():
    db_name = "Bikes"
    queries = {
        'MariaDB': [
            # zapytania
            "SELECT * FROM TripUsers WHERE tripduration > 30 * 60;",
            "SELECT * FROM TripUsers WHERE end_station_name = 'Newport Pkwy';",
            "SELECT DISTINCT usertype FROM TripUsers;",
            # grupowanie
            "SELECT usertype, COUNT(*) AS trip_count FROM TripUsers GROUP BY usertype;",
            "SELECT birth_year, SUM(tripduration) AS total_tripduration FROM TripUsers GROUP BY birth_year;", 
            "SELECT gender, AVG(tripduration) AS average_tripduration FROM TripUsers GROUP BY gender;",
            # joiny
            """SELECT 
            t.trip_id, t.tripduration, s.station_name AS start_stat_name, e.station_name AS end_stat_name 
            FROM TripUsers t JOIN Stations s ON t.start_station_id = s.station_id 
            JOIN Stations e ON t.end_station_id = e.station_id 
            WHERE t.tripduration > 20;""",
            """SELECT 
            t.trip_id, t.tripduration, start_stations.station_name AS start_station_name, end_stations.station_name AS end_station_name 
            FROM TripUsers t JOIN  Stations AS start_stations ON t.start_station_id = start_stations.station_id 
            JOIN Stations AS end_stations ON t.end_station_id = end_stations.station_id 
            WHERE t.tripduration > 20;""",
            """SELECT 
            t.trip_id, t.tripduration, t.starttime, start_station.station_name AS start_station, end_station.station_name AS end_station 
            FROM TripUsers t JOIN Stations start_station ON t.start_station_id = start_station.station_id 
            JOIN Stations end_station ON t.end_station_id = end_station.station_id;""",
            # podzapytania
            "SELECT * FROM TripUsers WHERE tripduration > (SELECT AVG(tripduration) FROM TripUsers);",
            "SELECT * FROM Stations WHERE station_id IN (SELECT DISTINCT end_station_id FROM TripUsers);",
            """SELECT * 
            FROM TripUsers
            WHERE birth_year < 1980 AND tripduration > (SELECT AVG(tripduration) FROM TripUsers WHERE birth_year < 1980);"""
        ],
        'MongoDB': [
            # zapytania
            {
                'collection': 'TripUsers',
                'query': {"tripduration": { "$gt": 1800 } },
                'projection': None 
            },
            {
                'collection': 'TripUsers',
                'query': {"end_station_name": "Newport Pkwy" },
                'projection': None
            },
            { 
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    {
                        "$group": {"_id": "$usertype"}
                    },  
                    {
                        "$project": {"usertype": "$_id", "_id": 0}
                    }  
                ],
                'projection': None
            },
            # grupowanie
            {
                'collection': 'TripUsers',
                'query': None,
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
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
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
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
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
                ],
                'projection': None
            },
            # joiny
            {
                'collection': 'TripUsers',
                'query': None,
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
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
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
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
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
                    
                ],
                'projection': None
            },
            # podzapytania
            {
                'collection': 'TripUsers',
                'query': None,
                'pipeline': [
                    {
                        '$facet': {
                            'avgDuration': [
                                {
                                    '$group': {
                                        '_id': None,
                                        'avg_tripduration': {'$avg': '$tripduration'}
                                    }
                                }
                            ],
                            'allTrips': [
                                {
                                    '$match': {}  
                                }
                            ]
                        }
                    },
                    {
                        '$unwind': '$avgDuration'
                    },
                    {
                        '$unwind': '$allTrips'
                    },
                    {
                        '$match': {
                            '$expr': {
                                '$gt': ['$allTrips.tripduration', '$avgDuration.avg_tripduration']
                            }
                        }
                    },
                    {
                        '$replaceRoot': { 'newRoot': '$allTrips' }
                    },
                    {
                        '$project': {
                            '_id': 0,
                            'trip_id': 1,
                            'tripduration': 1,
                            'starttime': 1
                        }
                    }
                ],
                'projection': None
            },
            {
                'collection': 'Stations',  
                'query': None,
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
                                }
                            ],
                            'as': 'end_stations_info'
                        }
                    },
                    {
                        '$unwind': '$end_stations_info'
                    },
                    {
                        '$match': {
                            '$expr': {
                                '$in': ['$station_id', '$end_stations_info.unique_end_stations']
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
                ],
                'projection': None
            },
            {
                'collection': 'TripUsers',
                'query': None,
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
                ],
                'projection': None
            },
        ]
    }

    test_database_performance(queries, db_name)

if __name__ == "__main__":
    main()
