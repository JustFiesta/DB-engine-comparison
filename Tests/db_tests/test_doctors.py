"""
Moduł testujący dla bazy Doctors_Appointments
"""

from testing_functions import test_database_performance

def main():
    db_name = "Doctors_Appointments"
    queries = {
        'MariaDB': [
            # zapytania
            "SELECT * FROM Doctors WHERE specialization = 'Cardiology';",
            "SELECT * FROM Patients WHERE birthdate < '1980-01-01';",
            "SELECT * FROM Appointments WHERE diagnosis = 'Hypertension';",
            # grupowanie
            "SELECT YEAR(birthdate) AS birth_year, COUNT(*) AS patient_count FROM Patients GROUP BY birth_year;",
            "SELECT patient_id, COUNT(DISTINCT doctor_id) AS doctor_count FROM Appointments GROUP BY patient_id HAVING doctor_count > 1;", 
            "SELECT diagnosis, COUNT(*) AS diagnosis_count FROM Appointments GROUP BY diagnosis;",
            # joiny
            """SELECT 
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
            """,
            """SELECT 
                a.appointment_date,
                d.first_name as doctor_first_name,
                d.last_name as doctor_last_name,
                p.first_name as patient_first_name,
                p.last_name as patient_last_name,
                a.diagnosis
            FROM Appointments a
            JOIN Doctors d ON a.doctor_id = d.doctor_id
            JOIN Patients p ON a.patient_id = p.patient_id
            LIMIT 10;""",
            """SELECT 
                p.first_name,
                p.last_name,
                COUNT(*) as total_appointments
            FROM Appointments a
            JOIN Patients p ON a.patient_id = p.patient_id
            GROUP BY a.patient_id, p.first_name, p.last_name
            HAVING COUNT(*) > 7
            LIMIT 10;""",
            # podzapytania
            """WITH DoctorWithMostPatients AS (
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
            """,
            """SELECT first_name, last_name
            FROM Patients
            WHERE patient_id IN (
                SELECT patient_id
                FROM Appointments
                GROUP BY patient_id, diagnosis
                HAVING COUNT(appointment_id) >= 2
            );
            """,
            """SELECT DISTINCT 
                first_name, 
                last_name
            FROM Patients
            WHERE patient_id IN (
                SELECT a.patient_id
                FROM Appointments a
                JOIN Doctors d ON a.doctor_id = d.doctor_id
                WHERE d.specialization = 'Cardiology'
            )
            LIMIT 10;"""
        ],
        'MongoDB': [
            # zapytania
            {
                'collection': 'Doctors',
                'query': { "specialization": 'Cardiology' },
                'projection': None 
            },
            {
                'collection': 'Patients',
                'query': { 'birthdate': {'$lt': '1980-01-01'} },
                'projection': None
            },
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
            },
            # grupowanie
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
            },
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
            },
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
            },
            # joiny
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
            },
            {
                'collection': 'Appointments',
                'pipeline': [
                    {'$lookup': {
                        'from': 'Doctors',
                        'localField': 'doctor_id',
                        'foreignField': 'doctor_id',
                        'as': 'doctor'
                    }},
                    {'$lookup': {
                        'from': 'Patients',
                        'localField': 'patient_id',
                        'foreignField': 'patient_id',
                        'as': 'patient'
                    }},
                    {'$unwind': '$doctor'},
                    {'$unwind': '$patient'},
                    {'$project': {
                        'appointment_date': 1,
                        'doctor_first_name': '$doctor.first_name',
                        'doctor_last_name': '$doctor.last_name',
                        'patient_first_name': '$patient.first_name',
                        'patient_last_name': '$patient.last_name',
                        'diagnosis': 1
                    }},
                    {'$limit': 10}
                ]
            },
            {
                'collection': 'Appointments',
                'pipeline': [
                    {'$group': {
                        '_id': '$patient_id',
                        'total_appointments': {'$sum': 1}
                    }},
                    {'$match': {'total_appointments': {'$gt': 5}}},
                    {'$lookup': {
                        'from': 'Patients',
                        'localField': '_id',
                        'foreignField': 'patient_id',
                        'as': 'patient_info'
                    }},
                    {'$unwind': '$patient_info'},
                    {'$project': {
                        'first_name': '$patient_info.first_name',
                        'last_name': '$patient_info.last_name',
                        'total_appointments': 1
                    }},
                    {'$limit': 10}
                ]
            },
            # podzapytania
            {
                'collection': 'Appointments',
                'pipeline': [
                    {'$group': {
                        '_id': '$doctor_id',
                        'patient_count': {'$sum': 1}
                    }},
                    {'$sort': {'patient_count': -1}},
                    {'$limit': 1},
                    {'$lookup': {
                        'from': 'Appointments',
                        'let': {'doctor_id': '$_id'},
                        'pipeline': [
                            {'$match': {
                                '$expr': {'$eq': ['$doctor_id', '$$doctor_id']}
                            }},
                            {'$lookup': {
                                'from': 'Patients',
                                'localField': 'patient_id',
                                'foreignField': 'patient_id',
                                'as': 'patient'
                            }},
                            {'$unwind': '$patient'},
                            {'$group': {
                                '_id': {
                                    'patient_id': '$patient_id',
                                    'first_name': '$patient.first_name',
                                    'last_name': '$patient.last_name'
                                }
                            }},
                            {'$project': {
                                'first_name': '$_id.first_name',
                                'last_name': '$_id.last_name',
                                '_id': 0
                            }},
                            {'$limit': 10}
                        ],
                        'as': 'patients'
                    }},
                    {'$unwind': '$patients'},
                    {'$replaceRoot': {'newRoot': '$patients'}
                                         },
                    {'$limit': 10}
                ]
            },
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
                    }
                ]
            },
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
                        '$match': {
                            'doctor.specialization': 'Cardiology' 
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
                        '$project': {
                            'patient': { '$arrayElemAt': ['$patient', 0] },  
                            '_id': 0  
                        }
                    },
                    {
                        '$project': {
                            'first_name': '$patient.first_name',
                            'last_name': '$patient.last_name'
                        }
                    },
                    {
                        '$limit': 10  
                    }
                ]
            },
        ]
    }

    test_database_performance(queries, db_name)

if __name__ == "__main__":
    main()
