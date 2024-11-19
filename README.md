# Database engine comperasion

**MongoDB** and **MariaDB** are compared performance vise via pyhon script.

Large datasets were used to determine output at a scale.

Both engines had optimisations made to even the odds:

* MongoDB - query optimisation
* Maria - indexes in tables

Similar queries of different types where made to check perfomance of both engines.

## Test description

Comperasion between relational and document database engines.
The test had to check performance of the engiens based on same datasets and similar queries.

Script moniors:

* time - how long it took to finish query
* memory %
* CPU %
* disk load %

### Queries

Queries examples are present inside `/Databases` directory.

## Infrastructure

We made an VM on AWS using Ubuntu 24.04 server.

VM has public IP, so public access is enabled.

Configuration is made via bootstrap (check `/Boostrap`) script, then datasets are fed to VM via scp.

Next the datasets are imported to DBs.

Testing script wrapper is used to run and output result into `csv` and then `xlsx` file.

## Datasets

This Project was testing three different datasets.

* Doctors_Appointments - Generated from `/Generator`
* [Airline Flight Delays](https://mavenanalytics.io/data-playground?order=date_added%2Cdesc&search=airline%20flight)
Records for 5,000,000+ commercial airline flights in 2015, compiled for the U.S. DOT Air Travel Consumer Report.
* [Bike trips](https://s3.amazonaws.com/tripdata/index.html) data from Amazon

### Database structures

MariaDB structures are present inside `/Databases` folder.
MongoDB is structureless, so there was no need for that.

## How-to

Most important know-hows are present inside `/How-to` folder
