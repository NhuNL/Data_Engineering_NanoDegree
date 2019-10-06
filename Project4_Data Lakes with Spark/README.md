# Project 4: Data Lake and Spark
## Build an ETL pipeline for a data lake hosted on S3 using Apache Spark

## Summary
* [Concept](#Concept)
* [Spark process](#Spark-process)
* [Project structure](#Project-structure)

--------------------------------------------

#### Concept
In this project we will use Amazon Web Services, S3 bucket where the data source is provided. The bucket contains all JSON files which has information about song, artists, and actions of users, for example what time/which songs are users listening to etc. Using Apache Spark to process data and load them back into S3. All is deployed on AWS cluster.


#### Spark process
Spark process or ETL job will process the song files then the log files. 
The song files are listed and iterated over entering relevant information in the artists and the song folders in parquet. 
The log files are filtered by the NextSong action. The subsequent dataset is then processed to extract the date , time , year etc. fields and records are then appropriately entered into the time, users and songplays folders in parquet for analysis.


--------------------------------------------

#### Project structure

/data - A folder that cointains two zip files. All files is in JSON format.
etl.py - The ETL engine done with Spark, data normalization and parquet file writing.
dl.cfg - Configuration file that contains info about AWS credentials

----------------------------
