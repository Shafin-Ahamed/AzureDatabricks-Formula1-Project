-- Databricks notebook source
-- MAGIC %md 
-- MAGIC #### Constructors table 

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use f1_raw;
show tables

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE f1_raw.constructors
(
  constructorId INT, 
  constructorRef STRING, 
  name STRING, 
  nationality STRING, 
  url STRING
)
USING JSON
OPTIONS (path "/mnt/f1datalakepractice/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Drivers File (NESTED JSON SINGLE LINE)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE f1_raw.drivers
(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename: STRING, surname: STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (path "/mnt/f1datalakepractice/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Results file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE f1_raw.results
(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT,
  grid INT,
  position INT,
  positionText STRING,
  positionOrder INT,
  points INT,
  laps INT,
  time STRING,
  milliseconds INT,
  fastestLap INT,
  rank INT,
  fastestLapTime STRING,
  fastestLapSpeed FLOAT,
  statusId STRING
)
USING JSON
OPTIONS (path "/mnt/f1datalakepractice/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Pit Stops File (MULTILINE JSON)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE f1_raw.pit_stops
(
raceId INT,
driverId INT,
stop INT,
lap INT,
time STRING,
duration STRING,
milliseconds INT
)
USING JSON
OPTIONS (path "/mnt/f1datalakepractice/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops

-- COMMAND ----------


