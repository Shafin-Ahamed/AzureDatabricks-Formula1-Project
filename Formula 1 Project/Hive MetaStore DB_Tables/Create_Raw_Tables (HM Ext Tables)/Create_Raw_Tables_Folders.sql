-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lap times folder

-- COMMAND ----------

use f1_raw;
SHOW TABLES

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE f1_raw.lap_times
(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
USING CSV 
OPTIONS (path "/mnt/f1datalakepractice/raw/lap_times")

-- COMMAND ----------

SELECT COUNT(*) FROM f1_raw.lap_times

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Qualifying Folder (MULTILINE)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE f1_raw.qualifying
(
qualifyId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
position INT,
q1 STRING,
q2 STRING,
q3 STRING
)
USING JSON
OPTIONS (path "/mnt/f1datalakepractice/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying

-- COMMAND ----------

DESCRIBE EXTENDED f1_raw.qualifying

-- COMMAND ----------


