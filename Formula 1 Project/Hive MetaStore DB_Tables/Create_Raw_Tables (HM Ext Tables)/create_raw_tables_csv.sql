-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create circuits table

-- COMMAND ----------

-- Creating raw tables in Hive MetaStore DB in Databricks using file path from Data Lake Mounts

DROP TABLE IF EXISTS f1_raw.circuits
CREATE TABLE IF NOT EXISTS f1_raw.circuits
(
circuitId INT,
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/f1datalakepractice/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

use f1_raw;
show tables

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races
(
  raceId INT,
  year INT,
  round INT,
  circuitId INT,
  Name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS (path "/mnt/f1datalakepractice/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------


