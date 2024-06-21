-- Databricks notebook source
use f1_raw;
show tables

-- COMMAND ----------

-- Managed table creation. Any table created in Hive Metastore database f1_processed will also be created on the processed ADLS mount

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/f1datalakepractice/processed"

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use f1_processed;
show tables

-- COMMAND ----------


