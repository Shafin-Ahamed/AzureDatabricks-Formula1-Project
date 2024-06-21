-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
SELECT driver_name,
       count(*) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points,
       RANK() OVER (ORDER BY avg(calculated_points) DESC) driver_rank
 FROM calculated_race_results
 GROUP BY driver_name
 HAVING count(*) >= 50
 ORDER BY avg_points desc

-- COMMAND ----------

-- Use previous statement as a temporary view to call on in the WHERE condition below

SELECT race_year,
       driver_name,
       count(*) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points
 FROM calculated_race_results
 WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <=10)
 GROUP BY driver_name, race_year
 ORDER BY race_year, avg_points desc

-- COMMAND ----------

SELECT race_year,
       driver_name,
       count(*) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points
 FROM calculated_race_results
 WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <=10)
 GROUP BY driver_name, race_year
 ORDER BY race_year, avg_points desc

-- COMMAND ----------

SELECT race_year,
       driver_name,
       count(*) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points
 FROM calculated_race_results
 WHERE driver_name IN (SELECT driver_name FROM v_dominant_drivers WHERE driver_rank <=10)
 GROUP BY driver_name, race_year
 ORDER BY race_year, avg_points desc

-- COMMAND ----------


