-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

SELECT *
FROM calculated_race_results

-- COMMAND ----------

SELECT driver_name, race_year, sum(calculated_points) as total_calculated_points, rank() OVER (PARTITION BY race_year order by sum(calculated_points) DESC) as points_rank
 FROM calculated_race_results
 GROUP BY driver_name, race_year
 SORT BY race_year desc

-- COMMAND ----------

SELECT driver_name,
       count(*) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points
 FROM calculated_race_results
 WHERE race_year BETWEEN 2001 AND 2010
 GROUP BY driver_name
 HAVING count(*) >= 50
 ORDER BY avg_points desc

-- COMMAND ----------


