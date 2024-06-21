-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name,
       count(*) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points
 FROM calculated_race_results
 GROUP BY team_name
 HAVING count(*) >= 100
 ORDER BY avg_points desc

-- COMMAND ----------


