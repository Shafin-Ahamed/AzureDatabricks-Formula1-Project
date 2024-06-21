-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
SELECT team_name,
       count(*) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points,
       RANK() OVER(ORDER BY avg(calculated_points) desc) team_rank
 FROM calculated_race_results
 GROUP BY team_name
 HAVING count(*) >= 100
 ORDER BY avg_points desc

-- COMMAND ----------

SELECT * FROM v_dominant_teams

-- COMMAND ----------

SELECT race_year,
       team_name,
       count(*) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points
 FROM calculated_race_results
 WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <=10)
 GROUP BY team_name, race_year
 ORDER BY race_year, avg_points desc

-- COMMAND ----------

SELECT race_year,
       team_name,
       count(*) as total_races,
       sum(calculated_points) as total_points,
       avg(calculated_points) as avg_points
 FROM calculated_race_results
 WHERE team_name IN (SELECT team_name FROM v_dominant_teams WHERE team_rank <=10)
 GROUP BY team_name, race_year
 ORDER BY race_year, avg_points desc

-- COMMAND ----------


