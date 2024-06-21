-- Databricks notebook source
use f1_processed

-- COMMAND ----------

show tables

-- COMMAND ----------



-- COMMAND ----------

--Use CTAS to create data from processed db (USE function) and save in table in presentation layer
CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT races.race_year, constructors.name AS team_name, drivers.name AS driver_name, results.position, results.points, 11-results.position AS calculated_points
FROM results
JOIN drivers ON (results.driver_id = drivers.driver_id)
JOIN constructors ON (results.constructor_id = constructors.constructor_id)
JOIN races on (results.race_id = races.race_id)
WHERE results.position <= 10

-- COMMAND ----------

use f1_presentation

-- COMMAND ----------

SELECT * FROM calculated_race_results

-- COMMAND ----------


