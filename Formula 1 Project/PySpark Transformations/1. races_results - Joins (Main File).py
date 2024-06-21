# Databricks notebook source
# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_ciruits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "left").select(races_df.race_year, races_df.name.alias("race_name"), races_df.date.alias("race_date"), circuits_df.location.alias("circuit_location"), races_df.race_id)

# COMMAND ----------

display(races_ciruits_df)

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

results_df = spark.read.parquet(f"{processed_folder_path}/results")

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_df.count()

# COMMAND ----------

results_drivers_df = results_df.join(drivers_df, results_df.driver_id == drivers_df.driver_id, "left") \
                               .select(drivers_df.name.alias("driver_name"), drivers_df.number.alias("driver_number"), drivers_df.nationality.alias("driver_nationality"),results_df.grid.alias("results_grid"), results_df.fastest_lap.alias("results_fastest_lap"), results_df.time.alias("results_race_time"), results_df.points.alias("results_points"), results_df.constructor_id.alias("results_constructor_id"), results_df.race_id.alias("results_race_id"), results_df.position.alias("results_position"))

# COMMAND ----------

display(results_drivers_df)

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

results_drivers_constructors_df = results_drivers_df.join(constructors_df, results_drivers_df.results_constructor_id == constructors_df.constructor_id, "left").drop(constructors_df.constructor_id, constructors_df.constructor_ref, constructors_df.nationality, constructors_df.ingestion_date)

# COMMAND ----------

results_drivers_constructors_df_2 = results_drivers_constructors_df.withColumnRenamed("name", "constructors_team")

# COMMAND ----------

display(results_drivers_constructors_df_2)

# COMMAND ----------

results_drivers_constructors_df_2.count()

# COMMAND ----------

semi_final_df = results_drivers_constructors_df_2.join(races_ciruits_df, results_drivers_constructors_df_2.results_race_id == races_ciruits_df.race_id, "left")

# COMMAND ----------

display(semi_final_df)

# COMMAND ----------

dropped_semi_df = semi_final_df.drop(semi_final_df.results_constructor_id, semi_final_df.results_race_id, semi_final_df.race_id)

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

final_df = add_ingestion(dropped_semi_df)

# COMMAND ----------

display(final_df)

# COMMAND ----------

super_final_df = final_df.withColumnRenamed("ingestion_date", "created_date") \
                         .select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "constructors_team", "results_grid", "results_fastest_lap", "results_race_time", "results_points", "results_position", "created_date")

# COMMAND ----------

display(super_final_df.where("race_year = 2020 AND race_name = 'Abu Dhabi Grand Prix'").orderBy(super_final_df.results_points.desc()))

# COMMAND ----------

super_final_df.count()

# COMMAND ----------

display(super_final_df)

# COMMAND ----------

super_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

races_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(races_results_df)

# COMMAND ----------

races_results_df.count()

# COMMAND ----------

display(races_results_df.where("race_year = 2020 AND race_name = 'Abu Dhabi Grand Prix'").orderBy(races_results_df.results_points.desc()))

# COMMAND ----------


