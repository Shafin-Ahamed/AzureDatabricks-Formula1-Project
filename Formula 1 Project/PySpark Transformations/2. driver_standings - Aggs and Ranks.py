# Databricks notebook source
# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, count, col, when

driver_standings_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality", "constructors_team") \
.agg(sum("results_points").alias("total_points"), \
     count(when(col("results_position") == 1, True)).alias("total_wins"))

# COMMAND ----------

display(driver_standings_df.where("race_year = 2020"))

# COMMAND ----------

# How to rank results

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("total_wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driverRankSpec))
display(final_df)


# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------


