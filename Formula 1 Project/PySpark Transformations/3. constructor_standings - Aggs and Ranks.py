# Databricks notebook source
# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import sum, countDistinct, count, col, when

constructor_standings_df = race_results_df.groupBy("race_year", "constructors_team") \
.agg(sum("results_points").alias("total_points"), \
     count(when(col("results_position") == 1, True)).alias("total_wins"))

# COMMAND ----------

display(constructor_standings_df.where("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

constructorRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("total_wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructorRankSpec))
display(final_df.where("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")

# COMMAND ----------

c_df = spark.read.parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------

display(c_df)

# COMMAND ----------


