# Databricks notebook source
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields = [StructField("resultId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("grid", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("positionText", StringType(), True),
                                      StructField("positionOrder", IntegerType(), True),
                                      StructField("points", FloatType(), True),
                                      StructField("laps", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),
                                      StructField("fastestLap", IntegerType(), True),
                                      StructField("rank", IntegerType(), True),
                                      StructField("fastestLapTime", StringType(), True),
                                      StructField("fastestLapSpeed", FloatType(), True),
                                      StructField("statusId", IntegerType(), True)])

# COMMAND ----------

dbutils.widgets.text("user_input_ds", "")
v_data_source = dbutils.widgets.get("user_input_ds")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json(f"{raw_folder_path}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

results_ingest_df = add_ingestion(results_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_transformed_df = results_ingest_df \
    .withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("data_source", lit(v_data_source)) \
    .drop("statusId")

# COMMAND ----------

display(results_transformed_df)

# COMMAND ----------

results_transformed_df.write.mode("overwrite").partitionBy("race_Id").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/results"))

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/results"))

# COMMAND ----------


