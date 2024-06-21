# Databricks notebook source
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType, FloatType

# COMMAND ----------

laps_schema = StructType(fields= [StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("lap", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("milliseconds", IntegerType(), True),
                                       ])

# COMMAND ----------

dbutils.widgets.text("user_input_ds", "")
v_data_source = dbutils.widgets.get("user_input_ds")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

# For folders with multiple files, we can just specify the folder. If we want only specific files from folder, we can use wildcards "*"
laps_df = spark.read.schema(laps_schema).csv(f"{raw_folder_path}/lap_times")

# COMMAND ----------

display(laps_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

laps_ingest_df = add_ingestion(laps_df)

# COMMAND ----------

laps_final_df = laps_ingest_df.withColumnRenamed("raceId", "race_id") \
                       .withColumnRenamed("driverId", "driver_id") \
                       .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(laps_final_df)

# COMMAND ----------

laps_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/lap_times"))

# COMMAND ----------

spark.read.parquet(f"{processed_folder_path}/lap_times").count()

# COMMAND ----------


