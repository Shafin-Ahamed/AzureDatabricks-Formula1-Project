# Databricks notebook source
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType, FloatType

# COMMAND ----------

pit_stops_schema = StructType(fields= [StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("stop", StringType(), True),
                                       StructField("lap", IntegerType(), True),
                                       StructField("time", StringType(), True),
                                       StructField("duration", StringType(), True),
                                       StructField("milliseconds", IntegerType(), True),
                                       ])

# COMMAND ----------

dbutils.widgets.text("user_input_ds", "")
v_data_source = dbutils.widgets.get("user_input_ds")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

# For multi-line JSON files
pit_stops_df = spark.read.schema(pit_stops_schema).option("multiLine", True).json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

pit_stops_ingest_df = add_ingestion(pit_stops_df)

# COMMAND ----------

pit_stops_transformed_df = pit_stops_ingest_df.withColumnRenamed("raceId","race_id") \
                                       .withColumnRenamed("driverId","driver_id") \
                                       .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(pit_stops_transformed_df)

# COMMAND ----------

pit_stops_transformed_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/pit_stops"))

# COMMAND ----------


