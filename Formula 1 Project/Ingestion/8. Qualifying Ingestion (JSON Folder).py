# Databricks notebook source
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType, FloatType

# COMMAND ----------

qualifying_schema = StructType(fields= [StructField("qualifyId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                       StructField("driverId", IntegerType(), True),
                                       StructField("constructorId", IntegerType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True),
                                       ])

# COMMAND ----------

dbutils.widgets.text("user_input_ds", "")
v_data_source = dbutils.widgets.get("user_input_ds")

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

qualifying_df = spark.read.option("multiLine", True).schema(qualifying_schema).json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

qualifying_ingest_df = add_ingestion(qualifying_df)

# COMMAND ----------

qualifying_final_df = qualifying_ingest_df.withColumnRenamed("constructorId", "constructor_id") \
                                   .withColumnRenamed("raceId","race_id") \
                                   .withColumnRenamed("driverId", "driver_id") \
                                   .withColumnRenamed("qualifyId", "qualify_id") \
                                   .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(qualifying_final_df)

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------


