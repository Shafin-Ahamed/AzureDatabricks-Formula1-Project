# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# We need to define the scheuma, but this JSON file has nested data
name_schema = StructType(fields = [StructField("forename", StringType(), True),
                                      StructField("surname", StringType(), True)  
                                      ])

driver_schema = StructType(fields = [StructField("driverId", IntegerType(), False),
                                     StructField("driverRef", StringType(), False),
                                     StructField("number", IntegerType(), False),
                                     StructField("code", StringType(), False),
                                     StructField("name", name_schema),
                                     StructField("dob", DateType(), False),
                                     StructField("nationality", StringType(), False),
                                     StructField("url", StringType(), False)
                                     ])


# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

dbutils.widgets.text("user_input_ds", "")
v_data_source = dbutils.widgets.get("user_input_ds")

# COMMAND ----------

v_data_source

# COMMAND ----------

drivers_df = spark.read \
    .schema(driver_schema) \
    .json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

display(drivers_df.printSchema())

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, concat, lit

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

drivers_ingest_df = add_ingestion(drivers_df)

# COMMAND ----------

drivers_final_df = drivers_ingest_df.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .drop("url") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref")

                                

# COMMAND ----------

display(drivers_final_df)

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/drivers"))

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

display(df)

# COMMAND ----------


