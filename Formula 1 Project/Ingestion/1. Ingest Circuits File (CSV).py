# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC #### Additional notes
# MAGIC 1. Never use inferSchema when reading into dataframe because it's not reliable
# MAGIC 2. Make sure that you use header option when reading csv into dataframe
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1: Identify the container and file location

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("user_input_ds", "")
v_data_source = dbutils.widgets.get("user_input_ds")

# COMMAND ----------

display(v_data_source)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# fs stands for file system and ls stands for listing
display(dbutils.fs.ls('/mnt/f1datalakepractice/raw'))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Read the CSV file using the Spark Dataframe reader, must create and connect schema when doing so (NO INFERSCHEMA)

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

# Here, we import functions that we can use to convert ingested data into data types
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# Here, we specify the schema by using the imported functions
circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),
                                       StructField("circuitRef", StringType(), True),
                                       StructField("name", StringType(), True),
                                       StructField("location", StringType(), True),
                                       StructField("country", StringType(), True),
                                       StructField("lat", DoubleType(), True),
                                       StructField("lng", DoubleType(), True),
                                       StructField("alt", IntegerType(), True),
                                       StructField("url", StringType(), True), 
                                       ])

# COMMAND ----------

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuits_schema) \
    .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

# More efficient function
display(circuits_df)

# COMMAND ----------

# Identify ingested data types
display(circuits_df.printSchema())

# COMMAND ----------

# Stats per column
display(circuits_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Select all the columns we need with "col" function

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Here, we use col to identify which columns we want, and we can also alias them with different names
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Rename the columns using ".withColumnRenamed"

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Add ingestion date column to dataframe

# COMMAND ----------

circuits_final_df = add_ingestion(circuits_renamed_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6: Write Data to datalake as parquet format

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_processed.circuits

# COMMAND ----------

display(dbutils.fs.ls("/mnt/f1datalakepractice/processed/circuits"))

# COMMAND ----------

df = spark.read.parquet("/mnt/f1datalakepractice/processed/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------


