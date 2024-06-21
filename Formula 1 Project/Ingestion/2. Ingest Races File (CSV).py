# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("user_input_ds", "")
v_data_source = dbutils.widgets.get("user_input_ds")

# COMMAND ----------

v_data_source

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType, TimestampNTZType, DayTimeIntervalType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), False),
                                  StructField("round", IntegerType(), False),
                                  StructField("circuitId", IntegerType(), False),
                                  StructField("Name", StringType(), False),
                                  StructField("date", DateType(), False),
                                  StructField("time", StringType(), False),
                                  StructField("url", StringType(), False),
                                  ])

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

races_df = spark.read \
    .option("Header", True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

races_selected_df = races_df.select(col('raceId'), col('year'), col('round'), col('circuitId'), col('Name'), col('date'), col('time'))

# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId","race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", 'circuit_id') \
    .withColumnRenamed("Name", "name") \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(races_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

races_ing_df = add_ingestion(races_renamed_df)

# COMMAND ----------

races_final_df = races_ing_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyy-MM-dd HH:mm:ss'))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# Partition by helps seperate the data by race year and put them in seperate folders within processed container
races_final_df.write.mode("overwrite").partitionBy("race_year").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/races"))

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(df)

# COMMAND ----------


