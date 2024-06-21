# Databricks notebook source
# We'll define schema using DDL style
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

dbutils.widgets.text("user_input_ds", "")
v_data_source = dbutils.widgets.get("user_input_ds")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC %run "../Includes/Configuration"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

display(constructor_df.printSchema())

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# This time, instead of using select and col to get only columns we want, we can use the drop function to just drop url
constructors_selected_df = constructor_df.drop("url")

# COMMAND ----------

display(constructors_selected_df)

# COMMAND ----------

# MAGIC %run "../Includes/common_functions"

# COMMAND ----------

constructors_ingest_df = add_ingestion(constructors_selected_df)

# COMMAND ----------

display(constructors_ingest_df)

# COMMAND ----------

constructors_final_df = constructors_ingest_df.withColumnRenamed("constructorId", "constructor_id") \
                                               .withColumnRenamed("constructorRef", "constructor_ref")


# COMMAND ----------

display(constructors_final_df)

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/constructors"))

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

display(df)

# COMMAND ----------


