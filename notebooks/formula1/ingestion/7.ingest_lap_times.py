# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                   StructField("driverIf", IntegerType(), True),
                                   StructField("lap", IntegerType(), True),
                                   StructField("position", IntegerType(), True),
                                   StructField("time", StringType()),
                                   StructField("miliseconds", IntegerType(), False)
])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv("/mnt/formula1joaodl/raw/lap_times")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC #####1. raceId renamed to race_id
# MAGIC #####2. driverId renamed to driver_id
# MAGIC #####3.ingestion date added

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId","race_id") \
.withColumnRenamed("driverRef","driver_ref") \
.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

lap_times_final_df.write.mode("overwrite").parquet("/mnt/formula1joaodl/processed/lap_times")