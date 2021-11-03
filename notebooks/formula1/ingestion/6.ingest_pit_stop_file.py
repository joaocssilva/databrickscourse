# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

pit_stop_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("stop", StringType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("duration", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True)
                                    ])

# COMMAND ----------

pit_stop_df = spark.read \
.schema(pit_stop_schema) \
.option("multiLine", True) \
.json("/mnt/formula1joaodl/raw/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = pit_stop_df.withColumnRenamed("driverId","driver_id") \
                                    .withColumnRenamed("raceId","race_id") \
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to parquet file

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1joaodl/processed/pit_stops")

# COMMAND ----------

