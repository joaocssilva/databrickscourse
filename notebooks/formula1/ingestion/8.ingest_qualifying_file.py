# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying folder

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                     StructField("raceId", IntegerType(), True),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("q1", StringType(), True),
                                     StructField("q2", StringType(), True),
                                     StructField("q3", StringType(), True)
                                    ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json("/mnt/formula1joaodl/raw/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add new columns
# MAGIC 1. Rename columns
# MAGIC 2. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId","qualify_id") \
                                    .withColumnRenamed("raceId","race_id") \
                                    .withColumnRenamed("driverId","driver_id") \
                                    .withColumnRenamed("constructorId","constructor_id") \
                                    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write output to parquet file

# COMMAND ----------

qualifying_final_df.write.mode("overwrite").parquet("/mnt/formula1joaodl/processed/qualifying")

# COMMAND ----------

