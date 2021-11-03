# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True),
])

# COMMAND ----------

driver_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                   StructField("driverRef", StringType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("code", IntegerType(), True),
                                   StructField("name", name_schema),
                                   StructField("dob", StringType(), False),
                                   StructField("nacionality", StringType(), False),
                                   StructField("url", StringType(), False)    
])

# COMMAND ----------

drivers_df = spark.read \
.schema(driver_schema) \
.json("/mnt/formula1joaodl/raw/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename columns and add new columns
# MAGIC ##### 1. driverid renamend ro driver_id
# MAGIC ##### 2. driverRef renamed to driver_ref
# MAGIC ##### 3.ingestion date added
# MAGIC ##### 4. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit

# COMMAND ----------

drivers_with_columns_df = drivers_df.withColumnRenamed("driverID", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("ingestion_date", current_timestamp()) \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop the unwanted columns
# MAGIC ##### 1. name.forename
# MAGIC ##### 2. name.surname
# MAGIC ##### 3. url

# COMMAND ----------

drivers_with_columns_df = drivers_with_columns_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

drivers_with_columns_df.write.mode("overwrite").parquet("/mnt/formula1joaodl/processed/drivers")