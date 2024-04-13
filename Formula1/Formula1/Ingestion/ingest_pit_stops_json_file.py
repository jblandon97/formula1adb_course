# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')
# print(v_data_source)

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')
# print(v_data_source)

# COMMAND ----------

# MAGIC %run "/Formula1/Ingestion/includes/configuration" 

# COMMAND ----------

# MAGIC %run "/Formula1/Ingestion/includes/functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

raw_uri = f"{raw_abfss}//{v_file_date}//pit_stops.json"

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(raw_uri)

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

df_pit_stops_final = pit_stops_df.withColumnRenamed("driverId", "driver_id").withColumnRenamed("raceId", "race_id").withColumn("ingestion_date", current_timestamp()).withColumn('data_source', lit(v_data_source)).withColumn('file_date', lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

silver_uri = f'{silver_abfss}//pit_stops'

# COMMAND ----------

# df_pit_stops_final.write.mode("overwrite").format('parquet').saveAsTable("f1_silver.pit_stops")

# COMMAND ----------

merge_detla_table('f1_silver', 'pit_stops', silver_uri, df_pit_stops_final, 'tgt.race_id = src.race_id AND\
                  tgt.driver_id = src.driver_id AND\
                  tgt.stop = src.stop AND\
                  tgt.race_id = src.race_id', 'race_id')

# COMMAND ----------

# incremental_load(df_pit_stops_final, 'race_id', 'f1_silver', 'pit_stops')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_silver.pit_stops

# COMMAND ----------

