# Databricks notebook source
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

raw_uri = f"{raw_abfss}//{v_file_date}//lap_times"

# COMMAND ----------

from pyspark.sql.types import StringType, FloatType, IntegerType, StructType, StructField
from pyspark.sql.functions import current_timestamp, lit, col, concat


# COMMAND ----------

schema = StructType(fields=[
 StructField("raceId", IntegerType(), False),\
StructField("driverId", IntegerType(), True),\
StructField("lap", IntegerType(), True),\
StructField("position", IntegerType(), True),\
StructField("time", StringType(), True),\
StructField("milliseconds", IntegerType(), True)
                    ]) 


# COMMAND ----------

df_lap_times = spark.read.schema(schema).csv(
  raw_uri
        # "abfss://raw@formula1adlg2.dfs.core.windows.net//lap_times//lap_times_split*.csv", si quiseramos seleccionar archivos específicos. 
)

# COMMAND ----------

df_lap_times.printSchema()

# COMMAND ----------

df_lap_times_final = (
    add_ingestion_date(df_lap_times)
    .withColumnRenamed("raceId", "race_id")
    .withColumnRenamed("driverId", "driver_id")
    .withColumn("data_source", lit(v_data_source))
    .withColumn("file_date", lit(v_file_date))
)

# COMMAND ----------

df_lap_times_final.printSchema()

# COMMAND ----------

display(df_lap_times_final)

# COMMAND ----------

silver_uri = f'{silver_abfss}//lap_times'

# COMMAND ----------

# df_lap_times_final.write.mode('overwrite').format('parquet').saveAsTable('f1_silver.lap_times')

# COMMAND ----------

merge_detla_table('f1_silver','lap_times', silver_uri, df_lap_times_final, 'tgt.race_id = src.race_id AND\
                  tgt.driver_id = src.driver_id AND\
                  tgt.lap = src.lap AND\
                  tgt.race_id = src.race_id', 'race_id')

# COMMAND ----------

# incremental_load(df_lap_times_final, 'race_id', 'f1_silver', 'lap_times')

# COMMAND ----------

dbutils.notebook.exit('Success')