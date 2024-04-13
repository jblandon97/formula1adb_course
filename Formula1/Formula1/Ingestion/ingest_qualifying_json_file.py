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

from pyspark.sql.types import StringType, FloatType, IntegerType, StructType, StructField
from pyspark.sql.functions import current_timestamp, lit, col, concat


# COMMAND ----------

schema = StructType(fields=[
StructField("qualifyId", IntegerType(), False),
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

raw_uri = f"{raw_abfss}//{v_file_date}//qualifying"

# COMMAND ----------

df_qualifying = spark.read.schema(schema).json(
    raw_uri,
        # "abfss://raw@formula1adlg2.dfs.core.windows.net//qualifying//qualifying_split*.csv", si quiseramos seleccionar archivos específicos. 
        multiLine=True
)

# COMMAND ----------

df_qualifying.printSchema()

# COMMAND ----------

df_qualifying_final = (
    add_ingestion_date(df_qualifying)\
    .withColumnRenamed("qualifyId", "qualify_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumn('data_source', lit(v_data_source))
    .withColumn('file_date', lit(v_file_date))

)

# COMMAND ----------

df_qualifying_final.printSchema()

# COMMAND ----------

display(df_qualifying_final)

# COMMAND ----------

silver_uri = f'{silver_abfss}//qualifying'

# COMMAND ----------

# df_qualifying_final.write.mode('overwrite').format('parquet').saveAsTable('f1_silver.qualifying')

# COMMAND ----------

merge_detla_table('f1_silver', 'qualifying', silver_uri, df_qualifying_final, 'tgt.qualify_id = src.qualify_id AND\
                  tgt.race_id = src.race_id', 'race_id')

# COMMAND ----------

# incremental_load(df_qualifying_final, 'race_id', 'f1_silver', 'qualifying')

# COMMAND ----------

dbutils.notebook.exit('Success')