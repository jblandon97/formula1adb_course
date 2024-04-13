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
                    StructField("resultId", IntegerType(), True),
                    StructField("raceId", IntegerType(), True),
                    StructField("driverId", IntegerType(), True),
                    StructField("constructorId", IntegerType(), True),
                    StructField("number", IntegerType(), True),
                    StructField("grid", IntegerType(), True),
                    StructField("position", IntegerType(), True),
                    StructField("positionText", StringType(), True),
                    StructField("positionOrder", IntegerType(), True),
                    StructField("points", FloatType(), True),
                    StructField("laps", IntegerType(), True),
                    StructField("time", StringType(), True),
                    StructField("milliseconds", IntegerType(), True),
                    StructField("fastestLap", IntegerType(), True),
                    StructField("rank", IntegerType(), True),
                    StructField("fastestLapTime", StringType(), True),
                    StructField("fastestLapSpeed", FloatType(), True),
                    StructField("statusId", StringType(), True)
                    ]) 


# COMMAND ----------

raw_uri = f"{raw_abfss}//{v_file_date}//results.json"
print(raw_uri)

# COMMAND ----------

df_results = spark.read.schema(schema).json(
    raw_uri
)

# COMMAND ----------

# 2021-03-21: 1 - 1047
# 2021-03-38: 1052
# 2021-04-18: 1053



# COMMAND ----------

df_results.printSchema()

# COMMAND ----------

df_results_renamed = (
    df_results.withColumnRenamed("resultId", "result_id")\
    .withColumnRenamed("raceId", "race_id")\
    .withColumnRenamed("driverId", "driver_id")\
    .withColumnRenamed("constructorId", "constructor_id")\
    .withColumnRenamed("positionText", "position_text")\
    .withColumnRenamed("positionOrder", "position_order")\
    .withColumnRenamed("fastestLap", "fastest_lap")\
    .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\

)

# COMMAND ----------

df_results_renamed.printSchema()

# COMMAND ----------

display(df_results_renamed)

# COMMAND ----------

df_results_dropped = df_results_renamed.drop('statusId')

# COMMAND ----------

df_results_final = add_ingestion_date(df_results_dropped).withColumn('data_source', lit(v_data_source)).withColumn('file_date', lit(v_file_date))

# COMMAND ----------

display(df_results_final)

# COMMAND ----------

results_deduped_df = df_results_final.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

print(df_results_final.count(), results_deduped_df.count())

# COMMAND ----------

silver_uri = f'{silver_abfss}//results'

# COMMAND ----------

# MAGIC %md
# MAGIC #### metodo 1

# COMMAND ----------

# for i in (df_results_final.select(col('race_id')).distinct().collect()):
#   if spark._jsparkSession.catalog().tableExists('f1_silver.results'):
#     spark.sql(f'ALTER TABLE f1_silver.results DROP IF EXISTS PARTITION (race_id = {i.race_id}) ')

# COMMAND ----------

# df_results_final.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_silver.results')

# COMMAND ----------

# MAGIC %md
# MAGIC ### metodo 2

# COMMAND ----------

merge_detla_table('f1_silver', 'results', silver_uri, results_deduped_df, 'tgt.result_id = src.result_id and tgt.race_id = src.race_id', 'race_id')

# COMMAND ----------

# incremental_load(df_results_final, 'race_id', 'f1_silver', 'results')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   race_id,
# MAGIC   count(1)
# MAGIC FROM
# MAGIC   f1_silver.results
# MAGIC GROUP BY
# MAGIC   race_id
# MAGIC ORDER BY
# MAGIC   race_id desc;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM f1_silver.results WHERE file_date = '2021-03-21'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table f1_silver.results

# COMMAND ----------

