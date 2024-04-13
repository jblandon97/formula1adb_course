# Databricks notebook source
dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')
# print(v_data_source)

# COMMAND ----------

# MAGIC %run "/Formula1/Transform/includes/configuration" 

# COMMAND ----------

# MAGIC %run "/Formula1/Transform/includes/functions" 

# COMMAND ----------

columns = ["Column Name", "Source"]
data = [
    ("race_year", "races"),
    ("race_name", "races"),
    ("race_date", "races"),
    ("circuit_location", "circuits"),
    ("driver_name", "drivers"),
    ("driver_number", "drivers"),
    ("driver_nationality", "drivers"),
    ("team", "constructors"),
    ("grid", "results"),
    ("fastest_lap", "results"),
    ("race_time", "results"),
    ("points", "results"),
    ("position", "results"),
    ("create_date", "current_timestamp"),
    ("race_id", "results & races"),
    ("file_date", "results")
]

dfFromData = spark.createDataFrame(data).toDF(*columns)

# COMMAND ----------

display(dfFromData)

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

df_races = spark.read.format("delta").load(f"{silver_abfss}//races").select(
    col('race_id'), col('circuit_id'), col("race_year"), col("name").alias('race_name'), col("date").alias('race_date')
)


df_circuits = spark.read.format("delta").load(f"{silver_abfss}//circuits").select(col('location').alias("circuit_location"), col('circuit_id')
)

df_drivers = spark.read.format("delta").load(f"{silver_abfss}//drivers").select(
    col('driver_id'), col('name').alias("driver_name"), col('number').alias("driver_number"), col('nationality').alias("driver_nationality"),
)

df_constructors = spark.read.format("delta").load(f"{silver_abfss}//constructors").select(col('constructor_id'), col('name').alias("team"))

df_results = spark.read.format("delta").load(f"{silver_abfss}//results").select(
     col('race_id').alias('result_race_id'), col('driver_id'), col('constructor_id'), "grid", "fastest_lap", col('time').alias("race_time"), "points", "position", 'file_date'
).filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

df_races.count()

# COMMAND ----------

df_races_results_vw1 = join_dataframes(df_races, df_circuits, 'circuit_id', 'inner')


# COMMAND ----------

df_races_results_vw2 = df_races_results_vw1.join(df_results, df_races_results_vw1['race_id']==df_results['result_race_id'] ,how='inner')

# COMMAND ----------

df_races_results_vw3 = join_dataframes(df_races_results_vw2, df_drivers, 'driver_id', 'inner')

# COMMAND ----------

columns_race_results = []
for i in (dfFromData.select('Column Name').collect()):
    columns_race_results.append(i['Column Name'])

# COMMAND ----------

df_races_results_final = add_ingestion_date(join_dataframes(df_races_results_vw3, df_constructors, 'constructor_id', 'inner')).select(*columns_race_results)

# COMMAND ----------

gold_uri = f'{gold_abfss}//race_results'

# COMMAND ----------

merge_delta_table('f1_gold', 'race_results', gold_uri, df_races_results_final, 'tgt.driver_name = src.driver_name and tgt.race_id = src.race_id', 'race_id')

# COMMAND ----------

# incremental_load(df_races_results_final, 'race_id', 'f1_gold', 'race_results')

# COMMAND ----------

# df_races_results_final.write.mode('overwrite').partitionBy('race_year').format('parquet').saveAsTable('f1_gold.race_results')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_gold.race_results

# COMMAND ----------

