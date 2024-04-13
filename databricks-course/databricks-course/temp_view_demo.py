# Databricks notebook source
# MAGIC %run "/Formula1/Ingestion/includes/configuration" 

# COMMAND ----------

# MAGIC %run "/Formula1/Ingestion/includes/functions" 

# COMMAND ----------

df_race_results = spark.read.parquet(f'{gold_abfss}//races_results')

# COMMAND ----------

# df_race_results.createTempView('v_race_results') # si esta linea se ejecuta una segunda vez, arrojara una excepción, dado que la vista ya existe. Por eso es mejor:

df_race_results.createOrReplaceTempView('v_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM v_race_results

# COMMAND ----------

df_sql_demo = spark.sql('SELECT * FROM v_race_results')

# COMMAND ----------

# una vista temporal local solo es valida desde una sesión de Spark, por lo que no estará disponible en otro cuaderno. Por otro lado, una vista temporal global estará disponible para todos notebooks que estén asociados a ese cluster. En cualquier caso, la vista temporal solo estará disponible hasta que se desconecte o reinicié el cluster !!

# COMMAND ----------

df_race_results.createOrReplaceGlobalTempView('v_global_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM global_temp.v_global_race_results

# COMMAND ----------

