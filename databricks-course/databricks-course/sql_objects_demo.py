# Databricks notebook source
# MAGIC %sql
# MAGIC -- En el contexto de Spark, base de datos y esquema son sinónimos. 

# COMMAND ----------

# MAGIC %run "/Formula1/Ingestion/includes/configuration" 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DATABASE EXTENDED demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database();

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in  demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE DEMO;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database();

# COMMAND ----------

# MAGIC %md
# MAGIC ### MANAGED TABLES

# COMMAND ----------

df_race_results = spark.read.parquet(f'{gold_abfss}//races_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE DATABASE demo;

# COMMAND ----------

# df_race_results.write.format('parquet').saveAsTable('race_results')
df_race_results.write.format('parquet').saveAsTable('demo.race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED demo.race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM demo.race_results WHERE race_year = 2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE demo.race_results_2020 as
# MAGIC SELECT * FROM demo.race_results WHERE race_year = 2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE EXTENDED demo.race_results_2020

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE demo.race_results_2020;

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXTERNAL TABLES

# COMMAND ----------

df_race_results.write.format('parquet').option('path', f'{gold_abfss}//race_results_ext').saveAsTable('demo.race_results_ext')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE EXTENDED demo.race_results_ext

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE demo.race_results_ext_v2
# MAGIC (
# MAGIC
# MAGIC   race_name	string,
# MAGIC   race_date	date,
# MAGIC   circuit_location	string,
# MAGIC   driver_name	string,
# MAGIC   driver_number	int,
# MAGIC   driver_nationality	string,
# MAGIC   team	string,
# MAGIC   grid	int,
# MAGIC   fastest_lap	int,
# MAGIC   race_time	string,
# MAGIC   points	float,
# MAGIC   position	int,
# MAGIC   create_date	timestamp,
# MAGIC   race_year	int
# MAGIC
# MAGIC )
# MAGIC USING PARQUET
# MAGIC LOCATION 'abfss://gold@formula1adlg2.dfs.core.windows.net/race_results_ext_v2'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO  demo.race_results_ext_v2
# MAGIC SELECT * FROM demo.race_results_ext WHERE race_year = 2020 
# MAGIC -- es cuando se insertan datos que se escribiran los parquets en ALDG2

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM demo.race_results_ext_v2

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE demo.race_results_ext_v2 --no se eliminarán los parquets del ADLG2

# COMMAND ----------

# MAGIC %md
# MAGIC ### VIEWS ON TABLES

# COMMAND ----------

# Las tablas, sean administradas o externas, almacenan y relacionan datos que estan almacenados en un almacen de objetos. Las vistas son simplemente una representación de esos datos, una vista no existe como conjunto de valores de datos almacenados en una base de datos. Las filas y las columnas de datos proceden de tablas a las que se hace referencia en la consulta que define la vista y se producen de forma dinámica cuando se hace referencia a la vista 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CURRENT_DATABASE();

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_race_results AS
# MAGIC SELECT * FROM demo.race_results where race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results AS
# MAGIC SELECT * FROM demo.race_results where race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW demo.v_race_results AS
# MAGIC SELECT * FROM demo.race_results where race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP VIEW demo.v_race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW demo.Pv_race_results AS
# MAGIC SELECT * FROM demo.race_results where race_year = 2020

# COMMAND ----------

