# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION 'abfss://demo@formula1adlg2.dfs.core.windows.net//'

# COMMAND ----------

df_results_json = spark.read.option("inferSchema", True).json(
    "abfss://raw@formula1adlg2.dfs.core.windows.net//2021-03-21//results.json"
)

# COMMAND ----------

df_results_json.write.mode('overwrite').format('delta').saveAsTable('f1_demo.results_json_D')

# COMMAND ----------

df_results_json.write.mode('overwrite').format('delta').option("overwriteSchema", "true").partitionBy('constructorId').save('abfss://demo@formula1adlg2.dfs.core.windows.net//results_json_DEXT')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_json_DEXT
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://demo@formula1adlg2.dfs.core.windows.net//results_json_DEXT'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_json_dext

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import *

deltaTable = DeltaTable.forPath(spark, "abfss://demo@formula1adlg2.dfs.core.windows.net//results_json_DEXT")

# Update every even value by adding 100 to it
deltaTable.update(
  condition = col('position') == "\\N",
  set = {'position': lit("-101")})

# COMMAND ----------

deltaTable.delete(condition = col("milliseconds") == '\\N')

# COMMAND ----------

display(deltaTable.toDF())

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_json_d
# MAGIC SET position = '-101'
# MAGIC WHERE position = '\\N'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_json_dext
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_json_d

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_json_DEXT

# COMMAND ----------

from pyspark.sql.functions import col, upper

# COMMAND ----------

df_drivers_day1 = spark.read.option('inferSchema', True).json('abfss://raw@formula1adlg2.dfs.core.windows.net//2021-03-28//drivers.json')\
    .filter('driverId <= 10') \
    .select('driverId', 'dob', col('name.forename').alias('forename'), col('name.surname').alias('surname'))

# COMMAND ----------

df_drivers_day1.createOrReplaceTempView('tempV_drivers_d1')

# COMMAND ----------

display(df_drivers_day1)

# COMMAND ----------


df_drivers_day2 = spark.read.option('inferSchema', True).json('abfss://raw@formula1adlg2.dfs.core.windows.net//2021-03-28//drivers.json')\
        .filter('driverId BETWEEN 6 AND 15')\
    .select('driverId', 'dob', upper('name.forename').alias('forename'), upper('name.surname').alias('surname'))

# COMMAND ----------

df_drivers_day2.createOrReplaceTempView('tempV_drivers_d2')

# COMMAND ----------

display(df_drivers_day2)

# COMMAND ----------

df_drivers_day2.columns

# COMMAND ----------

df_drivers_day3 = spark.read.option('inferSchema', True).json('abfss://raw@formula1adlg2.dfs.core.windows.net//2021-03-28//drivers.json')\
        .filter('driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20')\
    .select('driverId', 'dob', upper('name.forename').alias('forename'), upper('name.surname').alias('surname'))

# COMMAND ----------

display(df_drivers_day3)

# COMMAND ----------

df_drivers_day3.createOrReplaceTempView('tempV_drivers_d3')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_demo.drivers_merge;
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge
# MAGIC (
# MAGIC driverId INT, 
# MAGIC dob DATE, 
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC ) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM tempV_drivers_d1

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO f1_demo.drivers_merge as tgt
# MAGIC USING tempV_drivers_d1 as src
# MAGIC ON src.driverId = tgt.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = src.dob,
# MAGIC   tgt.forename = src.forename,
# MAGIC   tgt.surname = src.surname,
# MAGIC   tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT ( driverId,
# MAGIC dob,
# MAGIC forename,
# MAGIC surname,
# MAGIC createdDate) VALUES (
# MAGIC   driverId,
# MAGIC dob,
# MAGIC forename,
# MAGIC surname,
# MAGIC current_timestamp
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge as tgt
# MAGIC USING tempV_drivers_d2 as src
# MAGIC ON src.driverId = tgt.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = src.dob,
# MAGIC   tgt.forename = src.forename,
# MAGIC   tgt.surname = src.surname,
# MAGIC   tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT ( driverId,
# MAGIC dob,
# MAGIC forename,
# MAGIC surname,
# MAGIC createdDate) VALUES (
# MAGIC   driverId,
# MAGIC dob,
# MAGIC forename,
# MAGIC surname,
# MAGIC current_timestamp
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED f1_demo.drivers_merge;

# COMMAND ----------

from delta.tables import DeltaTable

deltaTableDriversMerge = DeltaTable.forPath(spark, 'abfss://demo@formula1adlg2.dfs.core.windows.net/drivers_merge')

deltaTableDriversMerge.alias('tgt') \
  .merge(
    df_drivers_day3.alias('src'),
    'src.driverId = tgt.driverId'
  ) \
  .whenMatchedUpdate(set =
    {
      "tgt.dob" : "src.dob",
      "tgt.forename" : "src.forename",
      "tgt.surname" : "src.surname",
      "tgt.updatedDate" : "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "src.driverId",
      "dob" : "src.dob",
      "forename" : "src.forename",
      "surname" : "src.surname",
      "createdDate" : "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge version as of 1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge version as of 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-03-07T16:11:08.000+00:00';

# COMMAND ----------


df = spark.read.format("delta").option("timestampAsOf", '2024-03-07T16:11:08.000+00:00').load("abfss://demo@formula1adlg2.dfs.core.windows.net/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum f1_demo.drivers_merge RETAIN 0 HOURS --BY DEFAULT 7 DAYS (168 HOURS)
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge as tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 3 as src
# MAGIC ON (src.driverId = tgt.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC INSERT *

# COMMAND ----------

# Los logs de delta lake se conservan durante 30 días. No es recomendable mantenerlos por más tiempo pero tampoco confirgurarlo de tal manera que la retención sea mínima (0) 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS f1_demo.drivers_merge_parquet;
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge_parquet
# MAGIC (
# MAGIC driverId INT, 
# MAGIC dob DATE, 
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC ) USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_merge_parquet
# MAGIC SELECT * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_merge_parquet

# COMMAND ----------

df = spark.table('f1_demo.drivers_merge_parquet')

# COMMAND ----------

df.write.parquet('abfss://demo@formula1adlg2.dfs.core.windows.net/drivers_merge_parquet2')

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`abfss://demo@formula1adlg2.dfs.core.windows.net/drivers_merge_parquet2`

# COMMAND ----------

