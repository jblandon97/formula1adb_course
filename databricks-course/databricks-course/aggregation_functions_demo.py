# Databricks notebook source
# MAGIC %run "/Formula1/Ingestion/includes/configuration" 

# COMMAND ----------

# MAGIC %run "/Formula1/Ingestion/includes/functions"

# COMMAND ----------

df_races_results = spark.read.parquet(f'{gold_abfss}//races_results')

# COMMAND ----------

display(df_races_results)

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, max, sum

# COMMAND ----------

df_races_results.filter('race_year = 2020').select(countDistinct('race_name')).withColumnRenamed('count(DISTINCT race_name)', 'Number of races in 2020').show()

# COMMAND ----------

df_races_results.filter('driver_name = "Lewis Hamilton" and race_year = 2020').select(sum('points'), countDistinct('race_name')).show()

# COMMAND ----------

df_races_results.groupBy('driver_name').sum('points').show()

# COMMAND ----------

df_races_results.groupBy('driver_name').agg(sum('points').alias('Total points'), countDistinct('race_name').alias('Number of races')).show()

# COMMAND ----------

demo_df_groupped = df_races_results.filter('race_year in (2019, 2020)').groupBy('race_year','driver_name').agg(sum('points').alias('Total points'), countDistinct('race_name').alias('Number of races'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

# COMMAND ----------

driverRankSpec = Window.partitionBy('race_year').orderBy(desc('Total points'))
demo_df_groupped.withColumn('rank', rank().over(driverRankSpec)).show(100)

# COMMAND ----------

