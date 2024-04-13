# Databricks notebook source
dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')
# print(v_data_source)

# COMMAND ----------

# MAGIC %run "/Formula1/Transform/includes/configuration" 

# COMMAND ----------

# MAGIC %run "/Formula1/Transform/includes/functions" 

# COMMAND ----------

# df_race_results = spark.read.parquet(f'{gold_abfss}//races_results')
race_results_years = spark.read.parquet(f'{gold_abfss}//race_results').filter(
    f'file_date = "{v_file_date}"'
).select('race_year').distinct().collect()
list_years =[]
for i in race_results_years:
    list_years.append(i.race_year)
print(list_years)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, max, sum, when, col

# COMMAND ----------

df_race_results = spark.read.parquet(f'{gold_abfss}//race_results').filter(
   col('race_year').isin(list_years)
)

# COMMAND ----------

display(df_race_results)

# COMMAND ----------

df_constructor_standings = df_race_results.groupBy('race_year', 'team').agg(sum('points').alias('Total points'), count(when(col('position')==1, True)).alias('wins'))

# COMMAND ----------

display(df_constructor_standings.filter('race_year = 2020'))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

# COMMAND ----------

driverRankSpec = Window.partitionBy('race_year').orderBy(desc('Total points'), desc('wins'))
df_constructor_standings_final = df_constructor_standings.withColumn('rank', rank().over(driverRankSpec))

# COMMAND ----------

display(df_constructor_standings_final)

# COMMAND ----------

# df_constructor_standings_final.write.mode('overwrite').partitionBy('race_year').format('parquet').saveAsTable('f1_gold.constructor_standings')

# COMMAND ----------

incremental_load(df_constructor_standings_final, 'race_year', 'f1_gold', 'constructor_standing')

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

