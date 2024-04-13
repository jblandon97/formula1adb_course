# Databricks notebook source
df = spark.read.parquet('abfss://silver@formula1adlg2.dfs.core.windows.net//races')

# COMMAND ----------

display(df)

# COMMAND ----------

# df_filter = df.filter("race_year = 2019") # WHERE SQL CLAUSULE 
df_filter = df.filter(df['race_year'] == 2019)

# COMMAND ----------

display(df_filter)

# COMMAND ----------

