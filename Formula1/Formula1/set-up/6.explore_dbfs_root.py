# Databricks notebook source
display(dbutils.fs.ls('/')) # '/': root

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/tables/'))

# COMMAND ----------

dfspark = spark.read.csv('/FileStore/tables/', header=True)

# COMMAND ----------

display(dfspark)

# COMMAND ----------

