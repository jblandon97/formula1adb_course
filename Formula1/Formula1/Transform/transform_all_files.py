# Databricks notebook source
v_success = dbutils.notebook.run('/Workspace/Formula1/Transform/race_results', 0)
print(v_success)

# COMMAND ----------

v_success = dbutils.notebook.run('/Workspace/Formula1/Transform/constructor_standings', 0)
print(v_success)

# COMMAND ----------

v_success = dbutils.notebook.run('/Workspace/Formula1/Transform/driver_standings', 0)
print(v_success)