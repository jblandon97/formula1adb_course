# Databricks notebook source
# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %fs
# MAGIC ls databricks-datasets/

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

dbutils.fs.ls('dbfs:/databricks-datasets/')

# COMMAND ----------

for files in dbutils.fs.ls('dbfs:/databricks-datasets/'):
  if files.name.endswith('/'):
    print(files)

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help('ls')

# COMMAND ----------

