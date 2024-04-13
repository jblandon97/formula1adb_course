# Databricks notebook source
# MAGIC %md
# MAGIC * The structure of the URI is: abfs[s]://file_system@account_name.dfs.core.windows.net/<path>/<path>/<file_name>

# COMMAND ----------

# spark.conf.set(\
# 'fs.azure.account.key.formula1adlg2.dfs.core.windows.net' # fs.azure.account.key.<endpoint_adlg2>
# , '...' # Access key
# )

# COMMAND ----------


endpoint = 'abfs://demo@formula1adlg2.dfs.core.windows.net/circuits.csv'
# dbutils.fs.ls(endpoint)
display(dbutils.fs.ls(endpoint))

# COMMAND ----------

dfspark = spark.read.csv(endpoint,header=True)
display(dfspark)

# COMMAND ----------

