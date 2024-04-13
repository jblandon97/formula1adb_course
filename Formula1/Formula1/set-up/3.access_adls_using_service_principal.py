# Databricks notebook source
# MAGIC %md
# MAGIC * The structure of the URI is: abfs[s]://file_system@account_name.dfs.core.windows.net/<path>/<path>/<file_name>

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1adlg2.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1adlg2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1adlg2.dfs.core.windows.net","sp=rl&st=2024-01-28T16:43:05Z&se=2024-01-29T00:43:05Z&spr=https&sv=2022-11-02&sr=c&sig=xf3N1X4CCUdg%2Briva%2B9NHcGiBHoS%2BWEnxVcjjCVy3%2FU%3D")

# COMMAND ----------


endpoint = 'abfs://demo@formula1adlg2.dfs.core.windows.net/circuits.csv'
# dbutils.fs.ls(endpoint)
display(dbutils.fs.ls(endpoint))

# COMMAND ----------

dfspark = spark.read.csv(endpoint)
display(dfspark)

# COMMAND ----------

