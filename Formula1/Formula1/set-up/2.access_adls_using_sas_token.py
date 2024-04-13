# Databricks notebook source
# MAGIC %md
# MAGIC * The structure of the URI is: abfs[s]://file_system@account_name.dfs.core.windows.net/<path>/<path>/<file_name>

# COMMAND ----------

formula1_adlg2_sas_token=dbutils.secrets.get(scope='formula1-secret-scope', key='pdb-formula-adlg2-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1adlg2.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1adlg2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1adlg2.dfs.core.windows.net",formula1_adlg2_sas_token)

# COMMAND ----------


endpoint = 'abfs://demo@formula1adlg2.dfs.core.windows.net/circuits.csv'
# dbutils.fs.ls(endpoint)
display(dbutils.fs.ls(endpoint))

# COMMAND ----------

dfspark = spark.read.csv(endpoint)
display(dfspark)

# COMMAND ----------

