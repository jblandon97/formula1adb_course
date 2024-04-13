# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list('formula1-secret-scope')

# COMMAND ----------

dbutils.secrets.get(scope='formula1-secret-scope', key='pdb-formula1-account-key')

# COMMAND ----------

