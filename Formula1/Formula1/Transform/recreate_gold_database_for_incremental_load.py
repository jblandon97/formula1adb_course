# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_gold CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_gold
# MAGIC LOCATION 'abfss://gold@formula1adlg2.dfs.core.windows.net/' -- va ha ser manejada pero sobre nuestra ubicación