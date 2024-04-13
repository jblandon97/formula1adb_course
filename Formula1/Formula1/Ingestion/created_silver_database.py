# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_silver
# MAGIC LOCATION 'abfss://silver@formula1adlg2.dfs.core.windows.net/' -- va ha ser manejada pero sobre nuestra ubicación

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DATABASE EXTENDED f1_silver;

# COMMAND ----------

