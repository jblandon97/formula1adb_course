# Databricks notebook source
# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_silver CASCADE;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_silver
# MAGIC LOCATION 'abfss://silver@formula1adlg2.dfs.core.windows.net/' -- va ha ser manejada pero sobre nuestra ubicación