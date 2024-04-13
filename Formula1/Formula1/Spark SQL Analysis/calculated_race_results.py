# Databricks notebook source
# MAGIC %sql
# MAGIC USE DATABASE f1_silver;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE f1_gold.calculated_race_results
# MAGIC USING parquet
# MAGIC AS
# MAGIC SELECT 
# MAGIC      t4.race_year,
# MAGIC      t3.name as constructor_name,
# MAGIC      t2.name as driver_name,
# MAGIC      t1.position,
# MAGIC      t1.points,
# MAGIC      (11-t1.position) as normalized_points
# MAGIC FROM results t1
# MAGIC INNER JOIN drivers t2 on t2.driver_id = t1.driver_id
# MAGIC INNER JOIN constructors t3 on t3.constructor_id = t1.constructor_id
# MAGIC INNER JOIN races t4 on t4.race_id = t1.race_id
# MAGIC where  t1.position <= 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED f1_gold.calculated_race_results;

# COMMAND ----------

