# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT 
# MAGIC    driver_name,
# MAGIC    count(1) AS number_of_races,
# MAGIC    SUM(normalized_points) as total_points,
# MAGIC    avg(normalized_points) as media
# MAGIC FROM f1_gold.calculated_race_results
# MAGIC GROUP BY driver_name
# MAGIC HAVING count(1) > 50
# MAGIC ORDER BY media DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC    driver_name,
# MAGIC    count(1) AS number_of_races,
# MAGIC    SUM(normalized_points) as total_points,
# MAGIC    avg(normalized_points) as media
# MAGIC FROM f1_gold.calculated_race_results
# MAGIC WHERE race_year BETWEEN 2000 AND 2010
# MAGIC GROUP BY driver_name
# MAGIC HAVING count(1) > 50
# MAGIC ORDER BY media DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC    t1.constructor_name,
# MAGIC    count(1) AS number_of_races,
# MAGIC    SUM(normalized_points) as total_points,
# MAGIC    avg(normalized_points) as media
# MAGIC FROM f1_gold.calculated_race_results as t1
# MAGIC GROUP BY t1.constructor_name
# MAGIC HAVING count(1) > 50
# MAGIC ORDER BY media DESC;

# COMMAND ----------

