# Databricks notebook source
# MAGIC %run "/Formula1/Ingestion/includes/configuration" 
# MAGIC

# COMMAND ----------

# MAGIC %run "/Formula1/Ingestion/includes/functions" 

# COMMAND ----------

df_circuits = spark.read.parquet(f'{silver_abfss}//circuits').filter('circuit_id < 70')
df_races = spark.read.parquet(f'{silver_abfss}//races').filter('race_year = 2019')

# COMMAND ----------

display(df_circuits)
display(df_races)

# COMMAND ----------

# Edit the code block to fix the error
select_exp = [df_circuits['circuit_id'], df_circuits["name"].alias("circuit_name"), df_circuits["location"], df_circuits["country"], df_races["name"].alias("race_name"), df_races["round"]]




# COMMAND ----------

df_races_in_circuits = join_dataframes(df_circuits, df_races, 'circuit_id', 'inner')\
    .select(*select_exp)



# COMMAND ----------

display(df_races_in_circuits)

# COMMAND ----------

df_all_circuits = join_dataframes(df_circuits, df_races, 'circuit_id', 'left')\
    .select(*select_exp)

# COMMAND ----------

display(df_all_circuits)

# COMMAND ----------

df_all_races = join_dataframes(df_circuits, df_races, 'circuit_id', 'right')\
    .select(*select_exp)

# COMMAND ----------

display(df_all_races)

# COMMAND ----------

df_full = join_dataframes(df_circuits, df_races, 'circuit_id', 'outer')\
    .select(*select_exp)

# COMMAND ----------

display(df_full)

# COMMAND ----------

# semi join: es como el inner join pero solo tendrá en cuenta las columnas del dataframe izquierdo

# COMMAND ----------

# anti join: devuelve todo lo que esta en el dataframe izquierdo y a la vez no esta en el dataframe derecho
df_anti_join = join_dataframes(df_circuits, df_races, 'circuit_id', 'anti')


# COMMAND ----------

# comparativa
display(df_races_in_circuits)
display(df_anti_join)

# COMMAND ----------

# cross join: producto cartesiano de los dos dataframe. No se recomienda, generara grandes cantidades de datos y por ende errores de memora, teniendo en cuenta que se trabajará en entornos de Big Data

df_cross_join = df_races.crossJoin(df_circuits)

# COMMAND ----------

display(df_cross_join)

# COMMAND ----------

int(df_races.count()*df_circuits.count()) == df_cross_join.count()

# COMMAND ----------

