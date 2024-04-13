# Databricks notebook source
dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')
# print(v_data_source)

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')
# print(v_data_source)

# COMMAND ----------

# MAGIC %run "/Formula1/Ingestion/includes/configuration" 

# COMMAND ----------

# MAGIC %run "/Formula1/Ingestion/includes/functions"

# COMMAND ----------

schema = "constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING"

# COMMAND ----------

raw_uri = f"{raw_abfss}//{v_file_date}//constructors.json"

# COMMAND ----------

df_constructors = spark.read.schema(schema).json(raw_uri)

# COMMAND ----------

df_constructors.printSchema()

# COMMAND ----------

display(df_constructors)

# COMMAND ----------

df_constructors_dropped = df_constructors.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, column

# COMMAND ----------

df_constructors_final = (
    add_ingestion_date(df_constructors_dropped)
    .withColumnRenamed("constructorId", "constructor_id")
    .withColumnRenamed("constructorRef", "constructor_ref")
    .withColumn("data_source", lit(v_data_source))
    .withColumn("file_date", lit(v_file_date))
)

# COMMAND ----------

display(df_constructors_final)

# COMMAND ----------

df_constructors_final.write.mode('overwrite').format('delta').saveAsTable('f1_silver.constructors')

# COMMAND ----------

dbutils.notebook.exit('Success')