# Databricks notebook source
# MAGIC %md
# MAGIC ## ingest circuits.csv to silver layer 

# COMMAND ----------

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

raw_uri = f"{raw_abfss}//{v_file_date}//circuits.csv"

# COMMAND ----------

# display(spark.read.csv(raw_uri))

# COMMAND ----------

# df_circuits = spark.read.csv(raw_uri, header=True) # 1 jobs

# COMMAND ----------

# df_circuits = spark.read.option("inferSchema", True).csv(raw_uri, header=True) # 2 jobs > 1 jobs => la option "inferSchema" hace ineficiente esta consulta. Además, queremos definir nuestro propio schema de tal manera que si los datos no llegan en el formato correcto, el proceso falle 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType

# COMMAND ----------

circuits_schema = StructType(fields = [StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)                                     
                                     ])

# COMMAND ----------

df_circuits = spark.read.schema(circuits_schema).csv(raw_uri, header=True)

# COMMAND ----------

# df_circuits.show(truncate=False)
display(df_circuits)

# COMMAND ----------

df_circuits.printSchema()

# COMMAND ----------

# df_circuits.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### select only necesary columns...

# COMMAND ----------

df_circuits_selected = df_circuits.select("circuitId",
"circuitRef",
"name",
"location",
"country",
"lat",
"lng",
"alt"
# ,
# "url"
)

# COMMAND ----------

df_circuits_selected = df_circuits.select(df_circuits["circuitId"],
df_circuits["circuitRef"],
df_circuits["name"],
df_circuits["location"],
df_circuits["country"],
df_circuits["lat"],
df_circuits["lng"],
df_circuits["alt"]
# ,
# df_circuits["url"]
)

# COMMAND ----------

df_circuits_selected = df_circuits.select(df_circuits["circuitId"],
df_circuits["circuitRef"],
df_circuits["name"],
df_circuits["location"],
df_circuits["country"],
df_circuits["lat"],
df_circuits["lng"],
df_circuits["alt"]
# ,
# df_circuits["url"]
)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_circuits_selected = df_circuits.select(col("circuitId"),
col("circuitRef"),
col("name"),
col("location"),#.alias('La concha de tu madre'), # en esta opción tengo la posibilidad de renombrar in situ una columna
col("country"),
col("lat"),
col("lng"),
col("alt")
# ,
# col("url")
)

# COMMAND ----------

display(df_circuits_selected)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

df_circuits_renamed = df_circuits_selected.withColumnRenamed("circuitId", "circuit_id")\
    .withColumnRenamed("circuitRef", "circuit_ref")\
    .withColumnRenamed("lat", "latitude")\
    .withColumnRenamed("lng", "longitude")\
    .withColumnRenamed("alt", "altitude")\
    


# COMMAND ----------

df_circuits_renamed.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

df_circuits_final = add_ingestion_date(df_circuits_renamed)\
.withColumn('data_source', lit(v_data_source))\
.withColumn('file_date', lit(v_file_date))
    # .withColumn('env', lit('Production')) # agregar una columna a partir de un valor literal

# COMMAND ----------

display(df_circuits_final)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES;

# COMMAND ----------

df_circuits_final.write.mode('overwrite').format('delta').saveAsTable('f1_silver.circuits')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_silver.circuits

# COMMAND ----------

dbutils.notebook.exit('Success')

# COMMAND ----------

