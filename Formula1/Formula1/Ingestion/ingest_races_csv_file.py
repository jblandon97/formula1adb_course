# Databricks notebook source
# MAGIC %md
# MAGIC ## ingest races.csv to silver layer 

# COMMAND ----------

dbutils.widgets.text('p_data_source', '')
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', '')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "/Formula1/Ingestion/includes/configuration" 

# COMMAND ----------

# MAGIC %run "/Formula1/Ingestion/includes/functions" 

# COMMAND ----------

raw_uri = f"{raw_abfss}//{v_file_date}//races.csv"

# COMMAND ----------

# display(spark.read.csv(raw_uri))

# COMMAND ----------

# df_races = spark.read.csv(raw_uri, header=True) # 1 jobs

# COMMAND ----------

# df_races = spark.read.option("inferSchema", True).csv(raw_uri, header=True) # 2 jobs > 1 jobs => la option "inferSchema" hace ineficiente esta consulta. Además, queremos definir nuestro propio schema de tal manera que si los datos no llegan en el formato correcto, el proceso falle 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, StringType, DateType, TimestampType

# COMMAND ----------

races_schema = StructType(
    fields=[
        StructField("raceId", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("circuitId", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("date", DateType(), True),
        StructField("time", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

df_races = spark.read.schema(races_schema).csv(raw_uri, header=True)


# COMMAND ----------

# df_races.show(truncate=False)
display(df_races)

# COMMAND ----------

# df_races.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### select only necesary columns...

# COMMAND ----------

df_races.columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_races_selected = df_races.select(col('raceId'), col('year'), col('round'), col('circuitId'), col('name'), col('date'), col('time')
# ,
# "url"
)

# COMMAND ----------

display(df_races_selected)

# COMMAND ----------

df_races_renamed =  df_races_selected.withColumnRenamed('raceId', 'race_id').withColumnRenamed('year', 'race_year').withColumnRenamed('circuitId', 'circuit_id')


# COMMAND ----------

df_races_renamed.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, to_timestamp, coalesce

# COMMAND ----------

df_races_final = (
    add_ingestion_date(
        df_races_renamed.withColumn(
            "rice_timestamp",
            coalesce(
                to_timestamp(
                    concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"
                ),
                col("date"),
            ),
        )
    )
    .withColumn("data_source", lit(v_data_source))
    .withColumn("file_date", lit(v_file_date))
)

# COMMAND ----------

display(df_races_final)

# COMMAND ----------

df_races_final.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### with partitionBy

# COMMAND ----------

df_races_final.write.option("mergeSchema", "true").partitionBy('race_year').mode('overwrite').\
    format('delta').saveAsTable('f1_silver.races')

# COMMAND ----------

dbutils.notebook.exit('Success')