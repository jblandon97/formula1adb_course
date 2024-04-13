# Databricks notebook source
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

schema = "driverId INT, driverRef STRING ,number INT ,code STRING ,name STRING ,dob DATE ,nationality STRING ,url STRING"

# COMMAND ----------

schema_json =  "forename STRING, surname STRING"

# COMMAND ----------

raw_uri = f"{raw_abfss}//{v_file_date}//drivers.json"

# COMMAND ----------

df_drivers = spark.read.schema(schema).json(
    raw_uri
)

# COMMAND ----------

from pyspark.sql.functions import from_json

# COMMAND ----------

df_drivers_with_namesch = df_drivers.withColumnRenamed('driverId', 'driver_id')\
                                    .withColumnRenamed('driverRef', 'driver_ref')\
                                    .withColumn('name', from_json(df_drivers.name, schema_json)) 

# COMMAND ----------

df_drivers_with_namesch.printSchema()

# COMMAND ----------

display(df_drivers_with_namesch)

# COMMAND ----------

df_drivers_dropped = df_drivers_with_namesch.drop('url')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col, concat

# COMMAND ----------

df_drivers_final = (
    add_ingestion_date(
        df_drivers_dropped.withColumn(
            "name", concat(col("name.forename"), lit(" "), col("name.surname"))
        )
    )
    .withColumn("data_source", lit(v_data_source))
    .withColumn("file_date", lit(v_file_date))
)

# COMMAND ----------

display(df_drivers_final)

# COMMAND ----------

df_drivers_final.write.mode('overwrite').format('delta').saveAsTable('f1_silver.drivers')

# COMMAND ----------

dbutils.notebook.exit('Success')