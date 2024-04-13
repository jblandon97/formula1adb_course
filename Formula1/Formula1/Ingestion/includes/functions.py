# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(df_in):
    df_out = df_in.withColumn('ingest_date', current_timestamp())
    return df_out

# COMMAND ----------

def join_dataframes(df1, df2, key, how):
    df_join = df1.join(df2, df1[key] == df2[key], how=how)
    return df_join


# COMMAND ----------

def select_columns_and_partition(df, partition_column):
    list_columns = []
    for i in df.columns:
        if i == partition_column: 
            continue
        list_columns.append(i)
    list_columns.append(partition_column)
    return df.select(list_columns)


# COMMAND ----------

def incremental_load(df, partition_column, database, table):
    df = select_columns_and_partition(df, partition_column)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if spark._jsparkSession.catalog().tableExists(f"{database}.{table}"):
        df.write.mode("overwrite").insertInto(f"{database}.{table}")
    else:
        df.write.mode("overwrite").partitionBy(partition_column).format(
            "parquet"
        ).saveAsTable(
            f"{database}.{table}"
        )  # primera vez

# COMMAND ----------

def merge_detla_table(database, table, path, input_df, merge_condition, partition_col):
    from delta.tables import DeltaTable 
    spark.conf.set(
        "spark.sql.optimizer.dynamicPartitionPruning.enabled", "true"
    )  # conf. necesaria para que de acuerdo a la partición, busque en la carpeta correspondiente. Si se quema el valor de race_id, no es necesaria esto.
    if spark._jsparkSession.catalog().tableExists(f"{database}.{table}"):
        # df.write.mode("overwrite").insertInto("f1_silver.results")
        deltaTable = DeltaTable.forPath(spark, path)

        deltaTable.alias("tgt").merge(
            input_df.alias("src"),
            merge_condition,  # para que no escanee todas las particiones en silver_uri
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    else:
        input_df.write.mode("overwrite").partitionBy(partition_col).format(
            "delta"
        ).saveAsTable(
            f"{database}.{table}"
        )  #

# COMMAND ----------

