# Databricks notebook source
list_file_date = []
for i in dbutils.fs.ls('abfss://raw@formula1adlg2.dfs.core.windows.net//'):
    list_file_date.append(i.name[:-1])

# COMMAND ----------

for file_date in list_file_date:
    v_success = dbutils.notebook.run('/Workspace/Formula1/Ingestion/ingest_circuits_csv_file', 0, \
        {
            "p_data_source": 'Ergast API',
            "p_file_date": file_date
        })
    print(v_success)

# COMMAND ----------

for file_date in list_file_date:   
    v_success = dbutils.notebook.run('/Workspace/Formula1/Ingestion/ingest_constructors_json_file', 0, \
        {
            "p_data_source": 'Ergast API',
            "p_file_date": file_date
        })
    print(v_success)

# COMMAND ----------

for file_date in list_file_date:       
    v_success = dbutils.notebook.run('/Workspace/Formula1/Ingestion/ingest_drivers_json_file', 0, \
        {
            "p_data_source": 'Ergast API',
            "p_file_date": file_date
        })
    print(v_success)

# COMMAND ----------

for file_date in list_file_date:      
    v_success = dbutils.notebook.run('/Workspace/Formula1/Ingestion/ingest_lap_times_csv_file', 0, \
        {
            "p_data_source": 'Ergast API',
            "p_file_date": file_date
        })
    print(v_success)

# COMMAND ----------

for file_date in list_file_date:     
    v_success = dbutils.notebook.run('/Workspace/Formula1/Ingestion/ingest_constructors_json_file', 0, \
        {
            "p_data_source": 'Ergast API',
            "p_file_date": file_date
        })
    print(v_success)

# COMMAND ----------

for file_date in list_file_date:       
    v_success = dbutils.notebook.run('/Workspace/Formula1/Ingestion/ingest_pit_stops_json_file', 0, \
        {
            "p_data_source": 'Ergast API',
            "p_file_date": file_date
        })
    print(v_success)

# COMMAND ----------

for file_date in list_file_date:    
    v_success = dbutils.notebook.run('/Workspace/Formula1/Ingestion/ingest_qualifying_json_file', 0, \
        {
            "p_data_source": 'Ergast API',
            "p_file_date": file_date
        })
    print(v_success)

# COMMAND ----------

for file_date in list_file_date:   
    v_success = dbutils.notebook.run('/Workspace/Formula1/Ingestion/ingest_races_csv_file', 0, \
        {
            "p_data_source": 'Ergast API',
            "p_file_date": file_date
        })
    print(v_success)

# COMMAND ----------

for file_date in list_file_date:     
    v_success = dbutils.notebook.run('/Workspace/Formula1/Ingestion/ingest_results_json_file', 0, \
        {
            "p_data_source": 'Ergast API',
            "p_file_date": file_date
        })
    print(v_success)