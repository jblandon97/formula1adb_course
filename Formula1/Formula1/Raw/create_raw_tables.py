# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE f1_raw;

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXTERNAL RAW TABLES
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.circuits_csv;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.circuits_csv
# MAGIC (
# MAGIC circuitId INT,
# MAGIC circuitRef STRING,
# MAGIC name STRING,
# MAGIC location STRING,
# MAGIC country STRING,
# MAGIC lat DOUBLE,
# MAGIC lng DOUBLE,
# MAGIC alt INT,
# MAGIC url STRING
# MAGIC )
# MAGIC USING CSV OPTIONS ("path" = "abfss://raw@formula1adlg2.dfs.core.windows.net//circuits.csv", 'header'= TRUE)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.circuits_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.races_csv; 
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.races_csv
# MAGIC (
# MAGIC         raceId INT, 
# MAGIC         year INT, 
# MAGIC         round INT, 
# MAGIC         circuitId INT, 
# MAGIC         name STRING, 
# MAGIC         date DATE, 
# MAGIC         time STRING, 
# MAGIC         url STRING
# MAGIC )
# MAGIC USING CSV OPTIONS ("path" = "abfss://raw@formula1adlg2.dfs.core.windows.net//races.csv", 'header'=TRUE)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.races_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.constructors_json; 
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.constructors_json
# MAGIC (
# MAGIC         constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING
# MAGIC )
# MAGIC USING JSON OPTIONS ("path" = "abfss://raw@formula1adlg2.dfs.core.windows.net//constructors.json", 'header'=TRUE)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.constructors_json; 
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.constructors_json
# MAGIC (
# MAGIC         constructorId INT,constructorRef STRING,name STRING,nationality STRING,url STRING
# MAGIC )
# MAGIC USING JSON OPTIONS ("path" = "abfss://raw@formula1adlg2.dfs.core.windows.net//constructors.json", 'header'=TRUE)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.constructors_json;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.drivers_json;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.drivers_json (
# MAGIC   driverId INT,
# MAGIC   driverRef STRING,
# MAGIC   number INT,
# MAGIC   code STRING,
# MAGIC   name STRUCT<forename : STRING, surname : STRING>,
# MAGIC   dob DATE,
# MAGIC   nationality STRING,
# MAGIC   url STRING
# MAGIC ) USING JSON OPTIONS (
# MAGIC   "path" = "abfss://raw@formula1adlg2.dfs.core.windows.net//drivers.json",
# MAGIC   'header' = TRUE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.drivers_json;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.results_json;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.results_json (
# MAGIC                     resultId INT,
# MAGIC                     raceId INT,
# MAGIC                     driverId INT,
# MAGIC                     constructorId INT,
# MAGIC                     number INT,
# MAGIC                     grid INT,
# MAGIC                     position INT,
# MAGIC                     positionText STRING,
# MAGIC                     positionOrder INT,
# MAGIC                     points FLOAT,
# MAGIC                     laps INT,
# MAGIC                     time STRING,
# MAGIC                     milliseconds INT,
# MAGIC                     fastestLap INT,
# MAGIC                     rank INT,
# MAGIC                     fastestLapTime STRING,
# MAGIC                     fastestLapSpeed FLOAT,
# MAGIC                     statusId STRING
# MAGIC ) USING JSON OPTIONS (
# MAGIC   "path" = "abfss://raw@formula1adlg2.dfs.core.windows.net//results.json",
# MAGIC   'header' = TRUE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.results_json;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.pit_stops_json;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.pit_stops_json (
# MAGIC  raceId        INT,
# MAGIC  driverId      INT,
# MAGIC  stop          INT,
# MAGIC  lap           INT,
# MAGIC  time          STRING   ,
# MAGIC  duration      STRING,
# MAGIC  milliseconds  INT     
# MAGIC ) USING JSON OPTIONS (
# MAGIC   "path" = "abfss://raw@formula1adlg2.dfs.core.windows.net//pit_stops.json",
# MAGIC   'header' = TRUE,
# MAGIC   'multiline' = TRUE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.pit_stops_json;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.lap_times_csv;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.lap_times_csv (
# MAGIC  raceId        INT,
# MAGIC  driverId      INT,
# MAGIC  lap           INT,
# MAGIC  position      INT,
# MAGIC  time          STRING,
# MAGIC  milliseconds  INT     
# MAGIC ) USING CSV OPTIONS (
# MAGIC   "path" = "abfss://raw@formula1adlg2.dfs.core.windows.net//lap_times//",
# MAGIC   'header' = TRUE
# MAGIC   -- 'multiline' = TRUE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.lap_times_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_raw.qualifying_json;
# MAGIC CREATE TABLE IF NOT EXISTS f1_raw.qualifying_json (
# MAGIC qualifyId INT, 
# MAGIC raceId INT, 
# MAGIC driverId INT, 
# MAGIC constructorId INT, 
# MAGIC number INT, 
# MAGIC position INT, 
# MAGIC q1 STRING, 
# MAGIC q2 STRING, 
# MAGIC q3 STRING
# MAGIC ) USING JSON OPTIONS (
# MAGIC   "path" = "abfss://raw@formula1adlg2.dfs.core.windows.net//qualifying//",
# MAGIC   'header' = TRUE,
# MAGIC   'multiline' = TRUE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_raw.qualifying_json;

# COMMAND ----------

