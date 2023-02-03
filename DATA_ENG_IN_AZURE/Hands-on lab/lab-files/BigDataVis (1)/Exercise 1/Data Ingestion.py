# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ##### Data Ingestion #####
# MAGIC Very often there is a need to add data incrementally to the existing tables
# MAGIC There are few ways to achieve it in Spark by using one of those:
# MAGIC 1. `INSERT INTO`
# MAGIC 2. `COPY INTO`
# MAGIC 3. `MERGE INTO`

# COMMAND ----------

# MAGIC %sql create or replace temp view incremental_flights_with_weather_data as
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   flights_with_weather_delta_from_view
# MAGIC where
# MAGIC   airportcode == "SJU"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %md
# MAGIC INSERT INTO

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into
# MAGIC   flights_with_weather_delta_from_view (
# MAGIC     select
# MAGIC       *
# MAGIC     from
# MAGIC       incremental_flights_with_weather_data
# MAGIC   )

# COMMAND ----------

# MAGIC %md
# MAGIC Run the previous `INSERT INTO` one more time, you will succeed, effectively duplicating the data. The meaning is `INSERT INTO` operation **is not idempotent**.

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### INSERT INTO vs. COPY INTO
# MAGIC 
# MAGIC Important: COPY INTO **is idempotent operation** and so recommended one for the incremental data load. 
# MAGIC 
# MAGIC It will help you to avoid data duplication in the target tables.
# MAGIC 
# MAGIC Let's see it in action

# COMMAND ----------

# MAGIC %sql create table airports_codes_locations_stg(
# MAGIC   airportid int,
# MAGIC   airport string,
# MAGIC   displayairportname string,
# MAGIC   latitude double,
# MAGIC   longitude double
# MAGIC ) 

# COMMAND ----------

# MAGIC %sql copy into airports_codes_locations_stg
# MAGIC from
# MAGIC   "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/AirportCodeLocationLookupClean.csv" 
# MAGIC     fileformat = CSV 
# MAGIC     format_options (
# MAGIC     "mergeSchema" = "true",
# MAGIC     "header" = "true",
# MAGIC     "inferSchema" = "true"
# MAGIC   ) copy_options ("mergeSchema" = "true")

# COMMAND ----------

# MAGIC %sql select count(*) from airports_codes_locations_stg

# COMMAND ----------

# MAGIC %md 
# MAGIC Run once again the previous `COPY INTO`- zero records will be inserted. 
# MAGIC 
# MAGIC You can see the second running for the same data afffects zero rows and inserts zero.  In other words `INSERT INTO` is idempotent operation 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### MERGE INTO

# COMMAND ----------

# MAGIC %sql create or replace temp view incremental_flights_with_weather_data_atl as
# MAGIC select
# MAGIC  *
# MAGIC from
# MAGIC   flights_with_weather_delta_from_view
# MAGIC where
# MAGIC   airportcode == "ATL"

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO flights_with_weather_delta_from_view a
# MAGIC USING incremental_flights_with_weather_data_atl b
# MAGIC ON a.airportcode = b.airportcode
# MAGIC WHEN NOT MATCHED THEN INSERT *
