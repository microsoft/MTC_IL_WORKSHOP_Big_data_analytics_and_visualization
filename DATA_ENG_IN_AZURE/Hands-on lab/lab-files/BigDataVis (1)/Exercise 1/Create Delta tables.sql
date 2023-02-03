-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Excersise. Build Bronze Data Lake layer.
-- MAGIC 
-- MAGIC Create 3 extrenal delta tables: flight_delay_bronze, airport_code_location_bronze, flight_with_weather_bronze.
-- MAGIC 
-- MAGIC Use temp view with inferSchema = "true"to load original CSV files from the FlightsDelays folder and then use CTAS to create Delta Lake tables in ./bronze folder
-- MAGIC 
-- MAGIC Every table should be located in the dedicated folder. For example table flight_delay_bronze will be in abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelay folder

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.text("storage_account", "")
-- MAGIC dbutils.widgets.text("container_name", "")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC storage_account = getArgument("storage_account")
-- MAGIC container_name = getArgument("container_name")
-- MAGIC print (storage_account)
-- MAGIC print (container_name)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
-- MAGIC spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
-- MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "")

-- COMMAND ----------

use flights

-- COMMAND ----------

create
or replace temp view flight_delay_bronze_view using csv options (
  header = "true",
  inferSchema = "true",
  path = "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv"
)

-- COMMAND ----------

create table flight_delay_bronze
using delta options(
'path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelay/'
)
as 
select * from flight_delay_bronze_view


-- COMMAND ----------

select * from flight_delay_bronze

-- COMMAND ----------

describe extended  flight_delay_bronze

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelays"))%python 
-- MAGIC display(dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightDelays"))

-- COMMAND ----------

create or replace temp view airport_code_location_bronze_view
using csv options (
  header = "true",
  inferSchema = "true",
  path = "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/AirportCodeLocationLookupClean.csv"
)

-- COMMAND ----------

create table airport_code_location_bronze
options(
'path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/AirportCodeLocation/'
)
as 
select * from airport_code_location_bronze_view

-- COMMAND ----------

select * from airport_code_location_bronze

-- COMMAND ----------

create or replace temp view flight_with_weather_bronze_view
using csv options (
  header = "true",
  inferSchema = "true",
  path = "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/FlightWeatherWithAirportCode.csv"
)

-- COMMAND ----------

create table flight_with_weather_bronze 
options(
  path = "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/FlightWithWeather/"
)
as select * from flight_with_weather_bronze_view

-- COMMAND ----------

show tables 

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC display(dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/bronze/"))

-- COMMAND ----------


