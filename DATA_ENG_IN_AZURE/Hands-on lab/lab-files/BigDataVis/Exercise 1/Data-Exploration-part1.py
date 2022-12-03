# Databricks notebook source
# MAGIC %md 
# MAGIC ##### Access ADLS Using SAS token
# MAGIC 
# MAGIC Note that the recommended way to access ADLS from Databricks is by using AAD Service Principal and the backed by Azure Key Vault Databricks Secret Scope

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.asastoremcw303474.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.asastoremcw303474.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.asastoremcw303474.dfs.core.windows.net", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### ABFS protocol
# MAGIC 
# MAGIC ABFS protocol (Azure Blob File System) - Azure Blob storage driver for Hadoop required by Spark.
# MAGIC ABFS is part of Apache Hadoop and is included in many of the commercial distributions of Hadoop. It's a recommended protocol today to use when working with ADLS v2
# MAGIC 
# MAGIC The objects in ADLS are represented as URIs with the following URI schema:
# MAGIC 
# MAGIC _abfs[s]://container_name@account_name.dfs.core.windows.net/<path>/<path>/<file_name>_
# MAGIC   
# MAGIC If you add an **_'s'_** at the end (abfss) then the ABFS Hadoop client driver will ALWAYS use Transport Layer Security (TLS) irrespective of the authentication method chosen.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### dbutils
# MAGIC 
# MAGIC Databricks utilities tool
# MAGIC 
# MAGIC You can do many opertations with dbutils, for more details see [dbutils](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils)
# MAGIC 
# MAGIC List the content of ADLS labs-303474 container:

# COMMAND ----------

dbutils.fs.ls("abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Initial data exploration
# MAGIC 
# MAGIC Run sparkSQL directly on the files in ADLS.
# MAGIC 
# MAGIC Later we'll create tables 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv`

# COMMAND ----------

#buil-in _sqldf dataframe created automatically by databricks
_sqldf.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/AirportCodeLocationLookupClean.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightWeatherWithAirportCode.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC select _c25 as airport,  max(_c9) as max_temp_c
# MAGIC from csv.`abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightWeatherWithAirportCode.csv`
# MAGIC group by _c25

# COMMAND ----------

# MAGIC %md 
# MAGIC You can see that data is not clean. There are many invalid values with `M` value

# COMMAND ----------

df = spark.read.csv('abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv', header=True)


# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

display(df.groupBy("Month").count())

# COMMAND ----------

display(df.select("Month").distinct().count())

# COMMAND ----------

# MAGIC %md Check the number of null values in DepDel15. States the departure delay of at least 15 min.
# MAGIC 
# MAGIC If we want to use this field for delay prediction we should fix those records with null values. 

# COMMAND ----------

from pyspark.sql.functions import col
percentage_nulls_in_DepDel15 = df.filter(col("DepDel15").isNull() ).count() / df.count() * 100
print (f"{percentage_nulls_in_DepDel15} % null values in DepDel15 column") 
      
 

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Excersise: 
# MAGIC Run dbutils.summarize for FlightWeatherWithAirportCode file
# MAGIC 
# MAGIC Check which columns have null values

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create schema and tables
# MAGIC 
# MAGIC After we explored the data we create schemas and tables in order to work with the data as we get used to work with data bases

# COMMAND ----------

# MAGIC %sql 
# MAGIC show schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema flights

# COMMAND ----------

# MAGIC %sql
# MAGIC describe schema flights

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/flights.db")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Create external table

# COMMAND ----------

# MAGIC %sql
# MAGIC use flights

# COMMAND ----------

# MAGIC %sql
# MAGIC create table flights_delays_external
# MAGIC using csv options (
# MAGIC   path = 'abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv',
# MAGIC   header = "true");
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flights_delays_external limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select DepDel15, count(*) as number_of_delays
# MAGIC from flights_delays_external
# MAGIC group by DepDel15

# COMMAND ----------

# MAGIC %sql
# MAGIC select DepDel15, count(DepDel15) as number_of_delays
# MAGIC from flights_delays_external
# MAGIC group by DepDel15

# COMMAND ----------

# MAGIC %md
# MAGIC Why results of previous two queries are different?

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct (DepDel15)
# MAGIC from flights_delays_external

# COMMAND ----------

# MAGIC %sql
# MAGIC select count (distinct (DepDel15))
# MAGIC from flights_delays_external

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe extended flights_delays_external

# COMMAND ----------

# MAGIC %md
# MAGIC Pay attention that this table is `External` and table format is `CSV`
# MAGIC Since it's extrernal table, its location is the original one and the data is in the original files.
# MAGIC 
# MAGIC Also you can see that there is no history for non-delta tables

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history flights_delays_external

# COMMAND ----------

# MAGIC %md
# MAGIC What's wrong with this table defintion?
# MAGIC 
# MAGIC Since the original data is in CSV format, Spark can not infer precisely the correct fields types.
# MAGIC 
# MAGIC The better way is to define the types strictly

# COMMAND ----------

# MAGIC %sql drop table flights_delays

# COMMAND ----------

# MAGIC %sql create table flights_delays (
# MAGIC   year int,
# MAGIC   month int,
# MAGIC   dayofmonth int,
# MAGIC   dayofweek int,
# MAGIC   carrier string,
# MAGIC   crsdeptime string,
# MAGIC   depdelay int,
# MAGIC   depdel15 int,
# MAGIC   crsarrTime string,
# MAGIC   arrdelay int,
# MAGIC   arrdel15 int,
# MAGIC   canceled smallint,
# MAGIC   originairportcode string,
# MAGIC   originairportname string,
# MAGIC   originlatitude float,
# MAGIC   originlongitude float,
# MAGIC   destairportcode string,
# MAGIC   destairportname string,
# MAGIC   destlatitude float,
# MAGIC   destlongitude float
# MAGIC ) using csv options (
# MAGIC   path = 'abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv',
# MAGIC   header = "true"
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flights_delays limit 10

# COMMAND ----------

# MAGIC %sql describe extended  flights_delays

# COMMAND ----------

# MAGIC %md
# MAGIC #### CTEs
# MAGIC 
# MAGIC Spark supports CTEs - Common Table Expessions.
# MAGIC 
# MAGIC CTEs defines a temporary result set that can be refernced multiple times in other queries 
# MAGIC 
# MAGIC In order to define CTE - you use `WITH` clause

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC   (
# MAGIC     with flights_dep_delays (origin_airport, dep_total_delays) as (
# MAGIC       select
# MAGIC         originairportname,
# MAGIC         sum(depdel15) as origindelay15min
# MAGIC       from
# MAGIC         flights_delays
# MAGIC       group by
# MAGIC         originairportname
# MAGIC     )
# MAGIC     select
# MAGIC       max(dep_total_delays) as total_dep_delays
# MAGIC     from
# MAGIC       flights_dep_delays
# MAGIC   )

# COMMAND ----------

# MAGIC %sql with flights_dep_delays (origin_airport, dep_total_delays) as (
# MAGIC   select
# MAGIC     originairportname,
# MAGIC     sum(depdel15) as origindelay15min
# MAGIC   from
# MAGIC     flights_delays
# MAGIC   group by
# MAGIC     originairportname
# MAGIC )
# MAGIC select
# MAGIC   origin_airport,
# MAGIC   dep_total_delays
# MAGIC from
# MAGIC   flights_dep_delays
# MAGIC order by dep_total_delays desc

# COMMAND ----------

# MAGIC %md
# MAGIC Let's create other two table with correct field types

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/AirportCodeLocationLookupClean.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Task TODO create external table: airports_codes_locations
# MAGIC create table airports_codes_locations(
# MAGIC airportid int,
# MAGIC airport string,
# MAGIC displayairportname string,
# MAGIC latitude float,
# MAGIC longitude float
# MAGIC )
# MAGIC using csv
# MAGIC options (
# MAGIC header = "true",
# MAGIC path = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/AirportCodeLocationLookupClean.csv"
# MAGIC )

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from airports_codes_locations limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightWeatherWithAirportCode.csv` limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC create table flights_with_weather( 
# MAGIC year int,
# MAGIC month int,
# MAGIC day int,
# MAGIC time int,
# MAGIC timezone int,
# MAGIC skycondition string,
# MAGIC visibility float,
# MAGIC weathertype string,
# MAGIC drybulbfarenheit float,
# MAGIC drybulbcelsius float,
# MAGIC wetbulbfarenheit float,
# MAGIC wetbulbcelsius float,
# MAGIC dewpointfarenheit float,
# MAGIC dewpointcelsius float,
# MAGIC relativehumidity int,
# MAGIC windspeed int,
# MAGIC winddirection int,
# MAGIC valueforwindcharacter int,
# MAGIC stationpressure float,
# MAGIC pressuretendency int,
# MAGIC pressurechange int,
# MAGIC sealevelpressure float,
# MAGIC recordtype string,
# MAGIC hourlyprecip string,
# MAGIC altimeter float,
# MAGIC airportcode string,
# MAGIC displayairportname string,
# MAGIC latitude float,
# MAGIC longitude float
# MAGIC )
# MAGIC using csv
# MAGIC options (
# MAGIC header = "true",
# MAGIC path = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightWeatherWithAirportCode.csv"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flights_with_weather limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flights_with_weather

# COMMAND ----------

# MAGIC %md
# MAGIC Create Delta Lake tables with CTAS (Create Table as Select)

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table flights_with_weather_delta as select * from flights_with_weather

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flights_with_weather_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended flights_with_weather_delta

# COMMAND ----------

# MAGIC %md
# MAGIC Note that CTAS doesn't support schema definition (it infers the schema from query results)
# MAGIC This could be a problem when you create a delta table from CSV or json
# MAGIC In this case you can create a temp view, define schema for it and then run CTAS
# MAGIC For example

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view flights_with_weather_temp_view( 
# MAGIC year int,
# MAGIC month int,
# MAGIC day int,
# MAGIC time int,
# MAGIC timezone int,
# MAGIC skycondition string,
# MAGIC visibility float,
# MAGIC weathertype string,
# MAGIC drybulbfarenheit float,
# MAGIC drybulbcelsius float,
# MAGIC wetbulbfarenheit float,
# MAGIC wetbulbcelsius float,
# MAGIC dewpointfarenheit float,
# MAGIC dewpointcelsius float,
# MAGIC relativehumidity int,
# MAGIC windspeed int,
# MAGIC winddirection int,
# MAGIC valueforwindcharacter int,
# MAGIC stationpressure float,
# MAGIC pressuretendency int,
# MAGIC pressurechange int,
# MAGIC sealevelpressure float,
# MAGIC recordtype string,
# MAGIC hourlyprecip string,
# MAGIC altimeter float,
# MAGIC airportcode string,
# MAGIC displayairportname string,
# MAGIC latitude float,
# MAGIC longitude float
# MAGIC )
# MAGIC using csv
# MAGIC options (
# MAGIC header = "true",
# MAGIC path = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightWeatherWithAirportCode.csv"
# MAGIC )

# COMMAND ----------

# MAGIC %md 
# MAGIC Now run CTAS- create managed table from temp view

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table flights_with_weather_delta_from_view as select * from flights_with_weather_temp_view

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe extended flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %md
# MAGIC Enriching data with additional meta-data like ingestion time

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table flights_with_weather_delta_from_view as select current_timestamp() as ingestiontime, * from flights_with_weather_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flights_with_weather_delta_from_view limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC drop  view incremental_flights_with_weather_data

# COMMAND ----------

# MAGIC %sql
# MAGIC create temp view incremental_flights_with_weather_data as select *, 1 as dirty_column from flights_with_weather_delta_from_view where airportcode == "SJU"

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from incremental_flights_with_weather_data limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from incremental_flights_with_weather_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into flights_with_weather_delta_from_view (select * from incremental_flights_with_weather_data)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled",  "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into flights_with_weather_delta_from_view (select * from incremental_flights_with_weather_data)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %sql select * from flights_with_weather_delta_from_view limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flights_with_weather_delta_from_view where airportcode=='SJU'

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO flights_with_weather_delta_from_view a
# MAGIC USING incremental_flights_with_weather_data b
# MAGIC ON a.airportcode = b.airportcode
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET a.dirty_column=10
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql describe extended flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flights_with_weather_delta_from_view version as of  0

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table flights_with_weather_delta_from_view version as of 0

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history flights_with_weather_delta_from_view

# COMMAND ----------


