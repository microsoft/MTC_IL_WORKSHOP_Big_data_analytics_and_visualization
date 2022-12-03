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
# MAGIC ##### `Excersise` 
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
# MAGIC We create a table in `flights` schema, which we created previously.
# MAGIC 
# MAGIC Notice using ***_header_*** option, which allows to define columns names from the header

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
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe extended flights_delays_external

# COMMAND ----------

# MAGIC %md
# MAGIC Look at table `Location` property. 
# MAGIC 
# MAGIC Where does it point to?
# MAGIC 
# MAGIC Pay attention that this table is External and the table format is CSV. Since it's an extrernal table, the location is the original one: the ADLS container.
# MAGIC 
# MAGIC Also you can see that there is no history for non-delta tables.
# MAGIC 
# MAGIC The history supported by Delta format.

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history flights_delays_external

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
# MAGIC describe table flights_delays_external

# COMMAND ----------

# MAGIC %md
# MAGIC What's wrong with this table defintion?
# MAGIC 
# MAGIC Since the original data is in CSV format, Spark can not infer precisely the correct fields types.
# MAGIC 
# MAGIC The better way to create table, is to define the types strictly

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
# MAGIC Let's create other two table with explicit field types defintion

# COMMAND ----------

# MAGIC %sql -- Task TODO create external table: airports_codes_locations
# MAGIC create table airports_codes_locations(
# MAGIC   airportid int,
# MAGIC   airport string,
# MAGIC   displayairportname string,
# MAGIC   latitude float,
# MAGIC   longitude float
# MAGIC ) using csv options (
# MAGIC   header = "true",
# MAGIC   path = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/AirportCodeLocationLookupClean.csv"
# MAGIC )

# COMMAND ----------

# MAGIC %sql create table flights_with_weather(
# MAGIC   year int,
# MAGIC   month int,
# MAGIC   day int,
# MAGIC   time int,
# MAGIC   timezone int,
# MAGIC   skycondition string,
# MAGIC   visibility float,
# MAGIC   weathertype string,
# MAGIC   drybulbfarenheit float,
# MAGIC   drybulbcelsius float,
# MAGIC   wetbulbfarenheit float,
# MAGIC   wetbulbcelsius float,
# MAGIC   dewpointfarenheit float,
# MAGIC   dewpointcelsius float,
# MAGIC   relativehumidity int,
# MAGIC   windspeed int,
# MAGIC   winddirection int,
# MAGIC   valueforwindcharacter int,
# MAGIC   stationpressure float,
# MAGIC   pressuretendency int,
# MAGIC   pressurechange int,
# MAGIC   sealevelpressure float,
# MAGIC   recordtype string,
# MAGIC   hourlyprecip string,
# MAGIC   altimeter float,
# MAGIC   airportcode string,
# MAGIC   displayairportname string,
# MAGIC   latitude float,
# MAGIC   longitude float
# MAGIC ) using csv options (
# MAGIC   header = "true",
# MAGIC   path = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightWeatherWithAirportCode.csv"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Create Table As Select (CTAS)
# MAGIC 
# MAGIC Create Delta Lake tables with CTAS

# COMMAND ----------

# MAGIC %sql create
# MAGIC or replace table flights_with_weather_delta as
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   flights_with_weather

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended flights_with_weather_delta

# COMMAND ----------

# MAGIC %md
# MAGIC Now the table type is `Managed`. 
# MAGIC 
# MAGIC For managed tables, the data is copied from original location (ADLS container) to the `schema` location in dbfs. 
# MAGIC 
# MAGIC If you delet the table all this data will be deleted as well (not the case with `External` tables)

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/flights.db/flights_with_weather_delta/")

# COMMAND ----------

# MAGIC %md
# MAGIC Note that CTAS syntax doesn't support schema definition (it infers the schema from query results)
# MAGIC 
# MAGIC The recommended way to overcome it is to create a temp view, define schema for it and then run CTAS (similar as we did before but we used intermediate tables rather than temp views)
# MAGIC 
# MAGIC For example

# COMMAND ----------

# MAGIC %sql create
# MAGIC or replace temp view flights_with_weather_temp_view(
# MAGIC   year int,
# MAGIC   month int,
# MAGIC   day int,
# MAGIC   time int,
# MAGIC   timezone int,
# MAGIC   skycondition string,
# MAGIC   visibility string,
# MAGIC   weathertype string,
# MAGIC   drybulbfarenheit string,
# MAGIC   drybulbcelsius float,
# MAGIC   wetbulbfarenheit float,
# MAGIC   wetbulbcelsius float,
# MAGIC   dewpointfarenheit float,
# MAGIC   dewpointcelsius float,
# MAGIC   relativehumidity int,
# MAGIC   windspeed int,
# MAGIC   winddirection int,
# MAGIC   valueforwindcharacter int,
# MAGIC   stationpressure float,
# MAGIC   pressuretendency int,
# MAGIC   pressurechange int,
# MAGIC   sealevelpressure float,
# MAGIC   recordtype string,
# MAGIC   hourlyprecip string,
# MAGIC   altimeter float,
# MAGIC   airportcode string,
# MAGIC   displayairportname string,
# MAGIC   latitude float,
# MAGIC   longitude float
# MAGIC ) using csv options (
# MAGIC   header = "true",
# MAGIC   path = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightWeatherWithAirportCode.csv"
# MAGIC )

# COMMAND ----------

# MAGIC %md 
# MAGIC Now run CTAS, which creates managed table from temp view

# COMMAND ----------

# MAGIC %sql create
# MAGIC or replace table flights_with_weather_delta_from_view as
# MAGIC select
# MAGIC   *
# MAGIC from
# MAGIC   flights_with_weather_temp_view

# COMMAND ----------

# MAGIC %sql describe extended flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %md
# MAGIC ##### `Excersise` 
# MAGIC 
# MAGIC Create 2 additional delta tables: **airports_codes_locations_delta** and **flights_delays_delta** using CTAS techique.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Enriching data with additional meta-data

# COMMAND ----------

# MAGIC %md 
# MAGIC Using built-in `current_timestamp()` function
# MAGIC 
# MAGIC There are many others, more details [databricks built-in functions](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/current_timestamp)

# COMMAND ----------

# MAGIC %sql create
# MAGIC or replace table flights_with_weather_delta_from_view as
# MAGIC select
# MAGIC   current_timestamp() as ingestiontime,
# MAGIC   *
# MAGIC from
# MAGIC   flights_with_weather_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flights_with_weather_delta_from_view limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC Very often there is a need to add data incrementally to the existing tables
# MAGIC There are few ways to achieve it in Spark by using one of those:
# MAGIC 1. `INSERT INTO`
# MAGIC 2. `COPY INTO`
# MAGIC 3. `MERGE INTO`

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC let's simulate incremental load flow
# MAGIC first we create a table: `incremental_flights_with_weather_data` which we want to copy incrementally to the original table:`flights_with_weather_delta_from_view`

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
# MAGIC Run the previous `insert into` one more time, you will succeed. This operation **is not idempotent one**.

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### INSERT INTO vs. COPY INTO
# MAGIC 
# MAGIC Important: COPY INTO **is idempotent operation**

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
# MAGIC select count(*) from incremental_flights_with_weather_data_atl

# COMMAND ----------

# MAGIC %sql select count(*) from flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %sql create or replace table airports_codes_locations_stg(
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
# MAGIC Run once again the previous `Copy Into`- zero records will be inserted. 
# MAGIC 
# MAGIC `COPY INTO` is idempotent operation 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### MERGE INTO

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO flights_with_weather_delta_from_view a
# MAGIC USING incremental_flights_with_weather_data_atl b
# MAGIC ON a.airportcode = b.airportcode
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Delta format
# MAGIC 
# MAGIC Let's explore Delta Lake format
# MAGIC 
# MAGIC Let's see 

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view incremental_flights_with_weather_data_temp_view as
# MAGIC select
# MAGIC   *,
# MAGIC   1 as dirty_column
# MAGIC from
# MAGIC   flights_with_weather_delta_from_view
# MAGIC where
# MAGIC   airportcode == "SJU"

# COMMAND ----------

# MAGIC %md
# MAGIC Note that we changed the schema - the new column `dirty_column` has been added
# MAGIC 
# MAGIC That's the one of great Delta features: schema enforcement. 
# MAGIC The default behaviour is to fail. 
# MAGIC You can enable the insert by allowing ***schema merge***

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into
# MAGIC   flights_with_weather_delta_from_view (
# MAGIC     select
# MAGIC       *
# MAGIC     from
# MAGIC       incremental_flights_with_weather_data_temp_view
# MAGIC   )

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled",  "true")

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
