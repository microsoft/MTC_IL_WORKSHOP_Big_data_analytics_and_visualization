-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ##### Define notebook variables with widgets
-- MAGIC 
-- MAGIC Widgets is a great way to use variables in the notebook session across all cells

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

-- MAGIC %md
-- MAGIC ##### Access ADLS Using SAS token
-- MAGIC 
-- MAGIC Note that the recommended way to access ADLS from Databricks is by using AAD Service Principal and the backed by Azure Key Vault Databricks Secret Scope.
-- MAGIC 
-- MAGIC Here for simplicity we use SAS token.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
-- MAGIC spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
-- MAGIC spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", "")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### dbutils
-- MAGIC 
-- MAGIC Databricks utilities tool
-- MAGIC 
-- MAGIC You can do many opertations with dbutils, for more details see [dbutils](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils)
-- MAGIC 
-- MAGIC List the content of ADLS folder:

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls( f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/FlightsDelays/"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Initial data exploration
-- MAGIC 
-- MAGIC Run sparkSQL directly on the files in ADLS.
-- MAGIC 
-- MAGIC Later we'll create schemas and tables 

-- COMMAND ----------



select * from csv.`abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv`


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's check other two tables

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from csv.`abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/AirportCodeLocationLookupClean.csv`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from csv.`abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/FlightWeatherWithAirportCode.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create schema and tables
-- MAGIC 
-- MAGIC After we explored the data we create schemas and tables in order to work with the data as we get used to work with regular data bases

-- COMMAND ----------

create schema flights

-- COMMAND ----------

show schemas

-- COMMAND ----------

describe schema flights

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Create external table
-- MAGIC We create a table in `flights` schema, which we created previously.
-- MAGIC 
-- MAGIC Notice using ***_header_*** option, which allows to define columns names from the header

-- COMMAND ----------

use flights

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC 
-- MAGIC ###### Note! Please add to the table names unique prefix, example 5 digits of your ID

-- COMMAND ----------

-- Note we use header = "true"

create table flights_delays_external_303474
using csv options (
  path = 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv',
  header = "true");
  

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC show tables

-- COMMAND ----------

describe extended flights_delays_external_303474

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Pay attention that this table is ***External*** and the table format is CSV. Since it's an extrernal table, the location is the original one: the ADLS container.
-- MAGIC 
-- MAGIC Also you can see that there is no history for non-delta tables.
-- MAGIC 
-- MAGIC The history is supported by Delta format in our case the table format is CSV

-- COMMAND ----------

-- History works only with Delta Lake format, this table is CSV format
describe history flights_delays_external_303474

-- COMMAND ----------

-- MAGIC %md Now we can work with this table as we get used working with regular tables...

-- COMMAND ----------

select * from flights_delays_external_303474 limit 10

-- COMMAND ----------

select DepDel15, count(*) as number_of_delays
from flights_delays_external_303474
group by DepDel15

-- COMMAND ----------

-- Note distinct counts null values

select distinct (DepDel15)
from flights_delays_external_303474

-- COMMAND ----------

-- Count distinct doesn't count null values

select count (distinct (DepDel15)) as DepDel15_Valid
from flights_delays_external_303474

-- COMMAND ----------

describe table flights_delays_external_303474

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What's wrong with this table defintion?
-- MAGIC 
-- MAGIC Since the original data is in CSV format, Spark can not infer precisely the correct fields types (by default). 
-- MAGIC 
-- MAGIC You can enforce schema inference also for CSV, JSON formats by setting format option : `inferSchema=true`, see below examples.
-- MAGIC 
-- MAGIC However the recomended way is to create table and define the types strictly.

-- COMMAND ----------

create table flights_delays_external_typed_303474 (
  year int,
  month int,
  dayofmonth int,
  dayofweek int,
  carrier string,
  crsdeptime string,
  depdelay int,
  depdel15 int,
  crsarrTime string,
  arrdelay int,
  arrdel15 int,
  canceled smallint,
  originairportcode string,
  originairportname string,
  originlatitude float,
  originlongitude float,
  destairportcode string,
  destairportname string,
  destlatitude float,
  destlongitude float
) using csv options (
  path = 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv',
  header = "true"
);

-- COMMAND ----------

-- With "inferSchema" = "true"

create table flights_delays_external_typed_infered_303474 (
) using csv options (
  path = 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv',
  header = "true",
  "inferSchema" = "true"
);

-- COMMAND ----------

describe extended  flights_delays_external_typed_303474

-- COMMAND ----------

describe extended flights_delays_external_typed_infered_303474

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CTEs
-- MAGIC 
-- MAGIC Spark supports CTEs - Common Table Expessions.
-- MAGIC 
-- MAGIC CTEs defines a temporary result set that can be refernced multiple times in other queries 
-- MAGIC 
-- MAGIC In order to define CTE - you use `WITH` clause

-- COMMAND ----------

 with flights_dep_delays (origin_airport, dep_total_delays) as (
  select
    originairportname,
    sum(depdel15) as origindelay15min
  from
    flights_delays_external_typed_303474
  group by
    originairportname
)

select
  origin_airport,
  dep_total_delays
from
  flights_dep_delays
order by dep_total_delays desc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Create Table As Select (CTAS)
-- MAGIC 
-- MAGIC Create Delta Lake tables with CTAS

-- COMMAND ----------

create
or replace table flights_delays_managed_delta_303474 as
select
  *
from
  flights_delays_external_typed_303474

-- COMMAND ----------

describe extended flights_delays_managed_delta_303474

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now the table type is `Managed`.  By default, if we don't specify the output format,  the tables are created in Delta format.
-- MAGIC 
-- MAGIC For managed tables, the data is copied from original location (ADLS container) to the `schema` location in dbfs (Databricks file system). 
-- MAGIC 
-- MAGIC The Databricks File System (DBFS) is a distributed file system mounted into an Azure Databricks workspace and available on Azure Databricks clusters. 
-- MAGIC 
-- MAGIC If you delete the table all this data will be deleted as well (not the case with `External` tables)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Let's structure of Delta table
-- MAGIC display(dbutils.fs.ls("dbfs:/user/hive/warehouse/flights.db/flights_delays_managed_delta_303474/"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Note that CTAS syntax doesn't support schema definition (it infers the schema from query results)
-- MAGIC 
-- MAGIC The recommended way to overcome it is to create a temp view, define schema for it and then run CTAS (similar as we did before but we used intermediate tables rather than temp views)
-- MAGIC 
-- MAGIC Temp view exists only during Spark Session.
-- MAGIC 
-- MAGIC For example

-- COMMAND ----------

-- MAGIC %sql create
-- MAGIC or replace temp view flights_delays_temp_view_303474(
-- MAGIC   year int,
-- MAGIC   month int,
-- MAGIC   day int,
-- MAGIC   time int,
-- MAGIC   timezone int,
-- MAGIC   skycondition string,
-- MAGIC   visibility string,
-- MAGIC   weathertype string,
-- MAGIC   drybulbfarenheit string,
-- MAGIC   drybulbcelsius float,
-- MAGIC   wetbulbfarenheit float,
-- MAGIC   wetbulbcelsius float,
-- MAGIC   dewpointfarenheit float,
-- MAGIC   dewpointcelsius float,
-- MAGIC   relativehumidity int,
-- MAGIC   windspeed int,
-- MAGIC   winddirection int,
-- MAGIC   valueforwindcharacter int,
-- MAGIC   stationpressure float,
-- MAGIC   pressuretendency int,
-- MAGIC   pressurechange int,
-- MAGIC   sealevelpressure float,
-- MAGIC   recordtype string,
-- MAGIC   hourlyprecip string,
-- MAGIC   altimeter float,
-- MAGIC   airportcode string,
-- MAGIC   displayairportname string,
-- MAGIC   latitude float,
-- MAGIC   longitude float
-- MAGIC ) using csv options (
-- MAGIC   header = "true",
-- MAGIC   path = "abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/FlightWeatherWithAirportCode.csv"
-- MAGIC )

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Now run CTAS, which creates managed table from temp view

-- COMMAND ----------

-- MAGIC %sql create
-- MAGIC or replace table flights_delays_303474 as
-- MAGIC select
-- MAGIC   *
-- MAGIC from
-- MAGIC   flights_delays_temp_view_303474

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can create a final table in the specific location by using ***options ('path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/new table location/')***

-- COMMAND ----------

-- MAGIC %sql create
-- MAGIC or replace table flights_delays_with_path_option_303474
-- MAGIC using delta options('path' 'abfss://${container_name}@${storage_account}.dfs.core.windows.net/FlightsDelays/bronze/tableName/')
-- MAGIC as
-- MAGIC select
-- MAGIC   *
-- MAGIC from
-- MAGIC   flights_delays_temp_view_303474

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC #### Enriching data with additional meta-data

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC Using built-in `current_timestamp()` function
-- MAGIC 
-- MAGIC There are many others, more details [databricks built-in functions](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/current_timestamp)

-- COMMAND ----------

-- MAGIC %sql create
-- MAGIC or replace table flights_delays_enriched_303474 as
-- MAGIC select
-- MAGIC   current_timestamp() as ingestiontime,
-- MAGIC   current_user as user,
-- MAGIC   *
-- MAGIC from
-- MAGIC   flights_delays_temp_view_303474

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select * from flights_delays_enriched_303474 limit 10
