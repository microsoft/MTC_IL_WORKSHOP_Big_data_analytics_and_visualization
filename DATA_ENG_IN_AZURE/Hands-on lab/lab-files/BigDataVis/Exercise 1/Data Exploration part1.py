# Databricks notebook source
service_credential = dbutils.secrets.get(scope="<DataBricks secrets scope name>",key="<Key Desciption in Key Vault>")  #scope : data-eng-workshop-new-secret_scope-303474 / secret id in key vault!  b456c94f-ed2b-4ded-bcb8-f3502cb2691e

spark.conf.set("fs.azure.account.auth.type.<storage account name>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage account name>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage account name>.dfs.core.windows.net", "<Application (client) ID in AAD>") # application id / cliend id
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage account name>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage account name>.dfs.core.windows.net", "https://login.microsoftonline.com/<AAD teanant ID>") # directory (tenant)id

# COMMAND ----------



# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("data-eng-workshop-secret_scope-303474")

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class":   spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}
dbutils.fs.mount(
  source = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays",
  mount_point = "/mnt/FlightsDelaysMount_02",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("/mnt/FlightsDelaysMount_02/FlightsDelays")

# COMMAND ----------

dbutils.fs.ls("abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from csv.`abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/AirportCodeLocationLookupClean.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from csv.`abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv` limit 10

# COMMAND ----------

df = spark.read.csv('abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv', header=True)
display(df)


# COMMAND ----------

# MAGIC %sql 
# MAGIC show schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC describe schema default

# COMMAND ----------

# MAGIC %sql
# MAGIC create table flights_delays_external
# MAGIC using csv options (
# MAGIC   path = 'abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv',
# MAGIC   header = "true");
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flights_delays_external

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flights_delays_external

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe extended flights_delays_external

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema flights

# COMMAND ----------

# MAGIC %sql
# MAGIC use flights 

# COMMAND ----------

# MAGIC %sql
# MAGIC create table flights_delays (
# MAGIC year int,
# MAGIC month int,
# MAGIC dayofmonth int,
# MAGIC dayofweek int,
# MAGIC carrier string,
# MAGIC crsdeptime string,
# MAGIC depdelay int,
# MAGIC depdel15 int,
# MAGIC crsarrTime string,
# MAGIC arrdelay int,
# MAGIC arrdel15 int,
# MAGIC canceled smallint,
# MAGIC originairportcode string,
# MAGIC originairportname string,
# MAGIC originlatitude float,
# MAGIC originlongitude float,
# MAGIC destairportcode string,
# MAGIC destairportname string,
# MAGIC destlatitude float,
# MAGIC destlongitude float
# MAGIC )
# MAGIC using csv options ( 
# MAGIC   path = 'abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightDelaysWithAirportCodes.csv',
# MAGIC   header = "true");
# MAGIC   

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flights_delays limit 10

# COMMAND ----------

# MAGIC %sql describe extended  flights_delays

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


