# Databricks notebook source
create table airports_codes_locations(
  airportid int,
  airport string,
  displayairportname string,
  latitude float,
  longitude float
) using csv options (
  header = "true",
  path = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/AirportCodeLocationLookupClean.csv"
)

# COMMAND ----------

create table flights_with_weather(
  year int,
  month int,
  day int,
  time int,
  timezone int,
  skycondition string,
  visibility float,
  weathertype string,
  drybulbfarenheit float,
  drybulbcelsius float,
  wetbulbfarenheit float,
  wetbulbcelsius float,
  dewpointfarenheit float,
  dewpointcelsius float,
  relativehumidity int,
  windspeed int,
  winddirection int,
  valueforwindcharacter int,
  stationpressure float,
  pressuretendency int,
  pressurechange int,
  sealevelpressure float,
  recordtype string,
  hourlyprecip string,
  altimeter float,
  airportcode string,
  displayairportname string,
  latitude float,
  longitude float
) using csv options (
  header = "true",
  path = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/FlightWeatherWithAirportCode.csv"
)
