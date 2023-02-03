# Databricks notebook source
# MAGIC %md 
# MAGIC ##### Delta Lake format
# MAGIC 
# MAGIC Let's explore Delta Lake format
# MAGIC 
# MAGIC Delta Lake provides ACID transactions, hstory, time travel, schema enforcement, perfromance optimizations  

# COMMAND ----------

# MAGIC %sql drop view incremental_flights_with_weather_data_temp_view

# COMMAND ----------

# MAGIC %sql
# MAGIC create temp view incremental_flights_with_weather_data_temp_view as
# MAGIC select
# MAGIC   *,
# MAGIC   1 as dirty_column_flag
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

# MAGIC %md Schema enforcement in action
# MAGIC In this specific case you can enforce merge, since adding new field doesn't break a schema (echema evolution)
# MAGIC 
# MAGIC Set Spark parameter below, to enforce schema merge and run the `INSERT INTO` once again

# COMMAND ----------

# MAGIC %python
# MAGIC spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled",  "true")

# COMMAND ----------

# MAGIC %md
# MAGIC Run again `INSERT INTO`

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %md 
# MAGIC Transactional history and Time Travel with Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history flights_with_weather_delta_from_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flights_with_weather_delta_from_view version as of  0

# COMMAND ----------

# MAGIC %md
# MAGIC Roolback to the table previous version with Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC restore table flights_with_weather_delta_from_view version as of 0

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe history flights_with_weather_delta_from_view
