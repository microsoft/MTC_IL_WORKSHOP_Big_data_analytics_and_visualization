# Databricks notebook source
# MAGIC %md # Prepare flight delay data

# COMMAND ----------

# MAGIC %md To start, let's import the Python libraries and modules we will use in this notebook.

# COMMAND ----------

import datetime
from pyspark.sql.types import *
from pyspark.sql.functions import unix_timestamp
import math
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md First, let's execute the below command to make sure all three tables were created.
# MAGIC You should see an output like the following:
# MAGIC 
# MAGIC | database | tableName | isTemporary |
# MAGIC | --- | --- | --- |
# MAGIC | default | airport_code_loca... | false |
# MAGIC | default | flight_delays_wit... | false |
# MAGIC | default | flight_weather_wi... | false |

# COMMAND ----------

spark.sql("show tables").show()

# COMMAND ----------

# MAGIC %md Now execute a SQL query using the `%sql` magic to select all columns from flight_delays_with_airport_codes. By default, only the first 1,000 rows will be returned.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flight_delays_with_airport_codes

# COMMAND ----------

# MAGIC %md Now let's see how many rows there are in the dataset.

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flight_delays_with_airport_codes

# COMMAND ----------

# MAGIC %md Based on the `count` result, you can see that the dataset has a total of 2,719,418 rows (also referred to as examples in Machine Learning literature). Looking at the table output from the previous query, you can see that the dataset contains 20 columns (also referred to as features).

# COMMAND ----------

# MAGIC %md Because all 20 columns are displayed, you can scroll the grid horizontally. Scroll until you see the **DepDel15** column. This column displays a 1 when the flight was delayed at least 15 minutes and 0 if there was no such delay. In the model you will construct, you will try to predict the value of this column for future data.
# MAGIC 
# MAGIC Let's execute another query that shows us how many rows do not have a value in the DepDel15 column.

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flight_delays_with_airport_codes where DepDel15 is null

# COMMAND ----------

# MAGIC %md Notice that the `count` result is 27444. This means that 27,444 rows do not have a value in this column. Since this value is very important to our model, we will need to eliminate any rows that do not have a value for this column.

# COMMAND ----------

# MAGIC %md Next, scroll over to the **CRSDepTime** column within the table view above. Our model will approximate departure times to the nearest hour, but departure time is captured as an integer. For example, 8:37 am is captured as 837. Therefore, we will need to process the CRSDepTime column, and round it down to the nearest hour. To perform this rounding will require two steps, first you will need to divide the value by 100 (so that 837 becomes 8.37). Second, you will round this value down to the nearest hour (so that 8.37 becomes 8).

# COMMAND ----------

# MAGIC %md Finally, we do not need all 20 columns present in the flight_delays_with_airport_codes dataset, so we will pare down the columns, or features, in the dataset to the 12 we do need.

# COMMAND ----------

# MAGIC %md Using `%sql` magic allows us view and visualize the data, but for working with the data in our tables, we want to take advantage of the rich optimizations provided by DataFrames. Let's execute the same query using Spark SQL, this time saving the query to a DataFrame.

# COMMAND ----------

dfFlightDelays = spark.sql("select * from flight_delays_with_airport_codes")

# COMMAND ----------

# MAGIC %md Let's print the schema for the DataFrame.

# COMMAND ----------

print(dfFlightDelays.dtypes)

# COMMAND ----------

# MAGIC %md Notice that the DepDel15 and CRSDepTime columns are both `string` data types. Both of these features need to be numeric, according to their descriptions above. We will cast these columns to their required data types next.

# COMMAND ----------

# MAGIC %md ## Perform data munging

# COMMAND ----------

# MAGIC %md To perform our data munging, we have multiple options, but in this case, we’ve chosen to take advantage of some useful features of R to perform the following tasks:
# MAGIC 
# MAGIC * Remove rows with missing values
# MAGIC * Generate a new column, named “CRSDepHour,” which contains the rounded down value from CRSDepTime
# MAGIC * Pare down columns to only those needed for our model

# COMMAND ----------

# Select only the columns we need, casting CRSDepTime as long and DepDel15 as int, into a new DataFrame
dfflights = spark.sql("SELECT OriginAirportCode, OriginLatitude, OriginLongitude, Month, DayofMonth, cast(CRSDepTime as long) CRSDepTime, DayOfWeek, Carrier, DestAirportCode, DestLatitude, DestLongitude, cast(DepDel15 as int) DepDel15 from flight_delays_with_airport_codes")

# Delete rows containing missing values
dfflights = dfflights.na.drop("any")

# Round departure times down to the nearest hour, and export the result as a new column named "CRSDepHour"
dfflights = dfflights.withColumn("CRSDepHour", F.floor(F.col('CRSDepTime') / 100))

display(dfflights)

# COMMAND ----------

# Create a Temporary Table / View with clean data from the DataFrame 
dfflights.createOrReplaceTempView("flight_delays_view")

# COMMAND ----------

# MAGIC %md Now let's take a look at the resulting data. Take note of the **CRSDepHour** column that we created, as well as the number of columns we now have (12). Verify that the new CRSDepHour column contains the rounded hour values from our CRSDepTime column.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from flight_delays_view

# COMMAND ----------

# MAGIC %md Now verify that the rows with missing data for the **DepDel15** column have been removed.

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from flight_delays_view

# COMMAND ----------

# MAGIC %md You should see a count of **2,691,974**. This is equal to the original 2,719,418 rows minus the 27,444 rows with missing data in the DepDel15 column.
# MAGIC 
# MAGIC Now save the contents of the temporary view into a new DataFrame.

# COMMAND ----------

dfFlightDelays_Clean = spark.sql("select * from flight_delays_view")

# COMMAND ----------

# MAGIC %md ## Export the prepared data to persistent a global table

# COMMAND ----------

# MAGIC %md There are two types of tables in Databricks. 
# MAGIC 
# MAGIC * Global tables, which are accessible across all clusters
# MAGIC * Local tables, which are available only within one cluster
# MAGIC 
# MAGIC To create a global table, you use the `saveAsTable()` method. To create a local table, you would use either the `createOrReplaceTempView()` or `registerTempTable()` method.
# MAGIC 
# MAGIC The `flight_delays_view` table was created as a local table using `createOrReplaceTempView`, and is therefore temporary. Local tables are tied to the Spark/SparkSQL Context that was used to create their associated DataFrame. When you shut down the SparkSession that is associated with the cluster (such as shutting down the cluster) then local, temporary tables will disappear. If we want our cleansed data to remain permanently, we should create a global table. 
# MAGIC 
# MAGIC Run the following to copy the data from the source location into a global table named `flight_delays_clean`.

# COMMAND ----------

dfFlightDelays_Clean.write.mode("overwrite").saveAsTable("flight_delays_clean")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL flight_delays_clean

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next step
# MAGIC 
# MAGIC Continue to the next notebook, [02 Weather Data Preparation]($./02%20Weather%20Data%20Preparation).
