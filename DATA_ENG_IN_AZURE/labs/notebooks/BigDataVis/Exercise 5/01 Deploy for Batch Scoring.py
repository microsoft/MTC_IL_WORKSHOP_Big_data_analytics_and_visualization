# Databricks notebook source
# MAGIC %md
# MAGIC For the batch scoring, we will persist the values in a new global persistent Databricks table. In production data workloads, you may save the scored data to Blob Storage, Azure Cosmos DB, or other serving layer. Another implementation detail we are skipping for the lab is processing only new files. This can be accomplished by creating a widget in the notebook that accepts a path parameter that is passed in from Azure Data Factory.

# COMMAND ----------

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, Bucketizer
from pyspark.sql.functions import array, col, lit
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC Replace STORAGE-ACCOUNT-NAME with the name of your storage account. You can find this in the Azure portal by locating the storage account that you created in the lab setup, within your resource group. The container name is set to the default used for this lab. If yours is different, update the containerName variable accordingly.

# COMMAND ----------

accountName = "STORAGE-ACCOUNT-NAME"
containerName = "sparkcontainer"

# COMMAND ----------

# MAGIC %md
# MAGIC Define the schema for the CSV files

# COMMAND ----------

data_schema = StructType([
        StructField('OriginAirportCode',StringType()),
        StructField('Month', IntegerType()),
        StructField('DayofMonth', IntegerType()),
        StructField('CRSDepHour', IntegerType()),
        StructField('DayOfWeek', IntegerType()),
        StructField('Carrier', StringType()),
        StructField('DestAirportCode', StringType()),
        StructField('DepDel15', IntegerType()),
        StructField('WindSpeed', DoubleType()),
        StructField('SeaLevelPressure', DoubleType()),  
        StructField('HourlyPrecip', DoubleType())])

# COMMAND ----------

# MAGIC %md
# MAGIC Create a new DataFrame from the CSV files, applying the schema

# COMMAND ----------

dfDelays = spark.read.csv("wasbs://" + containerName + "@" + accountName + ".blob.core.windows.net/FlightsAndWeather/*/*/FlightsAndWeather.csv",
                    schema=data_schema,
                    sep=",",
                    header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Load the trained machine learning model you created earlier in the lab

# COMMAND ----------

# Load the saved pipeline model
model = PipelineModel.load("/flightDelayModel")

# COMMAND ----------

# MAGIC %md
# MAGIC Make a prediction against the loaded data set

# COMMAND ----------

# Make a prediction against the dataset
prediction = model.transform(dfDelays)

# COMMAND ----------

# MAGIC %md
# MAGIC Save the scored data into a new global table called **scoredflights**

# COMMAND ----------

prediction.write.mode("overwrite").saveAsTable("scoredflights")
