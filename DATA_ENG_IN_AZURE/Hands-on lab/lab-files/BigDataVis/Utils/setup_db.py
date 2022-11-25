# Databricks notebook source
# DBTITLE 1,Prerequisites 
# MAGIC %md
# MAGIC #### Data Preparations
# MAGIC 1. Clone our git repo: * git clone https://github.com/microsoft/MTC_IL_WORKSHOP_Big_data_analytics_and_visualization.git*
# MAGIC 2. In Azure Portal create Resource Group: academyrg-_unique postfix_
# MAGIC 3. In the Resource Group create storage account
# MAGIC 4. Create Container: labs-_unique postfix_
# MAGIC 5. Create folder: FlightsDelays
# MAGIC 6. Unzip ./MTC_IL_WORKSHOP_Big_data_analytics_and_visualization/DATA_ENG_IN_AZURE/Hands-on lab/lab-files/FlightsAndWeather.zip. The zip contains 3 files.
# MAGIC 7. Upload those files into FlightsDelays folder.
# MAGIC 
# MAGIC 
# MAGIC #####  Security set up. Update following values in <> brackets before running the notebooks:
# MAGIC 1. Storage Account Name
# MAGIC 2. Databricks secret scope name
# MAGIC 3. Service Principal Secret Description as it defined in Key Vault
# MAGIC 4. Service Principal Accplication (client) ID 
# MAGIC 5. AAD Tenant ID where you are working

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="<DataBricks secrets scope name>",key="<Key Desciption in Key Vault>")  #secret scope / secret description in key vault!  

spark.conf.set("fs.azure.account.auth.type.<storage account name>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<storage account name>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<storage account name>.dfs.core.windows.net", "<Application (client) ID in AAD>") # application (client) id 
spark.conf.set("fs.azure.account.oauth2.client.secret.<storage account name>.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage account name>.dfs.core.windows.net", "https://login.microsoftonline.com/<AAD teanant ID>") # directory (tenant)id

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("<secret scope name>")

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class":   spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}
dbutils.fs.mount(
  source = "abfss://<container name>@a<storage account name>.dfs.core.windows.net/<folder name>",
  mount_point = "/mnt/<mount name>",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls("abfss://<container name>@<storage account name>.dfs.core.windows.net/")
