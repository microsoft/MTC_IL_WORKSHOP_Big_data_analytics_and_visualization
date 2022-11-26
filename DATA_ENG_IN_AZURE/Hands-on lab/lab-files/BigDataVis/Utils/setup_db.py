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
# MAGIC #####  Security set up. Following custom arguments will be set: 
# MAGIC 1. Storage Account Name
# MAGIC 2. Databricks secret scope name
# MAGIC 3. Service Principal Secret Description as it defined in Key Vault
# MAGIC 4. Service Principal Accplication (client) ID 
# MAGIC 5. AAD Tenant ID where you are working

# COMMAND ----------

print ("Starting arguments initialization...")

secret_scope_name = dbutils.widgets.get("SECRET_SCOPE_NAME")
secret_id = dbutils.widgets.get("SECRET_ID")
storage_account_name = dbutils.widgets.get("STORAGE_ACCOUNT_NAME")
sp_application_id = dbutils.widgets.get("SP_APPLICATION_ID")
tenant_id = dbutils.widgets.get("TENANT_ID")
container_name = dbutils.widgets.get("CONTAINER_NAME")
data_folder_name = dbutils.widgets.get("DATA_FOLDER_NAME")
adls_mount_point_name = dbutils.widgets.get("ADLS_MOUNT_POINT_NAME")

print (f"secret_scope_name = {secret_scope_name}")
print (f"secret_id = {secret_id}")  
print (f"storage_account_name = {storage_account_name}")
print (f"sp_application_id = {sp_application_id}")
print (f"tenant_id = {tenant_id}")
print (f"container_name = {container_name}")
print (f"adls_mount_point_name = {adls_mount_point_name}")

print ("Setting service principal configuration...")
service_credential = dbutils.secrets.get(scope=f"{secret_scope_name}",key=f"{secret_id}")  #secret scope name / the key is a secret description in key vault!  
spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", f"{sp_application_id}") # application (client) id 
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", service_credential)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token") # directory (tenant)id

# COMMAND ----------

print ("list all existing secret scopes in workspace:")
dbutils.secrets.listScopes()

# COMMAND ----------

#check specific secret scope
dbutils.secrets.list(f"{secret_scope_name}")

# COMMAND ----------

print ("Mount ADLS...")
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class":   spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

#Important! Change the container name, folder and storage account
dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{data_folder_name}",
  mount_point = f"/mnt/{adls_mount_point_name}",
  extra_configs = configs)

# COMMAND ----------

print("Input data folder content:")
dbutils.fs.ls(f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/")
