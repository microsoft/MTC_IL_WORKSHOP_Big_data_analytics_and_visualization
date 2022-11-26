# Databricks notebook source

res = dbutils.notebook.run("../Utils/setup_db", 
                           timeout_seconds = 0, 
                           arguments = {"SECRET_SCOPE_NAME"    : "<>",
                                        "SECRET_ID"            : "<>",
                                        "STORAGE_ACCOUNT_NAME" : "<>",
                                        "SP_APPLICATION_ID"    : "<>",
                                        "TENANT_ID"            : "<>",
                                        "CONTAINER_NAME"       : "<>",
                                        "DATA_FOLDER_NAME"     : "FlightsDelays",
                                        "ADLS_MOUNT_POINT_NAME":"FlightsDelaysMount"})

#if res == "INIT_OK":
#    print("initialization is done succesfully")
#else:
#    dbutils.notebook.exit(str("FAILURE in init"))


# COMMAND ----------

dbutils.fs.ls('abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/')

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class":   spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}
dbutils.fs.mount(
  source = "abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays",
  mount_point = "/mnt/FlightsDelaysMount_01",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.ls('abfss://labs-303474@asastoremcw303474.dfs.core.windows.net/FlightsDelays/')

# COMMAND ----------


