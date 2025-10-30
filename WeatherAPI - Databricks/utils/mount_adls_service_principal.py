# Databricks notebook source
# MAGIC %md
# MAGIC ## Access through service principal

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "openmeteo-scope", key = "openmeteo-client-id")
tenant_id = dbutils.secrets.get(scope = "openmeteo-scope", key = "openmeteo-tenant-id")
client_secret = dbutils.secrets.get(scope = "openmeteo-scope", key = "open-meteo-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@openmeteofp.dfs.core.windows.net/",
  mount_point = "/mnt/openmeteo/raw",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@openmeteofp.dfs.core.windows.net/",
  mount_point = "/mnt/openmeteo/processed",
  extra_configs = configs)

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://presentation@openmeteofp.dfs.core.windows.net/",
  mount_point = "/mnt/openmeteo/presentation",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/openmeteo/raw"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

