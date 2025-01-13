# Databricks notebook source
#The following service principal is an Azure Entra Service principal used for connecting to power bi and databricks 
client_id = dbutils.secrets.get("nshetty", "catalyst-client_id")
client_secret= dbutils.secrets.get("nshetty", "catalyst-client_secret")
# entra tenant id
tenant_id=dbutils.secrets.get("nshetty", "catalyst-tenant_id")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient(host="adb-984752964297111.11.azuredatabricks.net",
                    azure_tenant_id=tenant_id,
                    azure_client_id=client_id,
                    azure_client_secret=client_secret)

# COMMAND ----------

tokens_dict = w.tokens.list()
print(tokens_dict)

# COMMAND ----------

token_ids = [token.token_id for token in tokens_dict]
for t in token_ids:
  w.tokens.delete(token_id=t)