# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %md
# MAGIC Install the required libraries

# COMMAND ----------

# MAGIC %pip install azure-identity
# MAGIC %pip install databricks-sdk

# COMMAND ----------

# MAGIC %md
# MAGIC ### Background
# MAGIC In this example, we use the same Azure AD / Entra enterprise application / service principal to 
# MAGIC 1. connect to Power BI using REST API (to update the credentials)
# MAGIC 2. connect to DBX sql warehouse to extract the data (in direct query or import mode) 
# MAGIC
# MAGIC ### Pre-requisites
# MAGIC 1. The azure AD / Entra enterprise application / service principal is added to the [workspace](https://learn.microsoft.com/en-us/azure/databricks/admin/users-groups/service-principals#databricks-and-microsoft-entra-id-service-principals) with the required permissions.
# MAGIC
# MAGIC 2. The azure AD / Entra enterprise application / service principal needs the following permissions
# MAGIC   - Permissions to execute REST APIs
# MAGIC   - Permissions to access datasets in a power bi workspace
# MAGIC
# MAGIC 3. The client id , secret and tenant ids are saved as secrets in the DBX workspace

# COMMAND ----------

#The following service principal is an Azure Entra Service principal used for connecting to power bi and databricks 
client_id = dbutils.secrets.get("nshetty", "catalyst-client_id")
client_secret= dbutils.secrets.get("nshetty", "catalyst-client_secret")
# entra tenant id
tenant_id=dbutils.secrets.get("nshetty", "catalyst-tenant_id")

# get parameters
gateway_name=dbutils.widgets.get("gateway_name")
gateway_datasource_name=dbutils.widgets.get("gateway_datasource_name")
dataset_id=dbutils.widgets.get("dataset_id")
workspace_id=dbutils.widgets.get("workspace_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 : Generate PAT

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient(host=spark.conf.get("spark.databricks.workspaceUrl"),
                    azure_tenant_id=tenant_id,
                    azure_client_id=client_id,
                    azure_client_secret=client_secret)
                    
t = w.tokens.create(comment="PowerBI access",lifetime_seconds=86400)
pat = t.token_value   # This is the token to use in PowerBI

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 : Generate credentials for Power BI REST APIs (to authenticate with power bi service)

# COMMAND ----------

from services.encrypt_credential_service import EncryptCredentialService
from services.pbi_embed_service import PbiEmbedService

pbi_svc = PbiEmbedService()
pbi_svc.create_credential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret, user_login = False)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1 : Update the credentials in a gateway connection, this includes the following steps
# MAGIC 1. get the gateway id
# MAGIC 2. get the gateway public key

# COMMAND ----------

gatewayId=pbi_svc.get_gateway_id(gateway_name)
public_key=pbi_svc.get_gateway_public_key(gatewayId)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Encrypt the credentials

# COMMAND ----------

enc=EncryptCredentialService(public_key)
serialized_credentials = '{\'credentialData\':[{\'name\':\'key\',\'value\':\'' + pat + '\'}]}'
encrypted_credentials=enc.encode_credentials(serialized_credentials)

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Update the gateway data source to use the updated credentials

# COMMAND ----------

pbi_svc.update_gateway_pat(gatewayId,gateway_datasource_name,encrypted_credentials)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2 : Update the credentials in a dataset
# MAGIC 1. get the dataset and workspace id (in the example below this is extracted from power bi ui but can also be extracted using power bi rest apis)
# MAGIC 2. Update the dataset settings to include the PAT

# COMMAND ----------

pbi_svc.update_dataset_pat(workspace_id , dataset_id, pat)