# Databricks notebook source
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
# MAGIC 3. The client id , entra secret and tenant ids are saved as secrets in the DBX workspace
# MAGIC
# MAGIC 4. The PAT is generated with the appropriate lifetime value set

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) define variables for the gateway
# MAGIC
# MAGIC 1. The name of the [power bi gateway](https://learn.microsoft.com/en-us/power-platform/admin/onpremises-data-gateway-management#details)
# MAGIC
# MAGIC         gateway_name
# MAGIC
# MAGIC 2. The name of the [data source/connection](https://learn.microsoft.com/en-us/power-bi/connect-data/service-gateway-enterprise-manage-scheduled-refresh#add-a-data-source) created for the databricks workspace 
# MAGIC
# MAGIC         gateway_datasource_name=

# COMMAND ----------

gateway_name=""
gateway_datasource_name=""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) define variables for the semantic model
# MAGIC
# MAGIC 1. The id of the dataset. You can also look this up by dataset name using the [Power BI rest API](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-datasets) 
# MAGIC
# MAGIC         dataset_id
# MAGIC
# MAGIC 2. The id of the workspace. You can also look this up by workspace name using the [Power BI rest API](https://learn.microsoft.com/en-us/rest/api/power-bi/datasets/get-datasets https://learn.microsoft.com/en-us/rest/api/power-bi/groups/get-groups) 
# MAGIC
# MAGIC         workspace_id

# COMMAND ----------

dataset_id=""
workspace_id=""

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 3) define variables for the credentials
# MAGIC
# MAGIC In this example we will use the **same service principal** to manage power bi via [power bi rest APIs](https://learn.microsoft.com/en-us/rest/api/power-bi/) and to connect to databricks. However, **you can use different service principals for each**. 
# MAGIC
# MAGIC For both the approach the secrets to access Power BI rest APIs and databricks are different
# MAGIC
# MAGIC - to access power bi rest APIs you need to use Entra Client secrets
# MAGIC - to access databricks using M2M oauth, you need to use the Databricks service principal secret
# MAGIC
# MAGIC
# MAGIC 1. The following service principal is an Azure Entra Service principal used for connecting to power bi and databricks
# MAGIC   
# MAGIC             client_id 
# MAGIC
# MAGIC 2. This secret is set in [Azure Entra](https://learn.microsoft.com/en-us/entra/identity-platform/howto-create-service-principal-portal#option-3-create-a-new-client-secret).
# MAGIC
# MAGIC             entra_client_secret
# MAGIC   
# MAGIC 3. This is the id of the [Azure Entra tenant](https://learn.microsoft.com/en-us/azure/azure-portal/get-subscription-tenant-id)
# MAGIC
# MAGIC             tenant_id
# MAGIC
# MAGIC 4. Set the lifetime validity of the generated PAT
# MAGIC
# MAGIC
# MAGIC             pat_lifetime
# MAGIC
# MAGIC ##### In the example below, we follow the best practices and store the credentials in databricks secrets, the following [tutorial](https://learn.microsoft.com/en-us/azure/databricks/security/secrets/example-secret-workflow) shows how to set them up.
# MAGIC
# MAGIC

# COMMAND ----------

client_id = dbutils.secrets.get(dbutils.widgets.get(""), "")
entra_client_secret= dbutils.secrets.get(dbutils.widgets.get(""), "")
tenant_id=dbutils.secrets.get(dbutils.widgets.get(""), "")
pat_lifetime=86400


# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %md
# MAGIC # Install the required libraries

# COMMAND ----------

# MAGIC %pip install azure-identity
# MAGIC %pip install databricks-sdk

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 : Generate PAT

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient(host=spark.conf.get("spark.databricks.workspaceUrl"),
                    azure_tenant_id=tenant_id,
                    azure_client_id=client_id,
                    azure_client_secret=client_secret)
                    
t = w.tokens.create(comment="PowerBI access",lifetime_seconds=pat_lifetime)
pat = t.token_value   # This is the token to use in PowerBI

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 : Generate credentials for Power BI REST APIs (to authenticate with power bi service)
# MAGIC
# MAGIC #### This step is always required (you need to have a valid access token to interact with the Power BI REST APIs)

# COMMAND ----------

from services.encrypt_credential_service import EncryptCredentialService
from services.powerbi_service import PbiService

pbi_svc = PbiService()
pbi_svc.create_credential(tenant_id=tenant_id, client_id=client_id, client_secret=databricks_client_secret, user_login = False)


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
