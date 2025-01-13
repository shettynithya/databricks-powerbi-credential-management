import json, requests, azure.identity

class PbiEmbedService(object):

    def create_credential(self, tenant_id, client_id, client_secret, user_login = False):
        if user_login:
          self.credential = azure.identity.DeviceCodeCredential()
        else:
          self.credential = azure.identity.ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

    def get_access_token(self):
        access_token_class = self.credential.get_token('https://analysis.windows.net/powerbi/api/.default')
        return access_token_class.token

    def get_request_header(self):
        """
        Get Power BI API request header

        Returns:
            Dict: Request header
        """

        return {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.get_access_token()}
    
    def get_gateway_id(self, gateway_name):
            headers = self.get_request_header()
            r=requests.get(f"https://api.powerbi.com/v1.0/myorg/gateways", headers = headers)
            _gw = json.loads(r.text)
            for i in _gw["value"]:
                if i["name"]==gateway_name:
                    return i["id"]
    
    def get_gateway_public_key(self,gatewayId):
            headers = self.get_request_header()
            r=requests.get(f"https://api.powerbi.com/v1.0/myorg/gateways/{gatewayId}", headers = headers)
            t=json.loads(r.text)
            return t["publicKey"]

    def update_gateway_pat(self, gatewayId, datasource_name, pat):
            headers = self.get_request_header()
            r = requests.get(f"https://api.powerbi.com/v1.0/myorg/gateways/{gatewayId}/datasources", headers = headers)
            t=json.loads(r.text)
            for i in t["value"]:
                if i["datasourceName"]==datasource_name:
                    datasourceId=i["id"]
            requestBodyJson = {
            "credentialDetails": {
            "credentialType": "Key",
            "credentials": pat,
            "encryptedConnection": "Encrypted",
            "encryptionAlgorithm": "RSA-OAEP",
            "privacyLevel": "None"
            }
            }
            update_pat = requests.patch(f"https://api.powerbi.com/v1.0/myorg/gateways/{gatewayId}/datasources/{datasourceId}", json=requestBodyJson, headers = headers)
            return update_pat


    def update_dataset_pat(self, workspace_id, dataset_id, pat):
        headers = self.get_request_header()
        r = requests.post(f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/Default.TakeOver", headers = headers)
        
        _dsobject = requests.get(f" https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/datasources", headers = headers)
        
        _dso = json.loads(_dsobject.text)
        datasource_id =_dso["value"][0]["datasourceId"]
        gateway_id = _dso["value"][0]["gatewayId"]

        requestBodyJson = {
          "credentialDetails": {
          "credentialType": "Key",
           "credentials": f'{{"credentialData":[{{"name":"key", "value": "{pat}"}}]}}',
          "encryptedConnection": "Encrypted",
          "encryptionAlgorithm": "None",
          "privacyLevel": "None"
         }
        }
        update_pat = requests.patch(f"https://api.powerbi.com/v1.0/myorg/gateways/{gateway_id}/datasources/{datasource_id}", json=requestBodyJson, headers = headers)
        return update_pat