# Confluent Cloud Configuration - Microsoft Entra ID OAuth/OIDC (DKP CC)
import os

# Azure AD / Microsoft Entra ID Configuration
TENANT_ID = "1b9dca15-4db4-4905-8725-d318d11c6875"
CLIENT_ID = "<APP_ID>"  # Replace with your App Registration Client ID
CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET", "<YOUR_CLIENT_SECRET>")
SCOPE = "api://<APP_ID>/.default"  # Replace <APP_ID> with your App ID

# Confluent Cloud Configuration
BOOTSTRAP_SERVERS = "lkc-1o1jkv.eastus.azure.private.confluent.cloud:9092"
LOGICAL_CLUSTER = "lkc-vgr7p5"
IDENTITY_POOL_ID = "<POOL_ID>"  # Replace with your Identity Pool ID

# Topic Configuration
TOPIC = "dkp-python-client-test"

# Consumer Configuration
GROUP_ID = "dkp-python-oidc-test-group"
