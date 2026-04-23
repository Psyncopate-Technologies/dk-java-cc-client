"""OAuth callback for Microsoft Entra ID (Azure AD) token acquisition."""

import msal
from config import TENANT_ID, CLIENT_ID, CLIENT_SECRET, SCOPE


def oauth_cb(config_str):
    """
    OAuth callback function for confluent-kafka.
    Acquires an access token from Microsoft Entra ID using client credentials flow.

    Args:
        config_str: Configuration string (not used, but required by callback signature)

    Returns:
        tuple: (access_token, expiry_time_in_seconds)
    """
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"

    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=authority,
        client_credential=CLIENT_SECRET
    )

    # Acquire token using client credentials
    result = app.acquire_token_for_client(scopes=[SCOPE])

    if "access_token" in result:
        return result["access_token"], result["expires_in"]
    else:
        error = result.get("error", "Unknown error")
        error_desc = result.get("error_description", "No description")
        raise Exception(f"Failed to acquire token: {error} - {error_desc}")
