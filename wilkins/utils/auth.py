import os
import requests


def get_access_token() -> str:
    client_id = os.getenv('CLIENT_ID')
    client_secret = os.getenv('CLIENT_SECRET')
    tenant_id = os.getenv('TENANT_ID')
    scope = os.getenv('SCOPE')

    # print(client_id)
    # print(client_secret)
    # print(tenant_id)
    # print(scope)

    # Token endpoint URL
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    # Request headers
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    # Request body
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope
    }

    # Send POST request to get access token
    response = requests.post(token_url, headers=headers, data=data)

    # Parse the JSON response
    token_response = response.json()
    # print(token_response)

    # Extract the access token
    access_token = token_response["access_token"]

    return access_token
