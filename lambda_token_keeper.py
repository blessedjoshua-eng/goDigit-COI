"""
lambda_token_keeper.py
=======================
AWS Lambda function — triggered every 10 minutes by EventBridge.
Reads refresh token from SSM, calls Keycloak token API,
saves new access_token + refresh_token back to SSM.
"""

import os
import json
import time
import boto3
import requests
from datetime import datetime, timezone

AUTH_TOKEN_URL = (
    "https://accounts.godigit.com/auth/realms/ABS-21"
    "/protocol/openid-connect/token"
)
CLIENT_ID = "DigitCorporate"

_HEADERS = {
    "accept":             "application/json",
    "accept-language":    "en-US,en;q=0.9",
    "cache-control":      "no-cache",
    "pragma":             "no-cache",
    "content-type":       "application/x-www-form-urlencoded;charset=UTF-8",
    "origin":             "https://corporate.godigit.com",
    "referer":            "https://corporate.godigit.com/DigitCorporate/",
    "user-agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "sec-ch-ua":          '"Google Chrome";v="149", "Chromium";v="149", "Not)A;Brand";v="24"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
    "sec-fetch-dest":     "empty",
    "sec-fetch-mode":     "cors",
    "sec-fetch-site":     "same-site",
}

SSM_ACCESS_TOKEN  = os.environ.get("SSM_ACCESS_TOKEN_PATH",  "/godigit/access_token")
SSM_REFRESH_TOKEN = os.environ.get("SSM_REFRESH_TOKEN_PATH", "/godigit/refresh_token")
SSM_EXPIRES_AT    = os.environ.get("SSM_EXPIRES_AT_PATH",    "/godigit/expires_at")

ssm = boto3.client("ssm", region_name=os.environ.get("AWS_REGION", "ap-south-1"))


def ssm_get(path):
    return ssm.get_parameter(Name=path, WithDecryption=True)["Parameter"]["Value"]


def ssm_put(path, value, secure=True):
    ssm.put_parameter(
        Name=path,
        Value=value,
        Type="SecureString" if secure else "String",
        Overwrite=True,
    )


def handler(event, context):
    print(f"[{datetime.now(timezone.utc).isoformat()}] Token keeper triggered")

    try:
        refresh_token = ssm_get(SSM_REFRESH_TOKEN)
    except Exception as e:
        return {"statusCode": 500, "body": f"Cannot read refresh token from SSM: {e}"}

    cookies = {
        "AUTH_SESSION_ID":          os.environ.get("COOKIE_AUTH_SESSION_ID", ""),
        "AUTH_SESSION_ID_LEGACY":   os.environ.get("COOKIE_AUTH_SESSION_ID_LEGACY", ""),
        "KEYCLOAK_SESSION":         os.environ.get("COOKIE_KEYCLOAK_SESSION", ""),
        "KEYCLOAK_SESSION_LEGACY":  os.environ.get("COOKIE_KEYCLOAK_SESSION_LEGACY", ""),
        "KEYCLOAK_IDENTITY":        os.environ.get("COOKIE_KEYCLOAK_IDENTITY", ""),
        "KEYCLOAK_IDENTITY_LEGACY": os.environ.get("COOKIE_KEYCLOAK_IDENTITY_LEGACY", ""),
    }

    try:
        resp = requests.post(
            AUTH_TOKEN_URL,
            headers=_HEADERS,
            cookies=cookies,
            data={"grant_type": "refresh_token", "refresh_token": refresh_token, "client_id": CLIENT_ID},
            timeout=30,
        )
    except requests.RequestException as e:
        return {"statusCode": 500, "body": f"Network error: {e}"}

    if resp.status_code != 200:
        return {"statusCode": resp.status_code, "body": f"Token API failed: {resp.text[:300]}"}

    data              = resp.json()
    new_access_token  = data.get("access_token")
    new_refresh_token = data.get("refresh_token", refresh_token)
    expires_in        = data.get("expires_in", 900)
    expires_at        = str(time.time() + expires_in)

    if not new_access_token:
        return {"statusCode": 500, "body": f"No access_token in response"}

    ssm_put(SSM_ACCESS_TOKEN,  new_access_token)
    ssm_put(SSM_REFRESH_TOKEN, new_refresh_token)
    ssm_put(SSM_EXPIRES_AT,    expires_at, secure=False)

    print(f"Tokens saved to SSM. Expires in {expires_in}s.")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message":    "Token refreshed successfully",
            "expires_in": expires_in,
        }),
    }
