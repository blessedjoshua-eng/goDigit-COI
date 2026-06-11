"""
godigit_coi.py
==============
Single script that handles everything:
  - LOCAL mode  : runs token keeper in background thread + downloads PDFs
  - PROD mode   : reads token from AWS SSM (kept alive by Lambda) + downloads PDFs
  - SETUP mode  : deploys Lambda + EventBridge + SSM to AWS (one time only)

Usage:
    pip install requests pandas openpyxl boto3

    # Local testing:
    python godigit_coi.py --mode local

    # First time prod setup (run once):
    python godigit_coi.py --setup

    # Production run:
    python godigit_coi.py --mode prod
"""

import os
import sys
import json
import time
import base64
import logging
import argparse
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from pathlib import Path
from datetime import datetime

# ══════════════════════════════════════════════════════════════
#  CONFIG — Prefilled. Do not change unless values expire.
# ══════════════════════════════════════════════════════════════

CONFIG = {

    # ── File Paths ─────────────────────────────────────────────
    "excel_path": r"D:\Digit COI downloader\loan_accounts.xlsx",
    "output_dir": r"D:\Digit COI downloader\coi_downloads",

    # ── GoDigit Identity ───────────────────────────────────────
    "user_id":      "96246062",
    "company_code": "LI",

    # ── Refresh Token ──────────────────────────────────────────
    "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI3OWE2YmYwNS1iYjdmLTQ2ZGQtOTU4OC1lMWM5ODQ3ZGM4NzUifQ.eyJleHAiOjE3ODExODI3MzYsImlhdCI6MTc4MTE3OTEzOCwianRpIjoiY2M4NTI0YTctMzkwOC00ZWRjLWE5NGUtYzFhNTQ4YWZjZWY4IiwiaXNzIjoiaHR0cHM6Ly9hY2NvdW50cy5nb2RpZ2l0LmNvbS9hdXRoL3JlYWxtcy9BQlMtMjEiLCJhdWQiOiJodHRwczovL2FjY291bnRzLmdvZGlnaXQuY29tL2F1dGgvcmVhbG1zL0FCUy0yMSIsInN1YiI6Ijc3NDFhZmU0LTcxZjEtNDA2NS05NDA1LTM1Yjk3NWJmM2QzZSIsInR5cCI6IlJlZnJlc2giLCJhenAiOiJEaWdpdENvcnBvcmF0ZSIsIm5vbmNlIjoiYjdhOWMzN2QtODQzMC00ZDM2LTg3NjMtN2I5NDFkM2YzZWJkIiwic2Vzc2lvbl9zdGF0ZSI6ImZjMDAyZDE0LTEwNmUtNDg5MS05MzEyLTI3NWQwYTA4YzE1MCIsInNjb3BlIjoib3BlbmlkIGNvbXBhbnlfY29kZSB1c2VyaWQgUG9ydGFsX1R5cGUiLCJzaWQiOiJmYzAwMmQxNC0xMDZlLTQ4OTEtOTMxMi0yNzVkMGEwOGMxNTAifQ.INlqSDudZcPuwIbUrasjfG4GQ8Kh0QooEwgmkgaDYwc",

    # ── Cookies ────────────────────────────────────────────────
    "cookies": {
        "AUTH_SESSION_ID":          "fc002d14-106e-4891-9312-275d0a08c150.node-103",
        "AUTH_SESSION_ID_LEGACY":   "fc002d14-106e-4891-9312-275d0a08c150.node-103",
        "KEYCLOAK_SESSION":         "ABS-21/7741afe4-71f1-4065-9405-35b975bf3d3e/fc002d14-106e-4891-9312-275d0a08c150",
        "KEYCLOAK_SESSION_LEGACY":  "ABS-21/7741afe4-71f1-4065-9405-35b975bf3d3e/fc002d14-106e-4891-9312-275d0a08c150",
        "KEYCLOAK_IDENTITY":        "eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI3OWE2YmYwNS1iYjdmLTQ2ZGQtOTU4OC1lMWM5ODQ3ZGM4NzUifQ.eyJleHAiOjE3ODE0Mzc4NTUsImlhdCI6MTc4MTE3ODY1NSwianRpIjoiYzgwYTQ3NWMtNTI4ZS00OGYzLTlhNTktNmFlZjA1MTIzNWI1IiwiaXNzIjoiaHR0cHM6Ly9hY2NvdW50cy5nb2RpZ2l0LmNvbS9hdXRoL3JlYWxtcy9BQlMtMjEiLCJzdWIiOiI3NzQxYWZlNC03MWYxLTQwNjUtOTQwNS0zNWI5NzViZjNkM2UiLCJ0eXAiOiJTZXJpYWxpemVkLUlEIiwic2Vzc2lvbl9zdGF0ZSI6ImZjMDAyZDE0LTEwNmUtNDg5MS05MzEyLTI3NWQwYTA4YzE1MCIsInNpZCI6ImZjMDAyZDE0LTEwNmUtNDg5MS05MzEyLTI3NWQwYTA4YzE1MCIsInN0YXRlX2NoZWNrZXIiOiIwNnRrVXVqdl83anZwRkJJc1FCZElrSXBzY0ZDVHU0SUhMOGdEM1NXQ1VZIn0.A8NWnTMGVrzFUfnZw6fgI25bbUIV9pZIlYgSwdYZiT4",
        "KEYCLOAK_IDENTITY_LEGACY": "eyJhbGciOiJIUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICI3OWE2YmYwNS1iYjdmLTQ2ZGQtOTU4OC1lMWM5ODQ3ZGM4NzUifQ.eyJleHAiOjE3ODE0Mzc4NTUsImlhdCI6MTc4MTE3ODY1NSwianRpIjoiYzgwYTQ3NWMtNTI4ZS00OGYzLTlhNTktNmFlZjA1MTIzNWI1IiwiaXNzIjoiaHR0cHM6Ly9hY2NvdW50cy5nb2RpZ2l0LmNvbS9hdXRoL3JlYWxtcy9BQlMtMjEiLCJzdWIiOiI3NzQxYWZlNC03MWYxLTQwNjUtOTQwNS0zNWI5NzViZjNkM2UiLCJ0eXAiOiJTZXJpYWxpemVkLUlEIiwic2Vzc2lvbl9zdGF0ZSI6ImZjMDAyZDE0LTEwNmUtNDg5MS05MzEyLTI3NWQwYTA4YzE1MCIsInNpZCI6ImZjMDAyZDE0LTEwNmUtNDg5MS05MzEyLTI3NWQwYTA4YzE1MCIsInN0YXRlX2NoZWNrZXIiOiIwNnRrVXVqdl83anZwRkJJc1FCZElrSXBzY0ZDVHU0SUhMOGdEM1NXQ1VZIn0.A8NWnTMGVrzFUfnZw6fgI25bbUIV9pZIlYgSwdYZiT4",
    },

    # ── AWS Settings (only needed for --setup and --mode prod) ─
    "aws_region":     "ap-south-1",
    "aws_account_id": "",

    # ── Excel Column Names ─────────────────────────────────────
    "col_loan_account": "Loan Account Number",
    "col_product_type": "Product Type",

    # ── Behaviour ──────────────────────────────────────────────
    "local_refresh_interval": 10 * 60,  # token refresh interval in LOCAL mode

    # Parallel workers — how many rows to process simultaneously
    # Increase for more speed, decrease if server starts rate limiting (429 errors)
    # Recommended: 10 for normal use, 5 if you see 429 errors
    "max_workers": 10,

    # Batch size — rows processed per batch before a short pause
    # Prevents overwhelming the server with 500+ row files
    # Set to 0 to disable batching
    "batch_size": 50,

    # Pause between batches in seconds (only applies if batch_size > 0)
    "batch_pause_secs": 2,
}


# ══════════════════════════════════════════════════════════════
#  CONSTANTS — Do not change
# ══════════════════════════════════════════════════════════════
AUTH_TOKEN_URL   = "https://accounts.godigit.com/auth/realms/ABS-21/protocol/openid-connect/token"
POLICY_FETCH_URL = "https://prod-corporateservice.godigit.com/digitcorporateservice/tpa/search/childPolicy"
DOC_DOWNLOAD_URL = "https://prod-corporateservice.godigit.com/digitcorporateservice/document/download"
CLIENT_ID        = "DigitCorporate"

SSM_ACCESS_TOKEN  = "/godigit/access_token"
SSM_REFRESH_TOKEN = "/godigit/refresh_token"
SSM_EXPIRES_AT    = "/godigit/expires_at"

LAMBDA_FUNCTION  = "godigit-token-keeper"
LAMBDA_ROLE      = "godigit-token-keeper-role"
EVENTBRIDGE_RULE = "godigit-token-keeper-schedule"

LAMBDA_ZIP       = Path(__file__).parent / "lambda_token_keeper.zip"
TOKEN_CACHE_FILE = Path(__file__).parent / "token_cache.json"
LOG_FILE         = Path(__file__).parent / "godigit_coi.log"

_COMMON_HEADERS = {
    "accept-language":    "en-US,en;q=0.9",
    "cache-control":      "no-cache",
    "pragma":             "no-cache",
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

# ── Shared HTTP session with connection pooling ───────────────
# One session per thread — reuses TCP connections, significantly
# faster than creating a new connection per request
_thread_local = threading.local()

def _get_session() -> requests.Session:
    """Returns a thread-local requests.Session with connection pooling."""
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        # Pool 10 connections — enough for 10 parallel workers
        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=Retry(total=0),  # we handle retries ourselves
        )
        session.mount("https://", adapter)
        session.mount("http://",  adapter)
        _thread_local.session = session
    return _thread_local.session


# ── Logging ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("godigit")


# ══════════════════════════════════════════════════════════════
#  VALIDATION
# ══════════════════════════════════════════════════════════════
def validate_config(mode: str):
    errors = []

    if not CONFIG["refresh_token"] or "PASTE" in CONFIG["refresh_token"]:
        errors.append("refresh_token is not set in CONFIG")

    for k, v in CONFIG["cookies"].items():
        if not v or "PASTE" in v:
            errors.append(f"cookies['{k}'] is not set in CONFIG")
            break

    if not CONFIG["excel_path"]:
        errors.append("excel_path is not set in CONFIG")

    if not CONFIG["output_dir"]:
        errors.append("output_dir is not set in CONFIG")

    if mode in ("prod", "setup") and not LAMBDA_ZIP.exists():
        errors.append(
            f"lambda_token_keeper.zip not found at:\n"
            f"  {LAMBDA_ZIP}\n"
            f"  Make sure it is in the same folder as godigit_coi.py"
        )

    if errors:
        log.error("=" * 55)
        log.error("  CONFIG errors — fix these before running:")
        for e in errors:
            log.error(f"  • {e}")
        log.error("=" * 55)
        sys.exit(1)


# ══════════════════════════════════════════════════════════════
#  TOKEN API
# ══════════════════════════════════════════════════════════════
def call_token_api(refresh_token: str) -> dict:
    resp = requests.post(
        AUTH_TOKEN_URL,
        headers={
            **_COMMON_HEADERS,
            "accept":       "application/json",
            "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
        },
        cookies=CONFIG["cookies"],
        data={
            "grant_type":    "refresh_token",
            "refresh_token": refresh_token,
            "client_id":     CLIENT_ID,
        },
        timeout=30,
    )

    if resp.status_code != 200:
        raise RuntimeError(
            f"Token API returned [{resp.status_code}]: {resp.text[:300]}"
        )

    data = resp.json()
    if "access_token" not in data:
        raise RuntimeError(f"No access_token in response: {data}")

    return data


# ══════════════════════════════════════════════════════════════
#  LOCAL MODE — Background thread + token_cache.json
# ══════════════════════════════════════════════════════════════
def _write_local_cache(access_token: str, refresh_token: str, expires_in: int):
    data = {
        "access_token":  access_token,
        "refresh_token": refresh_token,
        "expires_at":    time.time() + expires_in,
        "updated_at":    datetime.now().isoformat() + "Z",
    }
    tmp = TOKEN_CACHE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(TOKEN_CACHE_FILE)


def _read_local_cache() -> dict:
    if not TOKEN_CACHE_FILE.exists():
        return {}
    try:
        return json.loads(TOKEN_CACHE_FILE.read_text())
    except Exception:
        return {}


class LocalTokenKeeper(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self._stop_event  = threading.Event()
        self._ready_event = threading.Event()
        self.current_refresh_token = CONFIG["refresh_token"]

    def run(self):
        interval = CONFIG["local_refresh_interval"]
        log.info("[TokenKeeper] Background thread started.")

        while not self._stop_event.is_set():
            try:
                log.info("[TokenKeeper] Refreshing token...")
                data = call_token_api(self.current_refresh_token)

                access_token  = data["access_token"]
                refresh_token = data.get("refresh_token", self.current_refresh_token)
                expires_in    = data.get("expires_in", 900)

                _write_local_cache(access_token, refresh_token, expires_in)
                self.current_refresh_token = refresh_token

                log.info(
                    f"[TokenKeeper] Token refreshed. "
                    f"Expires in {expires_in}s. "
                    f"Next refresh in {interval // 60} min."
                )
                self._ready_event.set()

            except Exception as e:
                log.error(f"[TokenKeeper] Refresh failed: {e}")

            for _ in range(interval):
                if self._stop_event.is_set():
                    break
                time.sleep(1)

    def wait_until_ready(self, timeout=30):
        if not self._ready_event.wait(timeout=timeout):
            raise RuntimeError(
                "Token keeper did not produce a token within 30 seconds.\n"
                "Check your refresh_token and cookies in CONFIG."
            )

    def stop(self):
        self._stop_event.set()


def get_local_token() -> str:
    cache      = _read_local_cache()
    expires_at = cache.get("expires_at", 0)
    buffer     = 60

    if time.time() < (expires_at - buffer):
        return cache["access_token"]

    log.info("Local token near expiry. Refreshing inline...")
    refresh_token = cache.get("refresh_token", CONFIG["refresh_token"])
    data          = call_token_api(refresh_token)
    _write_local_cache(
        data["access_token"],
        data.get("refresh_token", refresh_token),
        data.get("expires_in", 900),
    )
    return data["access_token"]


# ══════════════════════════════════════════════════════════════
#  PROD MODE — Read token from AWS SSM
# ══════════════════════════════════════════════════════════════
def _get_ssm_client():
    try:
        import boto3
        return boto3.client("ssm", region_name=CONFIG["aws_region"])
    except ImportError:
        raise RuntimeError("boto3 not installed. Run: pip install boto3")


def get_prod_token() -> str:
    ssm = _get_ssm_client()

    try:
        expires_at = float(
            ssm.get_parameter(Name=SSM_EXPIRES_AT)["Parameter"]["Value"]
        )
    except Exception:
        expires_at = 0

    buffer = 60

    if time.time() < (expires_at - buffer):
        token     = ssm.get_parameter(Name=SSM_ACCESS_TOKEN, WithDecryption=True)["Parameter"]["Value"]
        remaining = int(expires_at - time.time())
        log.info(f"Using SSM token. Expires in {remaining}s.")
        return token

    log.info("SSM token near expiry. Refreshing manually...")
    refresh_token = ssm.get_parameter(
        Name=SSM_REFRESH_TOKEN, WithDecryption=True
    )["Parameter"]["Value"]

    data         = call_token_api(refresh_token)
    access_token = data["access_token"]
    new_refresh  = data.get("refresh_token", refresh_token)
    expires_in   = data.get("expires_in", 900)

    ssm.put_parameter(Name=SSM_ACCESS_TOKEN,  Value=access_token,                Type="SecureString", Overwrite=True)
    ssm.put_parameter(Name=SSM_REFRESH_TOKEN, Value=new_refresh,                 Type="SecureString", Overwrite=True)
    ssm.put_parameter(Name=SSM_EXPIRES_AT,    Value=str(time.time()+expires_in), Type="String",       Overwrite=True)

    log.info(f"Token refreshed. Expires in {expires_in}s.")
    return access_token


# ══════════════════════════════════════════════════════════════
#  BUSINESS API CALLS
# ══════════════════════════════════════════════════════════════
def fetch_policy_number(token: str, loan_account: str) -> str:
    headers = {
        **_COMMON_HEADERS,
        "accept":        "application/json, text/plain, */*",
        "authorization": f"Bearer {token}",
        "companycode":   CONFIG["company_code"],
        "userid":        CONFIG["user_id"],
    }

    resp = _get_session().get(
        POLICY_FETCH_URL,
        headers=headers,
        params={"loanAccountNumber": loan_account},
        timeout=15,
    )

    if resp.status_code != 200:
        raise RuntimeError(
            f"Policy fetch failed [{resp.status_code}]: {resp.text[:300]}"
        )

    data = resp.json()

    if not data:
        raise ValueError(f"No policy found for loan: {loan_account}")

    # Normalise to list
    records = data if isinstance(data, list) else [data]

    policy_keys = ("policyNumber", "policy_number", "PolicyNumber", "POLICY_NUMBER")

    # Priority 1: EFFECTIVE policy
    for rec in records:
        if isinstance(rec, dict) and rec.get("policyStatus", "").upper() == "EFFECTIVE":
            for key in policy_keys:
                if rec.get(key):
                    return str(rec[key])

    # Priority 2: any policy (fallback — pick latest by number)
    all_policies = []
    for rec in records:
        if isinstance(rec, dict):
            for key in policy_keys:
                if rec.get(key):
                    all_policies.append(str(rec[key]))
                    break

    if not all_policies:
        raise ValueError(f"No policy number found. Response: {json.dumps(data)}")

    best = sorted(all_policies)[-1]
    log.warning(f"No EFFECTIVE policy for {loan_account}. Using latest: {best}")
    return best


def download_coi_pdf(token: str, policy_number: str, product_type: str) -> bytes:
    headers = {
        **_COMMON_HEADERS,
        "accept":        "application/json, text/plain, */*",
        "authorization": f"Bearer {token}",
        "companycode":   CONFIG["company_code"],
        "userid":        CONFIG["user_id"],
        "content-type":  "application/json",
    }
    payload = {
        "toDate":            None,
        "fromDate":          None,
        "reportName":        "Coi Document",
        "imdCode":           None,
        "machineIp":         None,
        "webUserId":         CONFIG["user_id"],
        "employeeCode":      [],
        "role":              [],
        "consolidated":      None,
        "communicationFlag": False,
        "createActivityId":  False,
        "productType":       product_type,
        "forceRegenerate":   False,
        "policyNumber":      policy_number,
        "quoteFlag":         False,
    }

    resp = _get_session().post(
        DOC_DOWNLOAD_URL, headers=headers, json=payload, timeout=30
    )

    if resp.status_code != 200:
        raise RuntimeError(
            f"Download failed [{resp.status_code}]: {resp.text[:300]}"
        )

    content_type = resp.headers.get("Content-Type", "")

    # Direct PDF binary
    if resp.content[:4] == b"%PDF" or "pdf" in content_type:
        return resp.content

    # JSON wrapper
    if "json" in content_type:
        data = resp.json()

        # base64 encoded PDF
        for key in ("document", "fileContent", "content", "data", "pdf"):
            val = data.get(key)
            if val and isinstance(val, str):
                try:
                    decoded = base64.b64decode(val)
                    if decoded[:4] == b"%PDF":
                        return decoded
                except Exception:
                    pass

        # Direct URL in response
        for key in ("url", "downloadUrl", "fileUrl", "documentUrl"):
            url = data.get(key)
            if url:
                return requests.get(url, timeout=60).content

        # schedulePath — confirmed working pattern from GoDigit API
        schedule_path = data.get("schedulePath")
        if schedule_path:
            pdf_resp = _get_session().get(
                schedule_path,
                headers={
                    **_COMMON_HEADERS,
                    "authorization": f"Bearer {token}",
                    "companycode":   CONFIG["company_code"],
                    "userid":        CONFIG["user_id"],
                },
                timeout=30,
            )
            if pdf_resp.status_code == 200 and pdf_resp.content[:4] == b"%PDF":
                return pdf_resp.content

        raise RuntimeError(
            f"PDF not found. Full API response: {json.dumps(data)}"
        )

    return resp.content


# ══════════════════════════════════════════════════════════════
#  EXCEL LOADER
# ══════════════════════════════════════════════════════════════
def load_excel() -> pd.DataFrame:
    path = CONFIG["excel_path"]

    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Excel file not found: {path}\n"
            f"Make sure loan_accounts.xlsx is in: D:\\Digit COI downloader\\"
        )

    df = pd.read_excel(path, dtype=str)
    df.columns = df.columns.str.strip()

    col_map = {
        CONFIG["col_loan_account"].strip(): "loan_account",
        CONFIG["col_product_type"].strip(): "product_type",
    }

    missing = [c for c in col_map if c not in df.columns]
    if missing:
        raise ValueError(
            f"Column(s) not found in Excel: {missing}\n"
            f"Your Excel has these columns: {list(df.columns)}\n"
            f"Expected: {list(col_map.keys())}"
        )

    df = df.rename(columns=col_map)[["loan_account", "product_type"]]
    df = df.dropna(subset=["loan_account"])
    df["loan_account"] = df["loan_account"].str.strip()
    df["product_type"] = df["product_type"].str.strip().str.upper()
    return df


# ══════════════════════════════════════════════════════════════
#  RESULTS TRACKER
# ══════════════════════════════════════════════════════════════
class Results:
    def __init__(self):
        self.rows = []

    def add(self, loan, product, policy, status, note, filepath=""):
        self.rows.append({
            "Loan Account Number": loan,
            "Product Type":        product,
            "Policy Number":       policy,
            "Status":              status,
            "Note":                note,
            "File Saved":          filepath,
            "Timestamp":           datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    def save_report(self):
        if not self.rows:
            return
        output_dir = CONFIG["output_dir"]
        ts         = datetime.now().strftime("%Y%m%d_%H%M%S")
        path       = os.path.join(output_dir, f"download_report_{ts}.xlsx")
        pd.DataFrame(self.rows).to_excel(path, index=False)
        log.info(f"Report saved: {path}")

    def print_summary(self):
        total   = len(self.rows)
        success = sum(1 for r in self.rows if r["Status"] == "SUCCESS")
        failed  = total - success
        log.info("=" * 55)
        log.info(f"  Total : {total}  |  Success : {success}  |  Failed : {failed}")
        log.info("=" * 55)
        if failed:
            log.warning("Failed rows:")
            for r in self.rows:
                if r["Status"] != "SUCCESS":
                    log.warning(f"  - {r['Loan Account Number']} -- {r['Note']}")


# ══════════════════════════════════════════════════════════════
#  DOWNLOADER
# ══════════════════════════════════════════════════════════════
def _process_row(args):
    """
    Processes a single row: fetch policy → download PDF → save.
    Retries up to MAX_RETRIES times on failure.
    Runs in a thread pool for parallel execution.
    """
    row_num, total, loan, product, output_dir, get_token_fn = args
    MAX_RETRIES = 5
    last_error  = None
    policy_num  = ""

    log.info(f"[{row_num}/{total}] Loan: {loan}  |  Product: {product}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if attempt > 1:
                wait = attempt * 3  # 6s, 9s, 12s, 15s between retries
                log.info(f"  [{loan}] Retry {attempt}/{MAX_RETRIES} in {wait}s...")
                time.sleep(wait)

            # Fetch token once per attempt — valid 15 min, reuse for both calls
            token      = get_token_fn()
            policy_num = fetch_policy_number(token, loan)
            log.info(f"  [{loan}] Policy : {policy_num}")

            pdf_bytes = download_coi_pdf(token, policy_num, product)

            filename = f"COI_{loan}_{policy_num}_{product}.pdf"
            filepath = os.path.join(output_dir, filename)
            with open(filepath, "wb") as f:
                f.write(pdf_bytes)

            log.info(f"  [{loan}] Saved  : {filename}  ({len(pdf_bytes)/1024:.1f} KB)")
            return (loan, product, policy_num, "SUCCESS", "", filepath)

        except RuntimeError as e:
            last_error = e
            err_str = str(e)
            # Rate limited — wait longer before retrying (don't burn attempts)
            if "429" in err_str:
                wait = 30 * attempt
                log.warning(f"  [{loan}] Rate limited (429). Waiting {wait}s before retry...")
                time.sleep(wait)
                continue
            log.warning(f"  [{loan}] Attempt {attempt}/{MAX_RETRIES} failed: {e}")

        except Exception as e:
            last_error = e
            log.warning(f"  [{loan}] Attempt {attempt}/{MAX_RETRIES} failed: {e}")

    log.error(f"  [{loan}] FAILED after {MAX_RETRIES} attempts: {last_error}")
    return (loan, product, policy_num, "FAILED", str(last_error), "")


def run_downloader(get_token_fn):
    from concurrent.futures import ThreadPoolExecutor, as_completed

    output_dir  = CONFIG["output_dir"]
    max_workers = CONFIG.get("max_workers", 10)
    batch_size  = CONFIG.get("batch_size", 50)
    batch_pause = CONFIG.get("batch_pause_secs", 2)

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    df      = load_excel()
    total   = len(df)
    results = Results()

    MAX_WORKERS = min(max_workers, total)

    log.info(f"Loaded {total} row(s) from Excel.")
    log.info(f"PDFs will be saved to: {output_dir}")
    log.info(f"Workers: {MAX_WORKERS}  |  Batch size: {batch_size}  |  Batch pause: {batch_pause}s")

    # Build full args list
    args_list = [
        (idx + 1, total, row["loan_account"], row["product_type"], output_dir, get_token_fn)
        for idx, row in df.iterrows()
    ]

    # Split into batches if batch_size is set
    if batch_size and batch_size > 0 and total > batch_size:
        batches = [args_list[i:i+batch_size] for i in range(0, total, batch_size)]
    else:
        batches = [args_list]

    total_batches = len(batches)
    completed     = 0

    for batch_num, batch in enumerate(batches, 1):
        if total_batches > 1:
            log.info(f"\n--- Batch {batch_num}/{total_batches} ({len(batch)} rows) ---")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_process_row, args): args for args in batch}
            for future in as_completed(futures):
                loan, product, policy_num, status, note, filepath = future.result()
                results.add(loan, product, policy_num, status, note, filepath)
                completed += 1
                # Live progress
                success_so_far = sum(1 for r in results.rows if r["Status"] == "SUCCESS")
                log.info(f"  Progress: {completed}/{total} done  |  Success: {success_so_far}  |  Failed: {completed - success_so_far}")

        # Pause between batches to avoid rate limiting
        if batch_num < total_batches:
            log.info(f"  Batch {batch_num} done. Pausing {batch_pause}s before next batch...")
            time.sleep(batch_pause)

    results.print_summary()
    results.save_report()


# ══════════════════════════════════════════════════════════════
#  SETUP MODE
# ══════════════════════════════════════════════════════════════
def run_setup():
    try:
        import boto3
    except ImportError:
        log.error("boto3 not installed. Run: pip install boto3")
        sys.exit(1)

    region = CONFIG["aws_region"]
    log.info("=" * 55)
    log.info("  GoDigit — AWS Lambda Setup")
    log.info("=" * 55)

    iam    = boto3.client("iam",    region_name=region)
    lam    = boto3.client("lambda", region_name=region)
    ssm    = boto3.client("ssm",    region_name=region)
    events = boto3.client("events", region_name=region)
    sts    = boto3.client("sts",    region_name=region)

    account_id = sts.get_caller_identity()["Account"]
    log.info(f"AWS Account : {account_id}")
    log.info(f"AWS Region  : {region}")

    log.info("\n[1/6] Creating IAM role...")
    trust = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}],
    })
    try:
        iam.create_role(RoleName=LAMBDA_ROLE, AssumeRolePolicyDocument=trust)
        log.info("  IAM role created.")
    except iam.exceptions.EntityAlreadyExistsException:
        log.info("  IAM role already exists.")

    iam.attach_role_policy(RoleName=LAMBDA_ROLE, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
    iam.put_role_policy(
        RoleName=LAMBDA_ROLE,
        PolicyName="GoDigitSSMAccess",
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": ["ssm:GetParameter", "ssm:PutParameter"], "Resource": f"arn:aws:ssm:{region}:{account_id}:parameter/godigit/*"}],
        }),
    )
    role_arn = f"arn:aws:iam::{account_id}:role/{LAMBDA_ROLE}"
    log.info(f"  Role ARN: {role_arn}")
    log.info("  Waiting 10s for IAM to propagate...")
    time.sleep(10)

    log.info("\n[2/6] Seeding tokens into SSM...")
    for name, value, secure in [
        (SSM_REFRESH_TOKEN, CONFIG["refresh_token"], True),
        (SSM_ACCESS_TOKEN,  "pending_first_refresh", True),
        (SSM_EXPIRES_AT,    "0",                     False),
    ]:
        ssm.put_parameter(Name=name, Value=value, Type="SecureString" if secure else "String", Overwrite=True)
    log.info("  SSM parameters created.")

    log.info("\n[3/6] Deploying Lambda function...")
    env_vars = {
        "Variables": {
            "SSM_ACCESS_TOKEN_PATH":  SSM_ACCESS_TOKEN,
            "SSM_REFRESH_TOKEN_PATH": SSM_REFRESH_TOKEN,
            "SSM_EXPIRES_AT_PATH":    SSM_EXPIRES_AT,
            **{f"COOKIE_{k}": v for k, v in CONFIG["cookies"].items()},
            "USER_ID":      CONFIG["user_id"],
            "COMPANY_CODE": CONFIG["company_code"],
        }
    }
    zip_bytes = LAMBDA_ZIP.read_bytes()
    try:
        lam.get_function(FunctionName=LAMBDA_FUNCTION)
        lam.update_function_code(FunctionName=LAMBDA_FUNCTION, ZipFile=zip_bytes)
        lam.update_function_configuration(FunctionName=LAMBDA_FUNCTION, Environment=env_vars)
        log.info("  Lambda function updated.")
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=LAMBDA_FUNCTION, Runtime="python3.12", Role=role_arn, Handler="lambda_token_keeper.handler", Code={"ZipFile": zip_bytes}, Timeout=30, MemorySize=128, Environment=env_vars)
        log.info("  Lambda function created.")

    func_arn = f"arn:aws:lambda:{region}:{account_id}:function:{LAMBDA_FUNCTION}"

    log.info("\n[4/6] Creating EventBridge schedule...")
    events.put_rule(Name=EVENTBRIDGE_RULE, ScheduleExpression="rate(10 minutes)", State="ENABLED")

    log.info("\n[5/6] Granting EventBridge permission...")
    try:
        lam.add_permission(FunctionName=LAMBDA_FUNCTION, StatementId="EventBridgeInvoke", Action="lambda:InvokeFunction", Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:{region}:{account_id}:rule/{EVENTBRIDGE_RULE}")
    except lam.exceptions.ResourceConflictException:
        log.info("  Permission already exists.")
    events.put_targets(Rule=EVENTBRIDGE_RULE, Targets=[{"Id": "1", "Arn": func_arn}])

    log.info("\n[6/6] Testing Lambda...")
    resp   = lam.invoke(FunctionName=LAMBDA_FUNCTION, LogType="Tail", Payload=b"{}")
    result = json.loads(resp["Payload"].read())
    log.info(f"  Status : {resp['StatusCode']}")
    log.info(f"  Result : {result}")

    if result.get("statusCode") == 200:
        log.info("\n" + "=" * 55)
        log.info("  SETUP COMPLETE!")
        log.info("  Lambda runs every 10 minutes automatically.")
        log.info("  Now run: python godigit_coi.py --mode prod")
        log.info("=" * 55)
    else:
        log.error("  Lambda test failed. Check:")
        log.error(f"  aws logs tail /aws/lambda/{LAMBDA_FUNCTION} --follow --region {region}")


# ══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="GoDigit Bulk COI Downloader", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--mode", choices=["local", "prod"], help="local: background thread\nprod: AWS SSM token")
    parser.add_argument("--setup", action="store_true", help="One-time AWS setup")
    args = parser.parse_args()

    if args.setup:
        validate_config("setup")
        run_setup()
        return

    if not args.mode:
        parser.print_help()
        print("\nExamples:")
        print("  python godigit_coi.py --mode local")
        print("  python godigit_coi.py --setup")
        print("  python godigit_coi.py --mode prod")
        return

    validate_config(args.mode)

    log.info("=" * 55)
    log.info(f"  GoDigit Bulk COI Downloader  [{args.mode.upper()} MODE]")
    log.info("=" * 55)

    if args.mode == "local":
        log.info("Starting local token keeper thread...")
        keeper = LocalTokenKeeper()
        keeper.start()
        log.info("Waiting for first token to be ready...")
        keeper.wait_until_ready(timeout=30)
        log.info("Token is ready. Starting downloads...\n")
        try:
            run_downloader(get_local_token)
        finally:
            keeper.stop()
            log.info("Token keeper stopped.")

    elif args.mode == "prod":
        log.info("Reading token from AWS SSM...")
        run_downloader(get_prod_token)


if __name__ == "__main__":
    main()