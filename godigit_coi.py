"""
godigit_coi.py
==============
Complete self-sustaining GoDigit COI Downloader.

Flow on every run:
  1. Validate token with a real API call
  2. If valid → download PDFs directly
  3. If invalid (cookie expired) → automatically trigger OTP login
  4. You enter OTP → fresh cookies + tokens saved
  5. Lambda updated with new refresh token
  6. Downloads PDFs
  7. Lambda keeps refreshing token every 10 min in background

Usage:
    pip install requests pandas openpyxl boto3 beautifulsoup4

    python godigit_coi.py --mode local    # local testing
    python godigit_coi.py --mode prod     # production
    python godigit_coi.py --setup         # deploy Lambda (one time)
"""

import os
import sys
import json
import time
import uuid
import base64
import hashlib
import secrets
import logging
import argparse
import threading
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlparse, parse_qs, urljoin
import pandas as pd
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup

# ══════════════════════════════════════════════════════════════
#  CONFIG — Fill in your details before running
# ══════════════════════════════════════════════════════════════
CONFIG = {
    # ── File Paths ─────────────────────────────────────────────
    "excel_path": r"D:\Digit COI downloader\loan_accounts.xlsx",
    "output_dir": r"D:\Digit COI downloader\coi_downloads",

    # ── Your registered mobile number (for OTP login) ──────────
    # Enter your 10-digit registered mobile number here
    "mobile": "",

    # ── AWS Settings (for prod mode) ───────────────────────────
    "aws_region": "ap-south-1",

    # ── Behaviour ──────────────────────────────────────────────
    # For 500+ rows: workers=20 gives ~2x speedup over 10
    # Reduce max_workers to 5 if you see 429 rate limit errors
    "max_workers":      20,

    # Batch size: process N rows before checking for 429s
    # Set to 0 to disable batching entirely
    "batch_size":       100,

    # Pause between batches in seconds
    # Only applied if a 429 was seen in the previous batch
    "batch_pause_secs": 2,

    # ── Excel Column Names ─────────────────────────────────────
    "col_loan_account": "Loan Account Number",
    "col_product_type": "Product Type",
}

# ══════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════
KEYCLOAK_BASE    = "https://accounts.godigit.com/auth/realms/ABS-21"
AUTH_URL         = f"{KEYCLOAK_BASE}/protocol/openid-connect/auth"
TOKEN_URL        = f"{KEYCLOAK_BASE}/protocol/openid-connect/token"
POLICY_FETCH_URL = "https://prod-corporateservice.godigit.com/digitcorporateservice/tpa/search/childPolicy"
DOC_DOWNLOAD_URL = "https://prod-corporateservice.godigit.com/digitcorporateservice/document/download"
CLIENT_ID        = "DigitCorporate"
REDIRECT_URI     = "https://corporate.godigit.com/DigitCorporate/#/"
SCOPE            = "openid company_code userid Portal_Type"

SSM_ACCESS_TOKEN  = "/godigit/access_token"
SSM_REFRESH_TOKEN = "/godigit/refresh_token"
SSM_EXPIRES_AT    = "/godigit/expires_at"
SSM_COOKIES       = "/godigit/cookies"
SSM_USER_ID       = "/godigit/user_id"
SSM_COMPANY_CODE  = "/godigit/company_code"

LAMBDA_FUNCTION  = "godigit-token-keeper"
LAMBDA_ROLE      = "godigit-token-keeper-role"
EVENTBRIDGE_RULE = "godigit-token-keeper-schedule"
LAMBDA_ZIP       = Path(__file__).parent / "lambda_token_keeper.zip"
TOKEN_CACHE_FILE = Path(__file__).parent / "token_cache.json"
LOG_FILE         = Path(__file__).parent / "godigit_coi.log"

_BROWSER_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control":   "no-cache",
    "Pragma":          "no-cache",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

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

# ── HTTP Session pool ──────────────────────────────────────────
_thread_local = threading.local()

def _get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        # Pool of 20 — each worker uses 2 connections (policy + pdf)
        a = HTTPAdapter(pool_connections=20, pool_maxsize=20, max_retries=Retry(total=0))
        s.mount("https://", a)
        s.mount("http://",  a)
        _thread_local.session = s
    return _thread_local.session


def log_row(level: str, loan: str, policy: str, status: str, message: str = ""):
    line = f"LOAN={loan:<12} | POLICY={policy:<14} | STATUS={status:<10} | MSG={message}"
    getattr(log, level.lower(), log.info)(line)


# ══════════════════════════════════════════════════════════════
#  CACHE — Read / Write token_cache.json
# ══════════════════════════════════════════════════════════════
def read_cache() -> dict:
    if not TOKEN_CACHE_FILE.exists():
        return {}
    try:
        return json.loads(TOKEN_CACHE_FILE.read_text())
    except Exception:
        return {}


def write_cache(data: dict):
    tmp = TOKEN_CACHE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    tmp.replace(TOKEN_CACHE_FILE)


# ══════════════════════════════════════════════════════════════
#  SSM CLIENT
# ══════════════════════════════════════════════════════════════
def _get_ssm_client():
    try:
        import boto3
        return boto3.client("ssm", region_name=CONFIG["aws_region"])
    except ImportError:
        raise RuntimeError("boto3 not installed. Run: pip install boto3")


# ══════════════════════════════════════════════════════════════
#  IN-MEMORY CACHE — avoids repeated disk reads during downloads
# ══════════════════════════════════════════════════════════════
# Populated once at startup, reused by all worker threads
_MEM_CACHE: dict = {
    "user_id":      "",
    "company_code": "LI",
    "access_token": "",
    "expires_at":   0.0,
    "cookies":      {},
}
_MEM_LOCK = threading.Lock()


def _mem_set(key: str, value):
    with _MEM_LOCK:
        _MEM_CACHE[key] = value


def _mem_get(key: str):
    return _MEM_CACHE.get(key)


def _mem_update(data: dict):
    """Bulk update in-memory cache from a dict."""
    with _MEM_LOCK:
        _MEM_CACHE.update(data)


# ══════════════════════════════════════════════════════════════
#  IDENTITY — user_id and company_code from cache/SSM
# ══════════════════════════════════════════════════════════════
def get_identity() -> tuple:
    """
    Returns (user_id, company_code).
    Checks in-memory cache first — zero disk/network reads during downloads.
    Falls back to disk cache or SSM only on first call.
    """
    # 1. In-memory (fastest — all worker threads share this)
    uid = _MEM_CACHE.get("user_id", "")
    cc  = _MEM_CACHE.get("company_code", "LI")
    if uid:
        return uid, cc

    # 2. Disk cache (token_cache.json)
    cache = read_cache()
    uid   = cache.get("user_id", "")
    cc    = cache.get("company_code", "LI")
    if uid:
        _mem_update({"user_id": uid, "company_code": cc})
        return uid, cc

    # 3. SSM (prod mode)
    try:
        ssm = _get_ssm_client()
        uid = ssm.get_parameter(Name=SSM_USER_ID)["Parameter"]["Value"]
        cc  = ssm.get_parameter(Name=SSM_COMPANY_CODE)["Parameter"]["Value"]
        _mem_update({"user_id": uid, "company_code": cc})
        return uid, cc
    except Exception:
        return "", "LI"


def get_cookies() -> dict:
    """Returns session cookies from cache or SSM."""
    cache   = read_cache()
    cookies = cache.get("cookies", {})
    if cookies:
        return cookies
    try:
        ssm = _get_ssm_client()
        raw = ssm.get_parameter(Name=SSM_COOKIES, WithDecryption=True)["Parameter"]["Value"]
        return json.loads(raw)
    except Exception:
        return {}


def decode_token_claims(access_token: str) -> dict:
    """Decodes JWT payload to extract userid, company_code etc."""
    try:
        payload = access_token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        return json.loads(base64.b64decode(payload).decode("utf-8"))
    except Exception:
        return {}


# ══════════════════════════════════════════════════════════════
#  TOKEN VALIDATION
# ══════════════════════════════════════════════════════════════
def validate_token(access_token: str) -> bool:
    """
    Makes a real API call to verify the token works.
    Catches cookie expiry where token is not time-expired
    but the server-side session is dead.
    """
    if not access_token:
        log.info("  [Token Check] No token found.")
        return False

    user_id, company_code = get_identity()
    if not user_id:
        log.info("  [Token Check] No user_id yet — assuming valid.")
        return True

    try:
        log.info("  [Token Check] Validating token with GoDigit API...")
        resp = _get_session().get(
            "https://prod-corporateservice.godigit.com/digitcorporateservice/loginEmployeeDetails",
            headers={
                **_COMMON_HEADERS,
                "accept":        "application/json, text/plain, */*",
                "authorization": f"Bearer {access_token}",
                "userid":        user_id,
                "companycode":   company_code,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            log.info("  [Token Check] ✓ Token is valid.")
            return True
        elif resp.status_code == 401:
            log.info("  [Token Check] ✗ Token rejected [401 Unauthorized] — session expired or cookies dead.")
            return False
        elif resp.status_code == 403:
            log.info("  [Token Check] ✗ Token rejected [403 Forbidden] — insufficient permissions.")
            return False
        else:
            log.info(f"  [Token Check] ✗ Token rejected [{resp.status_code}] — unexpected response.")
            return False
    except requests.exceptions.Timeout:
        log.info("  [Token Check] ✗ Validation timed out — assuming token invalid.")
        return False
    except requests.exceptions.ConnectionError:
        log.info("  [Token Check] ✗ No internet connection — assuming token invalid.")
        return False
    except Exception as e:
        log.info(f"  [Token Check] ✗ Validation error: {type(e).__name__}: {e}")
        return False


# ══════════════════════════════════════════════════════════════
#  OTP LOGIN FLOW
# ══════════════════════════════════════════════════════════════
def _generate_pkce() -> tuple:
    code_verifier  = base64.urlsafe_b64encode(secrets.token_bytes(64)).rstrip(b"=").decode()
    code_challenge = base64.urlsafe_b64encode(
        hashlib.sha256(code_verifier.encode()).digest()
    ).rstrip(b"=").decode()
    return code_verifier, code_challenge


def _get_username_form(session, code_challenge) -> tuple:
    params = {
        "client_id": CLIENT_ID, "redirect_uri": REDIRECT_URI,
        "response_type": "code", "scope": SCOPE,
        "code_challenge": code_challenge, "code_challenge_method": "S256",
        "nonce": str(uuid.uuid4()),
        "state": base64.urlsafe_b64encode(secrets.token_bytes(16)).rstrip(b"=").decode(),
    }
    resp = session.get(AUTH_URL, params=params, headers=_BROWSER_HEADERS, allow_redirects=True, timeout=30)
    if resp.status_code != 200:
        raise RuntimeError(f"Login page failed [{resp.status_code}]")
    soup   = BeautifulSoup(resp.text, "html.parser")
    form   = soup.find("form", id="kc-form-login") or soup.find("form", id="send-otp-form") or soup.find("form")
    if not form:
        raise RuntimeError(f"Login form not found. Page: {resp.text[:200]}")
    action = form.get("action")
    hidden = {i.get("name"): i.get("value", "") for i in form.find_all("input", type="hidden") if i.get("name")}
    return action, hidden, resp.url


def _submit_mobile_get_otp_form(session, action, hidden, referer) -> tuple:
    resp = session.post(
        action,
        data={**hidden, "username": CONFIG["mobile"]},
        headers={**_BROWSER_HEADERS, "Content-Type": "application/x-www-form-urlencoded", "Origin": "https://accounts.godigit.com", "Referer": referer},
        allow_redirects=True, timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Mobile submit failed [{resp.status_code}]")
    soup     = BeautifulSoup(resp.text, "html.parser")
    otp_form = soup.find("form", id="otp-form")
    if not otp_form:
        error = soup.find(class_="pf-c-alert__title") or soup.find(id="input-error")
        raise RuntimeError(f"OTP form not found: {error.get_text(strip=True) if error else resp.text[:200]}")
    otp_action = otp_form.get("action")
    otp_hidden = {i.get("name"): i.get("value", "") for i in otp_form.find_all("input", type="hidden") if i.get("name")}
    phone_inp  = soup.find("input", {"name": "phone"}) or soup.find("input", id="phone")
    phone      = phone_inp.get("value", "xxxxxxxxxx") if phone_inp else "xxxxxxxxxx"
    return otp_action, otp_hidden, phone, resp.url


def _submit_otp_get_auth_code(session, otp_action, otp_hidden, phone, otp, referer) -> str:
    resp = session.post(
        otp_action,
        data={**otp_hidden, "otp": otp, "phone": phone},
        headers={**_BROWSER_HEADERS, "Content-Type": "application/x-www-form-urlencoded", "Origin": "https://accounts.godigit.com", "Referer": referer},
        allow_redirects=False, timeout=30,
    )
    for _ in range(10):
        if resp.status_code not in (301, 302, 303, 307, 308):
            break
        location = resp.headers.get("Location", "")
        if "code=" in location:
            parsed = urlparse(location)
            for src in (parsed.fragment, parsed.query):
                qs   = parse_qs(src.lstrip("/").lstrip("#").lstrip("/?"))
                code = qs.get("code", [None])[0]
                if code:
                    return code
        next_url = location if location.startswith("http") else urljoin(otp_action, location)
        resp = session.get(next_url, headers=_BROWSER_HEADERS, allow_redirects=False, timeout=30)
    soup  = BeautifulSoup(resp.text, "html.parser")
    error = soup.find(id="input-error") or soup.find(class_="pf-c-alert__title")
    if error:
        raise RuntimeError(f"OTP rejected: {error.get_text(strip=True)}")
    raise RuntimeError(f"Auth code not found. Final status: {resp.status_code}")


def _exchange_code_for_tokens(session, auth_code, code_verifier) -> dict:
    resp = session.post(
        TOKEN_URL,
        headers={**_BROWSER_HEADERS, "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8", "Accept": "application/json", "Origin": "https://corporate.godigit.com"},
        data={"grant_type": "authorization_code", "code": auth_code, "code_verifier": code_verifier, "client_id": CLIENT_ID, "redirect_uri": REDIRECT_URI},
        timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Token exchange failed [{resp.status_code}]: {resp.text[:200]}")
    data = resp.json()
    if "access_token" not in data:
        raise RuntimeError(
            f"Token API did not return access_token. "
            f"Keys returned: {list(data.keys())}. "
            f"Response: {json.dumps(data)[:200]}"
        )
    return data


def do_otp_login() -> dict:
    """
    Full OTP login flow with unlimited retries.

    Retries the ENTIRE flow (including re-triggering OTP) on:
      - Wrong OTP entered
      - OTP service failure
      - Network error
      - Any other exception

    Only stops when:
      - Login succeeds
      - User presses Ctrl+C to kill the program
    """
    attempt = 0

    while True:
        attempt += 1
        try:
            log.info("=" * 55)
            if attempt == 1:
                log.info("  OTP Login Flow — Starting...")
            else:
                log.info(f"  OTP Login Flow — Attempt {attempt} (retrying)...")
            log.info("=" * 55)

            # Fresh session + PKCE on every attempt
            session                       = requests.Session()
            code_verifier, code_challenge = _generate_pkce()

            # ── Stage 1: Fetch login page ──────────────────────
            print("  [1/5] Connecting to GoDigit login page...")
            log.info("[Login] Stage 1/5 — Fetching Keycloak login page...")
            action, hidden, referer = _get_username_form(session, code_challenge)
            log.info("[Login] Stage 1/5 — ✓ Login page loaded.")

            # ── Stage 2: Submit mobile → trigger OTP ──────────
            print(f"  [2/5] Triggering OTP to mobile ...{CONFIG['mobile'][-4:]}...")
            log.info(f"[Login] Stage 2/5 — Submitting mobile number, triggering OTP...")
            otp_action, otp_hidden, phone, otp_referer = _submit_mobile_get_otp_form(
                session, action, hidden, referer
            )
            log.info(f"[Login] Stage 2/5 — ✓ OTP sent to mobile ending in ...{phone[-4:]}.")

            # ── Stage 3: Get OTP from user ─────────────────────
            print()
            print("  " + "=" * 48)
            print(f"  [3/5] OTP sent to mobile ending in: ...{phone[-4:]}")
            print(f"        (Press Ctrl+C anytime to cancel)")
            print("  " + "=" * 48)

            while True:
                otp = input("         Enter OTP: ").strip()
                if otp.isdigit() and len(otp) == 6:
                    break
                print("         ✗ Invalid. Please enter a 6-digit OTP.")

            # ── Stage 4: Submit OTP → get auth code ───────────
            print(f"  [4/5] Verifying OTP with Keycloak...")
            log.info("[Login] Stage 4/5 — Submitting OTP to Keycloak...")
            auth_code = _submit_otp_get_auth_code(
                session, otp_action, otp_hidden, phone, otp, otp_referer
            )
            log.info("[Login] Stage 4/5 — ✓ OTP verified. Auth code received.")

            # ── Stage 5: Exchange auth code for tokens ─────────
            print(f"  [5/5] Generating access token and refresh token...")
            log.info("[Login] Stage 5/5 — Exchanging auth code for tokens...")
            token_data = _exchange_code_for_tokens(session, auth_code, code_verifier)
            log.info(f"[Login] Stage 5/5 — ✓ Tokens generated. Expires in {token_data.get('expires_in')}s.")

            # ── Extract cookies + identity ─────────────────────
            cookies      = {c.name: c.value for c in session.cookies if c.name in ["AUTH_SESSION_ID", "AUTH_SESSION_ID_LEGACY", "KEYCLOAK_SESSION", "KEYCLOAK_SESSION_LEGACY", "KEYCLOAK_IDENTITY", "KEYCLOAK_IDENTITY_LEGACY"]}
            claims       = decode_token_claims(token_data["access_token"])
            user_id      = claims.get("userid", "")
            company_code = claims.get("company_code", "LI")

            result = {
                "access_token":  token_data["access_token"],
                "refresh_token": token_data.get("refresh_token", ""),
                "expires_in":    token_data.get("expires_in", 900),
                "expires_at":    time.time() + token_data.get("expires_in", 900),
                "cookies":       cookies,
                "user_id":       user_id,
                "company_code":  company_code,
                "logged_in_at":  datetime.now().isoformat(),
            }

            cookie_count = len([v for v in cookies.values() if v])
            log.info(f"[Login] ✓ {cookie_count}/6 session cookies captured.")

            print()
            print("  " + "=" * 48)
            print(f"  ✓ Login successful!")
            print(f"  ✓ Access token generated (valid 15 min)")
            print(f"  ✓ Refresh token generated (Lambda rotates every 10 min)")
            print(f"  ✓ {cookie_count} session cookies captured (valid ~3 days)")
            print(f"  ✓ User ID: {user_id}  |  Company: {company_code}")
            print("  " + "=" * 48)
            log.info("=" * 55)
            log.info("  Login complete. Proceeding to download PDFs...")
            log.info("=" * 55)
            return result

        except KeyboardInterrupt:
            # User pressed Ctrl+C — exit cleanly
            print()
            log.info("Login cancelled by user (Ctrl+C). Exiting.")
            sys.exit(0)

        except RuntimeError as e:
            err = str(e)
            print()
            if "OTP rejected" in err or "wrong" in err.lower() or "invalid" in err.lower():
                log.warning(f"  Wrong OTP entered. Retrying login flow...")
            else:
                log.warning(f"  Login failed: {err}")
                log.warning(f"  Retrying automatically in 3 seconds...")
                time.sleep(3)

        except Exception as e:
            print()
            log.warning(f"  Login error: {e}")
            log.warning(f"  Retrying automatically in 3 seconds...")
            time.sleep(3)


def _save_credentials(data: dict, mode: str):
    """Saves credentials to local cache and optionally SSM."""
    write_cache(data)
    print(f"  ✓ Credentials saved to local cache.")
    log.info(f"[Save] ✓ Credentials saved to: {TOKEN_CACHE_FILE}")

    if mode == "prod":
        ssm = _get_ssm_client()
        ssm.put_parameter(Name=SSM_ACCESS_TOKEN,  Value=data["access_token"],        Type="SecureString", Overwrite=True)
        ssm.put_parameter(Name=SSM_REFRESH_TOKEN, Value=data["refresh_token"],       Type="SecureString", Overwrite=True)
        ssm.put_parameter(Name=SSM_EXPIRES_AT,    Value=str(data["expires_at"]),     Type="String",       Overwrite=True)
        ssm.put_parameter(Name=SSM_COOKIES,       Value=json.dumps(data["cookies"]), Type="SecureString", Overwrite=True)
        ssm.put_parameter(Name=SSM_USER_ID,       Value=data.get("user_id", ""),     Type="String",       Overwrite=True)
        ssm.put_parameter(Name=SSM_COMPANY_CODE,  Value=data.get("company_code",""), Type="String",       Overwrite=True)
        print("  ✓ Credentials pushed to AWS SSM. Lambda will pick up new refresh_token.")
        log.info("[Save] ✓ Credentials saved to SSM. Lambda will rotate token every 10 min.")


# ══════════════════════════════════════════════════════════════
#  TOKEN MANAGER — Core function
# ══════════════════════════════════════════════════════════════
def get_valid_token(mode: str) -> str:
    """
    Gets a valid access token.

    On every call:
      1. Read token from cache (local) or SSM (prod)
      2. Validate with real API call
      3. If valid → return token
      4. If invalid → try refresh with existing refresh_token
      5. If refresh fails (cookie expired) → trigger OTP login automatically
      6. Save fresh credentials to cache + SSM
      7. Return new token

    This is the heart of the self-sustaining flow.
    OTP login is triggered automatically — user only needs to enter the OTP.
    """

    # ── Read current token ─────────────────────────────────────
    if mode == "local":
        cache      = read_cache()
        token      = cache.get("access_token", "")
        expires_at = cache.get("expires_at", 0)
    else:
        try:
            ssm        = _get_ssm_client()
            token      = ssm.get_parameter(Name=SSM_ACCESS_TOKEN, WithDecryption=True)["Parameter"]["Value"]
            expires_at = float(ssm.get_parameter(Name=SSM_EXPIRES_AT)["Parameter"]["Value"])
        except Exception:
            token = ""
            expires_at = 0

    # ── Validate token with real API call ──────────────────────
    log.info("Checking token validity...")
    if token and time.time() < (expires_at - 60) and validate_token(token):
        log.info(f"Token valid. Expires in {int(expires_at - time.time())}s.")
        return token

    log.info("Token invalid or expired. Attempting refresh...")

    # ── Try token refresh ──────────────────────────────────────
    if mode == "local":
        cache         = read_cache()
        refresh_token = cache.get("refresh_token", "")
        cookies       = cache.get("cookies", {})
    else:
        try:
            ssm           = _get_ssm_client()
            refresh_token = ssm.get_parameter(Name=SSM_REFRESH_TOKEN, WithDecryption=True)["Parameter"]["Value"]
            cookies       = json.loads(ssm.get_parameter(Name=SSM_COOKIES, WithDecryption=True)["Parameter"]["Value"])
        except Exception:
            refresh_token = ""
            cookies       = {}

    if refresh_token and cookies:
        try:
            resp = requests.post(
                TOKEN_URL,
                headers={**_COMMON_HEADERS, "accept": "application/json", "content-type": "application/x-www-form-urlencoded;charset=UTF-8"},
                cookies=cookies,
                data={"grant_type": "refresh_token", "refresh_token": refresh_token, "client_id": CLIENT_ID},
                timeout=30,
            )
            if resp.status_code == 200:
                data          = resp.json()
                new_token     = data.get("access_token", "")
                new_refresh   = data.get("refresh_token", refresh_token)
                expires_in    = data.get("expires_in", 900)

                # Validate the refreshed token
                if new_token and validate_token(new_token):
                    print("  ✓ Token refreshed successfully.")
                    log.info("[Token] ✓ Token refreshed and validated successfully.")
                    # Update cache
                    if mode == "local":
                        cache = read_cache()
                        cache.update({"access_token": new_token, "refresh_token": new_refresh, "expires_at": time.time() + expires_in})
                        write_cache(cache)
                    else:
                        ssm = _get_ssm_client()
                        ssm.put_parameter(Name=SSM_ACCESS_TOKEN,  Value=new_token,                      Type="SecureString", Overwrite=True)
                        ssm.put_parameter(Name=SSM_REFRESH_TOKEN, Value=new_refresh,                    Type="SecureString", Overwrite=True)
                        ssm.put_parameter(Name=SSM_EXPIRES_AT,    Value=str(time.time()+expires_in),    Type="String",       Overwrite=True)
                    return new_token
                else:
                    print("  ✗ Token refresh failed — cookies have expired.")
                    log.warning("[Token] ✗ Refreshed token failed API validation. Cookies likely expired.")
            else:
                print(f"  ✗ Token refresh API returned [{resp.status_code}] — cookies likely expired.")
                log.warning(f"[Token] ✗ Token refresh API returned [{resp.status_code}]. Cookies likely expired.")
        except requests.exceptions.Timeout:
            print("  ✗ Token refresh timed out — GoDigit server not responding.")
            log.warning("[Token] ✗ Token refresh timed out (>30s). Server may be down.")
        except requests.exceptions.ConnectionError:
            print("  ✗ Token refresh failed — no internet connection.")
            log.warning("[Token] ✗ Token refresh failed — connection error. Check internet.")
        except Exception as e:
            print(f"  ✗ Token refresh error: {e}")
            log.warning(f"[Token] ✗ Token refresh unexpected error: {type(e).__name__}: {e}")

    # ── Refresh failed → auto trigger OTP login ────────────────
    log.info("=" * 55)
    log.info("  Token refresh failed — cookies have expired.")
    log.info("  Automatically starting OTP login...")
    log.info("=" * 55)

    # Validate mobile is set before starting OTP flow
    if not CONFIG.get("mobile") or len(CONFIG["mobile"].strip()) != 10 or not CONFIG["mobile"].strip().isdigit():
        log.error("=" * 55)
        log.error("  ERROR: 'mobile' not set correctly in CONFIG.")
        log.error("  Open godigit_coi.py and set your 10-digit")
        log.error("  registered mobile number in the CONFIG section.")
        log.error("=" * 55)
        sys.exit(1)

    login_data = do_otp_login()
    _save_credentials(login_data, mode)
    return login_data["access_token"]


# ══════════════════════════════════════════════════════════════
#  LOCAL TOKEN KEEPER — background thread (mimics Lambda)
# ══════════════════════════════════════════════════════════════
class LocalTokenKeeper(threading.Thread):
    """Keeps token alive in local mode by refreshing every 10 min."""

    def __init__(self):
        super().__init__(daemon=True)
        self._stop  = threading.Event()
        self._ready = threading.Event()

    def run(self):
        log.info("[TokenKeeper] Background thread started.")
        while not self._stop.is_set():
            try:
                cache         = read_cache()
                refresh_token = cache.get("refresh_token", "")
                cookies       = cache.get("cookies", {})

                if refresh_token and cookies:
                    resp = requests.post(
                        TOKEN_URL,
                        headers={**_COMMON_HEADERS, "accept": "application/json", "content-type": "application/x-www-form-urlencoded;charset=UTF-8"},
                        cookies=cookies,
                        data={"grant_type": "refresh_token", "refresh_token": refresh_token, "client_id": CLIENT_ID},
                        timeout=30,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        cache.update({
                            "access_token":  data["access_token"],
                            "refresh_token": data.get("refresh_token", refresh_token),
                            "expires_at":    time.time() + data.get("expires_in", 900),
                        })
                        write_cache(cache)
                        print(f"  [Background] ✓ Token refreshed automatically. Expires in {data.get('expires_in')}s.")
                        log.info(f"[TokenKeeper] ✓ Token refreshed. Expires in {data.get('expires_in')}s.")
                    else:
                        body = resp.text[:150] if resp.text else "no body"
                        if resp.status_code == 400:
                            log.warning(f"[TokenKeeper] ✗ Refresh failed [400 Bad Request] — refresh token likely expired or revoked. Body: {body}")
                        elif resp.status_code == 401:
                            log.warning(f"[TokenKeeper] ✗ Refresh failed [401 Unauthorized] — cookies expired. OTP login needed. Body: {body}")
                        else:
                            log.warning(f"[TokenKeeper] ✗ Refresh failed [{resp.status_code}]. Body: {body}")

                self._ready.set()

            except Exception as e:
                if isinstance(e, requests.exceptions.Timeout):
                    log.error("[TokenKeeper] ✗ Timeout — GoDigit server did not respond within 30s.")
                elif isinstance(e, requests.exceptions.ConnectionError):
                    log.error("[TokenKeeper] ✗ Connection error — check internet connectivity.")
                else:
                    log.error(f"[TokenKeeper] ✗ Unexpected error: {type(e).__name__}: {e}")
                self._ready.set()

            for _ in range(600):
                if self._stop.is_set():
                    break
                time.sleep(1)

    def wait_ready(self, timeout=30):
        self._ready.wait(timeout=timeout)

    def stop(self):
        self._stop.set()


# ══════════════════════════════════════════════════════════════
#  BUSINESS API CALLS
# ══════════════════════════════════════════════════════════════
def fetch_policy_number(token: str, loan_account: str) -> str:
    user_id, company_code = get_identity()
    resp = _get_session().get(
        POLICY_FETCH_URL,
        headers={**_COMMON_HEADERS, "accept": "application/json, text/plain, */*", "authorization": f"Bearer {token}", "companycode": company_code, "userid": user_id},
        params={"loanAccountNumber": loan_account},
        timeout=15,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Policy fetch failed [{resp.status_code}]: {resp.text[:200]}")

    data    = resp.json()
    records = data if isinstance(data, list) else [data]

    # Priority 1: EFFECTIVE policy
    for rec in records:
        if isinstance(rec, dict) and rec.get("policyStatus", "").upper() == "EFFECTIVE":
            for key in ("policyNumber", "policy_number", "PolicyNumber"):
                if rec.get(key):
                    return str(rec[key])

    # Priority 2: latest by number
    all_policies = []
    for rec in records:
        if isinstance(rec, dict):
            for key in ("policyNumber", "policy_number", "PolicyNumber"):
                if rec.get(key):
                    all_policies.append(str(rec[key]))
                    break

    if not all_policies:
        raise ValueError(f"No policy number found. Response: {json.dumps(data)}")

    best = sorted(all_policies)[-1]
    if len(all_policies) > 1:
        log.warning(f"Multiple policies for {loan_account}: {all_policies} → using {best}")
    return best


def download_coi_pdf(token: str, policy_number: str, product_type: str) -> bytes:
    user_id, company_code = get_identity()
    payload = {
        "toDate": None, "fromDate": None, "reportName": "Coi Document",
        "imdCode": None, "machineIp": None, "webUserId": user_id,
        "employeeCode": [], "role": [], "consolidated": None,
        "communicationFlag": False, "createActivityId": False,
        "productType": product_type, "forceRegenerate": False,
        "policyNumber": policy_number, "quoteFlag": False,
    }
    resp = _get_session().post(
        DOC_DOWNLOAD_URL,
        headers={**_COMMON_HEADERS, "accept": "application/json, text/plain, */*", "authorization": f"Bearer {token}", "companycode": company_code, "userid": user_id, "content-type": "application/json"},
        json=payload, timeout=30,
    )
    if resp.status_code != 200:
        raise RuntimeError(f"Download failed [{resp.status_code}]: {resp.text[:200]}")

    content_type = resp.headers.get("Content-Type", "")

    if resp.content[:4] == b"%PDF" or "pdf" in content_type:
        return resp.content

    if "json" in content_type:
        data = resp.json()
        for key in ("document", "fileContent", "content", "data", "pdf"):
            val = data.get(key)
            if val and isinstance(val, str):
                try:
                    decoded = base64.b64decode(val)
                    if decoded[:4] == b"%PDF":
                        return decoded
                except Exception:
                    pass
        for key in ("url", "downloadUrl", "fileUrl", "documentUrl"):
            url = data.get(key)
            if url:
                return requests.get(url, timeout=60).content
        schedule_path = data.get("schedulePath")
        if schedule_path:
            pr = _get_session().get(schedule_path, headers={**_COMMON_HEADERS, "authorization": f"Bearer {token}", "companycode": company_code, "userid": user_id}, timeout=30)
            if pr.status_code == 200 and pr.content[:4] == b"%PDF":
                return pr.content
        raise RuntimeError(f"PDF not found. Keys: {list(data.keys())}")

    return resp.content


# ══════════════════════════════════════════════════════════════
#  EXCEL LOADER
# ══════════════════════════════════════════════════════════════
def load_excel() -> pd.DataFrame:
    path = CONFIG["excel_path"]
    if not os.path.exists(path):
        raise FileNotFoundError(f"Excel not found: {path}")

    df = pd.read_excel(path, dtype=str)
    df.columns = df.columns.str.strip()

    col_map = {CONFIG["col_loan_account"].strip(): "loan_account", CONFIG["col_product_type"].strip(): "product_type"}
    missing = [c for c in col_map if c not in df.columns]
    if missing:
        raise ValueError(f"Columns not found: {missing}. Available: {list(df.columns)}")

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
            "Loan Account Number": loan, "Product Type": product,
            "Policy Number": policy, "Status": status,
            "Note": note, "File Saved": filepath,
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        })

    def save_report(self):
        if not self.rows:
            return
        ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(CONFIG["output_dir"], f"download_report_{ts}.xlsx")
        pd.DataFrame(self.rows).to_excel(path, index=False)
        log.info(f"Report saved: {path}")

    def print_summary(self):
        total   = len(self.rows)
        success = sum(1 for r in self.rows if r["Status"] == "SUCCESS")
        skipped = sum(1 for r in self.rows if r["Status"] == "SKIPPED")
        failed  = sum(1 for r in self.rows if r["Status"] == "FAILED")
        log.info("=" * 55)
        log.info(f"  Total: {total}  |  ✓ Success: {success}  |  ↩ Skipped: {skipped}  |  ✗ Failed: {failed}")
        log.info("=" * 55)
        if failed:
            log.warning("  Failed rows:")
            for r in self.rows:
                if r["Status"] == "FAILED":
                    log.warning(f"    - {r['Loan Account Number']} -- {r['Note']}")


# ══════════════════════════════════════════════════════════════
#  ROW PROCESSOR
# ══════════════════════════════════════════════════════════════
def _process_row(args):
    row_num, total, loan, product, output_dir, get_token_fn = args
    MAX_RETRIES = 5
    last_error  = None
    policy_num  = ""

    log.info(f"[{row_num}/{total}] Loan: {loan}  |  Product: {product}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            if attempt > 1:
                wait = attempt * 3
                log.info(f"  [{loan}] Retry {attempt}/{MAX_RETRIES} in {wait}s...")
                time.sleep(wait)

            # Use cached token — avoid disk read on every row
            token = get_token_fn()

            # Fetch policy number
            log.info(f"  [{loan}] Fetching policy number...")
            policy_num = fetch_policy_number(token, loan)
            log.info(f"  [{loan}] ✓ Policy: {policy_num}")

            # Skip if already downloaded (resume support for 500+ rows)
            filename = f"COI_{loan}_{policy_num}_{product}.pdf"
            filepath = os.path.join(output_dir, filename)
            if os.path.exists(filepath) and os.path.getsize(filepath) > 1024:
                size_kb = os.path.getsize(filepath) / 1024
                log.info(f"  [{loan}] ↩ Already downloaded: {filename} ({size_kb:.1f} KB) — skipping.")
                log_row("info", loan, policy_num, "SKIPPED", f"Already exists: {filename}")
                return (loan, product, policy_num, "SKIPPED", "Already downloaded", filepath)

            # Download PDF — reuse same token (valid 15 min)
            log.info(f"  [{loan}] Downloading COI PDF...")
            pdf_bytes = download_coi_pdf(token, policy_num, product)

            # Save file
            with open(filepath, "wb") as f:
                f.write(pdf_bytes)

            size_kb = len(pdf_bytes) / 1024
            log.info(f"  [{loan}] ✓ Saved: {filename} ({size_kb:.1f} KB)")
            log_row("info", loan, policy_num, "SUCCESS", f"Saved {filename} ({size_kb:.1f} KB)")
            return (loan, product, policy_num, "SUCCESS", "", filepath)

        except RuntimeError as e:
            last_error = e
            if "429" in str(e):
                wait = 30 * attempt
                log_row("warning", loan, policy_num, "RETRY", f"Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue
            log_row("warning", loan, policy_num, f"ATTEMPT_{attempt}", str(e)[:120])
        except Exception as e:
            last_error = e
            log_row("warning", loan, policy_num, f"ATTEMPT_{attempt}", str(e)[:120])

    log_row("error", loan, policy_num, "FAILED", f"All {MAX_RETRIES} attempts failed. {str(last_error)[:120]}")
    return (loan, product, policy_num, "FAILED", str(last_error), "")


# ══════════════════════════════════════════════════════════════
#  DOWNLOADER
# ══════════════════════════════════════════════════════════════
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
    log.info(f"Loaded {total} row(s). Workers: {MAX_WORKERS}  Batch: {batch_size}")

    args_list = [
        (idx + 1, total, row["loan_account"], row["product_type"], output_dir, get_token_fn)
        for idx, row in df.iterrows()
    ]

    batches       = [args_list[i:i+batch_size] for i in range(0, total, batch_size)] if batch_size and total > batch_size else [args_list]
    total_batches = len(batches)
    completed     = 0

    for batch_num, batch in enumerate(batches, 1):
        if total_batches > 1:
            print(f"\n  --- Batch {batch_num}/{total_batches} ({len(batch)} rows) ---")
            log.info(f"--- Batch {batch_num}/{total_batches} ({len(batch)} rows) ---")

        batch_had_429 = False

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(_process_row, args): args for args in batch}
            for future in as_completed(futures):
                loan, product, policy_num, status, note, filepath = future.result()
                results.add(loan, product, policy_num, status, note, filepath)
                completed += 1

                # Track 429s to decide whether to pause between batches
                if "429" in note or "Rate limited" in note:
                    batch_had_429 = True

                success_so_far = sum(1 for r in results.rows if r["Status"] in ("SUCCESS", "SKIPPED"))
                failed_so_far  = sum(1 for r in results.rows if r["Status"] == "FAILED")
                skipped_so_far = sum(1 for r in results.rows if r["Status"] == "SKIPPED")
                log.info(
                    f"  Progress: {completed}/{total}"
                    f"  | ✓ {success_so_far}"
                    f"  | ↩ {skipped_so_far} skipped"
                    f"  | ✗ {failed_so_far} failed"
                )

        # Only pause between batches if server rate-limited us
        if batch_num < total_batches:
            if batch_had_429:
                log.info(f"  Rate limit detected. Pausing {batch_pause}s before next batch...")
                time.sleep(batch_pause)
            else:
                log.info(f"  Batch {batch_num} done. No rate limiting — continuing immediately.")

    results.print_summary()
    results.save_report()


# ══════════════════════════════════════════════════════════════
#  SETUP — Deploy Lambda
# ══════════════════════════════════════════════════════════════
def run_setup():
    import boto3

    if not LAMBDA_ZIP.exists():
        log.error(f"lambda_token_keeper.zip not found at {LAMBDA_ZIP}")
        sys.exit(1)

    region     = CONFIG["aws_region"]
    iam        = boto3.client("iam",    region_name=region)
    lam        = boto3.client("lambda", region_name=region)
    events     = boto3.client("events", region_name=region)
    sts        = boto3.client("sts",    region_name=region)
    account_id = sts.get_caller_identity()["Account"]

    log.info("=" * 55)
    log.info("  Deploying Token Keeper Lambda")
    log.info("=" * 55)

    # IAM
    log.info("\n[1/5] IAM role...")
    trust = json.dumps({"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]})
    try:
        iam.create_role(RoleName=LAMBDA_ROLE, AssumeRolePolicyDocument=trust)
    except iam.exceptions.EntityAlreadyExistsException:
        pass
    iam.attach_role_policy(RoleName=LAMBDA_ROLE, PolicyArn="arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
    iam.put_role_policy(RoleName=LAMBDA_ROLE, PolicyName="GoDigitSSMAccess", PolicyDocument=json.dumps({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": ["ssm:GetParameter", "ssm:PutParameter"], "Resource": f"arn:aws:ssm:{region}:{account_id}:parameter/godigit/*"}]
    }))
    role_arn = f"arn:aws:iam::{account_id}:role/{LAMBDA_ROLE}"
    log.info("  Waiting 10s...")
    time.sleep(10)

    # Read cookies + identity from SSM
    try:
        ssm_client   = boto3.client("ssm", region_name=region)
        cookies      = json.loads(ssm_client.get_parameter(Name=SSM_COOKIES, WithDecryption=True)["Parameter"]["Value"])
        user_id, company_code = get_identity()
    except Exception:
        log.warning("  Run --mode prod first to populate SSM, then re-run --setup")
        cookies = {}
        user_id = ""
        company_code = "LI"

    env_vars = {"Variables": {
        "SSM_ACCESS_TOKEN_PATH":  SSM_ACCESS_TOKEN,
        "SSM_REFRESH_TOKEN_PATH": SSM_REFRESH_TOKEN,
        "SSM_EXPIRES_AT_PATH":    SSM_EXPIRES_AT,
        "USER_ID":                user_id,
        "COMPANY_CODE":           company_code,
        **{f"COOKIE_{k}": v for k, v in cookies.items()},
    }}

    # Lambda
    log.info("\n[2/5] Deploying Lambda...")
    zip_bytes = LAMBDA_ZIP.read_bytes()
    try:
        lam.get_function(FunctionName=LAMBDA_FUNCTION)
        lam.update_function_code(FunctionName=LAMBDA_FUNCTION, ZipFile=zip_bytes)
        lam.update_function_configuration(FunctionName=LAMBDA_FUNCTION, Environment=env_vars)
        log.info("  Updated.")
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=LAMBDA_FUNCTION, Runtime="python3.12", Role=role_arn, Handler="lambda_token_keeper.handler", Code={"ZipFile": zip_bytes}, Timeout=30, MemorySize=128, Environment=env_vars)
        log.info("  Created.")

    func_arn = f"arn:aws:lambda:{region}:{account_id}:function:{LAMBDA_FUNCTION}"

    # EventBridge
    log.info("\n[3/5] EventBridge (every 10 min)...")
    events.put_rule(Name=EVENTBRIDGE_RULE, ScheduleExpression="rate(10 minutes)", State="ENABLED")
    try:
        lam.add_permission(FunctionName=LAMBDA_FUNCTION, StatementId="EventBridgeInvoke", Action="lambda:InvokeFunction", Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:{region}:{account_id}:rule/{EVENTBRIDGE_RULE}")
    except lam.exceptions.ResourceConflictException:
        pass
    events.put_targets(Rule=EVENTBRIDGE_RULE, Targets=[{"Id": "1", "Arn": func_arn}])

    # Test
    log.info("\n[4/5] Testing Lambda...")
    resp   = lam.invoke(FunctionName=LAMBDA_FUNCTION, LogType="Tail", Payload=b"{}")
    result = json.loads(resp["Payload"].read())
    log.info(f"  Result: {result}")

    log.info("\n[5/5] Done!")
    log.info("=" * 55)
    log.info("  SETUP COMPLETE! Lambda runs every 10 min.")
    log.info("  Now run: python godigit_coi.py --mode prod")
    log.info("=" * 55)


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="GoDigit Bulk COI Downloader", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--mode",  choices=["local", "prod"], help="local: uses token_cache.json\nprod: uses AWS SSM + Lambda")
    parser.add_argument("--setup", action="store_true",       help="Deploy Lambda to AWS (one time after first --mode prod)")
    args = parser.parse_args()

    if args.setup:
        run_setup()
        return

    if not args.mode:
        parser.print_help()
        print("\nExamples:")
        print("  python godigit_coi.py --mode local    # local testing")
        print("  python godigit_coi.py --mode prod     # production")
        print("  python godigit_coi.py --setup         # deploy Lambda (one time)")
        return

    print()
    print("=" * 55)
    print(f"  GoDigit Bulk COI Downloader  [{args.mode.upper()} MODE]")
    print("=" * 55)
    log.info("=" * 55)
    log.info(f"  GoDigit Bulk COI Downloader  [{args.mode.upper()} MODE]")
    log.info("=" * 55)

    mode = args.mode

    # ── Step 1: Seed in-memory cache from disk at startup ──────
    # This means all worker threads read identity from memory
    # instead of disk — zero disk reads during 500+ row downloads
    startup_cache = read_cache()
    if startup_cache:
        _mem_update({
            "user_id":      startup_cache.get("user_id", ""),
            "company_code": startup_cache.get("company_code", "LI"),
            "access_token": startup_cache.get("access_token", ""),
            "expires_at":   startup_cache.get("expires_at", 0.0),
            "cookies":      startup_cache.get("cookies", {}),
        })
        log.info(f"  In-memory cache seeded. user_id={_MEM_CACHE['user_id']}")

    # ── Step 2: Validate token — auto OTP login if needed ──────
    access_token = get_valid_token(mode)
    # Update memory cache with validated token
    _mem_update({"access_token": access_token, "expires_at": time.time() + 900})

    # ── Step 3: Token getter for downloader rows ───────────────
    def get_token_fn():
        """
        Returns valid token from in-memory cache.
        Zero disk reads — all worker threads share _MEM_CACHE.
        Only hits disk/network if token is near expiry (<60s).
        """
        expires_at = _MEM_CACHE.get("expires_at", 0)
        if time.time() < (expires_at - 60):
            return _MEM_CACHE.get("access_token", access_token)
        # Near expiry — refresh and update memory cache
        new_token = get_valid_token(mode)
        _mem_update({"access_token": new_token, "expires_at": time.time() + 900})
        return new_token

    # ── Step 3: Start local keeper thread (local mode only) ────
    keeper = None
    if mode == "local":
        keeper = LocalTokenKeeper()
        keeper.start()
        keeper.wait_ready(timeout=30)
        log.info("Local token keeper running in background.")

    # ── Step 4: Download PDFs ──────────────────────────────────
    try:
        run_downloader(get_token_fn)
    finally:
        if keeper:
            keeper.stop()
            log.info("Token keeper stopped.")


if __name__ == "__main__":
    main()
