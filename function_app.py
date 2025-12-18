import logging
import os
import json
import time
import requests
import azure.functions as func

from azure.storage.blob import BlobClient
from concurrent.futures import ThreadPoolExecutor, as_completed

app = func.FunctionApp()

# ========= Env / Config =========
TOKEN_URL     = os.getenv("TOKEN_URL")
API_URL       = os.getenv("API_URL")  # base only, e.g. https://stagingapi.smokeball.co.uk
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
API_KEY       = os.getenv("API_KEY")

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
STATE_CONTAINER = os.getenv("STATE_CONTAINER", "tokens")
STATE_BLOB_NAME = os.getenv("STATE_BLOB_NAME", "smokeball-token-state.json")
LOCK_CONTAINER  = os.getenv("LOCK_CONTAINER",  "locks")
LOCK_BLOB_NAME  = os.getenv("LOCK_BLOB_NAME",  "smokeball-token.lock")

if not all([TOKEN_URL, API_URL, CLIENT_ID, CLIENT_SECRET, API_KEY, AZURE_STORAGE_CONNECTION_STRING]):
    logging.warning("One or more required environment variables are missing.")

state_blob = BlobClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING, STATE_CONTAINER, STATE_BLOB_NAME
)
lock_blob = BlobClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING, LOCK_CONTAINER, LOCK_BLOB_NAME
)

# ========= Helpers: token state / lock =========
def _load_state():
    try:
        data = state_blob.download_blob().readall()
        return json.loads(data)
    except Exception:
        return {}  # first run / missing blob

def _save_state(state: dict):
    state_blob.upload_blob(json.dumps(state), overwrite=True)

def _ensure_lock_blob():
    try:
        lock_blob.get_blob_properties()
    except Exception:
        lock_blob.upload_blob(b"", overwrite=True)

def _acquire_lock(timeout_seconds=15):
    try:
        _ensure_lock_blob()
        return lock_blob.acquire_lease(timeout=timeout_seconds)
    except Exception:
        return None

def _release_lock(lease):
    if lease:
        try:
            lease.release()
        except Exception:
            pass

def _now():
    return time.time()

def _exchange_refresh_for_access(refresh_token: str):
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    resp = requests.post(TOKEN_URL, data=payload, headers=headers, timeout=60)
    if resp.status_code >= 400:
        logging.error(f"Failed to refresh token: {resp.status_code} - {resp.text}")
        resp.raise_for_status()
    js = resp.json()
    return js["access_token"], js.get("refresh_token"), js.get("expires_in", 3600)

def _get_valid_access_token():
    st = _load_state()
    access = st.get("access_token")
    exp    = st.get("expires_at", 0)
    if access and exp > _now():
        logging.info("Using cached access token from blob.")
        return access

    lease = _acquire_lock()
    try:
        # double-check after acquiring the lease
        st = _load_state()
        access = st.get("access_token")
        exp    = st.get("expires_at", 0)
        if access and exp > _now():
            logging.info("Another instance refreshed; using cached token.")
            return access

        refresh = st.get("refresh_token") or os.getenv("REFRESH_TOKEN")
        if not refresh:
            raise Exception("No refresh token found (state or env).")

        access, new_refresh, expires_in = _exchange_refresh_for_access(refresh)
        st["access_token"]  = access
        st["expires_at"]    = _now() + int(expires_in) - 60  # safety margin
        if new_refresh:
            st["refresh_token"] = new_refresh
        _save_state(st)
        logging.info("Access token refreshed and persisted.")
        return access
    finally:
        _release_lock(lease)

def _build_headers(access_token: str):
    return {
        "x-api-key": API_KEY,
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

# ========= Helpers: paging =========
def _fetch_all_pages(url: str, headers: dict):
    """
    Fixed limit=500, offset-based pagination.
    Expects array under 'value'.
    """
    LIMIT = 500
    all_rows = []
    offset = 0

    while True:
        params = {"limit": LIMIT, "offset": offset}
        resp = requests.get(url, headers=headers, params=params, timeout=240)

        if resp.status_code == 401:
            raise PermissionError("ACCESS_EXPIRED")

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            sleep_s = float(retry_after) if retry_after else 2
            time.sleep(sleep_s)
            continue

        if resp.status_code >= 400:
            logging.error(f"Upstream error {resp.status_code}: {resp.text}")
            resp.raise_for_status()

        js = resp.json()
        rows = js.get("value", [])
        all_rows.extend(rows)

        if len(rows) < LIMIT:
            break

        offset += LIMIT

    return all_rows

# ========= Optimized helpers: per-matter child fetch =========
def _get_with_retry(session: requests.Session, url: str, headers: dict, timeout=240, max_retries=6):
    """
    Retries for 401 (refresh token) and 429 (backoff, respects Retry-After if present).
    """
    last_resp = None

    for attempt in range(max_retries):
        resp = session.get(url, headers=headers, timeout=timeout)
        last_resp = resp

        if resp.status_code == 401:
            # refresh access token and retry
            new_access = _get_valid_access_token()
            headers = dict(headers)
            headers["Authorization"] = f"Bearer {new_access}"
            continue

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            # exponential backoff up to 30s
            sleep_s = float(retry_after) if retry_after else min(2 ** attempt, 30)
            time.sleep(sleep_s)
            continue

        return resp

    return last_resp

def _fetch_items_for_matter(session: requests.Session, matter_id: str, endpoint: str, headers: dict):
    """
    Fetch child items for one matter.
    NOTE: If Smokeball paginates these endpoints too, switch to _fetch_all_pages here.
    """
    url = f"{API_URL.rstrip('/')}/matters/{matter_id}/{endpoint}"
    resp = _get_with_retry(session, url, headers=headers)

    if resp is None:
        return []

    if resp.status_code == 200:
        raw = resp.json()
        return raw.get("value", []) if isinstance(raw, dict) else (raw or [])

    logging.warning(
        f"Failed for matter {matter_id} endpoint {endpoint}: "
        f"{resp.status_code} {resp.text[:200]}"
    )
    return []

def _fetch_related_for_matters(endpoint: str, max_workers: int = 8):
    """
    Efficient N+1 fetch:
      - fetch all matters once (paged)
      - fetch /matters/{id}/{endpoint} in parallel (bounded)
      - reuse HTTP connections via Session
      - handle 401/429 with retries/backoff
    """
    access = _get_valid_access_token()
    headers = _build_headers(access)

    matters = _fetch_all_pages(f"{API_URL.rstrip('/')}/matters", headers=headers)

    all_data = []
    with requests.Session() as session:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(_fetch_items_for_matter, session, m["id"], endpoint, headers)
                for m in matters
                if "id" in m
            ]

            for fut in as_completed(futures):
                items = fut.result()
                if items:
                    all_data.extend(items)

    return all_data

# ========= Routes =========

# Generic resource:
# GET /api/smokeball/contacts
# GET /api/smokeball/employees
# GET /api/smokeball/matters
@app.route(route="smokeball/{resource}", methods=["GET", "POST"], auth_level=func.AuthLevel.ANONYMOUS)
def smokeball_resource(req: func.HttpRequest) -> func.HttpResponse:
    resource = (req.route_params.get("resource") or "").strip().strip("/").lower()
    if not resource:
        return func.HttpResponse(
            json.dumps({"error": "missing resource in route"}),
            mimetype="application/json", status_code=400
        )

    url = f"{API_URL.rstrip('/')}/{resource}"

    try:
        access = _get_valid_access_token()
        headers = _build_headers(access)

        try:
            rows = _fetch_all_pages(url, headers)
        except PermissionError:
            access = _get_valid_access_token()
            headers = _build_headers(access)
            rows = _fetch_all_pages(url, headers)

        body = {"resource": resource, "count": len(rows), "rows": rows}
        return func.HttpResponse(json.dumps(body), mimetype="application/json", status_code=200)

    except Exception as e:
        logging.exception("smokeball_resource failed")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=502)


# Generic matter-child route (covers invoices/roles/fees + future endpoints):
# GET /api/smokeball/matters/invoices
# GET /api/smokeball/matters/roles
# GET /api/smokeball/matters/fees
@app.route(route="smokeball/matters/{endpoint}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def smokeball_matters_child(req: func.HttpRequest) -> func.HttpResponse:
    endpoint = (req.route_params.get("endpoint") or "").strip().strip("/").lower()
    if not endpoint:
        return func.HttpResponse(
            json.dumps({"error": "missing endpoint"}), mimetype="application/json", status_code=400
        )

    # Optional allow-list. Add more endpoints as needed.
    allowed = {"invoices", "roles", "fees"}
    if endpoint not in allowed:
        return func.HttpResponse(
            json.dumps({"error": f"unsupported endpoint '{endpoint}'", "allowed": sorted(list(allowed))}),
            mimetype="application/json", status_code=400
        )

    try:
        data = _fetch_related_for_matters(endpoint)
        body = {"resource": f"matters/{endpoint}", "count": len(data), "rows": data}
        return func.HttpResponse(json.dumps(body), mimetype="application/json", status_code=200)

    except Exception as e:
        logging.exception("smokeball_matters_child failed")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=502)


# Backwards-compatible routes (so your existing URLs still work)
@app.route(route="smokeball/invoices", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def fetch_all_invoices_func(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = _fetch_related_for_matters("invoices")
        body = {"resource": "invoices", "count": len(data), "rows": data}
        return func.HttpResponse(json.dumps(body), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception("fetch_all_invoices_func failed")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=502)

@app.route(route="smokeball/roles", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def fetch_all_roles_func(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = _fetch_related_for_matters("roles")
        body = {"resource": "roles", "count": len(data), "rows": data}
        return func.HttpResponse(json.dumps(body), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception("fetch_all_roles_func failed")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=502)

@app.route(route="smokeball/fees", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
def fetch_all_fees_func(req: func.HttpRequest) -> func.HttpResponse:
    try:
        data = _fetch_related_for_matters("fees")
        body = {"resource": "fees", "count": len(data), "rows": data}
        return func.HttpResponse(json.dumps(body), mimetype="application/json", status_code=200)
    except Exception as e:
        logging.exception("fetch_all_fees_func failed")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=502)
