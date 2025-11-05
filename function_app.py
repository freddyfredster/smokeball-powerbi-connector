import logging, os, json, time, requests, azure.functions as func
from azure.storage.blob import BlobClient

app = func.FunctionApp()

# ========= Env / Config (BASE URL ONLY — no resource here) =========
# Example: API_URL = "https://stagingapi.smokeball.co.uk"
TOKEN_URL     = os.getenv("TOKEN_URL")
API_URL       = os.getenv("API_URL")  # MUST be base only, e.g., https://stagingapi.smokeball.co.uk
CLIENT_ID     = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
API_KEY       = os.getenv("API_KEY")

# Storage for token state + lock
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
STATE_CONTAINER = os.getenv("STATE_CONTAINER", "tokens")
STATE_BLOB_NAME = os.getenv("STATE_BLOB_NAME", "smokeball-token-state.json")
LOCK_CONTAINER  = os.getenv("LOCK_CONTAINER",  "locks")
LOCK_BLOB_NAME  = os.getenv("LOCK_BLOB_NAME",  "smokeball-token.lock")

state_blob = BlobClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING, STATE_CONTAINER, STATE_BLOB_NAME
)
lock_blob = BlobClient.from_connection_string(
    AZURE_STORAGE_CONNECTION_STRING, LOCK_CONTAINER, LOCK_BLOB_NAME
)

# ========= Helpers =========
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
        st["expires_at"]    = _now() + int(expires_in) - 60  # 60s safety margin
        if new_refresh:
            st["refresh_token"] = new_refresh
        _save_state(st)
        logging.info("Access token refreshed and persisted.")
        return access
    finally:
        _release_lock(lease)

def _fetch_all_pages(url: str, headers: dict):
    """
    Fixed limit=500, offset-based pagination identical to your current approach.
    Expects array under 'value'.
    """
    LIMIT = 500
    all_rows = []
    offset = 0

    while True:
        params = {"limit": LIMIT, "offset": offset}
        resp = requests.get(url, headers=headers, params=params, timeout=240)

        if resp.status_code == 401:
            # Signal to caller to refresh once and retry
            raise PermissionError("ACCESS_EXPIRED")
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

# ========= HTTP Route (generic resource) =========
# Call like:  GET /api/smokeball/contacts
#             GET /api/smokeball/invoices
#             GET /api/smokeball/employees
@app.route(route="smokeball/{resource}", methods=["GET", "POST"], auth_level=func.AuthLevel.ANONYMOUS)
def smokeball_resource(req: func.HttpRequest) -> func.HttpResponse:
    resource = (req.route_params.get("resource") or "").strip().strip("/").lower()
    if not resource:
        return func.HttpResponse(
            json.dumps({"error": "missing resource in route"}),
            mimetype="application/json", status_code=400
        )

    # Build full URL from base + resource
    # e.g., API_URL="https://stagingapi.smokeball.co.uk" and resource="contacts"
    url = f"{API_URL.rstrip('/')}/{resource}"

    try:
        access = _get_valid_access_token()
        headers = {
            "x-api-key": API_KEY,
            "Authorization": f"Bearer {access}",
            "Content-Type": "application/json",
        }

        try:
            rows = _fetch_all_pages(url, headers)
        except PermissionError:
            # token may have expired mid-run; refresh once and retry
            access = _get_valid_access_token()
            headers["Authorization"] = f"Bearer {access}"
            rows = _fetch_all_pages(url, headers)

        body = {"resource": resource, "count": len(rows), "rows": rows}
        return func.HttpResponse(json.dumps(body), mimetype="application/json", status_code=200)

    except Exception as e:
        logging.exception("smokeball_resource failed")
        return func.HttpResponse(json.dumps({"error": str(e)}), mimetype="application/json", status_code=502)
