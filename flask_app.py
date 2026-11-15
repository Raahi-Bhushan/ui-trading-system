# ... (Keep existing imports and setup) ...
import os
import json
import logging
import re
import threading
import time
import hmac
import tempfile
from datetime import date, datetime, timedelta
from decimal import Decimal
import subprocess
import signal
import sys
import glob
from typing import Optional, List, Dict, Any

from functools import wraps
from flask import Flask, request, jsonify, session, render_template, render_template_string, redirect, url_for, send_from_directory
from werkzeug.security import check_password_hash

# CSRF protection. Imported defensively so the app still boots on a server
# where flask-wtf is not yet installed — protection is simply disabled with a
# loud warning instead of an ImportError taking down the dashboard.
try:
    from flask_wtf.csrf import CSRFProtect, CSRFError, generate_csrf
    _SECURITY_EXTENSIONS_AVAILABLE = True
except ImportError as _security_import_error:
    CSRFProtect = None  # type: ignore[assignment,misc]
    CSRFError = None  # type: ignore[assignment,misc]
    generate_csrf = None  # type: ignore[assignment]
    _SECURITY_EXTENSIONS_AVAILABLE = False
    logging.warning(
        "flask-wtf not installed (%s) — CSRF protection is DISABLED. "
        "Run: pip install flask-wtf",
        _security_import_error,
    )

# Shared rate limiter — also used by public-facing blueprints (e.g.
# cas_tracker) which cannot import this module themselves without a circular
# import. See rate_limiter.py for why it's bound to this app via init_app()
# below rather than constructed here.
from rate_limiter import limiter

from kiteconnect import KiteConnect
import instrument_cache
from common_lib import IST, get_ist_now
from kite_api_monitor import (
    MonitoredKite,
    get_monitor_stats,
    get_history_for_date,
    get_available_dates,
    reset_stats as _reset_monitor_stats,
    invalidate_positions_cache,
)

class _ISTFormatter(logging.Formatter):
    """Logging formatter that stamps records in IST (UTC+5:30) instead of local time."""

    def formatTime(self, record: logging.LogRecord, datefmt: Optional[str] = None) -> str:
        ist_dt = datetime.fromtimestamp(record.created, tz=IST)
        return ist_dt.strftime(datefmt or "%Y-%m-%d %H:%M:%S") + f",{record.msecs:03.0f}"


_handler = logging.StreamHandler()
_handler.setFormatter(_ISTFormatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[_handler], force=True)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Base settings
PORT = 5010
HOST = "127.0.0.1"

serializer = lambda obj: isinstance(obj, (date, datetime, Decimal)) and str(obj)  # noqa

import configparser

# Base directory for config file
base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, "configfile.ini")

# Read config
config = configparser.ConfigParser()
config.read(config_path)

if "kite_login_details" in config:
    kite_api_key = config["kite_login_details"]["api_key"]
    kite_api_secret = config["kite_login_details"]["api_secret"]
else:
    kite_api_key = "kite_api_key"
    kite_api_secret = "kite_api_secret"
    logging.error("kite_login_details not found in configfile.ini")

# Gatekeeper Authentication
if "gatekeeper" in config:
    APP_USERNAME = config["gatekeeper"]["username"]
    APP_PASSWORD = config["gatekeeper"]["password"]
else:
    APP_USERNAME = "admin"
    APP_PASSWORD = "password"
    logging.warning("gatekeeper credentials not found in configfile.ini, using defaults")

# The gatekeeper password may be stored either as a Werkzeug hash (recommended —
# generate with werkzeug.security.generate_password_hash) or, for backward
# compatibility, as plaintext. Hashed values start with a known method prefix.
_PASSWORD_HASH_PREFIXES = ("pbkdf2:", "scrypt:", "argon2")


def verify_gatekeeper_password(submitted_password: str) -> bool:
    """Verify a submitted dashboard password against the configured value.

    Uses a constant-time comparison to avoid timing attacks. If the stored value
    is a Werkzeug password hash it is checked with ``check_password_hash``;
    otherwise it falls back to a constant-time plaintext comparison and logs a
    warning to migrate to a hash.

    Args:
        submitted_password: The password provided in the login form.

    Returns:
        True if the password matches, False otherwise.
    """
    if not submitted_password:
        return False
    if APP_PASSWORD.startswith(_PASSWORD_HASH_PREFIXES):
        return check_password_hash(APP_PASSWORD, submitted_password)
    logging.warning(
        "Gatekeeper password is stored in plaintext; migrate to a hash via "
        "werkzeug.security.generate_password_hash and update configfile.ini."
    )
    return hmac.compare_digest(submitted_password, APP_PASSWORD)


# --- Login rate limiting / lockout (per client IP, in-memory) ---------------
_LOGIN_MAX_ATTEMPTS = 5
_LOGIN_LOCKOUT_SECONDS = 15 * 60
_login_attempts: dict[str, dict[str, float]] = {}
_login_attempts_lock = threading.Lock()


def _login_is_locked(client_ip: str) -> tuple[bool, int]:
    """Return whether the client IP is currently locked out and seconds remaining.

    Args:
        client_ip: The remote address of the login request.

    Returns:
        Tuple of (is_locked, seconds_remaining).
    """
    now = time.time()
    with _login_attempts_lock:
        record = _login_attempts.get(client_ip)
        if not record:
            return False, 0
        if record["count"] < _LOGIN_MAX_ATTEMPTS:
            return False, 0
        unlock_at = record["last"] + _LOGIN_LOCKOUT_SECONDS
        if now >= unlock_at:
            _login_attempts.pop(client_ip, None)
            return False, 0
        return True, int(unlock_at - now)


def _record_login_failure(client_ip: str) -> None:
    """Increment the failed-attempt counter for a client IP."""
    now = time.time()
    with _login_attempts_lock:
        record = _login_attempts.get(client_ip)
        if not record or now - record["last"] > _LOGIN_LOCKOUT_SECONDS:
            _login_attempts[client_ip] = {"count": 1, "last": now}
        else:
            record["count"] += 1
            record["last"] = now


def _clear_login_failures(client_ip: str) -> None:
    """Reset the failed-attempt counter after a successful login."""
    with _login_attempts_lock:
        _login_attempts.pop(client_ip, None)

# Create a redirect url (Upstox edition: the public URL registered on the Upstox app)
redirect_url = os.environ.get("UPSTOX_REDIRECT_URI") or "http://{host}:{port}/login".format(host=HOST, port=PORT)

# Login url (Upstox OAuth dialog, built by the Upstox-backed kiteconnect package)
login_url = KiteConnect(api_key=kite_api_key, redirect_uri=redirect_url).login_url()

# Upstox developer console url
console_url = "https://account.upstox.com/developer/apps"

# App
app = Flask(__name__)

# Use a persistent secret key to preserve sessions across restarts
SECRET_KEY_FILE = os.path.join(base_dir, ".flask_secret")
if os.path.exists(SECRET_KEY_FILE):
    with open(SECRET_KEY_FILE, "rb") as f:
        app.secret_key = f.read()
else:
    app.secret_key = os.urandom(24)
    with open(SECRET_KEY_FILE, "wb") as f:
        f.write(app.secret_key)

# Harden the session cookie. HttpOnly blocks JS access; SameSite=Lax blocks
# cross-site POSTs (the primary CSRF defense for cookie-based auth). Secure is
# enabled once the app is served over HTTPS (set FLASK_COOKIE_SECURE=true behind
# the TLS reverse proxy) — leaving it False on plain HTTP so cookies still work.
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("FLASK_COOKIE_SECURE", "false").lower() == "true",
    # Flask defaults permanent sessions to 31 days — too long for a dashboard
    # that can place/cancel live trades. session.permanent = True is set on
    # login, so this bounds how long that session stays valid.
    PERMANENT_SESSION_LIFETIME=timedelta(days=7),
)


# ---------------------------------------------------------------------------
# CSRF protection + rate limiting
# CSRF tokens are session-bound (no expiry — sessions already time out) and
# delivered to browser JS via GET /api/csrf-token; the shared fetch/XHR shim in
# templates/_header_partial.html attaches them as an X-CSRFToken header on all
# same-origin mutating requests. Classic HTML forms embed {{ csrf_token() }}.
# The rate limiter (imported from rate_limiter.py, shared with public
# blueprints) has NO default limits — dashboards poll aggressively — and is
# applied only via explicit decorators on order-affecting/public endpoints.
# Both use in-memory state: fine for the single-process waitress deployment,
# resets on restart (documented limitation for multi-worker setups).
# ---------------------------------------------------------------------------
if _SECURITY_EXTENSIONS_AVAILABLE:
    app.config["WTF_CSRF_TIME_LIMIT"] = None
    csrf = CSRFProtect(app)

    @app.errorhandler(CSRFError)
    def handle_csrf_error(error: "CSRFError"):  # noqa: ANN201 - Flask handler
        """Return JSON for API/XHR callers, plain text otherwise."""
        logging.warning("CSRF validation failed for %s: %s", request.path, error.description)
        if request.path.startswith("/api/") or request.is_json:
            return jsonify({"error": "CSRF validation failed", "detail": error.description}), 400
        return f"CSRF validation failed: {error.description}", 400

    @app.route("/api/csrf-token", methods=["GET"])
    def get_csrf_token():  # noqa: ANN201 - Flask handler
        """Issue the session's CSRF token for the fetch/XHR header shim."""
        return jsonify({"csrf_token": generate_csrf()})
else:
    csrf = None
    # Templates reference {{ csrf_token() }}; keep them rendering when
    # flask-wtf is absent by supplying an empty-string fallback.
    app.jinja_env.globals.setdefault("csrf_token", lambda: "")

# Bind the shared limiter (module-level singleton in rate_limiter.py, already
# decorating routes in this file and in cas_tracker/blueprint.py) to this
# app. A no-op if flask-limiter isn't installed — see rate_limiter.py.
limiter.init_app(app)

# ---------------------------------------------------------------------------
# Kite session probe cache
# Only authenticated=True results are cached (5-minute TTL keyed on token
# prefix). Unauthenticated results are never cached so every call gets a
# fresh probe until the session is live again.
# ---------------------------------------------------------------------------
_kite_session_cache: dict[str, Any] = {
    "token_prefix": "",
    "expires_at": 0.0,
    "last_failure_was_auth": False,
}
_kite_session_cache_lock = threading.Lock()

_KITE_SESSION_CACHE_TTL_SECONDS = 300

# ---------------------------------------------------------------------------
# Zerodha orders cache — overlays actual exchange prices onto /api/status
# so the UI always shows the real current order price (not the stale script
# copy), even for external modifications via the Zerodha app.
# ---------------------------------------------------------------------------
_zerodha_orders_cache: dict[str, Any] = {"data": [], "expires_at": 0.0}
_zerodha_orders_cache_lock = threading.Lock()
_ZERODHA_ORDERS_CACHE_TTL_SECONDS: float = 4.0

# SENSEX positions summary cache — IV computation via scipy is CPU-heavy on this
# single-core VM. The dashboard polls both /api/sensex_positions and
# /api/sensex_positions/next_expiry every 60s; without a cache, concurrent
# requests pile up and saturate the core. 20s TTL keeps the UI fresh while
# guaranteeing at most one IV solve per variant per 20s.
_sensex_positions_cache: dict[str, Any] = {
    "overall": {"data": None, "expires_at": 0.0},
    "next_expiry": {"data": None, "expires_at": 0.0},
}
_sensex_positions_cache_lock = threading.Lock()
_SENSEX_POSITIONS_CACHE_TTL_SECONDS: float = 570.0  # 9.5 min — slightly under the 10-min poll interval


def _probe_kite_session(access_token: str) -> tuple[bool, bool]:
    """Check whether the current Kite access token is valid.

    Uses a 10-second in-memory cache for successful probes to avoid hammering
    the Kite profile() API. Failed probes are never cached — each call after a
    failure triggers a fresh live check immediately.

    Args:
        access_token: The Kite access token from the current session.

    Returns:
        Tuple of (authenticated, is_auth_failure). is_auth_failure is True only
        when the token is definitively rejected by Kite (not a network error).
    """
    token_prefix = access_token[:8]

    with _kite_session_cache_lock:
        if (
            _kite_session_cache["token_prefix"] == token_prefix
            and time.monotonic() < _kite_session_cache["expires_at"]
        ):
            logging.debug("[SESSION_PROBE] Cache hit for token prefix %s", token_prefix)
            return True, False

    # Cache miss or expired — make the live API call outside the lock so other
    # requests are not serialised while we wait for the network round-trip.
    try:
        kite = get_kite_client()
        kite.profile()
        with _kite_session_cache_lock:
            _kite_session_cache["token_prefix"] = token_prefix
            _kite_session_cache["expires_at"] = time.monotonic() + _KITE_SESSION_CACHE_TTL_SECONDS
            _kite_session_cache["last_failure_was_auth"] = False
        logging.debug("[SESSION_PROBE] Live probe succeeded — cached for %ss", _KITE_SESSION_CACHE_TTL_SECONDS)
        return True, False
    except Exception as exc:
        error_msg = str(exc).lower()
        auth_errors = ["invalid", "incorrect", "token", "permission", "403", "expired"]
        is_auth_failure = any(err in error_msg for err in auth_errors)
        if is_auth_failure:
            logging.warning("[SESSION_PROBE] Auth failure — token is invalid: %s", exc)
        else:
            logging.warning("[SESSION_PROBE] Network/transient failure — not caching: %s", exc)
        with _kite_session_cache_lock:
            _kite_session_cache["last_failure_was_auth"] = is_auth_failure
        return False, is_auth_failure


def _get_cached_zerodha_orders() -> list[dict]:
    """Return all Zerodha orders for today, cached for _ZERODHA_ORDERS_CACHE_TTL_SECONDS.

    Used to overlay actual exchange prices onto the /api/status response so the
    UI reflects the true current order price regardless of what the wave extractor
    script has in memory.

    Returns:
        List of order dicts from kite.orders(); empty list on auth failure or error.
    """
    with _zerodha_orders_cache_lock:
        if time.monotonic() < _zerodha_orders_cache["expires_at"]:
            return _zerodha_orders_cache["data"]

    try:
        _kite = get_authenticated_kite_client({})
        orders: list[dict] = _kite.orders() or []
        with _zerodha_orders_cache_lock:
            _zerodha_orders_cache["data"] = orders
            _zerodha_orders_cache["expires_at"] = (
                time.monotonic() + _ZERODHA_ORDERS_CACHE_TTL_SECONDS
            )
        return orders
    except Exception as exc:
        logging.warning("Failed to fetch Zerodha orders for price overlay: %s", exc)
        return []


# First-run setup wizard: guides new installs through writing configfile.ini.
# Routes are self-locking — they redirect to login once a complete config
# exists, so this adds no surface to a configured install.
from setup_wizard import setup_bp, is_config_complete as _setup_config_complete
app.register_blueprint(setup_bp)


# Global Security Hook
@app.before_request
def enforce_auth():
    """Enforce authentication for all routes except login, static, and the API blueprint.

    Only the Claude-Skills API blueprint (``api_module``) is exempt from the
    gatekeeper — it self-authenticates via the ``X-API-Key`` header. Every other
    route, including app-level ``/api/*`` endpoints such as ``/api/start`` and
    ``/api/stop`` that place or cancel trades, requires an authenticated session.

    Fresh installs (missing/placeholder configfile.ini) are redirected to the
    setup wizard instead of a login they could never pass.
    """
    if request.blueprint == "setup_wizard":
        return

    if request.endpoint == "disclaimer":
        return

    if not _setup_config_complete():
        if request.endpoint == "static":
            return
        if request.path.startswith("/api/"):
            return jsonify({"error": "Setup required — open /setup in a browser"}), 503
        return redirect(url_for("setup_wizard.setup_page"))

    if request.endpoint in ("app_login", "static", "login"):
        return

    # The Claude-Skills API blueprint enforces its own X-API-Key auth per route.
    if request.blueprint == "api_module":
        return

    # Read-only public market data endpoints (used by static pages & public dashboards)
    if request.method == "GET" and request.path.startswith("/cas_tracker/api/"):
        return

    if 'app_authenticated' not in session:
        # Return JSON 401 for API/XHR calls instead of an HTML login redirect.
        if request.path.startswith("/api/"):
            return jsonify({"error": "Authentication required"}), 401
        return redirect(url_for('app_login', next=request.url))

@app.route("/app_login", methods=["GET", "POST"])
def app_login():
    """Login route for the application gatekeeper with lockout and hashed passwords."""
    error = None
    client_ip = request.remote_addr or "unknown"
    if request.method == "POST":
        locked, seconds_remaining = _login_is_locked(client_ip)
        if locked:
            minutes = max(1, seconds_remaining // 60)
            logging.warning("Login blocked for %s — locked out (%ss left)", client_ip, seconds_remaining)
            error = f"Too many failed attempts. Try again in ~{minutes} minute(s)."
            return render_template("app_login.html", error=error), 429

        username = request.form.get("username", "")
        password = request.form.get("password", "")
        username_ok = hmac.compare_digest(username, APP_USERNAME)
        password_ok = verify_gatekeeper_password(password)
        if username_ok and password_ok:
            _clear_login_failures(client_ip)
            session.permanent = True
            session["app_authenticated"] = True
            next_url = request.args.get("next") or url_for("home_page")
            return redirect(next_url)
        else:
            _record_login_failure(client_ip)
            logging.warning("Failed dashboard login for user '%s' from %s", username, client_ip)
            error = "Invalid username or password"

    return render_template("app_login.html", error=error)

@app.route("/app_logout")
def app_logout():
    """Logout route to clear the application session and Kite tokens."""
    session.pop('app_authenticated', None)
    session.pop('access_token', None)
    session.pop('request_token', None)
    logging.info("User logged out and session cleared.")
    return redirect(url_for('app_login'))

@app.route("/disclaimer")
def disclaimer():
    """No-warranty / no-liability / educational-use disclaimer page.

    Linked from every page's footer. Reachable at every stage of the app
    lifecycle (pre-setup, pre-login, fully configured) — see the exemption
    in enforce_auth() above — so it works even before a self-hoster has
    written configfile.ini or logged in.
    """
    return render_template("disclaimer.html")


# Register Notifications Blueprint
from notifications.blueprint import notifications_bp
app.register_blueprint(notifications_bp)


# Register Covered Calls Blueprint
from covered_calls.blueprint import covered_calls_bp
app.register_blueprint(covered_calls_bp)


# Register Position Guard Blueprint
from position_guard.blueprint import position_guard_bp
from position_guard.db import init_db as _position_guard_init_db
app.register_blueprint(position_guard_bp)
_position_guard_init_db()


# Register NIFTY CAS (Closing Auction Session) Tracker Blueprint
from cas_tracker.blueprint import cas_tracker_bp, init_cas_tracker
app.register_blueprint(cas_tracker_bp)
init_cas_tracker()


# ---------------------------------------------------------------------------
# Startup: instrument DB integrity check
# ---------------------------------------------------------------------------

def _startup_integrity_check() -> None:
    """Run once at startup to ensure the instruments table is never empty.

    If the live table is empty but ``instruments_backup`` has rows, the backup
    is copied back into the live table so the app always serves valid data
    even before the morning Kite login / sync has been triggered.
    """
    try:
        instrument_cache.init_db()
        restored = instrument_cache.restore_from_backup()
        if restored:
            logging.warning(
                "[startup] instruments table was empty — restored from backup"
            )
        else:
            from instrument_cache import get_db_stats
            stats = get_db_stats()
            logging.info(
                "[startup] instruments table OK: %d rows", stats.get("total_count", 0)
            )
    except Exception as exc:  # noqa: BLE001
        logging.error("[startup] instruments integrity check failed: %s", exc)


_startup_integrity_check()


def _startup_auto_sync_if_needed() -> None:
    """Kick off a background instrument sync at boot if the table is empty.

    Covers two installation-time gaps that a plain integrity check can't:
      - A brand-new install with no backup table to restore from (the only
        prior recovery path) sits at 0 rows until someone logs in AND the
        first login's own auto-sync (see /api/establish_session) completes.
      - A server restart mid-day, after backup-restore already ran, where a
        still-valid Kite token is on file (instrument_cache.get_kite_token())
        from an earlier login — previously nothing re-triggered a sync until
        the user manually clicked "Sync Instruments" or re-logged in.

    Runs in a background thread so a ~60k-instrument fetch never delays
    server startup; sync_instruments()'s own lock (instrument_cache.py)
    makes it safe to race harmlessly against a concurrent login-triggered
    sync — one wins, the other backs off instead of corrupting anything.
    """
    try:
        if not instrument_cache.needs_sync():
            return
        stored_token = instrument_cache.get_kite_token()
        if not stored_token:
            logging.info(
                "[startup] instruments table needs sync but no stored Kite "
                "token is available yet — will sync automatically on first login."
            )
            return

        def _run_sync() -> None:
            try:
                kite = MonitoredKite(KiteConnect(api_key=kite_api_key), account_id="main")
                kite.set_access_token(stored_token)
                if _safe_sync_instruments(kite):
                    logging.info("[startup] background instrument sync completed successfully")
                else:
                    logging.warning(
                        "[startup] background instrument sync did not complete — "
                        "stored token may be expired; will retry on next login."
                    )
            except Exception:  # noqa: BLE001
                logging.exception("[startup] background instrument sync failed")

        threading.Thread(target=_run_sync, name="startup-instrument-sync", daemon=True).start()
        logging.info("[startup] instruments table empty — background sync started using stored token")
    except Exception as exc:  # noqa: BLE001
        logging.error("[startup] auto-sync check failed: %s", exc)


# Invoked below, once _safe_sync_instruments() is defined (avoids a forward
# reference from the background thread this function spawns).

# Initialise notification DB tables and start the background scheduler
from notifications.database import init_db as _notifications_init_db
_notifications_init_db()

from notifications.scheduler import init_scheduler as _init_notification_scheduler


_init_notification_scheduler(app)


def _safe_sync_instruments(kite: object) -> bool:  # type: ignore[type-arg]
    """Attempt to sync instruments and return success flag, never raising.

    This wrapper ensures a failed Kite sync (expired token, network error)
    never propagates an exception into the caller's request / startup path.
    The live ``instruments`` table is left untouched on failure thanks to the
    atomic-swap implementation inside ``instrument_cache.sync_instruments``.
    """
    try:
        return instrument_cache.sync_instruments(kite)
    except Exception as exc:  # noqa: BLE001
        logging.error(
            "[sync] instrument sync raised unexpectedly: %s — "
            "existing data is preserved",
            exc,
        )
        return False


_startup_auto_sync_if_needed()

# Templates
index_template = """
    <div>Make sure your app with api_key - <b>{api_key}</b> has set redirect to <b>{redirect_url}</b>.</div>
    <div>If not you can set it from your <a href="{console_url}">Kite Connect developer console here</a>.</div>
    <a href="{login_url}"><h1>Login to generate access token.</h1></a>"""


def get_kite_client():
    """Returns a monitored kite client object.

    Returns:
        MonitoredKite: A proxy around KiteConnect that records every API call
        to the shared API monitor store (visible at /api-monitor).
    """
    kite = MonitoredKite(KiteConnect(api_key=kite_api_key), account_id="main")
    if "access_token" in session:
        kite.set_access_token(session["access_token"])
    return kite


@app.route("/")
def index():
    return index_template.format(
        api_key=kite_api_key,
        redirect_url=redirect_url,
        console_url=console_url,
        login_url=login_url
    )


def get_token_for_script(user_provided_token=None):
    """
    Get a token (request_token or access_token) for passing to background scripts.
    If it's an access token from the session, it will be prefixed with 'access:'.
    """
    if user_provided_token:
        return user_provided_token
    
    if "access_token" in session:
        return "access:" + session["access_token"]
        
    return None


def get_authenticated_kite_client(data: dict) -> KiteConnect:
    """Return an authenticated KiteConnect client using data or session.
    
    Tries request_token from data first (and persists to session), 
    then falls back to session access_token.
    
    Args:
        data: Dictionary which may contain 'request_token'.
        
    Returns:
        KiteConnect: An authenticated client.
        
    Raises:
        RuntimeError: If no authentication is available.
    """
    request_token = data.get("request_token")
    kite = KiteConnect(api_key=kite_api_key)
    
    if request_token:
        try:
            auth_data = kite.generate_session(request_token, api_secret=kite_api_secret)
            _set_kite_session(auth_data["access_token"])
            kite.set_access_token(auth_data["access_token"])
            logging.info("Generated fresh session with provided request_token")
        except Exception as e:
            if "access_token" in session:
                kite.set_access_token(session["access_token"])
                logging.warning(f"Failed to generate session with token, using existing session: {e}")
            else:
                raise e
    elif "access_token" in session:
        kite.set_access_token(session["access_token"])
    elif session.get("app_authenticated"):
        # Attempt to restore from DB if missing from session
        saved_token = _get_restored_kite_token()
        if saved_token:
            logging.info("Restoring access_token from DB in get_authenticated_kite_client")
            session["access_token"] = saved_token
            kite.set_access_token(saved_token)
        else:
            raise RuntimeError("Authentication required. Please connect Kite in the header.")
    else:
        raise RuntimeError("Authentication required. Please connect Kite in the header.")
        
    return kite


@app.route("/login", methods=["GET", "POST"])
def login():
    # Upstox redirects back with ?code=...; Kite used ?request_token=...
    request_token = (request.args.get("request_token") or request.form.get("request_token")
                     or request.args.get("code") or request.form.get("code"))
    logging.info("[LOGIN] /login called via %s. request_token present: %s", request.method, bool(request_token))

    if not request_token:
        logging.warning("[LOGIN] No request_token in query params")
        return """
            <span style="color: red">
                Error while generating request token.
            </span>
            <a href='/'>Try again.<a>"""

    logging.info("[LOGIN] Returning token relay page (popup will call establish_session).")
    # For popup flow: relay the request_token back to the opener via postMessage so the
    # parent page can call /api/establish_session (same-origin, session cookie intact).
    # For direct navigation (no opener): do the exchange here and redirect to /home.
    return render_template_string("""<!DOCTYPE html>
<html><head><title>Kite Login</title></head>
<body style="background:#0f1117;color:#48bb78;font-family:sans-serif;display:flex;
             align-items:center;justify-content:center;height:100vh;margin:0">
  <div style="text-align:center">
    <div style="font-size:32px;margin-bottom:8px">&#10003;</div>
    <div style="font-size:16px">Connected to Kite</div>
    <div style="font-size:12px;color:#718096;margin-top:4px">This window will close automatically</div>
  </div>
  <script>
    var token = {{ request_token | tojson }};
    if (window.opener) {
      window.opener.postMessage({type: 'ZERODHA_TOKEN', token: token}, '*');
      setTimeout(function() { window.close(); }, 800);
    } else {
      // Direct navigation — do the exchange via fetch then redirect
      fetch('/api/establish_session', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({request_token: token})
      }).then(function() {
        window.location.href = '/home';
      }).catch(function() {
        window.location.href = '/home';
      });
    }
  </script>
</body></html>""", request_token=request_token)

@app.route("/holdings.json")
def holdings():
    kite = get_kite_client()
    return jsonify(holdings=kite.holdings())


@app.route("/orders.json")
def orders():
    kite = get_kite_client()
    return jsonify(orders=kite.orders())


# ---------------------------------------------------------------------------
# Duplicate Order Monitor
# ---------------------------------------------------------------------------


@app.route("/duplicate-orders")
def duplicate_orders_dashboard():
    """Render the duplicate orders monitor dashboard."""
    return render_template("duplicate_orders_dashboard.html")


@app.route("/api/duplicate-orders")
def api_duplicate_orders():
    """Return current duplicate OPEN order groups as JSON.

    Calls Kite API live — no cache — so the dashboard always shows the freshest
    state. Called by the dashboard on load and every 60 s.

    Returns:
        JSON with keys: groups (list), total_groups (int), checked_at (ISO str),
        error (str, only on failure).
    """
    from duplicate_order_monitor import scan_duplicate_orders
    from common_lib import get_ist_now

    kite = get_kite_client()
    try:
        groups = scan_duplicate_orders(kite)
    except Exception as exc:
        logging.error("api_duplicate_orders: scan failed — %s", exc)
        return jsonify(
            groups=[],
            total_groups=0,
            checked_at=get_ist_now().isoformat(),
            error=str(exc),
        )

    return jsonify(
        groups=groups,
        total_groups=len(groups),
        checked_at=get_ist_now().isoformat(),
    )


@app.route("/api/cancel-duplicate-order", methods=["POST"])
@limiter.limit("30 per minute")
def api_cancel_duplicate_order():
    """Cancel a single OPEN order by order_id and variety.

    Request body (JSON):
        order_id (str): Kite order ID to cancel.
        variety  (str): Order variety — 'regular', 'co', 'bo', or 'amo'.

    Returns:
        JSON with keys: success (bool), order_id (str), message (str).
    """
    payload: dict = request.get_json(silent=True) or {}
    order_id: str = str(payload.get("order_id", "")).strip()
    variety: str = str(payload.get("variety", "regular")).strip() or "regular"

    if not order_id:
        return jsonify(success=False, order_id="", message="order_id is required"), 400

    kite = get_kite_client()
    try:
        kite.cancel_order(variety=variety, order_id=order_id)
        logging.info("api_cancel_duplicate_order: cancelled order_id=%s variety=%s", order_id, variety)
        return jsonify(success=True, order_id=order_id, message="Order cancelled successfully")
    except Exception as exc:
        error_msg = str(exc)
        logging.warning("api_cancel_duplicate_order: cancel failed order_id=%s — %s", order_id, error_msg)
        return jsonify(success=False, order_id=order_id, message=error_msg), 400

# ... (imports)

STATUS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "status")
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "logs")

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

@app.route("/home")
def home_page() -> str:
    """Render the home page with links to all modules."""
    return render_template("home.html")


@app.route("/wave-extractor")
def wave_extractor() -> str:
    """Render the Wave Extractor gap trading dashboard."""
    return render_template("wave_extractor.html")


@app.route("/dashboard")
def dashboard_redirect():
    """Redirect legacy /dashboard URL to /wave-extractor."""
    return redirect(url_for("wave_extractor"), code=301)


@app.route("/api-monitor")
def api_monitor_dashboard():
    """Render the Zerodha API call monitoring dashboard."""
    return render_template("api_monitor_dashboard.html")


@app.route("/api/api-monitor-data")
def api_monitor_data():
    """Return aggregated API call statistics as JSON for the live dashboard.

    Query params:
        date (str): Optional ``YYYY-MM-DD`` IST date to fetch historical data
            from SQLite. Omit for the live in-memory view (last 100 calls).

    Returns:
        JSON with keys: recent_calls, method_stats, top_sites, summary.
    """
    date_param = request.args.get("date", "").strip()
    if date_param:
        return jsonify(get_history_for_date(date_param))
    return jsonify(get_monitor_stats())


@app.route("/api/api-monitor-dates")
def api_monitor_dates():
    """Return the list of IST dates available in the SQLite history.

    Returns:
        JSON with key ``dates``: list of ``YYYY-MM-DD`` strings, newest first.
    """
    return jsonify({"dates": get_available_dates()})


@app.route("/api/api-monitor-reset", methods=["POST"])
def api_monitor_reset():
    """Reset all recorded API call statistics (memory + SQLite).

    Returns:
        JSON confirmation message.
    """
    _reset_monitor_stats()
    logging.info("API monitor stats reset via dashboard.")
    return jsonify({"status": "ok", "message": "Stats reset."})


@app.route("/api/delta_live_snapshot")
def delta_live_snapshot():
    """Return the latest live-delta-per-expiry snapshot as JSON.

    Reads the cached state computed by the last APScheduler run (see
    delta_live_tracker.py) — no live Kite API calls. Covers every
    underlying/expiry with an open position, independent of whether a
    wave-extractor process is currently running for it. Protected by the
    global enforce_auth before-request hook.

    Returns:
        JSON with keys NIFTY/BANKNIFTY/SENSEX (each {expiry_date: delta}) and
        updated_at (ISO timestamp string or null).
    """
    from delta_live_tracker import get_current_live_delta_snapshot

    return jsonify(get_current_live_delta_snapshot())


@app.route("/gtt_monitor/status")
def gtt_monitor_status():
    """Return the latest GTT vs position mismatch snapshot as JSON.

    Reads the cached state computed by the last APScheduler run — no live
    API calls. Protected by the global enforce_auth before-request hook.

    Returns:
        JSON with keys: mismatches (list), last_checked (ISO str or null), count (int).
    """
    from gtt_monitor import get_current_mismatch_snapshot

    return jsonify(get_current_mismatch_snapshot())


@app.route("/gtt_monitor/debug")
def gtt_monitor_debug():
    """Return raw positions and GTTs as seen by the monitor, for debugging.

    Builds its own KiteConnect instance (same as the scheduler) so this works
    even when the Flask session kite object is stale.
    """
    from gtt_monitor import _build_kite_client

    try:
        kite = _build_kite_client()
    except Exception as exc:
        return jsonify({"error": f"Could not build Kite client: {exc}"}), 500

    try:
        positions_raw = kite.positions()
        net_positions = positions_raw.get("net", [])
        nfo_positions = [
            {k: p[k] for k in ("tradingsymbol", "exchange", "product", "quantity")}
            for p in net_positions
            if p.get("quantity", 0) != 0
        ]
    except Exception as exc:
        return jsonify({"error_positions": str(exc)}), 500

    try:
        gtts_raw = kite.get_gtts()
        gtt_summary = []
        for g in gtts_raw:
            orders = g.get("orders", [])
            gtt_summary.append({
                "id": g.get("id"),
                "status": g.get("status"),
                "orders": [
                    {
                        "tradingsymbol": o.get("tradingsymbol"),
                        "exchange": o.get("exchange"),
                        "transaction_type": o.get("transaction_type"),
                        "quantity": o.get("quantity"),
                        "result": o.get("result"),
                    }
                    for o in orders
                ],
            })
    except Exception as exc:
        return jsonify({"error_gtts": str(exc)}), 500

    return jsonify({"positions_non_zero": nfo_positions, "gtts": gtt_summary})


@app.route("/gtt_monitor/check", methods=["GET", "POST"])
def gtt_monitor_check_now():
    """Force an immediate GTT vs position mismatch check, bypassing market-hours gate.

    Useful for manual testing and ad-hoc verification. Makes live Kite API calls.
    Notifications are dispatched for any NEW mismatches found (same dedup logic
    as the scheduled check).

    Returns:
        JSON with keys: mismatches (list), count (int), notifications_fired (int).
    """
    import gtt_monitor

    prev_known = set(gtt_monitor._known_mismatches)  # snapshot before run
    mismatches = gtt_monitor.run_gtt_monitor_check(force=True)
    new_known = gtt_monitor._known_mismatches
    notifications_fired = len(new_known - prev_known)
    return jsonify(
        {
            "mismatches": mismatches,
            "count": len(mismatches),
            "notifications_fired": notifications_fired,
        }
    )


@app.route("/api/status")
@app.route("/api/status")
def get_status():
    status_list = []
    processed_symbols = set()
    
    # Load global delta config
    global_delta_config = {}
    try:
        config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "delta_limits.json")
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                global_delta_config = json.load(f)
    except Exception as e:
        logging.error(f"Error loading global delta config in status: {e}")
    
    # 1. Read status files
    if os.path.exists(STATUS_DIR):
        files = glob.glob(os.path.join(STATUS_DIR, "status_*.json"))
        for file_path in files:
            # executed_orders check is technically redundant if we only glob status_*, but good for safety
            if os.path.basename(file_path).startswith("executed_orders_"):
                continue
            
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    symbol = data.get("symbol")
                    
                    # Skip files without a symbol (not process status files)
                    if not symbol:
                        continue
                    
                    processed_symbols.add(symbol)
                    
                    # Check if process is still running
                    pid = data.get("pid")
                    is_running = False
                    if pid:
                        try:
                            os.kill(pid, 0) # Check if process exists
                            is_running = True
                        except OSError:
                            is_running = False
                    
                    data["is_running"] = is_running

                    # Overwrite delta_config with the freshest global config
                    # to ensure UI sees the latest saved values immediately
                    data["delta_config"] = global_delta_config

                    status_list.append(data)
            except Exception as e:
                logging.error(f"Error reading status file {file_path}: {e}")

    # Flag duplicates: 2+ live processes trading the same symbol means doubled
    # legs — mark every live instance of such a symbol so the UI can badge them.
    running_symbol_counts: dict[str, int] = {}
    for instance in status_list:
        if instance.get("is_running") and instance.get("symbol"):
            running_symbol_counts[instance["symbol"]] = (
                running_symbol_counts.get(instance["symbol"], 0) + 1
            )
    duplicated_symbols = {s for s, c in running_symbol_counts.items() if c > 1}
    for instance in status_list:
        instance["duplicate"] = (
            bool(instance.get("is_running")) and instance.get("symbol") in duplicated_symbols
        )
    
    # 2. Check for orphan logs (failed starts)
    if os.path.exists(LOG_DIR):
        log_files = glob.glob(os.path.join(LOG_DIR, "*.log"))
        for log_file in log_files:
            symbol = os.path.splitext(os.path.basename(log_file))[0]
            if symbol not in processed_symbols:
                # Create a dummy entry for failed/orphan instances
                status_list.append({
                    "symbol": symbol,
                    "is_running": False,
                    "pid": None,
                    "buy_gap": "-",
                    "sell_gap": "-",
                    "quantity": "-",
                    "order_stats": {"status": "Failed/Stopped"},
                    "timestamp": datetime.fromtimestamp(os.path.getmtime(log_file), tz=IST).strftime('%Y-%m-%d %H:%M:%S') if os.path.exists(log_file) else "Unknown"
                })
    
    # Overlay actual Zerodha open-order prices so the UI always shows the
    # true exchange price, even if a wave extractor script's WebSocket hasn't
    # updated yet or the order was modified externally (e.g., Zerodha app).
    try:
        live_orders = _get_cached_zerodha_orders()
        live_price_map: dict[str, float] = {
            str(o["order_id"]): float(o["price"])
            for o in live_orders
            if o.get("status") == "OPEN" and o.get("price")
        }
        if live_price_map:
            for instance in status_list:
                for order_id, order_detail in (instance.get("orders") or {}).items():
                    if order_id in live_price_map:
                        order_detail["price"] = live_price_map[order_id]

        # Inject OPEN Zerodha orders whose order_id isn't in the status file yet.
        # This bridges the lag window between check_changes_in_restrictions() placing
        # a new sell/buy and write_status_to_file() persisting it (e.g., when BUY is
        # still active so place_duo_order() isn't called that loop cycle).
        if live_orders:
            for instance in status_list:
                symbol_str: str = instance.get("symbol", "")
                if not symbol_str:
                    continue
                # Skip injection for duplicated symbols — with two instances of
                # one symbol we can't attribute an unknown open order to the
                # right instance, and injecting into both double-counts it.
                if symbol_str in duplicated_symbols:
                    continue
                known_ids: set[str] = set((instance.get("orders") or {}).keys())
                for zo in live_orders:
                    zo_id = str(zo.get("order_id", ""))
                    if (
                        zo.get("status") == "OPEN"
                        and zo.get("tradingsymbol") == symbol_str
                        and zo_id
                        and zo_id not in known_ids
                    ):
                        if not instance.get("orders"):
                            instance["orders"] = {}
                        instance["orders"][zo_id] = {
                            "price": float(zo.get("price") or 0),
                            "transaction_type": zo.get("transaction_type", ""),
                            "symbol": symbol_str,
                            "injected_from_live": True,
                        }
                        known_ids.add(zo_id)
    except Exception as exc:
        logging.warning("Price overlay failed, returning cached prices: %s", exc)

    return jsonify(status_list)

@app.route("/api/logs/<symbol>", methods=["GET", "DELETE"])
def handle_logs(symbol):
    log_file = os.path.join(LOG_DIR, f"{symbol}.log")
    
    if request.method == "DELETE":
        if os.path.exists(log_file):
            try:
                # Truncate the file instead of deleting to keep the file handle valid if process is running
                # Or just remove it. If process is running, it might keep writing to the deleted file handle (linux behavior)
                # But user asked to "clear" logs. Truncating is safer if process is running.
                with open(log_file, 'w') as f:
                    f.write("")
                return jsonify({"message": "Logs cleared"})
            except Exception as e:
                return jsonify({"error": str(e)}), 500
        return jsonify({"message": "Log file not found"}), 404

    # GET request
    if os.path.exists(log_file):
        try:
            with open(log_file, 'r') as f:
                content = f.read()
            return jsonify({"content": content})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return jsonify({"content": "No log file found."})

# Global cache for instruments
INSTRUMENTS_MAP = {}

def get_instrument_lookup(kite):
    """Build and return a cached instrument lookup map using SQLite.
    
    The map is keyed by ``tradingsymbol`` for UI compatibility.
    """
    global INSTRUMENTS_MAP
    if not INSTRUMENTS_MAP:
        if instrument_cache.needs_sync():
            _safe_sync_instruments(kite)

        logging.info("Loading instruments from SQLite cache...")
        # Key by tradingsymbol as expected by the dashboard
        INSTRUMENTS_MAP = instrument_cache.get_all_fut_opt_instruments_by_symbol()
        logging.info(f"Loaded {len(INSTRUMENTS_MAP)} F&O instruments into memory map")
        
    return INSTRUMENTS_MAP

@app.route("/admin/instruments")
def admin_instruments():
    """Diagnostic page for instrument database."""
    stats = instrument_cache.get_db_stats()
    return render_template("admin_instruments.html", stats=stats)

@app.route("/admin/instruments/clear", methods=["POST"])
def admin_instruments_clear():
    """Wipes the instrument database for testing."""
    instrument_cache.clear_db()
    global INSTRUMENTS_MAP
    INSTRUMENTS_MAP = {}
    return redirect(url_for('admin_instruments'))

@app.route("/api/sync_instruments", methods=["POST"])
@limiter.limit("10 per minute")
def sync_instruments_route():
    """Manually trigger instrument sync from Zerodha."""
    try:
        kite = get_kite_client()
        if not session.get("access_token"):
            return jsonify({"success": False, "error": "No active session. Please login first."}), 401
            
        logging.info("Manual instrument sync triggered...")
        success = instrument_cache.sync_instruments(kite)
        
        if success:
            # Clear in-memory map to force reload from new DB
            global INSTRUMENTS_MAP
            INSTRUMENTS_MAP.clear()
            return jsonify({"success": True, "message": "Instruments synced successfully."})
        else:
            return jsonify({"success": False, "error": "Sync failed. Check logs."}), 500
            
    except Exception as e:
        logging.error(f"Error in manual sync: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/fetch_positions", methods=["POST"])
def fetch_positions():
    data = request.json
    try:
        kite = get_authenticated_kite_client(data)
        
        # Ensure instruments are loaded
        lookup = get_instrument_lookup(kite)
        
        positions = kite.positions()
        net_positions = positions.get("net", [])
        
        # Process and group positions
        # Structure: { "Underlying": { "Expiry": [ { position_details } ] } }
        grouped_positions = {}
        missing_symbols = False

        import re

        for pos in net_positions:
            if pos['quantity'] == 0:
                continue

            tradingsymbol = pos['tradingsymbol']

            # 1. Determine Underlying Name and Expiry
            # Priority: Lookup > Position expiry field > Regex > Full Symbol
            underlying = ""
            expiry_date_str = "OTHERS"
            inst_data = None

            if tradingsymbol in lookup:
                inst_data = lookup[tradingsymbol]
                underlying = inst_data.get('name')
                exp_date = inst_data.get('expiry')
                if exp_date:
                    if isinstance(exp_date, (date, datetime)):
                        expiry_date_str = exp_date.strftime('%Y-%m-%d')
                    else:
                        expiry_date_str = str(exp_date)
            else:
                # Symbol not in instruments cache — use position's
                # own expiry field from Kite API as fallback
                missing_symbols = True
                pos_expiry = pos.get('expiry')
                if pos_expiry:
                    if isinstance(pos_expiry, (date, datetime)):
                        expiry_date_str = pos_expiry.strftime('%Y-%m-%d')
                    elif isinstance(pos_expiry, str) and pos_expiry:
                        expiry_date_str = pos_expiry

            # Fallback for underlying if lookup didn't yield one (or empty)
            if not underlying:
                underlying = pos.get('name', '')

            if not underlying or underlying == 'UNKNOWN':
                match_name = re.match(r'^([A-Z\\\&\-]+)[\d]', tradingsymbol)
                if match_name:
                    underlying = match_name.group(1)
                else:
                    underlying = tradingsymbol

            # 2. Grouping
            if underlying not in grouped_positions:
                grouped_positions[underlying] = {}

            if expiry_date_str not in grouped_positions[underlying]:
                grouped_positions[underlying][expiry_date_str] = []

            grouped_positions[underlying][expiry_date_str].append({
                "symbol": tradingsymbol,
                "qty": pos['quantity'],
                "price": pos['average_price'],
                "ltp": pos['last_price'],
                "pnl": pos['pnl'],
                "product": pos['product'],
                "lot_size": inst_data.get('lot_size', 1) if inst_data else 1,
                "instrument_token": inst_data.get('instrument_token') if inst_data else None,
                "segment": inst_data.get('segment') if inst_data else None,
                "strike": float(inst_data.get('strike', 0)) if inst_data else 0,
            })

        # If any symbols were missing from instruments cache,
        # invalidate so next API call fetches fresh data
        if missing_symbols:
            logging.warning(
                "Some position symbols missing from instruments cache, "
                "clearing INSTRUMENTS_MAP for refresh on next call"
            )
            INSTRUMENTS_MAP.clear()

        # Sort expiries
        for u in grouped_positions:
            sorted_keys = sorted(grouped_positions[u].keys())
            sorted_inner = {k: grouped_positions[u][k] for k in sorted_keys}
            grouped_positions[u] = sorted_inner

        return jsonify({"positions": grouped_positions})

    except Exception as e:
        logging.error(f"Error fetching positions: {e}")
        return jsonify({"error": str(e)}), 500


# Strike gap mapping for each underlying
STRIKE_GAP_MAP = {
    "NIFTY": 50,
    "BANKNIFTY": 50,
    "FINNIFTY": 50,
    "SENSEX": 100,
}

# Spot symbol mapping for each underlying
SPOT_SYMBOL_MAP = {
    "NIFTY": "NSE:NIFTY 50",
    "BANKNIFTY": "NSE:NIFTY BANK",
    "FINNIFTY": "NSE:NIFTY FIN SERVICE",
    "SENSEX": "BSE:SENSEX",
}


@app.route("/api/all_strikes", methods=["POST"])
def get_all_strikes():
    """Generate strikes covering ATM±10 and all open positions for CE and PE.

    Given an underlying name and expiry date, this endpoint calculates
    the ATM strike from the live spot price and generates strikes covering
    ATM±10 OTM plus any open positions that are further away (filling all
    intermediate strikes). Each strike includes its live LTP via kite.quote().

    Request JSON:
        underlying (str): e.g. "NIFTY", "SENSEX"
        expiry (str): e.g. "2026-02-20" (YYYY-MM-DD format)
        open_ce_strikes (list[float], optional): strike prices of active CE
            positions; used to extend the CE range below ATM if needed.
        open_pe_strikes (list[float], optional): strike prices of active PE
            positions; used to extend the PE range above ATM if needed.

    Returns:
        JSON with 'strikes' list containing symbol, qty, ltp, lot_size
        for each strike, plus 'atm_strike' and 'spot_price'.
    """
    data = request.json or {}
    underlying = data.get("underlying")
    expiry = data.get("expiry")

    if not underlying or not expiry:
        return jsonify({"error": "Both 'underlying' and 'expiry' are required"}), 400

    if "access_token" not in session:
        return jsonify({"error": "Session expired. Please authenticate first."}), 401

    try:
        kite = get_kite_client()

        # Ensure instruments are loaded
        lookup = get_instrument_lookup(kite)
        if not lookup:
            return jsonify({"error": "Failed to load instruments"}), 500

        # Determine strike gap and spot symbol
        strike_gap = STRIKE_GAP_MAP.get(underlying, 50)
        spot_symbol = SPOT_SYMBOL_MAP.get(underlying)

        if not spot_symbol:
            return jsonify({"error": f"Unknown underlying: {underlying}"}), 400

        # Fetch spot price
        try:
            spot_quote = kite.quote(spot_symbol)
            spot_price = spot_quote[spot_symbol]["last_price"]
        except Exception as e:
            logging.error(f"Error fetching spot price for {spot_symbol}: {e}")
            return jsonify({"error": f"Failed to fetch spot price: {e}"}), 500

        # Calculate ATM strike
        atm_strike = round(spot_price / strike_gap) * strike_gap

        # Parse expiry string to date for comparison
        try:
            expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()
        except ValueError:
            return jsonify({"error": f"Invalid expiry format: {expiry}. Use YYYY-MM-DD"}), 400

        # Collect open position strikes passed from frontend (for range extension)
        open_ce_strikes = [float(s) for s in data.get("open_ce_strikes", []) if s]
        open_pe_strikes = [float(s) for s in data.get("open_pe_strikes", []) if s]

        # CE range: from furthest open CE position (or ATM) up to ATM + 10 OTM
        ce_range_min = int(min([atm_strike] + open_ce_strikes) / strike_gap) * strike_gap
        ce_range_max = atm_strike + 10 * strike_gap
        ce_strikes = set(range(int(ce_range_min), int(ce_range_max) + 1, int(strike_gap)))

        # PE range: from ATM - 10 OTM down to furthest open PE position (or ATM)
        pe_range_min = atm_strike - 10 * strike_gap
        pe_range_max = int(max([atm_strike] + open_pe_strikes) / strike_gap) * strike_gap
        pe_strikes = set(range(int(pe_range_min), int(pe_range_max) + 1, int(strike_gap)))

        # Find matching instruments from lookup
        matching_symbols = []
        for sym, inst in lookup.items():
            inst_name = inst.get("name", "")
            inst_expiry = inst.get("expiry")
            inst_type = inst.get("instrument_type", "")
            inst_strike = inst.get("strike")

            if inst_name != underlying:
                continue

            # Compare expiry dates
            if isinstance(inst_expiry, (date, datetime)):
                if hasattr(inst_expiry, 'date'):
                    inst_expiry_date = inst_expiry.date()
                else:
                    inst_expiry_date = inst_expiry
            else:
                try:
                    inst_expiry_date = datetime.strptime(str(inst_expiry), "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    continue

            if inst_expiry_date != expiry_date:
                continue

            if inst_type not in ("CE", "PE") or inst_strike is None:
                continue

            strike_val = float(inst_strike)

            if inst_type == "CE" and strike_val in ce_strikes:
                matching_symbols.append({
                    "symbol": sym,
                    "strike": strike_val,
                    "type": "CE",
                    "lot_size": inst.get("lot_size", 1),
                    "instrument_token": inst.get("instrument_token"),
                })
            elif inst_type == "PE" and strike_val in pe_strikes:
                matching_symbols.append({
                    "symbol": sym,
                    "strike": strike_val,
                    "type": "PE",
                    "lot_size": inst.get("lot_size", 1),
                    "instrument_token": inst.get("instrument_token"),
                })

        # Fetch LTP for all matching symbols
        results = []
        if matching_symbols:
            # Kite quote API accepts max ~500 instruments per call
            exchange_prefix = "BFO:" if underlying == "SENSEX" else "NFO:"
            quote_symbols = [exchange_prefix + m["symbol"] for m in matching_symbols]

            try:
                quotes = kite.quote(quote_symbols)
            except Exception as e:
                logging.error(f"Error fetching quotes for all strikes: {e}")
                quotes = {}

            for m in matching_symbols:
                full_key = exchange_prefix + m["symbol"]
                ltp = quotes.get(full_key, {}).get("last_price", 0)
                results.append({
                    "symbol": m["symbol"],
                    "qty": 0,
                    "price": 0,
                    "ltp": ltp,
                    "pnl": 0,
                    "product": "",
                    "lot_size": m["lot_size"],
                    "is_generated": True,
                    "strike": m["strike"],
                })

        # Sort CE and PE both ascending by strike (strike is already in each result dict)
        ce_results = sorted(
            [r for r in results if r["symbol"].endswith("CE")],
            key=lambda x: x["strike"],
        )
        pe_results = sorted(
            [r for r in results if r["symbol"].endswith("PE")],
            key=lambda x: x["strike"],
        )

        return jsonify({
            "strikes": ce_results + pe_results,
            "atm_strike": atm_strike,
            "spot_price": spot_price,
        })

    except Exception as e:
        logging.error(f"Error generating all strikes: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/trading_mode")
def trading_mode():
    """Report whether order placement is live or dry-run (simulated).

    Drives the LIVE / DRY-RUN badge in the global header so the trading mode
    is always visible on every dashboard page.

    Returns:
        JSON with 'live_trading' boolean from configfile.ini [safety].
    """
    import common_lib
    return jsonify({"live_trading": common_lib.is_live_trading_enabled()})


@app.route("/api/session_status")
def session_status():
    """Check if a valid Kite session exists by probing the API.

    Delegates to _probe_kite_session() which caches successful results for
    10 seconds. Failed probes are never cached. Clears the session token on
    definitive auth failures so all pages show the correct disconnected state.

    Returns:
        JSON with 'authenticated' boolean.
    """
    if "access_token" not in session and session.get("app_authenticated"):
        saved_token = _get_restored_kite_token()
        if saved_token:
            logging.info("[SESSION_STATUS] Restoring access_token from DB.")
            session["access_token"] = saved_token

    if "access_token" not in session:
        logging.info("[SESSION_STATUS] No access_token in session.")
        return jsonify({"authenticated": False})

    authenticated, is_auth_failure = _probe_kite_session(session["access_token"])

    if not authenticated and is_auth_failure:
        # Definitive auth rejection from Kite — clear the token so the UI
        # shows the connect button. Network failures leave the token intact
        # so the next poll retries automatically when connectivity recovers.
        logging.warning("[SESSION_STATUS] Clearing session token after auth failure.")
        session.pop("access_token", None)
        session.pop("request_token", None)

    return jsonify({"authenticated": authenticated})


@app.route("/api/sync_session_token", methods=["GET"])
def sync_session_token():
    """Persist the current browser session's Kite token to the server-side DB.

    This lets headless /api/* calls (e.g. from Claude skills) reuse the token
    without requiring a new browser login.  Call this once after any Kite login.

    Protected by the standard gatekeeper — must be called from an authenticated
    browser session that already has a valid access_token.

    Returns:
        JSON with 'saved': true on success, or an error message.
    """
    if "access_token" not in session:
        return jsonify({"error": "No Kite access_token in current session. Please log in via Kite first."}), 401

    try:
        instrument_cache.save_kite_token(session["access_token"])
        logging.info("Session token manually synced to server-side DB via /api/sync_session_token")
        return jsonify({"saved": True, "message": "Token saved. Headless /api/* calls will now work."})
    except Exception as sync_exc:
        logging.error("Failed to sync session token: %s", sync_exc)
        return jsonify({"error": str(sync_exc)}), 500


@app.route("/api/establish_session", methods=["POST"])
def establish_session():
    """Exchange a request token for an access token and store in session.

    This allows any page to establish a Flask session without
    navigating to /login. Used by the survivor dashboard.

    Request JSON:
        request_token (str): Zerodha request token from OAuth redirect.

    Returns:
        JSON with success status.
    """
    data = request.json or {}
    req_token = data.get("request_token")

    if not req_token:
        return jsonify({"error": "request_token required"}), 400

    try:
        kite_client = MonitoredKite(KiteConnect(api_key=kite_api_key), account_id="main")
        auth_data = kite_client.generate_session(
            req_token, api_secret=kite_api_secret
        )
        _set_kite_session(auth_data["access_token"])

        # Trigger instrument sync if needed (9 AM daily reset)
        if instrument_cache.needs_sync():
            _safe_sync_instruments(kite_client)

        return jsonify({"authenticated": True})
    except Exception as e:
        logging.error(f"Error establishing session: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/get_login_url")
def get_login_url():
    try:
        kite = KiteConnect(api_key=kite_api_key)
        return jsonify({"login_url": kite.login_url()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/get_request_token")
def get_request_token():
    status = request.args.get("status")
    request_token = request.args.get("request_token")
    type_ = request.args.get("type")

    if status == "success" and request_token:
        # Success page that posts message back to opener. request_token/status
        # are attacker-controlled query params — rendered via Jinja auto-escaping
        # (HTML context) and the `tojson` filter (JS context) rather than raw
        # f-string interpolation, mirroring the safe pattern already used by the
        # /login route above. Do not switch this back to an f-string.
        return render_template_string("""
        <html>
        <head><title>Auth Success</title></head>
        <body style="background:#e8f5e9; font-family:sans-serif; text-align:center; padding-top:50px;">
            <h2 style="color:green;">Authentication Successful!</h2>
            <p>You can close this window now.</p>

            <div onclick="copyToken()" title="Click to Copy" style="
                cursor: pointer;
                background: #fff;
                padding: 15px 30px;
                border-radius: 8px;
                border: 2px dashed #4CAF50;
                display: inline-block;
                margin: 20px;
                transition: all 0.2s;
                box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            " onmouseover="this.style.transform='scale(1.05)'" onmouseout="this.style.transform='scale(1)'">
                <div style="font-size: 24px; font-weight: bold; color: #333; letter-spacing: 1px;">{{ request_token }}</div>
                <div style="font-size: 12px; color: #888; margin-top: 5px;">CLICK TO COPY</div>
            </div>

            <script>
                var token = {{ request_token | tojson }};

                function copyToken() {
                    const el = document.createElement('textarea');
                    el.value = token;
                    document.body.appendChild(el);
                    el.select();
                    document.execCommand('copy');
                    document.body.removeChild(el);

                    // Visual feedback
                    const div = document.querySelector('div[onclick]');
                    const originalBg = div.style.backgroundColor;
                    div.style.backgroundColor = '#dcedc8'; // Light green
                    setTimeout(() => div.style.backgroundColor = '#fff', 300);
                }

                // Send token to parent window (same-origin only — never a wildcard)
                if (window.opener) {
                    window.opener.postMessage({
                        type: 'ZERODHA_TOKEN',
                        token: token
                    }, window.location.origin);

                    // Close self after short delay
                    setTimeout(function() {
                        window.close();
                    }, 1500);
                }
            </script>
        </body>
        </html>
        """, request_token=request_token)
    else:
        return render_template_string("<h3>Authentication Failed or Cancelled. Status: {{ status }}</h3>", status=status)

_SCRAPER_SCRIPT_NAME = "ticker_single_scraper_new.py"

# Serializes the running-check + Popen in start_instance so two simultaneous
# requests can't both pass the duplicate check before either child spawns.
_start_instance_lock = threading.Lock()


def _find_running_scrapers(symbol: Optional[str] = None) -> List[Dict[str, Any]]:
    """Scan live processes for wave extractor scraper instances.

    Ground truth is the process table (not status files), so duplicates are
    detected even if two processes share one legacy status file.

    Args:
        symbol: If given, only return instances trading this exact symbol.

    Returns:
        List of ``{"symbol": str, "pid": int, "started": str}`` for each live
        scraper process, empty on scan failure.
    """
    try:
        ps_output = subprocess.check_output(
            ["ps", "-eo", "pid,lstart,args"], text=True
        )
    except (subprocess.SubprocessError, OSError) as exc:
        logging.error("_find_running_scrapers: process scan failed: %s", exc)
        return []

    running: List[Dict[str, Any]] = []
    for line in ps_output.splitlines()[1:]:
        if _SCRAPER_SCRIPT_NAME not in line:
            continue
        parts = line.strip().split()
        try:
            pid = int(parts[0])
        except (ValueError, IndexError):
            continue
        # lstart is 5 tokens after split (e.g. "Mon Jul 6 04:16:12 2026")
        started = " ".join(parts[1:6])
        args = parts[6:]
        try:
            script_index = next(
                i for i, arg in enumerate(args) if arg.endswith(_SCRAPER_SCRIPT_NAME)
            )
            # argv: <script> <buy_gap> <sell_gap> <symbol> <quantity> <token> ...
            proc_symbol = args[script_index + 3]
        except (StopIteration, IndexError):
            continue
        if symbol and proc_symbol != symbol:
            continue
        running.append({"symbol": proc_symbol, "pid": pid, "started": started})
    return running


@app.route("/api/wave_extractor/running_symbols")
def wave_extractor_running_symbols():
    """Return live scraper instances (PID-verified via the process table).

    Feeds the dashboard's "already running" indicator in the Start New
    Instances table. Only processes alive right now are returned — stale
    status files and stopped instances never appear here.
    """
    return jsonify(_find_running_scrapers())


# Kite trading symbols are uppercase alphanumerics plus '&' (M&M) and '-'
# (BAJAJ-AUTO). Anything else — especially '/', '.', whitespace — is rejected
# before the symbol reaches a log-file path or a subprocess argument.
_TRADING_SYMBOL_RE = re.compile(r"^[A-Z0-9&-]{1,32}$")


def _validate_start_params(
    symbol: Any, buy_gap: Any, sell_gap: Any, buy_quantity: Any, sell_quantity: Any
) -> Optional[str]:
    """Validate /api/start trading parameters.

    Args:
        symbol: Trading symbol string (e.g. "NIFTY2670724400CE").
        buy_gap: Gap in points below LTP for the buy leg; numeric, 0 < gap <= 10000.
        sell_gap: Gap in points above LTP for the sell leg; numeric, 0 < gap <= 10000.
        buy_quantity: Positive integer lot quantity, <= 100000.
        sell_quantity: Positive integer lot quantity, <= 100000.

    Returns:
        An error message string if any parameter is invalid, else None.
    """
    if not isinstance(symbol, str) or not _TRADING_SYMBOL_RE.match(symbol):
        return "Invalid symbol: expected 1-32 uppercase alphanumeric characters (plus & or -)"
    for name, gap in (("buy_gap", buy_gap), ("sell_gap", sell_gap)):
        try:
            gap_value = float(gap)
        except (TypeError, ValueError):
            return f"Invalid {name}: must be a number"
        if not 0 < gap_value <= 10000:
            return f"Invalid {name}: must be > 0 and <= 10000"
    for name, qty in (("buy_quantity", buy_quantity), ("sell_quantity", sell_quantity)):
        try:
            qty_value = int(qty)
        except (TypeError, ValueError):
            return f"Invalid {name}: must be an integer"
        if not 0 < qty_value <= 100000 or qty_value != float(qty):
            return f"Invalid {name}: must be a whole number > 0 and <= 100000"
    return None


@app.route("/api/start", methods=["POST"])
@limiter.limit("30 per minute")
def start_instance():
    data = request.json
    symbol = data.get("symbol")
    buy_gap = data.get("buy_gap")
    sell_gap = data.get("sell_gap")
    buy_quantity = data.get("buy_quantity")
    sell_quantity = data.get("sell_quantity")
    request_token = get_token_for_script(data.get("request_token"))
    is_gtt = data.get("is_gtt", False)
    force = bool(data.get("force", False))

    if not all([symbol, buy_gap, sell_gap, buy_quantity, sell_quantity, request_token]):
        return jsonify({"error": "Missing required parameters or not authenticated"}), 400

    validation_error = _validate_start_params(symbol, buy_gap, sell_gap, buy_quantity, sell_quantity)
    if validation_error:
        logging.warning("start_instance: rejected invalid params — %s", validation_error)
        return jsonify({"error": validation_error}), 400

    quantity = f"{buy_quantity}:{sell_quantity}"

    # Use absolute path to avoid ambiguity
    # flask_app.py is in vibhu/, so we get that directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(base_dir, _SCRAPER_SCRIPT_NAME)

    cmd = [
        sys.executable, script_path,
        str(buy_gap), str(sell_gap), symbol, str(quantity), request_token
    ]

    if is_gtt:
        cmd.extend(["NRML", "GTT"])

    log_path = os.path.join(LOG_DIR, f"{symbol}.log")

    with _start_instance_lock:
        # Duplicate-instance guard: a second scraper for the same symbol places
        # its own BUY+SELL duo → doubled legs (NIFTY2670724400CE incident).
        # force=true is sent only after the user explicitly confirms a
        # duplicate, or by the restart flow after killing the old PID.
        existing = _find_running_scrapers(symbol)
        if existing and not force:
            live = existing[0]
            logging.warning(
                "start_instance: rejected duplicate start for %s — already running as PID %s (started %s)",
                symbol, live["pid"], live["started"],
            )
            return jsonify({
                "error": "already_running",
                "message": f"{symbol} already has a running instance (PID {live['pid']}, started {live['started']}).",
                "symbol": symbol,
                "pid": live["pid"],
                "started": live["started"],
            }), 409

        try:
            # Append (not truncate) so prior runs stay diagnosable; banner marks
            # each start so overlapping/duplicate runs are visible in the log.
            with open(log_path, "a") as log_file:
                log_file.write(
                    f"\n=== scraper start {get_ist_now().isoformat()} symbol={symbol} "
                    f"force={force} ===\n"
                )
                log_file.flush()
                process = subprocess.Popen(
                    cmd, stdout=log_file, stderr=subprocess.STDOUT, start_new_session=True
                )
            logging.info("start_instance: started %s (PID %s, force=%s)", symbol, process.pid, force)
            return jsonify({"message": f"Started instance for {symbol}. Check logs for details.",
                            "pid": process.pid})
        except (OSError, subprocess.SubprocessError) as e:
            logging.error("start_instance: failed to start %s: %s", symbol, e)
            return jsonify({"error": str(e)}), 500


# ... (Skipping to stop_all_instances update)

@app.route("/api/stop_all", methods=["POST"])
@limiter.limit("120 per minute")
def stop_all_instances():
    """Stop all running Wave Extractor instances.

    Accepts optional JSON body:
        cancel_orders (bool): If true, cancel each instance's open limit orders
            after killing its process (process killed first so it cannot re-place them).
    """
    data = request.json or {}
    cancel_orders: bool = bool(data.get("cancel_orders", False))

    # Only target status_*.json files
    status_files = glob.glob(os.path.join(STATUS_DIR, "status_*.json"))
    results = []

    # --- Phase 1: read order IDs + kill all processes ---
    instances: list[dict] = []
    for status_file in status_files:
        try:
            with open(status_file, "r") as f:
                instance_data = json.load(f)
            pid = instance_data.get("pid")
            symbol = instance_data.get("symbol", "")
            order_ids = list(instance_data.get("orders", {}).keys())
            instances.append({"pid": pid, "symbol": symbol, "order_ids": order_ids,
                               "status_file": status_file})
            if pid:
                try:
                    os.kill(int(pid), signal.SIGTERM)
                    results.append(f"Stopped {symbol} (PID: {pid})")
                except ProcessLookupError:
                    results.append(f"{symbol} (PID: {pid}) was not running")
                except Exception as exc:
                    results.append(f"Error stopping {symbol}: {exc}")
        except Exception as exc:
            results.append(f"Error reading {status_file}: {exc}")

    # --- Phase 2: cancel open orders (all processes now dead) ---
    if cancel_orders and instances:
        time.sleep(0.5)
        try:
            kite_client = get_authenticated_kite_client({})
            for inst in instances:
                sym = inst["symbol"]
                for oid in inst["order_ids"]:
                    try:
                        kite_client.cancel_order(variety=kite_client.VARIETY_REGULAR,
                                                 order_id=str(oid))
                        results.append(f"Cancelled order {oid} for {sym}")
                    except Exception as exc:
                        results.append(f"Cancel {oid} ({sym}): {exc}")
        except RuntimeError as auth_err:
            results.append(f"Order cancellation skipped — not authenticated: {auth_err}")

    # --- Phase 3: delete status files ---
    for inst in instances:
        try:
            if os.path.exists(inst["status_file"]):
                os.remove(inst["status_file"])
        except Exception as exc:
            results.append(f"Error deleting {inst['status_file']}: {exc}")

    return jsonify({"message": "Stop all command processed", "details": results})


@app.route("/api/stop_group", methods=["POST"])
@limiter.limit("120 per minute")
def stop_group_instances():
    """Stop a specific group of Wave Extractor instances by PID list.

    Accepts JSON body:
        pids (list[int]): Process IDs to stop. Required.
        cancel_orders (bool): If true, cancel open limit orders after killing processes.
    """
    data = request.json or {}
    pids_raw: list = data.get("pids", [])
    cancel_orders: bool = bool(data.get("cancel_orders", False))

    if not pids_raw:
        return jsonify({"error": "pids list required"}), 400

    target_pids: set[int] = {int(p) for p in pids_raw if p}
    status_files = glob.glob(os.path.join(STATUS_DIR, "status_*.json"))
    results: list[str] = []
    instances: list[dict] = []

    # --- Phase 1: read order IDs + kill target processes ---
    for status_file in status_files:
        try:
            with open(status_file, "r") as f:
                instance_data = json.load(f)
            pid = instance_data.get("pid")
            if not pid or int(pid) not in target_pids:
                continue
            symbol = instance_data.get("symbol", "")
            order_ids = list(instance_data.get("orders", {}).keys())
            instances.append({"pid": pid, "symbol": symbol, "order_ids": order_ids,
                               "status_file": status_file})
            try:
                os.kill(int(pid), signal.SIGTERM)
                results.append(f"Stopped {symbol} (PID: {pid})")
            except ProcessLookupError:
                results.append(f"{symbol} (PID: {pid}) was not running")
            except Exception as exc:
                results.append(f"Error stopping {symbol}: {exc}")
        except Exception as exc:
            results.append(f"Error reading {status_file}: {exc}")

    # --- Phase 2: cancel open orders (all target processes now dead) ---
    if cancel_orders and instances:
        time.sleep(0.5)
        try:
            kite_client = get_authenticated_kite_client({})
            for inst in instances:
                sym = inst["symbol"]
                for oid in inst["order_ids"]:
                    try:
                        kite_client.cancel_order(variety=kite_client.VARIETY_REGULAR,
                                                 order_id=str(oid))
                        results.append(f"Cancelled order {oid} for {sym}")
                    except Exception as exc:
                        results.append(f"Cancel {oid} ({sym}): {exc}")
        except RuntimeError as auth_err:
            results.append(f"Order cancellation skipped — not authenticated: {auth_err}")

    # --- Phase 3: delete status files ---
    for inst in instances:
        try:
            if os.path.exists(inst["status_file"]):
                os.remove(inst["status_file"])
        except Exception as exc:
            results.append(f"Error deleting {inst['status_file']}: {exc}")

    return jsonify({"message": f"Stop group command processed ({len(instances)} instance(s))", "details": results})


@app.route("/api/clear_status", methods=["POST"])
def clear_all_status():
    # Only target status_*.json files
    status_files = glob.glob(os.path.join(STATUS_DIR, "status_*.json"))
    results = []
    
    # 1. Process status files and their logs
    for status_file in status_files:
        try:
            with open(status_file, 'r') as f:
                data = json.load(f)
                pid = data.get("pid")
                is_running = False
                if pid:
                    try:
                        os.kill(pid, 0) # Check if process exists
                        is_running = True
                    except OSError:
                        is_running = False
                
                if not is_running:
                    os.remove(status_file)
                    results.append(f"Deleted {os.path.basename(status_file)}")
                else:
                    # results.append(f"Skipped {os.path.basename(status_file)} (Running)")
                    pass
                    
        except Exception as e:
            results.append(f"Error processing {os.path.basename(status_file)}: {str(e)}")

    # 2. Clean up orphan logs (logs without status files)
    if os.path.exists(LOG_DIR):
        log_files = glob.glob(os.path.join(LOG_DIR, "*.log"))
        for log_file in log_files:
            symbol = os.path.splitext(os.path.basename(log_file))[0]
            # Check for any status file of this symbol (legacy status_<symbol>.json
            # or per-PID status_<symbol>_<pid>.json)
            matching_status = glob.glob(os.path.join(STATUS_DIR, f"status_{symbol}*.json"))

            # If no status file exists, it's an orphan log
            if not matching_status:
                try:
                    os.remove(log_file)
                    results.append(f"Deleted orphan log {os.path.basename(log_file)}")
                except Exception as e:
                    results.append(f"Error deleting orphan log {os.path.basename(log_file)}: {str(e)}")
            
    return jsonify({"message": "Clear status command processed", "details": results})

@app.route("/api/stop", methods=["POST"])
@limiter.limit("120 per minute")
def stop_instance():
    """Stop a single Wave Extractor instance by PID.

    Accepts JSON body:
        pid (int): Process ID of the scraper subprocess. Required.
        symbol (str): Trading symbol (used to locate the status file). Optional.
        cancel_orders (bool): If true, cancel the instance's open limit orders after
            killing the process (process killed first so it cannot re-place them).
    """
    data = request.json or {}
    pid = data.get("pid")
    symbol = data.get("symbol")
    cancel_orders: bool = bool(data.get("cancel_orders", False))

    if not pid:
        return jsonify({"error": "PID required"}), 400

    # --- Step 1: read order IDs from status file before anything is deleted ---
    # Prefer the per-PID file (status_<symbol>_<pid>.json) so stopping one of
    # two duplicate instances only touches that instance's orders/file; fall
    # back to the legacy per-symbol file for pre-migration instances.
    order_ids: list[str] = []
    status_file: str | None = None
    if symbol:
        per_pid_file = os.path.join(STATUS_DIR, f"status_{symbol}_{pid}.json")
        legacy_file = os.path.join(STATUS_DIR, f"status_{symbol}.json")
        status_file = per_pid_file if os.path.exists(per_pid_file) else legacy_file
        if os.path.exists(status_file):
            try:
                with open(status_file, "r") as f:
                    status_data = json.load(f)
                order_ids = list(status_data.get("orders", {}).keys())
            except Exception as exc:
                logging.warning("stop_instance: could not read status file for %s: %s", symbol, exc)

    # --- Step 2: kill the process ---
    try:
        os.kill(int(pid), signal.SIGTERM)
    except ProcessLookupError:
        pass  # Already dead; proceed to cleanup

    # --- Step 3: cancel open orders (process is now dead, cannot re-place) ---
    cancel_results: list[dict] = []
    if cancel_orders and order_ids:
        time.sleep(0.5)  # Let the process exit cleanly before Kite sees cancellations
        try:
            kite_client = get_authenticated_kite_client({})
            for oid in order_ids:
                try:
                    kite_client.cancel_order(variety=kite_client.VARIETY_REGULAR,
                                             order_id=str(oid))
                    cancel_results.append({"order_id": oid, "status": "cancelled"})
                    logging.info("stop_instance: cancelled order %s for %s", oid, symbol)
                except Exception as exc:
                    cancel_results.append({"order_id": oid, "status": "error", "error": str(exc)})
                    logging.warning("stop_instance: cancel order %s failed: %s", oid, exc)
        except RuntimeError as auth_err:
            logging.warning("stop_instance: order cancellation skipped — not authenticated: %s", auth_err)
            cancel_results.append({"error": f"Not authenticated — orders not cancelled: {auth_err}"})

    # --- Step 4: delete status file ---
    if status_file and os.path.exists(status_file):
        try:
            os.remove(status_file)
        except Exception as exc:
            logging.warning("stop_instance: could not delete status file: %s", exc)

    return jsonify({"message": f"Stopped process {pid}", "cancel_results": cancel_results})

@app.route("/api/delta_config", methods=["GET", "POST"])
def delta_config():
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "delta_limits.json")

    if request.method == "GET":
        try:
            current_config: dict = {}
            if os.path.exists(config_path):
                with open(config_path, "r") as f:
                    current_config = json.load(f)

            from instrument_cache import get_upcoming_expiries
            upcoming_expiries: dict[str, list[str]] = {
                underlying: get_upcoming_expiries(underlying, 2)
                for underlying in ("NIFTY", "BANKNIFTY", "SENSEX")
            }

            return jsonify({"config": current_config, "upcoming_expiries": upcoming_expiries})
        except Exception as e:
            logging.error(f"Error reading delta config: {e}")
            return jsonify({"error": str(e)}), 500

    # POST — update a single underlying/expiry entry
    data = request.json
    underlying = data.get("underlying")
    expiry = data.get("expiry")
    min_val = data.get("min")
    max_val = data.get("max")

    if not all([underlying, expiry, min_val is not None, max_val is not None]):
        return jsonify({"error": "Missing parameters"}), 400

    try:
        current_config = {}
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                current_config = json.load(f)

        if underlying not in current_config:
            current_config[underlying] = {}

        current_config[underlying][expiry] = {
            "min": int(min_val),
            "max": int(max_val),
        }

        with open(config_path, "w") as f:
            json.dump(current_config, f, indent=4)

        return jsonify({"message": "Configuration updated successfully"})

    except Exception as e:
        logging.error(f"Error updating delta config: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# Kite Session Helpers
# ============================================================================


def _set_kite_session(access_token: str):
    """Store Kite access_token in both Flask session and server-side DB.

    This ensures that even if the Flask session is lost (e.g. server restart
    or cookie expiration), the token can be recovered as long as the user
    is still logged into the app.
    """
    session["access_token"] = access_token
    session.permanent = True
    try:
        instrument_cache.save_kite_token(access_token)
    except Exception as e:
        logging.error(f"Failed to auto-save Kite token to DB: {e}")

    # Authenticate the shared common_lib.kite singleton. Modules that call
    # common_lib.kite / common_lib.get_nifty_current_quote() directly
    # (cas_tracker, iv_spike_tracker, ticker.py, etc.) rely on this object —
    # it is otherwise never given an access_token under flask_app.py, since
    # every other consumer (get_kite_client(), the per-engine clients below)
    # builds its own separate MonitoredKite instance instead of touching it.
    try:
        from common_lib import kite as _shared_kite
        _shared_kite.set_access_token(access_token)
    except Exception:
        logging.exception("Failed to authenticate shared common_lib.kite singleton after login")


def _get_restored_kite_token() -> Optional[str]:
    """Attempt to restore the Kite access_token from the server-side DB."""
    try:
        return instrument_cache.get_kite_token()
    except Exception as e:
        logging.error(f"Failed to retrieve Kite token from DB: {e}")
        return None


# ============================================================================
# Order Logic Helpers
# ============================================================================


@app.route("/api/order_history")
def get_order_history():
    """
    Get today's executed order history.
    
    Returns:
        JSON list of orders sorted by time (newest first).
    """
    try:
        # Import here to avoid circular import
        from common_lib import load_todays_orders
        orders = load_todays_orders()
        return jsonify({"orders": orders})
    except Exception as e:
        logging.error(f"Error fetching order history: {e}")
        return jsonify({"error": str(e), "orders": []}), 500


@app.route("/api/order_summary")
def get_order_summary():
    """
    Get PE and CE summary statistics for today's orders.
    
    Returns:
        JSON with 'ce_summary' and 'pe_summary' lists containing
        symbol, expiry, buy_count, sell_count, realized_pnl for each.
    """
    try:
        # Import here to avoid circular import
        from common_lib import calculate_order_summary
        summary = calculate_order_summary()
        return jsonify(summary)
    except Exception as e:
        logging.error(f"Error fetching order summary: {e}")
        return jsonify({"error": str(e), "ce_summary": [], "pe_summary": []}), 500


# ============================================================================
# Wave Extractor Order Tracking Endpoints
# ============================================================================

@app.route("/api/wave_extractor/order_history")
def get_wave_extractor_order_history():
    """
    Get today's executed order history for Wave Extractor algo only.

    Returns:
        JSON list of Wave Extractor orders sorted by time (newest first).
    """
    try:
        from common_lib import load_wave_extractor_orders
        orders = load_wave_extractor_orders()
        return jsonify({"orders": orders})
    except Exception as e:
        logging.error(f"Error fetching Wave Extractor order history: {e}")
        return jsonify({"error": str(e), "orders": []}), 500


@app.route("/api/wave_extractor/order_summary")
def get_wave_extractor_order_summary():
    """
    Get PE and CE summary statistics for Wave Extractor orders only.

    Returns:
        JSON with 'ce_summary' and 'pe_summary' lists containing
        symbol, expiry, buy_count, sell_count, realized_pnl for each.
    """
    try:
        from common_lib import calculate_wave_extractor_order_summary
        summary = calculate_wave_extractor_order_summary()
        return jsonify(summary)
    except Exception as e:
        logging.error(f"Error fetching Wave Extractor order summary: {e}")
        return jsonify({"error": str(e), "ce_summary": [], "pe_summary": []}), 500


@app.route("/api/wave_extractor/live_positions", methods=["GET"])
def wave_extractor_live_positions():
    """Return net positions from Kite keyed by tradingsymbol for wave extractor display.

    Fetches all net positions and returns a flat dict so the frontend can look up
    each running instance's symbol in O(1) without additional API calls.

    Returns:
        JSON with 'positions' dict mapping tradingsymbol to {qty, pnl, realised, avg_price}.
        Always returns HTTP 200 (empty positions on auth failure) so the UI never breaks.
    """
    try:
        kite_client = get_authenticated_kite_client({})
        raw_positions = kite_client.positions()
        positions_by_symbol: dict[str, dict] = {}
        for pos in raw_positions.get("net", []):
            symbol = pos.get("tradingsymbol", "")
            if not symbol:
                continue
            positions_by_symbol[symbol] = {
                "qty": pos.get("quantity", 0),
                "pnl": round(pos.get("unrealised", 0), 2),
                "realised": round(pos.get("realised", 0), 2),
                "avg_price": round(pos.get("average_price", 0), 2),
            }
        return jsonify({"positions": positions_by_symbol})
    except Exception as exc:
        logging.error("wave_extractor_live_positions failed: %s", exc, exc_info=True)
        return jsonify({"positions": {}, "error": str(exc)})


@app.route("/api/wave_extractor/amplitude_expiries", methods=["GET"])
def wave_extractor_amplitude_expiries():
    """Return upcoming expiry dates for the given underlying (testbed dropdown).

    Query Args:
        underlying: "NIFTY", "BANKNIFTY", or "SENSEX".

    Returns:
        JSON {"expiries": [<YYYY-MM-DD>, ...]}.
    """
    underlying = request.args.get("underlying", "")
    if underlying not in ("NIFTY", "BANKNIFTY", "SENSEX"):
        return jsonify({"error": "Invalid underlying", "expiries": []}), 400
    expiries = instrument_cache.get_upcoming_expiries(underlying, count=12)
    return jsonify({"expiries": expiries})


@app.route("/api/wave_extractor/amplitude_table", methods=["GET"])
def wave_extractor_amplitude_table():
    """Compute the Amplitude Testbed's per-strike theoretical gap table.

    Read-only preview — never places or modifies any order and never touches
    a running scraper instance. See WAVE_EXTRACTOR.md for the amplitude
    formula's rationale.

    Rows come from either your currently-held open positions for the given
    underlying/expiry (default), or the full option chain for that expiry
    when ``positions_only=false``. DTE is holiday-aware (see
    ``instrument_cache.get_trading_days_to_expiry()``), not a naive Mon-Fri
    count.

    Each row returns raw fields (iv, delta, gamma, amplitude_pct,
    reference_amplitude_pct) — ``correction_multiplier`` and
    ``hard_floor_pct``/ceiling are intentionally NOT applied server-side so
    the frontend can recompute suggested gaps instantly on knob changes
    without a new network round-trip. ``reference_dte`` DOES require a new
    call, since it changes the Greeks computation itself.

    Query Args:
        underlying: "NIFTY", "BANKNIFTY", or "SENSEX".
        expiry: Expiry date string in YYYY-MM-DD format.
        positions_only: "true" (default) or "false".
        reference_dte: Optional hypothetical DTE baseline for
            amplitude_multiplier. When omitted, auto-derived as the trading
            days between the previous expiry and this one — i.e. this
            expiry's own full cycle length at issuance — via
            ``instrument_cache.get_previous_expiry()``. Falls back to the
            current DTE (multiplier ≈ 1) if there is no earlier expiry on
            record.

    Returns:
        JSON {"rows": [...], "dte": ..., "spot": ..., "reference_dte": ...}
        or {"error": ...}. Each row has: tradingsymbol, strike, option_type,
        qty (only when positions_only), spot, ltp, dte, iv, delta, gamma,
        amplitude_pct, reference_amplitude_pct, amplitude_multiplier.
    """
    underlying = request.args.get("underlying", "")
    expiry = request.args.get("expiry", "")
    positions_only = request.args.get("positions_only", "true").lower() != "false"
    reference_dte_override = request.args.get("reference_dte", type=float)
    if underlying not in ("NIFTY", "BANKNIFTY", "SENSEX") or not expiry:
        return jsonify({"error": "Invalid parameters"}), 400

    import common_lib

    expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()

    dte = max(instrument_cache.get_trading_days_to_expiry(expiry_date), 0.0001)

    if reference_dte_override is not None:
        reference_dte = max(reference_dte_override, 0.0001)
    else:
        # instruments.db only ever holds current/future expiries (Kite's live
        # dump drops expired contracts), so the previous expiry is reliably
        # absent for the nearest expiry — use the NEXT expiry's spacing as
        # the cycle-length proxy instead (weeklies/monthlies have a
        # consistent cadence, so this is equivalent in magnitude).
        next_expiry = instrument_cache.get_next_expiry(underlying, expiry)
        if next_expiry:
            next_expiry_date = datetime.strptime(next_expiry, "%Y-%m-%d").date()
            reference_dte = max(
                instrument_cache.get_trading_days_to_expiry(
                    next_expiry_date, from_date=expiry_date + timedelta(days=1)
                ),
                0.0001,
            )
        else:
            # No further expiry on record (e.g. the furthest one synced) —
            # fall back to this expiry's own DTE, so amplitude_multiplier ≈ 1.
            reference_dte = dte

    try:
        kite = get_authenticated_kite_client({})
    except RuntimeError as auth_err:
        return jsonify({"error": f"Not authenticated — please reconnect Kite: {auth_err}"}), 401

    # Resolve the row set: either held positions or the full chain.
    rows_meta: list[dict] = []
    if positions_only:
        try:
            net_positions = kite.positions().get("net", [])
        except Exception as exc:
            logging.error("wave_extractor_amplitude_table: positions fetch failed: %s", exc, exc_info=True)
            return jsonify({"error": f"Could not fetch positions: {exc}"}), 502
        for pos in net_positions:
            if pos.get("quantity", 0) == 0:
                continue
            inst = instrument_cache.get_instrument_by_token(pos.get("instrument_token"))
            if not inst or inst.get("name") != underlying:
                continue
            if str(inst.get("expiry")) != expiry or inst.get("instrument_type") not in ("CE", "PE"):
                continue
            rows_meta.append({
                "tradingsymbol": inst["tradingsymbol"],
                "exchange": inst["exchange"],
                "strike": inst["strike"],
                "option_type": inst["instrument_type"],
                "qty": pos["quantity"],
            })
    else:
        for inst in instrument_cache.get_option_chain_for_expiry(underlying, expiry):
            rows_meta.append({
                "tradingsymbol": inst["tradingsymbol"],
                "exchange": inst["exchange"],
                "strike": inst["strike"],
                "option_type": inst["instrument_type"],
                "qty": None,
            })

    if not rows_meta:
        return jsonify({"rows": [], "reference_dte": reference_dte})

    spot_key = common_lib._get_spot_symbol_for(underlying)
    quote_keys = [f"{r['exchange']}:{r['tradingsymbol']}" for r in rows_meta]
    if spot_key:
        quote_keys.append(spot_key)

    try:
        quotes = kite.quote(quote_keys)
        spot = quotes[spot_key]["last_price"] if spot_key else None
    except Exception as exc:
        logging.error("wave_extractor_amplitude_table: quote fetch failed: %s", exc, exc_info=True)
        return jsonify({"error": f"Could not fetch live quotes: {exc}"}), 502

    if not spot:
        return jsonify({"error": f"Could not resolve spot price for {underlying}"}), 502

    rows = []
    for meta in rows_meta:
        quote_key = f"{meta['exchange']}:{meta['tradingsymbol']}"
        ltp = quotes.get(quote_key, {}).get("last_price")
        if not ltp:
            continue

        result = common_lib.get_expected_amplitude_pct(
            spot=spot, strike=meta["strike"], dte=dte, ltp=ltp,
            option_type=meta["option_type"], interest_rate_pct=common_lib.interest_rate,
        )
        reference_result = common_lib.get_theoretical_amplitude_at_dte(
            spot=spot, strike=meta["strike"], dte=reference_dte,
            option_type=meta["option_type"], interest_rate_pct=common_lib.interest_rate,
            volatility_pct=result["iv"] or 15.0,
        )
        reference_amplitude_pct = reference_result["amplitude_pct"] or result["amplitude_pct"]
        amplitude_multiplier = (
            result["amplitude_pct"] / reference_amplitude_pct if reference_amplitude_pct else 1.0
        )

        rows.append({
            "tradingsymbol": meta["tradingsymbol"],
            "strike": meta["strike"],
            "option_type": meta["option_type"],
            "qty": meta["qty"],
            "spot": spot,
            "ltp": ltp,
            "dte": dte,
            "iv": result["iv"],
            "delta": result["delta"],
            "gamma": result["gamma"],
            "amplitude_pct": result["amplitude_pct"],
            "reference_amplitude_pct": reference_amplitude_pct,
            "amplitude_multiplier": round(amplitude_multiplier, 3),
        })

    return jsonify({"rows": rows, "dte": dte, "spot": spot, "reference_dte": reference_dte})


@app.route("/api/wave_extractor/update_prices", methods=["POST"])
def update_wave_extractor_prices():
    """
    Modify open BUY/SELL order prices for Wave Extractor instances.
    Accepts pre-calculated exact prices — the JS computes the final price
    from either the group % adjustment or a manually entered value.

    Args (JSON body):
        updates: list of {
            symbol: str,
            buyOrderId: str | None,
            newBuyPrice: float | None,
            sellOrderId: str | None,
            newSellPrice: float | None
        }

    Returns:
        JSON {"results": {symbol: {order_label: {"success": bool, "new_price": float}}}}
    """
    try:
        kite = get_authenticated_kite_client({})
    except RuntimeError as auth_err:
        logging.warning("[update_prices] No authenticated Kite session: %s", auth_err)
        return jsonify({"error": "Not authenticated — please reconnect Kite."}), 401

    data = request.json or {}
    updates: list[dict] = data.get("updates", [])

    if not updates:
        return jsonify({"error": "No updates provided"}), 400

    results: dict = {}

    for update in updates:
        symbol: str = update.get("symbol", "unknown")
        symbol_results: dict = {}

        order_sides = [
            ("buyOrderId", "newBuyPrice", "BUY"),
            ("sellOrderId", "newSellPrice", "SELL"),
        ]
        for order_id_key, price_key, tx_label in order_sides:
            order_id = update.get(order_id_key)
            new_price = update.get(price_key)

            if not order_id or new_price is None:
                continue

            try:
                # Wave extractor places regular limit orders — do NOT pass
                # trigger_price (only valid for SL orders; causes API rejection).
                kite.modify_order(
                    kite.VARIETY_REGULAR,
                    str(order_id),
                    price=float(new_price),
                )
                success = True
                logging.info(
                    "[update_prices] %s %s order %s → %.2f: OK",
                    symbol, tx_label, order_id, float(new_price),
                )
            except Exception as exc:
                logging.error(
                    "[update_prices] %s %s order %s failed: %s",
                    symbol, tx_label, order_id, exc, exc_info=True,
                )
                success = False

            result_key = f"{tx_label}_{order_id}"
            symbol_results[result_key] = {"success": success, "new_price": new_price}

        results[symbol] = symbol_results

    return jsonify({"results": results})


@app.route("/api/wave_extractor/config", methods=["GET", "POST"])
def wave_extractor_config_api():
    """Read or write the wave_extractor_config.json algo parameters.

    GET  → returns the full config dict as JSON
    POST → accepts a full or partial config dict; merges per-underlying keys
           and writes atomically via save_wave_extractor_config()

    Returns:
        GET:  JSON {"config": {...}}
        POST: JSON {"status": "ok"} or {"error": "..."} with HTTP 400/500
    """
    import common_lib as _cl

    if request.method == "GET":
        try:
            _cl.reload_wave_extractor_config()
            return jsonify({"config": _cl._we_config_cache})
        except Exception as exc:
            logging.error("wave_extractor/config GET error: %s", exc)
            return jsonify({"error": str(exc)}), 500

    # POST — merge incoming per-underlying sections and write atomically
    incoming: dict = request.json or {}
    if not incoming:
        return jsonify({"error": "Empty body"}), 400

    try:
        _cl.reload_wave_extractor_config()
        merged = dict(_cl._we_config_cache)
        for underlying, section in incoming.items():
            if underlying in merged:
                merged[underlying] = {**merged[underlying], **section}
            else:
                merged[underlying] = section

        success = _cl.save_wave_extractor_config(merged)
        if success:
            logging.info("wave_extractor_config updated via UI")
            return jsonify({"status": "ok"})
        return jsonify({"error": "Failed to write config — check server logs"}), 500
    except Exception as exc:
        logging.error("wave_extractor/config POST error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/wave_extractor/suggest_gaps", methods=["POST"])
def wave_extractor_suggest_gaps():
    """Suggest buy/sell gap (absolute premium points) per symbol via greeks reprice.

    For each requested symbol, looks up the per-underlying "gap_move_points"
    (up/down) from wave_extractor_config.json. If configured, reprices the
    option at spot+up/spot-down via common_lib.compute_greeks_based_gap() to
    derive buy_gap/sell_gap. If not configured for that underlying, the
    result is marked "fallback" so the frontend keeps its existing 25%
    smart-gap behavior for that symbol. The option's live LTP is always
    included (when the quote fetch succeeds) so the frontend can also use
    this endpoint to drive the percent-based gap mechanism.

    Request JSON:
        symbols (list[str]): option trading symbols, e.g. ["NIFTY25JUL25000CE"].

    Returns:
        JSON {"results": {symbol: {"buy_gap": float, "sell_gap": float, "source": "greeks", "ltp": float}
                           | {"source": "fallback", "ltp": float, "reason": str}
                           | {"source": "error", "reason": str}}}
    """
    import common_lib as _cl

    data = request.json or {}
    symbols = data.get("symbols", [])
    if not symbols:
        return jsonify({"error": "'symbols' must be a non-empty list"}), 400

    if "access_token" not in session:
        return jsonify({"error": "Session expired. Please authenticate first."}), 401

    kite = get_kite_client()
    _cl.reload_wave_extractor_config()

    symbols_by_underlying: dict[str, list[str]] = {}
    for symbol in symbols:
        underlying = _cl._get_underlying_name(symbol)
        symbols_by_underlying.setdefault(underlying, []).append(symbol)

    results: dict = {}
    for underlying, underlying_symbols in symbols_by_underlying.items():
        move_points = _cl._we_config_cache.get(underlying, {}).get("gap_move_points")
        exchange_prefix = "BFO:" if underlying == "SENSEX" else "NFO:"

        try:
            option_quotes = kite.quote([exchange_prefix + s for s in underlying_symbols])
        except Exception as exc:
            logging.error("suggest_gaps: option quote fetch failed for %s: %s", underlying, exc)
            for symbol in underlying_symbols:
                results[symbol] = {"source": "error", "reason": f"quote fetch failed: {exc}"}
            continue

        if not move_points:
            for symbol in underlying_symbols:
                option_ltp = option_quotes.get(exchange_prefix + symbol, {}).get("last_price", 0)
                results[symbol] = {
                    "source": "fallback",
                    "ltp": option_ltp,
                    "reason": f"no gap_move_points configured for {underlying}",
                }
            continue

        up_points = float(move_points.get("up", 0))
        down_points = float(move_points.get("down", 0))
        spot_symbol = SPOT_SYMBOL_MAP.get(underlying)

        try:
            spot_quote = kite.quote(spot_symbol)
            spot_price = spot_quote[spot_symbol]["last_price"]
        except Exception as exc:
            logging.error("suggest_gaps: spot quote fetch failed for %s: %s", underlying, exc)
            for symbol in underlying_symbols:
                results[symbol] = {"source": "error", "reason": f"spot quote fetch failed: {exc}"}
            continue

        for symbol in underlying_symbols:
            option_ltp = option_quotes.get(exchange_prefix + symbol, {}).get("last_price", 0)
            try:
                gaps = _cl.compute_greeks_based_gap(symbol, spot_price, option_ltp, up_points, down_points)
                results[symbol] = {**gaps, "source": "greeks", "ltp": option_ltp}
            except Exception as exc:
                logging.error("suggest_gaps: greeks calc failed for %s: %s", symbol, exc)
                results[symbol] = {"source": "error", "reason": str(exc)}

    return jsonify({"results": results})


# ============================================================================
# Survivor Algo Dashboard Endpoints
# ============================================================================

SURVIVOR_STATUS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "survivor_status")
SURVIVOR_LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "survivor_logs")

if not os.path.exists(SURVIVOR_STATUS_DIR):
    os.makedirs(SURVIVOR_STATUS_DIR)
if not os.path.exists(SURVIVOR_LOG_DIR):
    os.makedirs(SURVIVOR_LOG_DIR)


@app.route("/survivor")
def survivor_dashboard():
    """Render the Survivor Algo Dashboard page."""
    return render_template("survivor_dashboard.html")


@app.route("/api/survivor/order_history")
def get_survivor_order_history():
    """
    Get today's executed order history for Survivor algo only.
    
    Returns:
        JSON list of Survivor orders sorted by time (newest first).
    """
    try:
        from common_lib import load_survivor_orders
        from instrument_cache import get_instrument as _get_instrument
        orders = load_survivor_orders()
        for order in orders:
            if not order.get("instrument_token"):
                try:
                    instr = _get_instrument(order.get("symbol", ""))
                    if instr and instr.get("instrument_token"):
                        order["instrument_token"] = str(instr["instrument_token"])
                except Exception:
                    pass
            if not order.get("segment"):
                sym = order.get("symbol", "")
                order["segment"] = "BFO-OPT" if sym.startswith("SENSEX") else "NFO-OPT"
        return jsonify({"orders": orders})
    except Exception as e:
        logging.error(f"Error fetching Survivor order history: {e}")
        return jsonify({"error": str(e), "orders": []}), 500


@app.route("/api/survivor/spot_prices")
def get_spot_prices():
    """Get current spot prices for all tracked instruments.

    Survivor instances write their latest price to
    survivor_status/spot_prices.json on every tick. This endpoint
    returns all available prices dynamically (NIFTY, SENSEX,
    and any NFO stock like TCS, RELIANCE, etc.).

    Returns:
        JSON with all available price keys from spot_prices.json.
    """
    try:
        spot_file = os.path.join(SURVIVOR_STATUS_DIR, "spot_prices.json")
        if os.path.exists(spot_file):
            with open(spot_file, 'r') as f:
                data = json.load(f)
            return jsonify(data)
        else:
            return jsonify({
                "nifty_price": None,
                "sensex_price": None,
                "error": "No spot price data yet"
            })
    except Exception as e:
        logging.error(f"Error fetching spot prices: {e}")
        return jsonify({"error": str(e)})


@app.route("/api/survivor/start", methods=["POST"])
@limiter.limit("30 per minute")
def start_survivor_instance():
    """
    Start a survivor algo instance.
    
    Expects JSON with: index_type, mode, symbol_initials, distance, 
    order_type, gap, reset_gap, quantity, start_points, request_token
    """
    data = request.json
    
    # Required fields
    index_type = data.get("index_type")  # NIFTY, SENSEX, or STOCK
    mode = data.get("mode")  # sell or buy
    symbol_initials = data.get("symbol_initials")
    distance = data.get("distance")  # PE:CE format
    order_type = data.get("order_type")  # MIS or NRML
    gap = data.get("gap")  # PE:CE format
    reset_gap = data.get("reset_gap")  # PE:CE format
    quantity = data.get("quantity")  # PE:CE format
    start_points = data.get("start_points")  # PE:CE format
    request_token = get_token_for_script(data.get("request_token"))
    stock_name = data.get("stock_name")  # Required when index_type is STOCK
    enable_delta_rebalancing = bool(data.get("enable_delta_rebalancing", False))
    
    # Validate required fields
    if not all([index_type, mode, symbol_initials, distance, order_type, 
                gap, reset_gap, quantity, start_points, request_token]):
        return jsonify({"error": "Missing required parameters or not authenticated"}), 400
    
    # Validate index_type
    if index_type not in ["NIFTY", "SENSEX", "STOCK"]:
        return jsonify({"error": "index_type must be NIFTY, SENSEX, or STOCK"}), 400
    
    # Validate stock_name for STOCK type
    if index_type == "STOCK" and not stock_name:
        return jsonify({"error": "stock_name is required when index_type is STOCK"}), 400
    
    # Validate mode
    if mode not in ["sell", "buy"]:
        return jsonify({"error": "mode must be 'sell' or 'buy'"}), 400

    # Validate symbol_initials matches the selected index_type. This guards
    # against a stale symbol from a previously-selected index being submitted
    # (e.g. a SENSEX run launched with a leftover "NIFTY..." symbol_initials),
    # which crashes the launched script with an unhandled instrument lookup failure.
    symbol_initials_upper = symbol_initials.upper()
    if index_type == "NIFTY" and not symbol_initials_upper.startswith("NIFTY"):
        return jsonify({"error": f"symbol_initials '{symbol_initials}' does not match index_type NIFTY"}), 400
    if index_type == "SENSEX" and not symbol_initials_upper.startswith("SENSEX"):
        return jsonify({"error": f"symbol_initials '{symbol_initials}' does not match index_type SENSEX"}), 400
    if index_type == "STOCK" and not symbol_initials_upper.startswith(stock_name.upper()):
        return jsonify({"error": f"symbol_initials '{symbol_initials}' does not match stock_name {stock_name}"}), 400

    # Select script based on index_type and mode
    base_dir = os.path.dirname(os.path.abspath(__file__))
    if index_type == "NIFTY":
        if mode == "sell":
            script_name = "place_order_at_nifty.py"
        else:
            script_name = "place_order_at_nifty_with_buy_auto.py"
        exchange = "NFO"
    elif index_type == "SENSEX":
        if mode == "sell":
            script_name = "place_order_at_sensex.py"
        else:
            script_name = "place_order_at_sensex_with_buy_auto.py"
        exchange = "BFO"
    else:  # STOCK
        if mode == "sell":
            script_name = "place_order_at_stock.py"
        else:
            script_name = "place_order_at_stock_with_buy_auto.py"
        exchange = "NFO"
    
    script_path = os.path.join(base_dir, script_name)
    
    # Build exchange string with order type
    exchange_arg = f"{exchange}:{order_type}" if order_type == "MIS" else exchange
    
    # Build command
    if index_type == "STOCK":
        # Stock scripts take stock_name as first arg
        # Format: python3 script.py <STOCK_NAME> <SYMBOL> <DISTANCE> <EXCHANGE> <GAP> <RESET_GAP> <QTY> <START_POINTS> <TOKEN>
        cmd = [
            sys.executable, script_path,
            stock_name,
            symbol_initials,
            distance,
            exchange_arg,
            gap,
            reset_gap,
            quantity,
            start_points,
            request_token
        ]
    else:
        # Index scripts (NIFTY/SENSEX) - existing format
        # Format: python3 script.py <SYMBOL> <DISTANCE> <EXCHANGE> <GAP> <RESET_GAP> <QTY> <START_POINTS> <TOKEN>
        cmd = [
            sys.executable, script_path,
            symbol_initials,
            distance,
            exchange_arg,
            gap,
            reset_gap,
            quantity,
            start_points,
            request_token
        ]
        if enable_delta_rebalancing:
            cmd.append("delta_rebalancing")
    
    # Create unique log file name
    timestamp = get_ist_now().strftime("%Y%m%d_%H%M%S")
    label = stock_name if index_type == "STOCK" else index_type
    log_filename = f"survivor_{label}_{symbol_initials}_{timestamp}.log"
    log_path = os.path.join(SURVIVOR_LOG_DIR, log_filename)
    
    # Create status file
    status_filename = f"survivor_{label}_{symbol_initials}_{timestamp}.json"
    status_path = os.path.join(SURVIVOR_STATUS_DIR, status_filename)
    
    try:
        with open(log_path, "w") as log_file:
            process = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT, start_new_session=True)
        
        # Write initial status
        status_data = {
            "index_type": index_type,
            "mode": mode,
            "symbol_initials": symbol_initials,
            "distance": distance,
            "exchange": exchange,
            "order_type": order_type,
            "gap": gap,
            "reset_gap": reset_gap,
            "quantity": quantity,
            "start_points": start_points,
            "pid": process.pid,
            "log_file": log_filename,
            "start_time": get_ist_now().strftime("%Y-%m-%d %H:%M:%S"),
            "script": script_name,
            "enable_delta_rebalancing": enable_delta_rebalancing,
        }

        # Add stock_name to status for STOCK type
        if index_type == "STOCK":
            status_data["stock_name"] = stock_name
        
        with open(status_path, 'w') as f:
            json.dump(status_data, f, indent=2)
        
        return jsonify({
            "message": f"Started {script_name} for {symbol_initials}",
            "pid": process.pid
        })
        
    except Exception as e:
        logging.error(f"Error starting survivor instance: {e}")
        return jsonify({"error": str(e)}), 500

@app.route("/api/survivor/events/<int:pid>")
def get_survivor_events(pid: int):
    """Return the event log for one survivor instance.

    Args:
        pid: OS process ID of the survivor instance.

    Returns:
        JSON with 'events' list (newest first) and 'pid'.
    """
    try:
        import survivor_events_db
        events = survivor_events_db.get_events_for_pid(pid, limit=500)
        return jsonify({"events": events, "pid": pid})
    except Exception as exc:
        logging.error(f"Error fetching survivor events for PID {pid}: {exc}")
        return jsonify({"error": str(exc), "events": []}), 500


@app.route("/api/survivor/stock_config")
def get_stock_config():
    """Get lot size and strike gap for a stock from instruments.

    Auto-detects configuration by scanning Kite instruments data.

    Query params:
        stock_name: Stock name (e.g., 'TCS', 'RELIANCE')

    Returns:
        JSON with lot_size, strike_gap, spot_symbol.
    """
    stock_name = request.args.get("stock_name", "").upper().strip()
    if not stock_name:
        return jsonify({"error": "stock_name query parameter required"}), 400

    try:
        if "access_token" not in session:
            return jsonify({
                "lot_size": 1,
                "strike_gap": 50,
                "spot_symbol": f"NSE:{stock_name}",
                "warning": "No session - using defaults. Authenticate first."
            })

        kite_client = get_kite_client()

        # Scan NFO instruments for this stock
        instruments = kite_client.instruments("NFO")

        lot_size = 1
        strikes = set()

        for inst in instruments:
            if inst["name"] == stock_name and inst["instrument_type"] in ("CE", "PE"):
                if inst.get("lot_size"):
                    lot_size = int(inst["lot_size"])
                strike = inst.get("strike")
                if strike is not None and float(strike) > 0:
                    strikes.add(float(strike))

        # Calculate smallest strike gap
        strike_gap = 50.0  # default
        if len(strikes) >= 2:
            sorted_strikes = sorted(strikes)
            min_gap = float('inf')
            for i in range(1, len(sorted_strikes)):
                diff = sorted_strikes[i] - sorted_strikes[i - 1]
                if 0 < diff < min_gap:
                    min_gap = diff
            strike_gap = min_gap

        return jsonify({
            "lot_size": lot_size,
            "strike_gap": strike_gap,
            "spot_symbol": f"NSE:{stock_name}"
        })

    except Exception as e:
        logging.error(f"Error fetching stock config for {stock_name}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/survivor/symbol_expiries")
def get_symbol_expiries():
    """Get available symbol prefixes for an instrument, sorted by nearest expiry.

    Scans Kite instruments for the given name, extracts unique symbol
    prefixes (the part before strike price + CE/PE), and returns them
    sorted with the nearest expiry first.

    Query params:
        name: Instrument name (e.g., 'NIFTY', 'SENSEX', 'TCS', 'RELIANCE')
        exchange: Exchange to search ('NFO' or 'BFO'). Default: 'NFO'

    Returns:
        JSON with 'expiries' list of {prefix, expiry_date, days_to_expiry}.
    """
    import re
    from datetime import date

    name = request.args.get("name", "").upper().strip()
    exchange = request.args.get("exchange", "NFO").upper().strip()

    if not name:
        return jsonify({"error": "name query parameter required"}), 400

    try:
        if "access_token" not in session:
            return jsonify({
                "expiries": [],
                "warning": "No session - authenticate first."
            })

        kite_client = get_kite_client()

        instruments = kite_client.instruments(exchange)

        # Collect unique (prefix, expiry) pairs
        # prefix = tradingsymbol with strike digits and CE/PE/FUT stripped
        seen_prefixes = {}  # prefix -> expiry_date
        today = date.today()

        for inst in instruments:
            if inst.get("name") != name:
                continue
            if inst.get("instrument_type") not in ("CE", "PE"):
                continue

            symbol = inst.get("tradingsymbol", "")
            expiry = inst.get("expiry")
            if not symbol or not expiry:
                continue

            # Extract prefix by removing trailing strike digits + CE/PE
            # e.g., "NIFTY26FEB23000CE" -> "NIFTY26FEB"
            # e.g., "SENSEX2620583200PE" -> "SENSEX26205"
            # e.g., "TCS26FEB3800CE" -> "TCS26FEB"
            prefix_match = re.match(
                r'^([A-Z]+\d{2}[A-Z]{3}|[A-Z]+\d{5})', symbol
            )
            if prefix_match:
                prefix = prefix_match.group(1)
                if prefix not in seen_prefixes:
                    seen_prefixes[prefix] = expiry

        # Build result sorted by expiry date (nearest first)
        result = []
        for prefix, expiry_date in sorted(seen_prefixes.items(), key=lambda x: x[1]):
            days = (expiry_date - today).days
            result.append({
                "prefix": prefix,
                "expiry_date": str(expiry_date),
                "days_to_expiry": days
            })

        return jsonify({"expiries": result})

    except Exception as e:
        logging.error(f"Error fetching symbol expiries for {name}: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/survivor/status")
def get_survivor_status():
    """Get status of all survivor algo instances."""
    status_list = []

    if os.path.exists(SURVIVOR_STATUS_DIR):
        files = glob.glob(os.path.join(SURVIVOR_STATUS_DIR, "*.json"))
        for file_path in files:
            basename = os.path.basename(file_path)
            # Skip shared files
            if basename in ("spot_prices.json",) or basename.startswith("survivor_state_"):
                continue
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)

                # Check if process is still running
                pid = data.get("pid")
                is_running = False
                if pid:
                    try:
                        os.kill(pid, 0)
                        is_running = True
                    except OSError:
                        is_running = False

                data["is_running"] = is_running
                data["status_file"] = basename

                # Load current PE/CE tracking values from state file
                state_path = os.path.join(SURVIVOR_STATUS_DIR, f"survivor_state_{pid}.json")
                if pid and os.path.exists(state_path):
                    try:
                        with open(state_path, 'r') as sf:
                            state = json.load(sf)
                        data["current_pe"] = state.get("pe_value")
                        data["current_ce"] = state.get("ce_value")
                        data["current_points_updated"] = state.get("updated")
                    except (OSError, ValueError) as e:
                        logging.error(f"Error reading survivor state file {state_path}: {e}")
                        data["current_pe"] = None
                        data["current_ce"] = None
                else:
                    # Fall back to original start_points until first update
                    sp = data.get("start_points", "0:0").split(":")
                    data["current_pe"] = float(sp[0]) if sp[0] not in ("0", "") else None
                    data["current_ce"] = float(sp[1]) if len(sp) > 1 and sp[1] not in ("0", "") else None

                status_list.append(data)
            except Exception as e:
                logging.error(f"Error reading survivor status file {file_path}: {e}")

    return jsonify(status_list)


@app.route("/api/survivor/stop", methods=["POST"])
@limiter.limit("120 per minute")
def stop_survivor_instance():
    """Stop a running survivor instance."""
    data = request.json
    pid = data.get("pid")
    status_file = data.get("status_file")
    
    if not pid:
        return jsonify({"error": "PID required"}), 400
    
    try:
        try:
            os.kill(int(pid), signal.SIGTERM)
        except ProcessLookupError:
            pass  # Process already dead
        
        # Clean up status file and state file
        if status_file:
            status_path = os.path.join(SURVIVOR_STATUS_DIR, status_file)
            if os.path.exists(status_path):
                os.remove(status_path)
        state_path = os.path.join(SURVIVOR_STATUS_DIR, f"survivor_state_{pid}.json")
        if os.path.exists(state_path):
            os.remove(state_path)

        return jsonify({"message": f"Stopped process {pid}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/survivor/delete", methods=["POST"])
def delete_survivor_instance():
    """
    Delete a stopped survivor instance's status file.
    
    This removes the instance from the dashboard.
    Only works for stopped instances.
    """
    data = request.json
    status_file = data.get("status_file")
    
    if not status_file:
        return jsonify({"error": "status_file required"}), 400
    
    try:
        status_path = os.path.join(SURVIVOR_STATUS_DIR, status_file)
        
        if not os.path.exists(status_path):
            return jsonify({"message": "Instance already deleted"})
        
        # Verify instance is stopped before deleting
        with open(status_path, 'r') as f:
            status_data = json.load(f)
        
        pid = status_data.get("pid")
        is_running = False
        if pid:
            try:
                os.kill(pid, 0)  # Check if process exists
                is_running = True
            except (ProcessLookupError, PermissionError):
                is_running = False
        
        if is_running:
            return jsonify({"error": "Cannot delete a running instance. Stop it first."}), 400
        
        os.remove(status_path)
        # Clean up state file if present
        state_path = os.path.join(SURVIVOR_STATUS_DIR, f"survivor_state_{pid}.json")
        if pid and os.path.exists(state_path):
            os.remove(state_path)
        return jsonify({"message": f"Deleted instance {status_file}"})
        
    except Exception as e:
        logging.error(f"Error deleting survivor instance: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/survivor/delete_all_stopped", methods=["POST"])
@limiter.limit("30 per minute")
def delete_all_stopped_survivor_instances():
    """Delete every stopped (non-running) survivor instance's status file.

    Running instances are left untouched. Mirrors the per-instance
    behavior of ``delete_survivor_instance`` but applied in bulk so the
    dashboard can be cleared of finished test runs in one click.

    Returns:
        JSON response with the count of deleted instances and the count
        of instances skipped because they were still running.
    """
    deleted_count = 0
    skipped_running_count = 0

    if not os.path.exists(SURVIVOR_STATUS_DIR):
        return jsonify({"message": "No instances found", "deleted": 0})

    files = glob.glob(os.path.join(SURVIVOR_STATUS_DIR, "*.json"))
    for file_path in files:
        basename = os.path.basename(file_path)
        if basename in ("spot_prices.json",) or basename.startswith("survivor_state_"):
            continue

        try:
            with open(file_path, 'r') as f:
                status_data = json.load(f)
        except (OSError, ValueError) as e:
            logging.error(f"Error reading survivor status file {file_path}: {e}")
            continue

        pid = status_data.get("pid")
        is_running = False
        if pid:
            try:
                os.kill(pid, 0)
                is_running = True
            except (ProcessLookupError, PermissionError):
                is_running = False

        if is_running:
            skipped_running_count += 1
            continue

        try:
            os.remove(file_path)
            state_path = os.path.join(SURVIVOR_STATUS_DIR, f"survivor_state_{pid}.json")
            if pid and os.path.exists(state_path):
                os.remove(state_path)
            deleted_count += 1
        except OSError as e:
            logging.error(f"Error deleting survivor instance file {file_path}: {e}")

    logging.info(
        f"Deleted {deleted_count} stopped survivor instances "
        f"({skipped_running_count} running instances skipped)"
    )
    return jsonify({
        "message": f"Deleted {deleted_count} stopped instance(s)"
        + (f", {skipped_running_count} running instance(s) left untouched" if skipped_running_count else ""),
        "deleted": deleted_count,
        "skipped_running": skipped_running_count,
    })


@app.route("/api/survivor/logs/<filename>")
def get_survivor_logs(filename):
    """Get logs for a survivor instance."""
    log_path = os.path.join(SURVIVOR_LOG_DIR, filename)
    
    if os.path.exists(log_path):
        try:
            with open(log_path, 'r') as f:
                content = f.read()
            return jsonify({"content": content})
        except Exception as e:
            return jsonify({"error": str(e)}), 500
    return jsonify({"content": "No log file found."})


# ============================================================================
# NIFTY Positions Dashboard Endpoints
# ============================================================================

@app.route("/positions")
def positions_dashboard():
    """Render the NIFTY Positions Dashboard page."""
    return render_template("positions_dashboard.html")


@app.route("/api/nifty_positions", methods=["POST"])
def get_nifty_positions():
    """
    Get overall NIFTY positions with delta and margin.
    
    Expects JSON with optional request_token.
    Returns position summary including puts/calls sold, delta, margin.
    """
    data = request.json or {}
    request_token = data.get("request_token")
    force_refresh: bool = bool(data.get("force_refresh", False))

    if not request_token and "access_token" not in session:
        return jsonify({"error": "Authentication required. Please connect Kite in the header."}), 401

    if force_refresh:
        invalidate_positions_cache()

    try:
        base_kite = get_authenticated_kite_client(data)
        kite = MonitoredKite(base_kite, account_id="main")
        from positions_lib import get_nifty_positions_summary
        min_premium = float(data.get("min_premium", 0) or 0)
        interest_rate = float(config["option_details"].get("interest_rate", 10.0))
        fallback_vol = float(config["option_details"].get("current_volatility", 12.0))
        summary = get_nifty_positions_summary(
            kite, min_premium=min_premium,
            interest_rate=interest_rate, volatility=fallback_vol,
        )

        return jsonify(summary)

    except Exception as e:
        logging.error(f"Error fetching NIFTY positions: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/nifty_positions/next_expiry", methods=["POST"])
def get_nifty_next_expiry_positions():
    """
    Get NIFTY positions filtered to next expiry only.
    
    Expects JSON with optional request_token.
    Returns position summary for next expiry.
    """
    data = request.json or {}
    request_token = data.get("request_token")
    force_refresh: bool = bool(data.get("force_refresh", False))

    if not request_token and "access_token" not in session:
        return jsonify({"error": "Authentication required. Please connect Kite in the header."}), 401

    if force_refresh:
        invalidate_positions_cache()

    try:
        base_kite = get_authenticated_kite_client(data)
        kite = MonitoredKite(base_kite, account_id="main")
        from positions_lib import get_nifty_positions_summary
        min_premium = float(data.get("min_premium", 0) or 0)
        interest_rate = float(config["option_details"].get("interest_rate", 10.0))
        fallback_vol = float(config["option_details"].get("current_volatility", 12.0))
        summary = get_nifty_positions_summary(
            kite, restrict_to_next_expiry=True, min_premium=min_premium,
            interest_rate=interest_rate, volatility=fallback_vol,
        )

        return jsonify(summary)

    except Exception as e:
        logging.error(f"Error fetching next expiry positions: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/nifty_positions/custom_days", methods=["POST"])
def get_nifty_custom_days_positions():
    """
    Get NIFTY positions filtered by trading days.
    
    Expects JSON with:
        - request_token: optional, uses session if available
        - days: int, number of trading days to include
    
    Returns position summary for positions expiring within specified days.
    """
    data = request.json or {}
    request_token = data.get("request_token")
    days = data.get("days")
    force_refresh: bool = bool(data.get("force_refresh", False))

    if not days or not isinstance(days, int) or days < 1:
        return jsonify({"error": "Valid 'days' parameter required (positive integer)"}), 400

    try:
        base_kite = get_authenticated_kite_client(data)
        kite = MonitoredKite(base_kite, account_id="main")
        from positions_lib import get_nifty_positions_summary
        min_premium = float(data.get("min_premium", 0) or 0)
        interest_rate = float(config["option_details"].get("interest_rate", 10.0))
        fallback_vol = float(config["option_details"].get("current_volatility", 12.0))
        summary = get_nifty_positions_summary(
            kite, days=days, min_premium=min_premium,
            interest_rate=interest_rate, volatility=fallback_vol,
        )

        return jsonify(summary)

    except Exception as e:
        logging.error(f"Error fetching custom days positions: {e}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# SENSEX Positions Dashboard Endpoints
# ============================================================================

@app.route("/sensex_positions")
def positions_dashboard_sensex():
    """Render the SENSEX Positions Dashboard page."""
    return render_template("sensex_positions_dashboard.html")


@app.route("/api/sensex_positions", methods=["POST"])
def get_sensex_positions():
    """
    Get overall SENSEX positions with delta and margin.

    Expects JSON with optional request_token and force_refresh flag.
    Returns position summary including puts/calls sold, delta, margin.
    """
    data = request.json or {}
    request_token = data.get("request_token")
    force_refresh: bool = bool(data.get("force_refresh", False))

    if not request_token and "access_token" not in session:
        return jsonify({"error": "Authentication required. Please connect Kite in the header."}), 401

    if force_refresh:
        invalidate_positions_cache()
        with _sensex_positions_cache_lock:
            _sensex_positions_cache["overall"]["expires_at"] = 0.0

    with _sensex_positions_cache_lock:
        cached = _sensex_positions_cache["overall"]
        if not force_refresh and cached["data"] is not None and time.monotonic() < cached["expires_at"]:
            return jsonify(cached["data"])

    try:
        base_kite = get_authenticated_kite_client(data)
        kite = MonitoredKite(base_kite, account_id="main")
        from sensex_positions_lib import get_sensex_positions_summary
        interest_rate = float(config["option_details"].get("interest_rate", 10.0))
        fallback_vol = float(config["option_details"].get("current_volatility", 12.0))
        summary = get_sensex_positions_summary(
            kite, interest_rate=interest_rate, volatility=fallback_vol,
        )
        with _sensex_positions_cache_lock:
            _sensex_positions_cache["overall"] = {
                "data": summary,
                "expires_at": time.monotonic() + _SENSEX_POSITIONS_CACHE_TTL_SECONDS,
            }
        return jsonify(summary)

    except Exception as e:
        logging.error(f"Error fetching SENSEX positions: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/sensex_positions/next_expiry", methods=["POST"])
def get_sensex_next_expiry_positions():
    """
    Get SENSEX positions filtered to next expiry only.

    Expects JSON with optional request_token.
    Returns position summary for next expiry.
    """
    data = request.json or {}
    request_token = data.get("request_token")
    force_refresh: bool = bool(data.get("force_refresh", False))

    if not request_token and "access_token" not in session:
        return jsonify({"error": "Authentication required. Please connect Kite in the header."}), 401

    if force_refresh:
        invalidate_positions_cache()
        with _sensex_positions_cache_lock:
            _sensex_positions_cache["next_expiry"]["expires_at"] = 0.0

    with _sensex_positions_cache_lock:
        cached = _sensex_positions_cache["next_expiry"]
        if not force_refresh and cached["data"] is not None and time.monotonic() < cached["expires_at"]:
            return jsonify(cached["data"])

    try:
        base_kite = get_authenticated_kite_client(data)
        kite = MonitoredKite(base_kite, account_id="main")
        from sensex_positions_lib import get_sensex_positions_summary
        interest_rate = float(config["option_details"].get("interest_rate", 10.0))
        fallback_vol = float(config["option_details"].get("current_volatility", 12.0))
        summary = get_sensex_positions_summary(
            kite, restrict_to_next_expiry=True,
            interest_rate=interest_rate, volatility=fallback_vol,
        )
        with _sensex_positions_cache_lock:
            _sensex_positions_cache["next_expiry"] = {
                "data": summary,
                "expires_at": time.monotonic() + _SENSEX_POSITIONS_CACHE_TTL_SECONDS,
            }
        return jsonify(summary)

    except Exception as e:
        logging.error(f"Error fetching SENSEX next expiry positions: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500


@app.route("/api/sensex_positions/custom_days", methods=["POST"])
def get_sensex_custom_days_positions():
    """
    Get SENSEX positions filtered by trading days.

    Expects JSON with:
        - request_token: optional, uses session if available
        - days: int, number of trading days to include

    Returns position summary for positions expiring within specified days.
    """
    data = request.json or {}
    days = data.get("days")

    if not days or not isinstance(days, int) or days < 1:
        return jsonify({"error": "Valid 'days' parameter required (positive integer)"}), 400

    try:
        base_kite = get_authenticated_kite_client(data)
        kite = MonitoredKite(base_kite, account_id="main")
        from sensex_positions_lib import get_sensex_positions_summary
        interest_rate = float(config["option_details"].get("interest_rate", 10.0))
        fallback_vol = float(config["option_details"].get("current_volatility", 12.0))
        summary = get_sensex_positions_summary(
            kite, days=days,
            interest_rate=interest_rate, volatility=fallback_vol,
        )
        return jsonify(summary)

    except Exception as e:
        logging.error(f"Error fetching SENSEX custom days positions: {e}", exc_info=True)
        return jsonify({"error": str(e)})


# ============================================================================
# Expiry Trade Dashboard Endpoints
# ============================================================================

# Global Expiry Trade System instance
expiry_trade_system_instance = None


def get_expiry_trade_system():
    """
    Get or create the Expiry Trade System singleton instance.

    Returns:
        ExpiryTradeSystem: The singleton instance, or None on failure.
    """
    global expiry_trade_system_instance
    if expiry_trade_system_instance is None:
        try:
            from expiry_trade_lib import ExpiryTradeSystem
            kite = get_kite_client()
            expiry_trade_system_instance = ExpiryTradeSystem(kite=kite)
            logging.info("Expiry Trade System initialized")
        except Exception as e:
            logging.error(f"Failed to initialize Expiry Trade System: {e}")
            return None
    return expiry_trade_system_instance


@app.route("/expiry_trade")
def expiry_trade_dashboard():
    """Render the Expiry Trade Dashboard page."""
    return render_template("expiry_trade_dashboard.html")


@app.route("/api/expiry_trade/start", methods=["POST"])
@limiter.limit("30 per minute")
def start_expiry_trade():
    """
    Start the Expiry Trade system.

    Expects JSON with optional request_token for authentication.
    Detects today's expiry, starts ticker, and begins data collection.

    Returns:
        JSON with success status, message, active_index, and
        expiry_substring.
    """
    global expiry_trade_system_instance

    try:
        data = request.json or {}
        request_token = data.get("request_token")

        # Authenticate
        base_kite = KiteConnect(api_key=kite_api_key)

        if not request_token and "access_token" in session:
            base_kite.set_access_token(session["access_token"])
        elif request_token:
            try:
                auth_data = base_kite.generate_session(
                    request_token, api_secret=kite_api_secret
                )
                base_kite.set_access_token(auth_data["access_token"])
                _set_kite_session(auth_data["access_token"])
            except Exception as e:
                if "access_token" in session:
                    base_kite.set_access_token(session["access_token"])
                    logging.warning(f"Using existing session: {e}")
                else:
                    return jsonify({"success": False, "message": str(e)}), 400
        else:
            return jsonify({
                "success": False,
                "message": "Authentication required. Please connect Kite in the header."
            }), 401

        kite = MonitoredKite(base_kite, account_id="main")

        # Create / re-create system with fresh kite client
        from expiry_trade_lib import ExpiryTradeSystem
        expiry_trade_system_instance = ExpiryTradeSystem(kite=kite)
        result = expiry_trade_system_instance.start()

        return jsonify(result)

    except Exception as e:
        logging.error(f"Error starting Expiry Trade system: {e}")
        return jsonify({"success": False, "message": str(e)}), 500


@app.route("/api/expiry_trade/status")
def get_expiry_trade_status():
    """
    Get current Expiry Trade system status.

    Returns:
        JSON with is_active, is_stuck, active_index,
        expiry_substring, last_tick_time.
    """
    system = get_expiry_trade_system()
    if system is None:
        return jsonify({
            "is_active": False,
            "is_stuck": False,
            "active_index": None,
            "expiry_substring": None,
            "last_tick_time": None,
        })
    return jsonify(system.get_status())


@app.route("/api/expiry_trade/candles")
def get_expiry_trade_candles():
    """
    Get 3-minute OHLC candle data for the active index.

    Returns:
        JSON with candles list.
    """
    system = get_expiry_trade_system()
    if system is None:
        return jsonify({"candles": []})
    return jsonify({"candles": system.get_candles()})


@app.route("/api/expiry_trade/stoch_rsi")
def get_expiry_trade_stoch_rsi():
    """
    Get Stochastic RSI data for the active index.

    Returns:
        JSON with stoch_rsi list of {time, k, d} dicts.
    """
    system = get_expiry_trade_system()
    if system is None:
        return jsonify({"stoch_rsi": []})
    return jsonify({"stoch_rsi": system.get_stoch_rsi()})


@app.route("/api/expiry_trade/support_resistance")
def get_expiry_trade_support_resistance():
    """
    Get 1-hour support/resistance levels for the active index.

    Returns:
        JSON with support_resistance list of {time, support,
        resistance} dicts.
    """
    system = get_expiry_trade_system()
    if system is None:
        return jsonify({"support_resistance": []})
    return jsonify({"support_resistance": system.get_support_resistance()})


@app.route("/nifty_contributors")
def nifty_contributors_dashboard():
    """Render the NIFTY 50 stock contributors dashboard page.

    Returns:
        Rendered HTML page.
    """
    return render_template("nifty_contributors.html")


# ============================================================================
# Trade Journal Dashboard Endpoints
# ============================================================================


@app.route("/trade-journal")
def trade_journal_dashboard():
    """Render the Trade Journal Dashboard page."""
    return render_template("trade_journal.html")


@app.route("/tradebook-analysis", methods=["GET", "POST"])
def tradebook_analysis():
    """Render the Tradebook Analysis page and process uploads."""
    analysis = None
    if request.method == "POST":
        if 'file' not in request.files:
            return "No file part", 400
        file = request.files['file']
        if file.filename == '':
            return "No selected file", 400
        if file:
            from tradebook_analyzer import parse_tradebook_csv, calculate_pnl_and_margin
            
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
                file.save(tmp.name)
                tmp_path = tmp.name
            
            try:
                trades = parse_tradebook_csv(tmp_path)
                analysis = calculate_pnl_and_margin(trades)
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                    
    return render_template("tradebook_analysis.html", analysis=analysis)


@app.route("/api/trade_journal/available_dates")
def trade_journal_available_dates():
    """Get dates that have executed order data.

    Returns:
        JSON with 'dates' list of YYYY-MM-DD strings.
    """
    try:
        from trade_journal import get_available_dates
        dates = get_available_dates()
        return jsonify({"dates": dates})
    except Exception as e:
        logging.error(f"Error fetching available dates: {e}")
        return jsonify({"error": str(e), "dates": []}), 500


@app.route("/api/trade_journal/summary")
def trade_journal_summary():
    """Get aggregated P&L summary for a date range.

    Query params:
        start: Start date (YYYY-MM-DD). Default: 30 days ago.
        end: End date (YYYY-MM-DD). Default: today.

    Returns:
        JSON with total_pnl, algo_pnl, manual_pnl, by_algo,
        by_category, cumulative_pnl, insights.
    """
    try:
        from trade_journal import get_journal_summary

        start_str = request.args.get("start")
        end_str = request.args.get("end")

        if start_str:
            start = date.fromisoformat(start_str)
        else:
            start = date.today() - timedelta(days=30)

        if end_str:
            end = date.fromisoformat(end_str)
        else:
            end = date.today()

        summary = get_journal_summary(start, end)
        return jsonify(summary)
    except Exception as e:
        logging.error(f"Error fetching trade journal summary: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/trade_journal/trades")
def trade_journal_trades():
    """Get paginated round trips for a date range, sorted newest first.

    Query params:
        start: Start date (YYYY-MM-DD). Default: 30 days ago.
        end: End date (YYYY-MM-DD). Default: today.
        page: Page number (1-indexed). Default: 1.
        per_page: Items per page (1–100). Default: 25.
        attribution: Filter by attribution label, or "all". Default: "all".
        direction: Filter by direction ("sell_first"/"buy_first"), or "all". Default: "all".
        result: Filter by result ("profit"/"loss"), or "all". Default: "all".
        symbol: Case-insensitive substring match on trading symbol. Default: "" (no filter).
        trade_type: Filter by trade type ("expiry"/"regular"), or "all". Default: "all".

    Returns:
        JSON with round_trips (current page), total, page, per_page, total_pages, unpaired.
    """
    try:
        from trade_journal import get_trade_log

        start_str = request.args.get("start")
        end_str = request.args.get("end")

        if start_str:
            start = date.fromisoformat(start_str)
        else:
            start = date.today() - timedelta(days=30)

        if end_str:
            end = date.fromisoformat(end_str)
        else:
            end = date.today()

        page = max(1, int(request.args.get("page", 1)))
        per_page = min(100, max(1, int(request.args.get("per_page", 25))))
        attribution_filter = request.args.get("attribution", "all")
        direction_filter = request.args.get("direction", "all")
        result_filter = request.args.get("result", "all")
        symbol_filter = request.args.get("symbol", "").strip()
        trade_type_filter = request.args.get("trade_type", "all")

        trade_log = get_trade_log(start, end)
        round_trips: list = trade_log["round_trips"]

        if attribution_filter != "all":
            round_trips = [
                rt for rt in round_trips
                if rt.get("attribution", {}).get("attribution_label") == attribution_filter
            ]
        if direction_filter != "all":
            round_trips = [rt for rt in round_trips if rt.get("direction") == direction_filter]
        if result_filter == "profit":
            round_trips = [rt for rt in round_trips if rt.get("pnl", 0) >= 0]
        elif result_filter == "loss":
            round_trips = [rt for rt in round_trips if rt.get("pnl", 0) < 0]
        if symbol_filter:
            symbol_lower = symbol_filter.lower()
            round_trips = [
                rt for rt in round_trips
                if symbol_lower in rt.get("symbol", "").lower()
            ]
        if trade_type_filter == "expiry":
            round_trips = [rt for rt in round_trips if rt.get("is_expiry_close", False)]
        elif trade_type_filter == "regular":
            round_trips = [rt for rt in round_trips if not rt.get("is_expiry_close", False)]

        total = len(round_trips)
        total_pages = max(1, (total + per_page - 1) // per_page)
        page = min(page, total_pages)
        offset = (page - 1) * per_page

        return jsonify({
            "round_trips": round_trips[offset:offset + per_page],
            "unpaired": trade_log["unpaired"],
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
        })
    except Exception as e:
        logging.error(f"Error fetching trade journal trades: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/trade_journal/orders")
def trade_journal_orders():
    """Get individual executed orders for a date range, enriched with attribution.

    Query params:
        start: Start date (YYYY-MM-DD). Default: 30 days ago.
        end: End date (YYYY-MM-DD). Default: today.

    Returns:
        JSON with orders list sorted by timestamp DESC.
    """
    try:
        from trade_journal import load_orders_for_date, classify_source

        start_str = request.args.get("start")
        end_str = request.args.get("end")

        start = date.fromisoformat(start_str) if start_str else date.today() - timedelta(days=30)
        end = date.fromisoformat(end_str) if end_str else date.today()

        all_orders: list = []
        current = start
        while current <= end:
            day_orders = load_orders_for_date(current)
            for order in day_orders:
                algo_info = classify_source(order.get("algo_source", ""))
                all_orders.append({
                    **order,
                    "attribution": algo_info,
                })
            current += timedelta(days=1)

        all_orders.sort(key=lambda o: o.get("timestamp", ""), reverse=True)

        return jsonify({"orders": all_orders, "total": len(all_orders)})
    except Exception as exc:
        logging.error(f"Error fetching trade journal orders: {exc}")
        return jsonify({"error": str(exc)}), 500


@app.route("/api/trade_journal/notes", methods=["GET"])
def trade_journal_get_notes():
    """Get all trade notes.

    Returns:
        JSON with notes dict keyed by date_symbol.
    """
    try:
        from trade_journal import get_trade_notes
        notes = get_trade_notes()
        return jsonify({"notes": notes})
    except Exception as e:
        logging.error(f"Error fetching trade notes: {e}")
        return jsonify({"error": str(e), "notes": {}}), 500


@app.route("/api/trade_journal/notes", methods=["POST"])
def trade_journal_save_note():
    """Save a note for a specific trade.

    Request JSON:
        symbol: Trading symbol.
        trade_date: Date of the trade (YYYY-MM-DD).
        note: Free-text note.
        market_condition: Optional market condition tag.
        lesson: Optional lesson learned.

    Returns:
        JSON with success status.
    """
    try:
        from trade_journal import save_trade_note

        data = request.json or {}
        symbol = data.get("symbol", "")
        trade_date = data.get("trade_date", "")
        note = data.get("note", "")
        market_condition = data.get("market_condition", "")
        lesson = data.get("lesson", "")

        if not symbol or not trade_date:
            return jsonify({"error": "symbol and trade_date required"}), 400

        result = save_trade_note(
            symbol, trade_date, note, market_condition, lesson
        )
        return jsonify({"success": result})
    except Exception as e:
        logging.error(f"Error saving trade note: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/trade_journal/market_conditions")
def trade_journal_market_conditions():
    """Get available market condition tags.

    Returns:
        JSON with 'conditions' list.
    """
    from trade_journal import MARKET_CONDITIONS
    return jsonify({"conditions": MARKET_CONDITIONS})


@app.route("/api/trade_journal/reconcile", methods=["POST"])
def reconcile_trade_journal():
    """Reconcile today's journal against Zerodha's trade API.

    Fetches kite.trades() and inserts any fills that are missing from the
    local executed_orders JSON (e.g., trades placed manually on Zerodha).
    Missing trades are tagged as algo_source='Manual'.

    Returns:
        JSON with keys: added, skipped, errors.
    """
    from trade_journal import reconcile_with_zerodha
    try:
        kite_client = get_kite_client()
        result = reconcile_with_zerodha(kite_client, date.today())
        try:
            from trade_journal_db import force_rebuild
            force_rebuild()
            logging.info("Trade journal cache rebuilt after reconcile.")
        except Exception as cache_exc:
            logging.warning("Cache rebuild after reconcile failed: %s", cache_exc)
        return jsonify(result)
    except Exception as exc:
        logging.error("reconcile_trade_journal endpoint error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/trade_journal/rebuild", methods=["POST"])
def api_trade_journal_rebuild():
    """Force a full rebuild of the trade journal SQLite cache.

    Triggers FIFO re-pairing of all available order JSON files and
    re-enriches with attribution and instrument metadata.

    Returns:
        JSON with keys: round_trip_count, unpaired_count,
        source_file_count, duration_ms.
    """
    from trade_journal_db import force_rebuild
    try:
        stats = force_rebuild()
        return jsonify(stats)
    except Exception as exc:
        logging.error("Manual trade journal cache rebuild failed: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/trade_journal/position_validation", methods=["GET"])
def get_position_validation():
    """Return the latest position validation report for a given date.

    Query params:
        date (str): YYYY-MM-DD. Defaults to today.

    Returns:
        JSON validation report (validated_at, matched, discrepancies, phantoms),
        or {status: "not_run"} if no report exists for that date.
    """
    import os
    target_date_str = request.args.get("date", date.today().isoformat())
    status_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "status")
    report_path = os.path.join(status_dir, f"position_validation_{target_date_str}.json")
    if not os.path.exists(report_path):
        return jsonify({"status": "not_run", "message": f"No validation report for {target_date_str}"}), 200
    try:
        with open(report_path) as f:
            report = json.load(f)
        return jsonify(report)
    except Exception as exc:
        logging.error("get_position_validation error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/trade_journal/position_validation/run", methods=["POST"])
def run_position_validation():
    """Run position validation on demand against Zerodha live positions.

    Compares trade journal implied open positions with kite.positions() and
    saves a report to status/position_validation_<date>.json.

    Returns:
        JSON validation report.
    """
    from trade_journal import validate_positions_vs_zerodha
    from trade_journal_db import force_rebuild
    try:
        kite_client = get_kite_client()
        force_rebuild()
        report = validate_positions_vs_zerodha(kite_client, date.today())
        return jsonify(report)
    except Exception as exc:
        logging.error("run_position_validation error: %s", exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/trade_journal/inject_carry_forward", methods=["POST"])
def api_inject_carry_forward():
    """Inject a synthetic open-leg order to fix a cross-day data gap.

    Writes a synthetic 'Carry Forward' order into the specified date's
    executed_orders JSON file, then triggers a cache rebuild.

    Request JSON body:
        symbol (str): Trading symbol.
        transaction_type (str): 'BUY' or 'SELL'.
        price (float): Average fill price.
        quantity (int): Number of units.
        trade_date (str): YYYY-MM-DD — the date to inject into (usually yesterday).
        note (str, optional): Human-readable reason.

    Returns:
        JSON with keys: success, order, message.
    """
    from trade_journal import inject_carry_forward
    try:
        body = request.get_json(force=True)
        symbol = body.get("symbol", "").strip()
        transaction_type = body.get("transaction_type", "").strip().upper()
        price = float(body.get("price", 0))
        quantity = int(body.get("quantity", 0))
        trade_date_str = body.get("trade_date", "")
        note = body.get("note", "")

        if not symbol or not transaction_type or not trade_date_str:
            return jsonify({"error": "symbol, transaction_type, and trade_date are required"}), 400

        trade_date_obj = date.fromisoformat(trade_date_str)
        result = inject_carry_forward(
            symbol=symbol,
            transaction_type=transaction_type,
            price=price,
            quantity=quantity,
            trade_date=trade_date_obj,
            note=note,
        )
        # Rebuild is intentionally NOT triggered here — callers typically do
        # multiple injections in a batch and should trigger rebuild manually
        # afterward via POST /api/trade_journal/rebuild.
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logging.error("api_inject_carry_forward error: %s", exc)
        return jsonify({"error": str(exc)}), 500


# ============================================================
# EARLY EXIT TOOL
# ============================================================

import early_exit_lib


@app.route('/early-exit')
def early_exit_page():
    return render_template('early_exit.html')


@app.route('/early-exit/expiries')
def early_exit_expiries():
    kite = get_kite_client()
    try:
        expiries = early_exit_lib.get_available_expiries(kite)
        return jsonify({"expiries": [str(e) for e in expiries]})
    except Exception as exc:
        logging.exception("early_exit_expiries error")
        return jsonify({"error": str(exc)}), 500


@app.route('/early-exit/preview')
def early_exit_preview():
    kite = get_kite_client()
    interest_rate = 10.0
    try:
        interest_rate = float(config["option_details"].get("interest_rate", 10.0))
    except (KeyError, ValueError):
        pass

    # Optional expiry filter from query param e.g. ?expiry=2025-05-20
    expiry = None
    expiry_str = request.args.get('expiry', '').strip()
    if expiry_str:
        try:
            from datetime import date as _date
            expiry = _date.fromisoformat(expiry_str)
        except ValueError:
            return jsonify({"error": f"Invalid expiry format: {expiry_str!r} — use YYYY-MM-DD"}), 400

    # Optional discount override: ?discount=10.0
    discount_pct = early_exit_lib.DEFAULT_DISCOUNT_PCT
    discount_str = request.args.get('discount', '').strip()
    if discount_str:
        try:
            discount_pct = float(discount_str)
            if not (0 < discount_pct < 100):
                return jsonify({"error": "discount must be between 0 and 100"}), 400
        except ValueError:
            return jsonify({"error": f"Invalid discount value: {discount_str!r}"}), 400

    # Optional IV offset: ?iv_offset=1.0 (percentage points, may be negative)
    iv_offset_pct = 0.0
    iv_offset_str = request.args.get('iv_offset', '').strip()
    if iv_offset_str:
        try:
            iv_offset_pct = float(iv_offset_str)
        except ValueError:
            return jsonify({"error": f"Invalid iv_offset value: {iv_offset_str!r}"}), 400

    try:
        result = early_exit_lib.build_preview(
            kite, interest_rate=interest_rate, expiry=expiry,
            discount_pct=discount_pct, iv_offset_pct=iv_offset_pct,
        )
        return jsonify(result)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logging.exception("early_exit_preview unexpected error")
        return jsonify({"error": f"Unexpected error: {exc}"}), 500


@app.route('/early-exit/run', methods=['POST'])
@limiter.limit("30 per minute")
def early_exit_run():
    kite    = get_kite_client()
    payload = request.json or {}
    symbols = payload.get('symbols', [])
    legs    = payload.get('legs', [])
    if not symbols:
        return jsonify({"error": "No symbols provided"}), 400
    results = early_exit_lib.place_gtts(kite, symbols, legs)
    return jsonify({"results": results})


@app.route('/early-exit/run-active', methods=['POST'])
@limiter.limit("30 per minute")
def early_exit_run_active():
    kite    = get_kite_client()
    payload = request.json or {}
    symbols = payload.get('symbols', [])
    legs    = payload.get('legs', [])
    if not symbols:
        return jsonify({"error": "No symbols provided"}), 400
    results = early_exit_lib.place_active_orders(kite, symbols, legs)
    return jsonify({"results": results})


# ============================================================
# EARLY EXIT TOOL — SENSEX
# ============================================================

import early_exit_sensex_lib


@app.route('/early-exit-sensex')
def early_exit_sensex_page():
    return render_template('early_exit_sensex.html')


@app.route('/early-exit-sensex/expiries')
def early_exit_sensex_expiries():
    kite = get_kite_client()
    try:
        expiries = early_exit_sensex_lib.get_available_expiries(kite)
        return jsonify({"expiries": [str(e) for e in expiries]})
    except Exception as exc:
        logging.exception("early_exit_sensex_expiries error")
        return jsonify({"error": str(exc)}), 500


@app.route('/early-exit-sensex/preview')
def early_exit_sensex_preview():
    kite = get_kite_client()
    interest_rate = 10.0
    try:
        interest_rate = float(config["option_details"].get("interest_rate", 10.0))
    except (KeyError, ValueError):
        pass

    # Optional expiry filter: ?expiry=YYYY-MM-DD
    expiry = None
    expiry_str = request.args.get('expiry', '').strip()
    if expiry_str:
        try:
            from datetime import date as _date
            expiry = _date.fromisoformat(expiry_str)
        except ValueError:
            return jsonify({"error": f"Invalid expiry format: {expiry_str!r} — use YYYY-MM-DD"}), 400

    # Optional discount override: ?discount=10.0
    discount_pct = early_exit_sensex_lib.DEFAULT_DISCOUNT_PCT
    discount_str = request.args.get('discount', '').strip()
    if discount_str:
        try:
            discount_pct = float(discount_str)
            if not (0 < discount_pct < 100):
                return jsonify({"error": "discount must be between 0 and 100"}), 400
        except ValueError:
            return jsonify({"error": f"Invalid discount value: {discount_str!r}"}), 400

    # Optional IV offset: ?iv_offset=1.0 (percentage points, may be negative)
    iv_offset_pct = 0.0
    iv_offset_str = request.args.get('iv_offset', '').strip()
    if iv_offset_str:
        try:
            iv_offset_pct = float(iv_offset_str)
        except ValueError:
            return jsonify({"error": f"Invalid iv_offset value: {iv_offset_str!r}"}), 400

    try:
        result = early_exit_sensex_lib.build_preview(
            kite, interest_rate=interest_rate, expiry=expiry,
            discount_pct=discount_pct, iv_offset_pct=iv_offset_pct,
        )
        return jsonify(result)
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logging.exception("early_exit_sensex_preview unexpected error")
        return jsonify({"error": f"Unexpected error: {exc}"}), 500


@app.route('/early-exit-sensex/run', methods=['POST'])
@limiter.limit("30 per minute")
def early_exit_sensex_run():
    kite    = get_kite_client()
    payload = request.json or {}
    symbols = payload.get('symbols', [])
    legs    = payload.get('legs', [])
    if not symbols:
        return jsonify({"error": "No symbols provided"}), 400
    results = early_exit_sensex_lib.place_gtts(kite, symbols, legs)
    return jsonify({"results": results})


@app.route('/early-exit-sensex/run-active', methods=['POST'])
@limiter.limit("30 per minute")
def early_exit_sensex_run_active():
    kite    = get_kite_client()
    payload = request.json or {}
    symbols = payload.get('symbols', [])
    legs    = payload.get('legs', [])
    if not symbols:
        return jsonify({"error": "No symbols provided"}), 400
    results = early_exit_sensex_lib.place_active_orders(kite, symbols, legs)
    return jsonify({"results": results})


if __name__ == "__main__":
    from waitress import serve

    logging.info("Starting server: http://{host}:{port}".format(host=HOST, port=PORT))
    serve(app, host=HOST, port=PORT, threads=8)
