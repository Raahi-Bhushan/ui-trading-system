import os
import time
import logging
import math
import threading
from typing import Callable
from kiteconnect import KiteTicker
from kiteconnect import KiteConnect
import json
import datetime
from datetime import timezone, timedelta
import numpy as np
import configparser
import sys
from random import randint
import functools
from requests.exceptions import ReadTimeout, ConnectionError as RequestsConnectionError, SSLError
from kiteconnect import exceptions as kite_exceptions
import greeks_lib as mibian

try:
    from pymemcache.client.base import Client as _MemcacheClient
    _memcache_client: "_MemcacheClient | None" = _MemcacheClient(
        ('localhost', 11211), connect_timeout=0.5, timeout=0.5, ignore_exc=True
    )
except Exception:
    _memcache_client = None



from timeit import Timer
import os 
# Timezone standard: IST (GMT+5:30)
IST = timezone(timedelta(hours=5, minutes=30))

def get_ist_now():
    """Returns the current IST time."""
    return datetime.datetime.now(IST)


# Indian F&O market trading window (IST)
_MARKET_OPEN_TIME = datetime.time(9, 15)
_MARKET_CLOSE_TIME = datetime.time(15, 30)


def is_market_open() -> bool:
    """Return True only during NSE/BSE F&O trading hours on non-holiday weekdays.

    Trading hours are 09:15–15:30 IST on Monday through Friday, excluding
    NSE trading holidays cached in instruments.db by sync_instruments().
    """
    now = get_ist_now()
    today = now.date()
    current_time = now.time()

    is_weekend = today.weekday() >= 5
    is_holiday = instrument_cache.is_market_holiday(today)
    is_within_hours = _MARKET_OPEN_TIME <= current_time <= _MARKET_CLOSE_TIME

    is_open = not is_weekend and not is_holiday and is_within_hours

    if not is_open:
        logging.debug(
            "[MARKET_CHECK] Time: %s, Weekday: %d, Holiday: %s, Open: %s",
            now.strftime("%Y-%m-%d %H:%M:%S %Z"),
            today.weekday(),
            is_holiday,
            is_open,
        )

    return is_open


import instrument_cache
from kite_api_monitor import MonitoredKite


def retry_with_backoff(
    max_retries: int = 5,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True
):
    """
    Decorator that retries a function with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts.
        base_delay: Initial delay between retries in seconds.
        max_delay: Maximum delay between retries in seconds.
        exponential_base: Base for exponential backoff calculation.
        jitter: If True, adds random jitter to prevent thundering herd.

    Returns:
        Decorator function.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except (ReadTimeout, RequestsConnectionError, SSLError, TimeoutError,
                        ConnectionError, OSError,
                        kite_exceptions.DataException,
                        kite_exceptions.NetworkException) as e:
                    last_exception = e
                    if attempt < max_retries:
                        # Calculate delay with exponential backoff
                        delay = min(
                            base_delay * (exponential_base ** attempt),
                            max_delay
                        )
                        # Add jitter (random variation) to prevent thundering herd
                        if jitter:
                            delay = delay * (0.5 + randint(0, 100) / 100.0)
                        logging.warning(
                            f"Attempt {attempt + 1}/{max_retries + 1} failed for "
                            f"{func.__name__}: {type(e).__name__}: {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        time.sleep(delay)
                    else:
                        logging.error(
                            f"All {max_retries + 1} attempts failed for "
                            f"{func.__name__}: {type(e).__name__}: {e}"
                        )
                        raise
                except Exception as e:
                    # For non-network errors, don't retry
                    logging.error(f"Non-retryable error in {func.__name__}: {e}")
                    raise
            raise last_exception
        return wrapper
    return decorator


margin_spread = 43000 
margin_both_pe_ce = 222000 
margin_single_pe_ce = 162000 


request_token = "";
access_token = "";

symbol = "";
quantity="";

buy_quantity=""
sell_quantity=""

buy_gap = "";
sell_gap = "";
# Percentage gap tracking - calculated from initial price at startup
buy_gap_percentage = 0.0  # Stored as decimal (e.g., 0.27 for 27%)
sell_gap_percentage = 0.0
initial_sell_gap = 0.0  # Original absolute gap for max comparison (sell protection)

exchange = "";
tag = "Unknown";
segment = ""

_pending_trigger_spot: float = 0.0
_pending_trigger_index: str = ""

typeOfProduct = "NRML"
start_time = ""
instrument_token = ""
orders = {}
order_numbers_storage = {}

# GTT fallback tracking: key = unique tag string (echoed back by Zerodha on triggered order updates)
# value: {trigger_id, symbol, exchange, product, quantity, transaction_type, price,
#          associated_regular_order_id (-1 if sibling is also GTT),
#          associated_gtt_tag (sibling GTT tag or None)}
pending_gtt_fallbacks = {}
# Tracks order_ids currently being auto-repriced by _reprice_orders_on_multiplier_change().
# Used to distinguish our own modify_order() OPEN callbacks from user-initiated manual changes.
_pending_auto_reprice_ids: set[str] = set()
initial_positions = -1
current_positions = 0

kws = None

# Watchdog: track last tick time and stored callbacks for reconnection
last_tick_time = 0
_stored_ticker_callbacks = {}
_subscribed_tokens = []
# Timestamps used by watchdog to distinguish "connected but no ticks" (market closed)
# from "never connected" or "connection lost" — both need a reconnect attempt.
_ws_last_connected_at: float = 0.0   # set when on_connect fires
_ws_last_noreconnect_at: float = 0.0  # set when KiteTicker exhausts its own retries

# Tick fan-out: multiple modules can observe tick data without replacing kws callbacks
_tick_observers: list[Callable[[list[dict]], None]] = []
_tick_observers_lock = threading.Lock()


def register_tick_observer(callback: Callable[[list[dict]], None]) -> None:
    """Register a callable to receive every tick batch from on_ticks.

    Thread-safe and idempotent — registering the same function twice is a no-op.
    Observers are called in registration order; an observer that raises is logged
    and skipped so it never kills the tick pipeline.

    Args:
        callback: Function accepting a list of tick dicts (same format as kws.on_ticks).
    """
    with _tick_observers_lock:
        if callback not in _tick_observers:
            _tick_observers.append(callback)
    logging.info("register_tick_observer: registered %s", callback.__name__)


def _broadcast_ticks(ticks: list[dict]) -> None:
    """Fan-out ticks to all registered observers. Never raises.

    Args:
        ticks: List of tick dicts received from KiteTicker.
    """
    with _tick_observers_lock:
        observers = list(_tick_observers)
    for fn in observers:
        try:
            fn(ticks)
        except Exception:
            logging.exception("tick observer %s raised an error", fn.__name__)

exceptNFBNF = False


#Callback Functions Variable
on_order_update_solo_callback = None




config_obj =  configparser.ConfigParser()
# Use absolute path for config file
base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, "configfile.ini")
# The example file is read first as a defaults layer so a fresh install
# (no configfile.ini yet) can still import this module and reach the
# /setup wizard; real config values override the placeholders.
config_obj.read([os.path.join(base_dir, "configfile.ini.example"), config_path])

if not os.path.exists(config_path):
    logging.warning(
        "configfile.ini not found — running on placeholder defaults from "
        "configfile.ini.example. Complete setup at /setup before trading."
    )

login_details = config_obj["kite_login_details"]
option_details = config_obj["option_details"]
other_details = config_obj["others"]

todays_volatility = float(option_details['current_volatility']) 
interest_rate = float(option_details['interest_rate']) 
min_nifty_delta = int(option_details['min_nifty_delta']) 
max_nifty_delta = int(option_details['max_nifty_delta']) 

min_bank_nifty_delta = int(option_details['min_bank_nifty_delta']) 
max_bank_nifty_delta = int(option_details['max_bank_nifty_delta']) 

delta_calculation_days = int(option_details['delta_calculation_days']) 


cool_off_time = int(other_details['cool_off_time'])
order_gtt_regular = other_details['order_gtt_regular']
gtt_trigger_buffer_pct = float(other_details.get('gtt_trigger_buffer_pct', '0.05'))

# [safety] live_trading — dry-run kill switch for order placement.
# Read from the REAL configfile.ini only (never the example defaults layer,
# which would silently flip live installs to dry-run). Semantics:
#   - configfile.ini missing entirely  → dry-run (fresh install, safe)
#   - present but no [safety] key      → live (legacy installs, unchanged)
#   - present with the key             → whatever it says
_safety_parser = configparser.ConfigParser()
_safety_parser.read(config_path)
if not os.path.exists(config_path):
    live_trading_enabled = False
else:
    live_trading_enabled = _safety_parser.getboolean("safety", "live_trading", fallback=True)
if os.path.exists(config_path) and not _safety_parser.has_option("safety", "live_trading"):
    logging.info(
        "configfile.ini has no [safety] live_trading key — assuming live trading "
        "enabled (legacy behavior). Add '[safety]\\nlive_trading = true' to silence this."
    )
elif not live_trading_enabled:
    logging.warning(
        "[safety] live_trading = false — DRY-RUN MODE: all order placement is "
        "simulated and logged, nothing is sent to the broker (Upstox)."
    )


def is_live_trading_enabled() -> bool:
    """Return whether real orders may be sent to the broker.

    Returns:
        True when [safety] live_trading is true or absent (legacy installs);
        False when the config explicitly disables live trading (dry-run mode).
    """
    return live_trading_enabled


def _dry_run_block(action: str, tradingsymbol: str, detail: str) -> None:
    """Log a simulated order when dry-run mode blocks a broker call.

    Args:
        action: Short verb for the blocked call, e.g. "place_order".
        tradingsymbol: Instrument the order targeted.
        detail: Human-readable parameter summary for the log line.
    """
    logging.warning(
        "[DRY-RUN] %s for %s blocked — live_trading is disabled in configfile.ini "
        "[safety]. Would have sent: %s", action, tradingsymbol, detail
    )

delta_limits_config = {}

global_restrict_buy = 0
global_restrict_sell = 0

def load_delta_limits():
    global delta_limits_config
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "delta_limits.json")
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                delta_limits_config = json.load(f)
        else:
             # Default structure if file doesn't exist
            delta_limits_config = {
                "NIFTY": {"default": {"min": -3500, "max": 3500}},
                "BANKNIFTY": {"default": {"min": -3000, "max": 1000}}
            }
    except Exception as e:
        logging.error(f"Error loading delta limits: {e}")


_WE_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "wave_extractor_config.json")
_we_config_cache: dict = {}


def _get_underlying_name(symbol: str) -> str:
    """Extract the underlying index name from an options trading symbol."""
    if symbol.startswith("BANKNIFTY"):
        return "BANKNIFTY"
    elif symbol.startswith("SENSEX"):
        return "SENSEX"
    elif symbol.startswith("NIFTY") and not symbol.startswith("NIFTYBEES"):
        return "NIFTY"
    return "NIFTY"


def _get_spot_symbol_for(symbol: str) -> str | None:
    """Return the Kite quote key for the underlying spot index of an options symbol.

    Args:
        symbol: Options trading symbol (e.g. "NIFTY24JUN24500CE").

    Returns:
        Kite quote key such as "NSE:NIFTY 50", or None if unrecognised.
    """
    if symbol.startswith("BANKNIFTY"):
        return "NSE:NIFTY BANK"
    elif symbol.startswith("SENSEX"):
        return "BSE:SENSEX"
    elif symbol.startswith("NIFTY") and not symbol.startswith("NIFTYBEES"):
        return "NSE:NIFTY 50"
    return None


def load_wave_extractor_config(symbol: str) -> dict:
    """Load wave extractor config for the underlying index of the given symbol.

    Reads from wave_extractor_config.json. Missing keys are handled by each
    sub-function's own defaults, so the file can be partially populated.
    Results are cached in-process; call reload_wave_extractor_config() to
    force a fresh read after the file changes.

    Args:
        symbol: Trading symbol (e.g. "NIFTY24JUN24500CE").

    Returns:
        Dict with keys: cool_off_time, multiplier_scale, delta_multiplier,
        velocity_guard, fill_cooldown, cross_instance. Empty dict on error.
    """
    global _we_config_cache
    if not _we_config_cache:
        reload_wave_extractor_config()
    underlying = _get_underlying_name(symbol)
    return _we_config_cache.get(underlying, {})


def reload_wave_extractor_config() -> None:
    """Force a fresh read of wave_extractor_config.json into the in-process cache."""
    global _we_config_cache
    try:
        with open(_WE_CONFIG_PATH, "r") as f:
            _we_config_cache = json.load(f)
        logging.info("wave_extractor_config loaded: underlyings=%s", list(_we_config_cache.keys()))
    except FileNotFoundError:
        logging.warning("wave_extractor_config.json not found at %s — using empty defaults", _WE_CONFIG_PATH)
        _we_config_cache = {}
    except json.JSONDecodeError as exc:
        logging.error("wave_extractor_config.json is malformed: %s", exc)
        _we_config_cache = {}


def save_wave_extractor_config(config: dict) -> bool:
    """Write updated wave_extractor_config.json atomically (temp + rename).

    Args:
        config: The full config dict (all underlyings).

    Returns:
        True on success, False on error.
    """
    global _we_config_cache
    tmp_path = _WE_CONFIG_PATH + ".tmp"
    try:
        with open(tmp_path, "w") as f:
            json.dump(config, f, indent=2)
        os.rename(tmp_path, _WE_CONFIG_PATH)
        _we_config_cache = config
        logging.info("wave_extractor_config saved successfully")
        return True
    except OSError as exc:
        logging.error("Failed to save wave_extractor_config.json: %s", exc)
        return False


# ---------------------------------------------------------------------------
# Velocity guard — spot price history for momentum detection
# ---------------------------------------------------------------------------

_spot_price_history: list[tuple[datetime.datetime, float]] = []


def update_spot_price_history(spot_price: float, window_minutes: int = 30) -> None:
    """Append a spot price sample and trim entries older than 2× the window.

    Args:
        spot_price: Current underlying index last price.
        window_minutes: The velocity-guard window used for retention (kept at 2×).
    """
    now = get_ist_now()
    _spot_price_history.append((now, spot_price))
    cutoff = now - datetime.timedelta(minutes=window_minutes * 2)
    while _spot_price_history and _spot_price_history[0][0] < cutoff:
        _spot_price_history.pop(0)


def get_velocity_multiplier(symbol_type: str, symbol: str, we_config: dict) -> tuple[float, float]:
    """Return a gap multiplier when the underlying spot is in strong momentum.

    If spot moved > threshold_pct% in the last window_minutes, widens the gap
    for the side that would add directional risk (CE SELL when rising, PE SELL
    when falling). All parameters come from we_config['velocity_guard'].

    Args:
        symbol_type: 'ce' or 'pe' (lowercase).
        symbol: Trading symbol (for logging).
        we_config: Per-underlying wave extractor config dict.

    Returns:
        Tuple (buy_gap_multiplier, sell_gap_multiplier).
    """
    cfg = we_config.get("velocity_guard", {})
    window_minutes = float(cfg.get("window_minutes", 15))
    threshold_pct  = float(cfg.get("threshold_pct", 0.8))
    gap_multiplier = float(cfg.get("gap_multiplier", 1.5))

    if len(_spot_price_history) < 2:
        logging.info(
            "[WE_MULTIPLIER] velocity_guard | symbol=%s insufficient_history → buy=1.0 sell=1.0",
            symbol,
        )
        return (1.0, 1.0)

    cutoff = get_ist_now() - datetime.timedelta(minutes=window_minutes)
    recent_prices = [p for t, p in _spot_price_history if t >= cutoff]

    if len(recent_prices) < 2:
        logging.info(
            "[WE_MULTIPLIER] velocity_guard | symbol=%s no_recent_prices(window=%.0fmin) → buy=1.0 sell=1.0",
            symbol, window_minutes,
        )
        return (1.0, 1.0)

    spot_old = recent_prices[0]
    spot_new = recent_prices[-1]
    pct_move = (spot_new - spot_old) / spot_old * 100.0

    buy_mult, sell_mult = 1.0, 1.0
    triggered = abs(pct_move) > threshold_pct

    if pct_move > threshold_pct:        # market rising fast
        if symbol_type == "ce":
            sell_mult = gap_multiplier  # widen CE sell gap
        else:
            buy_mult = gap_multiplier   # widen PE buy gap
    elif pct_move < -threshold_pct:     # market falling fast
        if symbol_type == "ce":
            buy_mult = gap_multiplier   # widen CE buy gap
        else:
            sell_mult = gap_multiplier  # widen PE sell gap

    logging.info(
        "[WE_MULTIPLIER] velocity_guard | symbol=%s type=%s window_min=%.0f "
        "spot_old=%.2f spot_new=%.2f pct_move=%+.3f threshold=%.2f "
        "%s gap_mult=%.2f → buy=%.4f sell=%.4f",
        symbol, symbol_type, window_minutes,
        spot_old, spot_new, pct_move, threshold_pct,
        "TRIGGERED" if triggered else "not_triggered",
        gap_multiplier if triggered else 1.0,
        buy_mult, sell_mult,
    )
    return (buy_mult, sell_mult)


_ZERO_AMPLITUDE_RESULT = {"iv": 0.0, "delta": 0.0, "gamma": 0.0, "amplitude_pct": 0.0}


def _amplitude_from_volatility(
    spot: float,
    strike: float,
    dte: float,
    option_type: str,
    interest_rate_pct: float,
    volatility_pct: float,
) -> dict:
    """Forward-price an option at a given volatility/DTE and estimate its
    expected 1-trading-day premium move as a % of that theoretical price.

    Combines delta (first-order spot sensitivity) and gamma (acceleration of
    that sensitivity — the mechanism behind near-expiry ATM premium blowup)
    to project one trading day's expected premium swing.

    Args:
        spot: Current underlying price.
        strike: Option strike price.
        dte: Days to expiry (calendar days, as consumed by greeks_lib.BS).
        option_type: "CE" for call, "PE" for put.
        interest_rate_pct: Risk-free rate as a percentage (e.g. 10 for 10%).
        volatility_pct: Volatility to price with, as a percentage.

    Returns:
        dict with keys: iv (the input volatility_pct, echoed back), delta,
        gamma, amplitude_pct (fraction of the theoretical price) — or
        all-zero values if inputs are non-positive or the backend can't
        supply gamma.
    """
    if spot <= 0 or dte <= 0 or volatility_pct <= 0:
        return dict(_ZERO_AMPLITUDE_RESULT)

    bs = mibian.BS([spot, strike, interest_rate_pct, dte], volatility=volatility_pct)
    price = bs.callPrice if option_type == "CE" else bs.putPrice
    delta = bs.callDelta if option_type == "CE" else bs.putDelta
    gamma = bs.gamma
    if not price or price <= 0 or delta is None or gamma is None:
        logging.warning(
            "_amplitude_from_volatility: price/gamma unavailable from active "
            "greeks backend (%s) — configure [greeks] library = opengreeks "
            "in configfile.ini", mibian.get_active_backend(),
        )
        return {"iv": volatility_pct, "delta": delta or 0.0, "gamma": 0.0, "amplitude_pct": 0.0}

    expected_spot_move = spot * (volatility_pct / 100.0) * math.sqrt(1.0 / 365.0)
    expected_premium_move = abs(delta) * expected_spot_move + 0.5 * gamma * expected_spot_move ** 2
    amplitude_pct = expected_premium_move / price

    return {
        "iv": round(volatility_pct, 2),
        "delta": round(delta, 4),
        "gamma": round(gamma, 6),
        "amplitude_pct": round(amplitude_pct, 4),
    }


def get_expected_amplitude_pct(
    spot: float,
    strike: float,
    dte: float,
    ltp: float,
    option_type: str,
    interest_rate_pct: float,
) -> dict:
    """Estimate an option's expected 1-trading-day premium move as a % of LTP.

    Back-solves implied volatility from the option's live LTP and DTE, then
    delegates to `_amplitude_from_volatility()` for the delta+gamma premium
    projection. This is a read-only estimation helper for the Amplitude
    Testbed; it is not wired into live order placement.

    Args:
        spot: Current underlying price.
        strike: Option strike price.
        dte: Days to expiry (calendar days, as consumed by greeks_lib.BS).
        ltp: Option's last traded price.
        option_type: "CE" for call, "PE" for put.
        interest_rate_pct: Risk-free rate as a percentage (e.g. 10 for 10%).

    Returns:
        dict with keys: iv (percent), delta, gamma, amplitude_pct (fraction
        of LTP), or all-zero values if LTP/DTE are non-positive or the
        Greeks computation fails.

    Raises:
        ValueError: If option_type is not "CE" or "PE".
    """
    if option_type not in ("CE", "PE"):
        raise ValueError(f"option_type must be 'CE' or 'PE', got {option_type!r}")

    if ltp <= 0 or dte <= 0 or spot <= 0:
        return dict(_ZERO_AMPLITUDE_RESULT)

    try:
        iv_solver = mibian.BS(
            [spot, strike, interest_rate_pct, dte],
            callPrice=ltp if option_type == "CE" else None,
            putPrice=ltp if option_type == "PE" else None,
        )
        implied_vol = iv_solver.impliedVolatility
        if not implied_vol or implied_vol <= 0:
            return dict(_ZERO_AMPLITUDE_RESULT)
    except (ValueError, ZeroDivisionError, ArithmeticError) as e:
        logging.warning("get_expected_amplitude_pct: IV solve failed: %s", e)
        return dict(_ZERO_AMPLITUDE_RESULT)

    return _amplitude_from_volatility(spot, strike, dte, option_type, interest_rate_pct, implied_vol)


def get_theoretical_amplitude_at_dte(
    spot: float,
    strike: float,
    dte: float,
    option_type: str,
    interest_rate_pct: float,
    volatility_pct: float,
) -> dict:
    """Estimate expected 1-day amplitude at a hypothetical DTE, holding IV fixed.

    Used by the Amplitude Testbed to compare "amplitude today" against
    "amplitude if this same option had `dte` days left, at today's IV" — the
    baseline that defines the amplitude_multiplier shown in the table. Unlike
    calling `get_expected_amplitude_pct()` a second time with a different DTE
    against the *real* LTP (which would back-solve a distorted, incorrect
    IV — the real LTP embeds the real DTE's time value, not the hypothetical
    one's), this holds volatility constant and forward-prices the option at
    the hypothetical DTE instead.

    Args:
        spot: Current underlying price.
        strike: Option strike price.
        dte: Hypothetical days to expiry to evaluate at.
        option_type: "CE" for call, "PE" for put.
        interest_rate_pct: Risk-free rate as a percentage (e.g. 10 for 10%).
        volatility_pct: Volatility to hold fixed, as a percentage (typically
            the IV already back-solved from the option's real LTP/DTE).

    Returns:
        dict with keys: iv, delta, gamma, amplitude_pct — see
        `_amplitude_from_volatility()`.

    Raises:
        ValueError: If option_type is not "CE" or "PE".
    """
    if option_type not in ("CE", "PE"):
        raise ValueError(f"option_type must be 'CE' or 'PE', got {option_type!r}")
    return _amplitude_from_volatility(spot, strike, dte, option_type, interest_rate_pct, volatility_pct)


# ---------------------------------------------------------------------------
# Fill cooldown — suppress a side after rapid consecutive fills
# ---------------------------------------------------------------------------

_same_side_fill_times: dict[str, list[datetime.datetime]] = {"BUY": [], "SELL": []}
_cooldown_active_until: dict[str, datetime.datetime | None] = {"BUY": None, "SELL": None}

# Snapshot of the last multiplier pipeline computed by place_duo_order().
# Written to status_*.json so the dashboard can display it in real time.
_last_multiplier_info: dict = {}


def record_same_side_fill(side: str, symbol: str, we_config: dict) -> None:
    """Record a wave extractor fill and activate cooldown if threshold is reached.

    Called from on_order_update() when a Scraper algo order completes. Trims
    old fill timestamps outside the tracking window before checking the trigger.

    Args:
        side: Transaction type — kite.TRANSACTION_TYPE_BUY or SELL ('BUY'/'SELL').
        symbol: Trading symbol (for logging).
        we_config: Per-underlying wave extractor config dict.
    """
    cfg = we_config.get("fill_cooldown", {})
    window_minutes   = float(cfg.get("window_minutes", 10))
    trigger_count    = int(cfg.get("trigger_count", 2))
    cooldown_minutes = float(cfg.get("cooldown_duration_minutes", 15))

    now = get_ist_now()
    _same_side_fill_times[side].append(now)
    cutoff = now - datetime.timedelta(minutes=window_minutes)
    _same_side_fill_times[side] = [t for t in _same_side_fill_times[side] if t >= cutoff]
    recent_count = len(_same_side_fill_times[side])

    if recent_count >= trigger_count:
        cooldown_until = now + datetime.timedelta(minutes=cooldown_minutes)
        _cooldown_active_until[side] = cooldown_until
        logging.warning(
            "[WE_MULTIPLIER] fill_cooldown | ACTIVATED side=%s symbol=%s "
            "fills_in_window=%d trigger=%d cooldown_until=%s IST",
            side, symbol, recent_count, trigger_count,
            cooldown_until.strftime("%H:%M:%S"),
        )
    else:
        logging.info(
            "[WE_MULTIPLIER] fill_cooldown | recorded side=%s symbol=%s "
            "fills_in_window=%d/%d (no cooldown yet)",
            side, symbol, recent_count, trigger_count,
        )


def is_side_in_cooldown(side: str, symbol: str) -> bool:
    """Return True if this side is currently in fill cooldown.

    Auto-lifts the cooldown when it expires. Logs both ACTIVE and LIFTED states.

    Args:
        side: 'BUY' or 'SELL'.
        symbol: Trading symbol (for logging).

    Returns:
        True if cooldown is active, False otherwise.
    """
    until = _cooldown_active_until.get(side)
    if until is None:
        return False
    now = get_ist_now()
    if now >= until:
        _cooldown_active_until[side] = None
        logging.info(
            "[WE_MULTIPLIER] fill_cooldown | LIFTED side=%s symbol=%s",
            side, symbol,
        )
        return False
    remaining = (until - now).total_seconds() / 60.0
    logging.info(
        "[WE_MULTIPLIER] fill_cooldown | ACTIVE side=%s symbol=%s remaining_min=%.1f restrict=1",
        side, symbol, remaining,
    )
    return True


logging.basicConfig(level=logging.INFO)

all_instruments = {}
token_symbol_map = {}
nifty_symbol = "NSE:NIFTY 50"
gift_nifty_symbol = "NSEIX:GIFT NIFTY"
bank_nifty_symbol = "NSE:NIFTY BANK"



nifty_lot_size = 65  # Default, will be updated dynamically
nifty_strike_gap = 50  # NIFTY options have 50-point strike intervals
bank_nifty_lot_size = 15  # Default, will be updated dynamically
sensex_symbol = "BSE:SENSEX"
sensex_lot_size = 20  # Default, will be updated dynamically

# ---------------------------------------------------------------------------
# Two-tier in-memory cache for the 3 fixed index spot quotes.
# L1: in-process dict (15s) — eliminates within-loop duplicates, zero I/O.
# L2: memcached (30s)        — shared across all scraper processes on the host.
# L3: kite.quote()           — only called when both caches miss.
# NOTE: The velocity guard's kite.quote() call is INTENTIONALLY NOT cached —
#       see the comment there. Do not route it through _get_index_quote_cached.
# ---------------------------------------------------------------------------
_INDEX_QUOTE_L1_TTL = 15   # seconds — in-process TTL
_INDEX_QUOTE_L2_TTL = 30   # seconds — memcached TTL
_index_quote_cache: dict[str, tuple[float, dict]] = {}  # symbol → (expires_at, quote_dict)

# Per-symbol hit/miss counters. Keys added lazily on first access.
# Structure: { symbol: {"l1_hits": int, "l2_hits": int, "l3_fetches": int} }
_index_quote_stats: dict[str, dict[str, int]] = {}
_INDEX_QUOTE_STATS_LOG_INTERVAL = 100  # log aggregate totals every N calls (across all symbols)
_index_quote_total_calls: int = 0


def log_index_quote_cache_stats() -> None:
    """Log a summary of index quote cache hit/miss counts since process start.

    Call this at shutdown or from a scheduled task to review daily cache efficiency.
    """
    if not _index_quote_stats:
        logging.info("index_quote_cache_stats: no calls recorded yet")
        return
    for sym, counts in _index_quote_stats.items():
        total = counts["l1_hits"] + counts["l2_hits"] + counts["l3_fetches"]
        hit_rate = (counts["l1_hits"] + counts["l2_hits"]) / total * 100 if total else 0
        logging.info(
            "index_quote_cache_stats | symbol=%s total=%d l1_hits=%d l2_hits=%d "
            "l3_fetches(kite_api)=%d hit_rate=%.1f%%",
            sym, total, counts["l1_hits"], counts["l2_hits"], counts["l3_fetches"], hit_rate,
        )


def _increment_quote_stat(symbol: str, tier: str) -> None:
    """Increment hit/miss counter and periodically log aggregate totals."""
    global _index_quote_total_calls
    if symbol not in _index_quote_stats:
        _index_quote_stats[symbol] = {"l1_hits": 0, "l2_hits": 0, "l3_fetches": 0}
    _index_quote_stats[symbol][tier] += 1
    _index_quote_total_calls += 1
    if _index_quote_total_calls % _INDEX_QUOTE_STATS_LOG_INTERVAL == 0:
        log_index_quote_cache_stats()


def _get_index_quote_cached(symbol: str) -> dict:
    """Fetch an index spot quote with two-tier in-memory caching.

    Args:
        symbol: Kite quote key, e.g. "NSE:NIFTY 50".

    Returns:
        Quote dict with same structure as kite.quote()[symbol].

    Raises:
        Exception: Propagates from kite.quote() when both cache tiers miss.
    """
    global _index_quote_cache, _memcache_client
    now = time.time()

    # L1: in-process cache
    cached = _index_quote_cache.get(symbol)
    if cached and now < cached[0]:
        logging.debug("index_quote_cache L1 hit: %s", symbol)
        _increment_quote_stat(symbol, "l1_hits")
        return cached[1]

    # L2: memcached (skipped silently if client unavailable)
    if _memcache_client is not None:
        try:
            mc_val = _memcache_client.get(f"idx_quote:{symbol}".encode())
            if mc_val is not None:
                quote_dict = json.loads(mc_val)
                _index_quote_cache[symbol] = (now + _INDEX_QUOTE_L1_TTL, quote_dict)
                logging.debug("index_quote_cache L2 hit: %s", symbol)
                _increment_quote_stat(symbol, "l2_hits")
                return quote_dict
        except Exception as _me:
            logging.debug("memcache get failed for %s: %s", symbol, _me)

    # L3: Kite API
    logging.debug("index_quote_cache L3 fetch (kite API): %s", symbol)
    _increment_quote_stat(symbol, "l3_fetches")
    raw = kite.quote(symbol)
    quote_dict = raw[symbol]

    # Populate L1
    _index_quote_cache[symbol] = (now + _INDEX_QUOTE_L1_TTL, quote_dict)

    # Populate L2
    if _memcache_client is not None:
        try:
            _memcache_client.set(
                f"idx_quote:{symbol}".encode(),
                json.dumps(quote_dict).encode(),
                expire=_INDEX_QUOTE_L2_TTL,
            )
        except Exception as _me:
            logging.debug("memcache set failed for %s: %s", symbol, _me)

    return quote_dict
sensex_strike_gap = 100  # SENSEX options have 100-point strike intervals


def update_lot_sizes_from_instruments():
    """
    Fetch and update lot sizes for NIFTY, BANKNIFTY, and SENSEX from Kite instruments.
    
    This function should be called after kite client is initialized to get
    the current lot sizes dynamically instead of relying on hardcoded values.
    """
    global nifty_lot_size, bank_nifty_lot_size, sensex_lot_size
    
    try:
        instruments = kite.instruments("NFO")
        for inst in instruments:
            if inst["name"] == "NIFTY" and inst["instrument_type"] == "FUT":
                nifty_lot_size = int(inst["lot_size"])
                logging.info(f"Updated NIFTY lot size to {nifty_lot_size}")
            elif inst["name"] == "BANKNIFTY" and inst["instrument_type"] == "FUT":
                bank_nifty_lot_size = int(inst["lot_size"])
                logging.info(f"Updated BANKNIFTY lot size to {bank_nifty_lot_size}")
    except Exception as e:
        logging.warning(f"Could not fetch NFO lot sizes: {e}")
    
    try:
        bfo_instruments = kite.instruments("BFO")
        for inst in bfo_instruments:
            if inst["name"] == "SENSEX" and inst["instrument_type"] == "FUT":
                sensex_lot_size = int(inst["lot_size"])
                logging.info(f"Updated SENSEX lot size to {sensex_lot_size}")
                break
    except Exception as e:
        logging.warning(f"Could not fetch BFO lot sizes: {e}")

old_quote_price = -1
duo_old_sell_price = -1
duo_old_buy_price = -1

exchange = "NFO"


def set_exchange(passed_exchange):
    global exchange
    exchange = passed_exchange
    return


def set_execution_variables(passed_symbol, passed_buy_gap, passed_sell_gap, passed_quantity, passed_request_token):
    global symbol, buy_gap, sell_gap, quantity, request_token, start_time, segment, instrument_token
    symbol = passed_symbol
    buy_gap = passed_buy_gap
    sell_gap = passed_sell_gap
    quantity = passed_quantity
    request_token = passed_request_token
    if not start_time:
        start_time = str(get_ist_now())
    
    try:
        # Optimization: Use SQLite cache instead of fetching 60k instruments from API
        instr = instrument_cache.get_instrument(symbol)
        if instr:
            segment = instr['segment']
            instrument_token = instr['instrument_token']
        else:
            # Fallback if cache is missing (though it shouldn't be if synced)
            logging.warning(f"Symbol {symbol} not found in SQLite cache, falling back to API (Slow)")
            instruments = kite.instruments()
            for instrument in instruments:
                if instrument['tradingsymbol'] == symbol:
                    segment = instrument['segment']
                    instrument_token = instrument['instrument_token']
                    break
    except Exception as e:
        logging.error("Failed to fetch segment for symbol {}: {}".format(symbol, e))

    return



def reread_option_defaults():
    config_obj =  configparser.ConfigParser()
    # Use absolute path for config file
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "configfile.ini")
    config_obj.read(config_path)
    option_details = config_obj["option_details"]

    global todays_volatility, interest_rate, min_nifty_delta, max_nifty_delta, min_bank_nifty_delta, max_bank_nifty_delta, delta_calculation_days

    todays_volatility = float(option_details['current_volatility']) 
    interest_rate = float(option_details['interest_rate']) 
    min_nifty_delta = int(option_details['min_nifty_delta']) 
    max_nifty_delta = int(option_details['max_nifty_delta']) 
    
    min_bank_nifty_delta = int(option_details['min_bank_nifty_delta']) 
    max_bank_nifty_delta = int(option_details['max_bank_nifty_delta']) 
    
    delta_calculation_days = int(option_details['delta_calculation_days'])

    mibian.reread_greeks_config()

    load_delta_limits()
    print("Min and Max NIFTY Delta Values are - "+str(min_nifty_delta)+" ---  MAX ==== "+str(max_nifty_delta))




scraper_last_price = -1


def reset_restrictions() -> dict:
    """Return a fresh restrictions dict with all buy/sell set to 'yes'."""

    def _all_yes() -> dict:
        return {
            'futures': {'buy': 'yes', 'sell': 'yes'},
            'ce':      {'buy': 'yes', 'sell': 'yes'},
            'pe':      {'buy': 'yes', 'sell': 'yes'},
        }

    return {
        'nifty':      _all_yes(),
        'bank_nifty': _all_yes(),
        'sensex':     _all_yes(),
    }




order_ids_completed = {}


api_key= login_details['api_key']
api_secret= login_details['api_secret']
already_executing_order = 0

already_updating_order = 0

# Cooldown for order placement



kite = MonitoredKite(KiteConnect(api_key=api_key))
website=KiteConnect(api_key=api_key).login_url()  # Upstox OAuth dialog
print(website) # This is link used to show the website to get access token
print("-------------------------")

# Persistence for script-generated orders
# Using a global set (thread-safe for reads, GIL handles writes usually, but we are single process)
# NOTE: This list will be cleared on Application Restart.
script_order_ids = set()

def save_script_order(order_id):
    global script_order_ids
    try:
        script_order_ids.add(str(order_id))
        logging.info("Added order {} to in-memory script history (Total: {})".format(order_id, len(script_order_ids)))
    except Exception as e:
        logging.error("Error saving script order to memory: {}".format(e))


# ============================================================================
# Executed Orders Persistence (for Dashboard Order Tracking)
# ============================================================================

def _get_executed_orders_filepath() -> str:
    """
    Get the filepath for today's executed orders JSON file.
    
    Returns:
        str: Absolute path to executed_orders_YYYY-MM-DD.json file.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(base_dir)
    status_dir = os.path.join(project_root, "status")
    
    if not os.path.exists(status_dir):
        os.makedirs(status_dir)
    
    today = get_ist_now().strftime("%Y-%m-%d")
    return os.path.join(status_dir, f"executed_orders_{today}.json")


def _extract_symbol_info(symbol: str) -> dict:
    """
    Extract expiry and option type from a trading symbol.
    
    Args:
        symbol: Trading symbol like 'NIFTY24JAN23000CE', 'BANKNIFTY24JAN45000PE',
                or weekly format like 'SENSEX2620583200PE', 'NIFTY25117...CE'.
        
    Returns:
        dict: Contains 'expiry' (e.g., '24JAN' or '26205') and 'option_type' ('CE', 'PE', or 'FUT').
    """
    import re
    
    # First, check if symbol ends with CE or PE (most common case)
    if symbol.endswith('CE'):
        option_type = 'CE'
    elif symbol.endswith('PE'):
        option_type = 'PE'
    elif symbol.endswith('FUT'):
        option_type = 'FUT'
    else:
        option_type = 'OTHER'
    
    # Pattern 1: Monthly format like NIFTY24JAN23000CE or BANKNIFTY24JAN45000PE
    monthly_pattern = r'([A-Z]+)(\d{2}[A-Z]{3})(\d+)(CE|PE|FUT)?'
    match = re.match(monthly_pattern, symbol)
    
    if match and match.group(2):
        expiry = match.group(2)  # e.g., '24JAN'
        return {'expiry': expiry, 'option_type': option_type}
    
    # Pattern 2: Weekly format like SENSEX2620583200PE or NIFTY251D20000CE
    # Extract expiry from the date encoding (first 5 digits after underlying name)
    weekly_pattern = r'([A-Z]+)(\d{5})'
    match = re.match(weekly_pattern, symbol)
    
    if match:
        expiry = match.group(2)  # e.g., '26205' for Feb 5, 2026
        return {'expiry': expiry, 'option_type': option_type}
    
    # Fallback for futures or other formats
    return {'expiry': 'UNKNOWN', 'option_type': option_type}


def save_executed_order(symbol: str, transaction_type: str, price: float, quantity: int,
                        order_instrument_token: str = "", order_segment: str = "",
                        algo_source: str = "", order_id: str = "",
                        underlying_at_trigger: float = 0.0,
                        underlying_at_execution: float = 0.0,
                        multiplier_info: dict | None = None) -> bool:
    """
    Save an executed order to the daily JSON file for dashboard tracking.

    Args:
        symbol: Trading symbol (e.g., 'NIFTY24JAN23000CE').
        transaction_type: 'BUY' or 'SELL'.
        price: Execution price (should be average_price / actual fill price).
        quantity: Order quantity.
        order_instrument_token: Optional instrument token for chart links.
        order_segment: Optional segment (NFO/BFO) for chart links.
        algo_source: Optional algo identifier. Defaults to global tag variable.
        order_id: Optional Zerodha order ID for dedup during reconciliation.

    Returns:
        bool: True if saved successfully, False otherwise.
    """
    global instrument_token, segment, tag

    # Use global values as fallback if not provided
    token_to_save = order_instrument_token or str(instrument_token) if instrument_token else ""
    segment_to_save = order_segment or segment or ""
    algo_source_to_save = algo_source or tag or "Unknown"

    try:
        filepath = _get_executed_orders_filepath()
        symbol_info = _extract_symbol_info(symbol)

        order = {
            'timestamp': get_ist_now().isoformat(),
            'symbol': symbol,
            'expiry': symbol_info['expiry'],
            'option_type': symbol_info['option_type'],
            'transaction_type': transaction_type,
            'price': float(price),
            'quantity': int(quantity),
            'instrument_token': token_to_save,
            'segment': segment_to_save,
            'algo_source': algo_source_to_save,
            'order_id': str(order_id) if order_id else "",
            'underlying_at_trigger': underlying_at_trigger,
            'underlying_at_execution': underlying_at_execution,
            'multiplier_info': multiplier_info or {},
        }
        
        # Load existing orders or create new structure
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                data = json.load(f)
        else:
            data = {'orders': []}
        
        data['orders'].append(order)
        
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        
        logging.info(f"Saved executed order to history: {symbol} {transaction_type} @ {price}")
        return True
        
    except Exception as e:
        logging.error(f"Error saving executed order: {e}")
        return False


def load_todays_orders() -> list:
    """
    Load all executed orders for today.
    
    Returns:
        list: List of order dictionaries sorted by timestamp (newest first).
    """
    try:
        filepath = _get_executed_orders_filepath()
        
        if not os.path.exists(filepath):
            return []
        
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        orders = data.get('orders', [])
        # Sort by timestamp descending (newest first)
        orders.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        return orders
        
    except Exception as e:
        logging.error(f"Error loading today's orders: {e}")
        return []


# Survivor algo tags - used to filter orders by algo source
SURVIVOR_ALGO_TAGS = ["Trending_Market_Code", "Trend_Mkt_SENSEX", "Trend_Mkt_Stock"]

# Wave Extractor algo tags - used to filter orders by algo source
# Includes: Gap-Odr_Manual (manual gap scripts), Gap-Odr_Auto (auto gap scripts),
# Scraper (ticker_single_scraper_new.py), Unknown (default when tag not set)
WAVE_EXTRACTOR_ALGO_TAGS = ["Gap-Odr_Manual", "Gap-Odr_Auto", "Scraper"]


def load_survivor_orders() -> list:
    """
    Load executed orders for today that were placed by the Survivor algo.
    
    Filters orders where algo_source matches Survivor algo tags:
    - "Trending_Market_Code" (NIFTY Survivor)
    - "Trend_Mkt_SENSEX" (SENSEX Survivor)
    
    Returns:
        list: List of Survivor order dictionaries sorted by timestamp (newest first).
    """
    all_orders = load_todays_orders()
    survivor_orders = [
        order for order in all_orders
        if order.get('algo_source', '') in SURVIVOR_ALGO_TAGS
    ]
    return survivor_orders


def load_wave_extractor_orders() -> list:
    """
    Load executed orders for today that were placed by the Wave Extractor algo.

    Filters orders where algo_source matches Wave Extractor algo tags:
    - "Gap-Odr_Manual" (manual gap order scripts)
    - "Gap-Odr_Auto" (auto gap order scripts)
    - "Scraper" (ticker_single_scraper_new.py)
    - "Unknown" (default tag when not explicitly set)

    Returns:
        list: List of Wave Extractor order dictionaries sorted by timestamp (newest first).
    """
    all_orders = load_todays_orders()
    wave_extractor_orders = [
        order for order in all_orders
        if order.get('algo_source', '') in WAVE_EXTRACTOR_ALGO_TAGS
    ]
    return wave_extractor_orders


def calculate_order_summary() -> dict:
    """
    Calculate summary statistics for PE and CE options from today's orders.
    
    Groups orders by symbol, calculates buy/sell counts and realized P&L.
    P&L = (sell_price - buy_price) * min(buy_qty, sell_qty) for matched pairs.
    
    Returns:
        dict: Contains 'ce_summary' and 'pe_summary' lists.
              Each entry has: symbol, expiry, buy_count, sell_count, 
              buy_value, sell_value, realized_pnl.
    """
    orders = load_todays_orders()
    
    # Group orders by symbol
    symbol_data = {}
    
    for order in orders:
        sym = order.get('symbol', '')
        option_type = order.get('option_type', 'OTHER')
        expiry = order.get('expiry', 'UNKNOWN')
        
        if sym not in symbol_data:
            symbol_data[sym] = {
                'symbol': sym,
                'expiry': expiry,
                'option_type': option_type,
                'buys': [],
                'sells': []
            }
        
        if order.get('transaction_type') == 'BUY':
            symbol_data[sym]['buys'].append({
                'price': float(order.get('price', 0)),
                'quantity': int(order.get('quantity', 0))
            })
        elif order.get('transaction_type') == 'SELL':
            symbol_data[sym]['sells'].append({
                'price': float(order.get('price', 0)),
                'quantity': int(order.get('quantity', 0))
            })
    
    # Calculate summary for each symbol
    ce_summary = []
    pe_summary = []
    
    for sym, data in symbol_data.items():
        total_buy_qty = sum(b['quantity'] for b in data['buys'])
        total_sell_qty = sum(s['quantity'] for s in data['sells'])
        total_buy_value = sum(b['price'] * b['quantity'] for b in data['buys'])
        total_sell_value = sum(s['price'] * s['quantity'] for s in data['sells'])
        
        # Calculate realized P&L for matched quantities
        matched_qty = min(total_buy_qty, total_sell_qty)
        if matched_qty > 0 and total_buy_qty > 0 and total_sell_qty > 0:
            avg_buy_price = total_buy_value / total_buy_qty
            avg_sell_price = total_sell_value / total_sell_qty
            realized_pnl = round((avg_sell_price - avg_buy_price) * matched_qty, 2)
        else:
            realized_pnl = None  # NA - no matched pairs
        
        summary_entry = {
            'symbol': sym,
            'expiry': data['expiry'],
            'buy_count': total_buy_qty,
            'sell_count': total_sell_qty,
            'avg_buy_price': round(total_buy_value / total_buy_qty, 2) if total_buy_qty > 0 else 0,
            'avg_sell_price': round(total_sell_value / total_sell_qty, 2) if total_sell_qty > 0 else 0,
            'realized_pnl': realized_pnl
        }
        
        if data['option_type'] == 'CE':
            ce_summary.append(summary_entry)
        elif data['option_type'] == 'PE':
            pe_summary.append(summary_entry)
    
    return {
        'ce_summary': ce_summary,
        'pe_summary': pe_summary
    }


def calculate_wave_extractor_order_summary() -> dict:
    """
    Calculate summary statistics for PE and CE options from Wave Extractor orders only.

    Groups Wave Extractor orders by symbol, calculates buy/sell counts and realized P&L.
    P&L = (sell_price - buy_price) * min(buy_qty, sell_qty) for matched pairs.

    Returns:
        dict: Contains 'ce_summary' and 'pe_summary' lists.
              Each entry has: symbol, expiry, buy_count, sell_count,
              buy_value, sell_value, realized_pnl.
    """
    orders = load_wave_extractor_orders()

    # Group orders by symbol
    symbol_data = {}

    for order in orders:
        sym = order.get('symbol', '')
        option_type = order.get('option_type', 'OTHER')
        expiry = order.get('expiry', 'UNKNOWN')

        if sym not in symbol_data:
            symbol_data[sym] = {
                'symbol': sym,
                'expiry': expiry,
                'option_type': option_type,
                'buys': [],
                'sells': []
            }

        if order.get('transaction_type') == 'BUY':
            symbol_data[sym]['buys'].append({
                'price': float(order.get('price', 0)),
                'quantity': int(order.get('quantity', 0))
            })
        elif order.get('transaction_type') == 'SELL':
            symbol_data[sym]['sells'].append({
                'price': float(order.get('price', 0)),
                'quantity': int(order.get('quantity', 0))
            })

    # Calculate summary for each symbol
    ce_summary = []
    pe_summary = []

    for sym, data in symbol_data.items():
        total_buy_qty = sum(b['quantity'] for b in data['buys'])
        total_sell_qty = sum(s['quantity'] for s in data['sells'])
        total_buy_value = sum(b['price'] * b['quantity'] for b in data['buys'])
        total_sell_value = sum(s['price'] * s['quantity'] for s in data['sells'])

        # Calculate realized P&L for matched quantities
        matched_qty = min(total_buy_qty, total_sell_qty)
        if matched_qty > 0 and total_buy_qty > 0 and total_sell_qty > 0:
            avg_buy_price = total_buy_value / total_buy_qty
            avg_sell_price = total_sell_value / total_sell_qty
            realized_pnl = round((avg_sell_price - avg_buy_price) * matched_qty, 2)
        else:
            realized_pnl = None  # NA - no matched pairs

        summary_entry = {
            'symbol': sym,
            'expiry': data['expiry'],
            'buy_count': total_buy_qty,
            'sell_count': total_sell_qty,
            'avg_buy_price': round(total_buy_value / total_buy_qty, 2) if total_buy_qty > 0 else 0,
            'avg_sell_price': round(total_sell_value / total_sell_qty, 2) if total_sell_qty > 0 else 0,
            'realized_pnl': realized_pnl
        }

        if data['option_type'] == 'CE':
            ce_summary.append(summary_entry)
        elif data['option_type'] == 'PE':
            pe_summary.append(summary_entry)

    # Sort by symbol name ascending
    ce_summary.sort(key=lambda x: x['symbol'])
    pe_summary.sort(key=lambda x: x['symbol'])

    return {
        'ce_summary': ce_summary,
        'pe_summary': pe_summary
    }


# Position Caching
cached_positions = None
last_positions_fetch_time = 0
positions_cache_ttl = 10  # seconds (scraper loop is 180s; fills invalidate via invalidate_positions_cache())

@retry_with_backoff(max_retries=20, base_delay=2.0, max_delay=300.0)
def _fetch_positions_with_retry():
    """
    Internal helper function to fetch positions with retry and backoff.

    Returns:
        dict: Positions data from Kite API.

    Raises:
        Exception: If all retry attempts fail.
    """
    return kite.positions()


def get_cached_positions():
    """
    Get positions with caching and automatic retry on network failures.

    Uses a cache with TTL to reduce API calls. On cache miss or stale cache,
    fetches fresh data using exponential backoff retry for network resilience.

    Returns:
        dict: Positions data with 'net' and 'day' keys.

    Raises:
        Exception: If API call fails after all retry attempts and no cache exists.
    """
    global cached_positions
    global last_positions_fetch_time
    
    current_time = time.time()
    if cached_positions is not None and (current_time - last_positions_fetch_time) < positions_cache_ttl:
        return cached_positions
    
    try:
        cached_positions = _fetch_positions_with_retry()
        last_positions_fetch_time = current_time
        return cached_positions
    except Exception as e:
        logging.error("All retry attempts failed for fetching positions: {}".format(e))
        # Return stale cache if available, else raise
        if cached_positions is not None:
            logging.warning("Returning STALE positions data due to API failure")
            return cached_positions
        raise


def invalidate_positions_cache():
    """Invalidate the positions cache to force a fresh fetch on next call."""
    global cached_positions
    logging.info("Invalidating positions cache to force fresh fetch on next call")
    cached_positions = None


def _invalidate_all_positions_caches() -> None:
    """Invalidate both the common_lib and MonitoredKite positions caches.

    Call this after any order execution or cancellation so that the next
    kite.positions() / get_cached_positions() call fetches live data instead
    of returning a stale pre-fill snapshot.
    """
    invalidate_positions_cache()
    try:
        from kite_api_monitor import invalidate_positions_cache as _inv_monitor
        _inv_monitor()
    except Exception as _exc:
        logging.warning("Could not invalidate MonitoredKite positions cache: %s", _exc)


# Order History Cache
# Maps order_id → (history_list, fetched_at_epoch, is_terminal)
# Terminal orders (COMPLETE/CANCELLED) are cached indefinitely; live orders use a short TTL.
_order_history_cache: dict[str, tuple[list, float, bool]] = {}
_ORDER_HISTORY_CACHE_TTL_SECONDS: float = 15.0
_order_history_cache_lock = threading.Lock()


def get_cached_order_history(order_id: str) -> list:
    """Fetch order history for a single order ID, using a per-order TTL cache.

    Terminal states (COMPLETE/CANCELLED) are cached forever because Zerodha order
    status is monotonically progressive and never reverts. Live/open orders are
    cached for _ORDER_HISTORY_CACHE_TTL_SECONDS to reduce API call frequency while
    still detecting fills within one polling cycle.

    Args:
        order_id: The Zerodha order ID string.

    Returns:
        List of order history dicts as returned by kite.order_history(). Empty list
        if the API returns nothing.
    """
    with _order_history_cache_lock:
        entry = _order_history_cache.get(order_id)
        if entry is not None:
            cached_history, fetched_at, is_terminal = entry
            if is_terminal:
                return cached_history
            if (time.time() - fetched_at) < _ORDER_HISTORY_CACHE_TTL_SECONDS:
                return cached_history

    history = kite.order_history(order_id)
    if not history:
        return history

    last_status = history[-1].get("status", "")
    is_terminal = last_status in ("COMPLETE", "CANCELLED", "REJECTED")

    with _order_history_cache_lock:
        _order_history_cache[order_id] = (history, time.time(), is_terminal)

    return history


def invalidate_order_history_cache(order_id: str | None = None) -> None:
    """Invalidate the order history cache for a specific order or entirely.

    Args:
        order_id: If provided, evicts only that order's cache entry. If None,
                  clears the entire cache (e.g., on WebSocket reconnect).
    """
    with _order_history_cache_lock:
        if order_id is None:
            _order_history_cache.clear()
            logging.info("order_history cache cleared (full flush)")
        elif order_id in _order_history_cache:
            del _order_history_cache[order_id]
            logging.debug("order_history cache invalidated for order %s", order_id)


def _invalidate_order_caches(order_id: str | None = None) -> None:
    """Invalidate both the positions cache and the order history cache.

    Single call site for all post-order-event cache cleanup. Adding a new cache
    in the future only requires updating this one function.

    Args:
        order_id: Specific order to evict from history cache. Pass None to
                  flush the entire history cache (e.g., on WebSocket reconnect).
    """
    _invalidate_all_positions_caches()
    invalidate_order_history_cache(order_id)


@retry_with_backoff(max_retries=20, base_delay=2.0, max_delay=300.0)
def get_quote_with_retry(symbol: str) -> dict:
    """
    Get quote data for a symbol with automatic retry on network failures.

    Wraps kite.quote() with exponential backoff retry logic to handle
    transient network issues like timeouts and DNS resolution failures.

    Args:
        symbol: The full symbol identifier (e.g., 'NFO:NIFTY24JAN23000CE' 
                or 'NSE:NIFTY 50').

    Returns:
        dict: Quote data from Kite API.

    Raises:
        Exception: If all retry attempts fail.
    """
    return kite.quote(symbol)


@retry_with_backoff(max_retries=20, base_delay=2.0, max_delay=300.0)
def get_orders_with_retry() -> list:
    """
    Get all orders with automatic retry on network failures.

    Wraps kite.orders() with exponential backoff retry logic to handle
    transient network issues like SSL errors, timeouts, and connection failures.

    Returns:
        list: List of order dictionaries from Kite API.

    Raises:
        Exception: If all retry attempts fail.
    """
    return kite.orders()


tick=0
symbol_type = ""


multiplier_scale = ""


def play_sound(times):
    for i in range(1,times):
        os.system('afplay /Users/vibhu/zd/pykiteconnect-master/vibhu/700187__trader_one__long-buzzer.wav')


def set_scraper_last_price(last_price):
    """
    Set the scraper's last known price and manage gap percentages.
    
    On first call (startup): calculates percentage from initial price and gap.
    On subsequent calls (order execution): recalculates gaps based on stored percentages.
    
    Args:
        last_price: The current/execution price to use as base.
    
    Returns:
        bool: True on success.
    """
    global scraper_last_price, buy_gap_percentage, sell_gap_percentage, buy_gap, sell_gap, initial_sell_gap
    
    # On first call (startup), calculate percentage from initial price and gap
    if scraper_last_price == -1 and last_price > 0:
        initial_sell_gap = sell_gap  # Store original for max comparison
        buy_gap_percentage = buy_gap / last_price
        sell_gap_percentage = sell_gap / last_price
        logging.info(f"Initialized gap percentages: buy={buy_gap_percentage*100:.2f}%, "
                     f"sell={sell_gap_percentage*100:.2f}% (from gaps {buy_gap}/{sell_gap} at price {last_price})")
        logging.info(f"Stored initial_sell_gap={initial_sell_gap} for max comparison")
    else:
        # On subsequent calls (order execution), recalculate gaps from percentage
        update_gaps_from_percentage(last_price)
    
    scraper_last_price = last_price
    return True


def calculate_gap_from_percentage(current_price: float, gap_percentage: float) -> float:
    """
    Calculate absolute gap from percentage of current price.
    
    Args:
        current_price: The current quote/execution price.
        gap_percentage: Gap as decimal (e.g., 0.27 for 27%).
    
    Returns:
        float: Absolute gap value rounded to 1 decimal place.
    """
    return round(current_price * gap_percentage, 1)


MIN_GAP_POINTS = 0.5


def _implied_volatility_for_gap(
    spot: float,
    strike: float,
    dte: float,
    option_ltp: float,
    option_type: str,
) -> float:
    """Back-solve implied volatility from an option's LTP, falling back to config volatility.

    Mirrors positions_lib._get_implied_volatility() (duplicated here rather than imported
    to avoid a circular import, since positions_lib already imports from common_lib).

    Args:
        spot: Current underlying spot price.
        strike: Option strike price.
        dte: Days to expiration (already clamped to be > 0).
        option_ltp: Last traded price of the option.
        option_type: "ce" for call, "pe" for put.

    Returns:
        float: Implied volatility as a percentage, or todays_volatility on failure.
    """
    if option_ltp < 0.5:
        return todays_volatility
    try:
        args = [spot, strike, interest_rate, dte]
        if option_type == "ce":
            iv_calc = mibian.BS(args, callPrice=float(option_ltp))
        else:
            iv_calc = mibian.BS(args, putPrice=float(option_ltp))
        implied_vol = iv_calc.impliedVolatility
        if implied_vol is None or not (1.0 < implied_vol < 200.0):
            return todays_volatility
        return implied_vol
    except (ValueError, ZeroDivisionError, AttributeError) as exc:
        logging.debug(
            "IV back-solve failed for strike %s type %s ltp %s: %s", strike, option_type, option_ltp, exc
        )
        return todays_volatility


def compute_greeks_based_gap(
    symbol: str,
    spot_price: float,
    option_ltp: float,
    up_points: float,
    down_points: float,
) -> dict[str, float]:
    """Compute buy/sell gap (absolute premium points) from a hypothetical underlying move.

    Reprices the option at spot+up_points and spot-down_points using IV back-solved from
    option_ltp (falling back to configfile.ini current_volatility on failure), holding IV
    and days-to-expiry fixed — same pattern as early_exit_lib.compute_leg().

    Args:
        symbol: Option trading symbol, e.g. "NIFTY25JUL25000CE".
        spot_price: Current underlying spot LTP.
        option_ltp: Current option premium LTP.
        up_points: Hypothetical upward move in the underlying (points, >= 0).
        down_points: Hypothetical downward move in the underlying (points, >= 0).

    Returns:
        dict[str, float]: Dict with "buy_gap" and "sell_gap", each rounded to 1 decimal
        and floored at MIN_GAP_POINTS.

    Raises:
        ValueError: If the symbol's instrument details cannot be resolved, or its option
            type is neither "ce" nor "pe".
    """
    instrument_details = get_instrument_details(symbol)
    strike = instrument_details["strike"]
    dte = max(instrument_details["days_to_expiry"], 0.0001)

    option_type = get_symbol_type(symbol)
    if option_type not in ("ce", "pe"):
        raise ValueError(f"Cannot compute greeks-based gap for non-option symbol {symbol}")

    implied_vol = _implied_volatility_for_gap(spot_price, strike, dte, option_ltp, option_type)

    bs_up = mibian.BS([spot_price + up_points, strike, interest_rate, dte], volatility=implied_vol)
    bs_down = mibian.BS([spot_price - down_points, strike, interest_rate, dte], volatility=implied_vol)

    if option_type == "ce":
        price_up = bs_up.callPrice
        price_down = bs_down.callPrice
        sell_gap = price_up - option_ltp
        buy_gap = option_ltp - price_down
    else:
        price_up = bs_up.putPrice
        price_down = bs_down.putPrice
        sell_gap = price_down - option_ltp
        buy_gap = option_ltp - price_up

    return {
        "buy_gap": max(round(buy_gap, 1), MIN_GAP_POINTS),
        "sell_gap": max(round(sell_gap, 1), MIN_GAP_POINTS),
    }


def update_gaps_from_percentage(execution_price: float) -> None:
    """
    Recalculate buy_gap and sell_gap based on stored percentages and new price.
    
    For sell_gap, uses max(initial_absolute, percentage) to prevent selling too cheap.
    Called when an order is executed to dynamically adjust gaps.
    
    Args:
        execution_price: The price at which the order was executed.
    """
    global buy_gap, sell_gap, buy_gap_percentage, sell_gap_percentage, initial_sell_gap
    
    if execution_price > 0 and buy_gap_percentage > 0:
        old_buy_gap = buy_gap
        old_sell_gap = sell_gap
        
        # Buy gap uses percentage directly
        buy_gap = calculate_gap_from_percentage(execution_price, buy_gap_percentage)
        
        # Sell gap uses max(initial, percentage) to prevent selling too cheap
        percentage_sell_gap = calculate_gap_from_percentage(execution_price, sell_gap_percentage)
        sell_gap = max(initial_sell_gap, percentage_sell_gap)
        
        logging.info(f"Updated gaps: buy_gap {old_buy_gap}->{buy_gap}, "
                     f"sell_gap {old_sell_gap}->{sell_gap} (max of initial {initial_sell_gap} and {percentage_sell_gap})")


def get_delta_multiplier(symbol_type: str, symbol: str) -> tuple[float, float]:
    """Calculate a continuous delta-based gap multiplier for buy/sell gaps.

    As portfolio delta deviates from zero, gaps widen progressively to make it
    harder (more expensive) to add further directional risk. The formula is:

        multiplier = min(max_multiplier, 1.0 + max(0, |delta| - soft_limit) / hard_limit)

    All three parameters (soft_limit, hard_limit, max_multiplier) are read from
    wave_extractor_config.json so they can be tuned without code changes.

    Args:
        symbol_type: 'ce' or 'pe' (lowercase).
        symbol: Trading symbol used to determine index and load config.

    Returns:
        Tuple (buy_gap_multiplier, sell_gap_multiplier). The multiplier > 1.0 is
        applied to the gap that adds risk in the direction of the current delta bias.
    """
    buy_mult, sell_mult = 1.0, 1.0

    try:
        if symbol.startswith("NIFTY") and not symbol.startswith("NIFTYBEES"):
            greeks = get_nifty_current_greeks(restrict_days=True)
            delta = greeks.get("delta", 0)
        elif symbol.startswith("BANKNIFTY"):
            greeks = get_bank_nifty_current_greeks()
            delta = greeks.get("delta", 0)
        elif symbol.startswith("SENSEX"):
            greeks = get_sensex_current_greeks(restrict_days=True)
            delta = greeks.get("delta", 0)
        else:
            logging.debug("No delta multiplier for symbol: %s", symbol)
            return (1.0, 1.0)

        cfg = load_wave_extractor_config(symbol).get("delta_multiplier", {})
        soft_limit     = float(cfg.get("soft_limit", 500))
        hard_limit     = float(cfg.get("hard_limit", 2000))
        max_multiplier = float(cfg.get("max_multiplier", 3.0))

        excess = max(0.0, abs(delta) - soft_limit)
        raw_mult = 1.0 + excess / hard_limit
        continuous_multiplier = min(max_multiplier, raw_mult)

        if delta < -soft_limit:
            # Portfolio is net short — make it harder to add more shorts
            if symbol_type == "ce":
                sell_mult = continuous_multiplier   # wider SELL gap on CE
            elif symbol_type == "pe":
                buy_mult = continuous_multiplier    # wider BUY gap on PE
        elif delta > soft_limit:
            # Portfolio is net long — make it harder to add more longs
            if symbol_type == "ce":
                buy_mult = continuous_multiplier    # wider BUY gap on CE
            elif symbol_type == "pe":
                sell_mult = continuous_multiplier   # wider SELL gap on PE

        logging.info(
            "[WE_MULTIPLIER] delta_multiplier | symbol=%s type=%s delta=%.2f "
            "soft_limit=%.0f hard_limit=%.0f max_mult=%.2f excess=%.2f "
            "raw_mult=%.4f capped_mult=%.4f → buy=%.4f sell=%.4f",
            symbol, symbol_type, delta,
            soft_limit, hard_limit, max_multiplier, excess,
            raw_mult, continuous_multiplier,
            buy_mult, sell_mult,
        )

    except Exception as exc:
        logging.error("Error calculating delta multiplier for %s: %s", symbol, exc)
        return (1.0, 1.0)

    return (buy_mult, sell_mult)


def set_globalDeltaCalculationDays(days):
    global delta_calculation_days
    delta_calculation_days = int(days)
    return True


def set_defaultGlobalDeltaCalculationDays():
    global delta_calculation_days
    delta_calculation_days = int(option_details['delta_calculation_days']) 
    return True



def set_typeOfProduct(typeOfProduct_str):
    global typeOfProduct 
    typeOfProduct = typeOfProduct_str 
    return True

def set_exchange(exchange_str):
    global exchange
    exchange = exchange_str
    return True



def set_tag(tag_str):
    global tag
    tag = tag_str
    return True


def set_on_order_update_solo_callback(function_name):
    global on_order_update_solo_callback
    on_order_update_solo_callback = function_name
    return True




def set_order_gtt_regular(val):
    global order_gtt_regular
    order_gtt_regular = val


def update_on_connect_update(update_function):
    global kws
    kws.on_connect = update_function


def update_on_order_update(update_function):
    global kws
    kws.on_order_update = update_function


def update_on_tick_function(function):
    global kws
    kws.on_ticks = function



def initilise(request_token_passed, symbol_passed, quantity_passed, buy_gap_passed, sell_gap_passed, multiplier_scale_passed, exchange_passed ="NFO"):
    global request_token
    global symbol 
    global quantity 

    global buy_quantity
    global sell_quantity


    global buy_gap 
    global sell_gap 
    global initial_positions
    global current_positions
    global symbol_type
    global multiplier_scale
    global kws
    global exchange
    global instrument_token


    initilise_basic(request_token_passed)


    symbol = symbol_passed
    quantity = quantity_passed

    if isinstance(quantity, int) == False and ":" in quantity: 
        buy_quantity = quantity.split(":")[0]
        sell_quantity = quantity.split(":")[1]
        quantity =sell_quantity 
    else:
        buy_quantity = quantity
        sell_quantity = quantity



    buy_gap = buy_gap_passed
    sell_gap = sell_gap_passed
    multiplier_scale = multiplier_scale_passed 
    exchange = exchange_passed

    # Fetch instrument token (only update if we get a valid value)
    try:
        quote = kite.quote(symbol)  # TODO there can be an exeption here.
        if symbol in quote and quote[symbol].get('instrument_token'):
            instrument_token = quote[symbol]['instrument_token']
            logging.info(f"Fetched instrument token for {symbol}: {instrument_token}")
        else:
            logging.error(f"Could not fetch quote for {symbol}")
    except Exception as e:
        logging.error(f"Error fetching instrument token: {e}")


    if initial_positions == -1:
        initial_positions = {"position": get_position_for_symbol(symbol)};


    current_positions = initial_positions

    symbol_type = get_symbol_type(symbol, exchange)
    print("Symbol Type is "+symbol_type)
    if symbol_type == "" or symbol_type == "unknown":
        print("Unidentified Symbol for Trade --- Please check")
        quit()
    
    # Start the order event processor thread
    # start_order_event_processor()
    
    logging.info("Initialization complete with event queue enabled")

# ... (rest of the file)








def get_symbol_type(symbol, exchange="NFO"):
    if exchange == "NSE":
        return "Stock"
    if symbol.endswith("PE"):
        return "pe"
    elif symbol.endswith("CE") :
        return "ce"
    elif symbol.endswith("FUT"):
        return "futures"
    else:
        return "unknown"


def get_orders():
    global orders
    return orders



def set_orders(new_orders):
    global orders
    orders = new_orders 
    return True


def subscribe_for_tick(items):
    """Subscribe to instrument tokens and store them for reconnection.

    Args:
        items: List of instrument tokens to subscribe.
    """
    global kws
    global _subscribed_tokens
    _subscribed_tokens = list(items)
    kws.subscribe(items)
    kws.set_mode(kws.MODE_FULL, items)


def merge_tick_subscription(new_tokens: list[int]) -> None:
    """Add tokens to the active WebSocket subscription without replacing existing ones.

    Computes the union of current and new tokens, then calls subscribe_for_tick()
    so that _subscribed_tokens stays consistent for reconnection recovery.
    If kws is None (Flask process, no ticker running) the merged list is stored
    for later — the reconnection watchdog will re-subscribe on connect.

    Args:
        new_tokens: Instrument tokens to add to the current subscription set.
    """
    global _subscribed_tokens
    merged = list(set(_subscribed_tokens) | set(new_tokens))
    if set(merged) == set(_subscribed_tokens):
        return  # nothing new to add
    if kws is None:
        _subscribed_tokens = merged  # persist for when kws eventually connects
        logging.info("merge_tick_subscription: kws not connected — stored %d tokens for later", len(merged))
        return
    subscribe_for_tick(merged)
    logging.info("merge_tick_subscription: subscribed to %d tokens total", len(merged))






def on_ticks(ws, ticks):  # noqa
    # Callback to receive ticks.
    logging.error("Ticks: {}".format(json.dumps(ticks, sort_keys=True, indent=4, default=str)))
    global tick
    tick=tick+1
    _broadcast_ticks(ticks)





def order_complete_solo(order_id, is_on_disconnect):
    delete_order_from_list(order_id, "complete")


def check_orders_solo():
    orders = get_orders();
    for order_id in orders.keys():
        if order_id == -1:
            continue
        order_status = get_cached_order_history(order_id)
        if not order_status:
            logging.warning("Empty order history for %s — skipping", order_id)
            continue
        order_final_status = order_status[-1]

        if order_final_status['status'] == "CANCELLED": #Need to change the complete code here to order the new set at which the order was complete
            delete_order_from_list(order_id)
            associated_order_id = orders[order_id]['associated_order']
            if associated_order_id != -1:
                cancel_order(kite.VARIETY_REGULAR, associated_order_id)
        else:
            if order_final_status['status'] == "COMPLETE":
                record_order_complete(order_id, orders[order_id]['transaction_type'])
                order_complete_solo(order_id, True)
        logging.info("New Order List - {}".format(orders))





def on_connect_solo(ws, response):  # noqa
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    # Set RELIANCE to tick in `full` mode.
    #ws.set_mode(ws.MODE_FULL, [14523906,13756162,21048834])
    logging.info("Connected Back to Solo")
    orders = get_orders();
    logging.info("Current Solo Order Conditions {}".format(orders))
    if len(orders) > 0:
        check_orders_solo()





def on_order_update_solo(ws, data):
    global orders
    global on_order_update_solo_callback
    logging.info("Order Update is called in Solo setup - {}".format(data))

    if data['order_id'] in orders.keys():
        order_id =  data['order_id']
        _invalidate_order_caches(order_id)
        if data['status'] == 'COMPLETE':
            print("Order Executed with Order Id  = "+str(order_id))
            record_order_complete(order_id, orders[order_id]['transaction_type'])
            order_complete_solo(order_id, False)
            on_order_update_solo_callback(order_id, "complete")
        else:
            print("Something else happened with the order")
            if data['status'] == 'CANCELLED':
                print("Someone Cancelled the Order -- ")
                delete_order_from_list(order_id)
                on_order_update_solo_callback(order_id, "cancelled")
            else:
                on_order_update_solo_callback(order_id, "other")
            logging.info("New Order List - {}".format(orders))
    else:
        print("Some unknown order was updated  -- "+data['transaction_type']+" -- "+data['tradingsymbol']+" -- "+str(data['price']))
    #logging.info("Order update : {}".format(data))
    print("Tick Number "+str(tick))






def record_order_complete(order_id, transaction_type):
    """
    Record that an order has completed and save to order history.

    Args:
        order_id: The order ID that completed.
        transaction_type: 'BUY' or 'SELL'.
    """
    global order_ids_completed
    global orders

    if order_id in order_ids_completed:
        return True

    if transaction_type in order_numbers_storage:
        order_numbers_storage[transaction_type] = order_numbers_storage[transaction_type] + 1
    else:
        order_numbers_storage[transaction_type] = 1

    # Save executed order to daily history file for dashboard tracking.
    # Only mark as completed after a successful save so a disk failure can be retried.
    if order_id in orders:
        order_data = orders[order_id]
        # Prefer explicit algo_source (global script tag) over the opaque
        # unique tracking tag.  For GTT-triggered orders where the script may
        # have already exited, attempt a persistent DB lookup as fallback.
        raw_algo = order_data.get('algo_source') or order_data.get('tag', '')
        resolved_algo = instrument_cache.get_gtt_algo_source(raw_algo) or raw_algo
        underlying_at_trigger = float(order_data.get('underlying_at_trigger', 0.0))
        underlying_index = order_data.get('underlying_index', '')
        underlying_at_execution = _read_current_spot_price(underlying_index) if underlying_index else 0.0
        saved = save_executed_order(
            symbol=order_data.get('symbol', 'UNKNOWN'),
            transaction_type=transaction_type,
            price=order_data.get('price', 0),
            quantity=order_data.get('quantity', 0),
            order_instrument_token=order_data.get('instrument_token', ''),
            order_segment=order_data.get('segment', ''),
            algo_source=resolved_algo,
            order_id=str(order_id),
            underlying_at_trigger=underlying_at_trigger,
            underlying_at_execution=underlying_at_execution,
            multiplier_info=order_data.get('multiplier_info', {}),
        )
        if not saved:
            logging.error(
                "record_order_complete: save_executed_order failed for order %s — "
                "will NOT mark as completed so it can be retried.",
                order_id,
            )
            return False

    order_ids_completed[order_id] = 1

    # Wave extractor fill cooldown tracking — only for Scraper algo orders
    if order_id in orders:
        order_data = orders[order_id]
        resolved_algo_src = (
            instrument_cache.get_gtt_algo_source(order_data.get("algo_source") or order_data.get("tag", ""))
            or order_data.get("algo_source", "")
        )
        if resolved_algo_src in WAVE_EXTRACTOR_ALGO_TAGS:
            order_symbol = order_data.get("symbol", symbol)
            we_cfg = load_wave_extractor_config(order_symbol)
            record_same_side_fill(transaction_type, order_symbol, we_cfg)

    return True









def check_orders():
    global orders
    
    # Use list(orders.keys()) to avoid runtime error if dictionary changes during iteration
    for order_id in list(orders.keys()):
        if order_id == -1:
            continue

        try:
            order_status = get_cached_order_history(order_id)
            if not order_status:
                logging.warning("Empty order history for %s — skipping", order_id)
                continue
            order_final_status = order_status[-1]
            logging.info("Order Status {}".format(order_final_status))
            
            if order_final_status['status'] == "CANCELLED":
                # Process strictly synchronously
                order_complete(order_id, True) # Implies cancelled
                
            elif order_final_status['status'] == "COMPLETE":
                # Use actual fill price from order history before recording
                fill_price = order_final_status.get('average_price') or order_final_status.get('price', 0)
                if fill_price and order_id in orders:
                    orders[order_id]['price'] = fill_price
                record_order_complete(order_id, orders[order_id]['transaction_type'])
                
                # Process synchronously
                order_complete(order_id, True, is_complete="complete")
                
        except Exception as e:
            logging.error("Error checking order status for {}: {}".format(order_id, e))

        logging.info("New Order List - {}".format(orders))








def on_connect(ws, response):  # noqa
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    # Set RELIANCE to tick in `full` mode.
    #ws.set_mode(ws.MODE_FULL, [14523906,13756162,21048834])
    logging.info("Connected Back")
    global orders
    logging.info("Current Order Conditions {}".format(orders))
    _invalidate_order_caches()  # flush all; we don't know what changed during disconnect
    if len(orders) > 0:
        check_orders()


def order_complete(order_id, is_on_disconnect, is_complete=""):
    """
    Internal function that actually processes order completion.
    """
    global orders
    global exceptNFBNF
    global typeOfProduct
    
    # Check if order still exists (might have been processed already)
    if order_id not in orders:
        logging.info("Order {} not found in orders dict, already processed".format(order_id))
        return
    
    # Check if this order was cancelled by our system logic
    is_system_cancelled = orders[order_id].get('system_cancelled', False)
    
    associated_order_id = orders[order_id]['associated_order']
    symbol = orders[order_id]['symbol']
    
    # Mark associated order as system-cancelled BEFORE deleting this order
    # This prevents the associated order from triggering place_duo_order when it gets cancelled
    if associated_order_id in orders and associated_order_id != -1:
        orders[associated_order_id]['system_cancelled'] = True
        logging.info("Marked associated order {} as system_cancelled".format(associated_order_id))
    
    if is_complete:
        set_scraper_last_price(orders[order_id]['price'])
        delete_order_from_list(order_id, is_complete)
    else:
        delete_order_from_list(order_id)
    
    # Cancel associated order
    try:
        if associated_order_id != -1:
            cancel_order(kite.VARIETY_REGULAR, associated_order_id)
    except Exception as e:
        logging.info("Error Cancelling Order: {}".format(e))
    
    # Only place new orders if this wasn't a system-triggered cancellation
    if not is_system_cancelled:
        # Cancel any stale GTT fallbacks for this symbol before placing fresh orders.
        # This prevents a previously-queued GTT (e.g. the other leg in the both-fail
        # scenario) from firing after the new duo is already live.
        cancel_all_pending_gtts_for_symbol(symbol)
        if is_market_open():
            place_duo_order(symbol, typeOfProduct, exceptNFBNF)
        else:
            logging.warning(
                "Market is closed — skipping place_duo_order() for %s after order "
                "cancellation. Exiting process to avoid stale GTT risk.",
                symbol,
            )
            sys.exit(0)
    else:
        logging.info("Skipping place_duo_order because order {} was system cancelled".format(order_id))
    
    printCurrentStatus()


def _process_order_update(order_id, data):
    """Process order update (price/quantity changes)"""
    global orders
    
    if order_id in orders:
        if 'price' in data:
            orders[order_id]['price'] = data['price']
        if 'quantity' in data:
            orders[order_id]['quantity'] = data['quantity']
        logging.info("Updated order {}: {}".format(order_id, orders[order_id]))



def print_duo_old_prices():
    global duo_old_buy_price
    global duo_old_sell_price

    print("-------- Old Buy Price Set was "+str(duo_old_buy_price))
    print("-------- Old Sell Price Set was "+str(duo_old_sell_price))

def set_old_duo_prices():
    global orders
    global duo_old_buy_price
    global duo_old_sell_price


    for order_id in orders.keys():
        if order_id == -1:
            continue

        if orders[order_id]['transaction_type'] == "BUY":
            duo_old_buy_price = orders[order_id]['price']
            continue


        if orders[order_id]['transaction_type'] == "SELL":
            duo_old_sell_price = orders[order_id]['price']
            continue

 
    



def on_order_update(ws, data):
    global orders
    global already_updating_order

    if already_updating_order > 0:
        return

    print("Creating Lock on Order Update")
    already_updating_order = 1

    try:

        print("Order Updated "+format(data))
        
        if data['order_id'] in orders.keys():
            order_id = data['order_id']
            _invalidate_order_caches(order_id)

            if data['status'] == 'COMPLETE':
                print("Order Executed")
                # Use the actual fill price (average_price) rather than the stored limit price
                fill_price = data.get('average_price') or data.get('price', 0)
                if fill_price:
                    orders[order_id]['price'] = fill_price
                record_order_complete(order_id, orders[order_id]['transaction_type'])

                # Process immediately
                order_complete(order_id, False, is_complete="complete")

                try:
                    from notifications.service import dispatch as _dispatch
                    _order = orders.get(order_id, {})
                    _dispatch(
                        "ORDER_EXECUTED",
                        title=f"Order Executed: {_order.get('symbol', order_id)}",
                        body=(
                            f"{_order.get('transaction_type', '')} "
                            f"{_order.get('quantity', '')} "
                            f"@ ₹{_order.get('price', '')}"
                        ),
                        metadata={
                            "order_id": order_id,
                            "symbol": _order.get("symbol", ""),
                            "transaction_type": _order.get("transaction_type", ""),
                            "quantity": _order.get("quantity", ""),
                            "price": _order.get("price", ""),
                        },
                    )
                except Exception as _notify_exc:
                    logging.warning("ORDER_EXECUTED notification failed: %s", _notify_exc)

            elif data['status'] == 'CANCELLED':
                print("Order Cancelled")

                # Process immediately
                order_complete(order_id, False)

                try:
                    from notifications.service import dispatch as _dispatch
                    _order = orders.get(order_id, {})
                    _dispatch(
                        "ORDER_CANCELLED",
                        title=f"Order Cancelled: {_order.get('symbol', order_id)}",
                        body=(
                            f"{_order.get('transaction_type', '')} "
                            f"{_order.get('quantity', '')} "
                            f"@ ₹{_order.get('price', '')} — Cancelled"
                        ),
                        metadata={
                            "order_id": order_id,
                            "symbol": _order.get("symbol", ""),
                            "transaction_type": _order.get("transaction_type", ""),
                            "quantity": _order.get("quantity", ""),
                            "price": _order.get("price", ""),
                        },
                    )
                except Exception as _notify_exc:
                    logging.warning("ORDER_CANCELLED notification failed: %s", _notify_exc)
                
            elif data['status'] == 'OPEN':
                incoming_price = data.get('price')
                if order_id in _pending_auto_reprice_ids:
                    # This OPEN callback is the echo of our own kite.modify_order() call; clear the flag.
                    _pending_auto_reprice_ids.discard(order_id)
                elif incoming_price and incoming_price != orders[order_id].get('price'):
                    # Price changed by someone other than the auto-repricer — treat as manual override.
                    orders[order_id]['manually_overridden'] = True
                    logging.info(
                        "[WE_REPRICE] Order %s manually overridden to %.1f — auto-repricing suspended until fill",
                        order_id,
                        incoming_price,
                    )
                _process_order_update(order_id, data)
            
            set_old_duo_prices()
        else:
            # Check if this is a GTT-triggered order (Zerodha echoes the same tag)
            gtt_tag = data.get('tag', '')
            if gtt_tag and gtt_tag in pending_gtt_fallbacks:
                new_order_id = data['order_id']
                status = data.get('status', '')
                if status in ('OPEN', 'TRIGGER PENDING'):
                    # GTT fired, order live but not yet filled. Register it so the
                    # subsequent COMPLETE update is handled by the normal top-of-function path.
                    register_gtt_triggered_order(gtt_tag, data)
                elif status == 'COMPLETE':
                    # Race condition: COMPLETE arrived before OPEN (or OPEN was never sent).
                    # Register and immediately process completion so sibling is cancelled
                    # and place_duo_order() is called. Without this, order_complete() would
                    # never be triggered and place_duo_order() would never run.
                    _invalidate_order_caches(new_order_id)
                    register_gtt_triggered_order(gtt_tag, data)
                    # Capture actual fill price before recording
                    fill_price = data.get('average_price') or data.get('price', 0)
                    if new_order_id in orders:
                        if fill_price:
                            orders[new_order_id]['price'] = fill_price
                        record_order_complete(new_order_id, orders[new_order_id]['transaction_type'])
                        order_complete(new_order_id, False, is_complete="complete")  # NOT order_complete_solo
                    else:
                        logging.error(
                            "GTT COMPLETE for tag %s order %s but not in orders dict — skipping",
                            gtt_tag, new_order_id,
                        )
                elif status == 'CANCELLED':
                    _invalidate_order_caches(data['order_id'])
                    pending_gtt_fallbacks.pop(gtt_tag, None)
                    logging.warning(f"GTT fallback order for tag {gtt_tag} was CANCELLED externally")
            else:
                print("Some unknown order was updated  -- "+data['transaction_type']+" -- "+data['tradingsymbol']+" -- "+str(data['price']))

        #print("Tick Number "+str(tick))

        # Delayed refresh: Kite REST API lags WebSocket by ~3-5s; re-reading positions
        # immediately gives stale data. Also catches external/manual order fills that
        # bypass the orders dict check above.
        if data.get('status') == 'COMPLETE':
            def _refresh_status_after_fill() -> None:
                try:
                    time.sleep(5)
                    invalidate_positions_cache()
                    write_status_to_file()
                except Exception as _refresh_exc:
                    logging.warning("Post-fill status refresh failed: %s", _refresh_exc)
            threading.Thread(target=_refresh_status_after_fill, daemon=True).start()

    except Exception as e:
        logging.info("Error in on_order_update: {}".format(e))
    finally:
        print("Freeing Lock on Order Update")
        already_updating_order = 0




def cancel_order(variety, order_id):

    count_try = 0
    while True: 
        count_try = count_try+1
        try:
            delete_order_from_list(order_id)
            status = kite.cancel_order(variety=variety, order_id=order_id,parent_order_id=None)
            _invalidate_order_caches(order_id)
            logging.info("Order Cancelled ID is: {}".format(status))
            return
        except Exception as e:
            logging.info("Order cancelation failed: {}".format(e))
            if count_try >=5:
                return
    
def delete_gtt(trigger_id):
    try:
        delete_gtt = kite.delete_gtt(trigger_id)
    except Exception as e:
        print("Trigger Id not found "+str(trigger_id))
    print("Deleted GTT with TriggerId "+str(trigger_id))
    return


# ---------------------------------------------------------------------------
# GTT Fallback helpers
# ---------------------------------------------------------------------------

def is_any_gtt_pending():
    """Return True if any GTT fallback orders are pending."""
    return len(pending_gtt_fallbacks) > 0


def register_gtt_triggered_order(tag_key, order_data):
    """
    When a GTT fires, Zerodha creates a new regular order with a new order_id
    and echoes the same tag we set. This function plugs that new order into
    the `orders` tracking dict with the correct sibling linkage, then removes
    the GTT from pending_gtt_fallbacks. After this point the existing
    order_complete() machinery handles everything (cancel sibling, place_duo_order).
    """
    global orders, pending_gtt_fallbacks

    metadata = pending_gtt_fallbacks.get(tag_key)
    if metadata is None:
        return

    new_order_id = order_data['order_id']
    sibling_order_id = metadata['associated_regular_order_id']  # -1 if sibling is also a GTT

    # Register the new order in the tracking dict
    add_order_to_list(
        new_order_id,
        metadata['price'],
        metadata['quantity'],
        metadata['transaction_type'],
        metadata['symbol'],
        sibling_order_id if sibling_order_id != -1 else "-1",
        tag_used=tag_key
    )

    # Update the sibling to point back to this new order_id
    if sibling_order_id != -1 and sibling_order_id in orders:
        orders[sibling_order_id]['associated_order'] = new_order_id

    # Remove from GTT registry
    del pending_gtt_fallbacks[tag_key]

    logging.warning(
        f"GTT/ATO fired and registered: {metadata['symbol']} "
        f"{metadata['transaction_type']} qty={metadata['quantity']} "
        f"price={metadata['price']} new_order_id={new_order_id} sibling={sibling_order_id}"
    )


def check_gtt_orders():
    """
    Poll kite.orders() to find GTT-spawned orders whose tag matches a key in
    pending_gtt_fallbacks. Handles the reconnect/polling path where the WebSocket
    missed the update, including the race condition where the order is already
    COMPLETE before we see it for the first time.
    """
    global orders, pending_gtt_fallbacks

    if not pending_gtt_fallbacks:
        return

    try:
        live_orders = kite.orders()
    except Exception as e:
        logging.error(f"check_gtt_orders: failed to fetch orders: {e}")
        return

    for order in live_orders:
        tag_key = order.get('tag', '')
        if tag_key not in pending_gtt_fallbacks:
            continue

        new_order_id = order['order_id']
        if new_order_id in orders:
            continue  # already registered

        status = order.get('status', '')
        if status in ('OPEN', 'TRIGGER PENDING'):
            # GTT fired, order live but not yet filled — register it
            register_gtt_triggered_order(tag_key, order)

        elif status == 'COMPLETE':
            # Race condition: order already filled before we saw the OPEN update.
            # Register and immediately process completion so sibling is cancelled
            # and place_duo_order() is called.
            _invalidate_order_caches(new_order_id)
            register_gtt_triggered_order(tag_key, order)
            # Capture actual fill price from the polled order before recording
            fill_price = order.get('average_price') or order.get('price', 0)
            if fill_price and new_order_id in orders:
                orders[new_order_id]['price'] = fill_price
            record_order_complete(new_order_id, orders[new_order_id]['transaction_type'])
            order_complete(new_order_id, True, is_complete="complete")

        elif status == 'CANCELLED':
            _invalidate_order_caches(new_order_id)
            pending_gtt_fallbacks.pop(tag_key, None)
            logging.warning(f"GTT fallback order for tag {tag_key} was CANCELLED externally")


def cancel_all_pending_gtts_for_symbol(sym):
    """
    Cancel every GTT in pending_gtt_fallbacks that belongs to sym.
    Called from order_complete() after place_duo_order() so stale GTTs
    (e.g. the un-fired leg in the both-fail scenario) don't fire later
    and create ghost orders.
    """
    global pending_gtt_fallbacks

    stale_tags = [t for t, meta in pending_gtt_fallbacks.items() if meta.get('symbol') == sym]
    for tag_key in stale_tags:
        meta = pending_gtt_fallbacks.get(tag_key)
        if meta is None:
            continue
        trigger_id = meta.get('trigger_id')
        if trigger_id and trigger_id != -1:
            delete_gtt(trigger_id)
        pending_gtt_fallbacks.pop(tag_key, None)
        logging.warning(f"Cancelled stale GTT fallback {tag_key} (trigger_id={trigger_id}) for {sym}")


# ---------------------------------------------------------------------------


def get_gtts():
    all_gtts = kite.get_gtts()
    print("Got Gtts")
    #logging.error("single leg gtt order trigger_id : {}".format(all_gtts))
    return all_gtts

def place_gtt_order(tradingsymbol, variety, transactionType, exchange, product, price, quantity, tag_recv="Unknown"):
    global tag

    if not live_trading_enabled:
        _dry_run_block("place_gtt_order", tradingsymbol,
                       f"{transactionType} {quantity} @ {price} ({exchange}/{product})")
        return -1

    if tag_recv=="Unknown":
        tag_recv = tag


    #global order_numbers_storage
    #if transactionType in order_numbers_storage.keys():
    #    num = order_numbers_storage[transactionType]
    #else:
    #    num = 0

    if price <= 0:  #If price of the buy or sell is set to be less than or equal to 0 setting it as 0.05 paise
        price = 0.05

    price = convert_price_to_paise(price)  # round to nearest 0.05 tick (NSE/NFO requirement)

    try:
        print("In Place GTT")

        exchange_Symbol = exchange+":"+tradingsymbol;
        quote_data = kite.quote(exchange_Symbol)
        last_price = quote_data[exchange_Symbol]['last_price']

        order_single = [{
            "exchange":exchange,
            "tradingsymbol": tradingsymbol,
            "transaction_type": transactionType,
            "quantity": quantity,
            "order_type": "LIMIT",
            "product": product,
            "price": price,
            "tag": tag_recv
        }]
        single_gtt = kite.place_gtt(trigger_type=kite.GTT_TYPE_SINGLE, tradingsymbol=tradingsymbol, exchange=exchange, trigger_values=[price], last_price=last_price, orders=order_single)
        #logging.info("single leg gtt order trigger_id : {}".format(single_gtt))
        return single_gtt['trigger_id']
    except Exception as e:
        logging.info("Error placing single leg gtt order: {}".format(e))
        return -1




def place_order_market(tradingsymbol, variety, transactionType, exchange, product, quantity, tag_recv="Unknown"):
    global order_gtt_regular

    global tag

    if not live_trading_enabled:
        _dry_run_block("place_order_market", tradingsymbol,
                       f"{transactionType} {quantity} MARKET ({exchange}/{product})")
        return -1

    if tag_recv=="Unknown":
        tag_recv = tag

    print("Going to Place Market Order for the symbol -- "+tradingsymbol)

    if order_gtt_regular == "gtt":
        #This needs to be shifted to Market GTT
        logging.info("Can't make GTT order at Market Price -- Quitting -- ")
        quit()
        return gtt_order_details

    #global order_numbers_storage

    #if transactionType in order_numbers_storage.keys():
    #    num = order_numbers_storage[transactionType]
    #else:
    #    num = 0

    protection_percent = kite.MARKET_PROTECTION_AUTO
    logging.info(f"Applying market protection: AUTO for {tradingsymbol}")

    count_try = 0
    while True: 
        count_try = count_try+1
        try:
            order_id = kite.place_order(
                variety=variety,
                exchange=exchange,
                tradingsymbol=tradingsymbol,
                transaction_type=transactionType,
                quantity=quantity,
                product=product,
                order_type=kite.ORDER_TYPE_MARKET,
                tag=tag_recv,
                market_protection=protection_percent
            )
            logging.info("Order placed. ID is: {}".format(order_id))
            return order_id
        except Exception as e:
            logging.info("Order placement failed: {}".format(e))
            if count_try >= 5:
                return -1


def update_order_qty(order_id, new_qty, variety):
    try:
        kite.modify_order(variety, 
            order_id, 
            quantity = new_qty

        )
        return True
    except Exception as e:
        logging.info("Order Modification Failed: {}".format(e))
        return False





def update_order_price(order_id, new_price, variety):
    try:
        kite.modify_order(variety, 
            order_id, 
            price = new_price,
            trigger_price = new_price
        )
        return True
    except Exception as e:
        logging.info("Order Modification Failed: {}".format(e))
        return False



def convert_price_to_paise(price):
    return int(price*20)/20



def place_sl_order(tradingsymbol, variety, transactionType, exchange, product, price, trigger_price, quantity, tag_recv="Unknown"):


    global tag

    if not live_trading_enabled:
        _dry_run_block("place_sl_order", tradingsymbol,
                       f"{transactionType} {quantity} SL @ {price}/trig {trigger_price} ({exchange}/{product})")
        return -1

    if tag_recv=="Unknown":
        tag_recv = tag

    print("Price is "+str(price))
    print("Trigger Price is "+str(trigger_price))

    price = convert_price_to_paise(price)
    print("Price sent = "+str(price))
    trigger_price = convert_price_to_paise(trigger_price)
    print("Price sent = "+str(trigger_price))

    if price <= 0:  #If price of the buy or sell is set to be less than or equal to 0 setting it as 0.05 paise
        price = 0.05

    try:
        order_id = kite.place_order(
            variety=variety,
            exchange=exchange,
            tradingsymbol=tradingsymbol,
            transaction_type=transactionType,
            quantity=quantity,
            product=product,
            price=price,
            trigger_price=trigger_price,
            order_type=kite.ORDER_TYPE_SL,
            tag=tag_recv
        )
        logging.info("Order placed. ID is: {}".format(order_id))
        return order_id
    except Exception as e:
        logging.info("Order placement failed: {}".format(e))
        return -1






def place_order(tradingsymbol, variety, transactionType, exchange, product, price, quantity, tag_recv="Unknown"):
    global order_gtt_regular
    global tag

    if not live_trading_enabled:
        _dry_run_block("place_order", tradingsymbol,
                       f"{transactionType} {quantity} LIMIT @ {price} ({exchange}/{product})")
        return -1

    if tag_recv=="Unknown":
        tag_recv = tag


    if order_gtt_regular == "gtt":
        gtt_order_details =  place_gtt_order(tradingsymbol, variety, transactionType, exchange, product, price, quantity, tag_recv)
        logging.info("GTT Order Details are = {}".format(gtt_order_details))
        return gtt_order_details
    
    #global order_numbers_storage

    #if transactionType in order_numbers_storage.keys():
    #    num = order_numbers_storage[transactionType]
    #else:
    #    num = 0

    if price <= 0:  #If price of the buy or sell is set to be less than or equal to 0 setting it as 0.05 paise
        logging.warning(f"Price for {tradingsymbol} was {price}, clamping to 0.05. "
                        f"This may indicate uninitialized price variables.")
        price = 0.05

    price = convert_price_to_paise(price)  # round to nearest 0.05 tick (NSE/NFO requirement)

    count_try = 0
    while True: 
        count_try = count_try+1
        try:
            order_id = kite.place_order(
                variety=variety,
                exchange=exchange,
                tradingsymbol=tradingsymbol,
                transaction_type=transactionType,
                quantity=quantity,
                product=product,
                price=price,
                order_type=kite.ORDER_TYPE_LIMIT,
                tag=tag_recv
            )
        
            logging.info("Order placed. ID is: {}".format(order_id))
            return order_id
        except Exception as e:
            logging.info("Order placement failed: {}".format(e))
            if count_try >= 5:
                return -1







def place_gtt_test():
    try:
        print("In Place GTT")
        order_single = [{
            "exchange":"NSE",
            "tradingsymbol": "SBIN",
            "transaction_type": kite.TRANSACTION_TYPE_BUY,
            "quantity": 1,
            "order_type": "LIMIT",
            "product": "CNC",
            "price": 570,
        }]
        #single_gtt = kite.place_gtt(trigger_type=kite.GTT_TYPE_SINGLE, tradingsymbol="SBIN", exchange="NSE", trigger_values=[570], last_price=473, orders=order_single)
        logging.info("single leg gtt order trigger_id : ")
    except Exception as e:
        logging.info("Error placing single leg gtt order: {}".format(e))


def add_order_to_list(order_id, price, quantity, transaction_type, symbol, associated_order_id, tag_used=None, instrument_token=None, segment=None):
    """
    Add an order to the tracking list.
    
    Args:
        order_id: The order ID from Kite.
        price: Execution price.
        quantity: Order quantity.
        transaction_type: BUY or SELL.
        symbol: Trading symbol.
        associated_order_id: Related order ID for pairs.
        tag_used: Optional order tag.
        instrument_token: Optional instrument token for chart links.
        segment: Optional segment (e.g., 'BFO-OPT', 'NFO-OPT') for chart links.
    """
    global orders, tag
    now = get_ist_now()
    orders[order_id] = {}
    if tag_used:
        orders[order_id]['tag'] = tag_used
    # Store the global script algo tag separately so record_order_complete()
    # saves a meaningful source name rather than the opaque unique tracking tag.
    orders[order_id]['algo_source'] = tag or "Unknown"
    orders[order_id]['price'] = price
    orders[order_id]['quantity'] = quantity
    orders[order_id]['transaction_type'] = transaction_type  #kite.TRANSACTION_TYPE_SELL
    orders[order_id]['symbol'] = symbol 
    orders[order_id]['associated_order'] = associated_order_id
    orders[order_id]['hour'] = now.hour 
    orders[order_id]['min'] = now.minute 
    orders[order_id]['second'] = now.minute 
    orders[order_id]['time'] = str(now.hour)+":"+str(now.minute)+":"+str(now.second)
    
    # Store instrument details for chart links
    if instrument_token:
        orders[order_id]['instrument_token'] = str(instrument_token)
    if segment:
        orders[order_id]['segment'] = segment

    orders[order_id]['underlying_at_trigger'] = _pending_trigger_spot
    orders[order_id]['underlying_index'] = _pending_trigger_index

    logging.error("Current Orders List: {}".format(orders))
    
    # Save to persistence
    save_script_order(order_id)


def delete_order_from_list(order_id, transaction_status="notComplete"):
    global orders
    if order_id in orders:
        transaction_type = orders[order_id]['transaction_type']
        print("Transaction Status Type "+transaction_status+ " Transaction Type = "+orders[order_id]['transaction_type']);
        del orders[order_id]


def get_todays_current_positions():  #This talks about the intraday trades data

    try:
        live_positions = get_cached_positions()
    except Exception as e:
        # If centralized fetch failed twice, we might still want to sleep and try distinct fetch or just fail
        time.sleep(randint(3,5))
        live_positions = kite.positions()
    #net_position =  live_positions['net']
    todays_position = live_positions['day'] ## This is for intraday perspective
    return_position = 0
    for position in todays_position:
        if position['tradingsymbol'] == symbol:
            print("Symbol "+symbol)
            return_position = position['quantity']

    symbol_status = {"position": return_position}
    logging.info("SYMBOL STATUS IS ----------------- {}".format(symbol_status))
    return symbol_status


def get_all_instruments():
    instruments = kite.instruments()
    return instruments 


def get_all_fut_opt_instruments():
    global all_instruments
    global token_symbol_map
    all_instruments = instrument_cache.get_all_fut_opt_instruments()
    token_symbol_map = {v['tradingsymbol']: k for k, v in all_instruments.items()}
    return


def get_symbol_quote(symbol):
    try:
        quote_data = kite.quote(symbol)
    except Exception as e:
        time.sleep(randint(3,5))
        quote_data = {}
        quote_data[symbol] = get_symbol_quote(symbol)
    return quote_data[symbol]


def get_bank_nifty_current_quote() -> dict:
    return _get_index_quote_cached(bank_nifty_symbol)

def get_gift_nifty_current_quote():
    global gift_nifty_symbol
    quote_data = kite.quote(gift_nifty_symbol)
    logging.error("Gift Nifty Value  : {}".format(quote_data))
    return quote_data[gift_nifty_symbol]


def get_nifty_historical_data(from_date, to_date, interval):
    nifty_quote = get_nifty_current_quote()
    nifty_instrument_token = nifty_quote['instrument_token']
    #logging.error("Nifty Token = "+str(nifty_instrument_token))
    historical_data = kite.historical_data(nifty_instrument_token, from_date, to_date, interval) 
    return historical_data


def get_nifty_current_quote() -> dict:
    return _get_index_quote_cached(nifty_symbol)



def get_bank_nifty_current_greeks(restrict_days=True):
    global all_instruments
    global todays_volatility
    global interest_rate 
    global delta_calculation_days 
    global exchange

    try:
        live_positions = get_cached_positions()
    except Exception as e:
        # Fallback manual fetch
        time.sleep(randint(1,5))
        live_positions = kite.positions()
    net_positions = live_positions['net'] # For overall Positions Data
    total_delta = 0
    futures_delta = 0;
    bank_nifty_quote_data = get_bank_nifty_current_quote()
    bank_nifty_last_price = bank_nifty_quote_data['last_price']
    
    expiry_map = {}

    #logging.info("Current Positions are ----------------- {}".format(live_positions))
    for position in net_positions:
        tradingtoken = position['instrument_token']
        tradingsymbol = position['tradingsymbol']
        if tradingsymbol.startswith("BANKNIFTY") is False:
            continue
        if tradingtoken not in all_instruments:
            continue
        instrument_details = all_instruments[tradingtoken]
        
        expiry_date = str(instrument_details['expiry'])

        if instrument_details['segment'] != exchange+"-OPT":  #If not an option check see if futures otherwise you can continue
            if instrument_details['segment'] == exchange+"-FUT": #Check if it is futures and add full quantity to delta
                total_delta = total_delta + position['quantity']
                futures_delta = futures_delta + position['quantity']
                expiry_map[expiry_date] = expiry_map.get(expiry_date, 0) + position['quantity']
            continue
        if instrument_details['days_to_expiry'] > delta_calculation_days and restrict_days:   # This will ignore any option instrument which is more than 10 working days ahead.
            continue

        #logging.info("Current Positions are ----------------- {}".format(instrument_details))
        print("BANK NIFTY Last Price = "+str(bank_nifty_quote_data));
        print("BANKNIFTY Strike = "+str(instrument_details['strike']));
        print("BANKNIFTY Days to Expiry = "+str(instrument_details['days_to_expiry']))
        c = mibian.BS([bank_nifty_last_price, instrument_details['strike'], interest_rate, instrument_details['days_to_expiry']], volatility=todays_volatility)
        #mibian.BS([Current_price, instrument_strike_price, Interest Rate, Days_to_Expiry], volatility ..... ..... ) 
        computed_delta = 0
        if instrument_details['instrument_type'] == "CE":
            computed_delta = c.callDelta*position['quantity']
        elif instrument_details['instrument_type'] == "PE":
            computed_delta = c.putDelta*position['quantity']
        total_delta = total_delta+ computed_delta
        
        expiry_map[expiry_date] = expiry_map.get(expiry_date, 0) + computed_delta
        
        print("Added Value to BANK NIFTY Delta "+position['tradingsymbol']+"  -- Delta -- "+str(computed_delta));
        
    print("BANK NIFTY -- Final Delta --");
    print(total_delta);
    greeks = {}
    greeks['delta'] = total_delta
    greeks['expiry_delta'] = expiry_map
    return greeks

def extract_level(tags_arr):
    return_level =  10000 
    for tag in tags_arr:
        if "Level" in tag:
            temp_level = int(tag.replace("Level ", ""))
            if temp_level < return_level:
                return_level = temp_level

    if return_level == 10000:
        return -1
    else:
        return return_level
            


def get_position_for_symbol(symbol: str) -> int:
    """
    Get the current position quantity for a given trading symbol.

    Fetches positions using cached data with automatic retry on network errors.
    Falls back to direct API call with retry if cache fails.

    Args:
        symbol: The trading symbol to look up (e.g., 'NIFTY24JAN23000CE').

    Returns:
        int: The quantity of the position. Positive for long, negative for short,
             0 if no position exists.

    Raises:
        Exception: If both cache and direct API calls fail after all retries.
    """
    try:
        live_positions = get_cached_positions()
    except Exception as e:
        logging.warning(f"get_cached_positions failed, trying direct fetch: {e}")
        # Fallback to direct call with retry
        live_positions = _fetch_positions_with_retry()
    
    net_positions = live_positions['net']  # For overall Positions Data
    for position in net_positions:
        tradingsymbol = position['tradingsymbol']
        tradingtoken = position['instrument_token']
        print(" Trading Symbol is " + tradingsymbol)
        print(" Trading Token is " + str(tradingtoken))
        if tradingsymbol == symbol:
            logging.info("Current Positions for Symbol ------ {} ".format(position))
            return position["quantity"]
    return 0

def get_instrument_details(symbol):
    global all_instruments
    global token_symbol_map
    
    # Check in-memory F&O cache first
    if symbol in token_symbol_map:
        return all_instruments[token_symbol_map[symbol]]
        
    # Fallback/Optimization: Check local SQLite database directly
    details = instrument_cache.get_instrument(symbol)
    if details:
        # Calculate days_to_expiry if needed since SQLite stores raw expiry date
        if 'expiry' in details and details['expiry']:
            today = datetime.date.today()
            expiry_date = datetime.datetime.strptime(details['expiry'], '%Y-%m-%d').date()
            details['days_to_expiry'] = int(np.busday_count(today, expiry_date) + 1)
        return details
        
    raise ValueError(f"Instrument {symbol} not found in cache or database")
    

def get_next_week_nifty_weekly_expiry_initials():
    global exchange

    live_positions = get_cached_positions()
    net_positions = live_positions['net']
    for position in net_positions:
        tradingtoken = position['instrument_token']
        tradingsymbol = position['tradingsymbol']
        if tradingsymbol.startswith("NIFTY") is False:
            continue
        if tradingtoken not in all_instruments:
            continue
        instrument_details = all_instruments[tradingtoken]
        if instrument_details['segment'] != exchange+"-OPT":
            continue
        if instrument_details['days_to_expiry'] < 6 or instrument_details['days_to_expiry'] > 13:
            continue

        logging.info("Position Passed = {}".format(position))
        latest_nifty_weekly_expiry_initials = tradingsymbol[:10]
        return latest_nifty_weekly_expiry_initials 




def get_latest_nifty_weekly_expiry_initials():
    global exchange
    live_positions = get_cached_positions()
    net_positions = live_positions['net'] # For overall Positions Data
    for position in net_positions:
        tradingtoken = position['instrument_token']
        tradingsymbol = position['tradingsymbol']
        if tradingsymbol.startswith("NIFTY") is False:
            continue
        if tradingtoken not in all_instruments:
            continue
        instrument_details = all_instruments[tradingtoken]
        if instrument_details['segment'] != exchange+"-OPT":
            continue
        if instrument_details['days_to_expiry'] > 6:
            continue

        logging.info("Position Passed = {}".format(position))
        latest_nifty_weekly_expiry_initials = tradingsymbol[:10]
        return latest_nifty_weekly_expiry_initials 

        
    
def get_geeks(base_price, strike, interest_rate, days_to_expiry, volatility):
    c = mibian.BS([base_price, strike, interest_rate, days_to_expiry], volatility=todays_volatility)
    print(c)
    logging.error("Geeks Value = {}".format(c))
    return c




def get_delta_for_nifty_symbol(instrument_details):
    global nifty_lot_size 
    nifty_quote_data = get_nifty_current_quote()
    nifty_last_price = nifty_quote_data['last_price']
    c = mibian.BS([nifty_last_price, instrument_details['strike'], interest_rate, instrument_details['days_to_expiry']], volatility=todays_volatility)
    if instrument_details['instrument_type'] == "CE":
        computed_delta = c.callDelta*nifty_lot_size
    elif instrument_details['instrument_type'] == "PE":
        computed_delta = c.putDelta*nifty_lot_size
    return computed_delta
    




def get_nifty_current_greeks(restrict_days = True):
    global all_instruments
    global todays_volatility
    global interest_rate 
    global delta_calculation_days 
    global exchange

    print("Global Delta Calculation days are "+str(delta_calculation_days))

    try:
        live_positions = get_cached_positions()
    except Exception:
         live_positions = kite.positions()
    net_positions = live_positions['net'] # For overall Positions Data
    total_delta = 0
    futures_delta = 0;
    nifty_quote_data = get_nifty_current_quote()
    nifty_last_price = nifty_quote_data['last_price']

    total_ce=0
    total_pe=0

    total_positive_ce=0
    total_negative_ce=0

    total_positive_pe=0
    total_negative_pe=0

    total_ce_delta=0
    total_pe_delta=0
    
    expiry_map = {}

    #logging.info("Current Positions are ----------------- {}".format(live_positions))
    for position in net_positions:
        tradingtoken = position['instrument_token']
        tradingsymbol = position['tradingsymbol']
        if tradingsymbol.startswith("NIFTY") is False:
            continue
        if tradingtoken not in all_instruments:
            continue
        instrument_details = all_instruments[tradingtoken]
        
        expiry_date = str(instrument_details['expiry'])
        
        if instrument_details['segment'] != exchange+"-OPT":  #If not an option check see if futures otherwise you can continue
            if instrument_details['segment'] == exchange+"-FUT": #Check if it is futures and add full quantity to delta
                total_delta = total_delta + position['quantity']
                futures_delta = futures_delta + position['quantity']
                expiry_map[expiry_date] = expiry_map.get(expiry_date, 0) + position['quantity']
            continue
        if instrument_details['days_to_expiry'] > delta_calculation_days and restrict_days:   # This will ignore any option instrument which is more than 10 working days ahead.
            continue

        #logging.info("Current Positions are ----------------- {}".format(instrument_details))
        print("NIFTY Last Price = "+str(nifty_last_price));
        print("Strike = "+str(instrument_details['strike']));
        print("Days to Expiry = "+str(instrument_details['days_to_expiry']))
        c = mibian.BS([nifty_last_price, instrument_details['strike'], interest_rate, instrument_details['days_to_expiry']], volatility=todays_volatility)
        #mibian.BS([Current_price, instrument_strike_price, Interest Rate, Days_to_Expiry], volatility ..... ..... ) 
        computed_delta = 0
        if instrument_details['instrument_type'] == "CE":
            computed_delta = c.callDelta*position['quantity']
            total_ce_delta = total_ce_delta + computed_delta 
            total_ce = total_ce+position['quantity']
            if position['quantity'] > 0:
                total_positive_ce = total_positive_ce+abs(position['quantity'])
            else:
                total_negative_ce = total_negative_ce+abs(position['quantity'])
        elif instrument_details['instrument_type'] == "PE":
            computed_delta = c.putDelta*position['quantity']
            total_pe_delta = total_pe_delta + computed_delta 
            total_pe = total_pe+position['quantity']
            if position['quantity'] > 0:
                total_positive_pe = total_positive_pe+abs(position['quantity'])
            else:
                total_negative_pe = total_negative_pe+abs(position['quantity'])
        total_delta = total_delta+ computed_delta
        
        expiry_map[expiry_date] = expiry_map.get(expiry_date, 0) + computed_delta
        
        print("Added Value to Delta "+position['tradingsymbol']+"  -- Delta -- "+str(computed_delta));
        #print(vars(c))
        #quit()
    #print(vars(live_positions))
    print("-----  Final Delta  ------- ");
    print(total_delta);
    print("-----  Futures Delta  ------")
    print(futures_delta)
    print("-----  CE & PE Data  ------")
    print("CE Total "+str(total_ce)+" ---- PE Total = "+str(total_pe)+" Diff = "+str(total_pe-total_ce))
    print("CE Total Comparison = -"+str(total_negative_ce)+" & +"+str(total_positive_ce)+" ---- PE Total = -"+str(total_negative_pe)+" & +"+str(total_positive_pe)+" Diff = "+str(total_negative_ce-total_negative_pe))
    spread_count = abs(total_positive_ce+total_positive_pe)
    single_pe_ce = abs(total_pe - total_ce)
    both_ce_pe = abs(min(abs(total_ce), abs(total_pe)))
    margin_requirement = (calculate_margin_requirement(spread_count, single_pe_ce, both_ce_pe))/75
    formatted_market_requirement = formatINR(margin_requirement)
    print("\n\n\n")
    print("Total Margin Requirement = Rs. "+str(formatted_market_requirement))
    print("\n\n\n")

    greeks = {}
    greeks['ce'] = {}
    greeks['pe'] = {}

    greeks['ce']['amount'] = total_ce
    greeks['ce']['delta'] = total_ce_delta

    greeks['pe']['amount'] = total_pe
    greeks['pe']['delta'] = total_pe_delta

    greeks['delta'] = total_delta
    greeks['expiry_delta'] = expiry_map
    return greeks
    greeks['delta'] = total_delta
    return greeks


def get_sensex_current_greeks(restrict_days=True):
    """
    Calculate delta and greeks for SENSEX positions.
    
    Mirrors get_nifty_current_greeks() but filters for SENSEX positions.
    Uses BFO exchange and sensex_lot_size (20).
    
    Args:
        restrict_days: If True, only include options within delta_calculation_days.
    
    Returns:
        dict: Greeks including delta, ce/pe breakdown, and expiry_delta map.
    """
    global all_instruments
    global todays_volatility
    global interest_rate
    global delta_calculation_days
    global sensex_lot_size

    logging.info(f"Calculating SENSEX greeks (delta_calculation_days={delta_calculation_days})")

    try:
        live_positions = get_cached_positions()
    except Exception:
        live_positions = kite.positions()
    net_positions = live_positions['net']
    
    total_delta = 0
    futures_delta = 0
    
    sensex_quote_data = get_sensex_current_quote()
    sensex_last_price = sensex_quote_data['last_price']

    total_ce = 0
    total_pe = 0
    total_ce_delta = 0
    total_pe_delta = 0
    expiry_map = {}

    for position in net_positions:
        tradingtoken = position['instrument_token']
        tradingsymbol = position['tradingsymbol']
        
        # Only process SENSEX positions
        if not tradingsymbol.startswith("SENSEX"):
            continue
        if tradingtoken not in all_instruments:
            continue
            
        instrument_details = all_instruments[tradingtoken]
        expiry_date = str(instrument_details['expiry'])
        
        # Handle futures
        if instrument_details['segment'] != "BFO-OPT":
            if instrument_details['segment'] == "BFO-FUT":
                total_delta = total_delta + position['quantity']
                futures_delta = futures_delta + position['quantity']
                expiry_map[expiry_date] = expiry_map.get(expiry_date, 0) + position['quantity']
            continue
        
        # Filter by days to expiry
        if instrument_details['days_to_expiry'] > delta_calculation_days and restrict_days:
            continue

        # Calculate delta using mibian
        c = mibian.BS([sensex_last_price, instrument_details['strike'], interest_rate, 
                       instrument_details['days_to_expiry']], volatility=todays_volatility)
        
        computed_delta = 0
        if instrument_details['instrument_type'] == "CE":
            computed_delta = c.callDelta * position['quantity']
            total_ce_delta = total_ce_delta + computed_delta
            total_ce = total_ce + position['quantity']
        elif instrument_details['instrument_type'] == "PE":
            computed_delta = c.putDelta * position['quantity']
            total_pe_delta = total_pe_delta + computed_delta
            total_pe = total_pe + position['quantity']
        
        total_delta = total_delta + computed_delta
        expiry_map[expiry_date] = expiry_map.get(expiry_date, 0) + computed_delta
        
        logging.debug(f"SENSEX delta: {tradingsymbol} delta={computed_delta}")

    logging.info(f"SENSEX total_delta={total_delta}, futures_delta={futures_delta}")

    greeks = {
        'ce': {'amount': total_ce, 'delta': total_ce_delta},
        'pe': {'amount': total_pe, 'delta': total_pe_delta},
        'delta': total_delta,
        'expiry_delta': expiry_map
    }
    return greeks


def formatINR(number):
    s, *d = str(number).partition(".")
    r = ",".join([s[x-2:x] for x in range(-3, -len(s), -2)][::-1] + [s[-3:]])
    return "".join([r] + d)

def calculate_margin_requirement(spread_count, single_pe_ce, both_ce_pe):
    print(" Counts = "+str(spread_count)+" -- "+ str(single_pe_ce)+ " -- "+str(both_ce_pe))
    return spread_count*margin_spread + single_pe_ce*margin_single_pe_ce + both_ce_pe*margin_both_pe_ce


def get_current_positions():
    live_positions = kite.positions()
    net_position =  live_positions['net']
    #todays_position = live_positions['day'] ## This is for intraday perspective
    return net_position 


def printCurrentStatus():
    global order_numbers_storage
    global orders
    global current_positions
    global initial_positions 
    print("Number Status = ")
    print(order_numbers_storage)
    ct = get_ist_now()
    print("Orders Time = ", ct)
    print(orders)
    logging.error("Initial Positions were {}".format(initial_positions))
    logging.error("Current Positions are  {}".format(current_positions))
    logging.error("Symbol - {}".format(symbol))
    logging.error("Quantity - {}".format(quantity))
    logging.error("Buy Gap - {}".format(buy_gap))
    logging.error("Sell Gap - {}".format(sell_gap))

    try:
        write_status_to_file()
    except Exception as e:
        logging.error("Failed to write status file: {}".format(e))


def _get_sensex_greeks_for_status() -> dict:
    """Compute SENSEX greeks for the status file using sensex_positions_lib.

    Uses fresh BFO instrument data and per-option implied volatility, which avoids
    the stale-token / stale-DTE bug in get_sensex_current_greeks() (all_instruments
    is populated once at first write and never refreshed; tokens or DTE going stale
    causes positions to be silently skipped, producing wildly wrong delta values).

    Falls back to get_sensex_current_greeks() if the import or calculation fails.
    """
    try:
        from sensex_positions_lib import get_sensex_positions_summary
        summary = get_sensex_positions_summary(
            kite,
            volatility=todays_volatility,
            interest_rate=interest_rate,
        )
        return {
            "delta": summary.get("total_delta", 0),
            "expiry_delta": summary.get("expiry_map", {}),
            "ce": {
                "amount": summary.get("total_ce", 0),
                "delta": summary.get("ce_delta", 0),
            },
            "pe": {
                "amount": summary.get("total_pe", 0),
                "delta": summary.get("pe_delta", 0),
            },
        }
    except Exception as _e:
        logging.warning("SENSEX greeks via sensex_positions_lib failed, falling back to stale cache: %s", _e)
        return get_sensex_current_greeks(restrict_days=True)


def write_status_to_file():
    global symbol
    global orders
    global current_positions
    global initial_positions
    global buy_gap
    global sell_gap
    global quantity
    global order_numbers_storage
    global instrument_token
    global start_time
    global all_instruments

    # Lazy-load instrument metadata the first time we write status. The scraper
    # skips this at startup to save memory, but the greeks functions need the
    # token→expiry/strike mapping to compute per-expiry delta.
    if not all_instruments:
        try:
            get_all_fut_opt_instruments()  # populates all_instruments + token_symbol_map
            # SQLite stores raw expiry date strings; compute days_to_expiry so greeks
            # functions can use instrument_details['days_to_expiry'] without KeyError.
            _today = get_ist_now().date()
            for _details in all_instruments.values():
                _expiry_str = _details.get('expiry')
                if _expiry_str and 'days_to_expiry' not in _details:
                    try:
                        _expiry_date = datetime.datetime.strptime(str(_expiry_str), '%Y-%m-%d').date()
                        _details['days_to_expiry'] = int(np.busday_count(_today, _expiry_date) + 1)
                    except (ValueError, TypeError):
                        _details['days_to_expiry'] = 0
        except Exception as _e:
            logging.warning("write_status_to_file: could not load instruments: %s", _e)

    # Use absolute path for status directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(base_dir)
    status_dir = os.path.join(project_root, "status")


    if not symbol:
        return

    if not os.path.exists(status_dir):
        os.makedirs(status_dir)
        
    # Per-PID status file: each scraper process owns its own file so duplicate
    # processes for the same symbol are visible as separate dashboard entries
    # instead of silently overwriting each other (last-writer-wins masked the
    # NIFTY2670724400CE duplicate-leg incident).
    status_file = os.path.join(status_dir, "status_{}_{}.json".format(symbol, os.getpid()))

    # Fetch full position details for P&L
    position_details = get_full_position_details(symbol)
    
    # Update instrument_token from position if available (and persist it)
    if position_details.get("instrument_token") and not instrument_token:
        instrument_token = position_details.get("instrument_token")
    
    status_data = {
        "symbol": symbol,
        "timestamp": str(get_ist_now()),
        "start_time": start_time,
        "orders": orders,
        "current_positions": current_positions,
        "initial_positions": initial_positions,
        "buy_gap": buy_gap,
        "sell_gap": sell_gap,
        "quantity": quantity,
        "order_stats": order_numbers_storage,
        "pid": os.getpid(),
        "instrument_token": position_details.get("instrument_token") or instrument_token,
        "avg_buy_price": position_details.get("buy_price", 0),
        "avg_sell_price": position_details.get("sell_price", 0),
        "total_buy_quantity": position_details.get("buy_quantity", 0),
        "total_sell_quantity": position_details.get("sell_quantity", 0),
        "open_pnl": position_details.get("m2m", 0),
        "realized_pnl": position_details.get("realised", 0),
        "last_price": position_details.get("last_price", 0),
        "segment": segment,
        "delta_config": delta_limits_config,
        "delta_info": {
            "nifty_greeks": get_nifty_current_greeks(restrict_days=True) if symbol.startswith("NIFTY") else {},
            "bank_nifty_greeks": get_bank_nifty_current_greeks() if symbol.startswith("BANKNIFTY") else {},
            "sensex_greeks": _get_sensex_greeks_for_status() if symbol.startswith("SENSEX") else {},
        },
        "restrictions": {
            "buy": global_restrict_buy,
            "sell": global_restrict_sell
        },
        "last_multiplier_info": _last_multiplier_info,
    }
    
    try:
        with open(status_file, 'w') as f:
            json.dump(status_data, f, indent=4, default=str)
    except Exception as e:
        logging.error("Failed to write status file: {}".format(e))


def delete_own_status_file() -> None:
    """Delete this process's per-PID status file (and legacy per-symbol file).

    Called on scraper shutdown (SIGTERM from the dashboard Stop buttons or
    normal exit) so stopped instances don't linger as stale dashboard entries.
    The legacy ``status_<symbol>.json`` is only removed if it was written by
    this process (pid matches), to avoid deleting a sibling's file.
    """
    global symbol
    if not symbol:
        return
    base_dir = os.path.dirname(os.path.abspath(__file__))
    status_dir = os.path.join(os.path.dirname(base_dir), "status")
    own_pid = os.getpid()
    per_pid_file = os.path.join(status_dir, "status_{}_{}.json".format(symbol, own_pid))
    try:
        if os.path.exists(per_pid_file):
            os.remove(per_pid_file)
            logging.info("Deleted own status file %s", per_pid_file)
    except OSError as exc:
        logging.warning("Could not delete own status file %s: %s", per_pid_file, exc)
    legacy_file = os.path.join(status_dir, "status_{}.json".format(symbol))
    try:
        if os.path.exists(legacy_file):
            with open(legacy_file, "r") as f:
                legacy_pid = json.load(f).get("pid")
            if legacy_pid == own_pid:
                os.remove(legacy_file)
                logging.info("Deleted legacy status file %s", legacy_file)
    except (OSError, ValueError) as exc:
        logging.warning("Could not clean legacy status file %s: %s", legacy_file, exc)


def get_full_position_details(symbol):
    try:
        live_positions = kite.positions()
        net_positions = live_positions['net']
        for position in net_positions:
            if position['tradingsymbol'] == symbol:
                return position
    except Exception as e:
        logging.error("Error fetching position details: {}".format(e))
    return {}


def get_best_buy_sell_price(buy_price_1, buy_price_2, sell_price_1, sell_price_2):
    print("Price = "+str(buy_price_1)+" "+str(buy_price_2)+" "+str(sell_price_1)+" "+str(sell_price_2))
    return_price = {}
    if buy_price_1 <= buy_price_2:
        return_price['buy'] = buy_price_1
    else:
        return_price['buy'] = buy_price_2

    if sell_price_1 >= sell_price_2:
        return_price['sell'] = sell_price_1
    else:
        return_price['sell'] = sell_price_2

    return return_price



def check_is_any_order_active():
    global orders
    for order_id in orders.keys():
        if order_id == -1:
            continue
        else:
            return True
    

    return False


def _reprice_orders_on_multiplier_change() -> int:
    """Reprice live limit orders when delta or velocity multiplier has changed.

    For each tracked order with anchor prices stored (set by place_duo_order),
    computes the expected order price using current multipliers and calls
    kite.modify_order() if the price has shifted.

    Skips orders without anchor data (placed before this feature, or GTT fallbacks).
    Must only be called from within check_changes_in_restrictions() which already
    holds the already_executing_order / already_updating_order guard.

    Returns:
        int: Number of orders successfully repriced.
    """
    we_config = load_wave_extractor_config(symbol)
    delta_buy_mult, delta_sell_mult = get_delta_multiplier(symbol_type, symbol)
    vel_buy_mult, vel_sell_mult = get_velocity_multiplier(symbol_type, symbol, we_config)

    repriced_count = 0
    for order_id in list(orders.keys()):
        if order_id == -1:
            continue
        order = orders.get(order_id, {})
        m_info = order.get('multiplier_info') or {}

        if order.get('manually_overridden'):
            logging.debug("[WE_REPRICE] Skipping order %s — manually overridden", order_id)
            continue

        is_buy = order.get('transaction_type') == kite.TRANSACTION_TYPE_BUY
        anchor = m_info.get('anchor_buy_price' if is_buy else 'anchor_sell_price')
        if anchor is None:
            continue  # order placed before this feature was added; skip gracefully

        base_gap = m_info.get(
            'base_buy_gap' if is_buy else 'base_sell_gap',
            buy_gap if is_buy else sell_gap,
        )
        step_mult = m_info.get('scale_buy' if is_buy else 'scale_sell', 1.0)
        active_delta_mult = delta_buy_mult if is_buy else delta_sell_mult
        active_vel_mult = vel_buy_mult if is_buy else vel_sell_mult

        new_gap = round(base_gap * step_mult * active_delta_mult * active_vel_mult, 1)
        expected_price = round(anchor - new_gap if is_buy else anchor + new_gap, 1)
        current_price = order.get('price', 0)

        if expected_price == current_price:
            continue

        logging.info(
            "[WE_REPRICE] order=%s %s: %.1f → %.1f "
            "(anchor=%.1f base_gap=%.1f step=%.2f delta=%.3f vel=%.3f)",
            order_id,
            order.get('transaction_type', '?'),
            current_price,
            expected_price,
            anchor,
            base_gap,
            step_mult,
            active_delta_mult,
            active_vel_mult,
        )
        try:
            _pending_auto_reprice_ids.add(order_id)
            kite.modify_order(
                variety=kite.VARIETY_REGULAR,
                order_id=order_id,
                price=expected_price,
            )
            orders[order_id]['price'] = expected_price
            gap_key = 'final_buy_gap' if is_buy else 'final_sell_gap'
            m_info[gap_key] = new_gap
            repriced_count += 1
        except Exception as _mod_exc:
            _pending_auto_reprice_ids.discard(order_id)
            logging.warning("[WE_REPRICE] Failed to reprice order %s: %s", order_id, _mod_exc)

    return repriced_count


#This function can continously check if the delta is not wide and can delete the order.
#TODO the pending part is if the order is not there to create the order, right now it only deletes the order.
def check_changes_in_restrictions(symbol):
    global orders

    global sell_quantity
    global buy_quantity

    global buy_gap
    global sell_gap

    global tag
    global product_type

    global old_quote_price
    global duo_old_buy_price 
    global duo_old_sell_price 

    global already_executing_order

    global exchange
    global exceptNFBNF

    if already_executing_order > 0 or already_updating_order > 0:
        print("Already Executing Order or Updating Order so returning")
        return
    
    print_duo_old_prices()


    return_restrictions = set_restrictions()

    symbol_restrictions = {}
    restrict_buy_order = 0
    restrict_sell_order = 0
    is_symbol_nifty = False

    if symbol.startswith("BANKNIFTY"):
        exceptNFBNF = False
        symbol_restrictions = return_restrictions['bank_nifty']
        logging.info("Symbol restriction: BANKNIFTY")
    elif symbol.startswith("NIFTY"):
        exceptNFBNF = False
        symbol_restrictions = return_restrictions['nifty']
        is_symbol_nifty = True
        logging.info("Symbol restriction: NIFTY")
    elif symbol.startswith("SENSEX"):
        exceptNFBNF = False
        symbol_restrictions = return_restrictions['sensex']
        logging.info("Symbol restriction: SENSEX")
    elif exceptNFBNF == False:
        logging.warning(f"Unknown symbol {symbol}: applying no restrictions")
        symbol_restrictions = {
            'ce': {'buy': 'yes', 'sell': 'yes'},
            'pe': {'buy': 'yes', 'sell': 'yes'},
            'futures': {'buy': 'yes', 'sell': 'yes'},
        }

    current_pos_info = get_position_for_symbol(symbol)
    
    if (symbol_type == "ce" or symbol_type == "pe") and current_pos_info == 0:
        print("Order is pushing for positive Buying - Allowing it for 1 time but will not allow any further")
    elif (symbol_type == "ce" or symbol_type == "pe") and current_pos_info > 0:
        print("Restricting order as Option order is already in Positive, Will allow only SELL order to go through")
        restrict_buy_order = 1

    if exceptNFBNF == False and symbol_restrictions[symbol_type]["buy"] == "no":
        restrict_buy_order = 1;


    if exceptNFBNF == False and symbol_restrictions[symbol_type]["sell"] == "no":
        restrict_sell_order = 1;

    global global_restrict_buy, global_restrict_sell
    global_restrict_buy = restrict_buy_order
    global_restrict_sell = restrict_sell_order

    # Not sure why below if was there. It is causing issue in recreation of orders. This needs to be tested throughly.
    #if restrict_buy_order == 0 and restrict_sell_order == 0:
    #    return


    try:
        is_sell_present = 0
        is_buy_present = 0
        sell_price = -1
        buy_price = -1

        sell_order_id = -1
        buy_order_id = -1
        _orders_modified = False

        # Iterate over a snapshot — the loop body may delete entries via cancel_order()
        for order_id in list(orders.keys()):
            if order_id == -1:
                continue

            # If the product is NIFTY and buy price is less than 25, the buy restrict order
            # is not considered and the buy order is placed (saves on margins by closing position).
            if is_symbol_nifty == True and restrict_buy_order == 1 and orders[order_id]['transaction_type'] == "BUY" and orders[order_id]['price'] <= 25:
                continue

            if (restrict_sell_order == 1 and orders[order_id]['transaction_type'] == "SELL") or (restrict_buy_order == 1 and orders[order_id]['transaction_type'] == "BUY"):
                logging.info("check_changes_in_restrictions: order %s restricted, cancelling", order_id)
                associated_order_id = orders[order_id]['associated_order']
                orders[order_id]['associated_order'] = -1
                if associated_order_id != -1 and associated_order_id in orders:
                    orders[associated_order_id]['associated_order'] = -1
                cancel_order(kite.VARIETY_REGULAR, order_id)
                _orders_modified = True
                continue

            if orders[order_id]['transaction_type'] == "SELL":
                is_sell_present = 1
                sell_price = orders[order_id]['price']
                sell_order_id = order_id

            elif orders[order_id]['transaction_type'] == "BUY":
                is_buy_present = 1
                buy_price = orders[order_id]['price']
                buy_order_id = order_id

        if restrict_sell_order == 0 and is_sell_present == 0:
            logging.info("check_changes_in_restrictions: sell order missing, re-placing")

            if duo_old_sell_price == -1:
                # Prices not initialized (first place_duo_order likely failed) — use live quote
                exchange_symbol = exchange + ":" + symbol
                quote_data = get_quote_with_retry(exchange_symbol)
                current_price = quote_data[exchange_symbol]['last_price']
                final_sell_price = current_price + sell_gap
                logging.info(
                    "check_changes_in_restrictions: sell re-place at live %.2f + gap %.2f = %.2f "
                    "(duo_old_sell_price uninitialized)",
                    current_price, sell_gap, final_sell_price,
                )
            else:
                final_sell_price = duo_old_sell_price
                logging.info(
                    "check_changes_in_restrictions: sell re-place at original price %.2f",
                    final_sell_price,
                )

            if already_executing_order == 0 and already_updating_order == 0:
                unique_tag_sell = "S_" + str(int(time.time())) + "_" + str(randint(1000, 9999))
                sell_order_id = place_order(symbol, kite.VARIETY_REGULAR, kite.TRANSACTION_TYPE_SELL, exchange, typeOfProduct, final_sell_price, sell_quantity, unique_tag_sell)

                if sell_order_id != -1:
                    if is_buy_present == 1:
                        add_order_to_list(sell_order_id, final_sell_price, sell_quantity, kite.TRANSACTION_TYPE_SELL, symbol, buy_order_id, unique_tag_sell)
                        orders[buy_order_id]['associated_order'] = sell_order_id
                    else:
                        add_order_to_list(sell_order_id, final_sell_price, sell_quantity, kite.TRANSACTION_TYPE_SELL, symbol, -1, unique_tag_sell)
                    _orders_modified = True

        if restrict_buy_order == 0 and is_buy_present == 0:
            logging.info("check_changes_in_restrictions: buy order missing, re-placing")

            if duo_old_buy_price == -1:
                # Prices not initialized — use live quote
                exchange_symbol = exchange + ":" + symbol
                quote_data = get_quote_with_retry(exchange_symbol)
                current_price = quote_data[exchange_symbol]['last_price']
                final_buy_price = current_price - buy_gap
                logging.info(
                    "check_changes_in_restrictions: buy re-place at live %.2f - gap %.2f = %.2f "
                    "(duo_old_buy_price uninitialized)",
                    current_price, buy_gap, final_buy_price,
                )
            else:
                final_buy_price = duo_old_buy_price
                logging.info(
                    "check_changes_in_restrictions: buy re-place at original price %.2f",
                    final_buy_price,
                )

            if already_executing_order == 0 and already_updating_order == 0:
                unique_tag_buy = "B_" + str(int(time.time())) + "_" + str(randint(1000, 9999))
                buy_order_id = place_order(symbol, kite.VARIETY_REGULAR, kite.TRANSACTION_TYPE_BUY, exchange, typeOfProduct, final_buy_price, buy_quantity, unique_tag_buy)

                if buy_order_id != -1:
                    if is_sell_present == 1:
                        add_order_to_list(buy_order_id, final_buy_price, buy_quantity, kite.TRANSACTION_TYPE_BUY, symbol, sell_order_id, unique_tag_buy)
                        orders[sell_order_id]['associated_order'] = buy_order_id
                    else:
                        add_order_to_list(buy_order_id, final_buy_price, buy_quantity, kite.TRANSACTION_TYPE_BUY, symbol, -1, unique_tag_buy)
                    _orders_modified = True

        # Invalidate cache after placing orders to ensure next logic tick sees true state
        invalidate_positions_cache()

        # Reprice live orders if delta or velocity multiplier has shifted since placement
        repriced = _reprice_orders_on_multiplier_change()
        if repriced:
            _orders_modified = True

        # Write status immediately so the UI reflects the new order state without waiting
        # for the next place_duo_order() call or WebSocket order update.
        if _orders_modified:
            try:
                write_status_to_file()
            except Exception as _wse:
                logging.warning("check_changes_in_restrictions: write_status_to_file failed: %s", _wse)

    except Exception as e:
        logging.error("Error placing order: {}".format(e))
    






def place_duo_order(symbol, typeOfProduct = "NRML", exceptNFBNFLocal = False):
    global already_executing_order
    global initial_positions
    global current_positions
    global quantity

    global buy_quantity
    global sell_quantity


    global symbol_type
    global tag

    global exceptNFBNF

    global scraper_last_price
    global old_quote_price
    global duo_old_buy_price, duo_old_sell_price

    global exchange
    



    exceptNFBNF = exceptNFBNFLocal

    is_symbol_nifty = False


    if already_executing_order > 0:   # This acts as a semaphore
        logging.info("Already executing order, skipping place_duo_order")
        return
    
    
    # Set semaphore
    already_executing_order = 1

    try:
        return_restrictions = set_restrictions()

        symbol_restrictions = {}

        if symbol.startswith("BANKNIFTY"):
            exceptNFBNF = False
            symbol_restrictions = return_restrictions['bank_nifty']
            logging.info("Symbol restriction: BANKNIFTY")
        elif symbol.startswith("NIFTY"):
            exceptNFBNF = False
            symbol_restrictions = return_restrictions['nifty']
            is_symbol_nifty = True
            logging.info("Symbol restriction: NIFTY")
        elif symbol.startswith("SENSEX"):
            exceptNFBNF = False
            symbol_restrictions = return_restrictions['sensex']
            logging.info("Symbol restriction: SENSEX")
        elif exceptNFBNF == False:
            logging.warning(f"Unknown symbol {symbol}: no restrictions available, returning")
            return


        print("FINAL Symbol Restriction ")
        print(symbol_restrictions)

        initial_positions_net = initial_positions['position']
        current_positions = {"position": get_position_for_symbol(symbol)} 
        current_positions_net = current_positions


        restrict_buy_order = 0  # this is incase there is option order so we would not want to buy options so to restrict further Buy orders
        restrict_sell_order = 0  # this is incase there is option order so we would not want to buy options so to restrict further Buy orders

        scaled_buy_gap = buy_gap
        scaled_sell_gap = sell_gap


        new_initial_positions_net = initial_positions['position']
        new_current_positions_net = current_positions['position']

        print("Still under the limit: "+str(current_positions['position']))
        if (symbol_type == "ce" or symbol_type == "pe") and current_positions['position'] == 0:
            print("Order is pushing for positive Buying - Allowing it for 1 time but will not allow any further")
        elif (symbol_type == "ce" or symbol_type == "pe") and current_positions['position'] > 0:  #This is to restrict if the option current status is already Positive
            print("Restricting order as Option order is already in Positive, Will allow only SELL order to go through")
            restrict_buy_order = 1

        if exceptNFBNF == False and symbol_restrictions[symbol_type]["buy"] == "no":
            restrict_buy_order = 1;


        if exceptNFBNF == False and symbol_restrictions[symbol_type]["sell"] == "no":
            restrict_sell_order = 1;


        print("Restricted Setup is ----- BUY Order Restriction "+str(restrict_buy_order)+" SELL Order Restriction "+str(restrict_sell_order))

        global global_restrict_buy, global_restrict_sell
        global_restrict_buy = restrict_buy_order
        global_restrict_sell = restrict_sell_order

        current_diff_scale = (new_current_positions_net-new_initial_positions_net)/int(quantity)  # If new current_diff_scale > 0 it is more BUY, If it is < 0 it is SELL which has happened.

        # Load wave extractor config for this symbol's underlying
        we_config = load_wave_extractor_config(symbol)

        # Multiplier scale: prefer config file, fall back to module-level multiplier_scale
        _we_mscale = we_config.get("multiplier_scale", multiplier_scale)
        step_key = str(int(current_diff_scale))
        if step_key not in _we_mscale:
            multiplier_array = [1, 100] if int(current_diff_scale) < 0 else [100, 1]
        else:
            multiplier_array = _we_mscale[step_key]

        scaled_buy_gap  = round(buy_gap  * multiplier_array[0], 1)
        scaled_sell_gap = round(sell_gap * multiplier_array[1], 1)
        gap_after_scale = [scaled_buy_gap, scaled_sell_gap]

        # Apply delta-based continuous multiplier
        delta_buy_mult, delta_sell_mult = get_delta_multiplier(symbol_type, symbol)
        scaled_buy_gap  = round(scaled_buy_gap  * delta_buy_mult,  1)
        scaled_sell_gap = round(scaled_sell_gap * delta_sell_mult, 1)
        gap_after_delta = [scaled_buy_gap, scaled_sell_gap]

        # Apply fill cooldown before velocity guard — cooldown check is cheaper
        buy_in_cooldown  = is_side_in_cooldown(kite.TRANSACTION_TYPE_BUY,  symbol)
        sell_in_cooldown = is_side_in_cooldown(kite.TRANSACTION_TYPE_SELL, symbol)
        if buy_in_cooldown:
            restrict_buy_order = 1
        if sell_in_cooldown:
            restrict_sell_order = 1

        # Update spot price history BEFORE computing velocity so the current price
        # is included in the window that get_velocity_multiplier reads.
        # INTENTIONALLY NOT CACHED — velocity guard requires real-time spot price.
        # Caching would defeat momentum detection: a 15–30s stale price can miss
        # the exact intraday spike this guard is designed to catch.
        # Do NOT route this call through _get_index_quote_cached().
        try:
            underlying_spot_symbol = _get_spot_symbol_for(symbol)
            if underlying_spot_symbol:
                spot_quote = kite.quote(underlying_spot_symbol)
                spot_price = spot_quote[underlying_spot_symbol]['last_price']
                vel_cfg = we_config.get("velocity_guard", {})
                update_spot_price_history(spot_price, float(vel_cfg.get("window_minutes", 15)))
        except Exception as _spot_exc:
            logging.debug("Could not update spot price history: %s", _spot_exc)

        # Apply spot velocity guard (stacks on top of delta multiplier)
        vel_buy_mult, vel_sell_mult = get_velocity_multiplier(symbol_type, symbol, we_config)
        scaled_buy_gap  = round(scaled_buy_gap  * vel_buy_mult,  1)
        scaled_sell_gap = round(scaled_sell_gap * vel_sell_mult, 1)
        gap_after_velocity = [scaled_buy_gap, scaled_sell_gap]

        logging.info(
            "[WE_MULTIPLIER] gap_summary | symbol=%s step=%s "
            "base=[%.1f,%.1f] after_scale=[%.1f,%.1f] after_delta=[%.1f,%.1f] "
            "after_velocity=[%.1f,%.1f] cooldown=[buy=%s,sell=%s] "
            "final_buy=%.1f final_sell=%.1f restrict=[buy=%d,sell=%d]",
            symbol, step_key,
            buy_gap, sell_gap,
            gap_after_scale[0], gap_after_scale[1],
            gap_after_delta[0], gap_after_delta[1],
            gap_after_velocity[0], gap_after_velocity[1],
            "SUPPRESSED" if buy_in_cooldown else "ok",
            "SUPPRESSED" if sell_in_cooldown else "ok",
            scaled_buy_gap, scaled_sell_gap,
            restrict_buy_order, restrict_sell_order,
        )

        logging.info("Scaled Gaps are - BUY - %s  SELL - %s", scaled_buy_gap, scaled_sell_gap)
        print(f"Gap percentages: buy={buy_gap_percentage*100:.1f}%, sell={sell_gap_percentage*100:.1f}%")

        # Build multiplier snapshot for dashboard visibility and order history.
        multiplier_info: dict = {
            "step": step_key,
            "scale_buy": multiplier_array[0],
            "scale_sell": multiplier_array[1],
            "delta_buy": round(delta_buy_mult, 3),
            "delta_sell": round(delta_sell_mult, 3),
            "velocity_buy": round(vel_buy_mult, 3),
            "velocity_sell": round(vel_sell_mult, 3),
            "buy_cooldown": buy_in_cooldown,
            "sell_cooldown": sell_in_cooldown,
            "base_buy_gap": buy_gap,
            "base_sell_gap": sell_gap,
            "final_buy_gap": scaled_buy_gap,
            "final_sell_gap": scaled_sell_gap,
            "effective_buy_mult": round(scaled_buy_gap / buy_gap, 2) if buy_gap else 1.0,
            "effective_sell_mult": round(scaled_sell_gap / sell_gap, 2) if sell_gap else 1.0,
            "ts": get_ist_now().strftime("%H:%M:%S"),
        }
        global _last_multiplier_info
        _last_multiplier_info = multiplier_info

        exchange_Symbol = exchange+":"+symbol

        try:
            quote_data = kite.quote(exchange_Symbol)
        except Exception as e:
            time.sleep(randint(1,5))
            quote_data = kite.quote(exchange_Symbol)
        price = quote_data[exchange_Symbol]['last_price']





        best_current_prices = get_best_buy_sell_price(price-scaled_buy_gap, scraper_last_price-scaled_buy_gap, price+scaled_sell_gap, scraper_last_price + scaled_sell_gap)


        sec30_before_start_new_sell_price = best_current_prices['sell']
        sec30_before_start_new_buy_price = best_current_prices['buy'] 

        effective_cool_off = int(we_config.get("cool_off_time", cool_off_time))
        logging.info("Cool-off period of %d secs started", effective_cool_off)
        time.sleep(effective_cool_off)
        logging.info("Cool-off period ended")

        # Re-fetch quote after sleep
        try:
            quote_data = kite.quote(exchange_Symbol)
        except Exception as e:
            time.sleep(randint(1,5))
            quote_data = kite.quote(exchange_Symbol)
        price = quote_data[exchange_Symbol]['last_price']


        best_current_prices = get_best_buy_sell_price(price-scaled_buy_gap, scraper_last_price-scaled_buy_gap, price+scaled_sell_gap, scraper_last_price + scaled_sell_gap)


        final_sell_price = best_current_prices['sell']
        final_buy_price = best_current_prices['buy']

        # Store effective anchors for multiplier-based repricing in check_changes_in_restrictions().
        # anchor = min/max(quote_price, scraper_last_price) that get_best_buy_sell_price chose.
        multiplier_info['anchor_buy_price'] = final_buy_price + scaled_buy_gap
        multiplier_info['anchor_sell_price'] = final_sell_price - scaled_sell_gap

        print("Final Buy Price "+str(final_buy_price)+" Final Sell Price "+str(final_sell_price))
        print("Current Price "+str(price))
        print("Scraper Last Price "+str(scraper_last_price))


        if is_symbol_nifty == True and final_buy_price <= 25 and restrict_buy_order == 1:
            restrict_buy_order = 0
            print("Important QC Check ----------------- I am not restricting Buy order here because the price is less than 25")




        print_data = {"buy restriction":restrict_buy_order,"sell restriction":restrict_sell_order}
        logging.error("Restrictions are  "+format(print_data))

        # --- SELL order ---
        sell_order_id = -1
        gtt_sell_tag = None
        if restrict_sell_order == 0:
            unique_tag_sell = "S_" + str(int(time.time())) + "_" + str(randint(1000, 9999))
            sell_order_id = place_order(symbol, kite.VARIETY_REGULAR, kite.TRANSACTION_TYPE_SELL, exchange, typeOfProduct, final_sell_price, sell_quantity, unique_tag_sell)
            if sell_order_id != -1:
                add_order_to_list(sell_order_id, final_sell_price, sell_quantity, kite.TRANSACTION_TYPE_SELL, symbol, "-1", unique_tag_sell)
                orders[str(sell_order_id)]['multiplier_info'] = multiplier_info
                logging.info(" -------------------------    Sell order placed successfully")
            else:
                # Fallback: place a GTT for SELL only if market is still open.
                # After 15:30 IST, Zerodha will have cancelled orders due to market close;
                # placing a GTT at that point creates overnight risk on gap moves.
                if not is_market_open():
                    logging.warning(
                        "SELL order failed for %s and market is closed — "
                        "suppressing GTT fallback to avoid overnight gap risk.",
                        symbol,
                    )
                else:
                    gtt_sell_tag = "GTT_S_" + str(int(time.time())) + "_" + str(randint(1000, 9999))
                    instrument_cache.save_gtt_algo_tag(gtt_sell_tag, tag)
                    sell_trigger_id = place_gtt_order(symbol, kite.VARIETY_REGULAR, kite.TRANSACTION_TYPE_SELL, exchange, typeOfProduct, final_sell_price, sell_quantity, gtt_sell_tag)
                    if sell_trigger_id != -1:
                        pending_gtt_fallbacks[gtt_sell_tag] = {
                            'trigger_id': sell_trigger_id, 'symbol': symbol, 'exchange': exchange,
                            'product': typeOfProduct, 'quantity': sell_quantity,
                            'transaction_type': kite.TRANSACTION_TYPE_SELL, 'price': final_sell_price,
                            'associated_regular_order_id': -1,  # updated once BUY order_id is known
                            'associated_gtt_tag': None
                        }
                        logging.warning(f"SELL order failed, placed GTT fallback {gtt_sell_tag} at {final_sell_price}")
                    else:
                        gtt_sell_tag = None  # GTT also failed, treat as if sell not placed

        # --- BUY order ---
        # Attempt BUY whether SELL was a regular order OR a GTT fallback
        sell_is_live = sell_order_id != -1 or gtt_sell_tag is not None
        if (restrict_sell_order == 1 or sell_is_live) and restrict_buy_order == 0:
            unique_tag_buy = "B_" + str(int(time.time())) + "_" + str(randint(1000, 9999))
            buy_order_id = place_order(symbol, kite.VARIETY_REGULAR, kite.TRANSACTION_TYPE_BUY, exchange, typeOfProduct, final_buy_price, buy_quantity, unique_tag_buy)
            if buy_order_id != -1:
                if sell_order_id != -1:
                    # Normal path: both regular orders — pair them bidirectionally
                    add_order_to_list(sell_order_id, final_sell_price, sell_quantity, kite.TRANSACTION_TYPE_SELL, symbol, buy_order_id, unique_tag_sell)
                    add_order_to_list(buy_order_id, final_buy_price, buy_quantity, kite.TRANSACTION_TYPE_BUY, symbol, sell_order_id, unique_tag_buy)
                    orders[str(sell_order_id)]['multiplier_info'] = multiplier_info
                    orders[str(buy_order_id)]['multiplier_info'] = multiplier_info
                    logging.info(" -----------------------------   Both Buy and sell placed successfully")
                else:
                    # SELL was a GTT — BUY is regular. Register BUY with no sibling for now.
                    # Store buy_order_id back into the GTT metadata so register_gtt_triggered_order()
                    # can pair them correctly when the GTT fires.
                    add_order_to_list(buy_order_id, final_buy_price, buy_quantity, kite.TRANSACTION_TYPE_BUY, symbol, "-1", unique_tag_buy)
                    orders[str(buy_order_id)]['multiplier_info'] = multiplier_info
                    if gtt_sell_tag and gtt_sell_tag in pending_gtt_fallbacks:
                        pending_gtt_fallbacks[gtt_sell_tag]['associated_regular_order_id'] = buy_order_id
                    logging.info(" -----------------------------   BUY placed successfully, SELL is GTT fallback")
            else:
                # BUY regular order failed — try GTT fallback only if market is still open.
                # After 15:30 IST, placing a GTT creates overnight gap risk.
                if not is_market_open():
                    logging.warning(
                        "BUY order failed for %s and market is closed — "
                        "suppressing GTT fallback to avoid overnight gap risk.",
                        symbol,
                    )
                    # Cancel the SELL side since we have no paired BUY
                    if sell_order_id != -1:
                        cancel_order(kite.VARIETY_REGULAR, sell_order_id)
                    if gtt_sell_tag and gtt_sell_tag in pending_gtt_fallbacks:
                        delete_gtt(pending_gtt_fallbacks[gtt_sell_tag]['trigger_id'])
                        del pending_gtt_fallbacks[gtt_sell_tag]
                else:
                    gtt_buy_tag = "GTT_B_" + str(int(time.time())) + "_" + str(randint(1000, 9999))
                    instrument_cache.save_gtt_algo_tag(gtt_buy_tag, tag)
                    buy_trigger_id = place_gtt_order(symbol, kite.VARIETY_REGULAR, kite.TRANSACTION_TYPE_BUY, exchange, typeOfProduct, final_buy_price, buy_quantity, gtt_buy_tag)
                    if buy_trigger_id != -1:
                        pending_gtt_fallbacks[gtt_buy_tag] = {
                            'trigger_id': buy_trigger_id, 'symbol': symbol, 'exchange': exchange,
                            'product': typeOfProduct, 'quantity': buy_quantity,
                            'transaction_type': kite.TRANSACTION_TYPE_BUY, 'price': final_buy_price,
                            'associated_regular_order_id': sell_order_id,  # -1 if SELL was also GTT
                            'associated_gtt_tag': gtt_sell_tag              # sibling GTT tag if SELL was GTT
                        }
                        # Cross-link: tell the SELL GTT about its sibling GTT
                        if gtt_sell_tag and gtt_sell_tag in pending_gtt_fallbacks:
                            pending_gtt_fallbacks[gtt_sell_tag]['associated_gtt_tag'] = gtt_buy_tag
                        logging.warning(f"BUY order failed, placed GTT fallback {gtt_buy_tag} at {final_buy_price}")
                    else:
                        # Both regular and GTT failed for BUY — cancel whatever SELL we placed
                        logging.error("BUY GTT fallback also failed — cancelling SELL side")
                        if sell_order_id != -1:
                            cancel_order(kite.VARIETY_REGULAR, sell_order_id)
                        if gtt_sell_tag and gtt_sell_tag in pending_gtt_fallbacks:
                            delete_gtt(pending_gtt_fallbacks[gtt_sell_tag]['trigger_id'])
                            del pending_gtt_fallbacks[gtt_sell_tag]
        
        old_quote_price = price
        duo_old_sell_price = final_sell_price 
        duo_old_buy_price = final_buy_price 




        print("Duo Order Buy and Sell Price Set -- "+str(duo_old_sell_price) + "---"+str(duo_old_buy_price))

        print("Duo Order Placed, if there is an empty array that means order was not placed")
        print("The case here can be that if its option and already have a buy order the buy order will not be placed. And the sell order can be restrictied because of delta restriction")
        print(orders)
        time.sleep(3)
    
    except (kite_exceptions.NetworkException, kite_exceptions.DataException) as e:
        logging.warning(f"Transient error in place_duo_order: {e}. Retrying after 5s delay...")
        already_executing_order = 0
        time.sleep(5)
        try:
            place_duo_order(symbol, typeOfProduct, exceptNFBNFLocal)
            return
        except Exception as retry_e:
            logging.error(f"Retry also failed in place_duo_order: {retry_e}")
    except Exception as e:
        logging.error("Error in place_duo_order: {}".format(e))
    finally:
        already_executing_order=0
        logging.info("Released semaphore in place_duo_order")


def get_last_sold_position(symbol_initials, symbol_type):
    current_all_positions = get_current_positions() 
    global all_instruments

    current_strike = -1
    current_token = -1

    for current_position in current_all_positions:
        instrument_token = current_position['instrument_token']
        tradingsymbol = current_position['tradingsymbol']

        if current_position['quantity'] >= 0: 
            continue

        if current_position['tradingsymbol'].startswith(symbol_initials) and  current_position['tradingsymbol'].endswith(symbol_type):
            if instrument_token in all_instruments:
                instrument_details = all_instruments[instrument_token]
                if symbol_type == "PE" and (current_strike == -1 or current_strike > instrument_details['strike']):
                    current_strike = instrument_details['strike']
                    current_token = instrument_token
                else :
                    if symbol_type == "CE" and (current_strike == -1 or current_strike < instrument_details['strike']):
                        current_strike = instrument_details['strike']
                        current_token = instrument_token

    return current_token    
    logging.error("Current Positions =  {}".format(current_all_positions))



def evaluate_optimal_hedge_margin(sell_symbol, base_qty, candidate_buy_symbols, max_acceptable_premium):
    """
    Evaluates the margin requirement for a sell position hedged with different buy strikes
    and returns the most efficient instrument.

    Args:
        sell_symbol: The symbol being sold.
        base_qty: The quantity to evaluate with.
        candidate_buy_symbols: List of candidate hedge instrument dicts.
        max_acceptable_premium: Absolute upper limit on premium cost.

    Returns:
        The best instrument dict, or the first one within acceptable premium if margin check fails.
    """
    global kite
    
    # 1. Calculate unhedged margin
    try:
        unhedged_param = [{
            "exchange": "BFO" if "SENSEX" in sell_symbol else "NFO",
            "tradingsymbol": sell_symbol,
            "transaction_type": "SELL",
            "variety": "regular",
            "product": "NRML",
            "order_type": "MARKET",
            "quantity": base_qty
        }]
        unhedged_margin = kite.basket_order_margins(unhedged_param, mode='compact')
        unhedged_total = unhedged_margin['initial_margin']['total']
    except Exception as e:
        logging.error(f"Error calculating unhedged margin for {sell_symbol}: {e}")
        unhedged_total = None

    best_efficiency = -1
    best_candidate = None
    fallback_candidate = None
    
    # 2. Evaluate candidates
    for candidate_inst in candidate_buy_symbols:
        buy_symbol = candidate_inst['tradingsymbol']
        exchange_buy = "BFO" if "SENSEX" in buy_symbol else "NFO"
        
        try:
            quote_data = get_quote_with_retry(f"{exchange_buy}:{buy_symbol}")
            buy_ltp = float(quote_data[f"{exchange_buy}:{buy_symbol}"]['last_price'])
        except Exception as e:
            logging.error(f"Error fetching LTP for candidate {buy_symbol}: {e}")
            continue
            
        if buy_ltp > max_acceptable_premium:
            continue
            
        if fallback_candidate is None:
            fallback_candidate = candidate_inst # First valid premium candidate

        if unhedged_total is None:
            continue # Can't evaluate efficiency, rely on fallback

        basket = [
            {
                "exchange": exchange_buy,
                "tradingsymbol": buy_symbol,
                "transaction_type": "BUY",
                "variety": "regular",
                "product": "NRML",
                "order_type": "MARKET",
                "quantity": base_qty
            },
            unhedged_param[0]
        ]
        
        try:
            margin_resp = kite.basket_order_margins(basket, mode='compact')
            total_margin = margin_resp['initial_margin']['total']
            
            premium_cost = buy_ltp * base_qty
            margin_saved = (unhedged_total - total_margin)
            
            # Efficiency: Margin reduction per rupee of premium spent
            efficiency = margin_saved / premium_cost if premium_cost > 0 else 0
            
            if efficiency > best_efficiency:
                best_efficiency = efficiency
                best_candidate = candidate_inst
                
        except Exception as e:
            logging.error(f"Error calculating hedged margin for {buy_symbol}: {e}")
            
    # Return best margin efficiency option. If API failed, return first option under max premium.
    return best_candidate if best_candidate else fallback_candidate


def find_nifty_symbol_auto_buy(symbol_initials, symbol_gap, symbol_type, max_allowed_price, sell_instrument_symbol):
    print("Find Nifty Symbol = "+symbol_initials+" -- "+str(symbol_gap)+" == "+symbol_type)

    orig_symbol_gap = symbol_gap

    nifty_quote_data = get_nifty_current_quote()
    nifty_last_price = nifty_quote_data['last_price']

    if symbol_type == "PE":
        symbol_gap = 0 - symbol_gap;

    gap_point = nifty_last_price+symbol_gap

    global all_instruments
    global nifty_strike_gap
    global exchange
    count_of_matching_symbol = 0
    final_instrument = {}



    sell_quote_data = get_quote_with_retry(exchange+":"+sell_instrument_symbol)
    sell_price = float(sell_quote_data[exchange+":"+sell_instrument_symbol]['last_price']) 
    print("Sell Price = "+str(sell_price))

    token_in_position = get_last_sold_position(symbol_initials, symbol_type)
    existing_price = 0
    existing_instrument_details = None
    if token_in_position == -1:
        print("No token found for closure")
    else:
        existing_instrument_details = all_instruments[token_in_position]
        existing_quote_data = get_quote_with_retry(exchange+":"+existing_instrument_details['tradingsymbol'])
        existing_price = float(existing_quote_data[exchange+":"+existing_instrument_details['tradingsymbol']]['last_price']) 
        
        # New 12% closing constraint
        CLOSE_POSITION_PCT_LIMIT = 0.12
        if existing_price <= (sell_price * CLOSE_POSITION_PCT_LIMIT):
            logging.info(f"Closing condition met: existing {existing_price} <= {sell_price * CLOSE_POSITION_PCT_LIMIT} (12% of {sell_price})")
            return existing_instrument_details

    # Generate Candidate Buy Strikes dynamically based on steps
    # Test strikes at 6, 8, 10, 12, and 16 steps away from the sell target gap
    gap_steps = [6, 8, 10, 12, 16]
    candidate_instruments = []
    
    for step in gap_steps:
        target_distance = symbol_gap + (nifty_strike_gap * step if symbol_type == "CE" else -(nifty_strike_gap * step))
        candidate_point = nifty_last_price + target_distance
        
        for tradingtoken in all_instruments:
            instrument_details = all_instruments[tradingtoken]
            if instrument_details['segment'] == exchange+"-OPT":
                if instrument_details['tradingsymbol'].startswith(symbol_initials) and symbol_type == instrument_details['instrument_type']:
                    instrument_strike_price = instrument_details['strike']
                    if abs(instrument_strike_price - candidate_point) <= nifty_strike_gap/2:
                        candidate_instruments.append(instrument_details)
                        break

    if not candidate_instruments:
        print("\n\n\n\n --- Matching Symbol candidates not found --- \n\n\n")
        # Legacy fallback if something completely goes wrong finding options
        return find_nifty_symbol_from_gap(symbol_initials, gap_point, symbol_type)

    # Use evaluate optimal margin function to determine best auto buy
    final_instrument = evaluate_optimal_hedge_margin(
        sell_symbol=sell_instrument_symbol,
        base_qty=nifty_lot_size, # Or base qty if accessible
        candidate_buy_symbols=candidate_instruments,
        max_acceptable_premium=max_allowed_price
    )
    
    if final_instrument is None:
         # Failsafe if API completely fails or no instruments meet the max premium criteria
         print("Failed to evaluate margin or no candidates within premium limit. Using legacy nearest candidate fallback.")
         final_instrument = candidate_instruments[-1] # Farthest OTM

    final_quote_data = get_quote_with_retry(exchange+":"+final_instrument['tradingsymbol'])
    final_price = float(final_quote_data[exchange+":"+final_instrument['tradingsymbol']]['last_price'])

    if token_in_position != -1:
        if final_price + 10 > existing_price: #This is to make sure that if the gap is less than 10 Rs then close the existing open position
            return existing_instrument_details

    return final_instrument




def find_nifty_symbol_from_gap(symbol_initials, symbol_gap, symbol_type):
    print("Find Nifty Symbol = "+symbol_initials+" -- "+str(symbol_gap)+" == "+symbol_type)
    nifty_quote_data = get_nifty_current_quote()
    nifty_last_price = nifty_quote_data['last_price']
    
    if symbol_type == "PE":
        symbol_gap = 0 - symbol_gap;

    gap_point = nifty_last_price + symbol_gap

    global nifty_strike_gap
    global exchange
    
    # Optimization: Use SQLite query to find the best matching strike
    # This avoids iterating over thousands of instruments in a Python loop.
    import instrument_cache
    conn = instrument_cache.get_db_connection()
    cursor = conn.cursor()
    
    # We want the instrument where |strike - gap_point| is minimized and within nifty_strike_gap/2
    # AND tradingsymbol starts with symbol_initials AND type matches.
    cursor.execute("""
        SELECT * FROM instruments 
        WHERE tradingsymbol LIKE ? 
        AND instrument_type = ? 
        AND segment = ?
        AND ABS(strike - ?) <= ?
        LIMIT 1
    """, (f"{symbol_initials}%", symbol_type, f"{exchange}-OPT", gap_point, nifty_strike_gap / 2))
    
    row = cursor.fetchone()
    conn.close()
    
    if row:
        final_instrument = dict(row)
        print(final_instrument)
        return final_instrument
    
    print("\n\n\n\n --- Matching Symbol not found --- \n\n\n")
    logging.error("Symbol not found for {} {} {}".format(symbol_initials, symbol_gap, symbol_type))
    return None


def get_sensex_current_quote() -> dict:
    return _get_index_quote_cached(sensex_symbol)


def find_sensex_symbol_from_gap(symbol_initials, symbol_gap, symbol_type):
    """
    Find a SENSEX option symbol at a specified gap from current SENSEX price.
    
    Args:
        symbol_initials: Option symbol prefix (e.g., 'SENSEX26JAN')
        symbol_gap: Points away from current price (positive value)
        symbol_type: 'PE' or 'CE'
    
    Returns:
        dict: Instrument details including 'tradingsymbol', 'strike', etc.
    """
    print("Find Sensex Symbol = " + symbol_initials + " -- " + str(symbol_gap) + " == " + symbol_type)
    sensex_quote_data = get_sensex_current_quote()
    sensex_last_price = sensex_quote_data['last_price']
    if symbol_type == "PE":
        symbol_gap = 0 - symbol_gap

    gap_point = sensex_last_price + symbol_gap

    global all_instruments
    global sensex_strike_gap
    # SENSEX options are always on BFO exchange, not NFO
    sensex_exchange = "BFO"
    
    final_instrument = {}
    for tradingtoken in all_instruments:
        instrument_details = all_instruments[tradingtoken]
        if instrument_details['segment'] == sensex_exchange + "-OPT":
            if instrument_details['tradingsymbol'].startswith(symbol_initials) and symbol_type == instrument_details['instrument_type']:
                instrument_strike_price = instrument_details['strike']
                # Use sensex_strike_gap/2 (50) for matching tolerance
                if abs(instrument_strike_price - gap_point) <= sensex_strike_gap / 2:
                    final_instrument = instrument_details
                    print(final_instrument)
                    break
    
    if final_instrument == {}:
        print("\n\n\n\n --- Matching SENSEX Symbol not found --- \n\n\n")
        logging.error("SENSEX Symbol not found for {} {} {}".format(symbol_initials, symbol_gap, symbol_type))
        return None
    return final_instrument


def find_sensex_symbol_auto_buy(symbol_initials, symbol_gap, symbol_type, max_allowed_price, sell_instrument_symbol):
    """
    Find a SENSEX option symbol for auto-buy hedging.
    
    Automatically selects a farther OTM option if the initial choice is too expensive,
    or uses an existing position if it provides better value.
    
    Args:
        symbol_initials: Option symbol prefix (e.g., 'SENSEX26JAN')
        symbol_gap: Points away from current price (positive value)
        symbol_type: 'PE' or 'CE'
        max_allowed_price: Maximum price to pay for the hedge option
        sell_instrument_symbol: The symbol being sold (for comparison)
    
    Returns:
        dict: Instrument details for the buy option
    """
    print("Find Sensex Symbol Auto Buy = " + symbol_initials + " -- " + str(symbol_gap) + " == " + symbol_type)

    orig_symbol_gap = symbol_gap

    sensex_quote_data = get_sensex_current_quote()
    sensex_last_price = sensex_quote_data['last_price']

    if symbol_type == "PE":
        symbol_gap = 0 - symbol_gap

    gap_point = sensex_last_price + symbol_gap

    global all_instruments
    global sensex_strike_gap
    # SENSEX options are always on BFO exchange, not NFO
    sensex_exchange = "BFO"
    
    final_instrument = {}

    sell_quote_data = get_quote_with_retry(sensex_exchange + ":" + sell_instrument_symbol)
    sell_price = float(sell_quote_data[sensex_exchange + ":" + sell_instrument_symbol]['last_price'])
    print("Sell Price = " + str(sell_price))

    token_in_position = get_last_sold_position(symbol_initials, symbol_type)
    existing_price = 0
    existing_instrument_details = None
    if token_in_position == -1:
        print("No token found for closure")
    else:
        existing_instrument_details = all_instruments[token_in_position]
        existing_quote_data = get_quote_with_retry(sensex_exchange + ":" + existing_instrument_details['tradingsymbol'])
        existing_price = float(existing_quote_data[sensex_exchange + ":" + existing_instrument_details['tradingsymbol']]['last_price'])
        
        # New 12% closing constraint
        CLOSE_POSITION_PCT_LIMIT = 0.12
        if existing_price <= (sell_price * CLOSE_POSITION_PCT_LIMIT):
            logging.info(f"Closing condition met: existing {existing_price} <= {sell_price * CLOSE_POSITION_PCT_LIMIT} (12% of {sell_price})")
            return existing_instrument_details

    # Generate Candidate Buy Strikes dynamically based on steps
    # Test strikes at 6, 8, 10, 12, and 16 steps away from the sell target gap
    gap_steps = [6, 8, 10, 12, 16]
    candidate_instruments = []
    
    for step in gap_steps:
        target_distance = symbol_gap + (sensex_strike_gap * step if symbol_type == "CE" else -(sensex_strike_gap * step))
        candidate_point = sensex_last_price + target_distance
        
        for tradingtoken in all_instruments:
            instrument_details = all_instruments[tradingtoken]
            if instrument_details['segment'] == sensex_exchange + "-OPT":
                if instrument_details['tradingsymbol'].startswith(symbol_initials) and symbol_type == instrument_details['instrument_type']:
                    instrument_strike_price = instrument_details['strike']
                    # Use sensex_strike_gap/2 (50) for matching tolerance
                    if abs(instrument_strike_price - candidate_point) <= sensex_strike_gap / 2:
                        candidate_instruments.append(instrument_details)
                        break

    if not candidate_instruments:
        print("\n\n\n\n --- Matching SENSEX Symbol candidates not found --- \n\n\n")
        logging.error("SENSEX Symbol candidates not found for auto_buy {} {} {}".format(symbol_initials, symbol_gap, symbol_type))
        # Legacy fallback
        return find_sensex_symbol_from_gap(symbol_initials, gap_point, symbol_type)

    # Use evaluate optimal margin function to determine best auto buy
    final_instrument = evaluate_optimal_hedge_margin(
        sell_symbol=sell_instrument_symbol,
        base_qty=sensex_lot_size, # Or base qty if accessible
        candidate_buy_symbols=candidate_instruments,
        max_acceptable_premium=max_allowed_price
    )

    if final_instrument is None:
         print("Failed to evaluate margin or no candidates within premium limit. Using legacy nearest candidate fallback.")
         final_instrument = candidate_instruments[-1]

    final_quote_data = get_quote_with_retry(sensex_exchange + ":" + final_instrument['tradingsymbol'])
    final_price = float(final_quote_data[sensex_exchange + ":" + final_instrument['tradingsymbol']]['last_price'])

    if token_in_position != -1:
        if final_price + 10 > existing_price:
            return existing_instrument_details

    return final_instrument


# ============================================================================
# Generic NFO Stock Option Functions
# ============================================================================

def get_stock_current_quote(stock_name: str) -> dict:
    """Fetch the current quote for any NSE-listed stock.

    Args:
        stock_name: Stock name as it appears on NSE (e.g., 'TCS', 'RELIANCE').

    Returns:
        dict: Quote data containing 'last_price', 'instrument_token', etc.

    Raises:
        Exception: If quote fetch fails after retry.
    """
    spot_symbol = f"NSE:{stock_name}"
    try:
        quote_data = kite.quote(spot_symbol)
    except Exception as e:
        time.sleep(randint(3, 5))
        quote_data = kite.quote(spot_symbol)
    return quote_data[spot_symbol]


def get_stock_strike_gap(stock_name: str) -> float:
    """Auto-detect the smallest strike gap for a stock from loaded instruments.

    Scans all loaded NFO-OPT instruments matching the stock name,
    collects unique strike prices, sorts them, and returns the smallest
    difference between consecutive strikes.

    Args:
        stock_name: Stock name (e.g., 'TCS', 'RELIANCE').

    Returns:
        float: Smallest strike gap in points. Defaults to 50 if not detected.
    """
    global all_instruments

    strikes = set()
    for tradingtoken in all_instruments:
        inst = all_instruments[tradingtoken]
        if inst.get('segment') == "NFO-OPT" and inst.get('name') == stock_name:
            strike = inst.get('strike')
            if strike is not None and strike > 0:
                strikes.add(float(strike))

    if len(strikes) < 2:
        logging.warning(
            f"Could not detect strike gap for {stock_name}, "
            f"found {len(strikes)} strikes. Defaulting to 50."
        )
        return 50.0

    sorted_strikes = sorted(strikes)
    min_gap = float('inf')
    for i in range(1, len(sorted_strikes)):
        diff = sorted_strikes[i] - sorted_strikes[i - 1]
        if diff > 0 and diff < min_gap:
            min_gap = diff

    logging.info(f"Auto-detected strike gap for {stock_name}: {min_gap}")
    return min_gap


def get_stock_lot_size(stock_name: str) -> int:
    """Auto-detect the lot size for a stock from loaded instruments.

    Scans NFO instruments matching the stock name and returns the lot size
    from the first matching option instrument found.

    Args:
        stock_name: Stock name (e.g., 'TCS', 'RELIANCE').

    Returns:
        int: Lot size. Defaults to 1 if not detected.
    """
    global all_instruments

    for tradingtoken in all_instruments:
        inst = all_instruments[tradingtoken]
        if (inst.get('segment') == "NFO-OPT"
                and inst.get('name') == stock_name
                and inst.get('lot_size')):
            lot_size = int(inst['lot_size'])
            logging.info(f"Auto-detected lot size for {stock_name}: {lot_size}")
            return lot_size

    logging.warning(f"Could not detect lot size for {stock_name}. Defaulting to 1.")
    return 1


def find_stock_symbol_from_gap(
    stock_name: str,
    symbol_initials: str,
    symbol_gap: float,
    symbol_type: str
) -> dict:
    """Find an NFO stock option symbol at a specified gap from current price.

    Generic version of find_nifty_symbol_from_gap for any NFO stock.

    Args:
        stock_name: Stock name (e.g., 'TCS', 'RELIANCE').
        symbol_initials: Option symbol prefix (e.g., 'TCS26FEB').
        symbol_gap: Points away from current price (positive value).
        symbol_type: 'PE' or 'CE'.

    Returns:
        dict: Instrument details including 'tradingsymbol', 'strike', etc.
              None if no matching instrument found.
    """
    print(f"Find Stock Symbol = {symbol_initials} -- {symbol_gap} == {symbol_type}")
    stock_quote_data = get_stock_current_quote(stock_name)
    stock_last_price = stock_quote_data['last_price']

    if symbol_type == "PE":
        symbol_gap = 0 - symbol_gap

    gap_point = stock_last_price + symbol_gap

    global all_instruments
    strike_gap = get_stock_strike_gap(stock_name)

    final_instrument = {}
    for tradingtoken in all_instruments:
        instrument_details = all_instruments[tradingtoken]
        if instrument_details.get('segment') == "NFO-OPT":
            if (instrument_details.get('tradingsymbol', '').startswith(symbol_initials)
                    and symbol_type == instrument_details.get('instrument_type')):
                instrument_strike_price = instrument_details.get('strike', 0)
                if abs(instrument_strike_price - gap_point) <= strike_gap / 2:
                    final_instrument = instrument_details
                    print(final_instrument)
                    break

    if final_instrument == {}:
        print(f"\n\n\n\n --- Matching {stock_name} Symbol not found --- \n\n\n")
        logging.error(
            f"{stock_name} Symbol not found for {symbol_initials} "
            f"{symbol_gap} {symbol_type}"
        )
        return None
    return final_instrument


def find_stock_symbol_auto_buy(
    stock_name: str,
    symbol_initials: str,
    symbol_gap: float,
    symbol_type: str,
    max_allowed_price: float,
    sell_instrument_symbol: str
) -> dict:
    """Find an NFO stock option symbol for auto-buy hedging.

    Generic version of find_nifty_symbol_auto_buy for any NFO stock.
    Automatically selects a farther OTM option if the initial choice is
    too expensive, or uses an existing position if it provides better value.

    Args:
        stock_name: Stock name (e.g., 'TCS', 'RELIANCE').
        symbol_initials: Option symbol prefix (e.g., 'TCS26FEB').
        symbol_gap: Points away from current price (positive value).
        symbol_type: 'PE' or 'CE'.
        max_allowed_price: Maximum price to pay for the hedge option.
        sell_instrument_symbol: The symbol being sold (for comparison).

    Returns:
        dict: Instrument details for the buy option. None if not found.
    """
    print(
        f"Find Stock Symbol Auto Buy = {symbol_initials} -- "
        f"{symbol_gap} == {symbol_type}"
    )

    orig_symbol_gap = symbol_gap
    stock_exchange = "NFO"

    stock_quote_data = get_stock_current_quote(stock_name)
    stock_last_price = stock_quote_data['last_price']

    if symbol_type == "PE":
        symbol_gap = 0 - symbol_gap

    gap_point = stock_last_price + symbol_gap

    global all_instruments
    strike_gap = get_stock_strike_gap(stock_name)

    final_instrument = {}

    sell_quote_data = get_quote_with_retry(
        f"{stock_exchange}:{sell_instrument_symbol}"
    )
    sell_price = float(
        sell_quote_data[f"{stock_exchange}:{sell_instrument_symbol}"]["last_price"]
    )
    print(f"Sell Price = {sell_price}")

    token_in_position = get_last_sold_position(symbol_initials, symbol_type)
    existing_price = 0
    existing_instrument_details = None
    if token_in_position == -1:
        print("No token found for closure")
    else:
        existing_instrument_details = all_instruments[token_in_position]
        existing_quote_data = get_quote_with_retry(
            f"{stock_exchange}:{existing_instrument_details['tradingsymbol']}"
        )
        existing_price = float(
            existing_quote_data[
                f"{stock_exchange}:{existing_instrument_details['tradingsymbol']}"
            ]["last_price"]
        )
        
        # New 12% closing constraint
        CLOSE_POSITION_PCT_LIMIT = 0.12
        if existing_price <= (sell_price * CLOSE_POSITION_PCT_LIMIT):
            logging.info(f"Closing condition met: existing {existing_price} <= {sell_price * CLOSE_POSITION_PCT_LIMIT} (12% of {sell_price})")
            return existing_instrument_details

    # Generate Candidate Buy Strikes dynamically based on steps
    # Test strikes at 6, 8, 10, 12, and 16 steps away from the sell target gap
    gap_steps = [6, 8, 10, 12, 16]
    candidate_instruments = []
    
    for step in gap_steps:
        target_distance = symbol_gap + (strike_gap * step if symbol_type == "CE" else -(strike_gap * step))
        candidate_point = stock_last_price + target_distance

        for tradingtoken in all_instruments:
            instrument_details = all_instruments[tradingtoken]
            if instrument_details.get('segment') == "NFO-OPT":
                if (instrument_details.get('tradingsymbol', '').startswith(symbol_initials)
                        and symbol_type == instrument_details.get('instrument_type')):
                    instrument_strike_price = instrument_details.get('strike', 0)
                    if abs(instrument_strike_price - candidate_point) <= strike_gap / 2:
                        candidate_instruments.append(instrument_details)
                        break

    if not candidate_instruments:
        print(f"\n\n\n\n --- Matching {stock_name} Symbol candidates not found --- \n\n\n")
        logging.error(f"{stock_name} Symbol candidates not found for auto_buy {symbol_initials} {symbol_gap} {symbol_type}")
        # Legacy fallback
        return find_stock_symbol_from_gap(stock_name, symbol_initials, gap_point, symbol_type)

    stock_lot_size = get_stock_lot_size(stock_name)

    # Use evaluate optimal margin function to determine best auto buy
    final_instrument = evaluate_optimal_hedge_margin(
        sell_symbol=sell_instrument_symbol,
        base_qty=stock_lot_size, 
        candidate_buy_symbols=candidate_instruments,
        max_acceptable_premium=max_allowed_price
    )

    if final_instrument is None:
         print("Failed to evaluate margin or no candidates within premium limit. Using legacy nearest candidate fallback.")
         final_instrument = candidate_instruments[-1]

    final_quote_data = get_quote_with_retry(
        f"{stock_exchange}:{final_instrument['tradingsymbol']}"
    )
    final_price = float(
        final_quote_data[
            f"{stock_exchange}:{final_instrument['tradingsymbol']}"
        ]["last_price"]
    )

    if token_in_position != -1:
        if final_price + 10 > existing_price:
            return existing_instrument_details

    return final_instrument




def get_expiry_for_symbol(symbol):
    global token_symbol_map
    global all_instruments
    
    # Needs logic to determine expiry for the running symbol
    # Assuming symbol is a valid tradingsymbol
    if symbol in token_symbol_map:
        token = token_symbol_map[symbol]
        if token in all_instruments:
             return str(all_instruments[token]['expiry'])
    return None

def _apply_delta_restriction(
    restrictions: dict,
    delta_val: float,
    effective_min: float,
    effective_max: float,
) -> None:
    """Mutate a single-underlying restrictions dict based on delta vs limits."""
    if delta_val < effective_min:
        restrictions['futures']['sell'] = "no"
        restrictions['ce']['sell'] = "no"
        restrictions['pe']['buy'] = "no"
    elif delta_val > effective_max:
        restrictions['futures']['buy'] = "no"
        restrictions['ce']['buy'] = "no"
        restrictions['pe']['sell'] = "no"


def _resolve_expiry_config(
    underlying_config: dict,
    expiry_date: str | None,
    legacy_min: float,
    legacy_max: float,
) -> tuple[float, float]:
    """Pick effective min/max from per-expiry or default config entry.

    Returns:
        Tuple of (effective_min, effective_max).
    """
    expiry_cfg = (underlying_config.get(expiry_date) if expiry_date else None) or underlying_config.get("default")
    if expiry_cfg:
        return (
            expiry_cfg.get("min", legacy_min),
            expiry_cfg.get("max", legacy_max),
        )
    return legacy_min, legacy_max


def set_restrictions() -> dict:
    """Compute buy/sell restrictions for the current running symbol.

    Uses per-expiry delta (not total portfolio delta) when checking against
    configured limits, so each wave-extractor instance is only restricted by
    its own expiry's risk — not the aggregate of all open positions.

    Returns:
        Dict with keys 'nifty', 'bank_nifty', 'sensex', each containing
        sub-dicts for 'futures', 'ce', 'pe' with 'buy'/'sell' values of
        'yes' or 'no'.
    """
    global min_nifty_delta, max_nifty_delta
    global min_bank_nifty_delta, max_bank_nifty_delta
    global delta_limits_config
    global symbol

    restrictions = reset_restrictions()

    # Lazy-load instrument metadata if not yet populated.
    # On scraper startup, all_instruments is empty until write_status_to_file() runs;
    # without this, the initial place_duo_order() call skips all delta restriction checks
    # because get_expiry_for_symbol() returns None when token_symbol_map is empty.
    if not all_instruments:
        try:
            get_all_fut_opt_instruments()
            _today = get_ist_now().date()
            for _details in all_instruments.values():
                _expiry_str = _details.get('expiry')
                if _expiry_str and 'days_to_expiry' not in _details:
                    try:
                        _expiry_date = datetime.datetime.strptime(str(_expiry_str), '%Y-%m-%d').date()
                        _details['days_to_expiry'] = int(np.busday_count(_today, _expiry_date) + 1)
                    except (ValueError, TypeError):
                        _details['days_to_expiry'] = 0
        except Exception as _e:
            logging.warning("set_restrictions: could not load instruments: %s", _e)

    current_expiry_date = get_expiry_for_symbol(symbol)

    # ---- NIFTY ---- (only fetched when this instance trades a NIFTY symbol)
    if symbol.startswith("NIFTY") and current_expiry_date:
        nifty_greeks = get_nifty_current_greeks()
        nifty_expiry_delta_map = nifty_greeks.get('expiry_delta', {})
        logging.info(f"NIFTY total delta: {nifty_greeks['delta']:.2f}")
        eff_min, eff_max = _resolve_expiry_config(
            delta_limits_config.get("NIFTY", {}),
            current_expiry_date,
            min_nifty_delta,
            max_nifty_delta,
        )
        # Always compare the expiry-specific delta against the limit — never total portfolio delta
        eff_delta = nifty_expiry_delta_map.get(current_expiry_date, 0)
        logging.info(f"NIFTY {current_expiry_date}: limits=[{eff_min}, {eff_max}], expiry_delta={eff_delta:.2f}")
        _apply_delta_restriction(restrictions['nifty'], eff_delta, eff_min, eff_max)

    # ---- BANKNIFTY ---- (only fetched when this instance trades a BANKNIFTY symbol)
    if symbol.startswith("BANKNIFTY") and current_expiry_date:
        bn_greeks = get_bank_nifty_current_greeks()
        bn_expiry_delta_map = bn_greeks.get('expiry_delta', {})
        logging.info(f"BANKNIFTY total delta: {bn_greeks['delta']:.2f}")
        eff_min, eff_max = _resolve_expiry_config(
            delta_limits_config.get("BANKNIFTY", {}),
            current_expiry_date,
            min_bank_nifty_delta,
            max_bank_nifty_delta,
        )
        eff_delta = bn_expiry_delta_map.get(current_expiry_date, 0)
        logging.info(f"BANKNIFTY {current_expiry_date}: limits=[{eff_min}, {eff_max}], expiry_delta={eff_delta:.2f}")
        _apply_delta_restriction(restrictions['bank_nifty'], eff_delta, eff_min, eff_max)

    # ---- SENSEX ----
    if symbol.startswith("SENSEX") and current_expiry_date:
        sensex_greeks = get_sensex_current_greeks()
        sensex_expiry_delta_map = sensex_greeks.get('expiry_delta', {})
        logging.info(f"SENSEX total delta: {sensex_greeks['delta']:.2f}")

        sensex_cfg = delta_limits_config.get("SENSEX", {})
        default_sensex_min = sensex_cfg.get("default", {}).get("min", -5000)
        default_sensex_max = sensex_cfg.get("default", {}).get("max", 5000)
        eff_min, eff_max = _resolve_expiry_config(
            sensex_cfg,
            current_expiry_date,
            default_sensex_min,
            default_sensex_max,
        )
        eff_delta = sensex_expiry_delta_map.get(current_expiry_date, 0)
        logging.info(f"SENSEX {current_expiry_date}: limits=[{eff_min}, {eff_max}], expiry_delta={eff_delta:.2f}")
        _apply_delta_restriction(restrictions['sensex'], eff_delta, eff_min, eff_max)

    return restrictions

# End of Set Restrictions Function


def initilise_basic(request_token_passed, kws_on_ticks = on_ticks, kws_on_connect= on_connect, kws_on_order_update = on_order_update):
    global request_token
    global access_token
    
    if request_token_passed and request_token_passed.startswith("access:"):
        access_token = request_token_passed.replace("access:", "")
        request_token = "" # No request token used
        logging.info("Initializing with provided access token")
        kite.set_access_token(access_token)
    else:
        request_token = request_token_passed
        data = kite.generate_session(request_token, api_secret)
        access_token = data["access_token"]
        kite.set_access_token(access_token)
    
    # Update lot sizes dynamically from instruments
    update_lot_sizes_from_instruments()
    
    initialise_ticker(kws_on_ticks, kws_on_connect, kws_on_order_update)




def initialise_ticker(kws_on_ticks, kws_on_connect, kws_on_order_update):
    """Initialize KiteTicker WebSocket connection.

    Stores callbacks for potential re-initialization by the watchdog
    if the connection is lost (e.g., after laptop sleep).

    Upstox edition: the KiteTicker runs its own websocket thread (no Twisted
    reactor). On a watchdog-triggered re-initialisation the previous ticker
    is closed first (so ticks are never delivered twice), a fresh one is
    started, and on_connect is wrapped to auto-resubscribe stored tokens.

    Args:
        kws_on_ticks: Callback for tick data.
        kws_on_connect: Callback on successful connection.
        kws_on_order_update: Callback for order updates.
    """
    global access_token
    global kws
    global _stored_ticker_callbacks
    global _subscribed_tokens

    # Store callbacks so watchdog can re-initialize later
    _stored_ticker_callbacks = {
        'on_ticks': kws_on_ticks,
        'on_connect': kws_on_connect,
        'on_order_update': kws_on_order_update
    }

    # Wrap on_connect to:
    #   1. Record _ws_last_connected_at so watchdog can distinguish "connected
    #      but no ticks (market closed)" from "never connected".
    #   2. Re-subscribe stored tokens after a watchdog-triggered reconnect.
    original_on_connect = kws_on_connect
    tokens_to_restore = list(_subscribed_tokens)
    previous_kws = globals().get("kws")
    is_reconnect = previous_kws is not None  # True on watchdog-triggered reconnects
    if previous_kws is not None:
        try:
            previous_kws.close()
        except Exception as e:  # noqa: BLE001
            logging.error("WATCHDOG: closing previous ticker failed: %s", e)

    def _wrapped_on_connect(ws, response):
        """Track connection time and re-subscribe tokens on reconnect."""
        global _ws_last_connected_at
        _ws_last_connected_at = time.time()
        original_on_connect(ws, response)
        if is_reconnect and tokens_to_restore:
            logging.error(
                "WATCHDOG: Re-subscribing to %d tokens after reconnect.",
                len(tokens_to_restore)
            )
            try:
                subscribe_for_tick(tokens_to_restore)
            except Exception as e:
                logging.error(
                    "WATCHDOG: Failed to re-subscribe tokens: %s", e
                )

    kws = KiteTicker(api_key, access_token)
    kws.on_ticks = kws_on_ticks
    kws.on_connect = _wrapped_on_connect
    kws.on_order_update = kws_on_order_update
    kws.on_noreconnect = _on_noreconnect_handler

    kws.connect(threaded=True)
    if is_reconnect:
        logging.error("WATCHDOG: Previous ticker closed; new WebSocket connection started.")


def _on_noreconnect_handler(ws):
    """Called when KiteTicker exhausts all reconnection attempts.

    Logs the event and records the timestamp so watchdog_sleep can detect
    that the connection is dead even when last_tick_time is still 0
    (e.g., connection dropped before the market opened).
    """
    global _ws_last_noreconnect_at
    _ws_last_noreconnect_at = time.time()
    logging.error("WATCHDOG: KiteTicker exhausted all reconnection attempts. "
                  "Watchdog will re-initialize the ticker.")


def update_last_tick_time():
    """Update the last tick timestamp. Call this from on_ticks callbacks."""
    global last_tick_time
    last_tick_time = time.time()


def write_spot_price(index_type: str, price: float) -> None:
    """Write the latest index spot price to a shared file for the dashboard.

    Each survivor instance calls this on every tick so the flask app
    can serve live NIFTY/SENSEX prices without its own kite auth.

    Args:
        index_type: 'NIFTY' or 'SENSEX'.
        price: The current index value from the tick.
    """
    try:
        spot_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "survivor_status", "spot_prices.json"
        )
        # Read existing data
        existing = {}
        if os.path.exists(spot_file):
            try:
                with open(spot_file, 'r') as f:
                    existing = json.load(f)
            except (json.JSONDecodeError, IOError):
                existing = {}

        # Update with new price
        key = f"{index_type.lower()}_price"
        existing[key] = price
        existing[f"{index_type.lower()}_updated"] = time.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        with open(spot_file, 'w') as f:
            json.dump(existing, f, indent=2)
    except Exception as e:
        logging.warning(f"Could not write spot price: {e}")


def set_trigger_spot_price(price: float, index_name: str) -> None:
    """Store the underlying spot price that triggered this order, for order history.

    Args:
        price: The current underlying spot price when the gap condition fires.
        index_name: Index name matching the write_spot_price() key (e.g. 'NIFTY', 'SENSEX', stock name).
    """
    global _pending_trigger_spot, _pending_trigger_index
    _pending_trigger_spot = price
    _pending_trigger_index = index_name


def _read_current_spot_price(index_name: str) -> float:
    """Read most-recent spot price from spot_prices.json for a given index.

    Args:
        index_name: Index key used when writing (e.g. 'NIFTY', 'SENSEX', 'TCS').

    Returns:
        Latest spot price, or 0.0 if unavailable.
    """
    try:
        spot_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "survivor_status", "spot_prices.json",
        )
        if os.path.exists(spot_file):
            with open(spot_file, "r") as f:
                data = json.load(f)
            return float(data.get(f"{index_name.lower()}_price", 0.0))
    except Exception:
        pass
    return 0.0


def watchdog_sleep(
    total_seconds: int = 22000,
    check_interval: int = 30,
    stale_threshold: int = 60,
    max_reconnect_attempts: int = 10,
    initial_connect_timeout: int = 300,
) -> None:
    """Sleep with periodic watchdog checks for WebSocket health.

    Replaces time.sleep(22000) in survivor scripts. Periodically checks
    if ticks have stopped flowing (or if the connection never established)
    and re-initializes the KiteTicker if needed.

    Three distinct states are handled:
    - ``last_tick_time == 0`` AND WebSocket connected (``_ws_last_connected_at``
      is set and no subsequent noreconnect): market is closed, ticks not
      expected — skip reconnect.
    - ``last_tick_time == 0`` AND connection never established OR noreconnect
      fired after last connect: dead connection — reconnect after
      ``initial_connect_timeout`` seconds.
    - ``last_tick_time > 0`` AND ticks have gone stale: dead connection —
      reconnect immediately.

    Args:
        total_seconds: Total duration to run in seconds.
        check_interval: How often to check connection health in seconds.
        stale_threshold: Seconds without ticks (after first tick received)
            before treating the connection as dead.
        max_reconnect_attempts: Max consecutive reconnect attempts before
            giving up and letting the script exit.
        initial_connect_timeout: Seconds to wait for the first WebSocket
            connection before forcing a reconnect. Protects against auth
            failures or network issues on startup.
    """
    global last_tick_time, kws, _stored_ticker_callbacks
    global _ws_last_connected_at, _ws_last_noreconnect_at

    startup_time = time.time()
    elapsed = 0
    consecutive_reconnects = 0

    while elapsed < total_seconds:
        time.sleep(check_interval)
        elapsed += check_interval

        needs_reconnect = False

        if last_tick_time == 0:
            # No ticks received yet (or reset after a reconnect attempt).
            ws_connected = _ws_last_connected_at > 0
            ws_dead = _ws_last_noreconnect_at > _ws_last_connected_at

            if ws_connected and not ws_dead:
                # WebSocket is alive; market is simply closed. No action needed.
                consecutive_reconnects = 0
                continue

            # Either never connected, or KiteTicker exhausted its own retries.
            # Use the later of startup_time and the noreconnect event as the
            # reference so the timeout resets after each failed attempt.
            reference = max(startup_time, _ws_last_noreconnect_at)
            waited = time.time() - reference
            if waited < initial_connect_timeout:
                continue  # Still within the startup grace window

            logging.error(
                "WATCHDOG: No WebSocket connection in %.0f seconds "
                "(timeout=%ds, connected_at=%.0f, noreconnect_at=%.0f). "
                "Forcing reconnect (attempt %d/%d).",
                waited, initial_connect_timeout,
                _ws_last_connected_at, _ws_last_noreconnect_at,
                consecutive_reconnects + 1, max_reconnect_attempts,
            )
            needs_reconnect = True
        else:
            time_since_last_tick = time.time() - last_tick_time
            if time_since_last_tick <= stale_threshold:
                consecutive_reconnects = 0
                continue

            logging.error(
                "WATCHDOG: No ticks for %.0f seconds (threshold=%ds). "
                "Re-initializing ticker... (attempt %d/%d)",
                time_since_last_tick, stale_threshold,
                consecutive_reconnects + 1, max_reconnect_attempts,
            )
            needs_reconnect = True

        if not needs_reconnect:
            continue

        consecutive_reconnects += 1
        if consecutive_reconnects > max_reconnect_attempts:
            logging.error(
                "WATCHDOG: Max reconnect attempts (%d) exceeded. "
                "Giving up on reconnection.",
                max_reconnect_attempts,
            )
            break

        try:
            if kws is not None:
                kws.close()
        except Exception as close_err:
            logging.warning("WATCHDOG: Error closing old ticker: %s", close_err)

        time.sleep(5)

        if _stored_ticker_callbacks:
            try:
                # Reset tracking so _wrapped_on_connect sets a fresh timestamp
                # and the startup timeout begins from now.
                _ws_last_connected_at = 0.0
                startup_time = time.time()
                initialise_ticker(
                    _stored_ticker_callbacks['on_ticks'],
                    _stored_ticker_callbacks['on_connect'],
                    _stored_ticker_callbacks['on_order_update'],
                )
                logging.error(
                    "WATCHDOG: Ticker re-initialized successfully. "
                    "Waiting for reconnection..."
                )
                last_tick_time = 0
                time.sleep(15)
            except Exception as reinit_err:
                logging.error(
                    "WATCHDOG: Failed to re-initialize ticker: %s", reinit_err
                )
        else:
            logging.error(
                "WATCHDOG: No stored callbacks, cannot re-initialize."
            )

    logging.error(
        "WATCHDOG: watchdog_sleep completed after %d seconds.", total_seconds
    )
