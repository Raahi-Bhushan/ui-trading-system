# -*- coding: utf-8 -*-
"""KiteConnect-compatible REST client backed by the Upstox API (v2/v3)."""
from __future__ import annotations

import datetime as _dt
import logging
import os
import sqlite3
import threading
import time
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote as _q, urlencode

import requests

from kiteconnect import exceptions as ex
from kiteconnect.instruments import (EXCH, EXPIRY, IST, ITYPE, LOT, SEG, STRIKE, TOKEN, TSYM, UKEY, InstrumentMap,
                                     fallback_from_ukey, ist_today)

log = logging.getLogger("kiteconnect.upstox")

PRODUCT_TO_UPSTOX = {"MIS": "I", "NRML": "D", "CNC": "D", "MTF": "MTF", "CO": "CO", "BO": "I"}
STATUS_MAP = {
    "open": "OPEN", "complete": "COMPLETE", "cancelled": "CANCELLED", "rejected": "REJECTED",
    "trigger pending": "TRIGGER PENDING", "after market order req received": "AMO REQ RECEIVED",
    "modify after market order req received": "AMO REQ RECEIVED", "cancelled after market order": "CANCELLED",
    "not modified": "OPEN", "not cancelled": "OPEN",
}
GTT_STATUS = {
    "scheduled": "active", "pending": "active", "open": "active", "active": "active",
    "triggered": "triggered", "completed": "triggered", "complete": "triggered", "executed": "triggered", "filled": "triggered",
    "cancelled": "cancelled", "canceled": "cancelled", "expired": "expired", "failed": "rejected", "rejected": "rejected",
}
HIST_INTERVALS = {  # kite interval -> (upstox unit, n, max days per request)
    "minute": ("minutes", 1, 28), "2minute": ("minutes", 2, 28), "3minute": ("minutes", 3, 28),
    "4minute": ("minutes", 4, 28), "5minute": ("minutes", 5, 28), "10minute": ("minutes", 10, 28),
    "15minute": ("minutes", 15, 28), "30minute": ("minutes", 30, 89), "60minute": ("hours", 1, 89),
    "2hour": ("hours", 2, 89), "3hour": ("hours", 3, 89), "4hour": ("hours", 4, 89),
    "day": ("days", 1, 3650), "week": ("weeks", 1, 3650), "month": ("months", 1, 3650),
}


def _ts(v: Any) -> Optional[_dt.datetime]:
    """Upstox timestamp (string / ISO / epoch-ms) -> naive IST datetime (Kite style)."""
    if v in (None, "", 0, "0"):
        return None
    try:
        if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()):
            n = int(v)
            return _dt.datetime.fromtimestamp(n / 1000 if n > 10**11 else n, IST).replace(tzinfo=None)
        s = str(v).replace("T", " ")
        d = _dt.datetime.fromisoformat(s[:26] if "+" not in s[19:] and "Z" not in s else s.replace("Z", "+00:00"))
        if d.tzinfo:
            d = d.astimezone(IST).replace(tzinfo=None)
        return d
    except (ValueError, OSError, OverflowError):
        try:
            import dateutil.parser
            d = dateutil.parser.parse(str(v))
            return d.astimezone(IST).replace(tzinfo=None) if d.tzinfo else d
        except Exception:
            return None


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def _i(v: Any, default: int = 0) -> int:
    try:
        return int(float(v)) if v is not None else default
    except (TypeError, ValueError):
        return default


class _GttIds:
    """Persistent int <-> Upstox GTT id map (Kite GTT ids are integers)."""

    def __init__(self) -> None:
        self.path = os.path.join(InstrumentMap.cache_dir(), "gtt_ids.sqlite")
        self.lock = threading.Lock()
        with self._db() as c:
            c.execute("CREATE TABLE IF NOT EXISTS gtt (id INTEGER PRIMARY KEY AUTOINCREMENT, upstox TEXT UNIQUE, last_price REAL)")
            c.execute("INSERT OR IGNORE INTO sqlite_sequence(name, seq) SELECT 'gtt', 100000 WHERE NOT EXISTS (SELECT 1 FROM sqlite_sequence WHERE name='gtt')")

    def _db(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.path, timeout=10)
        c.isolation_level = None
        return c

    def to_int(self, upstox_id: str, last_price: Optional[float] = None) -> int:
        with self.lock, self._db() as c:
            c.execute("INSERT OR IGNORE INTO gtt(upstox, last_price) VALUES (?, ?)", (upstox_id, last_price))
            if last_price is not None:
                c.execute("UPDATE gtt SET last_price=? WHERE upstox=?", (last_price, upstox_id))
            return int(c.execute("SELECT id FROM gtt WHERE upstox=?", (upstox_id,)).fetchone()[0])

    def to_upstox(self, gid: Any) -> str:
        with self._db() as c:
            r = c.execute("SELECT upstox FROM gtt WHERE id=?", (int(gid),)).fetchone()
        if not r:
            raise ex.InputException(f"Unknown GTT trigger_id {gid}", code=404)
        return r[0]

    def last_price(self, upstox_id: str) -> float:
        with self._db() as c:
            r = c.execute("SELECT last_price FROM gtt WHERE upstox=?", (upstox_id,)).fetchone()
        return float(r[0]) if r and r[0] is not None else 0.0


class KiteConnect(object):
    """Upstox-backed implementation of the pykiteconnect ``KiteConnect`` class."""

    # Constants (identical to pykiteconnect)
    PRODUCT_MIS = "MIS"
    PRODUCT_CNC = "CNC"
    PRODUCT_NRML = "NRML"
    PRODUCT_CO = "CO"
    PRODUCT_BO = "BO"
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_LIMIT = "LIMIT"
    ORDER_TYPE_SLM = "SL-M"
    ORDER_TYPE_SL = "SL"
    MARKET_PROTECTION_AUTO = -1
    VARIETY_REGULAR = "regular"
    VARIETY_BO = "bo"
    VARIETY_CO = "co"
    VARIETY_AMO = "amo"
    VARIETY_ICEBERG = "iceberg"
    TRANSACTION_TYPE_BUY = "BUY"
    TRANSACTION_TYPE_SELL = "SELL"
    VALIDITY_DAY = "DAY"
    VALIDITY_IOC = "IOC"
    VALIDITY_TTL = "TTL"
    POSITION_TYPE_DAY = "day"
    POSITION_TYPE_OVERNIGHT = "overnight"
    EXCHANGE_NSE = "NSE"
    EXCHANGE_BSE = "BSE"
    EXCHANGE_NFO = "NFO"
    EXCHANGE_CDS = "CDS"
    EXCHANGE_BFO = "BFO"
    EXCHANGE_MCX = "MCX"
    EXCHANGE_BCD = "BCD"
    MARGIN_EQUITY = "equity"
    MARGIN_COMMODITY = "commodity"
    STATUS_COMPLETE = "COMPLETE"
    STATUS_REJECTED = "REJECTED"
    STATUS_CANCELLED = "CANCELLED"
    GTT_TYPE_OCO = "two-leg"
    GTT_TYPE_SINGLE = "single"
    GTT_STATUS_ACTIVE = "active"
    GTT_STATUS_TRIGGERED = "triggered"
    GTT_STATUS_DISABLED = "disabled"
    GTT_STATUS_EXPIRED = "expired"
    GTT_STATUS_CANCELLED = "cancelled"
    GTT_STATUS_REJECTED = "rejected"
    GTT_STATUS_DELETED = "deleted"

    _default_root_uri = "https://api.upstox.com"
    _default_login_uri = "https://api.upstox.com/v2/login/authorization/dialog"
    _default_timeout = 7

    def __init__(self, api_key, access_token=None, root=None, debug=False, timeout=None, proxies=None, pool=None,
                 disable_ssl=False, redirect_uri=None):
        self.debug = debug
        self.api_key = api_key
        self.session_expiry_hook = None
        self.disable_ssl = disable_ssl
        self.access_token = access_token
        self.proxies = proxies if proxies else {}
        self.root = (root or self._default_root_uri).rstrip("/")
        self.timeout = timeout or self._default_timeout
        self.redirect_uri = redirect_uri or os.environ.get("UPSTOX_REDIRECT_URI", "")
        self.algo_name = os.environ.get("UPSTOX_ALGO_NAME", "").strip()
        self.reqsession = requests.Session()
        if pool:
            self.reqsession.mount("https://", requests.adapters.HTTPAdapter(**pool))
        self._gtt_ids: Optional[_GttIds] = None

    # ------------------------------------------------------------------ plumbing
    def _imap(self) -> InstrumentMap:
        return InstrumentMap.get(self.reqsession)

    def _gtt(self) -> _GttIds:
        if self._gtt_ids is None:
            self._gtt_ids = _GttIds()
        return self._gtt_ids

    def _user_agent(self) -> str:
        return "Kiteconnect-python-upstox/4.1.0"

    def _request(self, method: str, path: str, params: Optional[dict] = None, json_body: Any = None,
                 form: Optional[dict] = None, order_api: bool = False, raw: bool = False) -> Any:
        headers = {"Accept": "application/json", "User-Agent": self._user_agent()}
        if self.access_token:
            headers["Authorization"] = "Bearer " + str(self.access_token)
        if order_api and self.algo_name:
            headers["X-Algo-Name"] = self.algo_name
        url = self.root + path
        attempts = 2 if method == "GET" else 1  # never auto-retry anything that changes orders
        for attempt in range(attempts):
            try:
                r = self.reqsession.request(method, url, params=params, json=json_body, data=form, headers=headers,
                                            timeout=self.timeout, proxies=self.proxies, verify=not self.disable_ssl)
            except requests.RequestException as e:
                if attempt + 1 < attempts:
                    time.sleep(0.5)
                    continue
                raise ex.NetworkException(f"Upstox request failed: {e}", code=503)
            if self.debug:
                log.debug("Upstox %s %s -> %s %s", method, url, r.status_code, r.text[:500])
            if r.status_code == 429 and attempt + 1 < attempts:
                time.sleep(1.0)
                continue
            break
        try:
            data = r.json()
        except ValueError:
            if r.status_code >= 400:
                raise ex.NetworkException(f"Upstox HTTP {r.status_code}: {r.text[:200]}", code=r.status_code)
            raise ex.DataException(f"Couldn't parse the JSON response received from Upstox: {r.text[:200]}")
        if r.status_code >= 400 or (isinstance(data, dict) and data.get("status") == "error"):
            self._raise(r.status_code, data, order_api)
        if raw:
            return data
        return data.get("data") if isinstance(data, dict) else data

    def _raise(self, status: int, data: Any, order_api: bool) -> None:
        errs = (data or {}).get("errors") or []
        msg = "; ".join(str(e.get("message") or e.get("errorCode")) for e in errs if isinstance(e, dict)) \
            or str((data or {}).get("message") or data)
        code = errs[0].get("errorCode") if errs and isinstance(errs[0], dict) else ""
        text = f"{msg} [{code}]" if code else msg
        if status == 401 or "token" in msg.lower() and ("invalid" in msg.lower() or "expired" in msg.lower()):
            if self.session_expiry_hook:
                try:
                    self.session_expiry_hook()
                except Exception:  # noqa: BLE001
                    log.exception("session_expiry_hook failed")
            raise ex.TokenException(text, code=403)
        if status == 403:
            raise ex.PermissionException(text, code=403)
        if status == 429:
            raise ex.NetworkException(f"Too many requests: {text}", code=429)
        if status in (502, 503, 504):
            raise ex.NetworkException(text, code=status)
        if status >= 500:
            raise ex.GeneralException(text, code=status)
        if order_api:
            raise ex.OrderException(text, code=status or 400)
        raise ex.InputException(text, code=status or 400)

    # ------------------------------------------------------------------ instrument helpers
    def _row_for_kite(self, key: Any) -> Optional[tuple]:
        im = self._imap()
        if isinstance(key, int) or (isinstance(key, str) and key.isdigit()):
            return im.from_token(key)
        return im.from_kite_key(str(key))

    def _row_for_symbol(self, exchange: str, tradingsymbol: str) -> tuple:
        t = self._imap().from_symbol(exchange, tradingsymbol)
        if t is None:
            raise ex.InputException(f"Unknown instrument {exchange}:{tradingsymbol}", code=400)
        return t

    def _row_for_ukey(self, ukey: Optional[str], upstox_symbol: str = "") -> tuple:
        if not ukey:
            return fallback_from_ukey("NSE_EQ|" + (upstox_symbol or "UNKNOWN"), upstox_symbol)
        return self._imap().from_ukey(ukey) or fallback_from_ukey(ukey, upstox_symbol)

    @staticmethod
    def _kite_product(product: Optional[str], row: tuple) -> str:
        p = (product or "").upper()
        if p == "I":
            return "MIS"
        if p == "D":
            return "NRML" if row[EXCH] in ("NFO", "BFO") else "CNC"
        return p or "NRML"

    @staticmethod
    def _upstox_product(product: Optional[str]) -> str:
        p = (product or "NRML").upper()
        if p not in PRODUCT_TO_UPSTOX:
            raise ex.InputException(f"Unsupported product {product} on Upstox")
        return PRODUCT_TO_UPSTOX[p]

    # ------------------------------------------------------------------ session
    def set_session_expiry_hook(self, method):
        if not callable(method):
            raise TypeError("Invalid input type. Only functions are accepted.")
        self.session_expiry_hook = method

    def set_access_token(self, access_token):
        self.access_token = access_token

    def login_url(self):
        if not self.redirect_uri:
            log.warning("UPSTOX_REDIRECT_URI is not set - Upstox login will fail until it matches your Upstox app")
        return self._default_login_uri + "?" + urlencode(
            {"response_type": "code", "client_id": self.api_key, "redirect_uri": self.redirect_uri})

    def generate_session(self, request_token, api_secret):
        """Exchange the Upstox ``code`` (passed as Kite's request_token) for an access token."""
        d = self._request("POST", "/v2/login/authorization/token", form={
            "code": request_token, "client_id": self.api_key, "client_secret": api_secret,
            "redirect_uri": self.redirect_uri, "grant_type": "authorization_code"}, raw=True)
        token = d.get("access_token")
        if not token:
            raise ex.TokenException(f"Upstox did not return an access token: {d}")
        self.set_access_token(token)
        name = d.get("user_name") or ""
        return {
            "user_id": d.get("user_id"), "user_name": name, "user_shortname": name.split(" ")[0] if name else "",
            "email": d.get("email"), "user_type": d.get("user_type") or "individual", "broker": d.get("broker") or "UPSTOX",
            "exchanges": d.get("exchanges") or [], "products": d.get("products") or [], "order_types": d.get("order_types") or [],
            "avatar_url": None, "api_key": self.api_key, "access_token": token, "public_token": "", "refresh_token": "",
            "enctoken": "", "login_time": _dt.datetime.now(IST).replace(tzinfo=None, microsecond=0), "meta": {"demat_consent": ""},
        }

    def invalidate_access_token(self, access_token=None):
        tok = self.access_token
        if access_token:
            self.access_token = access_token
        try:
            self._request("DELETE", "/v2/logout")
        finally:
            self.access_token = tok
        return True

    def renew_access_token(self, refresh_token, api_secret):
        raise ex.TokenException("Upstox tokens cannot be renewed; log in again (they expire daily ~3:30 AM IST).")

    def invalidate_refresh_token(self, refresh_token):
        return True

    # ------------------------------------------------------------------ user
    def profile(self):
        d = self._request("GET", "/v2/user/profile") or {}
        name = d.get("user_name") or ""
        return {"user_id": d.get("user_id"), "user_name": name, "user_shortname": name.split(" ")[0] if name else "",
                "email": d.get("email"), "user_type": d.get("user_type") or "individual", "broker": d.get("broker") or "UPSTOX",
                "exchanges": d.get("exchanges") or [], "products": d.get("products") or [],
                "order_types": d.get("order_types") or [], "avatar_url": None, "meta": {"demat_consent": ""}}

    @staticmethod
    def _margin_segment(m: Dict[str, Any]) -> Dict[str, Any]:
        avail = _f(m.get("available_margin"))
        used = _f(m.get("used_margin"))
        return {
            "enabled": True,
            "net": avail,
            "available": {"adhoc_margin": _f(m.get("adhoc_margin")), "cash": avail, "opening_balance": avail + used,
                          "live_balance": avail, "collateral": 0.0, "intraday_payin": _f(m.get("payin_amount"))},
            "utilised": {"debits": used, "exposure": _f(m.get("exposure_margin")), "m2m_realised": 0.0, "m2m_unrealised": 0.0,
                         "option_premium": 0.0, "payout": 0.0, "span": _f(m.get("span_margin")), "holding_sales": 0.0,
                         "turnover": 0.0, "liquid_collateral": 0.0, "stock_collateral": 0.0, "delivery": 0.0},
        }

    def margins(self, segment=None):
        d = self._request("GET", "/v2/user/get-funds-and-margin") or {}
        out = {k: self._margin_segment(d.get(k) or {}) for k in ("equity", "commodity")}
        return out[segment] if segment else out

    # ------------------------------------------------------------------ orders
    def _order(self, o: Dict[str, Any]) -> Dict[str, Any]:
        row = self._row_for_ukey(o.get("instrument_token"), o.get("trading_symbol") or o.get("tradingsymbol") or "")
        status_raw = str(o.get("status") or "").strip().lower()
        status = STATUS_MAP.get(status_raw, status_raw.upper())
        qty, filled = _i(o.get("quantity")), _i(o.get("filled_quantity"))
        pending = _i(o.get("pending_quantity"), max(qty - filled, 0))
        done = status in ("COMPLETE", "CANCELLED", "REJECTED")
        tag = o.get("tag") or None
        return {
            "placed_by": o.get("placed_by") or "", "order_id": str(o.get("order_id") or ""),
            "exchange_order_id": o.get("exchange_order_id") or None, "parent_order_id": o.get("parent_order_id") or None,
            "status": status, "status_message": o.get("status_message") or None,
            "status_message_raw": o.get("status_message_raw") or o.get("status_message") or None,
            "order_timestamp": _ts(o.get("order_timestamp")), "exchange_update_timestamp": _ts(o.get("exchange_timestamp")),
            "exchange_timestamp": _ts(o.get("exchange_timestamp")),
            "variety": "amo" if o.get("is_amo") or str(o.get("variety") or "").upper() == "AMO" else "regular",
            "modified": False, "exchange": row[EXCH], "tradingsymbol": row[TSYM], "instrument_token": row[TOKEN],
            "order_type": o.get("order_type"), "transaction_type": o.get("transaction_type"),
            "validity": o.get("validity") or "DAY", "validity_ttl": 0, "product": self._kite_product(o.get("product"), row),
            "quantity": qty, "disclosed_quantity": _i(o.get("disclosed_quantity")), "price": _f(o.get("price")),
            "trigger_price": _f(o.get("trigger_price")), "average_price": _f(o.get("average_price")),
            "filled_quantity": filled, "pending_quantity": 0 if done else pending,
            "cancelled_quantity": max(qty - filled, 0) if status == "CANCELLED" else 0,
            "market_protection": 0, "meta": {}, "tag": tag, "tags": [tag] if tag else [],
            "guid": o.get("guid") or o.get("order_ref_id") or "", "auction_number": None,
        }

    def orders(self):
        return [self._order(o) for o in (self._request("GET", "/v2/order/retrieve-all") or [])]

    def order_history(self, order_id):
        return [self._order(o) for o in (self._request("GET", "/v2/order/history", params={"order_id": order_id}) or [])]

    def _trade(self, t: Dict[str, Any]) -> Dict[str, Any]:
        row = self._row_for_ukey(t.get("instrument_token"), t.get("trading_symbol") or t.get("tradingsymbol") or "")
        ts = _ts(t.get("exchange_timestamp")) or _ts(t.get("order_timestamp"))
        return {"trade_id": str(t.get("trade_id") or ""), "order_id": str(t.get("order_id") or ""),
                "exchange_order_id": t.get("exchange_order_id"), "tradingsymbol": row[TSYM], "exchange": row[EXCH],
                "instrument_token": row[TOKEN], "product": self._kite_product(t.get("product"), row),
                "average_price": _f(t.get("average_price")), "quantity": _i(t.get("quantity")),
                "fill_timestamp": ts, "exchange_timestamp": ts, "order_timestamp": _ts(t.get("order_timestamp")),
                "transaction_type": t.get("transaction_type")}

    def trades(self):
        return [self._trade(t) for t in (self._request("GET", "/v2/order/trades/get-trades-for-day") or [])]

    def order_trades(self, order_id):
        return [self._trade(t) for t in (self._request("GET", "/v2/order/trades", params={"order_id": order_id}) or [])]

    def place_order(self, variety, exchange, tradingsymbol, transaction_type, quantity, product, order_type, price=None,
                    validity=None, validity_ttl=None, disclosed_quantity=None, trigger_price=None, squareoff=None,
                    stoploss=None, trailing_stoploss=None, iceberg_legs=None, iceberg_quantity=None, tag=None,
                    market_protection=None, autoslice=None):
        if variety in (self.VARIETY_BO, self.VARIETY_CO, self.VARIETY_ICEBERG):
            raise ex.InputException(f"Order variety '{variety}' is not supported on Upstox")
        row = self._row_for_symbol(exchange, tradingsymbol)
        body = {
            "quantity": int(quantity), "product": self._upstox_product(product),
            "validity": "IOC" if validity == self.VALIDITY_IOC else "DAY",
            "price": float(price or 0), "tag": (tag or "")[:20], "instrument_token": row[UKEY],
            "order_type": order_type, "transaction_type": transaction_type,
            "disclosed_quantity": int(disclosed_quantity or 0), "trigger_price": float(trigger_price or 0),
            "is_amo": variety == self.VARIETY_AMO, "slice": bool(autoslice),
        }
        if market_protection not in (None, -1, self.MARKET_PROTECTION_AUTO):
            body["market_protection"] = float(market_protection)
        d = self._request("POST", "/v3/order/place", json_body=body, order_api=True) or {}
        ids = d.get("order_ids") or ([d["order_id"]] if d.get("order_id") else [])
        if not ids:
            raise ex.OrderException(f"Upstox accepted the order but returned no order id: {d}")
        if len(ids) > 1:
            log.info("Upstox sliced order into %d legs: %s", len(ids), ids)
        return str(ids[0])

    def modify_order(self, variety, order_id, parent_order_id=None, quantity=None, price=None, order_type=None,
                     trigger_price=None, validity=None, disclosed_quantity=None, market_protection=None):
        cur = self._request("GET", "/v2/order/details", params={"order_id": order_id}) or {}
        body = {
            "order_id": str(order_id),
            "quantity": int(quantity if quantity is not None else _i(cur.get("quantity"))),
            "validity": validity or cur.get("validity") or "DAY",
            "price": float(price if price is not None else _f(cur.get("price"))),
            "order_type": order_type or cur.get("order_type") or "LIMIT",
            "trigger_price": float(trigger_price if trigger_price is not None else _f(cur.get("trigger_price"))),
            "disclosed_quantity": int(disclosed_quantity if disclosed_quantity is not None else _i(cur.get("disclosed_quantity"))),
        }
        if market_protection not in (None, -1):
            body["market_protection"] = float(market_protection)
        d = self._request("PUT", "/v3/order/modify", json_body=body, order_api=True) or {}
        return str(d.get("order_id") or order_id)

    def cancel_order(self, variety, order_id, parent_order_id=None):
        d = self._request("DELETE", "/v3/order/cancel", params={"order_id": order_id}, order_api=True) or {}
        return str(d.get("order_id") or order_id)

    def exit_order(self, variety, order_id, parent_order_id=None):
        return self.cancel_order(variety, order_id, parent_order_id=parent_order_id)

    # ------------------------------------------------------------------ portfolio
    def _position(self, p: Dict[str, Any]) -> Dict[str, Any]:
        row = self._row_for_ukey(p.get("instrument_token"), p.get("trading_symbol") or p.get("tradingsymbol") or "")
        mult = _f(p.get("multiplier"), 1.0) or 1.0
        dbq, dsq = _i(p.get("day_buy_quantity")), _i(p.get("day_sell_quantity"))
        obq, osq = _i(p.get("overnight_buy_quantity")), _i(p.get("overnight_sell_quantity"))
        bv, sv = _f(p.get("buy_value")), _f(p.get("sell_value"))
        return {
            "tradingsymbol": row[TSYM], "exchange": row[EXCH], "instrument_token": row[TOKEN],
            "product": self._kite_product(p.get("product"), row), "quantity": _i(p.get("quantity")),
            "overnight_quantity": _i(p.get("overnight_quantity")), "multiplier": mult,
            "average_price": _f(p.get("average_price")), "close_price": _f(p.get("close_price")),
            "last_price": _f(p.get("last_price")), "value": sv - bv, "pnl": _f(p.get("pnl")), "m2m": _f(p.get("pnl")),
            "unrealised": _f(p.get("unrealised")), "realised": _f(p.get("realised")),
            "buy_quantity": dbq + obq, "buy_price": _f(p.get("buy_price")), "buy_value": bv, "buy_m2m": bv,
            "sell_quantity": dsq + osq, "sell_price": _f(p.get("sell_price")), "sell_value": sv, "sell_m2m": sv,
            "day_buy_quantity": dbq, "day_buy_price": _f(p.get("day_buy_price")), "day_buy_value": _f(p.get("day_buy_value")),
            "day_sell_quantity": dsq, "day_sell_price": _f(p.get("day_sell_price")), "day_sell_value": _f(p.get("day_sell_value")),
        }

    def positions(self):
        net = [self._position(p) for p in (self._request("GET", "/v2/portfolio/short-term-positions") or [])]
        day = []
        for p in net:
            if p["day_buy_quantity"] or p["day_sell_quantity"]:
                q = p["day_buy_quantity"] - p["day_sell_quantity"]
                d = dict(p)
                d.update(quantity=q, overnight_quantity=0, buy_quantity=p["day_buy_quantity"], buy_price=p["day_buy_price"],
                         buy_value=p["day_buy_value"], sell_quantity=p["day_sell_quantity"], sell_price=p["day_sell_price"],
                         sell_value=p["day_sell_value"],
                         average_price=p["day_buy_price"] if q > 0 else p["day_sell_price"] if q < 0 else 0.0,
                         value=p["day_sell_value"] - p["day_buy_value"])
                d["pnl"] = d["m2m"] = d["value"] + q * p["last_price"] * p["multiplier"]
                day.append(d)
        return {"net": net, "day": day}

    def holdings(self):
        out = []
        for h in self._request("GET", "/v2/portfolio/long-term-holdings") or []:
            row = self._row_for_ukey(h.get("instrument_token"), h.get("trading_symbol") or h.get("tradingsymbol") or "")
            out.append({
                "tradingsymbol": row[TSYM], "exchange": row[EXCH], "instrument_token": row[TOKEN], "isin": h.get("isin"),
                "product": "CNC", "price": 0.0, "quantity": _i(h.get("quantity")), "used_quantity": _i(h.get("cnc_used_quantity")),
                "t1_quantity": _i(h.get("t1_quantity")), "realised_quantity": _i(h.get("quantity")),
                "authorised_quantity": 0, "authorised_date": None, "opening_quantity": _i(h.get("quantity")),
                "collateral_quantity": _i(h.get("collateral_quantity")), "collateral_type": h.get("collateral_type") or "",
                "discrepancy": False, "average_price": _f(h.get("average_price")), "last_price": _f(h.get("last_price")),
                "close_price": _f(h.get("close_price")), "pnl": _f(h.get("pnl")), "day_change": _f(h.get("day_change")),
                "day_change_percentage": _f(h.get("day_change_percentage")),
            })
        return out

    def convert_position(self, exchange, tradingsymbol, transaction_type, position_type, quantity, old_product, new_product):
        row = self._row_for_symbol(exchange, tradingsymbol)
        self._request("PUT", "/v2/portfolio/convert-position", json_body={
            "instrument_token": row[UKEY], "new_product": self._upstox_product(new_product),
            "old_product": self._upstox_product(old_product), "transaction_type": transaction_type, "quantity": int(quantity)},
            order_api=True)
        return True

    # ------------------------------------------------------------------ mutual funds (not on Upstox API)
    def _no_mf(self, *a, **k):
        raise ex.InputException("Mutual fund APIs are not available on Upstox")

    mf_orders = place_mf_order = cancel_mf_order = mf_sips = place_mf_sip = modify_mf_sip = cancel_mf_sip = _no_mf

    def mf_holdings(self):
        return []

    def mf_instruments(self):
        return []

    # ------------------------------------------------------------------ market data
    def instruments(self, exchange=None):
        return self._imap().kite_rows(exchange)

    def _resolve(self, instruments: tuple) -> Dict[str, List[str]]:
        ins = list(instruments)
        if len(instruments) > 0 and isinstance(instruments[0], (list, tuple)):
            ins = list(instruments[0])
        out: Dict[str, List[str]] = {}
        for k in ins:
            t = self._row_for_kite(k)
            if t is None:
                log.debug("quote: unknown instrument %s", k)
                continue
            out.setdefault(t[UKEY], []).append(str(k))
        return out

    def _fetch(self, path: str, ukeys: List[str], extra: Optional[dict] = None) -> Dict[str, Any]:
        res: Dict[str, Any] = {}
        for i in range(0, len(ukeys), 500):
            params = {"instrument_key": ",".join(ukeys[i:i + 500])}
            params.update(extra or {})
            data = self._request("GET", path, params=params) or {}
            for v in data.values():
                uk = v.get("instrument_token") or v.get("instrument_key")
                if uk:
                    res[uk] = v
        return res

    def quote(self, *instruments):
        want = self._resolve(instruments)
        out = {}
        for uk, v in self._fetch("/v2/market-quote/quotes", list(want)).items():
            row = self._imap().from_ukey(uk) or fallback_from_ukey(uk)
            lp = _f(v.get("last_price"))
            nc = _f(v.get("net_change"))
            o = v.get("ohlc") or {}
            depth = v.get("depth") or {}

            def side(name):
                lv = [{"price": _f(x.get("price")), "quantity": _i(x.get("quantity")), "orders": _i(x.get("orders"))}
                      for x in (depth.get(name) or [])][:5]
                return lv + [{"price": 0.0, "quantity": 0, "orders": 0}] * (5 - len(lv))

            q = {"instrument_token": row[TOKEN], "timestamp": _ts(v.get("timestamp")),
                 "last_trade_time": _ts(v.get("last_trade_time")), "last_price": lp, "last_quantity": 0,
                 "buy_quantity": _i(v.get("total_buy_quantity")), "sell_quantity": _i(v.get("total_sell_quantity")),
                 "volume": _i(v.get("volume")), "average_price": _f(v.get("average_price")), "oi": _f(v.get("oi")),
                 "oi_day_high": _f(v.get("oi_day_high")), "oi_day_low": _f(v.get("oi_day_low")), "net_change": nc,
                 "lower_circuit_limit": _f(v.get("lower_circuit_limit")), "upper_circuit_limit": _f(v.get("upper_circuit_limit")),
                 "ohlc": {"open": _f(o.get("open")), "high": _f(o.get("high")), "low": _f(o.get("low")),
                          "close": round(lp - nc, 4) if lp else _f(o.get("close"))},
                 "depth": {"buy": side("buy"), "sell": side("sell")}}
            for k in want.get(uk, []):
                out[k] = q
        return out

    def ltp(self, *instruments):
        want = self._resolve(instruments)
        out = {}
        for uk, v in self._fetch("/v3/market-quote/ltp", list(want)).items():
            row = self._imap().from_ukey(uk) or fallback_from_ukey(uk)
            for k in want.get(uk, []):
                out[k] = {"instrument_token": row[TOKEN], "last_price": _f(v.get("last_price"))}
        return out

    def ohlc(self, *instruments):
        want = self._resolve(instruments)
        out = {}
        for uk, v in self._fetch("/v3/market-quote/ohlc", list(want), {"interval": "1d"}).items():
            row = self._imap().from_ukey(uk) or fallback_from_ukey(uk)
            live, prev = v.get("live_ohlc") or {}, v.get("prev_ohlc") or {}
            for k in want.get(uk, []):
                out[k] = {"instrument_token": row[TOKEN], "last_price": _f(v.get("last_price")),
                          "ohlc": {"open": _f(live.get("open")), "high": _f(live.get("high")), "low": _f(live.get("low")),
                                   "close": _f(prev.get("close"))}}
        return out

    def trigger_range(self, transaction_type, *instruments):
        raise ex.InputException("trigger_range (cover orders) is not available on Upstox")

    def historical_data(self, instrument_token, from_date, to_date, interval, continuous=False, oi=False):
        row = self._imap().from_token(instrument_token)
        if row is None:
            raise ex.InputException(f"Unknown instrument_token {instrument_token}")
        if interval not in HIST_INTERVALS:
            raise ex.InputException(f"Unsupported interval {interval}")
        unit, n, span = HIST_INTERVALS[interval]

        def as_dt(v, end=False):
            if isinstance(v, _dt.datetime):
                return v.replace(tzinfo=None) if v.tzinfo is None else v.astimezone(IST).replace(tzinfo=None)
            if isinstance(v, _dt.date):
                return _dt.datetime.combine(v, _dt.time(23, 59, 59) if end else _dt.time(0, 0))
            s = str(v)
            d = _dt.datetime.fromisoformat(s if len(s) > 10 else s + (" 23:59:59" if end else " 00:00:00"))
            return d.replace(tzinfo=None) if d.tzinfo is None else d.astimezone(IST).replace(tzinfo=None)

        f_dt, t_dt = as_dt(from_date), as_dt(to_date, end=True)
        uk = _q(row[UKEY], safe="")
        candles: Dict[str, list] = {}
        today = ist_today()
        a = f_dt.date()
        last_hist_day = min(t_dt.date(), today - _dt.timedelta(days=1))
        while a <= last_hist_day:
            b = min(last_hist_day, a + _dt.timedelta(days=span - 1))
            d = self._request("GET", f"/v3/historical-candle/{uk}/{unit}/{n}/{b.isoformat()}/{a.isoformat()}") or {}
            for c in d.get("candles") or []:
                candles[c[0]] = c
            a = b + _dt.timedelta(days=1)
        if t_dt.date() >= today and unit in ("minutes", "hours", "days"):
            d = self._request("GET", f"/v3/historical-candle/intraday/{uk}/{unit}/{n}") or {}
            for c in d.get("candles") or []:
                candles[c[0]] = c
        import dateutil.parser
        out = []
        for c in candles.values():
            dt = dateutil.parser.parse(c[0])
            naive = dt.astimezone(IST).replace(tzinfo=None) if dt.tzinfo else dt
            if naive < f_dt or naive > t_dt:
                continue
            rec = {"date": dt, "open": c[1], "high": c[2], "low": c[3], "close": c[4], "volume": c[5] if len(c) > 5 else 0}
            if oi:
                rec["oi"] = c[6] if len(c) > 6 else 0
            out.append(rec)
        out.sort(key=lambda r: r["date"])
        return out

    # ------------------------------------------------------------------ margins
    def _margin_request(self, params: List[Dict[str, Any]]) -> Dict[str, Any]:
        ins = []
        for p in params:
            row = self._row_for_symbol(p["exchange"], p["tradingsymbol"])
            ins.append({"instrument_key": row[UKEY], "quantity": int(p["quantity"]),
                        "product": self._upstox_product(p.get("product")), "transaction_type": p["transaction_type"],
                        "price": float(p.get("price") or 0)})
        return self._request("POST", "/v2/charges/margin", json_body={"instruments": ins}) or {}

    @staticmethod
    def _margin_obj(m: Dict[str, Any], p: Optional[Dict[str, Any]] = None, total: Optional[float] = None) -> Dict[str, Any]:
        return {"type": "equity", "tradingsymbol": (p or {}).get("tradingsymbol", ""), "exchange": (p or {}).get("exchange", ""),
                "span": _f(m.get("span_margin")), "exposure": _f(m.get("exposure_margin")),
                "option_premium": _f(m.get("net_buy_premium")), "additional": _f(m.get("additional_margin")),
                "bo": 0.0, "cash": 0.0, "var": _f(m.get("equity_margin")), "pnl": {"realised": 0.0, "unrealised": 0.0},
                "leverage": 1, "charges": {"total": 0.0},
                "total": _f(m.get("total_margin")) if total is None else float(total)}

    def order_margins(self, params):
        d = self._margin_request(params)
        return [self._margin_obj(m, p) for m, p in zip(d.get("margins") or [], params)]

    def basket_order_margins(self, params, consider_positions=True, mode=None):
        d = self._margin_request(params)
        ms = d.get("margins") or []
        agg = {k: sum(_f(m.get(k)) for m in ms) for k in ("span_margin", "exposure_margin", "net_buy_premium",
                                                          "additional_margin", "equity_margin", "total_margin")}
        initial = self._margin_obj(agg, total=_f(d.get("required_margin")) or agg["total_margin"])
        final = self._margin_obj(agg, total=_f(d.get("final_margin")) or initial["total"])
        res = {"initial": initial, "final": final, "orders": [self._margin_obj(m, p) for m, p in zip(ms, params)],
               "charges": {"total": 0.0}}
        res["initial_margin"], res["final_margin"] = initial, final
        return res

    # ------------------------------------------------------------------ GTT
    def _gtt_kite(self, g: Dict[str, Any]) -> Dict[str, Any]:
        uid = str(g.get("gtt_order_id") or "")
        rules = g.get("rules") or []
        rule = next((r for r in rules if str(r.get("strategy", "")).upper() == "ENTRY"), rules[0] if rules else {})
        row = self._row_for_ukey(g.get("instrument_token"), g.get("trading_symbol") or "")
        trig = _f(rule.get("trigger_price"))
        st = GTT_STATUS.get(str(rule.get("status") or g.get("status") or "").lower(), str(rule.get("status") or "active").lower())
        product = self._kite_product(g.get("product"), row)
        result = None
        if rule.get("order_id"):
            result = {"order_result": {"order_id": rule.get("order_id"), "status": "success", "rejection_reason": ""}}
        return {
            "id": self._gtt().to_int(uid), "user_id": "", "parent_trigger": None,
            "type": "single" if str(g.get("type", "SINGLE")).upper() == "SINGLE" else "two-leg",
            "created_at": _ts(g.get("created_at")), "updated_at": _ts(g.get("created_at")), "expires_at": _ts(g.get("expires_at")),
            "status": st,
            "condition": {"exchange": row[EXCH], "last_price": self._gtt().last_price(uid), "tradingsymbol": row[TSYM],
                          "trigger_values": [trig], "instrument_token": row[TOKEN]},
            "orders": [{"exchange": row[EXCH], "tradingsymbol": row[TSYM], "product": product, "order_type": "LIMIT",
                        "transaction_type": rule.get("transaction_type") or g.get("transaction_type"),
                        "quantity": _i(g.get("quantity")), "price": trig, "result": result}],
            "meta": {"upstox_gtt_order_id": uid, "message": rule.get("message")},
        }

    def get_gtts(self):
        return [self._gtt_kite(g) for g in (self._request("GET", "/v3/order/gtt") or [])]

    def get_gtt(self, trigger_id):
        uid = self._gtt().to_upstox(trigger_id)
        d = self._request("GET", "/v3/order/gtt", params={"gtt_order_id": uid}) or []
        if isinstance(d, dict):
            d = [d]
        if not d:
            raise ex.InputException(f"GTT {trigger_id} not found", code=404)
        return self._gtt_kite(d[0])

    def _gtt_rule(self, trigger_type, trigger_values, last_price, orders):
        if not isinstance(trigger_values, list):
            raise ex.InputException("invalid type for `trigger_values`")
        if trigger_type != self.GTT_TYPE_SINGLE:
            raise ex.InputException("Two-leg (OCO) GTT is not supported on Upstox")
        if len(trigger_values) != 1 or len(orders) != 1:
            raise ex.InputException("invalid `trigger_values` for single leg order type")
        o = orders[0]
        for req in ("transaction_type", "quantity", "order_type", "product", "price"):
            if req not in o:
                raise ex.InputException(f"`{req}` missing inside orders")
        trig = float(trigger_values[0])
        if abs(float(o["price"]) - trig) > 1e-9:
            log.warning("Upstox GTT fires a LIMIT at the trigger price %.2f; requested limit %.2f is ignored", trig, float(o["price"]))
        lp = float(last_price or 0)
        tt = "IMMEDIATE" if lp and abs(trig - lp) < 1e-9 else ("ABOVE" if trig > lp else "BELOW")
        return o, {"strategy": "ENTRY", "trigger_type": tt, "trigger_price": trig}

    def place_gtt(self, trigger_type, tradingsymbol, exchange, trigger_values, last_price, orders):
        o, rule = self._gtt_rule(trigger_type, trigger_values, last_price, orders)
        row = self._row_for_symbol(exchange, tradingsymbol)
        d = self._request("POST", "/v3/order/gtt/place", order_api=True, json_body={
            "type": "SINGLE", "quantity": int(o["quantity"]), "product": self._upstox_product(o["product"]),
            "rules": [rule], "instrument_token": row[UKEY], "transaction_type": o["transaction_type"]}) or {}
        ids = d.get("gtt_order_ids") or []
        if not ids:
            raise ex.OrderException(f"Upstox returned no GTT id: {d}")
        return {"trigger_id": self._gtt().to_int(str(ids[0]), float(last_price or 0))}

    def modify_gtt(self, trigger_id, trigger_type, tradingsymbol, exchange, trigger_values, last_price, orders):
        o, rule = self._gtt_rule(trigger_type, trigger_values, last_price, orders)
        uid = self._gtt().to_upstox(trigger_id)
        self._request("PUT", "/v3/order/gtt/modify", order_api=True, json_body={
            "type": "SINGLE", "quantity": int(o["quantity"]), "rules": [rule], "gtt_order_id": uid})
        self._gtt().to_int(uid, float(last_price or 0))
        return {"trigger_id": int(trigger_id)}

    def delete_gtt(self, trigger_id):
        uid = self._gtt().to_upstox(trigger_id)
        self._request("DELETE", "/v3/order/gtt/cancel", json_body={"gtt_order_id": uid}, order_api=True)
        return {"trigger_id": int(trigger_id)}
