# -*- coding: utf-8 -*-
"""KiteTicker-compatible websocket client backed by the Upstox v3 market feed.

* Market data: wss://api.upstox.com/v3/feed/market-data-feed (protobuf)
* Order updates: wss://api.upstox.com/v2/feed/portfolio-stream-feed (JSON),
  started only when ``on_order_update`` is set before ``connect()``.

Tokens are the integer ``instrument_token`` values produced by
``KiteConnect.instruments()``.  Ticks are delivered as Kite-shaped dicts via
``on_ticks(ws, ticks)`` where ``ws`` is this KiteTicker instance.  TLS
certificates are verified (unlike the Upstox SDK's own feeder).
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
import threading
import time
import uuid
from typing import Any, Callable, Dict, Iterable, List, Optional

from kiteconnect.instruments import IST, TOKEN, UKEY, InstrumentMap, fallback_from_ukey

log = logging.getLogger("kiteconnect.upstox.ticker")

MARKET_FEED_URL = "wss://api.upstox.com/v3/feed/market-data-feed"
PORTFOLIO_FEED_URL = "wss://api.upstox.com/v2/feed/portfolio-stream-feed?update_types=order"
UPSTOX_MODE = {"ltp": "ltpc", "quote": "full", "full": "full"}


def _pb():
    from upstox_client.feeder.proto import MarketDataFeedV3_pb2  # provided by upstox-python-sdk
    return MarketDataFeedV3_pb2


def _ms_to_dt(v: Any) -> Optional[_dt.datetime]:
    try:
        n = int(v)
        if n <= 0:
            return None
        return _dt.datetime.fromtimestamp(n / 1000.0, IST).replace(tzinfo=None)
    except (TypeError, ValueError, OSError):
        return None


class _Loop:
    """websocket-client run_forever with Kite-like reconnect behaviour."""

    def __init__(self, owner: "KiteTicker", url: str, on_open, on_message, name: str):
        self.owner, self.url, self.on_open_cb, self.on_message_cb, self.name = owner, url, on_open, on_message, name
        self.app = None
        self.connected = False
        self.stop = False
        self.thread: Optional[threading.Thread] = None

    def start(self) -> threading.Thread:
        self.thread = threading.Thread(target=self._run, name=f"upstox-{self.name}", daemon=True)
        self.thread.start()
        return self.thread

    def _run(self) -> None:
        import websocket  # websocket-client
        o = self.owner
        tries = 0
        while not self.stop:
            self.app = websocket.WebSocketApp(
                self.url, header=[f"Authorization: Bearer {o.access_token}", "Accept: */*"],
                on_open=self._opened, on_message=lambda ws, m: self.on_message_cb(m),
                on_error=lambda ws, e: o._emit("on_error", o, 0, str(e)),
                on_close=lambda ws, code, reason: self._closed(code, reason))
            self.app.run_forever(ping_interval=20, ping_timeout=10)
            self.connected = False
            if self.stop or not o.should_reconnect:
                break
            tries = 1 if o._reset_tries.pop(self.name, False) else tries + 1
            if tries > o.reconnect_max_tries:
                o._emit("on_noreconnect", o)
                break
            delay = min(2 ** tries, o.reconnect_max_delay)
            o._emit("on_reconnect", o, tries)
            log.warning("Upstox %s feed disconnected - reconnect %d in %ss", self.name, tries, delay)
            for _ in range(int(delay * 10)):
                if self.stop:
                    return
                time.sleep(0.1)

    def _opened(self, ws) -> None:
        self.connected = True
        self.owner._reset_tries[self.name] = True
        self.on_open_cb()

    def _closed(self, code, reason) -> None:
        self.connected = False
        self.owner._on_feed_closed(self.name, code, reason)

    def send(self, payload: bytes) -> None:
        import websocket
        if not (self.app and self.connected):
            raise RuntimeError("WebSocket is not connected")
        self.app.send(payload, opcode=websocket.ABNF.OPCODE_BINARY)

    def close(self) -> None:
        self.stop = True
        try:
            if self.app:
                self.app.close(status=1000)
        except Exception:  # noqa: BLE001
            pass


class KiteTicker(object):
    """Upstox-backed implementation of pykiteconnect's ``KiteTicker``."""

    EXCHANGE_MAP = {"nse": 1, "nfo": 2, "cds": 3, "bse": 4, "bfo": 5, "bcd": 6, "mcx": 7, "mcxsx": 8, "indices": 9}
    CONNECT_TIMEOUT = 30
    RECONNECT_MAX_DELAY = 60
    RECONNECT_MAX_TRIES = 50
    ROOT_URI = MARKET_FEED_URL
    MODE_FULL = "full"
    MODE_QUOTE = "quote"
    MODE_LTP = "ltp"
    UPSTOX = True  # lets calling code detect this implementation

    def __init__(self, api_key, access_token, debug=False, root=None, reconnect=True,
                 reconnect_max_tries=RECONNECT_MAX_TRIES, reconnect_max_delay=RECONNECT_MAX_DELAY,
                 connect_timeout=CONNECT_TIMEOUT):
        self.api_key = api_key
        self.access_token = access_token
        self.debug = debug
        self.socket_url = root or self.ROOT_URI
        self.connect_timeout = connect_timeout
        self.should_reconnect = reconnect
        self.reconnect_max_tries = min(int(reconnect_max_tries), 300)
        self.reconnect_max_delay = max(int(reconnect_max_delay), 5)
        self.ws = None          # set to the live connection once open (Kite semantics)
        self.factory = None     # Kite/Twisted compatibility attribute (unused)
        self.subscribed_tokens: Dict[int, str] = {}
        self._reset_tries: Dict[str, bool] = {}
        self._market: Optional[_Loop] = None
        self._orders: Optional[_Loop] = None
        self._mapper = None
        # callbacks
        self.on_ticks: Optional[Callable] = None
        self.on_open: Optional[Callable] = None
        self.on_close: Optional[Callable] = None
        self.on_error: Optional[Callable] = None
        self.on_connect: Optional[Callable] = None
        self.on_message: Optional[Callable] = None
        self.on_reconnect: Optional[Callable] = None
        self.on_noreconnect: Optional[Callable] = None
        self.on_order_update: Optional[Callable] = None

    # ---- compatibility helpers ----------------------------------------------
    def _user_agent(self) -> str:
        return "kiteconnect-python-upstox/4.1.0"

    def _create_connection(self, url, **kwargs):  # Twisted-era hook, intentionally a no-op
        return None

    def _emit(self, name: str, *args) -> None:
        cb = getattr(self, name, None)
        if cb:
            try:
                cb(*args)
            except Exception:  # noqa: BLE001  (never let a user callback kill the feed thread)
                log.exception("KiteTicker callback %s raised", name)

    # ---- lifecycle -------------------------------------------------------------
    def connect(self, threaded=False, disable_ssl_verification=False, proxy=None):
        if disable_ssl_verification:
            log.warning("disable_ssl_verification is ignored: Upstox feeds always verify TLS")
        self._market = _Loop(self, self.socket_url, self._market_opened, self._market_message, "market")
        t = self._market.start()
        if self.on_order_update:
            self._orders = _Loop(self, PORTFOLIO_FEED_URL, lambda: None, self._order_message, "orders")
            self._orders.start()
        if not threaded:
            t.join()

    def is_connected(self) -> bool:
        return bool(self._market and self._market.connected)

    def close(self, code=None, reason=None):
        self.should_reconnect = False
        for lp in (self._market, self._orders):
            if lp:
                lp.close()
        self.ws = None

    def stop(self):
        self.close()

    def stop_retry(self):
        self.should_reconnect = False

    def _market_opened(self) -> None:
        self.ws = self
        if self.subscribed_tokens:  # Kite resubscribes automatically after a reconnect
            by_mode: Dict[str, List[int]] = {}
            for tok, mode in self.subscribed_tokens.items():
                by_mode.setdefault(mode, []).append(tok)
            for mode, toks in by_mode.items():
                try:
                    self._send("sub", toks, mode)
                except Exception:  # noqa: BLE001
                    log.exception("resubscribe failed")
        self._emit("on_open", self)
        self._emit("on_connect", self, None)

    def _on_feed_closed(self, name: str, code, reason) -> None:
        if name == "market":
            self.ws = None
            self._emit("on_close", self, code, reason)

    # ---- subscriptions -----------------------------------------------------------
    def _ukeys(self, tokens: Iterable[Any]) -> List[str]:
        im = InstrumentMap.get()
        keys = []
        for t in tokens:
            row = im.from_token(t)
            if row is None:
                log.warning("KiteTicker: unknown instrument_token %s ignored", t)
                continue
            keys.append(row[UKEY])
        return keys

    def _send(self, method: str, tokens: List[int], mode: Optional[str] = None) -> None:
        if not self._market:
            raise RuntimeError("WebSocket is not connected")
        keys = self._ukeys(tokens)
        if not keys:
            return
        for i in range(0, len(keys), 1000):
            req: Dict[str, Any] = {"guid": uuid.uuid4().hex, "method": method, "data": {"instrumentKeys": keys[i:i + 1000]}}
            if mode:
                req["data"]["mode"] = UPSTOX_MODE.get(mode, "full")
            self._market.send(json.dumps(req).encode("utf-8"))

    def subscribe(self, instrument_tokens):
        toks = [int(t) for t in instrument_tokens]
        self._send("sub", toks, self.MODE_QUOTE)
        for t in toks:
            self.subscribed_tokens.setdefault(t, self.MODE_QUOTE)
        return True

    def unsubscribe(self, instrument_tokens):
        toks = [int(t) for t in instrument_tokens]
        self._send("unsub", toks)
        for t in toks:
            self.subscribed_tokens.pop(t, None)
        return True

    def set_mode(self, mode, instrument_tokens):
        toks = [int(t) for t in instrument_tokens]
        self._send("change_mode", toks, mode)
        for t in toks:
            self.subscribed_tokens[t] = mode
        return True

    def resubscribe(self):
        self._market_opened()

    # ---- messages ------------------------------------------------------------------
    def _market_message(self, message: Any) -> None:
        if isinstance(message, str):
            self._emit("on_message", self, message, False)
            return
        self._emit("on_message", self, message, True)
        try:
            ticks = self.parse_feed(message)
        except Exception:  # noqa: BLE001
            log.exception("could not decode Upstox feed message")
            return
        if ticks:
            self._emit("on_ticks", self, ticks)

    def parse_feed(self, message: bytes) -> List[Dict[str, Any]]:
        pb = _pb()
        resp = pb.FeedResponse.FromString(message)
        if not resp.feeds:
            return []
        im = InstrumentMap.get()
        now_dt = _ms_to_dt(resp.currentTs)
        ticks = []
        for ukey, feed in resp.feeds.items():
            row = im.from_ukey(ukey) or fallback_from_ukey(ukey)
            token = row[TOKEN]
            mode = self.subscribed_tokens.get(token, self.MODE_QUOTE)
            ticks.append(self._tick(ukey, token, mode, feed, now_dt))
        return ticks

    @staticmethod
    def _tick(ukey: str, token: int, mode: str, feed: Any, now_dt) -> Dict[str, Any]:
        which = feed.WhichOneof("FeedUnion")
        tradable = "_INDEX|" not in ukey
        mff = iff = flg = None
        if which == "fullFeed":
            sub = feed.fullFeed.WhichOneof("FullFeedUnion")
            mff = feed.fullFeed.marketFF if sub == "marketFF" else None
            iff = feed.fullFeed.indexFF if sub == "indexFF" else None
            L = (mff or iff).ltpc
        elif which == "firstLevelWithGreeks":
            flg = feed.firstLevelWithGreeks
            L = flg.ltpc
        else:
            L = feed.ltpc
        lp, cp = float(L.ltp), float(L.cp)
        tick: Dict[str, Any] = {"tradable": tradable, "mode": mode, "instrument_token": token, "last_price": lp}
        if mode == "ltp":
            return tick
        day = None
        src = mff or iff
        if src is not None:
            for o in src.marketOHLC.ohlc:
                if o.interval == "1d":
                    day = o
                    break
        tick["ohlc"] = {"open": float(day.open) if day else 0.0, "high": float(day.high) if day else 0.0,
                        "low": float(day.low) if day else 0.0, "close": cp}
        tick["change"] = (lp - cp) * 100.0 / cp if cp else 0.0
        ltt = _ms_to_dt(L.ltt)
        if not tradable:
            tick["exchange_timestamp"] = ltt or now_dt
            return tick
        vol = int(mff.vtt) if mff else int(flg.vtt) if flg else 0
        tick.update({
            "last_traded_quantity": int(L.ltq or 0),
            "average_traded_price": float(mff.atp) if mff else 0.0,
            "volume_traded": vol,
            "total_buy_quantity": int(mff.tbq) if mff else 0,
            "total_sell_quantity": int(mff.tsq) if mff else 0,
        })
        if mode == "full":
            quotes = list(mff.marketLevel.bidAskQuote)[:5] if mff else ([flg.firstDepth] if flg else [])
            buy = [{"quantity": int(q.bidQ), "price": float(q.bidP), "orders": 0} for q in quotes]
            sell = [{"quantity": int(q.askQ), "price": float(q.askP), "orders": 0} for q in quotes]
            pad = [{"quantity": 0, "price": 0.0, "orders": 0}]
            oi = float(mff.oi) if mff else float(flg.oi) if flg else 0.0
            tick.update({"last_trade_time": ltt, "oi": oi, "oi_day_high": 0, "oi_day_low": 0,
                         "exchange_timestamp": now_dt or ltt,
                         "depth": {"buy": buy + pad * (5 - len(buy)), "sell": sell + pad * (5 - len(sell))}})
        return tick

    def _order_message(self, message: Any) -> None:
        try:
            data = json.loads(message if isinstance(message, str) else message.decode("utf-8"))
        except Exception:  # noqa: BLE001
            return
        if not isinstance(data, dict) or data.get("update_type", "order") != "order" or not data.get("order_id"):
            return
        if self._mapper is None:
            from kiteconnect.connect import KiteConnect
            self._mapper = KiteConnect(self.api_key, access_token=self.access_token)
        try:
            order = self._mapper._order(data)
        except Exception:  # noqa: BLE001
            log.exception("could not map Upstox order update")
            return
        self._emit("on_order_update", self, order)
