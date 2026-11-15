"""Tests for vendor/upstox_kiteconnect: the Kite-compatible Upstox adapter.

All HTTP is faked; payload shapes follow the Upstox v2/v3 API models.
"""
import datetime as dt
import json
import os

import pytest

pytest.importorskip("kiteconnect.instruments", reason="Upstox adapter not installed")

from kiteconnect import KiteConnect, KiteTicker, exceptions as kex  # noqa: E402
from kiteconnect import instruments as I  # noqa: E402

IST = I.IST


def ms(d):  # IST date -> epoch ms like the Upstox master
    return int(dt.datetime.combine(d, dt.time(0, 0), IST).timestamp() * 1000)


W1, W2, M = dt.date(2026, 11, 17), dt.date(2026, 11, 24), dt.date(2026, 11, 26)
RAW = [
    {"segment": "NSE_INDEX", "name": "Nifty 50", "exchange": "NSE", "instrument_type": "INDEX",
     "instrument_key": "NSE_INDEX|Nifty 50", "exchange_token": "26000", "trading_symbol": "NIFTY"},
    {"segment": "BSE_INDEX", "name": "BSE SENSEX", "exchange": "BSE", "instrument_type": "INDEX",
     "instrument_key": "BSE_INDEX|SENSEX", "exchange_token": "1", "trading_symbol": "SENSEX"},
    {"segment": "NSE_EQ", "name": "RELIANCE INDUSTRIES LTD", "exchange": "NSE", "isin": "INE002A01018",
     "instrument_type": "EQ", "instrument_key": "NSE_EQ|INE002A01018", "lot_size": 1, "exchange_token": "2885",
     "tick_size": 10.0, "trading_symbol": "RELIANCE"},
    {"segment": "NSE_FO", "name": "NIFTY", "exchange": "NSE", "expiry": ms(W1), "weekly": True,
     "instrument_key": "NSE_FO|40001", "exchange_token": "40001", "trading_symbol": "NIFTY 24500 CE 17 NOV 26",
     "tick_size": 5.0, "lot_size": 75, "instrument_type": "CE", "underlying_symbol": "NIFTY", "strike_price": 24500.0,
     "freeze_quantity": 1800.0},
    {"segment": "NSE_FO", "name": "NIFTY", "exchange": "NSE", "expiry": ms(M), "weekly": False,
     "instrument_key": "NSE_FO|40002", "exchange_token": "40002", "trading_symbol": "NIFTY 24500 PE 26 NOV 26",
     "tick_size": 5.0, "lot_size": 75, "instrument_type": "PE", "underlying_symbol": "NIFTY", "strike_price": 24500.0},
    {"segment": "NSE_FO", "name": "NIFTY", "exchange": "NSE", "expiry": ms(M), "instrument_key": "NSE_FO|40003",
     "exchange_token": "40003", "trading_symbol": "NIFTY FUT 26 NOV 26", "tick_size": 10.0, "lot_size": 75,
     "instrument_type": "FUT", "underlying_symbol": "NIFTY", "strike_price": 0},
    {"segment": "BSE_FO", "name": "SENSEX", "exchange": "BSE", "expiry": ms(dt.date(2026, 11, 19)),  # no 'weekly' key
     "instrument_key": "BSE_FO|1000", "exchange_token": "1000", "trading_symbol": "SENSEX 81000 CE 19 NOV 26",
     "tick_size": 5.0, "lot_size": 20, "instrument_type": "CE", "underlying_symbol": "SENSEX", "strike_price": 81000.0},
    {"segment": "BSE_FO", "name": "SENSEX", "exchange": "BSE", "expiry": ms(dt.date(2026, 11, 26)),
     "instrument_key": "BSE_FO|1001", "exchange_token": "1001", "trading_symbol": "SENSEX 81000 CE 26 NOV 26",
     "tick_size": 5.0, "lot_size": 20, "instrument_type": "CE", "underlying_symbol": "SENSEX", "strike_price": 81000.0},
    {"segment": "MCX_FO", "instrument_key": "MCX_FO|1", "trading_symbol": "CRUDE"},  # ignored
]


@pytest.fixture(autouse=True)
def imap(tmp_path, monkeypatch):
    monkeypatch.setenv("UPSTOX_KITE_CACHE_DIR", str(tmp_path))
    return I.InstrumentMap.set_rows_for_tests(RAW)


class Resp:
    def __init__(self, status, body):
        self.status_code, self._body = status, body
        self.text = json.dumps(body)

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def request(self, method, url, params=None, json=None, data=None, headers=None, **kw):
        path = url.replace("https://api.upstox.com", "")
        self.calls.append({"method": method, "path": path, "params": params, "json": json, "data": data, "headers": headers})
        r = self.routes.get((method, path))
        if r is None:
            for (m, p), v in self.routes.items():
                if m == method and p.endswith("*") and path.startswith(p[:-1]):
                    r = v
                    break
        if r is None:
            raise AssertionError(f"unexpected call {method} {path}")
        r = r(self.calls[-1]) if callable(r) else r
        return r if isinstance(r, Resp) else Resp(200, r)


def client(routes):
    k = KiteConnect("APIKEY", access_token="TOK", redirect_uri="https://x/login")
    k.reqsession = FakeSession(routes)
    return k


# ---------------------------------------------------------------- instruments
def test_kite_symbols_and_tokens(imap):
    rows = {r["tradingsymbol"]: r for r in KiteConnect("k").instruments()}
    assert "NIFTY26N1724500CE" in rows                      # weekly: YY + M + DD
    assert "NIFTY26NOV24500PE" in rows                      # monthly
    assert "NIFTY26NOVFUT" in rows
    assert "SENSEX26N1981000CE" in rows and "SENSEX26NOV81000CE" in rows   # monthly inferred without 'weekly'
    assert rows["NIFTY 50"]["segment"] == "INDICES" and rows["NIFTY 50"]["exchange"] == "NSE"
    assert rows["SENSEX"]["segment"] == "INDICES" and rows["SENSEX"]["exchange"] == "BSE"   # Upstox "BSE SENSEX"
    assert rows["RELIANCE"]["tick_size"] == pytest.approx(0.10)
    w = rows["NIFTY26N1724500CE"]
    assert (w["exchange"], w["segment"], w["lot_size"], w["strike"], w["expiry"]) == ("NFO", "NFO-OPT", 75, 24500.0, W1)
    assert w["tick_size"] == pytest.approx(0.05) and w["name"] == "NIFTY"
    assert w["instrument_token"] == 40001 * 256 + 2
    assert len({r["instrument_token"] for r in rows.values()}) == len(rows)
    assert all(r["exchange"] != "MCX" for r in rows.values())
    assert {r["exchange"] for r in KiteConnect("k").instruments("BFO")} == {"BFO"}


def test_lookups(imap):
    assert imap.from_kite_key("NSE:NIFTY 50")[I.UKEY] == "NSE_INDEX|Nifty 50"
    assert imap.from_kite_key("BSE:SENSEX")[I.UKEY] == "BSE_INDEX|SENSEX"
    assert imap.from_kite_key("NFO:NIFTY26N1724500CE")[I.UKEY] == "NSE_FO|40001"
    assert imap.from_token(40001 * 256 + 2)[I.TSYM] == "NIFTY26N1724500CE"


# ---------------------------------------------------------------- quotes
def test_quote_ltp_ohlc_shapes():
    q = {"NSE_FO:NIFTY 24500 CE 17 NOV 26": {
        "instrument_token": "NSE_FO|40001", "last_price": 120.5, "net_change": 20.5, "volume": 1000, "oi": 5000,
        "average_price": 110, "total_buy_quantity": 10, "total_sell_quantity": 20, "timestamp": "2026-11-16T10:00:00.000+05:30",
        "last_trade_time": "1795320000000", "ohlc": {"open": 100, "high": 130, "low": 95, "close": 120.5},
        "depth": {"buy": [{"quantity": 75, "price": 120, "orders": 1}], "sell": [{"quantity": 150, "price": 121, "orders": 2}]}}}
    k = client({("GET", "/v2/market-quote/quotes"): {"status": "success", "data": q},
                ("GET", "/v3/market-quote/ltp"): {"status": "success", "data": {"NSE_INDEX:Nifty 50": {"instrument_token": "NSE_INDEX|Nifty 50", "last_price": 24400.1}}},
                ("GET", "/v3/market-quote/ohlc"): {"status": "success", "data": {"NSE_INDEX:Nifty 50": {
                    "instrument_token": "NSE_INDEX|Nifty 50", "last_price": 24400.1,
                    "live_ohlc": {"open": 24300, "high": 24450, "low": 24280, "close": 24400.1}, "prev_ohlc": {"close": 24250}}}}})
    out = k.quote(["NFO:NIFTY26N1724500CE", "NFO:DOESNOTEXIST"])
    assert list(out) == ["NFO:NIFTY26N1724500CE"]
    d = out["NFO:NIFTY26N1724500CE"]
    assert d["last_price"] == 120.5 and d["ohlc"]["close"] == 100.0 and d["instrument_token"] == 40001 * 256 + 2
    assert len(d["depth"]["buy"]) == 5 and d["depth"]["sell"][0] == {"price": 121.0, "quantity": 150, "orders": 2}
    assert isinstance(d["timestamp"], dt.datetime) and d["timestamp"].tzinfo is None
    assert k.reqsession.calls[0]["params"]["instrument_key"] == "NSE_FO|40001"
    assert k.ltp("NSE:NIFTY 50")["NSE:NIFTY 50"]["last_price"] == 24400.1
    o = k.ohlc("NSE:NIFTY 50")["NSE:NIFTY 50"]
    assert o["ohlc"] == {"open": 24300.0, "high": 24450.0, "low": 24280.0, "close": 24250.0}


# ---------------------------------------------------------------- orders
def test_place_order_body_and_algo_header(monkeypatch):
    monkeypatch.setenv("UPSTOX_ALGO_NAME", "jk-ui-trading")
    k = client({("POST", "/v3/order/place"): {"status": "success", "data": {"order_ids": ["250101000123"]}}})
    oid = k.place_order(variety=k.VARIETY_REGULAR, exchange="NFO", tradingsymbol="NIFTY26N1724500CE",
                        transaction_type=k.TRANSACTION_TYPE_SELL, quantity=75, product=k.PRODUCT_NRML,
                        order_type=k.ORDER_TYPE_LIMIT, price=120.05, tag="survivor")
    assert oid == "250101000123"
    c = k.reqsession.calls[0]
    assert c["json"]["instrument_token"] == "NSE_FO|40001" and c["json"]["product"] == "D"
    assert c["json"]["is_amo"] is False and c["json"]["price"] == 120.05 and c["json"]["transaction_type"] == "SELL"
    assert c["headers"]["X-Algo-Name"] == "jk-ui-trading" and c["headers"]["Authorization"] == "Bearer TOK"
    k2 = client({("POST", "/v3/order/place"): {"status": "success", "data": {"order_ids": ["1"]}}})
    k2.place_order(variety="amo", exchange="NSE", tradingsymbol="RELIANCE", transaction_type="BUY", quantity=1,
                   product="MIS", order_type="MARKET", market_protection=-1)
    b = k2.reqsession.calls[0]["json"]
    assert b["product"] == "I" and b["is_amo"] is True and "market_protection" not in b
    with pytest.raises(kex.InputException):
        k2.place_order(variety="regular", exchange="NFO", tradingsymbol="NOPE", transaction_type="BUY", quantity=1,
                       product="NRML", order_type="LIMIT", price=1)


def test_modify_fills_missing_fields_and_cancel():
    k = client({("GET", "/v2/order/details"): {"status": "success", "data": {
                    "quantity": 75, "price": 120, "order_type": "LIMIT", "validity": "DAY", "trigger_price": 0}},
                ("PUT", "/v3/order/modify"): {"status": "success", "data": {"order_id": "9"}},
                ("DELETE", "/v3/order/cancel"): {"status": "success", "data": {"order_id": "9"}}})
    assert k.modify_order("regular", "9", price=118.5) == "9"
    body = k.reqsession.calls[1]["json"]
    assert body == {"order_id": "9", "quantity": 75, "validity": "DAY", "price": 118.5, "order_type": "LIMIT",
                    "trigger_price": 0.0, "disclosed_quantity": 0}
    assert k.cancel_order("regular", "9") == "9"
    assert k.reqsession.calls[2]["params"] == {"order_id": "9"}


def test_orders_positions_trades_mapping():
    order = {"exchange": "NSE_FO", "product": "D", "price": 120.0, "quantity": 75, "status": "trigger pending",
             "tag": "S_1_2", "instrument_token": "NSE_FO|40001", "trading_symbol": "NIFTY 24500 CE 17 NOV 26",
             "order_type": "SL", "validity": "DAY", "trigger_price": 119.0, "transaction_type": "BUY", "average_price": 0,
             "filled_quantity": 0, "pending_quantity": 75, "order_id": "1", "variety": "SIMPLE",
             "order_timestamp": "2026-11-16 10:01:02", "exchange_timestamp": None, "is_amo": False}
    eq_order = dict(order, instrument_token="NSE_EQ|INE002A01018", product="D", status="complete", filled_quantity=75)
    pos = {"exchange": "NSE_FO", "multiplier": 1.0, "pnl": 150.0, "product": "D", "instrument_token": "NSE_FO|40001",
           "average_price": 120.0, "buy_value": 0, "sell_value": 9000.0, "overnight_quantity": 0, "day_buy_quantity": 0,
           "day_sell_quantity": 75, "day_sell_price": 120.0, "day_sell_value": 9000.0, "quantity": -75, "last_price": 118.0,
           "unrealised": 150.0, "realised": 0.0, "close_price": 110.0}
    k = client({("GET", "/v2/order/retrieve-all"): {"status": "success", "data": [order, eq_order]},
                ("GET", "/v2/portfolio/short-term-positions"): {"status": "success", "data": [pos]},
                ("GET", "/v2/order/trades/get-trades-for-day"): {"status": "success", "data": [{
                    "exchange": "NSE_FO", "product": "I", "instrument_token": "NSE_FO|40001", "order_type": "LIMIT",
                    "transaction_type": "SELL", "quantity": 75, "order_id": "1", "trade_id": "T1",
                    "exchange_timestamp": "16-Nov-2026 10:01:05", "average_price": 120.0}]}})
    o, e = k.orders()
    assert (o["status"], o["product"], o["tradingsymbol"], o["exchange"], o["variety"]) == \
        ("TRIGGER PENDING", "NRML", "NIFTY26N1724500CE", "NFO", "regular")
    assert o["order_timestamp"] == dt.datetime(2026, 11, 16, 10, 1, 2) and o["tags"] == ["S_1_2"]
    assert (e["status"], e["product"], e["tradingsymbol"], e["pending_quantity"]) == ("COMPLETE", "CNC", "RELIANCE", 0)
    p = k.positions()
    n = p["net"][0]
    assert (n["quantity"], n["product"], n["tradingsymbol"], n["sell_quantity"], n["value"]) == (-75, "NRML", "NIFTY26N1724500CE", 75, 9000.0)
    assert p["day"][0]["quantity"] == -75 and p["day"][0]["pnl"] == pytest.approx(9000 - 75 * 118)
    t = k.trades()[0]
    assert t["product"] == "MIS" and t["fill_timestamp"] == dt.datetime(2026, 11, 16, 10, 1, 5)


# ---------------------------------------------------------------- GTT
def test_gtt_roundtrip():
    gtts = [{"type": "SINGLE", "exchange": "NSE_FO", "quantity": 75, "product": "D", "instrument_token": "NSE_FO|40001",
             "gtt_order_id": "GTT-C26111600001", "expires_at": 1826764200000, "created_at": 1795320000000,
             "rules": [{"strategy": "ENTRY", "status": "SCHEDULED", "trigger_type": "BELOW", "trigger_price": 100.0,
                        "transaction_type": "BUY", "order_id": None}]}]
    k = client({("POST", "/v3/order/gtt/place"): {"status": "success", "data": {"gtt_order_ids": ["GTT-C26111600001"]}},
                ("GET", "/v3/order/gtt"): {"status": "success", "data": gtts},
                ("PUT", "/v3/order/gtt/modify"): {"status": "success", "data": {"gtt_order_ids": ["GTT-C26111600001"]}},
                ("DELETE", "/v3/order/gtt/cancel"): {"status": "success", "data": {"gtt_order_ids": ["GTT-C26111600001"]}}})
    orders = [{"transaction_type": "BUY", "quantity": 75, "order_type": "LIMIT", "product": "NRML", "price": 100.0}]
    r = k.place_gtt(k.GTT_TYPE_SINGLE, "NIFTY26N1724500CE", "NFO", [100.0], 120.0, orders)
    gid = r["trigger_id"]
    assert isinstance(gid, int) and gid > 100000
    body = k.reqsession.calls[0]["json"]
    assert body["rules"] == [{"strategy": "ENTRY", "trigger_type": "BELOW", "trigger_price": 100.0}]
    assert body["instrument_token"] == "NSE_FO|40001" and body["type"] == "SINGLE" and body["product"] == "D"
    g = k.get_gtts()[0]
    assert g["id"] == gid and g["status"] == "active" and g["condition"]["trigger_values"] == [100.0]
    assert g["condition"]["last_price"] == 120.0 and g["orders"][0]["transaction_type"] == "BUY"
    assert g["condition"]["tradingsymbol"] == "NIFTY26N1724500CE"
    k.modify_gtt(gid, k.GTT_TYPE_SINGLE, "NIFTY26N1724500CE", "NFO", [130.0], 120.0, orders)
    assert k.reqsession.calls[-1]["json"]["rules"][0]["trigger_type"] == "ABOVE"
    assert k.reqsession.calls[-1]["json"]["gtt_order_id"] == "GTT-C26111600001"
    assert k.delete_gtt(gid) == {"trigger_id": gid}
    with pytest.raises(kex.InputException):
        k.place_gtt(k.GTT_TYPE_OCO, "NIFTY26N1724500CE", "NFO", [90, 150], 120.0, orders * 2)


# ---------------------------------------------------------------- historical
def test_historical_chunks_merges_and_sorts(monkeypatch):
    today = I.ist_today()
    old = today - dt.timedelta(days=40)

    def hist(call):
        to_d, fr_d = call["path"].rsplit("/", 2)[-2:]
        d = dt.date.fromisoformat(fr_d)
        return {"status": "success", "data": {"candles": [[f"{d}T09:15:00+05:30", 1, 2, 0.5, 1.5, 100, 7]]}}

    k = client({("GET", "/v3/historical-candle/NSE_FO%7C40001/minutes/5/*"): hist,
                ("GET", "/v3/historical-candle/intraday/NSE_FO%7C40001/minutes/5"): {"status": "success", "data": {"candles": [
                    [f"{today}T09:20:00+05:30", 3, 4, 2, 3.5, 10, 8], [f"{today}T09:15:00+05:30", 2, 3, 1, 2.5, 10, 8]]}}})
    out = k.historical_data(40001 * 256 + 2, dt.datetime.combine(old, dt.time()), dt.datetime.combine(today, dt.time(15, 30)), "5minute", oi=True)
    hist_calls = [c for c in k.reqsession.calls if "intraday" not in c["path"]]
    assert len(hist_calls) == 2  # 40 days split into 28-day windows
    assert [r["date"] for r in out] == sorted(r["date"] for r in out)
    assert out[-1]["close"] == 3.5 and out[-1]["oi"] == 8 and out[0]["date"].tzinfo is not None


# ---------------------------------------------------------------- margins
def test_margins_and_basket():
    k = client({("GET", "/v2/user/get-funds-and-margin"): {"status": "success", "data": {
                    "equity": {"used_margin": 1000, "payin_amount": 0, "span_margin": 600, "adhoc_margin": 0, "notional_cash": 0,
                               "available_margin": 50000, "exposure_margin": 400}, "commodity": {}}},
                ("POST", "/v2/charges/margin"): {"status": "success", "data": {
                    "margins": [{"span_margin": 100000, "exposure_margin": 20000, "total_margin": 120000, "net_buy_premium": 0},
                                {"span_margin": 0, "exposure_margin": 0, "total_margin": 900, "net_buy_premium": 900}],
                    "required_margin": 120900, "final_margin": 45000}}})
    m = k.margins()
    assert m["equity"]["available"]["live_balance"] == 50000 and m["equity"]["utilised"]["debits"] == 1000
    assert k.margins("equity")["net"] == 50000
    basket = [{"exchange": "NFO", "tradingsymbol": "NIFTY26N1724500CE", "transaction_type": "SELL", "variety": "regular",
               "product": "NRML", "order_type": "LIMIT", "quantity": 75, "price": 120},
              {"exchange": "NFO", "tradingsymbol": "NIFTY26NOV24500PE", "transaction_type": "BUY", "variety": "regular",
               "product": "NRML", "order_type": "LIMIT", "quantity": 75, "price": 12}]
    b = k.basket_order_margins(basket, mode="compact")
    assert b["initial_margin"]["total"] == 120900 and b["final"]["total"] == 45000 and b["initial"]["span"] == 100000
    assert k.reqsession.calls[-1]["json"]["instruments"][1]["instrument_key"] == "NSE_FO|40002"


# ---------------------------------------------------------------- errors & session
def test_error_mapping_and_retries(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    hook = []
    k = client({("GET", "/v2/user/profile"): Resp(401, {"status": "error", "errors": [{"errorCode": "UDAPI100050", "message": "Invalid token used to access API"}]})})
    k.set_session_expiry_hook(lambda: hook.append(1))
    with pytest.raises(kex.TokenException):
        k.profile()
    assert hook == [1]
    k = client({("POST", "/v3/order/place"): Resp(400, {"status": "error", "errors": [{"errorCode": "UDAPI1", "message": "Insufficient funds"}]})})
    with pytest.raises(kex.OrderException, match="Insufficient funds"):
        k.place_order("regular", "NSE", "RELIANCE", "BUY", 1, "CNC", "LIMIT", price=1)
    assert len(k.reqsession.calls) == 1  # never retried
    k = client({("POST", "/v3/order/place"): Resp(403, {"status": "error", "errors": [{"message": "IP not whitelisted"}]})})
    with pytest.raises(kex.PermissionException):
        k.place_order("regular", "NSE", "RELIANCE", "BUY", 1, "CNC", "LIMIT", price=1)
    seq = [Resp(429, {"status": "error", "errors": [{"message": "Too many"}]}), {"status": "success", "data": {"user_name": "J K"}}]
    k = client({("GET", "/v2/user/profile"): lambda c: seq.pop(0)})
    assert k.profile()["user_shortname"] == "J"  # GET retried once after 429


def test_login_url_and_generate_session():
    k = client({("POST", "/v2/login/authorization/token"): {"email": "a@b", "user_id": "U1", "user_name": "Jay K",
                                                             "access_token": "NEWTOK", "exchanges": ["NSE"]}})
    assert "client_id=APIKEY" in k.login_url() and "redirect_uri=https%3A%2F%2Fx%2Flogin" in k.login_url()
    s = k.generate_session("CODE123", api_secret="SECRET")
    assert s["access_token"] == "NEWTOK" and k.access_token == "NEWTOK" and isinstance(s["login_time"], dt.datetime)
    form = k.reqsession.calls[0]["data"]
    assert form == {"code": "CODE123", "client_id": "APIKEY", "client_secret": "SECRET",
                    "redirect_uri": "https://x/login", "grant_type": "authorization_code"}
    with pytest.raises(kex.InputException):
        k.place_mf_order("INF", "BUY")


# ---------------------------------------------------------------- ticker
def test_ticker_parses_protobuf_feed():
    pb = pytest.importorskip("upstox_client.feeder.proto.MarketDataFeedV3_pb2")
    msg = pb.FeedResponse(type=1, currentTs=1795320000000)
    idx = msg.feeds["NSE_INDEX|Nifty 50"]
    idx.fullFeed.indexFF.ltpc.ltp = 24400.0
    idx.fullFeed.indexFF.ltpc.cp = 24200.0
    idx.fullFeed.indexFF.ltpc.ltt = 1795320000000
    o = idx.fullFeed.indexFF.marketOHLC.ohlc.add()
    o.interval, o.open, o.high, o.low, o.close = "1d", 24250, 24450, 24210, 24400
    opt = msg.feeds["NSE_FO|40001"]
    m = opt.fullFeed.marketFF
    m.ltpc.ltp, m.ltpc.cp, m.ltpc.ltq, m.vtt, m.oi, m.atp, m.tbq, m.tsq = 120.0, 100.0, 75, 150000, 9000, 110.0, 10, 20
    q = m.marketLevel.bidAskQuote.add()
    q.bidQ, q.bidP, q.askQ, q.askP = 75, 119.95, 150, 120.05
    ltp_only = msg.feeds["NSE_EQ|INE002A01018"]
    ltp_only.ltpc.ltp = 1500.0

    kt = KiteTicker("k", "t")
    kt.subscribed_tokens = {40001 * 256 + 2: "full", 2885 * 256 + 1: "ltp"}
    ticks = {t["instrument_token"]: t for t in kt.parse_feed(msg.SerializeToString())}
    nifty = ticks[26000 * 256 + 9]
    assert nifty["tradable"] is False and nifty["last_price"] == 24400.0 and nifty["ohlc"]["close"] == 24200.0
    assert nifty["change"] == pytest.approx(200 / 242)
    op = ticks[40001 * 256 + 2]
    assert op["mode"] == "full" and op["volume_traded"] == 150000 and op["oi"] == 9000
    assert op["depth"]["buy"][0] == {"quantity": 75, "price": 119.95, "orders": 0} and len(op["depth"]["sell"]) == 5
    assert ticks[2885 * 256 + 1] == {"tradable": True, "mode": "ltp", "instrument_token": 2885 * 256 + 1, "last_price": 1500.0}


def test_ticker_subscribe_requires_connection():
    kt = KiteTicker("k", "t")
    assert kt.ws is None
    with pytest.raises(RuntimeError):
        kt.subscribe([40001 * 256 + 2])

    class FakeLoop:
        connected = True
        sent = []

        def send(self, payload):
            self.sent.append(json.loads(payload))
    kt._market = FakeLoop()
    kt.subscribe([40001 * 256 + 2])
    kt.set_mode(kt.MODE_FULL, [40001 * 256 + 2])
    assert FakeLoop.sent[0]["method"] == "sub" and FakeLoop.sent[0]["data"] == {"instrumentKeys": ["NSE_FO|40001"], "mode": "full"}
    assert FakeLoop.sent[1]["method"] == "change_mode" and kt.subscribed_tokens[40001 * 256 + 2] == "full"
