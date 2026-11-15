# -*- coding: utf-8 -*-
"""Upstox instrument master -> Kite-style instruments, symbols and tokens.

The Upstox master (``NSE.json.gz`` + ``BSE.json.gz``) is downloaded once per
IST day, converted, and cached on disk as a SQLite file that every process
on the machine shares.  Lookups go both ways:

    "NFO:NIFTY25N1824500CE"  <->  "NSE_FO|35003"  <->  9046018 (int token)
"""
from __future__ import annotations

import datetime as _dt
import gzip
import json
import logging
import os
import threading
import zlib
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

log = logging.getLogger("kiteconnect.upstox")

IST = _dt.timezone(_dt.timedelta(hours=5, minutes=30))
MASTER_URL = "https://assets.upstox.com/market-quote/instruments/exchange/{ex}.json.gz"
CACHE_VERSION = 5

# upstox segment -> (kite exchange, kite segment for non-derivatives, token segment code)
SEGMENTS = {
    "NSE_EQ": ("NSE", "NSE", 1),
    "NSE_FO": ("NFO", None, 2),
    "BSE_EQ": ("BSE", "BSE", 3),
    "BSE_FO": ("BFO", None, 4),
    "NSE_INDEX": ("NSE", "INDICES", 9),
    "BSE_INDEX": ("BSE", "INDICES", 10),
}
# kite exchange -> upstox segments to search (indices live under NSE/BSE in Kite)
KITE_TO_UPSTOX = {
    "NSE": ("NSE_EQ", "NSE_INDEX"),
    "BSE": ("BSE_EQ", "BSE_INDEX"),
    "NFO": ("NSE_FO",),
    "BFO": ("BSE_FO",),
}
MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
WEEKLY_MONTH = "123456789OND"

# Row tuple layout (kept compact; ~100k rows per process)
TOKEN, UKEY, EXCH, TSYM, NAME, EXPIRY, STRIKE, TICK, LOT, ITYPE, SEG, EXTOKEN, FREEZE = range(13)


def ist_today() -> _dt.date:
    return _dt.datetime.now(IST).date()


def _strike_str(v: float) -> str:
    v = float(v)
    if v.is_integer():
        return str(int(v))
    return ("%f" % v).rstrip("0").rstrip(".")


def _expiry_date(raw: Any) -> Optional[_dt.date]:
    if raw in (None, "", 0):
        return None
    try:
        if isinstance(raw, (int, float)) or str(raw).isdigit():
            return _dt.datetime.fromtimestamp(int(raw) / 1000, IST).date()
        return _dt.date.fromisoformat(str(raw)[:10])
    except (ValueError, OSError, OverflowError):
        return None


def _tick(raw: Any) -> float:
    """Upstox publishes tick size in paise (5.0 = Rs 0.05); Kite uses rupees."""
    try:
        v = float(raw or 0)
    except (TypeError, ValueError):
        return 0.05
    return round(v / 100.0, 4) if v >= 1 else v


def make_token(ukey: str, exchange_token: Any, code: int) -> int:
    """Stable int token like Kite's (exchange_token << 8 | segment)."""
    try:
        et = int(str(exchange_token))
        if 0 < et < (1 << 31) // 256:
            return et * 256 + code
    except (TypeError, ValueError):
        pass
    return ((zlib.crc32(ukey.encode()) & 0x7FFFFF) | 0x800000) * 256 + code


def kite_fo_symbol(name: str, expiry: _dt.date, itype: str, strike: float, monthly: bool) -> str:
    yy = "%02d" % (expiry.year % 100)
    if itype == "FUT":
        return f"{name}{yy}{MONTHS[expiry.month - 1]}FUT"
    if monthly:
        return f"{name}{yy}{MONTHS[expiry.month - 1]}{_strike_str(strike)}{itype}"
    return f"{name}{yy}{WEEKLY_MONTH[expiry.month - 1]}{expiry.day:02d}{_strike_str(strike)}{itype}"


def convert(raw_rows: Iterable[Dict[str, Any]]) -> List[tuple]:
    """Convert Upstox master rows to compact Kite rows (see tuple layout)."""
    raw_rows = [r for r in raw_rows if r.get("segment") in SEGMENTS and r.get("instrument_key")]

    # Monthly expiry per (underlying, year, month) - used when 'weekly' is absent.
    last_exp: Dict[Tuple[str, int, int], _dt.date] = {}
    for r in raw_rows:
        if r["segment"].endswith("_FO") and r.get("instrument_type") in ("CE", "PE"):
            e = _expiry_date(r.get("expiry"))
            if e:
                k = (r.get("underlying_symbol") or r.get("name") or "", e.year, e.month)
                if e > last_exp.get(k, _dt.date.min):
                    last_exp[k] = e

    out: List[tuple] = []
    for r in raw_rows:
        seg = r["segment"]
        kex, kseg, code = SEGMENTS[seg]
        ukey = r["instrument_key"]
        token = make_token(ukey, r.get("exchange_token"), code)
        itype = (r.get("instrument_type") or "").upper()
        tick = _tick(r.get("tick_size"))
        lot = int(r.get("lot_size") or 1)
        freeze = float(r.get("freeze_quantity") or 0)
        if seg.endswith("_INDEX"):
            name = (r.get("name") or r.get("trading_symbol") or "").upper()
            if kex == "BSE" and name.startswith("BSE "):  # Upstox "BSE SENSEX" -> Kite "SENSEX"
                name = name[4:]
            out.append((token, ukey, kex, name, name, None, 0.0, 0.0, 0, "EQ", "INDICES", str(r.get("exchange_token") or ""), 0.0))
        elif seg.endswith("_EQ"):
            sym = r.get("trading_symbol") or ""
            if not sym:
                continue
            if itype and itype not in ("EQ", "INDEX") and seg == "NSE_EQ":
                sym = f"{sym}-{itype}"
            name = (r.get("name") or sym).upper()
            out.append((token, ukey, kex, sym, name, None, 0.0, tick, lot, "EQ", kseg, str(r.get("exchange_token") or ""), freeze))
        else:  # derivatives
            if itype not in ("CE", "PE", "FUT"):
                continue
            exp = _expiry_date(r.get("expiry"))
            if not exp:
                continue
            under = (r.get("underlying_symbol") or r.get("name") or "").upper().replace(" ", "")
            strike = float(r.get("strike_price") or 0)
            if itype == "FUT":
                monthly = True
            elif "weekly" in r and r.get("weekly") is not None:
                monthly = not bool(r.get("weekly"))
            else:
                monthly = last_exp.get((r.get("underlying_symbol") or r.get("name") or "", exp.year, exp.month)) == exp
            sym = kite_fo_symbol(under, exp, itype, strike, monthly)
            kseg2 = f"{kex}-{'FUT' if itype == 'FUT' else 'OPT'}"
            out.append((token, ukey, kex, sym, under, exp, strike, tick, lot, itype, kseg2, str(r.get("exchange_token") or ""), freeze))
    return out


class InstrumentMap:
    """Process-wide, day-scoped instrument lookup backed by a shared SQLite file.

    The converted master lives in ``kite_instruments_v5_<date>.sqlite`` in the
    cache dir; every process (Flask app + each strategy script) queries it on
    demand and keeps only the instruments it actually touches in memory.
    """

    _lock = threading.Lock()
    _instance: Optional["InstrumentMap"] = None
    COLS = "token, ukey, exch, tsym, name, expiry, strike, tick, lot, itype, seg, extoken, freeze"

    def __init__(self, db_path: str, day: _dt.date):
        import sqlite3
        self.day = day
        self.path = db_path
        self._db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, check_same_thread=False)
        self._q = threading.Lock()
        self._by_token: Dict[int, Optional[tuple]] = {}
        self._by_ukey: Dict[str, Optional[tuple]] = {}
        self._by_kite: Dict[Tuple[str, str], Optional[tuple]] = {}

    @staticmethod
    def _row(r) -> Optional[tuple]:
        if r is None:
            return None
        r = list(r)
        r[EXPIRY] = _dt.date.fromisoformat(r[EXPIRY]) if r[EXPIRY] else None
        return tuple(r)

    def _one(self, where: str, args: tuple) -> Optional[tuple]:
        with self._q:
            r = self._db.execute(f"SELECT {self.COLS} FROM inst WHERE {where} ORDER BY rowid LIMIT 1", args).fetchone()
        return self._row(r)

    # ---- building / loading ------------------------------------------------
    @staticmethod
    def build_db(rows: List[tuple], path: str) -> None:
        import sqlite3
        tmp = path + ".tmp"
        if os.path.exists(tmp):
            os.remove(tmp)
        c = sqlite3.connect(tmp)
        c.execute("CREATE TABLE inst (token INTEGER, ukey TEXT, exch TEXT, tsym TEXT, name TEXT, expiry TEXT, strike REAL,"
                  " tick REAL, lot INTEGER, itype TEXT, seg TEXT, extoken TEXT, freeze REAL)")
        c.executemany("INSERT INTO inst VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      [(t[0], t[1], t[2], t[3], t[4], t[5].isoformat() if t[5] else None) + tuple(t[6:]) for t in rows])
        c.execute("CREATE INDEX i_token ON inst(token)")
        c.execute("CREATE INDEX i_ukey ON inst(ukey)")
        c.execute("CREATE INDEX i_kite ON inst(exch, tsym)")
        c.commit()
        c.close()
        os.replace(tmp, path)

    @classmethod
    def get(cls, session: Optional[requests.Session] = None) -> "InstrumentMap":
        today = ist_today()
        inst = cls._instance
        if inst is not None and inst.day == today:
            return inst
        with cls._lock:
            inst = cls._instance
            if inst is None or inst.day != today:
                cls._instance = cls._load(today, session)
            return cls._instance

    @classmethod
    def set_rows_for_tests(cls, raw_rows: List[Dict[str, Any]], day: Optional[_dt.date] = None) -> "InstrumentMap":
        day = day or ist_today()
        path = os.path.join(cls.cache_dir(), f"kite_instruments_v{CACHE_VERSION}_{day.isoformat()}.sqlite")
        cls.build_db(convert(raw_rows), path)
        cls._instance = InstrumentMap(path, day)
        return cls._instance

    @staticmethod
    def cache_dir() -> str:
        d = os.environ.get("UPSTOX_KITE_CACHE_DIR") or os.path.join(os.path.expanduser("~"), ".cache", "upstox_kiteconnect")
        os.makedirs(d, exist_ok=True)
        return d

    @classmethod
    def _load(cls, day: _dt.date, session: Optional[requests.Session]) -> "InstrumentMap":
        d = cls.cache_dir()
        path = os.path.join(d, f"kite_instruments_v{CACHE_VERSION}_{day.isoformat()}.sqlite")
        if os.path.exists(path):
            return InstrumentMap(path, day)
        with open(os.path.join(d, ".build.lock"), "w") as lockf:
            try:
                import fcntl
                fcntl.flock(lockf, fcntl.LOCK_EX)
            except ImportError:  # pragma: no cover (Windows)
                pass
            if not os.path.exists(path):  # another process may have built it meanwhile
                raw: List[Dict[str, Any]] = []
                s = session or requests
                for ex in ("NSE", "BSE"):
                    r = s.get(MASTER_URL.format(ex=ex), timeout=90)
                    r.raise_for_status()
                    raw.extend(json.loads(gzip.decompress(r.content)))
                rows = convert(raw)
                del raw
                cls.build_db(rows, path)
                log.info("Upstox instrument master converted: %d instruments", len(rows))
                for old in os.listdir(d):  # keep only today's cache
                    if old.startswith("kite_instruments_") and old != os.path.basename(path):
                        try:
                            os.remove(os.path.join(d, old))
                        except OSError:
                            pass
        return InstrumentMap(path, day)

    # ---- lookups (cached per process, only for instruments actually used) ----
    def from_kite_key(self, key: str) -> Optional[tuple]:
        """'NFO:NIFTY25N1824500CE' / 'NSE:NIFTY 50' -> row."""
        if ":" not in key:
            return None
        ex, sym = key.split(":", 1)
        ex, sym = ex.strip().upper(), sym.strip()
        t = self.from_symbol(ex, sym) or self.from_symbol(ex, sym.upper())
        if t is None and ex in ("NSE", "BSE"):  # index names are case-insensitive in practice
            t = self._one("exch=? AND seg='INDICES' AND upper(tsym)=?", (ex, sym.upper()))
            if t is None:  # Kite index name == Upstox key suffix, e.g. BSE_INDEX|SENSEX
                t = self._one("ukey=?", (f"{ex}_INDEX|{sym.upper().replace(' ', '')}",))
        return t

    def from_symbol(self, exchange: str, tradingsymbol: str) -> Optional[tuple]:
        k = (exchange.upper(), tradingsymbol)
        if k not in self._by_kite:
            self._by_kite[k] = self._one("exch=? AND tsym=?", k)
        return self._by_kite[k]

    def from_token(self, token: Any) -> Optional[tuple]:
        try:
            tok = int(token)
        except (TypeError, ValueError):
            return None
        if tok not in self._by_token:
            self._by_token[tok] = self._one("token=?", (tok,))
        return self._by_token[tok]

    def from_ukey(self, ukey: str) -> Optional[tuple]:
        if ukey not in self._by_ukey:
            self._by_ukey[ukey] = self._one("ukey=?", (ukey,))
        return self._by_ukey[ukey]

    @property
    def rows(self) -> List[tuple]:
        with self._q:
            return [self._row(r) for r in self._db.execute(f"SELECT {self.COLS} FROM inst ORDER BY rowid")]

    @staticmethod
    def as_kite_row(t: tuple) -> Dict[str, Any]:
        return {
            "instrument_token": t[TOKEN],
            "exchange_token": t[EXTOKEN],
            "tradingsymbol": t[TSYM],
            "name": t[NAME],
            "last_price": 0.0,
            "expiry": t[EXPIRY] if t[EXPIRY] else "",
            "strike": float(t[STRIKE]),
            "tick_size": float(t[TICK]),
            "lot_size": int(t[LOT]),
            "instrument_type": t[ITYPE],
            "segment": t[SEG],
            "exchange": t[EXCH],
        }

    def kite_rows(self, exchange: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = f"SELECT {self.COLS} FROM inst" + (" WHERE exch=?" if exchange else "") + " ORDER BY rowid"
        with self._q:
            cur = self._db.execute(sql, (exchange.upper(),) if exchange else ())
            return [self.as_kite_row(self._row(r)) for r in cur]


def ukey_segment(ukey: str) -> str:
    return ukey.split("|", 1)[0] if "|" in ukey else ""


def fallback_from_ukey(ukey: str, upstox_symbol: str = "") -> tuple:
    """Row for an instrument missing from today's master (e.g. expired today)."""
    seg = ukey_segment(ukey)
    kex, kseg, code = SEGMENTS.get(seg, ("NSE", "NSE", 1))
    et = ukey.split("|", 1)[1] if "|" in ukey else ukey
    return (make_token(ukey, et, code), ukey, kex, upstox_symbol or et, upstox_symbol or et, None, 0.0, 0.05, 1,
            "EQ", kseg or f"{kex}-OPT", et, 0.0)
