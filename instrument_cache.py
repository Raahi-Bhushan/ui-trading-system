"""Instrument cache module — atomic shadow-table swap.

The live ``instruments`` table is **never touched** until fresh data is fully
loaded in the staging ``instruments_new`` table.  On success the tables are
swapped atomically; on failure the live table is left intact so callers
always see either fresh or stale-but-valid data, never an empty table.

A ``instruments_backup`` table holds the previous successful dataset and is
used as a last-resort restore on startup if the live table is found empty.
"""

import sqlite3
import os
import logging
import datetime
import functools
import threading
from typing import Optional, Dict, Any, List

import holidays as holidays_lib

from common_lib import get_ist_now, IST
from datetime import time as dtime
from db_utils import connect_with_busy_timeout, enable_wal_once

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "instruments.db")
SYNC_MARKER_PATH = os.path.join(BASE_DIR, ".last_instrument_sync")

# Guards sync_instruments() against concurrent execution. Without this, two
# near-simultaneous triggers (e.g. the auto-sync fired by a successful Kite
# login racing a manual "Sync Instruments" click) each open their own
# connection and DROP/CREATE/INSERT into the same `instruments_new` staging
# table — one thread's DROP can remove the table out from under the other
# mid-INSERT, failing with "no such table: instruments_new" and leaving the
# live table stuck empty even though neither sync corrupted it individually.
_sync_lock = threading.Lock()

# DDL shared between the live and staging tables.
_HOLIDAYS_DDL = """
    CREATE TABLE IF NOT EXISTS market_holidays (
        holiday_date TEXT PRIMARY KEY,
        description  TEXT,
        exchange     TEXT NOT NULL DEFAULT 'NSE',
        year         INTEGER NOT NULL
    )
"""

_INSTRUMENTS_DDL = """
    instrument_token INTEGER PRIMARY KEY,
    tradingsymbol TEXT NOT NULL,
    name TEXT,
    expiry DATE,
    strike REAL,
    instrument_type TEXT,
    lot_size INTEGER,
    segment TEXT,
    exchange TEXT,
    tick_size REAL DEFAULT 0.05
"""

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def get_db_connection() -> sqlite3.Connection:
    """Return a connection to the SQLite instruments database.

    Uses a bounded busy_timeout so a reader waits out a brief writer lock
    (e.g. sync_instruments()'s atomic swap) instead of failing immediately
    with "database is locked" — this is the most-called DB in the app, so
    public-traffic-driven read concurrency needs the same protection already
    applied to cas_tracker.db and sensibull.db.
    """
    return connect_with_busy_timeout(DB_PATH)


def _table_exists(cursor: sqlite3.Cursor, table_name: str) -> bool:
    """Return True if *table_name* exists in the database."""
    cursor.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    )
    return cursor.fetchone()[0] > 0


def _row_count(cursor: sqlite3.Cursor, table_name: str) -> int:
    """Return the number of rows in *table_name*, or 0 if it does not exist."""
    if not _table_exists(cursor, table_name):
        return 0
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")  # noqa: S608
    return cursor.fetchone()[0]


def _create_instruments_table(cursor: sqlite3.Cursor, table_name: str) -> None:
    """Create an instruments-schema table with the given *table_name*."""
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} ({_INSTRUMENTS_DDL})"  # noqa: S608
    )


def _add_indexes(cursor: sqlite3.Cursor) -> None:
    """Add performance indexes to the live ``instruments`` table."""
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_symbol ON instruments (tradingsymbol)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_segment ON instruments (segment)"
    )


# ---------------------------------------------------------------------------
# Public: init_db
# ---------------------------------------------------------------------------


def init_db() -> None:
    """Initialise all required tables if they do not already exist.

    Creates:
    * ``instruments``      — live table consumers read from
    * ``instruments_backup`` — previous successful sync (restore fallback)
    * ``sync_history``     — audit log of every sync attempt

    Also cleans up any orphaned ``instruments_new`` staging table left by a
    previously crashed sync.
    """
    enable_wal_once(DB_PATH)

    conn = get_db_connection()
    cursor = conn.cursor()

    # Live table
    _create_instruments_table(cursor, "instruments")

    # Backup table (same schema, no data on first init)
    _create_instruments_table(cursor, "instruments_backup")

    # Sync audit history
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            status TEXT,
            records_synced INTEGER,
            error_message TEXT
        )
    """)

    # Indexes on the live table
    _add_indexes(cursor)

    # Clean up any staging table orphan from a previous crash — but only if no
    # sync is currently building instruments_new. init_db() is called from many
    # unguarded call sites; without this check one of them could drop the
    # staging table out from under a concurrent sync_instruments() run (which
    # holds _sync_lock for the several seconds the Kite instruments() fetch
    # takes), causing a spurious "no such table: instruments_new" failure.
    if _sync_lock.acquire(blocking=False):
        try:
            cursor.execute("DROP TABLE IF EXISTS instruments_new")
        finally:
            _sync_lock.release()

    # Session token table for headless API access
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS kite_session_tokens (
            id        INTEGER PRIMARY KEY,
            token     TEXT NOT NULL,
            saved_at  DATETIME NOT NULL
        )
    """)

    # NSE market holidays cache (populated during instrument sync)
    cursor.execute(_HOLIDAYS_DDL)

    # GTT tag → algo_source mapping (for deferred attribution after GTT fires)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gtt_algo_tags (
            gtt_tag      TEXT PRIMARY KEY,
            algo_source  TEXT NOT NULL,
            created_at   TEXT NOT NULL
        )
    """)

    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# Public: GTT algo tag persistence
# ---------------------------------------------------------------------------


def save_gtt_algo_tag(gtt_tag: str, algo_source: str) -> None:
    """Persist a GTT unique tag → algo_source mapping for deferred attribution.

    Called at GTT placement time (while the script is running) so the algo
    name can be recovered later when the GTT fires asynchronously.

    Args:
        gtt_tag: The unique GTT tag string (e.g. 'GTT_S_1778045112_8314').
        algo_source: The script's global algo tag (e.g. 'Trending_Market_Code').
    """
    try:
        conn = get_db_connection()
        conn.execute(
            "INSERT OR REPLACE INTO gtt_algo_tags (gtt_tag, algo_source, created_at) VALUES (?, ?, ?)",
            (gtt_tag, algo_source, datetime.datetime.now(datetime.timezone.utc).isoformat()),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        logger.warning("save_gtt_algo_tag: could not persist GTT map for %s: %s", gtt_tag, exc)


def get_gtt_algo_source(gtt_tag: str) -> Optional[str]:
    """Look up the algo_source for a GTT unique tag.

    Args:
        gtt_tag: The unique GTT tag string.

    Returns:
        The stored algo_source, or None if not found.
    """
    try:
        conn = get_db_connection()
        row = conn.execute(
            "SELECT algo_source FROM gtt_algo_tags WHERE gtt_tag = ?", (gtt_tag,)
        ).fetchone()
        conn.close()
        return row["algo_source"] if row else None
    except Exception as exc:
        logger.warning("get_gtt_algo_source: lookup failed for %s: %s", gtt_tag, exc)
        return None


# ---------------------------------------------------------------------------
# Public: needs_sync
# ---------------------------------------------------------------------------


def needs_sync() -> bool:
    """Return True if a fresh sync is required.

    A sync is needed when:
    1. The database file or sync-marker file does not exist.
    2. The live ``instruments`` table is empty (data was lost).
    3. Today's 9:00 AM IST has passed and the last successful sync pre-dates it.
    """
    # Guard: if DB missing or marker missing, sync is always needed
    if not os.path.exists(DB_PATH) or not os.path.exists(SYNC_MARKER_PATH):
        return True

    # Guard: if live table is empty, always request a sync regardless of marker
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        live_count = _row_count(cursor, "instruments")
        conn.close()
        if live_count == 0:
            logger.warning(
                "instruments table is empty — marking sync as needed"
            )
            return True
    except Exception as exc:  # noqa: BLE001
        logger.error("Could not check instruments row count: %s", exc)
        return True

    # Parse the sync marker timestamp
    try:
        with open(SYNC_MARKER_PATH, "r") as fh:
            last_sync_str = fh.read().strip()
            last_sync_dt = datetime.datetime.fromisoformat(last_sync_str)
            if last_sync_dt.tzinfo is None:
                last_sync_dt = last_sync_dt.replace(tzinfo=IST)
    except Exception:  # noqa: BLE001
        return True

    now = get_ist_now()
    today_9am = now.replace(hour=9, minute=0, second=0, microsecond=0)

    # If past 9 AM today and last sync predates today's 9 AM → need refresh
    if now >= today_9am and last_sync_dt < today_9am:
        return True

    return False


# ---------------------------------------------------------------------------
# Public: restore_from_backup
# ---------------------------------------------------------------------------


def restore_from_backup() -> bool:
    """Restore the live ``instruments`` table from ``instruments_backup``.

    This is called automatically on Flask startup when the live table is
    found to be empty.  Returns True if a restore was performed.
    """
    init_db()
    conn = get_db_connection()
    cursor = conn.cursor()

    backup_count = _row_count(cursor, "instruments_backup")
    if backup_count == 0:
        conn.close()
        logger.warning("restore_from_backup: backup table is also empty — nothing to restore")
        return False

    live_count = _row_count(cursor, "instruments")
    if live_count > 0:
        conn.close()
        logger.info("restore_from_backup: live table already has data (%d rows), skipping", live_count)
        return False

    try:
        # Copy rows from backup into the live table
        cursor.execute("""
            INSERT INTO instruments
            SELECT * FROM instruments_backup
        """)
        conn.commit()
        restored = _row_count(cursor, "instruments")
        logger.warning(
            "restore_from_backup: restored %d instruments from backup", restored
        )
        return True
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        logger.error("restore_from_backup: failed — %s", exc)
        return False
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Private: _populate_market_holidays
# ---------------------------------------------------------------------------


def _populate_market_holidays(conn: sqlite3.Connection, years: list[int]) -> None:
    """Refresh the market_holidays table for the given years using the holidays library.

    Args:
        conn: An open SQLite connection (caller owns the transaction).
        years: List of calendar years to populate (e.g. [2025, 2026]).
    """
    nse_holidays = holidays_lib.financial_holidays("XNSE", years=years)
    rows = [
        (holiday_date.isoformat(), description, "NSE", holiday_date.year)
        for holiday_date, description in nse_holidays.items()
    ]
    placeholders = ",".join("?" * len(years))
    conn.execute(
        f"DELETE FROM market_holidays WHERE year IN ({placeholders})",  # noqa: S608
        years,
    )
    conn.executemany(
        "INSERT OR REPLACE INTO market_holidays (holiday_date, description, exchange, year)"
        " VALUES (?, ?, ?, ?)",
        rows,
    )
    logger.info(
        "market_holidays populated: %d holidays for years %s", len(rows), years
    )


# ---------------------------------------------------------------------------
# Public: sync_instruments  (atomic swap)
# ---------------------------------------------------------------------------


def sync_instruments(kite: Any) -> bool:  # type: ignore[type-arg]
    """Fetch all instruments from Zerodha and atomically refresh the DB.

    Strategy
    --------
    1. Fetch ~60k instruments from the Kite API.
    2. Insert them into a **staging** table ``instruments_new``.
    3. Only if the insert succeeds, atomically swap:
       ``instruments → instruments_backup``, ``instruments_new → instruments``.
    4. On any failure, drop ``instruments_new`` and leave the live table
       **untouched** — callers continue to see the previous (stale but valid)
       data.

    Returns
    -------
    bool
        True when the sync completed and the live table was swapped.
    """
    if not _sync_lock.acquire(blocking=False):
        logger.warning(
            "sync_instruments: another sync is already in progress — "
            "skipping this concurrent call rather than racing on the "
            "instruments_new staging table."
        )
        return False

    try:
        return _sync_instruments_locked(kite)
    finally:
        _sync_lock.release()


def _sync_instruments_locked(kite: Any) -> bool:  # type: ignore[type-arg]
    """Body of :func:`sync_instruments`, run while ``_sync_lock`` is held."""
    logger.info("Starting instrument sync from Zerodha (atomic swap)...")
    start_time = get_ist_now()
    records_synced = 0
    error_msg: Optional[str] = None
    status = "FAILED"

    # Ensure all base tables exist and insert an audit row
    init_db()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO sync_history (start_time, status) VALUES (?, ?)",
        (start_time.replace(tzinfo=None).isoformat(sep=" "), "RUNNING"),  # sqlite3 TIMESTAMP converter needs "YYYY-MM-DD HH:MM:SS"
    )
    history_id = cursor.lastrowid
    conn.commit()
    conn.close()

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # ------------------------------------------------------------------
        # Step 1 — Create staging table (clean slate)
        # ------------------------------------------------------------------
        cursor.execute("DROP TABLE IF EXISTS instruments_new")
        cursor.execute(
            f"CREATE TABLE instruments_new ({_INSTRUMENTS_DDL})"  # noqa: S608
        )

        # ------------------------------------------------------------------
        # Step 2 — Fetch fresh data from Zerodha
        # ------------------------------------------------------------------
        instruments: List[Dict[str, Any]] = kite.instruments()
        records_synced = len(instruments)
        logger.info("Fetched %d instruments. Inserting into staging table...", records_synced)

        # ------------------------------------------------------------------
        # Step 3 — Batch-insert into instruments_new
        # ------------------------------------------------------------------
        batch_size = 5000
        for i in range(0, records_synced, batch_size):
            batch = instruments[i : i + batch_size]
            data_to_insert = [
                (
                    inst["instrument_token"],
                    inst["tradingsymbol"],
                    inst.get("name"),
                    str(inst["expiry"]) if inst.get("expiry") else None,
                    inst.get("strike"),
                    inst.get("instrument_type"),
                    inst.get("lot_size"),
                    inst.get("segment"),
                    inst.get("exchange"),
                    inst.get("tick_size", 0.05),
                )
                for inst in batch
            ]
            cursor.executemany(
                """
                INSERT INTO instruments_new (
                    instrument_token, tradingsymbol, name, expiry,
                    strike, instrument_type, lot_size, segment, exchange, tick_size
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                data_to_insert,
            )

        # Verify staging table has the expected count before swapping
        staging_count = _row_count(cursor, "instruments_new")
        if staging_count == 0:
            raise RuntimeError("Staging table is empty after insert — aborting swap")

        # ------------------------------------------------------------------
        # Step 4 — Atomic swap
        #   instruments       → instruments_backup  (preserve previous good data)
        #   instruments_new   → instruments          (promote fresh data)
        # ------------------------------------------------------------------
        cursor.execute("DROP TABLE IF EXISTS instruments_backup")
        cursor.execute("ALTER TABLE instruments RENAME TO instruments_backup")
        cursor.execute("ALTER TABLE instruments_new RENAME TO instruments")

        # Re-create indexes (ALTER TABLE RENAME does not carry named indexes)
        _add_indexes(cursor)

        conn.commit()
        logger.info(
            "Atomic swap complete: %d instruments now live, %d in backup",
            staging_count,
            _row_count(cursor, "instruments_backup"),
        )

        # ------------------------------------------------------------------
        # Step 5 — Populate market holidays for this year (+ next if Dec)
        # ------------------------------------------------------------------
        today_ist = datetime.datetime.now(IST).date()
        sync_years = [today_ist.year]
        if today_ist.month >= 11:
            sync_years.append(today_ist.year + 1)
        _populate_market_holidays(conn, sync_years)
        conn.commit()

        # ------------------------------------------------------------------
        # Step 6 — Update sync marker
        # ------------------------------------------------------------------
        with open(SYNC_MARKER_PATH, "w") as fh:
            fh.write(get_ist_now().isoformat())

        status = "SUCCESS"
        logger.info("Instrument sync completed successfully.")

        # Clear lookup caches so callers see the fresh data
        get_instrument.cache_clear()
        get_instrument_by_token.cache_clear()
        is_market_holiday.cache_clear()

        # Prune Position Guard ignore rows for symbols (e.g. expired
        # contracts) that no longer exist in the freshly synced table.
        try:
            from position_guard.db import cleanup_ignored_symbols_not_in_instruments

            cleanup_ignored_symbols_not_in_instruments()
        except Exception:
            logger.exception(
                "sync_instruments: position_guard ignore-list cleanup failed"
            )

    except Exception as exc:
        error_msg = str(exc)
        logger.error(
            "Sync failed — live instruments table is untouched. Error: %s", error_msg
        )
        # Clean up staging table; live table is never touched on this path
        try:
            cursor.execute("DROP TABLE IF EXISTS instruments_new")
            conn.commit()
        except Exception:  # noqa: BLE001
            pass

    finally:
        # Finalise audit row
        try:
            cursor.execute(
                """
                UPDATE sync_history
                SET end_time = ?, status = ?, records_synced = ?, error_message = ?
                WHERE id = ?
                """,
                (
                    get_ist_now().replace(tzinfo=None).isoformat(sep=" "),
                    status,
                    records_synced,
                    error_msg,
                    history_id,
                ),
            )
            conn.commit()
        except Exception:  # noqa: BLE001
            pass
        conn.close()

    return status == "SUCCESS"


# ---------------------------------------------------------------------------
# Public: query helpers
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=366)
def is_market_holiday(check_date: datetime.date) -> bool:
    """Return True if *check_date* is a cached NSE trading holiday.

    Falls back to False (no holiday blocking) if the table has not yet been
    populated — this happens before the first successful instrument sync.

    Args:
        check_date: The IST date to check.

    Returns:
        True if the date is an NSE holiday, False otherwise.
    """
    try:
        with connect_with_busy_timeout(DB_PATH) as conn:
            row = conn.execute(
                "SELECT 1 FROM market_holidays"
                " WHERE holiday_date = ? AND exchange = 'NSE'",
                (check_date.isoformat(),),
            ).fetchone()
        return row is not None
    except sqlite3.Error as exc:
        logger.warning(
            "is_market_holiday: could not query market_holidays (%s) — "
            "treating day as non-holiday",
            exc,
        )
        return False


@functools.lru_cache(maxsize=2048)
def get_instrument(
    symbol: str, exchange: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Return metadata dict for a specific tradingsymbol, or None.

    Args:
        symbol: The trading symbol to look up (e.g. ``"NIFTY24APR25000CE"``).
        exchange: Optional exchange filter (e.g. ``"MCX"``, ``"NFO"``).
            When provided, the query is narrowed to ``tradingsymbol = ?
            AND exchange = ?`` to avoid collisions where the same symbol
            exists on multiple exchanges with different expiry dates
            (e.g. CRUDEOIL options on both NCO and MCX).  When omitted,
            the first matching row is returned (legacy behaviour).

    Returns:
        A dict of instrument metadata, or ``None`` if not found.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    if exchange:
        cursor.execute(
            "SELECT * FROM instruments WHERE tradingsymbol = ? AND exchange = ?",
            (symbol, exchange),
        )
    else:
        cursor.execute(
            "SELECT * FROM instruments WHERE tradingsymbol = ?", (symbol,)
        )
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


@functools.lru_cache(maxsize=2048)
def get_instrument_by_token(token: int) -> Optional[Dict[str, Any]]:
    """Return metadata dict for a specific instrument_token, or None."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT * FROM instruments WHERE instrument_token = ?", (token,)
    )
    row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def get_tick_size(tradingsymbol: str, exchange: str = "NFO") -> float:
    """Return the minimum price tick for an instrument from the cache.

    Args:
        tradingsymbol: Instrument trading symbol (e.g. ``"SILVER26JUL215000PE"``).
        exchange: Exchange code (e.g. ``"MCX"``, ``"NFO"``).

    Returns:
        Tick size as a float; defaults to 0.05 if not found or column absent
        (graceful fallback before the first 9AM sync after a deploy that adds
        the ``tick_size`` column to ``_INSTRUMENTS_DDL``).
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT tick_size FROM instruments WHERE tradingsymbol = ? AND exchange = ?",
            (tradingsymbol, exchange),
        )
        row = cursor.fetchone()
        return float(row["tick_size"]) if row and row["tick_size"] else 0.05
    except Exception:
        return 0.05
    finally:
        cursor.close()
        conn.close()


def search_instruments(query: str, limit: int = 15) -> List[Dict[str, Any]]:
    """Search instruments by tradingsymbol.

    Single-token queries use a prefix match (index-friendly, preserves existing UX).
    Multi-token queries (space-separated) AND-match each token as a substring in
    any order, so "NIFTY 23500" and "23500 NIFTY" return identical results.

    Args:
        query: Search string; whitespace separates independent tokens.
        limit: Maximum number of results to return.

    Returns:
        List of instrument metadata dicts sorted by tradingsymbol.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    raw_tokens = query.upper().split()
    tokens = [t.replace("%", "").replace("_", "") for t in raw_tokens if t]

    if not tokens:
        conn.close()
        return []

    if len(tokens) == 1:
        pattern = tokens[0] + "%"
        cursor.execute(
            """
            SELECT tradingsymbol, exchange, segment, instrument_type,
                   strike, expiry, lot_size, instrument_token
            FROM instruments
            WHERE tradingsymbol LIKE ?
            ORDER BY tradingsymbol
            LIMIT ?
            """,
            (pattern, limit),
        )
    else:
        where_clauses = " AND ".join("tradingsymbol LIKE ?" for _ in tokens)
        substring_patterns = [f"%{t}%" for t in tokens]
        first_prefix = f"{tokens[0]}%"
        cursor.execute(
            f"""
            SELECT tradingsymbol, exchange, segment, instrument_type,
                   strike, expiry, lot_size, instrument_token
            FROM instruments
            WHERE {where_clauses}
            ORDER BY
                CASE WHEN tradingsymbol LIKE ? THEN 0 ELSE 1 END,
                tradingsymbol
            LIMIT ?
            """,
            (*substring_patterns, first_prefix, limit),
        )

    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def search_nse_equity(query: str, limit: int = 30) -> List[Dict[str, Any]]:
    """Search NSE equity stocks by tradingsymbol prefix.

    Filters to EQ instrument type and NSE/NSE-EQ segments so that F&O contracts,
    indices, and other derivative instruments are excluded from the result set.

    Args:
        query: Prefix search string (case-insensitive). An empty string returns
            the first ``limit`` equity stocks alphabetically.
        limit: Maximum number of results to return.

    Returns:
        List of dicts with keys: tradingsymbol, name, instrument_token.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        safe_query = query.upper().replace("%", "").replace("_", r"\_")
        cursor.execute(
            """
            SELECT tradingsymbol, name, instrument_token
            FROM instruments
            WHERE tradingsymbol LIKE ?
              AND instrument_type = 'EQ'
              AND segment IN ('NSE', 'NSE-EQ')
            ORDER BY tradingsymbol
            LIMIT ?
            """,
            (safe_query + "%", limit),
        )
        return [dict(row) for row in cursor.fetchall()]
    finally:
        conn.close()


def get_stock_lot_size(tradingsymbol: str, default_qty: int = 200) -> tuple[int, bool]:
    """Return the default paper-trade quantity and F&O status for an NSE equity stock.

    Queries the NFO-FUT segment for a futures contract matching the given
    tradingsymbol prefix (e.g. 'RELIANCE' matches 'RELIANCE25MAYFUT').
    Returns ``(lot_size * 2, True)`` for 2-lot default when in F&O, or
    ``(default_qty, False)`` when the stock has no futures contracts.

    Args:
        tradingsymbol: NSE equity tradingsymbol, e.g. 'RELIANCE'.
        default_qty: Quantity to use when the stock is not in F&O (default 200).

    Returns:
        Tuple of (quantity, is_fo) where quantity is the recommended paper-trade
        default and is_fo is True when the stock has NFO-FUT contracts.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT lot_size FROM instruments
            WHERE tradingsymbol LIKE ?
              AND instrument_type = 'FUT'
              AND segment = 'NFO-FUT'
            ORDER BY expiry ASC
            LIMIT 1
            """,
            (tradingsymbol + "%",),
        )
        row = cursor.fetchone()
    finally:
        conn.close()

    if row and row["lot_size"]:
        return int(row["lot_size"]) * 2, True

    logger.info(
        "get_stock_lot_size: %s not found in NFO-FUT — using default_qty=%d",
        tradingsymbol,
        default_qty,
    )
    return default_qty, False


def get_nse_equity_token(tradingsymbol: str) -> int | None:
    """Return the instrument_token for an NSE equity (EQ) stock.

    Args:
        tradingsymbol: Exact NSE tradingsymbol, e.g. 'RELIANCE'.

    Returns:
        instrument_token integer, or None if not found in the cache.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT instrument_token FROM instruments
            WHERE tradingsymbol = ?
              AND instrument_type = 'EQ'
              AND segment IN ('NSE', 'NSE-EQ')
            LIMIT 1
            """,
            (tradingsymbol,),
        )
        row = cursor.fetchone()
        return int(row["instrument_token"]) if row else None
    finally:
        conn.close()


def get_fo_underlyings(search_query: str, limit: int = 30) -> List[str]:
    """Return distinct F&O underlying names matching a search substring.

    Queries the local instruments.db cache (synced daily) across NFO and BFO
    segments to support the copytrade blocking autocomplete picker.

    Args:
        search_query: Substring matched case-insensitively against the ``name`` column.
            An empty string returns the first ``limit`` underlyings alphabetically.
        limit: Maximum number of distinct names to return.

    Returns:
        Sorted list of underlying name strings, e.g. ``["NIFTY 50", "NIFTY BANK"]``.
    """
    safe_query = search_query.replace("%", "").replace("_", r"\_")
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT name FROM instruments
            WHERE segment IN ('NFO-OPT', 'NFO-FUT', 'BFO-OPT', 'BFO-FUT')
              AND name IS NOT NULL
              AND name LIKE ?
            ORDER BY name
            LIMIT ?
            """,
            (f"%{safe_query}%", limit),
        )
        return [row["name"] for row in cursor.fetchall()]
    finally:
        conn.close()


def get_all_fut_opt_instruments() -> Dict[int, Dict[str, Any]]:
    """Return a mapping of instrument_token → metadata for F&O instruments."""
    return _get_all_fut_opt_subset("instrument_token")


def get_all_fut_opt_instruments_by_symbol() -> Dict[str, Dict[str, Any]]:
    """Return a mapping of tradingsymbol → metadata for F&O instruments."""
    return _get_all_fut_opt_subset("tradingsymbol")


def _get_all_fut_opt_subset(key_field: str) -> Dict[Any, Dict[str, Any]]:
    """Fetch the F&O instrument subset keyed by *key_field*."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM instruments
        WHERE segment IN ('NFO-OPT', 'NFO-FUT', 'BFO-OPT', 'BFO-FUT')
    """)
    rows = cursor.fetchall()
    conn.close()

    result: Dict[Any, Dict[str, Any]] = {}
    for row in rows:
        d = dict(row)
        key = d[key_field]
        result[key] = {
            "tradingsymbol": d["tradingsymbol"],
            "expiry": d["expiry"],
            "strike": d["strike"],
            "instrument_type": d["instrument_type"],
            "segment": d["segment"],
            "exchange": d["exchange"],
            "lot_size": d["lot_size"],
            "instrument_token": d["instrument_token"],
            "name": d["name"],
        }
    return result


# ---------------------------------------------------------------------------
# Public: get_db_stats
# ---------------------------------------------------------------------------


def get_db_stats() -> Dict[str, Any]:
    """Return diagnostic stats about the instrument database."""
    init_db()

    if not os.path.exists(DB_PATH):
        return {"exists": False}

    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM instruments")
    total_count: int = cursor.fetchone()[0]

    cursor.execute("SELECT segment, COUNT(*) FROM instruments GROUP BY segment")
    segment_counts: Dict[str, int] = dict(cursor.fetchall())

    cursor.execute("SELECT * FROM instruments LIMIT 50")
    sample_rows = [dict(r) for r in cursor.fetchall()]

    cursor.execute(
        "SELECT * FROM sync_history ORDER BY start_time DESC LIMIT 20"
    )
    history_rows = [dict(r) for r in cursor.fetchall()]

    # Backup table info
    backup_count = _row_count(cursor, "instruments_backup")

    conn.close()

    last_sync = "Never"
    if os.path.exists(SYNC_MARKER_PATH):
        with open(SYNC_MARKER_PATH, "r") as fh:
            last_sync = fh.read().strip()

    return {
        "exists": True,
        "total_count": total_count,
        "backup_count": backup_count,
        "segment_counts": segment_counts,
        "last_sync": last_sync,
        "sample": sample_rows,
        "history": history_rows,
    }


# ---------------------------------------------------------------------------
# Public: save_kite_token / get_kite_token  (headless API session)
# ---------------------------------------------------------------------------


def save_kite_token(access_token: str) -> None:
    """Persist the Kite access_token so headless /api/* calls can reuse it.

    Replaces any previously saved token (single-row table).  Called from
    flask_app.py whenever a successful Kite OAuth login completes.

    Args:
        access_token: The Zerodha Kite access token to persist.
    """
    init_db()
    now_iso = get_ist_now().isoformat()
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM kite_session_tokens")
        cursor.execute(
            "INSERT INTO kite_session_tokens (token, saved_at) VALUES (?, ?)",
            (access_token, now_iso),
        )
        conn.commit()
        logger.info("Kite session token saved to DB at %s", now_iso)
    except sqlite3.Error as exc:
        conn.rollback()
        logger.error("Failed to save Kite session token: %s", exc)
        raise
    finally:
        conn.close()


def get_kite_token() -> Optional[str]:
    """Return the most recently persisted Kite access_token, or None.

    Returns:
        The stored access token string, or None if no token has been saved.
    """
    init_db()
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT token FROM kite_session_tokens ORDER BY id DESC LIMIT 1")
        row = cursor.fetchone()
        return row["token"] if row else None
    except sqlite3.Error as exc:
        logger.error("Failed to retrieve Kite session token: %s", exc)
        return None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Public: clear_db  (admin / testing only)
# ---------------------------------------------------------------------------


def clear_db() -> bool:
    """Wipe the live instruments table and delete the sync marker.

    The ``instruments_backup`` table is **not** cleared so data can still be
    restored if needed.  This is intended for manual admin/testing use only.
    """
    if os.path.exists(DB_PATH):
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM instruments")
        conn.commit()
        conn.close()

    if os.path.exists(SYNC_MARKER_PATH):
        os.remove(SYNC_MARKER_PATH)

    return True


# ---------------------------------------------------------------------------
# Public: get_lot_size / get_near_month_nifty_future  (Early Exit tool)
# ---------------------------------------------------------------------------


def get_lot_size(name: str) -> int:
    """Return the lot size for a given index by name (e.g. 'NIFTY 50').

    Queries the first matching NFO-FUT row so the value stays current after
    every SEBI lot-size revision without any code change.  Falls back to 65
    if the DB has no matching row.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT lot_size FROM instruments
        WHERE name = ? AND instrument_type = 'FUT' AND segment = 'NFO-FUT'
        LIMIT 1
        """,
        (name,),
    )
    row = cursor.fetchone()
    conn.close()
    if row and row["lot_size"]:
        return int(row["lot_size"])
    logger.warning("get_lot_size: no NFO-FUT row found for name=%r, returning 65", name)
    return 65


def get_sensex_lot_size() -> int:
    """Return SENSEX lot size from the BFO-FUT instruments table.

    Falls back to querying by tradingsymbol prefix if name='SENSEX' yields
    no result (handles variation in how Zerodha names the instrument).
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT lot_size FROM instruments
        WHERE name = 'SENSEX' AND instrument_type = 'FUT' AND segment = 'BFO-FUT'
        LIMIT 1
        """,
    )
    row = cursor.fetchone()
    if not row or not row["lot_size"]:
        cursor.execute(
            """
            SELECT lot_size FROM instruments
            WHERE tradingsymbol LIKE 'SENSEX%FUT' AND segment = 'BFO-FUT'
            LIMIT 1
            """
        )
        row = cursor.fetchone()
    conn.close()
    if row and row["lot_size"]:
        return int(row["lot_size"])
    logger.warning("get_sensex_lot_size: no BFO-FUT row found, returning 20")
    return 20


def get_near_month_sensex_future() -> Optional[Dict[str, Any]]:
    """Return the nearest upcoming SENSEX futures contract (by expiry date).

    Used by the SENSEX Early Exit tool to estimate spot during pre-open via
    the futures basis-deduction method (NIFTY table × dynamic multiplier).
    Falls back to tradingsymbol LIKE query if name='SENSEX' finds nothing.
    """
    today = datetime.date.today()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM instruments
        WHERE name = 'SENSEX'
          AND instrument_type = 'FUT'
          AND segment = 'BFO-FUT'
          AND expiry >= ?
        ORDER BY expiry ASC
        LIMIT 1
        """,
        (str(today),),
    )
    row = cursor.fetchone()
    if not row:
        cursor.execute(
            """
            SELECT * FROM instruments
            WHERE tradingsymbol LIKE 'SENSEX%FUT'
              AND segment = 'BFO-FUT'
              AND expiry >= ?
            ORDER BY expiry ASC
            LIMIT 1
            """,
            (str(today),),
        )
        row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def get_near_month_nifty_future() -> Optional[Dict[str, Any]]:
    """Return the nearest upcoming NIFTY 50 futures contract (by expiry date).

    Used by the Early Exit tool to estimate the NIFTY spot price during
    the pre-open session via the futures basis-deduction method.
    Falls back to a tradingsymbol LIKE query if name='NIFTY 50' yields
    no result (handles variation in how Zerodha names the instrument).
    """
    today = datetime.date.today()
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT * FROM instruments
        WHERE name = 'NIFTY 50'
          AND instrument_type = 'FUT'
          AND segment = 'NFO-FUT'
          AND expiry >= ?
        ORDER BY expiry ASC
        LIMIT 1
        """,
        (str(today),),
    )
    row = cursor.fetchone()
    if not row:
        # Fallback: match by tradingsymbol pattern (e.g. NIFTY25MAYFUT)
        # Exclude BANKNIFTY, NIFTYIT, MIDCPNIFTY etc. by anchoring on segment
        cursor.execute(
            """
            SELECT * FROM instruments
            WHERE tradingsymbol LIKE 'NIFTY%FUT'
              AND instrument_type = 'FUT'
              AND segment = 'NFO-FUT'
              AND name NOT LIKE 'BANK%'
              AND name NOT LIKE 'MIDCP%'
              AND name NOT LIKE 'FIN%'
              AND expiry >= ?
            ORDER BY expiry ASC
            LIMIT 1
            """,
            (str(today),),
        )
        row = cursor.fetchone()
    conn.close()
    return dict(row) if row else None


def get_sensex_bfo_instruments() -> Dict[int, Dict[str, Any]]:
    """Return all SENSEX BFO-OPT and BFO-FUT instruments from the daily-synced DB.

    Reads from instruments.db (populated once a day at 9 AM IST by
    sync_instruments) so callers never need a live kite.instruments('BFO') call.

    Returns:
        Mapping of instrument_token → instrument metadata dict, restricted to
        rows whose tradingsymbol starts with 'SENSEX' and segment is BFO-OPT
        or BFO-FUT.  Returns an empty dict if the DB is unavailable.
    """
    today = datetime.date.today()
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT instrument_token, tradingsymbol, expiry, strike,
                   instrument_type, segment, exchange, lot_size
            FROM instruments
            WHERE tradingsymbol LIKE 'SENSEX%'
              AND segment IN ('BFO-OPT', 'BFO-FUT')
            """
        )
        rows = cursor.fetchall()
    except sqlite3.Error as exc:
        logger.error("get_sensex_bfo_instruments: DB query failed — %s", exc)
        return {}
    finally:
        conn.close()

    result: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        expiry_raw = row["expiry"]
        if expiry_raw is None:
            continue
        try:
            expiry_date = (
                datetime.date.fromisoformat(expiry_raw)
                if isinstance(expiry_raw, str)
                else expiry_raw
            )
        except (ValueError, TypeError):
            logger.warning("get_sensex_bfo_instruments: bad expiry %r for %s", expiry_raw, row["tradingsymbol"])
            continue

        result[row["instrument_token"]] = {
            "tradingsymbol": row["tradingsymbol"],
            "expiry": expiry_date,
            "strike": row["strike"],
            "instrument_type": row["instrument_type"],
            "segment": row["segment"],
            "exchange": row["exchange"],
            "lot_size": row["lot_size"],
            "days_to_expiry": (expiry_date - today).days,
        }

    if not result:
        logger.warning(
            "get_sensex_bfo_instruments: 0 SENSEX BFO rows found in instruments.db. "
            "Trigger a manual sync via /api/sync_instruments if this is unexpected."
        )
    else:
        logger.debug("get_sensex_bfo_instruments: returned %d instruments", len(result))

    return result


def get_upcoming_expiries(underlying_name: str, count: int = 2) -> list[str]:
    """Return the next N upcoming expiry dates for a given underlying.

    Args:
        underlying_name: e.g. "NIFTY", "BANKNIFTY", "SENSEX", "FINNIFTY"
        count: How many upcoming expiries to return.

    Returns:
        List of expiry date strings in YYYY-MM-DD format, sorted ascending.
        Returns an empty list if the DB is unavailable or has no data.
    """
    segment_map: dict[str, str] = {
        "NIFTY": "NFO-OPT",
        "BANKNIFTY": "NFO-OPT",
        "FINNIFTY": "NFO-OPT",
        "SENSEX": "BFO-OPT",
    }
    segment = segment_map.get(underlying_name, "NFO-OPT")
    today = str(datetime.date.today())

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT expiry
            FROM instruments
            WHERE name = ?
              AND segment = ?
              AND expiry >= ?
            ORDER BY expiry ASC
            LIMIT ?
            """,
            (underlying_name, segment, today, count),
        )
        rows = cursor.fetchall()
        conn.close()
        return [row["expiry"] for row in rows]
    except Exception as exc:
        logger.warning("get_upcoming_expiries(%s): %s", underlying_name, exc)
        return []


def get_strikes_for_expiry(
    underlying_name: str, expiry: str, option_type: str
) -> list[float]:
    """Return sorted strikes available for an underlying/expiry/option type.

    Args:
        underlying_name: e.g. "NIFTY", "BANKNIFTY", "SENSEX", "FINNIFTY"
        expiry: Expiry date string in YYYY-MM-DD format.
        option_type: "CE" or "PE".

    Returns:
        Sorted list of strike prices. Empty list if the DB is unavailable
        or has no matching rows.
    """
    segment_map: dict[str, str] = {
        "NIFTY": "NFO-OPT",
        "BANKNIFTY": "NFO-OPT",
        "FINNIFTY": "NFO-OPT",
        "SENSEX": "BFO-OPT",
    }
    segment = segment_map.get(underlying_name, "NFO-OPT")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT strike
            FROM instruments
            WHERE name = ?
              AND segment = ?
              AND expiry = ?
              AND instrument_type = ?
            ORDER BY strike ASC
            """,
            (underlying_name, segment, expiry, option_type),
        )
        rows = cursor.fetchall()
        conn.close()
        return [row["strike"] for row in rows]
    except Exception as exc:
        logger.warning(
            "get_strikes_for_expiry(%s, %s, %s): %s",
            underlying_name, expiry, option_type, exc,
        )
        return []


def get_trading_days_to_expiry(
    expiry_date: datetime.date, from_date: Optional[datetime.date] = None
) -> int:
    """Count NSE trading days remaining to expiry, excluding weekends and holidays.

    Unlike the naive ``np.busday_count`` used elsewhere in this codebase
    (which only excludes Sat/Sun), this consults the ``market_holidays``
    table so declared NSE holidays (Diwali, Republic Day, etc.) are also
    excluded — giving a materially more accurate DTE for the Amplitude
    Testbed's gamma-sensitive calculations near expiry.

    Args:
        expiry_date: The option's expiry date.
        from_date: Date to count from (defaults to today).

    Returns:
        Number of trading days from from_date to expiry_date, inclusive of
        both endpoints if they are trading days. 0 if expiry_date is before
        from_date.
    """
    start = from_date or datetime.date.today()
    if expiry_date < start:
        return 0
    count = 0
    current = start
    one_day = datetime.timedelta(days=1)
    while current <= expiry_date:
        if current.weekday() < 5 and not is_market_holiday(current):
            count += 1
        current += one_day
    return count


def get_next_expiry(underlying_name: str, expiry: str) -> Optional[str]:
    """Return the earliest expiry strictly after the given one, for the same underlying.

    Used to auto-derive the Amplitude Testbed's "Reference DTE" as the
    trading-day spacing between two consecutive expiries — i.e. the nominal
    full cycle length a freshly-listed contract of this series would have.
    We use the *next* expiry rather than the previous one because
    ``instruments.db`` is synced from Kite's live instrument dump, which
    never includes expired contracts — the previous expiry is reliably
    absent for the nearest (most commonly selected) expiry, which would
    make that direction useless in practice.

    Args:
        underlying_name: e.g. "NIFTY", "BANKNIFTY", "SENSEX", "FINNIFTY"
        expiry: Expiry date string in YYYY-MM-DD format.

    Returns:
        The next expiry date string in YYYY-MM-DD format, or None if none
        exists (e.g. `expiry` is the furthest one currently synced) or the
        DB is unavailable.
    """
    segment_map: dict[str, str] = {
        "NIFTY": "NFO-OPT",
        "BANKNIFTY": "NFO-OPT",
        "FINNIFTY": "NFO-OPT",
        "SENSEX": "BFO-OPT",
    }
    segment = segment_map.get(underlying_name, "NFO-OPT")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT expiry
            FROM instruments
            WHERE name = ? AND segment = ? AND expiry > ?
            ORDER BY expiry ASC
            LIMIT 1
            """,
            (underlying_name, segment, expiry),
        )
        row = cursor.fetchone()
        conn.close()
        return row["expiry"] if row else None
    except Exception as exc:
        logger.warning("get_next_expiry(%s, %s): %s", underlying_name, expiry, exc)
        return None


def get_option_chain_for_expiry(
    underlying_name: str, expiry: str
) -> List[Dict[str, Any]]:
    """Return every CE/PE instrument row for an underlying's expiry in one query.

    Args:
        underlying_name: e.g. "NIFTY", "BANKNIFTY", "SENSEX", "FINNIFTY"
        expiry: Expiry date string in YYYY-MM-DD format.

    Returns:
        List of instrument rows (tradingsymbol, exchange, strike,
        instrument_type, ...), sorted by strike then instrument_type.
        Empty list if the DB is unavailable or has no matching rows.
    """
    segment_map: dict[str, str] = {
        "NIFTY": "NFO-OPT",
        "BANKNIFTY": "NFO-OPT",
        "FINNIFTY": "NFO-OPT",
        "SENSEX": "BFO-OPT",
    }
    segment = segment_map.get(underlying_name, "NFO-OPT")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM instruments
            WHERE name = ? AND segment = ? AND expiry = ?
              AND instrument_type IN ('CE', 'PE')
            ORDER BY strike ASC, instrument_type ASC
            """,
            (underlying_name, segment, expiry),
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]
    except Exception as exc:
        logger.warning(
            "get_option_chain_for_expiry(%s, %s): %s", underlying_name, expiry, exc
        )
        return []


def get_option_instrument(
    underlying_name: str, expiry: str, strike: float, option_type: str
) -> Optional[Dict[str, Any]]:
    """Return the instrument row for a specific underlying/expiry/strike/type.

    Args:
        underlying_name: e.g. "NIFTY", "BANKNIFTY", "SENSEX", "FINNIFTY"
        expiry: Expiry date string in YYYY-MM-DD format.
        strike: Strike price.
        option_type: "CE" or "PE".

    Returns:
        A dict of instrument metadata (including tradingsymbol, exchange),
        or None if not found.
    """
    segment_map: dict[str, str] = {
        "NIFTY": "NFO-OPT",
        "BANKNIFTY": "NFO-OPT",
        "FINNIFTY": "NFO-OPT",
        "SENSEX": "BFO-OPT",
    }
    segment = segment_map.get(underlying_name, "NFO-OPT")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT * FROM instruments
            WHERE name = ? AND segment = ? AND expiry = ?
              AND strike = ? AND instrument_type = ?
            """,
            (underlying_name, segment, expiry, strike, option_type),
        )
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as exc:
        logger.warning(
            "get_option_instrument(%s, %s, %s, %s): %s",
            underlying_name, expiry, strike, option_type, exc,
        )
        return None


# ---------------------------------------------------------------------------
# Module self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    init_db()
    print("Database initialised at", DB_PATH)
    stats = get_db_stats()
    print(f"Live rows: {stats['total_count']}, Backup rows: {stats['backup_count']}")
