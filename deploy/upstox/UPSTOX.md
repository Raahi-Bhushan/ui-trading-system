# UI Trading System: Upstox edition

This fork runs the original Zerodha-based system on an **Upstox** account. The app code is almost unchanged. `vendor/upstox_kiteconnect` is a drop-in `kiteconnect` package that has the same classes, constants, return shapes and exceptions as Zerodha's, but calls Upstox's API underneath.

## What changed
| File | Change |
|---|---|
| `vendor/upstox_kiteconnect/` | **New.** Kite-compatible client (`KiteConnect`) and live-price websocket (`KiteTicker`) for Upstox |
| `requirements.txt` | Installs the Upstox adapter instead of `vendor/pykiteconnect` |
| `flask_app.py` | The login callback accepts Upstox's `?code=`; the login link points to Upstox |
| `common_lib.py` | The ticker watchdog reconnects without Twisted: it closes the old feed, starts a new one and resubscribes. The login-URL print and the dry-run message now name Upstox |
| `instrument_cache.py` | Sync timestamps are stored as `YYYY-MM-DD HH:MM:SS`. This fixes an upstream crash in `get_db_stats()` |
| `templates/setup_wizard.html` | The wizard points to Upstox Developer Apps |
| `tests/test_upstox_adapter.py` | **New.** 13 tests for the adapter |
| `tests/test_watchdog.py` | The ticker tests now match the Upstox reconnect behaviour |

Test suite: **294 passed**. The same 11 tests fail with the original Zerodha library too (pre-existing upstream issues), and the adapter adds no new failures.

## How it maps
- **Symbols:** Upstox instruments get Kite names: weekly `NIFTY25N1824500CE`, monthly `NIFTY25NOV24500CE`, futures `NIFTY25NOVFUT`. Indices are `NSE:NIFTY 50` and `BSE:SENSEX`. Instrument tokens are stable integers.
- **Exchanges:** NSE/BSE/NFO/BFO map to NSE_EQ/BSE_EQ/NSE_FO/BSE_FO.
- **Products:** MIS maps to I. NRML and CNC both map to D.
- **Statuses:** order statuses use Kite's upper-case style (OPEN, COMPLETE, TRIGGER PENDING …).
- **Instrument master:** downloaded once per day into a shared SQLite file (`UPSTOX_KITE_CACHE_DIR`). Each strategy process uses about 0 MB extra memory for it.
- **Live prices:** Upstox v3 protobuf feed, with TLS verified. Order updates come from Upstox's portfolio stream.

## Differences from Zerodha you should know
1. **GTT:** a GTT is a single trigger that places a LIMIT order at the trigger price. The app already sets limit = trigger, so behaviour matches. Two-leg (OCO) GTTs are not available.
2. **Market orders:** Upstox applies Market Price Protection to every API market order (exchange rule).
3. **Static IP:** order APIs only accept requests from the IPs registered on your Upstox app. Register the server's Elastic IP. Changing the IP logs the app out.
4. **Algo name:** if Upstox rejects orders asking for an algo name, set `UPSTOX_ALGO_NAME` in `/etc/ui-trading.env` to the exact Algo Name from My Apps.
5. **Daily login:** Upstox tokens expire every day around 3:30 AM. Log in from the dashboard header each morning.
6. **Not available on Upstox:** mutual-fund orders, cover/bracket/iceberg orders, `trigger_range`.
7. **Approximations:**
   - Position `m2m` equals Upstox `pnl`.
   - Tick `oi_day_high`/`oi_day_low` are 0, and depth `orders` counts are 0 (Upstox doesn't send them).
8. **Chart links** in some pages still open Zerodha Kite charts. They are cosmetic only.

## Go-live checklist (keep `live_trading = false` until every box is ticked)
- [ ] `verify_upstox.py` shows correct NIFTY/SENSEX symbols, lot sizes and tick sizes (0.05)
- [ ] After the Upstox login: positions, orders, GTT list and Greeks match the Upstox app
- [ ] Run each strategy for a few sessions in DRY-RUN and read the `[DRY-RUN]` log lines: right symbols, quantities and prices
- [ ] Static IP registered; one tiny live order placed and cancelled by hand from the dashboard
- [ ] Then set `[safety] live_trading = true` in `configfile.ini` and restart: `sudo systemctl restart ui-trading`
