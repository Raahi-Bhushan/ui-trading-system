"""Post-install check for the Upstox edition (run as the service user).

    sudo -u uitrading /opt/ui-trading/app/.venv/bin/python deploy/upstox/verify_upstox.py

1. Downloads + converts the Upstox instrument master and prints sample Kite-style symbols.
2. If an access token is stored (after the first Upstox login), calls profile/ltp read-only.
Never places, modifies or cancels anything.
"""
import collections
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from kiteconnect import KiteConnect  # noqa: E402
from kiteconnect.instruments import EXCH, EXPIRY, ITYPE, LOT, NAME, SEG, TICK, TSYM, InstrumentMap  # noqa: E402


def main() -> int:
    im = InstrumentMap.get()
    print(f"instruments converted: {len(im.rows)}")
    print("by segment:", dict(collections.Counter(r[SEG] for r in im.rows)))
    for name, exch in (("NIFTY", "NFO"), ("SENSEX", "BFO"), ("BANKNIFTY", "NFO")):
        opts = sorted((r for r in im.rows if r[NAME] == name and r[EXCH] == exch and r[ITYPE] in ("CE", "PE")),
                      key=lambda r: (r[EXPIRY], r[TSYM]))
        exps = sorted({r[EXPIRY] for r in opts})[:4]
        print(f"{name}: {len(opts)} options, next expiries {exps}")
        for e in exps[:3]:
            r = next(r for r in opts if r[EXPIRY] == e)
            print(f"   {r[TSYM]:28s} lot={r[LOT]} tick={r[TICK]}")
    for key in ("NSE:NIFTY 50", "NSE:NIFTY BANK", "BSE:SENSEX", "NSE:INDIA VIX", "NSE:RELIANCE"):
        r = im.from_kite_key(key)
        print(f"{key:18s} -> {r[1] if r else 'NOT FOUND'}{'' if not r else f'  tick={r[TICK]}'}")
    ticks = collections.Counter(r[TICK] for r in im.rows if r[SEG] in ("NFO-OPT", "NSE"))
    print("tick sizes (NFO options + NSE equity):", ticks.most_common(5))

    try:
        import instrument_cache
        tok = instrument_cache.get_kite_token()
    except Exception as e:  # noqa: BLE001
        tok = None
        print("token lookup skipped:", e)
    if not tok:
        print("\nNo Upstox token stored yet - log in once from the dashboard, then re-run for live checks.")
        return 0
    import configparser
    cfg = configparser.ConfigParser()
    cfg.read(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "configfile.ini"))
    k = KiteConnect(cfg["kite_login_details"]["api_key"], access_token=tok)
    p = k.profile()
    print(f"\nprofile: {p['user_id']} {p['user_name']}  exchanges={p['exchanges']}")
    print("ltp:", k.ltp("NSE:NIFTY 50", "BSE:SENSEX"))
    pos = k.positions()["net"]
    print(f"positions: {len(pos)} | orders today: {len(k.orders())} | GTTs: {len(k.get_gtts())}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
