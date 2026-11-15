# -*- coding: utf-8 -*-
"""
kiteconnect (Upstox edition)
============================

Drop-in replacement for Zerodha's ``kiteconnect`` package that talks to the
**Upstox** APIs instead. It exposes the same ``KiteConnect`` / ``KiteTicker``
classes, constants, return shapes and exceptions, so code written for Kite
runs unchanged on an Upstox account.

Key translations
----------------
* Instruments: Upstox's instrument master is converted to Kite rows, with
  Kite-style trading symbols (``NIFTY25N1824500CE`` weekly,
  ``NIFTY25NOV24500CE`` monthly, ``NIFTY25NOVFUT``) and stable integer
  ``instrument_token`` values.  Exchanges map NSE/BSE/NFO/BFO <-> NSE_EQ/
  BSE_EQ/NSE_FO/BSE_FO; indices are ``NSE:NIFTY 50``, ``BSE:SENSEX`` ...
* Products: MIS <-> I, NRML/CNC <-> D.  Order statuses are upper-cased Kite
  style (OPEN, COMPLETE, CANCELLED, REJECTED, TRIGGER PENDING ...).
* GTT: single-leg GTTs map to Upstox GTT (type SINGLE, one ENTRY rule).
  Upstox fires a LIMIT order at the trigger price.  Two-leg (OCO) GTTs are
  not supported and raise ``InputException``.
* Mutual-fund APIs are not available on Upstox and raise ``InputException``.

Environment
-----------
UPSTOX_REDIRECT_URI    redirect URL registered on the Upstox app (login flow)
UPSTOX_ALGO_NAME       value for the ``X-Algo-Name`` header on order APIs
UPSTOX_KITE_CACHE_DIR  where the converted instrument master is cached
"""

from __future__ import unicode_literals, absolute_import

from kiteconnect import exceptions
from kiteconnect.connect import KiteConnect
from kiteconnect.ticker import KiteTicker

__all__ = ["KiteConnect", "KiteTicker", "exceptions"]
__title__ = "kiteconnect"
__description__ = "Kite Connect compatible client backed by the Upstox API"
__version__ = "4.1.0+upstox.1"
__license__ = "MIT"
