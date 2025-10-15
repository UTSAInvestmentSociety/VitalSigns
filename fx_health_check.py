#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FX Market Health Checks using Bloomberg Desktop API (blpapi)

Now sources vols/skew from dedicated BVOL & derived tickers, e.g.:
- Realized Vol (1M):   EURUSDH1M Curncy  -> PX_LAST
- ATM IV (1M):         EURUSDV1M Curncy -> PX_LAST  
- 25Δ Risk Reversal:   EURUSD25R1M Curncy -> PX_LAST
- 25Δ Butterfly:       EURUSD25B1M Curncy -> PX_LAST 

Also keeps liquidity metrics from spot (bid/ask/last).

Setup:
- pip install blpapi
- Run on a Bloomberg-enabled machine
- Confirm exact mnemonics in your terminal with FLDS <GO>
"""

import sys
import math
import datetime as dt
from collections import defaultdict
import statistics

import blpapi  # Bloomberg Desktop API

# -----------------------------
# User Configuration
# -----------------------------
# Define the FX pairs you want (base format like 'EURUSD', 'USDJPY', etc.)
PAIRS = [
    "EURUSD",
    "USDJPY",
    "GBPUSD",
    "AUDUSD",
    "USDCHF",
    "USDCAD",
]

# Tenors for implied vols / skew (configure what you actually use)
VOL_TENORS = ["1M", "3M"]  # you can remove "3M" if you only want 1M

# Which realized-vol horizons to fetch via dedicated tickers (e.g., H1M/H3M)
REALIZED_TENORS = ["1M", "3M"]  # reduce to ["1M"] if preferred

# Session configuration
SESSION_HOST = "localhost"
SESSION_PORT = 8194

# -----------------------------
# Helpers: ticker builders
# -----------------------------
def spot_ticker(pair):
    # e.g., "EURUSD Curncy"
    return f"{pair} Curncy"

def realized_vol_ticker(pair, tenor):
    # e.g., "EURUSDH1M Curncy" (1M realized) / "EURUSDH3M Curncy"
    # For 1M/3M we assume H{tenor} format; confirm in your terminal if different.
    return f"{pair}H{tenor} Curncy"

def atm_bvol_ticker(pair, tenor):
    # e.g., "EURUSD 1M ATM VOL BVOL Curncy"
    return f"{pair}V{tenor} Curncy"

def rr25_ticker(pair, tenor):
    # e.g., "EURUSD25R1M Curncy" (25-delta risk reversal, 1M)
    return f"{pair}25R{tenor} Curncy"

def bf25_ticker(pair, tenor):
    # e.g., "EURUSD25B1M Curncy" (25-delta butterfly, 1M)
    return f"{pair}25B{tenor} Curncy"

# -----------------------------
# Type/format helpers
# -----------------------------
def to_number(x):
    """Return float(x) if numeric-like; convert common NA tokens to NaN; else return original."""
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if s in ("", "N.A.", "NA", "N/A", "—", "-", "NaN"):
            return float("nan")
        try:
            return float(s.replace(",", ""))
        except Exception:
            return x
    return x

def fmt(x, nd=4):
    """Pretty-print numbers; '—' for NaN/None; strings passed through."""
    if x is None:
        return "—"
    if isinstance(x, float) and x != x:  # NaN
        return "—"
    if isinstance(x, (int, float)):
        return f"{x:.{nd}f}"
    return str(x)

def to_pips(pair, price_diff):
    """Convert price difference to pips based on pair convention."""
    if "JPY" in pair[:6] or "JPY" in pair[-6:]:
        # 1 pip ~ 0.01 for JPY pairs
        return price_diff * 100.0
    # Most majors: 1 pip = 0.0001
    return price_diff * 10000.0

# -----------------------------
# Bloomberg Session + requests
# -----------------------------
class BloombergSession:
    def __init__(self, host=SESSION_HOST, port=SESSION_PORT):
        self.host = host
        self.port = port
        self.session = None

    def __enter__(self):
        opts = blpapi.SessionOptions()
        opts.setServerHost(self.host)
        opts.setServerPort(self.port)
        self.session = blpapi.Session(opts)
        if not self.session.start():
            raise RuntimeError("Failed to start Bloomberg session.")
        if not self.session.openService("//blp/refdata"):
            raise RuntimeError("Failed to open //blp/refdata service.")
        return self.session

    def __exit__(self, exc_type, exc_value, traceback):
        if self.session is not None:
            self.session.stop()

def _send_request(session, request):
    session.sendRequest(request)
    data = []
    while True:
        ev = session.nextEvent()
        for msg in ev:
            if ev.eventType() in (blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE):
                data.append(msg)
        if ev.eventType() == blpapi.Event.RESPONSE:
            break
    return data

def get_reference_data(session, tickers_fields):
    """
    tickers_fields: dict[ticker] = list[fields]
    Returns: {ticker: {field: value}}
    """
    svc = session.getService("//blp/refdata")
    req = svc.createRequest("ReferenceDataRequest")
    fields_added = set()
    for tkr, flist in tickers_fields.items():
        req.getElement("securities").appendValue(tkr)
        for f in flist:
            if f and f not in fields_added:
                req.getElement("fields").appendValue(f)
                fields_added.add(f)
    responses = _send_request(session, req)

    out = {}
    for msg in responses:
        if not msg.hasElement("securityData"):
            continue
        for sdata in msg.getElement("securityData").values():
            sec = sdata.getElementAsString("security")
            fdict = {}
            if sdata.hasElement("fieldData"):
                fd = sdata.getElement("fieldData")
                for f in tickers_fields.get(sec, []):
                    if f and fd.hasElement(f):
                        val = None
                        try:
                            val = fd.getElementAsFloat64(f)
                        except Exception:
                            try:
                                val = fd.getElementAsString(f)
                            except Exception:
                                val = None
                        fdict[f] = to_number(val)
            out[sec] = fdict
    return out

# -----------------------------
# Orchestrate tickers to fetch
# -----------------------------
def build_universe_and_fields(pairs, vol_tenors, realized_tenors):
    """
    Build a dict[ticker] -> fields list for one-shot ReferenceDataRequest.
    - Spot for liquidity (BID/ASK/PX_LAST)
    - Realized vol tickers (PX_LAST)
    - ATM BVOL tickers by tenor (PX_LAST)
    - 25Δ RR & BF tickers by tenor (PX_LAST)
    """
    tkrs = {}

    # Core spot fields
    SPOT_FIELDS = ["BID", "ASK", "PX_LAST"]

    for p in pairs:
        # Spot
        tkrs[spot_ticker(p)] = SPOT_FIELDS.copy()

        # Realized vols
        for rt in realized_tenors:
            tkrs[realized_vol_ticker(p, rt)] = ["PX_LAST"]

        # IV / skew per tenor
        for t in vol_tenors:
            tkrs[atm_bvol_ticker(p, t)] = ["PX_LAST"]
            tkrs[rr25_ticker(p, t)] = ["PX_LAST"]
            tkrs[bf25_ticker(p, t)] = ["PX_LAST"]

    return tkrs

# -----------------------------
# Main runner
# -----------------------------
def run_fx_health_checks():
    # Build the one-shot universe
    tickers_fields = build_universe_and_fields(PAIRS, VOL_TENORS, REALIZED_TENORS)

    with BloombergSession() as session:
        ref = get_reference_data(session, tickers_fields)

    # Build report rows per pair
    rows = []
    for pair in PAIRS:
        # Liquidity from spot
        sp = spot_ticker(pair)
        sd = ref.get(sp, {})
        bid = sd.get("BID")
        ask = sd.get("ASK")
        spot = sd.get("PX_LAST")
        spread = (ask - bid) if (isinstance(ask, (int, float)) and isinstance(bid, (int, float))) else float("nan")
        pips = to_pips(pair, spread) if spread == spread else float("nan")
        bps_of_spot = (spread / spot * 10000.0) if (spread == spread and spot and spot == spot and spot != 0) else float("nan")

        # Realized vols (from dedicated tickers)
        realized = {}
        for rt in REALIZED_TENORS:
            rv_tkr = realized_vol_ticker(pair, rt)
            realized[rt] = ref.get(rv_tkr, {}).get("PX_LAST")

        # ATM IVs (BVOL) & skew (RR/BF) from dedicated tickers
        ivs, rrs, bfs = {}, {}, {}
        for t in VOL_TENORS:
            iv_tkr = atm_bvol_ticker(pair, t)
            rr_tkr = rr25_ticker(pair, t)
            bf_tkr = bf25_ticker(pair, t)
            ivs[t] = ref.get(iv_tkr, {}).get("PX_LAST")
            rrs[t] = ref.get(rr_tkr, {}).get("PX_LAST")
            bfs[t] = ref.get(bf_tkr, {}).get("PX_LAST")

        rows.append({
            "pair": pair,
            "spot": spot,
            "bid": bid,
            "ask": ask,
            "spread": spread,
            "spread_pips": pips,
            "spread_bps_of_spot": bps_of_spot,
            "realized": realized,  # dict tenor->level
            "iv": ivs,             # dict tenor->ATM IV
            "rr25": rrs,           # dict tenor->RR
            "bf25": bfs,           # dict tenor->BF
        })

    # ---- Render ----
    print("\nFX Health Check (spot liquidity + BVOL/derived vols & skew)\n")

    # Liquidity table
    headers = ["PAIR", "SPOT", "BID", "ASK", "SPR", "SPR PIPS", "SPR BPS/Spot"]
    print("{:>10} {:>12} {:>12} {:>12} {:>10} {:>12} {:>14}".format(*headers))
    for r in rows:
        print("{:>10} {:>12} {:>12} {:>12} {:>10} {:>12} {:>14}".format(
            r["pair"],
            fmt(r["spot"], 6),
            fmt(r["bid"], 6),
            fmt(r["ask"], 6),
            fmt(r["spread"], 6),
            fmt(r["spread_pips"], 2),
            fmt(r["spread_bps_of_spot"], 2),
        ))

    # Details
    print("\nVol & Skew (levels from dedicated tickers)\n")
    for r in rows:
        print(f"{r['pair']}:")
        # Realized vols
        if r["realized"]:
            rv_str = ", ".join([f"{ten}:{fmt(val,4)}" for ten, val in r["realized"].items()])
            print(f"  Realized Vol -> {rv_str}")
        # ATM IVs
        if r["iv"]:
            iv_str = ", ".join([f"{ten}:{fmt(val,4)}" for ten, val in r["iv"].items()])
            print(f"  ATM IVs      -> {iv_str}")
        # RR
        if r["rr25"]:
            rr_str = ", ".join([f"{ten}:{fmt(val,4)}" for ten, val in r["rr25"].items()])
            print(f"  25Δ RR       -> {rr_str}")
        # BF
        if r["bf25"]:
            bf_str = ", ".join([f"{ten}:{fmt(val,4)}" for ten, val in r["bf25"].items()])
            print(f"  25Δ Fly      -> {bf_str}")
        print()

# -----------------------------
# Entry
# -----------------------------
if __name__ == "__main__":
    try:
        run_fx_health_checks()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
