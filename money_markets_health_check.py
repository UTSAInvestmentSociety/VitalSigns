#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Money Markets Health Checks using Bloomberg Desktop API (blpapi)
Now includes CPDR3ANC Index (30D AA Non-Financial CP) alongside SOFR.

Core USD checks:
1) Funding stress:
   a) SOFR - EFFR (basis)
   b) CP (30D AA Non-Fin) - EFFR (basis)
   c) Optional: CP - SOFR (basis)
   d) EFFR vs FOMC target band (distance to bounds and midpoint)
2) Liquidity conditions:
   a) SOFR (GC repo proxy)
   b) CP (credit-based unsecured proxy)
   c) ON RRP (level, direction)
3) Optional: OIS curve points, 3M credit vs OIS

All rates normalized to DECIMAL units internally (e.g., 5.33% -> 0.0533).
"""

import sys
import datetime as dt
from collections import defaultdict
import statistics
import blpapi  # Bloomberg Desktop API

# -----------------------------------
# User Configuration: Tickers/Fields
# -----------------------------------
CFG = {
    # --- Core USD references (commonly available) ---
    # Effective Fed Funds Rate (PX_LAST is in percent)
    "EFFR_TICKER": "FEDL01 Index",
    "EFFR_FIELD":  "PX_LAST",

    # FOMC target band (percent)
    "FDTR_UP_TICKER": "FDTR Index",        # Upper bound
    "FDTR_DN_TICKER": "FDTRFTRL Index",    # Lower bound (use FDTRD/FDTRL if needed)
    "FDTR_FIELD": "PX_LAST",

    # SOFR (percent)
    "SOFR_TICKER": "SOFRRATE Index",
    "SOFR_FIELD":  "PX_LAST",

    # 30D AA Non-Financial Commercial Paper (percent)
    "CP_TICKER": "CPDR3ANC Index",
    "CP_FIELD":  "PX_LAST",

    # ON RRP award rate (percent) — confirm your house ticker
    "RRP_TICKER": "TOMOTCSO Index",
    "RRP_FIELD":  "PX_LAST",

    # --- Optional USD points (set to None if you don't use them) ---
    "USD_OIS_TICKERS": {
        "1M": None,   # e.g., "USSO1M Curncy"
        "3M": None,   # e.g., "USSO3M Curncy"
        "6M": None,   # e.g., "USSO6M Curncy"
        "1Y": None,   # e.g., "USSO1 Curncy"
    },
    "OIS_FIELD": "PX_LAST",  # usually percent

    # Optional: 3M credit vs OIS
    "USD_3M_CREDIT_TICKER": None,  # e.g., "SOFR3M Index" or "US0003M Index"
    "USD_3M_OIS_TICKER":    None,
    "CREDIT_OIS_FIELD":     "PX_LAST",

    # History windows
    "LOOKBACK_CAL_DAYS": 40,
    "OBS_DAYS": 20,
}

SESSION_HOST = "localhost"
SESSION_PORT = 8194

# -----------------------------------
# Type helpers (coercion & units)
# -----------------------------------
def to_number(x):
    """Coerce Bloomberg values to float; map common NA tokens to NaN."""
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = x.strip()
        if s in ("", "N.A.", "NA", "N/A", "—", "-", "NaN"):
            return float("nan")
        try:
            return float(s.replace(",", ""))
        except Exception:
            return float("nan")
    return float("nan")

def to_decimal_rate(x):
    """
    Convert a percent-quoted rate to decimal (e.g., 5.33 -> 0.0533).
    We assume gov/overnight rates are returned in PERCENT units.
    """
    v = to_number(x)
    if v == v:  # not NaN
        return v / 100.0
    return v

def pct(x):
    """Render decimal rate as percentage (not bp)."""
    if x is None or x != x:
        return None
    return x * 100.0

def bp(x):
    """Render decimal rate difference as basis points."""
    if x is None or x != x:
        return None
    return x * 10000.0

# -----------------------------------
# Bloomberg Session Helpers
# -----------------------------------
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
    msgs = []
    while True:
        ev = session.nextEvent()
        for msg in ev:
            if ev.eventType() in (blpapi.Event.PARTIAL_RESPONSE, blpapi.Event.RESPONSE):
                msgs.append(msg)
        if ev.eventType() == blpapi.Event.RESPONSE:
            break
    return msgs

def bbg_reference(session, tickers_fields):
    """tickers_fields: dict[ticker] = list[field]"""
    svc = session.getService("//blp/refdata")
    req = svc.createRequest("ReferenceDataRequest")
    for t in tickers_fields:
        req.getElement("securities").appendValue(t)
    added = set()
    for flist in tickers_fields.values():
        for f in flist:
            if f and f not in added:
                req.getElement("fields").appendValue(f)
                added.add(f)
    msgs = _send_request(session, req)
    out = {}
    for msg in msgs:
        if not msg.hasElement("securityData"):
            continue
        for sdata in msg.getElement("securityData").values():
            sec = sdata.getElementAsString("security")
            fdict = {}
            if sdata.hasElement("fieldData"):
                fd = sdata.getElement("fieldData")
                for f in tickers_fields.get(sec, []):
                    if f and fd.hasElement(f):
                        # Try float then string, then coerce & normalize to decimal
                        val = None
                        try:
                            val = fd.getElementAsFloat64(f)
                        except Exception:
                            try:
                                val = fd.getElementAsString(f)
                            except Exception:
                                val = None
                        fdict[f] = to_decimal_rate(val)  # percent -> decimal
            out[sec] = fdict
    return out

def bbg_history(session, tickers, field, start_date, end_date, periodicity="DAILY"):
    svc = session.getService("//blp/refdata")
    req = svc.createRequest("HistoricalDataRequest")
    for t in tickers:
        req.getElement("securities").appendValue(t)
    req.getElement("fields").appendValue(field)
    req.set("periodicitySelection", periodicity)
    req.set("startDate", start_date.strftime("%Y%m%d"))
    req.set("endDate", end_date.strftime("%Y%m%d"))
    req.set("nonTradingDayFillOption", "NON_TRADING_WEEKDAYS")
    req.set("nonTradingDayFillMethod", "PREVIOUS_VALUE")
    msgs = _send_request(session, req)
    out = defaultdict(list)
    for msg in msgs:
        if not msg.hasElement("securityData"):
            continue
        sdata = msg.getElement("securityData")
        sec = sdata.getElementAsString("security")
        if sdata.hasElement("fieldData"):
            for bar in sdata.getElement("fieldData").values():
                d = bar.getElementAsDatetime("date")
                val = float("nan")
                if bar.hasElement(field):
                    try:
                        raw = bar.getElementAsFloat64(field)
                    except Exception:
                        raw = None
                    val = to_decimal_rate(raw)  # percent -> decimal
                out[sec].append((d, val))
    return dict(out)

# -----------------------------------
# Stats Helper
# -----------------------------------
def realized_stdev(values, last_n=20):
    xs = [v for v in values if v == v]
    if len(xs) < last_n:
        return float("nan")
    xs = xs[-last_n:]
    if len(xs) < 2:
        return float("nan")
    return statistics.pstdev(xs)

# -----------------------------------
# Health Checks
# -----------------------------------
def run_money_market_health_checks():
    today = dt.date.today()
    start = today - dt.timedelta(days=CFG["LOOKBACK_CAL_DAYS"])
    end = today

    tickers_fields = {
        CFG["EFFR_TICKER"]:      [CFG["EFFR_FIELD"]],
        CFG["FDTR_UP_TICKER"]:   [CFG["FDTR_FIELD"]],
        CFG["FDTR_DN_TICKER"]:   [CFG["FDTR_FIELD"]],
        CFG["SOFR_TICKER"]:      [CFG["SOFR_FIELD"]],
        CFG["CP_TICKER"]:        [CFG["CP_FIELD"]],
        CFG["RRP_TICKER"]:       [CFG["RRP_FIELD"]],
    }

    # Optional OIS points
    for tenor, tkr in CFG["USD_OIS_TICKERS"].items():
        if tkr:
            tickers_fields[tkr] = [CFG["OIS_FIELD"]]

    # Optional credit vs OIS
    if CFG["USD_3M_CREDIT_TICKER"] and CFG["USD_3M_OIS_TICKER"]:
        tickers_fields[CFG["USD_3M_CREDIT_TICKER"]] = [CFG["CREDIT_OIS_FIELD"]]
        tickers_fields[CFG["USD_3M_OIS_TICKER"]]    = [CFG["CREDIT_OIS_FIELD"]]

    with BloombergSession() as session:
        ref = bbg_reference(session, tickers_fields)

        # Histories (normalize to decimal inside bbg_history)
        hist_tickers = [
            CFG["EFFR_TICKER"],
            CFG["SOFR_TICKER"],
            CFG["CP_TICKER"],
            CFG["RRP_TICKER"],
        ]
        # Use field-by-field because some series may differ
        hist = {}
        # EFFR
        hist.update(bbg_history(session, [CFG["EFFR_TICKER"]], CFG["EFFR_FIELD"], start, end))
        # SOFR
        hist.update(bbg_history(session, [CFG["SOFR_TICKER"]], CFG["SOFR_FIELD"], start, end))
        # CP
        hist.update(bbg_history(session, [CFG["CP_TICKER"]], CFG["CP_FIELD"], start, end))
        # RRP
        hist.update(bbg_history(session, [CFG["RRP_TICKER"]], CFG["RRP_FIELD"], start, end))

    # Extract snapshot (already decimal)
    def get_val(tkr, fld):
        d = ref.get(tkr, {})
        return d.get(fld, float("nan"))

    effr    = get_val(CFG["EFFR_TICKER"], CFG["EFFR_FIELD"])
    sofr    = get_val(CFG["SOFR_TICKER"], CFG["SOFR_FIELD"])
    cp30d   = get_val(CFG["CP_TICKER"],   CFG["CP_FIELD"])
    fdtr_up = get_val(CFG["FDTR_UP_TICKER"], CFG["FDTR_FIELD"])
    fdtr_dn = get_val(CFG["FDTR_DN_TICKER"], CFG["FDTR_FIELD"])
    rrp     = get_val(CFG["RRP_TICKER"], CFG["RRP_FIELD"])

    # Core spreads (all decimal)
    sofr_effr   = sofr - effr if (sofr == sofr and effr == effr) else float("nan")
    cp_effr     = cp30d - effr if (cp30d == cp30d and effr == effr) else float("nan")
    cp_sofr     = cp30d - sofr if (cp30d == cp30d and sofr == sofr) else float("nan")

    band_mid    = (fdtr_up + fdtr_dn) / 2.0 if (fdtr_up == fdtr_up and fdtr_dn == fdtr_dn) else float("nan")
    effr_to_up  = fdtr_up - effr if (fdtr_up == fdtr_up and effr == effr) else float("nan")
    effr_to_lo  = effr - fdtr_dn if (fdtr_dn == fdtr_dn and effr == effr) else float("nan")
    effr_to_mid = effr - band_mid if (band_mid == band_mid and effr == effr) else float("nan")

    # Recent variability (stdev of levels in decimal)
    def series_vals(tkr):
        arr = hist.get(tkr, [])
        return [v for (_, v) in arr if v == v]

    effr_stdev = realized_stdev(series_vals(CFG["EFFR_TICKER"]), CFG["OBS_DAYS"])
    sofr_stdev = realized_stdev(series_vals(CFG["SOFR_TICKER"]), CFG["OBS_DAYS"])
    cp_stdev   = realized_stdev(series_vals(CFG["CP_TICKER"]),   CFG["OBS_DAYS"])
    rrp_stdev  = realized_stdev(series_vals(CFG["RRP_TICKER"]),  CFG["OBS_DAYS"])

    # Optional: OIS snapshot map
    ois_points = {}
    for tenor, tkr in CFG["USD_OIS_TICKERS"].items():
        if tkr:
            ois_points[tenor] = get_val(tkr, CFG["OIS_FIELD"])

    # Optional: 3M credit vs OIS
    credit_ois_spread = float("nan")
    if CFG["USD_3M_CREDIT_TICKER"] and CFG["USD_3M_OIS_TICKER"]:
        c3m = get_val(CFG["USD_3M_CREDIT_TICKER"], CFG["CREDIT_OIS_FIELD"])
        o3m = get_val(CFG["USD_3M_OIS_TICKER"],    CFG["CREDIT_OIS_FIELD"])
        if c3m == c3m and o3m == o3m:
            credit_ois_spread = c3m - o3m

    # --------------------------
    # Render/Report
    # --------------------------
    def fmt(x, nd=3, as_bp=False, as_pct=False):
        if x is None or x != x:
            return "—"
        if as_bp:
            return f"{bp(x):.1f} bp"
        if as_pct:
            return f"{pct(x):.{nd}f}%"
        return f"{x:.{nd}f}"

    print("\nUSD Money Market Health Check (snapshot + recent variability)\n")

    print("Overnight / Short-Tenor Benchmarks")
    print("-----------------------------------")
    print(f"EFFR (Effective Fed Funds):       {fmt(effr, 4, as_pct=True)}   (σ_{CFG['OBS_DAYS']}d ≈ {fmt(effr_stdev, 4, as_bp=True)})")
    print(f"SOFR (GC repo proxy):             {fmt(sofr, 4, as_pct=True)}   (σ_{CFG['OBS_DAYS']}d ≈ {fmt(sofr_stdev, 4, as_bp=True)})")
    print(f"CP 30D AA Non-Fin (CPDR3ANC):     {fmt(cp30d, 4, as_pct=True)}   (σ_{CFG['OBS_DAYS']}d ≈ {fmt(cp_stdev, 4, as_bp=True)})")
    print(f"ON RRP (administered rate):       {fmt(rrp,  4, as_pct=True)}   (σ_{CFG['OBS_DAYS']}d ≈ {fmt(rrp_stdev, 4, as_bp=True)})")
    print()

    print("Funding Stress & Policy Transmission")
    print("------------------------------------")
    print(f"SOFR - EFFR (basis):              {fmt(sofr_effr, as_bp=True)}")
    print(f"CP 30D - EFFR (basis):            {fmt(cp_effr,   as_bp=True)}")
    print(f"CP 30D - SOFR (basis):            {fmt(cp_sofr,   as_bp=True)}")
    print(f"FOMC Target Band (Lower→Upper):   {fmt(fdtr_dn, 4, as_pct=True)} → {fmt(fdtr_up, 4, as_pct=True)}")
    print(f"EFFR distance to Lower/Upper:     {fmt(effr_to_lo, as_bp=True)} / {fmt(effr_to_up, as_bp=True)}")
    print(f"EFFR distance to Midpoint:        {fmt(effr_to_mid, as_bp=True)}")
    print()

    if ois_points:
        print("Simple USD OIS Points (snapshot)")
        print("--------------------------------")
        for tenor in sorted(ois_points, key=lambda x: (len(x), x)):
            val = ois_points[tenor]
            print(f"OIS {tenor}:                      {fmt(val, 4, as_pct=True)}")
        print()

    if credit_ois_spread == credit_ois_spread:
        print("3M Credit vs OIS (optional)")
        print("---------------------------")
        print(f"3M Credit – 3M OIS:              {fmt(credit_ois_spread, as_bp=True)}")
        print()

    # Simple flags (rule-of-thumb; customize for your risk framework)
    print("Diagnostics / Flags (heuristics)")
    print("--------------------------------")
    flags = []
    # SOFR–EFFR dislocation
    if sofr_effr == sofr_effr and abs(bp(sofr_effr)) > 5.0:
        flags.append(f"SOFR–EFFR basis |abs| > 5 bp ({fmt(sofr_effr, as_bp=True)})")
    # CP–EFFR signal (credit tightening/loosening)
    if cp_effr == cp_effr and abs(bp(cp_effr)) > 10.0:
        flags.append(f"CP(30D)–EFFR basis |abs| > 10 bp ({fmt(cp_effr, as_bp=True)})")
    # CP–SOFR large gap (repo vs unsecured credit conditions)
    if cp_sofr == cp_sofr and abs(bp(cp_sofr)) > 10.0:
        flags.append(f"CP(30D)–SOFR basis |abs| > 10 bp ({fmt(cp_sofr, as_bp=True)})")
    # EFFR proximity to corridor bounds
    if effr_to_lo == effr_to_lo and effr_to_lo < 2e-4:   # < 2 bp from lower band
        flags.append(f"EFFR near LOWER band ({fmt(effr_to_lo, as_bp=True)} from lower)")
    if effr_to_up == effr_to_up and effr_to_up < 2e-4:   # < 2 bp from upper band
        flags.append(f"EFFR near UPPER band ({fmt(effr_to_up, as_bp=True)} from upper)")

    if not flags:
        print("No heuristic flags triggered.")
    else:
        for f in flags:
            print(f"- {f}")

# -----------------------------------
# Entry Point
# -----------------------------------
if __name__ == "__main__":
    try:
        run_money_market_health_checks()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
