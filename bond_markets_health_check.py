#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bond Markets Health Checks using Bloomberg Desktop API (blpapi)

Fixes:
- Coerce Bloomberg values (incl. strings like "N.A.") to floats
- Keep units consistent (UST yields in %, OAS in bp)
"""

import sys
import datetime as dt
import statistics
from collections import defaultdict

import blpapi  # Bloomberg Desktop API

# -----------------------------------
# User Configuration (USD defaults)
# -----------------------------------
CFG = {
    # U.S. Treasury benchmark yields (indices, %)
    "UST_2Y_TICKER":  "USGG2YR Index",
    "UST_10Y_TICKER": "USGG10YR Index",
    "UST_3M_TICKER":  "USGG3M Index",
    "YIELD_FIELD":    "PX_LAST",       # yields in %

    # Credit spreads (ICE BofA OAS, typically in bps)
    "IG_OAS_TICKER":  "LUACOAS Index",  # ICE BofA US Corporate Index OAS
    "HY_OAS_TICKER":  "LF98OAS Index",  # ICE BofA US High Yield Index OAS
    "OAS_FIELD":      "PX_LAST",        # spreads in bps

    # Volatility (UST): MOVE index
    "MOVE_TICKER":    "MOVE Index",
    "MOVE_FIELD":     "PX_LAST",

    # Liquidity proxies (optional): IG/HY ETFs
    "IG_LIQ_TICKER":  "LQD US Equity",
    "HY_LIQ_TICKER":  "HYG US Equity",
    "LIQ_FIELDS":     {
        "BID": "BID",
        "ASK": "ASK",
        "LAST": "PX_LAST",
        "VOLUME": "VOLUME"  # shares
    },

    # History windows
    "LOOKBACK_CAL_DAYS": 45,   # calendar days fetched
    "OBS_DAYS": 20,            # bars used for realized variability and changes

    # Heuristic flag thresholds
    "CURVE_INVERSION_BP": -1.0,   # trigger if slope < -1bp
    "IG_WIDEN_BP": 10.0,          # widen > +10bp over OBS_DAYS
    "HY_WIDEN_BP": 25.0,          # widen > +25bp over OBS_DAYS
}

SESSION_HOST = "localhost"
SESSION_PORT = 8194

# -----------------------------------
# Type / Format Helpers
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

def stdev_last(xs, n=20):
    ys = [v for v in xs if v == v]
    if len(ys) < n:
        return float("nan")
    ys = ys[-n:]
    if len(ys) < 2:
        return float("nan")
    return statistics.pstdev(ys)

def last_change(xs, n=20):
    ys = [v for v in xs if v == v]
    if len(ys) < n:
        return float("nan")
    return ys[-1] - ys[-n]

def fmt(x, nd=2):
    if x is None or x != x:
        return "—"
    return f"{x:.{nd}f}"

def extract_values(series):
    return [to_number(v) for (_, v) in series if to_number(v) == to_number(v)]

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

    def __exit__(self, exc_type, exc_val, exc_tb):
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
    """
    tickers_fields: dict[str, list[str]]
    Returns: {ticker: {field: float_or_nan}}
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
                        # Try float, else string -> coerce
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
                        try:
                            raw = bar.getElementAsString(field)
                        except Exception:
                            raw = None
                    val = to_number(raw)
                out[sec].append((d, val))
    return dict(out)

# -----------------------------------
# Health Checks
# -----------------------------------
def run_bond_market_health_checks():
    today = dt.date.today()
    start = today - dt.timedelta(days=CFG["LOOKBACK_CAL_DAYS"])
    end = today

    # Snapshot tickers/fields
    tickers_fields = {
        CFG["UST_2Y_TICKER"]:  [CFG["YIELD_FIELD"]],
        CFG["UST_10Y_TICKER"]: [CFG["YIELD_FIELD"]],
        CFG["UST_3M_TICKER"]:  [CFG["YIELD_FIELD"]],
        CFG["IG_OAS_TICKER"]:  [CFG["OAS_FIELD"]],
        CFG["HY_OAS_TICKER"]:  [CFG["OAS_FIELD"]],
        CFG["MOVE_TICKER"]:    [CFG["MOVE_FIELD"]],
    }

    # Liquidity proxy ETFs (optional)
    if CFG["IG_LIQ_TICKER"]:
        tickers_fields[CFG["IG_LIQ_TICKER"]] = list(CFG["LIQ_FIELDS"].values())
    if CFG["HY_LIQ_TICKER"]:
        tickers_fields[CFG["HY_LIQ_TICKER"]] = list(CFG["LIQ_FIELDS"].values())

    with BloombergSession() as session:
        ref = bbg_reference(session, tickers_fields)

        # Histories
        yld_tkrs = [CFG["UST_2Y_TICKER"], CFG["UST_10Y_TICKER"], CFG["UST_3M_TICKER"]]
        hist_yields = {}
        for t in yld_tkrs:
            hist_yields.update(bbg_history(session, [t], CFG["YIELD_FIELD"], start, end))

        oas_tkrs = [CFG["IG_OAS_TICKER"], CFG["HY_OAS_TICKER"]]
        hist_oas = {}
        for t in oas_tkrs:
            hist_oas.update(bbg_history(session, [t], CFG["OAS_FIELD"], start, end))

    # Extract snapshots (coerced to float)
    def snap(tkr, fld):
        return ref.get(tkr, {}).get(fld, float("nan"))

    y2  = snap(CFG["UST_2Y_TICKER"],  CFG["YIELD_FIELD"])   # percent (e.g., 4.32)
    y10 = snap(CFG["UST_10Y_TICKER"], CFG["YIELD_FIELD"])   # percent
    y3m = snap(CFG["UST_3M_TICKER"],  CFG["YIELD_FIELD"])   # percent

    ig_oas = snap(CFG["IG_OAS_TICKER"], CFG["OAS_FIELD"])   # bp
    hy_oas = snap(CFG["HY_OAS_TICKER"], CFG["OAS_FIELD"])   # bp
    move   = snap(CFG["MOVE_TICKER"],   CFG["MOVE_FIELD"])  # index level

    # Slopes (bp): yields are % points, so multiply by 100
    slope_2s10s = (y10 - y2) * 100.0 if (y10 == y10 and y2 == y2) else float("nan")
    slope_3m10y = (y10 - y3m) * 100.0 if (y10 == y10 and y3m == y3m) else float("nan")

    # Hist-derived stats
    y2_hist  = extract_values(hist_yields.get(CFG["UST_2Y_TICKER"], []))
    y10_hist = extract_values(hist_yields.get(CFG["UST_10Y_TICKER"], []))
    y3m_hist = extract_values(hist_yields.get(CFG["UST_3M_TICKER"], []))
    ig_hist  = extract_values(hist_oas.get(CFG["IG_OAS_TICKER"], []))
    hy_hist  = extract_values(hist_oas.get(CFG["HY_OAS_TICKER"], []))

    # Yield variability (bp) — since yields are in %, multiply stdev by 100
    y2_stdev_bp  = stdev_last(y2_hist,  CFG["OBS_DAYS"]) * 100.0
    y10_stdev_bp = stdev_last(y10_hist, CFG["OBS_DAYS"]) * 100.0
    y3m_stdev_bp = stdev_last(y3m_hist, CFG["OBS_DAYS"]) * 100.0

    # OAS changes are already in bp; no rescale
    ig_chg_bp = last_change(ig_hist, CFG["OBS_DAYS"])
    hy_chg_bp = last_change(hy_hist, CFG["OBS_DAYS"])

    # Liquidity proxies (ETF)
    def liq_metrics(ticker):
        if not ticker:
            return None
        f = CFG["LIQ_FIELDS"]
        d = ref.get(ticker, {})
        bid = to_number(d.get(f["BID"], float("nan")))
        ask = to_number(d.get(f["ASK"], float("nan")))
        last = to_number(d.get(f["LAST"], float("nan")))
        vol  = to_number(d.get(f["VOLUME"], float("nan")))
        spr  = (ask - bid) if (ask == ask and bid == bid) else float("nan")
        mid  = (ask + bid)/2.0 if (ask == ask and bid == bid) else float("nan")
        spr_bps_of_mid = (spr / mid * 10000.0) if (spr == spr and mid and mid == mid and mid != 0) else float("nan")
        return {
            "bid": bid, "ask": ask, "last": last, "volume": vol,
            "spr": spr, "spr_bps_mid": spr_bps_of_mid
        }

    ig_liq = liq_metrics(CFG["IG_LIQ_TICKER"]) if CFG["IG_LIQ_TICKER"] else None
    hy_liq = liq_metrics(CFG["HY_LIQ_TICKER"]) if CFG["HY_LIQ_TICKER"] else None

    # --------------------------
    # Render
    # --------------------------
    print("\nBond Market Health Check\n")

    print("Rates / Curve")
    print("-------------")
    print(f"UST 2Y:        {fmt(y2)}%   (σ_{CFG['OBS_DAYS']}d ≈ {fmt(y2_stdev_bp,1)} bp)")
    print(f"UST 10Y:       {fmt(y10)}%  (σ_{CFG['OBS_DAYS']}d ≈ {fmt(y10_stdev_bp,1)} bp)")
    print(f"UST 3M:        {fmt(y3m)}%  (σ_{CFG['OBS_DAYS']}d ≈ {fmt(y3m_stdev_bp,1)} bp)")
    print(f"Slope 2s10s:   {fmt(slope_2s10s,1)} bp")
    print(f"Slope 3m10y:   {fmt(slope_3m10y,1)} bp")
    print()

    print("Credit Spreads (OAS)")
    print("--------------------")
    print(f"IG OAS:        {fmt(ig_oas,1)} bp   ({CFG['OBS_DAYS']}d Δ: {fmt(ig_chg_bp,1)} bp)")
    print(f"HY OAS:        {fmt(hy_oas,1)} bp   ({CFG['OBS_DAYS']}d Δ: {fmt(hy_chg_bp,1)} bp)")
    print()

    print("Rates Volatility Proxy")
    print("----------------------")
    print(f"MOVE Index:    {fmt(move,1)}")
    print()

    if ig_liq or hy_liq:
        print("Liquidity Proxies (ETFs)")
        print("------------------------")
        if ig_liq:
            print(f"{CFG['IG_LIQ_TICKER']}: bid {fmt(ig_liq['bid'])}, ask {fmt(ig_liq['ask'])}, "
                  f"spr {fmt(ig_liq['spr'],3)} ({fmt(ig_liq['spr_bps_mid'],1)} bp of mid), vol {fmt(ig_liq['volume'],0)}")
        if hy_liq:
            print(f"{CFG['HY_LIQ_TICKER']}: bid {fmt(hy_liq['bid'])}, ask {fmt(hy_liq['ask'])}, "
                  f"spr {fmt(hy_liq['spr'],3)} ({fmt(hy_liq['spr_bps_mid'],1)} bp of mid), vol {fmt(hy_liq['volume'],0)}")
        print()

    # --------------------------
    # Heuristic Flags
    # --------------------------
    print("Diagnostics / Flags (heuristics)")
    print("--------------------------------")
    flags = []
    if slope_2s10s == slope_2s10s and slope_2s10s < CFG["CURVE_INVERSION_BP"]:
        flags.append(f"2s10s inverted ({fmt(slope_2s10s,1)} bp)")
    if isinstance(ig_chg_bp, float) and ig_chg_bp == ig_chg_bp and ig_chg_bp > CFG["IG_WIDEN_BP"]:
        flags.append(f"IG OAS widened > {CFG['IG_WIDEN_BP']} bp over {CFG['OBS_DAYS']}d ({fmt(ig_chg_bp,1)} bp)")
    if isinstance(hy_chg_bp, float) and hy_chg_bp == hy_chg_bp and hy_chg_bp > CFG["HY_WIDEN_BP"]:
        flags.append(f"HY OAS widened > {CFG['HY_WIDEN_BP']} bp over {CFG['OBS_DAYS']}d ({fmt(hy_chg_bp,1)} bp)")
    if not flags:
        print("No heuristic flags triggered.")
    else:
        for f in flags:
            print(f"- {f}")

# -----------------------------------
# Entry
# -----------------------------------
if __name__ == "__main__":
    try:
        run_bond_market_health_checks()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

