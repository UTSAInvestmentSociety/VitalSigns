#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Futures & Options Markets Health Checks using Bloomberg Desktop API (blpapi)

Fixes:
- Coerce Bloomberg refdata to floats (handle "N.A.", "-", commas)
- Guard all numeric comparisons to avoid str/int errors
"""

import sys
import math
import datetime as dt
import statistics
from collections import defaultdict

import blpapi  # Bloomberg Desktop API

# -----------------------------------
# USER CONFIGURATION
# -----------------------------------
CFG = {
    "UNIVERSE": {
        "WTI": {
            "contracts": ["CLX5 Comdty", "CLZ5 Comdty", "CLF6 Comdty"],
            "spot":      "USOILSP Index",
        },
        "S&P": {
            "contracts": ["ESZ5 Index", "ESH6 Index", "ESM6 Index"],
            "spot":      "SPX Index",
        },
        "Gold": {
            "contracts": ["GCZ5 Comdty", "GCG6 Comdty", "GCM6 Comdty"],
            "spot":      "XAU Curncy",
        },
    },

    "FUT_FIELDS": {
        "LAST": "PX_LAST",
        "BID":  "BID",
        "ASK":  "ASK",
        "VOL":  "PX_VOLUME",
        "OI":   "OPEN_INT",
        "EXP":  "LAST_TRADEABLE_DT",
    },

    "SPOT_FIELD": "PX_LAST",

    "OPT_TENORS": ["1M", "3M"],
    "OPT_FIELDS": {
        "ATM_TEMPLATE":  None,
        "RR25_TEMPLATE": None,
        "BF25_TEMPLATE": None,
        "SURFACE_TICKER": {
            "WTI": None,
            "S&P": None,
            "Gold": None,
        },
    },

    "OPTION_VOLUME_FIELD": "PX_VOLUME",
    "OPTION_UNIVERSE": {
        "WTI": {"calls": [], "puts": []},
        "S&P": {"calls": [], "puts": []},
        "Gold":{"calls": [], "puts": []},
    },

    "LOOKBACK_CAL_DAYS": 35,
    "HEURISTICS": {
        "CONTANGO_FLAG_BP": 50.0,
        "BACKWARD_FLAG_BP": -50.0,
        "WIDE_SPREAD_BPS":  10.0,
        "LOW_OI_THRESHOLD": 1_000,
    }
}

SESSION_HOST = "localhost"
SESSION_PORT = 8194

# -----------------------------------
# Coercion / format helpers
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

def is_num(x):
    return isinstance(x, (int, float)) and x == x  # not NaN

def fmt(x, nd=2):
    return "—" if not is_num(x) else f"{x:.{nd}f}"

def pct(x, nd=2):
    return "—" if not is_num(x) else f"{x*100.0:.{nd}f}%"

def to_bps_of_mid(bid, ask):
    bid = to_number(bid); ask = to_number(ask)
    if is_num(bid) and is_num(ask) and bid > 0 and ask > 0:
        mid = 0.5 * (bid + ask)
        spr = ask - bid
        if mid > 0:
            return (spr / mid) * 10000.0
    return float("nan")

def annualized_roll(front_px, next_px, front_exp, next_exp):
    front_px = to_number(front_px); next_px = to_number(next_px)
    if not (is_num(front_px) and is_num(next_px) and front_px > 0 and next_px > 0):
        return float("nan")
    # handle dates / datetimes / strings
    def _to_date(v):
        if v is None: return None
        if isinstance(v, dt.date): return v
        try:
            return v.date()
        except Exception:
            pass
        for fmt_ in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
            try:
                return dt.datetime.strptime(str(v), fmt_).date()
            except Exception:
                continue
        return None
    d1 = _to_date(front_exp); d2 = _to_date(next_exp)
    if not (is_num(front_px) and is_num(next_px) and d1 and d2):
        return float("nan")
    days = (d2 - d1).days
    if days <= 0:
        return float("nan")
    return ((next_px / front_px) - 1.0) * (365.0 / days)

def parse_bbg_date(val):
    if val is None or val != val:
        return None
    if isinstance(val, dt.date):
        return val
    try:
        return val.date()
    except Exception:
        pass
    for fmt_ in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime(str(val), fmt_).date()
        except Exception:
            continue
    return None

# -----------------------------------
# Bloomberg helpers
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
    svc = session.getService("//blp/refdata")
    req = svc.createRequest("ReferenceDataRequest")
    added = set()
    for t, flist in tickers_fields.items():
        req.getElement("securities").appendValue(t)
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
                        # try float first, else string -> coerce
                        val = None
                        try:
                            val = fd.getElementAsFloat64(f)
                        except Exception:
                            try:
                                val = fd.getElementAsString(f)
                            except Exception:
                                val = None
                        # Coerce numbers and leave dates for EXP to parse separately
                        fdict[f] = val if f == CFG["FUT_FIELDS"]["EXP"] else to_number(val)
            out[sec] = fdict
    return out

# -----------------------------------
# Core Health Checks
# -----------------------------------
def run_futures_options_health_checks():
    tf = {}
    fut_f = CFG["FUT_FIELDS"]
    fut_fields = [fut_f["LAST"], fut_f["BID"], fut_f["ASK"], fut_f["VOL"], fut_f["OI"], fut_f["EXP"]]

    under_cfgs = {}
    for name, meta in CFG["UNIVERSE"].items():
        contracts = meta["contracts"]
        spot = meta.get("spot")
        under_cfgs[name] = {"contracts": contracts, "spot": spot}
        for c in contracts:
            tf[c] = fut_fields
        if spot:
            tf[spot] = [CFG["SPOT_FIELD"]]

        # Option surface fields (if configured)
        opt = CFG["OPT_FIELDS"]
        surf = opt["SURFACE_TICKER"].get(name) if opt["SURFACE_TICKER"] else None
        if any([opt["ATM_TEMPLATE"], opt["RR25_TEMPLATE"], opt["BF25_TEMPLATE"]]):
            target = surf if surf else contracts[0]
            fields = []
            for t in CFG["OPT_TENORS"]:
                if opt["ATM_TEMPLATE"]:
                    fields.append(opt["ATM_TEMPLATE"].format(tenor=t))
                if opt["RR25_TEMPLATE"]:
                    fields.append(opt["RR25_TEMPLATE"].format(tenor=t))
                if opt["BF25_TEMPLATE"]:
                    fields.append(opt["BF25_TEMPLATE"].format(tenor=t))
            if fields:
                tf[target] = tf.get(target, []) + fields

        # Option volumes for PCR
        opt_uni = CFG["OPTION_UNIVERSE"].get(name, {})
        for k in ("calls", "puts"):
            for t in opt_uni.get(k, []):
                tf[t] = [CFG["OPTION_VOLUME_FIELD"]]

    with BloombergSession() as session:
        ref = bbg_reference(session, tf)

    print("\nFutures & Options Market Health Check\n")

    for name, meta in under_cfgs.items():
        cs = meta["contracts"]
        spot_tkr = meta.get("spot")

        # Pull futures contract data (coerced)
        rows = []
        for c in cs:
            d = ref.get(c, {})
            last = to_number(d.get(fut_f["LAST"]))
            bid  = to_number(d.get(fut_f["BID"]))
            ask  = to_number(d.get(fut_f["ASK"]))
            vol  = to_number(d.get(fut_f["VOL"]))
            oi   = to_number(d.get(fut_f["OI"]))
            exp  = parse_bbg_date(d.get(fut_f["EXP"]))
            spr_bps = to_bps_of_mid(bid, ask)
            rows.append({"ticker": c, "last": last, "bid": bid, "ask": ask, "spr_bps": spr_bps,
                         "vol": vol, "oi": oi, "exp": exp})

        # Basis vs spot (optional)
        basis = float("nan")
        if spot_tkr:
            spot_px = to_number(ref.get(spot_tkr, {}).get(CFG["SPOT_FIELD"]))
            if is_num(spot_px) and rows and is_num(rows[0]["last"]) and spot_px != 0:
                basis = (rows[0]["last"] - spot_px) / spot_px  # decimal

        # Front-next roll yield
        roll_ann = float("nan")
        if len(rows) >= 2:
            roll_ann = annualized_roll(rows[0]["last"], rows[1]["last"], rows[0]["exp"], rows[1]["exp"])

        # Curve slope (front to back, % diff)
        curve_slope = float("nan")
        if len(rows) >= 3 and is_num(rows[0]["last"]) and is_num(rows[-1]["last"]) and rows[0]["last"] != 0:
            curve_slope = (rows[-1]["last"]/rows[0]["last"] - 1.0)

        # Liquidity summaries
        total_oi = sum([r["oi"] for r in rows if is_num(r["oi"])])
        total_vol = sum([r["vol"] for r in rows if is_num(r["vol"])])
        median_spr = statistics.median([r["spr_bps"] for r in rows if is_num(r["spr_bps"])]) if rows else float("nan")

        print(f"{name}  [{', '.join(cs)}]")
        print("-" * max(20, len(name)+8))
        print("Contracts (px / spr bps / OI / Vol / Exp)")
        for r in rows:
            exp_str = r["exp"].isoformat() if isinstance(r["exp"], dt.date) else "—"
            print(f"  {r['ticker']:<15} px {fmt(r['last'],4):>8}  spr {fmt(r['spr_bps'],1):>6}  "
                  f"OI {fmt(r['oi'],0):>8}  Vol {fmt(r['vol'],0):>8}  Exp {exp_str}")
        print(f"Front→Next annualized roll:   {pct(roll_ann, 2)}")
        print(f"Curve slope (front→back):     {pct(curve_slope, 2)}")
        if spot_tkr:
            print(f"Basis vs spot ({spot_tkr}):    {pct(basis, 2)}")
        print(f"Liquidity: Total OI {fmt(total_oi,0)}, Total Vol {fmt(total_vol,0)}, Median spr {fmt(median_spr,1)} bps")
        print()

        # ----- Options (optional) -----
        opt = CFG["OPT_FIELDS"]
        have_iv = any([opt["ATM_TEMPLATE"], opt["RR25_TEMPLATE"], opt["BF25_TEMPLATE"]])
        if have_iv:
            surf = opt["SURFACE_TICKER"].get(name) if opt["SURFACE_TICKER"] else None
            target = surf if surf else cs[0]
            td = ref.get(target, {})
            ivs, rrs, bfs = {}, {}, {}
            for t in CFG["OPT_TENORS"]:
                if opt["ATM_TEMPLATE"]:
                    f = opt["ATM_TEMPLATE"].format(tenor=t)
                    ivs[t] = to_number(td.get(f))
                if opt["RR25_TEMPLATE"]:
                    f = opt["RR25_TEMPLATE"].format(tenor=t)
                    rrs[t] = to_number(td.get(f))
                if opt["BF25_TEMPLATE"]:
                    f = opt["BF25_TEMPLATE"].format(tenor=t)
                    bfs[t] = to_number(td.get(f))

            if any(is_num(v) for v in (list(ivs.values())+list(rrs.values())+list(bfs.values()))):
                print("Options on Futures (IV/Skew)")
                for t in CFG["OPT_TENORS"]:
                    iv = ivs.get(t, float("nan"))
                    rr = rrs.get(t, float("nan"))
                    bf = bfs.get(t, float("nan"))
                    iv_str = fmt(iv, 2)
                    rr_str = fmt(rr, 2)
                    bf_str = fmt(bf, 2)
                    print(f"  {t:<3} ATM IV {iv_str:>6}   25Δ RR {rr_str:>6}   25Δ BF {bf_str:>6}")
                print()

        # Put/Call ratio (if option lists provided)
        opt_uni = CFG["OPTION_UNIVERSE"].get(name, {})
        if opt_uni and (opt_uni.get("calls") or opt_uni.get("puts")):
            calls = opt_uni.get("calls", [])
            puts  = opt_uni.get("puts", [])
            c_vol = sum([to_number(ref.get(t, {}).get(CFG["OPTION_VOLUME_FIELD"], 0.0)) for t in calls])
            p_vol = sum([to_number(ref.get(t, {}).get(CFG["OPTION_VOLUME_FIELD"], 0.0)) for t in puts])
            pcr = (p_vol / c_vol) if is_num(c_vol) and c_vol != 0 else float("nan")
            print("Option Activity")
            print(f"  Calls Vol (Σ): {fmt(c_vol,0)}   Puts Vol (Σ): {fmt(p_vol,0)}   Put/Call Ratio: {fmt(pcr,2)}")
            print()

        # ----- Heuristic flags -----
        H = CFG["HEURISTICS"]
        flags = []
        if len(rows) >= 2 and is_num(rows[0]["last"]) and is_num(rows[1]["last"]) and rows[0]["last"] != 0:
            slope_bp = (rows[1]["last"] - rows[0]["last"]) / rows[0]["last"] * 10000.0
            if slope_bp > H["CONTANGO_FLAG_BP"]:
                flags.append(f"Notable contango front→next ({slope_bp:.0f} bp of front)")
            if slope_bp < H["BACKWARD_FLAG_BP"]:
                flags.append(f"Notable backwardation front→next ({slope_bp:.0f} bp of front)")
        low_oi = [r["ticker"] for r in rows if is_num(r["oi"]) and r["oi"] < H["LOW_OI_THRESHOLD"]]
        wide_spr = [r["ticker"] for r in rows if is_num(r["spr_bps"]) and r["spr_bps"] > H["WIDE_SPREAD_BPS"]]
        if low_oi:
            flags.append(f"Low OI: {', '.join(low_oi)}")
        if wide_spr:
            flags.append(f"Wide spreads: {', '.join(wide_spr)}")
        if flags:
            print("Diagnostics / Flags")
            for f in flags:
                print(f"  - {f}")
            print()

# -----------------------------------
# Entry point
# -----------------------------------
if __name__ == "__main__":
    try:
        run_futures_options_health_checks()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
