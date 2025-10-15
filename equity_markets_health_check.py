#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Equity Markets Health Checks using Bloomberg Desktop API (blpapi)

Fixes:
- Coerce Bloomberg values (incl. strings like "N.A.") to floats
- Guard all comparisons/multiplications with is_num()
"""

import sys
import math
import datetime as dt
import statistics
from collections import defaultdict

import blpapi  # Bloomberg Desktop API

# -----------------------------------
# User Configuration (U.S. defaults)
# -----------------------------------
CFG = {
    "INDEX_TICKER":       "SPX Index",
    "INDEX_PX_FIELD":     "PX_LAST",
    "VOL_PROXY_TICKER":   "VIX Index",
    "VOL_PROXY_FIELD":    "PX_LAST",

    "TEN_YR_TICKER":      "USGG10YR Index",
    "TEN_YR_FIELD":       "PX_LAST",       # percent

    "BDS_MEMBERS_FIELD":  "INDX_MEMBERS",

    "MEMBER_FIELDS": {
        "PX":   "PX_LAST",
        "MA200":"MOV_AVG_200D",
        "BID":  "BID",
        "ASK":  "ASK",
        "VOL":  "VOLUME",
        "SHARES_OUT": "CUR_MKT_CAP_SHARES_OUT",
        "MKT_CAP":     "CUR_MKT_CAP"
    },

    "INDEX_FWD_PE_FIELD": "FWD_PX_TO_EPS",
    "MEMBER_FWD_PE_FIELD":"FWD_PX_TO_EPS",

    "LOOKBACK_CAL_DAYS": 45,
    "RV_OBS_DAYS": 20,
    "MAX_MEMBERS": 1200,
}

SESSION_HOST = "localhost"
SESSION_PORT = 8194

# -----------------------------------
# Type / Guard Helpers
# -----------------------------------
def to_number(x):
    """Coerce Bloomberg values to float; map common NA tokens to NaN."""
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        s = s_orig = x.strip()
        if s in ("", "N.A.", "NA", "N/A", "—", "-", "NaN"):
            return float("nan")
        try:
            return float(s.replace(",", ""))
        except Exception:
            return float("nan")
    return float("nan")

def is_num(x):
    """True if x is a finite float/int (not NaN)."""
    return isinstance(x, (int, float)) and x == x

def fmt(x, nd=2):
    if not is_num(x):
        return "—"
    return f"{x:.{nd}f}"

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

# Reference (per-ticker list of fields)
def bbg_reference(session, tickers_fields):
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

# Historical (single field)
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

# BDS members (index constituents)
def bbg_bds_members(session, index_ticker, bds_field, max_members=1200):
    svc = session.getService("//blp/refdata")
    req = svc.createRequest("ReferenceDataRequest")
    req.getElement("securities").appendValue(index_ticker)
    req.getElement("fields").appendValue(bds_field)
    msgs = _send_request(session, req)
    members = []
    for msg in msgs:
        if not msg.hasElement("securityData"):
            continue
        for sdata in msg.getElement("securityData").values():
            if not sdata.hasElement("fieldData"):
                continue
            fd = sdata.getElement("fieldData")
            if fd.hasElement(bds_field):
                table = fd.getElement(bds_field)
                for row in table.values():
                    sec = None
                    for col in ("Member Ticker and Exchange Code", "Security", "Member Ticker"):
                        if row.hasElement(col):
                            try:
                                sec = row.getElementAsString(col)
                                break
                            except Exception:
                                pass
                    if sec:
                        members.append(sec)
                    if len(members) >= max_members:
                        return members
    return members

# -----------------------------------
# Math Helpers
# -----------------------------------
def realized_vol_from_prices(prices, obs=20, ann_factor=252):
    xs = [v for v in prices if is_num(v)]
    if len(xs) < obs + 1:
        return float("nan")
    xs = xs[-(obs + 1):]
    rets = []
    for i in range(1, len(xs)):
        if xs[i-1] > 0 and xs[i] > 0:
            rets.append(math.log(xs[i] / xs[i-1]))
    if not rets:
        return float("nan")
    stdev = statistics.pstdev(rets)
    return stdev * math.sqrt(ann_factor)

# -----------------------------------
# Health Checks
# -----------------------------------
def run_equity_market_health_checks():
    today = dt.date.today()
    start = today - dt.timedelta(days=CFG["LOOKBACK_CAL_DAYS"])
    end = today

    with BloombergSession() as session:
        members = bbg_bds_members(session, CFG["INDEX_TICKER"], CFG["BDS_MEMBERS_FIELD"], CFG["MAX_MEMBERS"])
        # Append Bloomberg yellow-key suffix " Equity" to each member
        members = [m.strip() + " Equity" for m in members]
        # Member snapshots
        m_fields = CFG["MEMBER_FIELDS"]
        tickers_fields = {}
        for m in members:
            tickers_fields[m] = [
                m_fields["PX"], m_fields["MA200"], m_fields["BID"], m_fields["ASK"], m_fields["VOL"],
                m_fields["SHARES_OUT"], m_fields["MKT_CAP"], CFG["MEMBER_FWD_PE_FIELD"]
            ]
        # Index + proxies
        tickers_fields[CFG["INDEX_TICKER"]] = [CFG["INDEX_PX_FIELD"], CFG["INDEX_FWD_PE_FIELD"]]
        if CFG["VOL_PROXY_TICKER"]:
            tickers_fields[CFG["VOL_PROXY_TICKER"]] = [CFG["VOL_PROXY_FIELD"]]
        if CFG["TEN_YR_TICKER"]:
            tickers_fields[CFG["TEN_YR_TICKER"]] = [CFG["TEN_YR_FIELD"]]

        ref = bbg_reference(session, tickers_fields)
        hist = bbg_history(session, [CFG["INDEX_TICKER"]], CFG["INDEX_PX_FIELD"], start, end)

    # ---------- Breadth / Liquidity / Valuation prep ----------
    above_200 = 0
    valid_ma = 0
    spreads_bps = []
    dollar_volumes = []
    weights = []
    member_fwd_pe = []
    m_fields = CFG["MEMBER_FIELDS"]

    for m in members:
        md = ref.get(m, {})

        px   = to_number(md.get(m_fields["PX"]))
        ma   = to_number(md.get(m_fields["MA200"]))
        bid  = to_number(md.get(m_fields["BID"]))
        ask  = to_number(md.get(m_fields["ASK"]))
        vol  = to_number(md.get(m_fields["VOL"]))
        shs  = to_number(md.get(m_fields["SHARES_OUT"]))
        mcap = to_number(md.get(m_fields["MKT_CAP"]))
        fpe  = to_number(md.get(CFG["MEMBER_FWD_PE_FIELD"]))

        # print(f"PX: {px}")
        # print(f"MA: {ma}")

        # Breadth
        if is_num(px) and is_num(ma):
            valid_ma += 1
            if px > ma:
                above_200 += 1

        # Liquidity
        if is_num(bid) and is_num(ask) and bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
            if mid > 0:
                spr = (ask - bid) / mid * 10000.0
                spreads_bps.append(spr)
        if is_num(px) and is_num(vol) and vol >= 0:
            dollar_volumes.append(px * vol)

        # Weights for cap-weighted P/E
        if is_num(mcap) and mcap > 0:
            weights.append(mcap)
            member_fwd_pe.append(fpe if (is_num(fpe) and fpe > 0) else float("nan"))
        elif is_num(shs) and shs > 0 and is_num(px):
            weights.append(shs * px)
            member_fwd_pe.append(fpe if (is_num(fpe) and fpe > 0) else float("nan"))
        else:
            # skip
            pass


    breadth_pct = (above_200 / valid_ma * 100.0) if valid_ma > 0 else float("nan")
    median_spr_bps = statistics.median(spreads_bps) if spreads_bps else float("nan")
    agg_dollar_vol = sum(dollar_volumes) if dollar_volumes else float("nan")

    # ---------- Volatility ----------
    idx_prices = [to_number(v) for (_, v) in hist.get(CFG["INDEX_TICKER"], []) if is_num(to_number(v))]
    rv20 = realized_vol_from_prices(idx_prices, obs=CFG["RV_OBS_DAYS"])  # decimal annualized
    vol_proxy = to_number(ref.get(CFG["VOL_PROXY_TICKER"], {}).get(CFG["VOL_PROXY_FIELD"])) if CFG["VOL_PROXY_TICKER"] else float("nan")

    # ---------- Valuation ----------
    idx_px = to_number(ref.get(CFG["INDEX_TICKER"], {}).get(CFG["INDEX_PX_FIELD"]))
    idx_fwd_pe = to_number(ref.get(CFG["INDEX_TICKER"], {}).get(CFG["INDEX_FWD_PE_FIELD"]))
    capw_fwd_pe = float("nan")
    if weights and member_fwd_pe and len(weights) == len(member_fwd_pe):
        pairs = [(w, pe) for (w, pe) in zip(weights, member_fwd_pe) if is_num(pe) and pe > 0]
        if pairs:
            total_w = sum(w for (w, _) in pairs)
            denom = sum(w/pe for (w, pe) in pairs)
            if denom and denom > 0:
                capw_fwd_pe = total_w / denom

    y10 = to_number(ref.get(CFG["TEN_YR_TICKER"], {}).get(CFG["TEN_YR_FIELD"])) if CFG["TEN_YR_TICKER"] else float("nan")
    erp_bp = float("nan")
    fwd_pe_used = idx_fwd_pe if (is_num(idx_fwd_pe) and idx_fwd_pe > 0) else capw_fwd_pe
    if is_num(fwd_pe_used) and fwd_pe_used > 0 and is_num(y10):
        earnings_yield_pct = 100.0 / fwd_pe_used            # in %
        erp_bp = (earnings_yield_pct - y10) * 100.0         # % points -> bp

    # ---------- Render ----------
    def pct(x, nd=1):
        if not is_num(x):
            return "—"
        return f"{x:.{nd}f}%"

    print("\nEquity Market Health Check\n")

    print("Breadth")
    print("-------")
    print(f"Universe: {len(members)} members (sampled)")
    print(f"% above 200D MA:           {pct(breadth_pct, 1)}")

    print("\nVolatility")
    print("---------")
    # rv20 is decimal; convert to %
    rv20_pct = (rv20 * 100.0) if is_num(rv20) else float("nan")
    print(f"Realized Vol (20D, ann.):  {pct(rv20_pct, 2)}")
    if CFG['VOL_PROXY_TICKER']:
        print(f"Implied Vol Proxy ({CFG['VOL_PROXY_TICKER']}): {fmt(vol_proxy, 2)}")

    print("\nLiquidity")
    print("---------")
    print(f"Aggregate Dollar Volume:   {fmt(agg_dollar_vol/1e9, 2)} Bn")
    print(f"Median Bid–Ask (bps mid):  {fmt(median_spr_bps, 1)}")

    print("\nValuation")
    print("--------")
    if is_num(idx_fwd_pe):
        print(f"Index Forward P/E:         {fmt(idx_fwd_pe, 2)}")
    if is_num(capw_fwd_pe):
        print(f"Cap-weighted Fwd P/E:      {fmt(capw_fwd_pe, 2)}")
    if is_num(erp_bp):
        print(f"Simple ERP vs 10Y:         {fmt(erp_bp, 0)} bp")
    if is_num(y10):
        print(f"UST 10Y Yield:             {fmt(y10, 2)}%")

# -----------------------------------
# Entry
# -----------------------------------
if __name__ == "__main__":
    try:
        run_equity_market_health_checks()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
