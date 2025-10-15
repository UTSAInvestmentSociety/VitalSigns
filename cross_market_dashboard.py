#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cross-Market Diagnostic Meta-Dashboard

Runs any combination of:
  - FX
  - Money
  - Bonds
  - Equities
  - FuturesOptions

Captures each module's console output, extracts "Diagnostics / Flags"
sections (when available), and prints a unified report in text or JSON.

Usage examples:
  python cross_market_dashboard.py --all
  python cross_market_dashboard.py --markets FX Bonds --format json
  python cross_market_dashboard.py --markets Money Equities --quiet-on-success

Exit codes:
  0 = success, no flags detected
  1 = ran but at least one market raised flags
  2 = configuration or import error (missing module/function)
"""

import argparse
import importlib
import io
import json
import sys
import textwrap
from contextlib import redirect_stdout
from datetime import datetime

# ------------ Config: module names and callable entrypoints ------------
MARKETS = {
    "FX": {
        "module": "fx_health_check",
        "callable": "run_fx_health_checks",
        "title": "Foreign Exchange (FX)"
    },
    "Money": {
        "module": "money_markets_health_check",
        "callable": "run_money_market_health_checks",
        "title": "Money Markets"
    },
    "Bonds": {
        "module": "bond_markets_health_check",
        "callable": "run_bond_market_health_checks",
        "title": "Bond Markets"
    },
    "Equities": {
        "module": "equity_markets_health_check",
        "callable": "run_equity_market_health_checks",
        "title": "Equity Markets"
    },
    "FuturesOptions": {
        "module": "futures_options_health_check",
        "callable": "run_futures_options_health_checks",
        "title": "Futures & Options"
    },
}

# ------------ Helpers ------------
def import_runner(module_name: str, callable_name: str):
    try:
        mod = importlib.import_module(module_name)
    except Exception as e:
        raise ImportError(f"Could not import module '{module_name}': {e}") from e
    fn = getattr(mod, callable_name, None)
    if fn is None or not callable(fn):
        raise ImportError(f"Module '{module_name}' does not expose callable '{callable_name}()'.")
    return fn

def capture_stdout(fn):
    buf = io.StringIO()
    try:
        with redirect_stdout(buf):
            fn()
    except SystemExit as se:
        # If the underlying script exits, still capture what we got
        pass
    except Exception as e:
        # return the exception string in the output so the user can see the cause
        print(f"ERROR during execution: {e}", file=sys.stderr)
        print(f"ERROR during execution: {e}", file=buf)
    return buf.getvalue()

def extract_flags(report_text: str):
    """
    Pull out 'Diagnostics / Flags' block if present.
    Returns a list of lines (without leading bullets) deemed as flags.
    """
    lines = report_text.splitlines()
    flags = []
    in_block = False
    for ln in lines:
        s = ln.strip()
        # enter block when a line starts with 'Diagnostics' (case-insensitive)
        if s.lower().startswith("diagnostics"):
            in_block = True
            continue
        if in_block:
            # blank line ends the block (simple heuristic)
            if s == "":
                in_block = False
                continue
            # capture bullet lines or plain lines that look like flag statements
            if s.startswith("- "):
                flags.append(s[2:].strip())
            else:
                flags.append(s)
    # Remove boilerplate line if present
    flags = [f for f in flags if "No heuristic flags" not in f]
    return flags

def print_text_summary(results, quiet_on_success=False):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    print(f"\n=== Cross-Market Diagnostic Meta-Dashboard ===")
    print(f"Timestamp (UTC): {ts}")
    print("")

    any_flags = False
    for name, res in results.items():
        title = MARKETS[name]["title"]
        status = "FLAGS" if res["flags"] else "OK"
        if res["error"]:
            status = "ERROR"
        print(f"[{title}]  Status: {status}")
        if res["error"]:
            print(textwrap.indent(f"Error: {res['error']}", prefix="  "))
        if res["flags"]:
            any_flags = True
            print("  Flags:")
            for f in res["flags"]:
                print(textwrap.indent(f"- {f}", prefix="    "))
        if not quiet_on_success or res["flags"] or res["error"]:
            print("  --- Report ---")
            # indent the (possibly long) report a bit for readability
            rep = res["report"].rstrip()
            if rep:
                print(textwrap.indent(rep, prefix="    "))
            else:
                print("    (no output captured)")
        print("")

    return any_flags

def main():
    parser = argparse.ArgumentParser(description="Run cross-market diagnostic dashboard.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--all", action="store_true", help="Run all market modules")
    group.add_argument("--markets", nargs="+", choices=list(MARKETS.keys()),
                       help="Subset of markets to run (choices: %(choices)s)")

    parser.add_argument("--format", choices=["text", "json"], default="text",
                        help="Output format (default: text)")
    parser.add_argument("--quiet-on-success", action="store_true",
                        help="In text mode, collapse full reports when no flags/errors.")
    args = parser.parse_args()

    selection = list(MARKETS.keys()) if args.all else args.markets

    # Load and run
    results = {}
    import_errors = []
    for m in selection:
        module_name = MARKETS[m]["module"]
        callable_name = MARKETS[m]["callable"]
        try:
            runner = import_runner(module_name, callable_name)
        except Exception as e:
            err = f"{e}"
            results[m] = {"report": "", "flags": [], "error": err}
            import_errors.append((m, err))
            continue

        # Capture the printed report of this market's health check
        report = capture_stdout(runner)
        flags = extract_flags(report)
        results[m] = {"report": report, "flags": flags, "error": ""}

    # If any import errors, return config error (2) regardless of format
    if import_errors and args.format == "text":
        print("One or more modules could not be loaded:", file=sys.stderr)
        for m, err in import_errors:
            print(f"  - {MARKETS[m]['title']}: {err}", file=sys.stderr)

    # Emit
    if args.format == "json":
        out = {
            "timestamp_utc": datetime.utcnow().isoformat() + "Z",
            "results": {
                k: {
                    "title": MARKETS[k]["title"],
                    "flags": v["flags"],
                    "error": v["error"],
                    "report": v["report"],
                } for k, v in results.items()
            }
        }
        print(json.dumps(out, indent=2))
        # Exit code: 2 if imports failed, else 1 if any flags, else 0
        if import_errors:
            sys.exit(2)
        any_flags = any(results[m]["flags"] for m in results)
        sys.exit(1 if any_flags else 0)
    else:
        any_flags = print_text_summary(results, quiet_on_success=args.quiet_on_success)
        if import_errors:
            sys.exit(2)
        sys.exit(1 if any_flags else 0)

if __name__ == "__main__":
    main()
