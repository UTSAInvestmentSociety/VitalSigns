# VitalSigns

VitalSigns is a collection of Bloomberg-enabled market health-check dashboards designed for
macro and cross-asset risk monitoring. Each module connects to the Bloomberg Desktop API
(`blpapi`) to pull the latest reference and historical data for a specific asset class, then
emits a console report with key diagnostics and heuristics. A coordinating CLI ties the
modules together so that risk teams can run a consolidated cross-market sweep or drill into
a single asset class on demand.

## Prerequisites

All scripts expect to run on a workstation that has access to the Bloomberg Desktop API and
network entitlements for the referenced securities. Before running any script:

- Install the Bloomberg Python SDK (`pip install blpapi`).
- Ensure a Bloomberg Terminal session is active and the Desktop API is enabled.
- Confirm that the configured ticker mnemonics match your local Bloomberg setup (use
  `FLDS <GO>` in the terminal to validate fields when in doubt).

## Repository Layout

| File | Description |
| ---- | ----------- |
| `cross_market_dashboard.py` | Meta-runner that orchestrates one or more health-check modules, consolidates their console output, and extracts common "Diagnostics / Flags" sections for a unified text or JSON report. |
| `fx_health_check.py` | Fetches spot liquidity, realized volatility, implied volatility, and skew metrics for configured FX pairs using dedicated BVOL tickers. |
| `money_markets_health_check.py` | Monitors USD funding stress and liquidity using SOFR, EFFR, commercial paper, ON RRP, and optional OIS/credit curves. Includes historical lookbacks for basis calculations and heuristics. |
| `bond_markets_health_check.py` | Tracks U.S. Treasury yields, credit spreads, MOVE index, and ETF liquidity proxies, computing slope changes and variability with heuristic flagging. |
| `equity_markets_health_check.py` | Evaluates equity index breadth, volatility, and cross-asset overlays (e.g., VIX vs. realized vol) with optional factor/sector heatmaps. |
| `futures_options_health_check.py` | Aggregates futures and listed options signals such as term structure, open interest shifts, and volatility surfaces for key contracts. |

Each module exposes a top-level function (`run_*_health_checks`) that performs the API
calls, formats the console report, and is callable from the cross-market dashboard. Running
a module directly executes the corresponding function and exits with a non-zero status if an
error occurs.

## Running the Dashboards

### Cross-market meta dashboard

The meta dashboard coordinates any combination of asset-class modules and standardizes their
output. Example invocations:

```bash
# Run the full suite and view text output (default)
python cross_market_dashboard.py --all

# Run only FX and Bonds, returning JSON for machine parsing
python cross_market_dashboard.py --markets FX Bonds --format json

# Suppress full reports when a module has no flags or errors
python cross_market_dashboard.py --all --quiet-on-success
```

Exit codes follow a convention that downstream automation can rely on:

- `0`: success, no flags were detected.
- `1`: modules ran but at least one raised heuristic flags.
- `2`: configuration or import error (e.g., missing module or callable).

### Individual modules

Each asset-class script can be run in isolation when deeper investigation is required. The
modules share a common structure:

- Configuration section at the top that lists tickers, fields, thresholds, and lookback
  windows. Update these values to fit your coverage universe.
- Bloomberg session helpers that manage connections to `//blp/refdata` and wrap common
  Reference and Historical Data requests.
- Reporting code that translates raw Bloomberg data into human-readable tables and flag
  summaries.

Invoke a module directly to print its report, for example:

```bash
python fx_health_check.py
python money_markets_health_check.py
```

Modules emit their own diagnostic sections where appropriate. When these sections include a
"Diagnostics / Flags" header followed by bullet points, the cross-market dashboard will
collect and aggregate them.

## Customization and Extension

The scripts are designed to be easily extended:

- Adjust ticker lists, tenors, and thresholds in the configuration dictionaries at the top of
  each module to reflect your institution's benchmarks.
- Expand the `MARKETS` mapping in `cross_market_dashboard.py` to plug in new modules. Each
  entry specifies the importable module name, callable function, and display title.
- Leverage the helper utilities (e.g., `build_universe_and_fields`, `bbg_reference`,
  `bbg_history`) when adding new data pulls to ensure consistent error handling and
  normalisation of Bloomberg field formats.

Because each module prints plain-text output, the meta dashboard can continue to parse it as
long as new diagnostics conform to the established structure.

## Error Handling

All modules attempt to degrade gracefully:

- Bloomberg connection failures raise descriptive runtime errors that surface in the console
  and bubble up to the cross-market dashboard.
- Reference and historical data calls coerce common Bloomberg "N.A." tokens to `NaN` and
  avoid crashing when a field is missing.
- The cross-market dashboard captures stdout from each module, stores any import exceptions,
  and summarises errors alongside successful reports. It propagates a non-zero exit code when
  imports fail so automated monitors can alert on missing dependencies.

## Contributing

When enhancing the health checks, keep diagnostics human-readable and include a dedicated
"Diagnostics / Flags" section with bullet points describing potential issues. This makes it
straightforward for the cross-market dashboard to highlight actionable items without parsing
free-form prose.
