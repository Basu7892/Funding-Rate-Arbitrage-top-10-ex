"""
================================================================================
 FUNDING RATE ARBITRAGE SCANNER  (ccxt version - for GitHub / PC / server)
================================================================================
Fetches HISTORICAL FUNDING RATES for ALL altcoin perpetual contracts
(stablecoins and BTC excluded - see ALTCOIN-ONLY note below) across:

    Binance, Bybit, OKX, Hyperliquid, MEXC, Bitget, Gate.io,
    Deribit, KuCoin, BingX

...over the last 180 days, using ccxt's unified API. All 10 exchanges are
fetched IN PARALLEL (separate threads). If a coin fails, it is retried
(with backoff) on the SAME coin before moving on. Produces an Excel report
with the funding-rate spread (arbitrage gap) between the highest- and
lowest-paying exchange for every coin/hour, highlighted by spread size.

--------------------------------------------------------------------------
SETUP
--------------------------------------------------------------------------
    pip install -r requirements.txt

RUN:
    python funding_arbitrage_scanner.py test      # BTC/ETH-USDT only, quick check
    python funding_arbitrage_scanner.py fetch     # full fetch, resumable
    python funding_arbitrage_scanner.py report    # build Excel from CSV so far
    python funding_arbitrage_scanner.py all       # fetch + report

Safe to stop (Ctrl+C) any time - `funding_checkpoint.json` remembers which
(exchange, symbol) pairs are already done, so re-running "fetch" resumes.

Files created next to this script:
    funding_checkpoint.json        - resume state
    funding_raw_all.csv            - all funding-rate rows collected
    funding_errors.log             - symbols that failed even after retries
    funding_arbitrage_report.xlsx  - final report
================================================================================
"""

import os
import sys
import json
import time
import csv
import threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor

import ccxt

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------
DAYS_BACK = 180
NOW_MS = int(time.time() * 1000)
START_MS = NOW_MS - DAYS_BACK * 24 * 60 * 60 * 1000
IST = timezone(timedelta(hours=5, minutes=30))

# ALTCOIN-ONLY FILTER:
#   - Stablecoins are always excluded (they have no meaningful funding arb).
#   - BTC is excluded too because you asked for "altcoin only". If you also
#     want BTC included, just remove "BTC" from EXCLUDE_COINS below.
STABLECOINS = {
    "USDT", "USDC", "DAI", "BUSD", "TUSD", "USDD", "USDE", "FDUSD", "PYUSD",
    "GUSD", "USDP", "EURT", "EUR", "USTC", "UST", "FRAX", "LUSD", "SUSD",
    "USD1", "XUSD",
}
EXCLUDE_COINS = STABLECOINS | {"BTC"}

MAX_RETRIES = 5          # per-symbol retries before giving up on that symbol
RETRY_BASE_DELAY = 3      # seconds, multiplied by attempt number (backoff)
PAGE_LIMIT = 500          # rows per ccxt fetchFundingRateHistory call

HERE = os.path.dirname(os.path.abspath(__file__))
CHECKPOINT_FILE = os.path.join(HERE, "funding_checkpoint.json")
RAW_CSV_FILE = os.path.join(HERE, "funding_raw_all.csv")
ERROR_LOG_FILE = os.path.join(HERE, "funding_errors.log")
REPORT_FILE = os.path.join(HERE, "funding_arbitrage_report.xlsx")

CSV_FIELDS = ["exchange", "symbol", "coin", "funding_time_utc",
              "funding_time_ist", "funding_rate_pct"]

csv_lock = threading.Lock()
checkpoint_lock = threading.Lock()
log_lock = threading.Lock()

# ccxt exchange ids used for each display name
EXCHANGE_IDS = {
    "Binance": "binanceusdm",
    "Bybit": "bybit",
    "OKX": "okx",
    "Hyperliquid": "hyperliquid",
    "MEXC": "mexc",
    "Bitget": "bitget",
    "Gate.io": "gateio",
    "Deribit": "deribit",
    "KuCoin": "kucoinfutures",
    "BingX": "bingx",
}


def log_error(msg):
    print("  [ERROR] " + msg)
    with log_lock:
        with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().isoformat()}  {msg}\n")


def to_ist_str(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M:%S")


def to_utc_str(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default


def save_json_locked(path, data, lock):
    with lock:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f)


def append_rows_csv(rows):
    if not rows:
        return
    with csv_lock:
        file_exists = os.path.exists(RAW_CSV_FILE)
        with open(RAW_CSV_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            if not file_exists:
                w.writeheader()
            for r in rows:
                w.writerow(r)


def base_coin_of(market):
    """Return the base coin symbol for a ccxt market dict, e.g. 'BTC'."""
    base = market.get("base", "")
    return base.upper()


def get_perp_markets(exchange):
    """Return list of (unified_symbol, base_coin) for altcoin perpetuals."""
    markets = exchange.load_markets()
    out = []
    for sym, m in markets.items():
        if not m.get("swap"):
            continue
        if m.get("expiry"):  # skip dated futures, keep perpetual only
            continue
        base = base_coin_of(m)
        if base in EXCLUDE_COINS:
            continue
        quote = (m.get("quote") or "").upper()
        if quote not in ("USDT", "USDC", "USD"):
            continue
        out.append((sym, base))
    return out


def fetch_symbol_history_with_retry(exchange, ex_name, symbol, coin):
    """Fetch full funding history for one symbol, retrying the SAME symbol
    on failure (bounded) instead of skipping straight to the next coin."""
    all_rows = []
    since = START_MS
    attempt = 0
    while since < NOW_MS:
        try:
            batch = exchange.fetch_funding_rate_history(symbol, since=since, limit=PAGE_LIMIT)
            attempt = 0  # reset retry counter after a successful call
            if not batch:
                break
            for entry in batch:
                t = entry.get("timestamp")
                rate = entry.get("fundingRate")
                if t is None or rate is None:
                    continue
                if t < START_MS or t > NOW_MS:
                    continue
                all_rows.append({
                    "exchange": ex_name,
                    "symbol": symbol,
                    "coin": coin,
                    "funding_time_utc": to_utc_str(t),
                    "funding_time_ist": to_ist_str(t),
                    "funding_rate_pct": round(rate * 100, 6),
                })
            newest = max(e["timestamp"] for e in batch if e.get("timestamp"))
            if newest <= since or len(batch) < PAGE_LIMIT:
                break
            since = newest + 1
            time.sleep(exchange.rateLimit / 1000)
        except Exception as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                log_error(f"{ex_name} {symbol}: gave up after {MAX_RETRIES} retries ({e})")
                break
            wait = RETRY_BASE_DELAY * attempt
            print(f"  [retry {attempt}/{MAX_RETRIES}] {ex_name} {symbol} failed ({e}); retrying in {wait}s")
            time.sleep(wait)
            # loop again WITHOUT advancing `since` -> retries same window/coin
    return all_rows


def run_exchange(ex_name, checkpoint, test_mode):
    print(f"\n=== {ex_name} starting ===")
    try:
        klass = getattr(ccxt, EXCHANGE_IDS[ex_name])
        exchange = klass({"enableRateLimit": True})
    except Exception as e:
        log_error(f"{ex_name}: could not initialize ccxt exchange ({e})")
        return

    try:
        markets = get_perp_markets(exchange)
    except Exception as e:
        log_error(f"{ex_name}: could not load markets ({e})")
        return

    if test_mode:
        markets = [(s, c) for s, c in markets if c == "ETH"][:1]

    print(f"  {ex_name}: {len(markets)} altcoin perpetual symbols")

    for i, (symbol, coin) in enumerate(markets, 1):
        key = f"{ex_name}:{symbol}"
        if not test_mode and checkpoint.get(key) == "done":
            continue
        rows = fetch_symbol_history_with_retry(exchange, ex_name, symbol, coin)
        append_rows_csv(rows)
        print(f"  [{ex_name} {i}/{len(markets)}] {symbol}: {len(rows)} records")
        if not test_mode:
            checkpoint[key] = "done"
            save_json_locked(CHECKPOINT_FILE, checkpoint, checkpoint_lock)

    print(f"=== {ex_name} finished ===")


def fetch_all(test_mode=False):
    checkpoint = load_json(CHECKPOINT_FILE, {})
    # All exchanges run AT ONCE, each in its own thread.
    with ThreadPoolExecutor(max_workers=len(EXCHANGE_IDS)) as pool:
        futures = [pool.submit(run_exchange, name, checkpoint, test_mode)
                   for name in EXCHANGE_IDS]
        for f in futures:
            f.result()
    print("\nAll exchanges done. Raw data:", RAW_CSV_FILE)


# --------------------------------------------------------------------------
# REPORT (Excel, with highlighted top spreads)
# --------------------------------------------------------------------------
def build_report():
    import pandas as pd
    from openpyxl.formatting.rule import ColorScaleRule
    from openpyxl.styles import Font, PatternFill

    if not os.path.exists(RAW_CSV_FILE):
        print("No raw data found yet. Run 'fetch' first.")
        return

    df = pd.read_csv(RAW_CSV_FILE)
    if df.empty:
        print("Raw data file is empty.")
        return

    df["funding_time_utc_dt"] = pd.to_datetime(df["funding_time_utc"])
    df["window_utc"] = df["funding_time_utc_dt"].dt.round("h")
    df["window_ist"] = (df["window_utc"] + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%Y-%m-%d %H:%M")

    opp_rows = []
    for (coin, window), g in df.groupby(["coin", "window_utc"]):
        g = g.drop_duplicates(subset=["exchange"])
        if g["exchange"].nunique() < 2:
            continue
        g_sorted = g.sort_values("funding_rate_pct")
        low, high = g_sorted.iloc[0], g_sorted.iloc[-1]
        spread = round(high["funding_rate_pct"] - low["funding_rate_pct"], 6)
        if spread <= 0:
            continue
        all_rates = "; ".join(f"{r.exchange}={r.funding_rate_pct}%" for r in g_sorted.itertuples())
        opp_rows.append({
            "DateTime_IST": high["window_ist"],
            "Coin": coin,
            "Spread_%": spread,
            "Exchange_High": high["exchange"],
            "Rate_High_%": high["funding_rate_pct"],
            "Exchange_Low": low["exchange"],
            "Rate_Low_%": low["funding_rate_pct"],
            "Num_Exchanges": g["exchange"].nunique(),
            "All_Exchange_Rates": all_rates,
        })

    opp_df = pd.DataFrame(opp_rows).sort_values("Spread_%", ascending=False).reset_index(drop=True)
    top_df = opp_df.head(50)

    with pd.ExcelWriter(REPORT_FILE, engine="openpyxl") as writer:
        top_df.to_excel(writer, sheet_name="Top_50_Opportunities", index=False)
        opp_df.to_excel(writer, sheet_name="Arbitrage_Opportunities", index=False)
        df.drop(columns=["funding_time_utc_dt", "window_utc", "window_ist"]).to_excel(
            writer, sheet_name="Raw_Data", index=False)

        wb = writer.book
        header_font = Font(bold=True)
        highlight_fill = PatternFill(start_color="FFF2A65A", end_color="FFF2A65A", fill_type="solid")

        for sheet_name in ("Top_50_Opportunities", "Arbitrage_Opportunities"):
            ws = wb[sheet_name]
            for cell in ws[1]:
                cell.font = header_font
            ws.freeze_panes = "A2"
            last_row = ws.max_row
            if last_row > 1:
                # Spread_% is column C in both sheets
                rng = f"C2:C{last_row}"
                ws.conditional_formatting.add(
                    rng,
                    ColorScaleRule(
                        start_type="min", start_color="FFFFFFFF",
                        mid_type="percentile", mid_value=50, mid_color="FFFFEB84",
                        end_type="max", end_color="FFFF5B5B",
                    ),
                )
                # bold-highlight the Exchange_High / Exchange_Low columns (D, F)
                for row in range(2, last_row + 1):
                    ws.cell(row=row, column=4).fill = highlight_fill  # Exchange_High
                    ws.cell(row=row, column=6).fill = highlight_fill  # Exchange_Low

    print(f"Report written: {REPORT_FILE}")
    print(f"  {len(opp_df)} arbitrage windows found across {df['coin'].nunique()} altcoins")


# --------------------------------------------------------------------------
if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode == "test":
        fetch_all(test_mode=True)
    elif mode == "fetch":
        fetch_all(test_mode=False)
    elif mode == "report":
        build_report()
    elif mode == "all":
        fetch_all(test_mode=False)
        build_report()
    else:
        print("Usage: python funding_arbitrage_scanner.py [test|fetch|report|all]")
