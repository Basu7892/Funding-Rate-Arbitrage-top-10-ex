"""
================================================================================
 FUNDING RATE ARBITRAGE SCANNER  (ccxt version - for GitHub Actions / PC)
================================================================================
WORKFLOW (4 steps):

  1. list    -> for every exchange, list all altcoin perpetual coins
                (stablecoins + BTC excluded). Saved to exchange_coin_lists.json
  2. qualify -> keep only coins that exist on AT LEAST 3 exchanges
                (this is the MIN_EXCHANGES setting below).
                Saved to qualified_coins.json
  3. fetch   -> for each exchange (one at a time, fully, before the next),
                fetch 200 days of funding-rate history but ONLY for the
                qualified coins. Each exchange writes to its OWN file:
                    funding_data_Binance.csv
                    funding_data_Bybit.csv
                    funding_data_KuCoin.csv
                    funding_data_MEXC.csv
                    funding_data_Kraken.csv
                    funding_data_HTX.csv
                    funding_data_Gate_Io.csv
                    funding_data_Coinbase.csv
                    funding_data_Hyperliquid_DEX.csv
                    funding_data_Bitget.csv
  4. report  -> reads ALL funding_data_*.csv files, merges them, and builds
                the Excel report (Funding_Report sheet, high/low bolded).

  "all" runs all 4 steps in order.

--------------------------------------------------------------------------
SETUP
--------------------------------------------------------------------------
    pip install -r requirements.txt

RUN:
    python funding_arbitrage_scanner.py list
    python funding_arbitrage_scanner.py qualify
    python funding_arbitrage_scanner.py fetch
    python funding_arbitrage_scanner.py report
    python funding_arbitrage_scanner.py all        # does all 4 in order
    python funding_arbitrage_scanner.py test       # quick ETH-only connectivity check

Safe to stop (Ctrl+C) any time during "fetch" - `funding_checkpoint.json`
remembers which (exchange, symbol) pairs are already done, so re-running
"fetch" resumes instead of restarting.
================================================================================
"""

import os
import re
import sys
import json
import time
import csv
import glob
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import ccxt

# --------------------------------------------------------------------------
# CONFIG
# --------------------------------------------------------------------------
DAYS_BACK = 200
MIN_EXCHANGES = 3       # a coin must exist on at least this many exchanges
NOW_MS = int(time.time() * 1000)
START_MS = NOW_MS - DAYS_BACK * 24 * 60 * 60 * 1000
IST = timezone(timedelta(hours=5, minutes=30))

STABLECOINS = {
    "USDT", "USDC", "DAI", "BUSD", "TUSD", "USDD", "USDE", "FDUSD", "PYUSD",
    "GUSD", "USDP", "EURT", "EUR", "USTC", "UST", "FRAX", "LUSD", "SUSD",
    "USD1", "XUSD",
}
EXCLUDE_COINS = STABLECOINS | {"BTC"}

MAX_RETRIES = 5
RETRY_BASE_DELAY = 3
PAGE_LIMIT = 500

HERE = os.path.dirname(os.path.abspath(__file__))
SYMBOL_LIST_FILE = os.path.join(HERE, "exchange_coin_lists.json")
QUALIFIED_FILE = os.path.join(HERE, "qualified_coins.json")
CHECKPOINT_FILE = os.path.join(HERE, "funding_checkpoint.json")
ERROR_LOG_FILE = os.path.join(HERE, "funding_errors.log")
REPORT_FILE = os.path.join(HERE, "funding_arbitrage_report.xlsx")

CSV_FIELDS = ["exchange", "symbol", "coin", "funding_time_utc",
              "funding_time_ist", "funding_rate_pct"]

csv_lock = threading.Lock()
checkpoint_lock = threading.Lock()
log_lock = threading.Lock()

EXCHANGE_IDS = {
    "Binance": "binanceusdm",
    "Bybit": "bybit",
    "KuCoin": "kucoinfutures",
    "MEXC": "mexc",
    "Kraken": "krakenfutures",
    "HTX": "htx",
    "Gate.Io": "gate",
    "Coinbase": "coinbaseinternational",
    "Hyperliquid(DEX)": "hyperliquid",
    "Bitget": "bitget",
}
EXCHANGE_ORDER = list(EXCHANGE_IDS.keys())


def safe_name(ex_name):
    """Turn an exchange display name into a filesystem-safe file name."""
    return re.sub(r"[^A-Za-z0-9]+", "_", ex_name).strip("_")


def data_file_for(ex_name):
    return os.path.join(HERE, f"funding_data_{safe_name(ex_name)}.csv")


def log_error(msg):
    print("  [ERROR] " + msg)
    with log_lock:
        with open(ERROR_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now().isoformat()}  {msg}\n")


def to_ist_str(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(IST).strftime("%Y-%m-%d %H:%M")


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


def checkpoint_get(checkpoint, key):
    with checkpoint_lock:
        return checkpoint.get(key)


def checkpoint_mark_done(checkpoint, key):
    with checkpoint_lock:
        checkpoint[key] = "done"
        snapshot = dict(checkpoint)
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(snapshot, f)


def append_rows_csv(path, rows):
    if not rows:
        return
    with csv_lock:
        file_exists = os.path.exists(path)
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            if not file_exists:
                w.writeheader()
            for r in rows:
                w.writerow(r)


def base_coin_of(market):
    return (market.get("base") or "").upper()


def get_perp_markets(exchange):
    """Return list of [unified_symbol, base_coin] for altcoin perpetuals."""
    markets = exchange.load_markets()
    out = []
    for sym, m in markets.items():
        if not m.get("swap"):
            continue
        if m.get("expiry"):
            continue
        base = base_coin_of(m)
        if base in EXCLUDE_COINS:
            continue
        quote = (m.get("quote") or "").upper()
        if quote not in ("USDT", "USDC", "USD"):
            continue
        out.append([sym, base])
    return out


# ==========================================================================
# STEP 1: list all coins per exchange
# ==========================================================================
def step_list():
    result = {}
    for ex_name, ccxt_id in EXCHANGE_IDS.items():
        print(f"Listing {ex_name} ...")
        try:
            klass = getattr(ccxt, ccxt_id)
            exchange = klass({"enableRateLimit": True})
            markets = get_perp_markets(exchange)
            result[ex_name] = markets
            coins = sorted({c for _, c in markets})
            print(f"  {ex_name}: {len(markets)} symbols, {len(coins)} unique coins")
        except Exception as e:
            log_error(f"{ex_name}: could not list markets ({e})")
            result[ex_name] = []
    with open(SYMBOL_LIST_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
    print(f"\nSaved: {SYMBOL_LIST_FILE}")


# ==========================================================================
# STEP 2: qualify coins present on >= MIN_EXCHANGES exchanges
# ==========================================================================
def step_qualify():
    cache = load_json(SYMBOL_LIST_FILE, None)
    if cache is None:
        print("exchange_coin_lists.json not found - running 'list' first.")
        step_list()
        cache = load_json(SYMBOL_LIST_FILE, {})

    coin_to_exchanges = defaultdict(set)
    for ex_name, markets in cache.items():
        for _, coin in markets:
            coin_to_exchanges[coin].add(ex_name)

    qualified = sorted([c for c, exs in coin_to_exchanges.items() if len(exs) >= MIN_EXCHANGES])

    with open(QUALIFIED_FILE, "w", encoding="utf-8") as f:
        json.dump(qualified, f, indent=2)

    print(f"{len(qualified)} coins qualify (present on >= {MIN_EXCHANGES} exchanges)")
    print(f"Saved: {QUALIFIED_FILE}")
    # small preview
    for c in qualified[:15]:
        print(f"  {c}: {sorted(coin_to_exchanges[c])}")
    if len(qualified) > 15:
        print(f"  ... and {len(qualified) - 15} more")


# ==========================================================================
# STEP 3: fetch funding history (qualified coins only), one file per exchange
# ==========================================================================
def fetch_symbol_history_with_retry(exchange, ex_name, symbol, coin):
    all_rows = []
    since = START_MS
    attempt = 0
    while since < NOW_MS:
        try:
            batch = exchange.fetch_funding_rate_history(symbol, since=since, limit=PAGE_LIMIT)
            attempt = 0
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
    return all_rows


def run_exchange_fetch(ex_name, qualified_set, checkpoint, test_mode):
    print(f"\n=== {ex_name} starting ===")
    out_file = data_file_for(ex_name)
    try:
        klass = getattr(ccxt, EXCHANGE_IDS[ex_name])
        exchange = klass({"enableRateLimit": True})
        markets = get_perp_markets(exchange)
    except Exception as e:
        log_error(f"{ex_name}: could not initialize/list ({e})")
        return

    markets = [(s, c) for s, c in markets if c in qualified_set]

    if test_mode:
        markets = [(s, c) for s, c in markets if c == "ETH"][:1]

    print(f"  {ex_name}: {len(markets)} qualified symbols -> {out_file}")

    for i, (symbol, coin) in enumerate(markets, 1):
        key = f"{ex_name}:{symbol}"
        if not test_mode and checkpoint_get(checkpoint, key) == "done":
            continue
        rows = fetch_symbol_history_with_retry(exchange, ex_name, symbol, coin)
        append_rows_csv(out_file, rows)
        print(f"  [{ex_name} {i}/{len(markets)}] {symbol}: {len(rows)} records")
        if not test_mode:
            checkpoint_mark_done(checkpoint, key)

    print(f"=== {ex_name} finished ===")


def step_fetch(test_mode=False):
    qualified = load_json(QUALIFIED_FILE, None)
    if qualified is None:
        print("qualified_coins.json not found - running 'list' + 'qualify' first.")
        step_list()
        step_qualify()
        qualified = load_json(QUALIFIED_FILE, [])
    qualified_set = set(qualified)

    checkpoint = load_json(CHECKPOINT_FILE, {})
    # SEQUENTIAL: one exchange fully finishes before the next one starts.
    for ex_name in EXCHANGE_IDS:
        run_exchange_fetch(ex_name, qualified_set, checkpoint, test_mode)

    print("\nAll exchanges done. Per-exchange files: funding_data_<Exchange>.csv")


# ==========================================================================
# STEP 4: report - merge all per-exchange files, build Excel
# ==========================================================================
def step_report():
    import pandas as pd
    from openpyxl.styles import Font, PatternFill

    files = glob.glob(os.path.join(HERE, "funding_data_*.csv"))
    if not files:
        print("No funding_data_*.csv files found yet. Run 'fetch' first.")
        return

    dfs = [pd.read_csv(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    if df.empty:
        print("Data files are empty.")
        return

    df["funding_time_utc_dt"] = pd.to_datetime(df["funding_time_utc"])
    df["window_utc"] = df["funding_time_utc_dt"].dt.round("h")
    df["window_ist"] = (df["window_utc"] + pd.Timedelta(hours=5, minutes=30)).dt.strftime("%d-%m-%Y %H:%M")
    df["coin_pair"] = df["coin"] + "USDT"

    pivot = df.pivot_table(index=["coin_pair", "window_ist", "window_utc"],
                            columns="exchange", values="funding_rate_pct", aggfunc="first")

    for ex in EXCHANGE_ORDER:
        if ex not in pivot.columns:
            pivot[ex] = None
    pivot = pivot[EXCHANGE_ORDER]

    pivot["Spread"] = pivot[EXCHANGE_ORDER].max(axis=1, skipna=True) - pivot[EXCHANGE_ORDER].min(axis=1, skipna=True)
    pivot = pivot.reset_index().sort_values(["window_utc", "coin_pair"]).reset_index(drop=True)
    pivot = pivot[pivot["Spread"].notna() & (pivot["Spread"] > 0)]
    pivot.insert(0, "SL No", range(1, len(pivot) + 1))
    pivot = pivot.rename(columns={"coin_pair": "Coin Name", "window_ist": "Date & Time"})
    pivot = pivot.drop(columns=["window_utc"])

    final_cols = ["SL No", "Coin Name", "Date & Time", "Spread"] + EXCHANGE_ORDER
    pivot = pivot[final_cols]

    with pd.ExcelWriter(REPORT_FILE, engine="openpyxl") as writer:
        pivot.to_excel(writer, sheet_name="Funding_Report", index=False)
        df.drop(columns=["funding_time_utc_dt", "window_utc", "coin_pair"]).to_excel(
            writer, sheet_name="Raw_Data", index=False)

        wb = writer.book
        ws = wb["Funding_Report"]
        bold = Font(bold=True)
        header_fill = PatternFill(start_color="FFDCE6F1", end_color="FFDCE6F1", fill_type="solid")
        for cell in ws[1]:
            cell.font = bold
            cell.fill = header_fill
        ws.freeze_panes = "E2"

        ex_col_start = 5
        ex_col_end = ex_col_start + len(EXCHANGE_ORDER) - 1
        for row in range(2, ws.max_row + 1):
            vals = []
            for col in range(ex_col_start, ex_col_end + 1):
                v = ws.cell(row=row, column=col).value
                if v is not None:
                    vals.append((col, v))
            if len(vals) < 2:
                continue
            max_col = max(vals, key=lambda x: x[1])[0]
            min_col = min(vals, key=lambda x: x[1])[0]
            ws.cell(row=row, column=max_col).font = bold
            ws.cell(row=row, column=min_col).font = bold
            ws.cell(row=row, column=4).font = bold

    print(f"Report written: {REPORT_FILE}")
    print(f"  {len(pivot)} rows across {df['coin'].nunique()} qualified altcoins")


# --------------------------------------------------------------------------
if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode == "list":
        step_list()
    elif mode == "qualify":
        step_qualify()
    elif mode == "fetch":
        step_fetch(test_mode=False)
    elif mode == "report":
        step_report()
    elif mode == "test":
        step_fetch(test_mode=True)
    elif mode == "all":
        step_list()
        step_qualify()
        step_fetch(test_mode=False)
        step_report()
    else:
        print("Usage: python funding_arbitrage_scanner.py [list|qualify|fetch|report|test|all]")
