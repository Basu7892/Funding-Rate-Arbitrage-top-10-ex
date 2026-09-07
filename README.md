# Funding Rate Arbitrage Scanner

Fetches historical **funding rates** for altcoin perpetual futures (stablecoins
and BTC excluded) from 10 exchanges, over the **last 200 days** - but only for
coins that exist on **at least 3 of those exchanges** (so arbitrage between
exchanges is actually possible). Each exchange's data is saved to its own
file, and a combined Excel report shows the funding-rate spread between
exchanges for every coin and every funding window (all times in **IST**).

## Exchanges covered
Binance, Bybit, KuCoin, MEXC, Kraken, HTX, Gate.Io, Coinbase,
Hyperliquid(DEX), Bitget

## How it works (4 steps)

1. **list** - lists every altcoin perpetual coin available on each exchange
   → `exchange_coin_lists.json`
2. **qualify** - keeps only coins present on 3+ exchanges
   → `qualified_coins.json`
3. **fetch** - fetches 200 days of funding history for qualified coins only,
   one exchange at a time (fully finishes one before starting the next),
   writing each exchange to its **own file**:
   `funding_data_Binance.csv`, `funding_data_Bybit.csv`,
   `funding_data_KuCoin.csv`, `funding_data_MEXC.csv`,
   `funding_data_Kraken.csv`, `funding_data_HTX.csv`,
   `funding_data_Gate_Io.csv`, `funding_data_Coinbase.csv`,
   `funding_data_Hyperliquid_DEX.csv`, `funding_data_Bitget.csv`
4. **report** - merges all of the above and builds the Excel report

## Files in this repo

| File | Purpose |
|---|---|
| `funding_arbitrage_scanner.py` | The scanner itself |
| `requirements.txt` | Python dependencies |
| `.github/workflows/funding-scan.yml` | Runs the scanner automatically in the background on GitHub (no need to keep Codespace/PC open) |
| `exchange_coin_lists.json` | Auto-created (step 1) - every coin found per exchange |
| `qualified_coins.json` | Auto-created (step 2) - coins present on 3+ exchanges |
| `funding_data_<Exchange>.csv` | Auto-created (step 3) - one file per exchange |
| `funding_checkpoint.json` | Auto-created - tracks which (exchange, symbol) pairs are already fetched, so runs resume instead of restarting |
| `funding_errors.log` | Auto-created - symbols that failed even after retries |
| `funding_arbitrage_report.xlsx` | Auto-created (step 4) - the final report |

## Running it yourself (PC / Codespace terminal)

```bash
pip install -r requirements.txt

python funding_arbitrage_scanner.py test      # quick check, ETH only
python funding_arbitrage_scanner.py list      # step 1
python funding_arbitrage_scanner.py qualify   # step 2
python funding_arbitrage_scanner.py fetch     # step 3 (resumable, can take hours)
python funding_arbitrage_scanner.py report    # step 4
python funding_arbitrage_scanner.py all       # all 4 steps in order
```

You can stop the `fetch` step at any time (Ctrl+C) - running it again picks up
exactly where it left off using `funding_checkpoint.json`. If you run `fetch`
or `report` without having run `list`/`qualify` first, the script runs them
for you automatically.

## Running it automatically on GitHub (background, no Codespace needed)

1. Push all these files to a GitHub repo, keeping
   `.github/workflows/funding-scan.yml` at that exact path.
2. Go to the repo's **Actions** tab → **Funding Arbitrage Scanner** →
   **Run workflow** to trigger it manually, or just wait - it also runs
   automatically every 6 hours on its own.
3. Each run fetches as much as it can within GitHub's 6-hour job limit,
   commits `exchange_coin_lists.json` / `qualified_coins.json` /
   `funding_checkpoint.json` / `funding_data_*.csv` /
   `funding_arbitrage_report.xlsx` back into the repo, and also uploads the
   `.xlsx` report as a downloadable **Artifact** on that run's page.
4. Because progress is committed back to the repo, a full 200-day / all-coin
   / all-exchange fetch simply continues across multiple scheduled runs -
   nothing needs to be babysat.

## Report format (`Funding_Report` sheet)

| SL No | Coin Name | Date & Time | Spread | Binance | Bybit | KuCoin | MEXC | Kraken | HTX | Gate.Io | Coinbase | Hyperliquid(DEX) | Bitget |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|

- One row per coin per funding timestamp (IST).
- `Spread` = highest exchange rate − lowest exchange rate for that row.
- The **highest** and **lowest** rate in each row are shown in **bold** so
  the two exchanges worth arbitraging are easy to spot at a glance.
- A `Raw_Data` sheet with every individual funding-rate record is included
  as a second sheet for reference.

## Notes / known limitations

- **Coinbase**: perpetuals only exist on *Coinbase International Exchange*
  (not the regular retail Coinbase app). The script uses ccxt's
  `coinbaseinternational` id for this - if ccxt's support for it changes,
  check `funding_errors.log`.
- If any exchange/coin fails, the script retries **the same coin** up to 5
  times with backoff before giving up and logging it - it does not skip
  straight to the next coin on the first failure.
- Only **altcoins** are included - stablecoins (USDT, USDC, DAI, etc.) and
  **BTC** are excluded on purpose. To include BTC, remove `"BTC"` from
  `EXCLUDE_COINS` near the top of `funding_arbitrage_scanner.py`.
