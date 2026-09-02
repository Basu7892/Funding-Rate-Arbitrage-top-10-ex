# Funding Rate Arbitrage Scanner

Fetches historical **funding rates** for altcoin perpetual futures (stablecoins
and BTC excluded) from 10 exchanges, over the **last 200 days**, and builds an
Excel report showing the funding-rate spread between exchanges for every coin
and every funding window (all times shown in **IST**).

## Exchanges covered
Binance, Bybit, KuCoin, MEXC, Kraken, HTX, Gate.Io, Coinbase,
Hyperliquid(DEX), Bitget

## Files in this repo

| File | Purpose |
|---|---|
| `funding_arbitrage_scanner.py` | The scanner itself |
| `requirements.txt` | Python dependencies |
| `.github/workflows/funding-scan.yml` | Runs the scanner automatically in the background on GitHub (no need to keep Codespace/PC open) |
| `funding_checkpoint.json` | Auto-created - tracks which (exchange, symbol) pairs are already fetched, so runs resume instead of restarting |
| `funding_raw_all.csv` | Auto-created - every funding-rate row collected so far |
| `funding_errors.log` | Auto-created - symbols that failed even after retries |
| `funding_arbitrage_report.xlsx` | Auto-created - the final report |

## Running it yourself (PC / Codespace terminal)

```bash
pip install -r requirements.txt

python funding_arbitrage_scanner.py test     # quick check, ETH only
python funding_arbitrage_scanner.py fetch    # full 200-day fetch (resumable, can take hours)
python funding_arbitrage_scanner.py report   # build/rebuild the Excel report
python funding_arbitrage_scanner.py all      # fetch + report in one go
```

You can stop the `fetch` step at any time (Ctrl+C) - running it again picks up
exactly where it left off using `funding_checkpoint.json`.

## Running it automatically on GitHub (background, no Codespace needed)

1. Push all these files to a GitHub repo, keeping
   `.github/workflows/funding-scan.yml` at that exact path.
2. Go to the repo's **Actions** tab → **Funding Arbitrage Scanner** →
   **Run workflow** to trigger it manually, or just wait - it also runs
   automatically every 6 hours on its own.
3. Each run fetches as much as it can within GitHub's 6-hour job limit,
   commits `funding_checkpoint.json` / `funding_raw_all.csv` /
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
