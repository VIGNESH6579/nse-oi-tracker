# Data used for signals, and the quality gate

**Removed (no signal value / not needed):** NSE corporate announcements & news tab,
FII/DII cash flow in the scan, participant-OI in scoring, option-chain history storage.
Gap days (results/news reactions) are handled without a news feed: if the open gaps more than
1x ATR from the previous close, the gate demands stronger volume (`GAP_REL_VOLUME_MIN`).

## Inputs
| Input | Source | Used for |
|---|---|---|
| Price, OI per F&O symbol (every scan) | NSE OI-spurts, else Angel futures (near month; near+next OI within 5 days of expiry) | candidates + rolling window |
| Rolling OI/price window (15/30/60 min, in RAM) | built from every scan | classification "now", persistence, OI strength |
| 5-minute candles (today's IST session) | Angel `getCandleData` | session VWAP, opening range (09:15-09:30), volume pace |
| Daily bars (60 d, F&O only) + ATR14/EMA | NSE bhavcopy | extension guard, trend, avg volume, gap |
| Nifty 5-minute regime | Angel | soft penalty when trading against the index |
| F&O ban list | NSE `fo_secban.csv` (daily) | hard exclude |

## Gate (all must pass for a paper entry; otherwise reasons are listed)
buildup only, not in ban, fresh price, >=10 min OI history, 15-min window agrees with the day signal,
persistence >= 4 scans, price on the right side of VWAP and beyond the opening range, session volume
>= 1.2x expected pace (1.8x on gap days), not extended (<=1.0 ATR from open, <=1.5 ATR from VWAP), ATR available.
The gate **fails closed**: missing data blocks the signal and shows up in `/api/health` -> `data_quality.gate_last_scan`.


## Interpreting quiet scans and paper exits

A scan that returns no published OI candidates is not proof of a broken feed or proof that the market has no opportunities. Inspect `/api/oi-signals` → `scan_diagnostics` for raw row count, parsed directional candidates, publication/stability rejections, the actual OI source, and the reason no quality signal was published. Do not weaken confidence, absolute-OI, stability, candle-freshness, volume, VWAP, or opening-range gates just to force a signal.

For `scalp_chain`, open positions expose explicit option-premium stop/target levels and a freshness status for the latest option quote. A time or hard-flat exit with no current quote is closed as `UNPRICED`; its option exit and P&L are null, not copied from the entry. This system is paper-only and quoted marks do not model real fills or slippage.
