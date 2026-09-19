# Signal and analytics logic

## Scanner boundary

The all-F&O scanner consumes the public NSE OI-spurts underlying feed. It
classifies the observable price/OI relationship as long buildup, short
buildup, short covering, or long unwinding. The classification is always an
`OI_PRICE_CANDIDATE` with `NO_TRADE`; it is not an options recommendation.

The confidence value ranks the size of price change, OI change, and absolute
OI for the current scan. It is not a probability of profit and is not used to
turn an observation into BUY CALL, BUY PUT, or BUY BOTH.

## Independent context

Daily NSE bhavcopy bars supply EMA20/EMA50, ATR14, efficiency ratio, relative
volume, a 20-day daily VWAP proxy, and a daily trend/range regime. The proxy
is explicitly not an intraday VWAP. Corporate disclosures are labelled for
possible event volatility, not headline sentiment. Public India VIX is used
for volatility context; the application reports IV, gamma, intraday VWAP, and
opening gap as missing because it does not have a validated live source for
them.

NSE participant-wise OI is ingested only after its end-of-day report is
published. The report date is retained in the API response; it is not combined
with intraday candidates or presented as a real-time participant-position feed.

## Option-chain analytics

PCR is aggregate PE OI divided by CE OI. Max pain is the strike minimizing
aggregate intrinsic settlement loss across the supplied strikes. OI walls,
writing zones, the strike ladder, and the heatmap are descriptive. Their
relative colour intensity is normalised to the current returned chain window;
none of these measures is guaranteed support/resistance or a price forecast.

## Trap labels and outcome summaries

Bull/bear-trap risk requires several daily-context conditions: a break of a
prior 20-day level, non-confirming OI, failure relative to the daily VWAP
proxy, and fading relative daily volume. Missing context returns
`INSUFFICIENT_DATA`; a price/OI mismatch alone cannot be called a trap.

Outcome analytics report stored candidate-event lifecycle outcomes, not
executable options performance. They exclude contract selection, fills,
slippage, brokerage, position sizing, and market impact. P&L points are an
audit calculation over the stored underlying-price fallback levels only.

## Paper-only go-live gates

Signals remain paper-only and are decision support, not advice. Do not trust
strategy results until at least **20 trading days and 100 setups** of paper
data are available. The minimum bar for considering promotion is positive
expectancy after costs, profit factor of at least **1.3**, maximum drawdown no
greater than **8R**, and a monotonic score-bucket calibration table. Tuning is
walk-forward only: fit on the first half of the sample and evaluate on the
second half, never on the full sample.

Intraday grading is expressed in signed **R multiples**. A 15:15 IST time exit
uses that candle's close; if a single candle crosses both stop and target, the
stop is conservatively counted first. No close-vs-entry WIN/LOSS label is a
strategy result.
