# Data Sources

This project now uses a mixed source stack based on the panel type.

## Source Map

| Panel | Primary Source | Fallbacks | Notes |
| --- | --- | --- | --- |
| US equity and ETF daily bars | Massive aggregates API | Yahoo chart API, Stooq CSV, local cache | Used for SPY, IWM, HYG, LQD, XHB, ITB, IYT, XRT, SMH, KRE, XLY, XLP when `MASSIVE_API_KEY` is set. |
| Macro liquidity series | FRED CSV | local cache not used | Uses WALCL, RRPONTSYD, WTREGEN, M2SL, GDP. |
| Treasury curve series | FRED CSV | Yahoo/FRED proxy for market panel formatting | FRED remains the authoritative macro source for DGS2, DGS10, and T10Y2Y. |
| Broad equity indexes | Yahoo chart API | Stooq CSV, local cache | Keeps public proxies for `^GSPC`, `^IXIC`, `^RUT`, `^VIX`. |
| FX and commodity proxies | Yahoo chart API | FRED proxy series, local cache | Covers DXY, AUDJPY, crude, gold, and copper. |
| Breadth proxy | Derived from local panel universe | none | Uses the available ETF universe inside the snapshot. |
| Morning news flow | Massive news API | Google News RSS search lanes | Massive is preferred when available because it gives cleaner market metadata. |

## Why Massive Is Partial, Not Universal

Massive fits the stock and ETF side of this workflow well, but the repo still needs macro series that belong on FRED:

- Fed balance sheet
- reverse repo usage
- Treasury General Account
- M2
- GDP
- Treasury curve levels
- high-yield spread proxies

Those are not stock-market bars, so FRED remains the right source.

## Required Secrets

- `MASSIVE_API_KEY`: enables Massive-backed prices and news in GitHub Actions and local runs

## Public Fallback Behavior

If Massive is unavailable:

1. The snapshot falls back to Yahoo, Stooq, and FRED proxy series.
2. The news collector falls back to Google News RSS search lanes.
3. The final review can still render, but it may mark itself `DATA LIMITED` if critical inputs are missing.
