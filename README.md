# Stanley Druckenmiller Workflow

This repository is now a plain GitHub automation project for a Druckenmiller-style morning market review. It no longer depends on ClawHub, OpenClaw, or skill-packaging metadata.

The repo does five things:

1. Builds a market snapshot from public macro feeds plus optional Massive/Polygon market data.
2. Generates an English-only morning review in Markdown with cross-asset throughlines.
3. Identifies the top 3 forward-looking investment themes (18-month horizon) using LLM extraction from 7-day news with anti-hallucination guardrails.
4. Pulls portfolio holdings via SnapTrade, strips ETFs, and applies per-position PM-desk analysis.
5. Runs that review automatically on weekday mornings through GitHub Actions and emails it through Gmail SMTP.

## Repo Layout

```text
.
├── .github/workflows/morning-review.yml
├── docs/
│   ├── data-sources.md
│   └── druckenmiller_persona.md
├── requirements.txt
├── REVIEW_SPEC.md
└── scripts/
    ├── generate_morning_review.py
    ├── market_panels.py
    ├── forward_themes.py
    ├── news_ingest.py
    ├── snaptrade_portfolio.py
    └── send_review_email.py
```

## Morning Review Routine

The scheduled workflow lives at `.github/workflows/morning-review.yml`.

- Schedule: weekday mornings around 7:15 AM Chicago time
- Trigger: `workflow_dispatch` and `schedule`
- Output:
  - `reports/latest/morning-review.md`
  - `reports/latest/market-panels.json`
  - `reports/latest/news.json`
  - dated archive copies under `reports/archive/`
  - email delivery through Gmail SMTP

The workflow uses GitHub Actions' timezone-aware schedule support with `America/Chicago`, so the daily run stays aligned with the local morning across daylight saving changes.

## Data Sources

The source map is documented in [docs/data-sources.md](/C:/Users/awang/Downloads/Github_repo/stanley-druckenmiller-workflow/docs/data-sources.md).

In short:

- Massive is the preferred source for stock and ETF daily bars when `MASSIVE_API_KEY` is configured.
- Polygon is the second tier for equities and commodities when `POLYGON_API_KEY` is configured.
- FRED remains the source of record for liquidity and macro time series, with Yahoo CBOE yield indices (^TNX, ^FVX, etc.) as fallback when FRED CSV returns stale/empty data.
- Yahoo, Stooq, and FRED proxy series remain as public fallbacks where Massive/Polygon do not fit or no key is present.
- News uses Massive + Polygon + Google News RSS for both the daily narrative and the 7-day forward-themes corpus.

## Required GitHub Secrets

Set these repository secrets:

- `MASSIVE_API_KEY`
- `GMAIL_SMTP_USERNAME`
- `GMAIL_APP_PASSWORD`
- `MORNING_REVIEW_EMAIL_TO`

Optional (sections gracefully degrade when absent):

- `POLYGON_API_KEY` — adds Polygon as a data tier for equities, commodities, and news.
- `OPENAI_API_KEY` — enables the 18-Month Lens forward-themes section (requires `openai` SDK).
- `SNAPTRADE_CLIENT_ID` — enables portfolio integration.
- `SNAPTRADE_CONSUMER_KEY`
- `SNAPTRADE_USER_ID`
- `SNAPTRADE_USER_SECRET`
- `MORNING_REVIEW_EMAIL_FROM`
- `MORNING_REVIEW_EMAIL_CC`
- `MORNING_REVIEW_SUBJECT_PREFIX`

Without `MASSIVE_API_KEY`, the workflow still runs using the public fallback sources, but the review may mark itself `DATA LIMITED` if live news coverage is too thin. Without `OPENAI_API_KEY` or `SNAPTRADE_*` credentials, the corresponding sections are omitted with a `DATA LIMITED` marker.

## Manual Usage

Build a snapshot:

```bash
python scripts/market_panels.py --pretty --output reports/latest/market-panels.json
```

Generate the full morning review:

```bash
python scripts/generate_morning_review.py --force --output-dir reports/latest --archive-dir reports/archive
```

Email the latest review:

```bash
python scripts/send_review_email.py \
  --markdown reports/latest/morning-review.md \
  --snapshot reports/latest/market-panels.json \
  --news reports/latest/news.json \
  --smtp-username "$GMAIL_SMTP_USERNAME" \
  --app-password "$GMAIL_APP_PASSWORD" \
  --to "you@example.com"
```

## Notes

- The output is English-only.
- The project is inspired by Druckenmiller-style cross-asset thinking and does not claim private access or direct affiliation.
- The generated review is research workflow output, not investment advice.
- Gmail app passwords require Google account 2-Step Verification. Google also notes that app passwords are not recommended and may be unavailable for some managed Workspace accounts.
