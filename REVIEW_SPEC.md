# Review Specification

## Objective

Produce a thesis-driven morning market review in clear English with a live PM memo voice:

- Rates and liquidity first
- Consensus versus tape clearly separated
- Facts and interpretation explicitly distinguished
- Cross-asset causality over panel-by-panel narration
- No trade instructions, targets, stops, or sizing

## Core Output Rules

1. Write in natural English only.
2. Keep the tone direct, conditional, concise, and accountable.
3. Treat every conclusion as a testable hypothesis, not a certainty.
4. Separate observed facts from inferred regime conclusions.
5. Always include:
   - `what_would_change_my_mind`
   - `data_timestamp`
   - `Evidence anchors`
   - the disclaimer line

## Morning Review Structure

The daily morning review should follow this order:

1. Core Macro Thesis
2. Market Truth
3. Rates and FX Anchor
4. Throughlines
5. The Asymmetry
6. **The 18-Month Lens** (top 3 forward themes with thesis, counterargument, and citations)
7. **Portfolio Lens** (top 10 individual equities from SnapTrade, ETFs excluded)
8. PM Desk Color
9. what_would_change_my_mind
10. Regime Stability and Confidence
11. data_timestamp
12. Evidence anchors
13. Disclaimer

## Throughline Rules

1. Every substantive paragraph should connect at least two panels or markets.
2. Focus on 2 to 4 causal throughlines, not a dashboard dump.
3. Each throughline should be tagged as:
   - `Validates`
   - `Refutes`
   - `Nuances`
4. Each throughline should end with a status:
   - `Confirmed`
   - `Mixed`
   - `Failing`

## Asset Hierarchy

1. Rates and FX define the macro weather.
2. Credit, breadth, and internals confirm or reject that weather.
3. Equities are downstream expressions, not the starting point.
4. When equities disagree with rates and FX, flag the divergence immediately.

## Required Panels

- Liquidity
- Rates and credit
- Equity internals
- Breadth
- Commodities and FX
- News flow for the last 24 hours

## Required Evidence Anchor Fields

Each anchor should include:

- panel or metric
- direction or change
- lookback window
- timestamp
- source

If a required field is missing, tag the claim as:

`[EVIDENCE INSUFFICIENT: missing X]`

## Data-Limited Downgrade

If critical panels or news coverage are missing:

1. Start the review with `DATA LIMITED`.
2. List the missing panels or lanes explicitly.
3. Limit the write-up to factual observations from available data.
4. Do not overstate confidence.

## The 18-Month Lens Rules

Requires `OPENAI_API_KEY`. Section is omitted with `DATA LIMITED` if unavailable.

1. Exactly 3 forward themes, ranked by conviction from news + macro snapshot.
2. Each theme must include: headline, thesis, why, how (with tickers), counterargument, what_would_change_my_mind.
3. Every claim must cite at least 2 articles from the 7-day news corpus with a verbatim quoted phrase.
4. All tickers must resolve against Yahoo or Polygon. Themes with fewer than 3 valid tickers are dropped.
5. A critic LLM pass scores each theme; themes below threshold are dropped rather than regenerated.
6. If fewer than 3 themes survive validation, render what passed plus an `[INCOMPLETE]` marker.
7. Voice follows `docs/druckenmiller_persona.md`.

## Portfolio Lens Rules

Requires `SNAPTRADE_CLIENT_ID`, `SNAPTRADE_CONSUMER_KEY`, `SNAPTRADE_USER_ID`, and `SNAPTRADE_USER_SECRET`. Section is omitted with `DATA LIMITED` if credentials are missing.

1. Pull holdings from SnapTrade, aggregate across accounts.
2. Filter out ETFs (SnapTrade `security_type` + Yahoo `quoteType` fallback).
3. Top 10 individual equities by portfolio weight.
4. Each position gets a deterministic throughline tag (Validates / Refutes / Nuances) plus a 1-sentence conditional PM note via LLM.
5. No trade instructions, targets, or sizing.

## PM Voice Requirements

The review should include:

- one line for what I think is happening now
- one line for where I may be wrong first
- one crowding or pain-trade line
- one friction line naming the biggest contradiction in the tape
- one desk-color line stating the next validation signal to watch

## Safety Footer

Always append:

`Disclaimer: The above content is research framework information and does not constitute investment advice or trading instructions.`
