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
6. PM Desk Color
7. what_would_change_my_mind
8. Regime Stability and Confidence
9. data_timestamp
10. Evidence anchors
11. Disclaimer

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
