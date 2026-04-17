#!/usr/bin/env python3
"""SnapTrade portfolio ingest — aggregate holdings across accounts and filter ETFs.

Credentials from env:
  SNAPTRADE_CLIENT_ID
  SNAPTRADE_CONSUMER_KEY
  SNAPTRADE_USER_ID
  SNAPTRADE_USER_SECRET

All four are required; if any is missing, every public entry point returns a
DATA LIMITED placeholder rather than raising.
"""

from __future__ import annotations

import json
import os
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path

from market_panels import http_get

try:
    from snaptrade_client import SnapTrade
    _HAS_SNAPTRADE = True
except ImportError:
    _HAS_SNAPTRADE = False


SNAPTRADE_CLIENT_ID = os.environ.get("SNAPTRADE_CLIENT_ID", "").strip()
SNAPTRADE_CONSUMER_KEY = os.environ.get("SNAPTRADE_CONSUMER_KEY", "").strip()
SNAPTRADE_USER_ID = os.environ.get("SNAPTRADE_USER_ID", "").strip()
SNAPTRADE_USER_SECRET = os.environ.get("SNAPTRADE_USER_SECRET", "").strip()

ETF_TYPE_CODES = {"et", "etf", "etn"}
FUND_DESC_HINTS = ("etf", "exchange traded", "etn")


def _creds_present() -> bool:
    return bool(
        SNAPTRADE_CLIENT_ID
        and SNAPTRADE_CONSUMER_KEY
        and SNAPTRADE_USER_ID
        and SNAPTRADE_USER_SECRET
    )


def _as_dict(obj) -> dict:
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "to_dict"):
        try:
            return obj.to_dict()  # type: ignore[no-any-return]
        except Exception:  # noqa: BLE001
            pass
    if hasattr(obj, "__dict__"):
        return {k: v for k, v in obj.__dict__.items() if not k.startswith("_")}
    return {}


def _fetch_holdings_raw() -> list[dict]:
    """Call SnapTrade and return the list of per-account holdings payloads."""
    if not _HAS_SNAPTRADE:
        raise RuntimeError("snaptrade-python-sdk not installed")
    client = SnapTrade(
        client_id=SNAPTRADE_CLIENT_ID,
        consumer_key=SNAPTRADE_CONSUMER_KEY,
    )
    response = client.account_information.get_all_user_holdings(
        user_id=SNAPTRADE_USER_ID,
        user_secret=SNAPTRADE_USER_SECRET,
    )
    body = getattr(response, "body", response)
    if isinstance(body, (list, tuple)):
        return [_as_dict(item) for item in body]
    body_dict = _as_dict(body)
    if isinstance(body_dict.get("account_holdings"), list):
        return [_as_dict(item) for item in body_dict["account_holdings"]]
    return []


def _extract_symbol_info(position: dict) -> tuple[str, str, str]:
    """Return (raw_symbol, description, type_code_lower) from a SnapTrade position."""
    symbol_outer = _as_dict(position.get("symbol"))
    symbol_inner = _as_dict(symbol_outer.get("symbol")) or symbol_outer

    raw = (
        symbol_inner.get("raw_symbol")
        or symbol_inner.get("symbol")
        or symbol_outer.get("raw_symbol")
        or ""
    )
    description = (
        symbol_inner.get("description")
        or symbol_outer.get("description")
        or ""
    )

    type_node = _as_dict(symbol_inner.get("type") or symbol_outer.get("type"))
    type_code = (type_node.get("code") or type_node.get("description") or "").lower()

    return str(raw).strip().upper(), str(description).strip(), type_code


def _yahoo_quote_type(symbol: str) -> str | None:
    if not symbol:
        return None
    encoded = urllib.parse.quote(symbol, safe="")
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}"
        f"?range=5d&interval=1d&includePrePost=false"
    )
    try:
        payload = json.loads(http_get(url, timeout=10, retries=2))
        result = payload["chart"]["result"][0]
        meta = result.get("meta") or {}
        return (meta.get("instrumentType") or "").upper() or None
    except Exception:
        return None


def _is_etf(type_code: str, description: str, symbol: str) -> bool:
    if type_code in ETF_TYPE_CODES:
        return True
    desc_lower = description.lower()
    if any(hint in desc_lower for hint in FUND_DESC_HINTS):
        return True
    if type_code:
        # SnapTrade gave us an explicit non-ETF type — trust it, skip Yahoo call.
        return False
    # Ambiguous: ask Yahoo.
    return _yahoo_quote_type(symbol) == "ETF"


def _aggregate(holdings: list[dict]) -> list[dict]:
    buckets: dict[str, dict] = {}
    for account_payload in holdings:
        account = _as_dict(account_payload.get("account"))
        account_id = account.get("id") or account.get("number") or "unknown"
        positions = account_payload.get("positions") or []
        for position in positions:
            pos = _as_dict(position)
            symbol, description, type_code = _extract_symbol_info(pos)
            if not symbol:
                continue
            units = float(pos.get("units") or 0)
            price = float(pos.get("price") or 0)
            avg_cost = pos.get("average_purchase_price")
            if avg_cost is None:
                avg_cost = price
            avg_cost = float(avg_cost or 0)
            market_value = units * price

            bucket = buckets.setdefault(symbol, {
                "symbol": symbol,
                "description": description,
                "type_code": type_code,
                "units": 0.0,
                "market_value": 0.0,
                "cost_basis_total": 0.0,
                "last_price": price,
                "account_ids": [],
            })
            bucket["units"] += units
            bucket["market_value"] += market_value
            bucket["cost_basis_total"] += units * avg_cost
            bucket["last_price"] = price or bucket["last_price"]
            if account_id not in bucket["account_ids"]:
                bucket["account_ids"].append(account_id)
            if not bucket["description"] and description:
                bucket["description"] = description
            if not bucket["type_code"] and type_code:
                bucket["type_code"] = type_code

    aggregated: list[dict] = []
    for bucket in buckets.values():
        units = bucket["units"]
        avg_cost = (bucket["cost_basis_total"] / units) if units else 0.0
        bucket["avg_cost"] = avg_cost
        bucket["unrealized_gain_pct"] = (
            ((bucket["last_price"] - avg_cost) / avg_cost) * 100.0
            if avg_cost
            else None
        )
        aggregated.append(bucket)

    total_mv = sum(b["market_value"] for b in aggregated) or 1.0
    for bucket in aggregated:
        bucket["weight_pct"] = (bucket["market_value"] / total_mv) * 100.0

    aggregated.sort(key=lambda b: b["market_value"], reverse=True)
    return aggregated


def load_portfolio() -> dict:
    """Return {'ok': bool, 'aggregated_all': [...], 'aggregated_equities_only': [...], ...}."""
    if not _creds_present():
        return {
            "ok": False,
            "error": "SnapTrade credentials missing",
            "required_env": [
                "SNAPTRADE_CLIENT_ID",
                "SNAPTRADE_CONSUMER_KEY",
                "SNAPTRADE_USER_ID",
                "SNAPTRADE_USER_SECRET",
            ],
        }
    if not _HAS_SNAPTRADE:
        return {"ok": False, "error": "snaptrade-python-sdk not installed"}

    try:
        raw = _fetch_holdings_raw()
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "error": f"SnapTrade fetch failed: {exc}"}

    aggregated_all = _aggregate(raw)
    equities_only: list[dict] = []
    for bucket in aggregated_all:
        is_etf = _is_etf(bucket["type_code"], bucket["description"], bucket["symbol"])
        bucket["is_etf"] = is_etf
        if not is_etf:
            equities_only.append(dict(bucket))

    # Recompute weights within equities-only view so percentages stay meaningful.
    eq_total = sum(b["market_value"] for b in equities_only) or 1.0
    for bucket in equities_only:
        bucket["weight_pct_of_equities"] = (bucket["market_value"] / eq_total) * 100.0

    return {
        "ok": True,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "account_count": len(raw),
        "aggregated_all": aggregated_all,
        "aggregated_equities_only": equities_only,
    }


def persist_portfolio(output_dir: Path, portfolio: dict) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "portfolio.json"
    path.write_text(json.dumps(portfolio, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


PORTFOLIO_NOTES_SCHEMA = {
    "name": "portfolio_position_notes",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "notes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "symbol": {"type": "string"},
                        "alignment": {"type": "string"},
                        "comment": {"type": "string"},
                    },
                    "required": ["symbol", "alignment", "comment"],
                },
            },
        },
        "required": ["notes"],
    },
}


def _format_position_line(position: dict) -> str:
    symbol = position.get("symbol", "")
    weight = position.get("weight_pct_of_equities") or position.get("weight_pct") or 0.0
    units = position.get("units") or 0.0
    last = position.get("last_price") or 0.0
    avg_cost = position.get("avg_cost") or 0.0
    gain = position.get("unrealized_gain_pct")
    gain_str = f"{gain:+.1f}%" if isinstance(gain, (int, float)) else "n/a"
    return (
        f"- **{symbol}** — {weight:.1f}% of equities, {units:.2f} sh, "
        f"last {last:.2f} vs avg cost {avg_cost:.2f} ({gain_str})"
    )


def _generate_position_notes(
    snapshot: dict,
    positions: list[dict],
    thesis_headline: str,
) -> dict[str, dict]:
    """Call OpenAI once to produce a conditional note per position. Returns {symbol: {...}}."""
    if not positions:
        return {}
    if not os.environ.get("OPENAI_API_KEY", "").strip():
        return {}
    try:
        from openai import OpenAI
    except ImportError:
        return {}

    from forward_themes import _compact_macro_for_prompt, _load_persona, OPENAI_MODEL

    persona = _load_persona()
    macro = _compact_macro_for_prompt(snapshot)
    trimmed = [
        {
            "symbol": p["symbol"],
            "description": p.get("description", ""),
            "weight_pct_of_equities": round(p.get("weight_pct_of_equities") or 0.0, 2),
            "unrealized_gain_pct": (
                round(p["unrealized_gain_pct"], 1)
                if isinstance(p.get("unrealized_gain_pct"), (int, float))
                else None
            ),
        }
        for p in positions
    ]

    system = (
        persona
        + "\n\nTask: for each portfolio position below, write ONE conditional sentence "
        "of PM-desk color. Do not give investment advice. Frame as a hypothesis with a "
        "concrete macro kill-switch (reference rates, credit, or FX from the snapshot). "
        "alignment must be one of: Validates, Refutes, Nuances — describing how the "
        "position relates to the current macro thesis. If the evidence is thin, say "
        "'no clear read' rather than inventing a thesis."
    )
    user_payload = {
        "current_thesis": thesis_headline,
        "macro_snapshot": macro,
        "positions": trimmed,
    }
    try:
        client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
            response_format={"type": "json_schema", "json_schema": PORTFOLIO_NOTES_SCHEMA},
        )
        content = response.choices[0].message.content or "{}"
        parsed = json.loads(content).get("notes") or []
    except Exception as exc:  # noqa: BLE001
        print(f"[snaptrade_portfolio] position notes call failed: {exc}", flush=True)
        return {}

    return {
        (n.get("symbol") or "").upper(): n
        for n in parsed
        if (n.get("symbol") or "").strip()
    }


def render_section(
    portfolio: dict,
    snapshot: dict,
    thesis_headline: str,
    top_n: int = 10,
) -> str:
    """Render the Portfolio Lens Markdown block from a portfolio dict + snapshot."""
    lines: list[str] = ["## Portfolio Lens", ""]
    if not portfolio.get("ok"):
        lines.append(
            f"[DATA LIMITED: portfolio skipped — {portfolio.get('error', 'unknown reason')}]"
        )
        lines.append("")
        return "\n".join(lines)

    equities = portfolio.get("aggregated_equities_only") or []
    if not equities:
        lines.append(
            "No individual equities in aggregated holdings after ETF filter. "
            "Portfolio is entirely in ETFs or cash."
        )
        lines.append("")
        return "\n".join(lines)

    top = equities[:top_n]
    notes_by_symbol = _generate_position_notes(snapshot, top, thesis_headline)

    lines.append(
        f"Top {len(top)} individual stock positions (ETFs excluded), with conditional "
        "PM-desk color anchored to the current macro thesis:"
    )
    lines.append("")
    for position in top:
        symbol = position["symbol"].upper()
        lines.append(_format_position_line(position))
        note = notes_by_symbol.get(symbol)
        if note:
            alignment = (note.get("alignment") or "Nuances").strip()
            comment = (note.get("comment") or "").strip()
            lines.append(f"  - **[{alignment}]** {comment}")
    lines.append("")
    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    result = load_portfolio()
    print(json.dumps(result, indent=2, ensure_ascii=False))
    sys.exit(0 if result.get("ok") else 1)
