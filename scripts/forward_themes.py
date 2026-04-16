#!/usr/bin/env python3
"""Forward themes (18-month lens) — LLM extraction with grounding guardrails.

Anti-hallucination contract:
  1. Every theme citation must quote a phrase that literally appears in the input
     news corpus (normalised comparison).
  2. Every theme ticker must resolve against Yahoo — unresolvable tickers are
     dropped; themes with fewer than 3 valid tickers are dropped entirely.
  3. A second LLM "critic" pass scores surviving themes; low scorers drop.
  4. If fewer than 3 themes survive, we render whatever passes with an INCOMPLETE
     marker rather than asking the model to invent replacements.
"""

from __future__ import annotations

import json
import os
import re
import urllib.parse
from pathlib import Path

from market_panels import http_get

try:
    from openai import OpenAI
    _HAS_OPENAI = True
except ImportError:
    _HAS_OPENAI = False

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-2024-08-06").strip()
REPO_ROOT = Path(__file__).resolve().parents[1]
PERSONA_PATH = REPO_ROOT / "docs" / "druckenmiller_persona.md"

MIN_CITATIONS_PER_THEME = 2
MIN_TICKERS_PER_THEME = 3
CRITIC_PASS_THRESHOLD = 6  # out of 10


THEMES_EXTRACTION_SCHEMA = {
    "name": "forward_themes_extraction",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "themes": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "headline": {"type": "string"},
                        "thesis": {"type": "string"},
                        "why_this_could_work": {"type": "string"},
                        "how_to_express_it": {"type": "string"},
                        "counterargument": {"type": "string"},
                        "what_would_change_my_mind": {"type": "string"},
                        "tickers": {"type": "array", "items": {"type": "string"}},
                        "citations": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "additionalProperties": False,
                                "properties": {
                                    "url": {"type": "string"},
                                    "quoted_phrase": {"type": "string"},
                                },
                                "required": ["url", "quoted_phrase"],
                            },
                        },
                    },
                    "required": [
                        "headline",
                        "thesis",
                        "why_this_could_work",
                        "how_to_express_it",
                        "counterargument",
                        "what_would_change_my_mind",
                        "tickers",
                        "citations",
                    ],
                },
            },
        },
        "required": ["themes"],
    },
}

CRITIC_SCHEMA = {
    "name": "theme_critic",
    "strict": True,
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "score": {"type": "integer"},
            "reason": {"type": "string"},
        },
        "required": ["score", "reason"],
    },
}


def _normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _build_news_index(news: list[dict]) -> dict[str, str]:
    """Map url -> normalised concatenation of title + summary for substring checks."""
    index: dict[str, str] = {}
    for item in news:
        url = (item.get("url") or "").strip()
        if not url:
            continue
        body = _normalize(" ".join([item.get("title") or "", item.get("summary") or ""]))
        if url in index:
            index[url] = f"{index[url]} {body}".strip()
        else:
            index[url] = body
    return index


def _validate_citations(theme: dict, news_index: dict[str, str]) -> list[dict]:
    """Drop citations whose quoted_phrase does not appear in the cited article."""
    valid: list[dict] = []
    for citation in theme.get("citations") or []:
        url = (citation.get("url") or "").strip()
        phrase = _normalize(citation.get("quoted_phrase") or "")
        if not url or not phrase:
            continue
        body = news_index.get(url)
        if body and phrase in body:
            valid.append(citation)
    return valid


def _validate_tickers(tickers: list[str]) -> list[str]:
    """Keep only tickers that resolve via Yahoo chart API."""
    valid: list[str] = []
    seen: set[str] = set()
    for raw in tickers:
        symbol = (raw or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        if _yahoo_ticker_exists(symbol):
            valid.append(symbol)
    return valid


def _yahoo_ticker_exists(symbol: str) -> bool:
    encoded = urllib.parse.quote(symbol, safe="")
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}"
        f"?range=5d&interval=1d&includePrePost=false"
    )
    try:
        payload = json.loads(http_get(url, timeout=10, retries=2))
    except Exception:
        return False
    try:
        result = payload["chart"]["result"]
    except (KeyError, TypeError):
        return False
    if not result:
        return False
    meta = (result[0] or {}).get("meta") or {}
    return bool(meta.get("symbol"))


def _load_persona() -> str:
    try:
        return PERSONA_PATH.read_text(encoding="utf-8")
    except OSError:
        return (
            "Write in the disciplined, conditional, asymmetry-hunting voice of a "
            "veteran macro PM. Never fabricate facts, numbers, companies, or quotes."
        )


def _compact_news_for_prompt(news: list[dict], limit: int = 80) -> list[dict]:
    out: list[dict] = []
    for item in news[:limit]:
        out.append({
            "url": item.get("url"),
            "title": item.get("title"),
            "summary": (item.get("summary") or "")[:400],
            "published_utc": item.get("published_utc"),
            "publisher": item.get("publisher"),
        })
    return out


def _compact_macro_for_prompt(snapshot: dict) -> dict:
    fred = snapshot.get("fred") or {}
    panels = snapshot.get("panels") or {}
    equities = panels.get("equities") or {}
    credit = panels.get("credit") or {}
    commodities = panels.get("commodities") or {}
    fx = panels.get("fx") or {}

    def _val(node: dict, key: str = "value") -> float | None:
        if not isinstance(node, dict) or not node.get("ok", False):
            return None
        return node.get(key)

    return {
        "dgs10_pct": _val(fred.get("DGS10") or {}),
        "dgs2_pct": _val(fred.get("DGS2") or {}),
        "curve_10y2y_pct_pts": (snapshot.get("derived") or {}).get("curve_10y_minus_2y_pct_pts"),
        "spy_5d_pct": (equities.get("SPY") or {}).get("chg_5d_pct"),
        "iwm_5d_pct": (equities.get("IWM") or {}).get("chg_5d_pct"),
        "hyg_5d_pct": (credit.get("HYG") or {}).get("chg_5d_pct"),
        "lqd_5d_pct": (credit.get("LQD") or {}).get("chg_5d_pct"),
        "dxy_5d_pct": (fx.get("DX-Y.NYB") or {}).get("chg_5d_pct"),
        "gold_latest": (commodities.get("GC=F") or {}).get("latest"),
        "copper_latest": (commodities.get("HG=F") or {}).get("latest"),
        "oil_latest": (commodities.get("CL=F") or {}).get("latest"),
    }


def _extract_themes(client: "OpenAI", persona: str, news: list[dict], snapshot: dict) -> list[dict]:
    system = (
        persona
        + "\n\nTask: read the news corpus and macro snapshot below. Identify EXACTLY 3 "
        "themes that could dominate market returns over the next 18 months. For each theme:\n"
        "- headline: one punchy sentence.\n"
        "- thesis: 2-3 sentences in your voice.\n"
        "- why_this_could_work: macro/flow/structural reasoning.\n"
        "- how_to_express_it: sectors and 3-8 public equity tickers — US-listed, real tickers only.\n"
        "- counterargument: a real skeptic's case, not a strawman.\n"
        "- what_would_change_my_mind: one concrete observable signal.\n"
        "- tickers: 3-8 real US-listed tickers mentioned in the news or clearly in scope.\n"
        "- citations: at least 2 items, each {url, quoted_phrase}. quoted_phrase MUST be a "
        "short verbatim string from the article at that URL. Do not paraphrase. Do not cite "
        "URLs not in the corpus.\n"
        "Return strict JSON matching the schema."
    )
    user_payload = {
        "macro_snapshot": _compact_macro_for_prompt(snapshot),
        "news_corpus": _compact_news_for_prompt(news),
    }
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
            ],
            response_format={"type": "json_schema", "json_schema": THEMES_EXTRACTION_SCHEMA},
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[forward_themes] extraction call failed: {exc}", flush=True)
        return []

    try:
        content = response.choices[0].message.content or "{}"
        return (json.loads(content) or {}).get("themes") or []
    except Exception as exc:  # noqa: BLE001
        print(f"[forward_themes] extraction parse failed: {exc}", flush=True)
        return []


def _critic_score(client: "OpenAI", persona: str, theme: dict) -> int:
    system = (
        persona
        + "\n\nYou are a critic scoring a proposed 18-month theme on a 0-10 scale. "
        "Consider: citation relevance, strength of counterargument, voice fit, "
        "specificity of the 'what would change my mind' signal. Return integer + reason."
    )
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            temperature=0,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(theme, ensure_ascii=False)},
            ],
            response_format={"type": "json_schema", "json_schema": CRITIC_SCHEMA},
        )
        content = response.choices[0].message.content or "{}"
        parsed = json.loads(content)
        return int(parsed.get("score") or 0)
    except Exception as exc:  # noqa: BLE001
        print(f"[forward_themes] critic call failed: {exc}", flush=True)
        return 0


def _render_markdown(themes: list[dict], incomplete_note: str | None) -> str:
    lines: list[str] = ["## The 18-Month Lens", ""]
    if incomplete_note:
        lines.extend([f"[INCOMPLETE: {incomplete_note}]", ""])

    if not themes:
        lines.append(
            "No themes survived grounding validation. The corpus either lacked "
            "coverage or the candidates failed citation and ticker checks."
        )
        lines.append("")
        return "\n".join(lines)

    for idx, theme in enumerate(themes, start=1):
        citations = theme.get("citations") or []
        citation_lines = []
        for c in citations:
            url = c.get("url", "")
            phrase = (c.get("quoted_phrase") or "").strip()
            citation_lines.append(f'  - "{phrase}" — {url}')
        tickers = ", ".join(theme.get("tickers") or []) or "n/a"

        lines.extend([
            f"### Theme {idx}: {theme.get('headline', '').strip()}",
            f"**Thesis.** {theme.get('thesis', '').strip()}",
            f"**Why this could work.** {theme.get('why_this_could_work', '').strip()}",
            f"**How to express it.** {theme.get('how_to_express_it', '').strip()} Tickers in scope: {tickers}.",
            f"**Counterargument.** {theme.get('counterargument', '').strip()}",
            f"**What would change my mind.** {theme.get('what_would_change_my_mind', '').strip()}",
            "**Citations.**",
            *citation_lines,
            "",
        ])
    return "\n".join(lines)


def render_section(snapshot: dict, news: list[dict] | None = None) -> str | None:
    """Return the Markdown block for the 18-Month Lens or None if disabled.

    Returns a DATA LIMITED placeholder string if OpenAI is unavailable so the
    caller can decide whether to include it.
    """
    if not OPENAI_API_KEY or not _HAS_OPENAI:
        return (
            "## The 18-Month Lens\n\n"
            "[DATA LIMITED: forward_themes skipped — OPENAI_API_KEY or openai SDK not present.]\n"
        )

    if news is None:
        from news_ingest import collect_forward_news
        news = collect_forward_news()

    if len(news) < 10:
        return (
            "## The 18-Month Lens\n\n"
            f"[DATA LIMITED: forward_themes skipped — only {len(news)} articles "
            "available, below floor for grounded extraction.]\n"
        )

    client = OpenAI(api_key=OPENAI_API_KEY)
    persona = _load_persona()
    news_index = _build_news_index(news)

    raw_themes = _extract_themes(client, persona, news, snapshot)
    validated: list[dict] = []
    for theme in raw_themes:
        citations = _validate_citations(theme, news_index)
        if len(citations) < MIN_CITATIONS_PER_THEME:
            continue
        tickers = _validate_tickers(theme.get("tickers") or [])
        if len(tickers) < MIN_TICKERS_PER_THEME:
            continue
        theme["citations"] = citations
        theme["tickers"] = tickers
        score = _critic_score(client, persona, theme)
        theme["_critic_score"] = score
        if score < CRITIC_PASS_THRESHOLD:
            continue
        validated.append(theme)

    incomplete = None
    if len(validated) < 3:
        incomplete = (
            f"only {len(validated)} theme(s) of {len(raw_themes)} candidates met the "
            "citation + ticker + critic thresholds"
        )

    return _render_markdown(validated[:3], incomplete)


if __name__ == "__main__":
    import sys
    from market_panels import build_snapshot
    snap = build_snapshot()
    out = render_section(snap)
    sys.stdout.write(out or "(section suppressed)\n")
