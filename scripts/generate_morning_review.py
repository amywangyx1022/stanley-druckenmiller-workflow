#!/usr/bin/env python3
"""Generate an English morning market review and persist the output files."""

from __future__ import annotations

import argparse
import json
import re
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from market_panels import MASSIVE_API_KEY, MASSIVE_BASE_URL, build_snapshot, http_get, http_get_json

DISCLAIMER = (
    "Disclaimer: The above content is research framework information and does not constitute "
    "investment advice or trading instructions."
)

LANE_KEYWORDS = {
    "macro_policy": [
        "fed",
        "fomc",
        "treasury",
        "inflation",
        "cpi",
        "ppi",
        "pce",
        "jobs",
        "payroll",
        "yield",
        "rates",
        "ecb",
        "boj",
        "central bank",
    ],
    "sector_earnings": [
        "earnings",
        "guidance",
        "chip",
        "semiconductor",
        "software",
        "bank",
        "financial",
        "retail",
        "housing",
        "consumer",
        "ai",
        "cloud",
    ],
    "geopolitics_commodities_fx": [
        "oil",
        "copper",
        "gold",
        "dollar",
        "yuan",
        "yen",
        "tariff",
        "sanction",
        "war",
        "opec",
        "commodity",
        "fx",
        "currency",
        "shipping",
    ],
}

RSS_QUERIES = {
    "macro_policy": "Federal Reserve OR inflation OR Treasury yields OR payrolls when:1d",
    "sector_earnings": "earnings OR semiconductor OR banks OR retail when:1d",
    "geopolitics_commodities_fx": "oil OR copper OR dollar OR tariffs OR geopolitics when:1d",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate the daily morning review")
    parser.add_argument("--output-dir", required=True, help="Directory for the latest output files")
    parser.add_argument("--archive-dir", help="Directory for dated archive copies")
    parser.add_argument("--timezone", default="America/Chicago", help="Local timezone for scheduling gate")
    parser.add_argument("--require-local-hour", type=int, help="Only run when the local hour matches")
    parser.add_argument("--force", action="store_true", help="Ignore the local-hour scheduling gate")
    parser.add_argument("--pause", type=float, default=0.25, help="Pause passed through to market snapshot calls")
    return parser.parse_args()


def maybe_skip_run(timezone_name: str, required_hour: int | None, force: bool) -> bool:
    if force or required_hour is None:
        return False
    local_now = datetime.now(ZoneInfo(timezone_name))
    return local_now.hour != required_hour


def massive_news(limit: int = 15) -> list[dict]:
    if not MASSIVE_API_KEY:
        return []
    try:
        since = (datetime.now(timezone.utc) - timedelta(hours=36)).isoformat().replace("+00:00", "Z")
        payload = http_get_json(
            f"{MASSIVE_BASE_URL}/v2/reference/news",
            {
                "limit": str(limit),
                "order": "desc",
                "sort": "published_utc",
                "published_utc.gte": since,
                "apiKey": MASSIVE_API_KEY,
            },
        )
        out: list[dict] = []
        for item in payload.get("results") or []:
            title = (item.get("title") or "").strip()
            if not title:
                continue
            out.append(
                {
                    "title": title,
                    "summary": (item.get("description") or "").strip(),
                    "publisher": ((item.get("publisher") or {}).get("name") or "Unknown").strip(),
                    "published_utc": item.get("published_utc"),
                    "url": item.get("article_url"),
                    "source": "massive",
                }
            )
        return out
    except Exception:
        return []


def google_news_lane(query: str, lane: str, limit: int = 4) -> list[dict]:
    try:
        url = (
            "https://news.google.com/rss/search?"
            + urllib.parse.urlencode({"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"})
        )
        xml_text = http_get(url)
        root = ET.fromstring(xml_text)
        out: list[dict] = []
        for item in root.findall("./channel/item")[:limit]:
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            source = item.findtext("source") or "Google News"
            if not title:
                continue
            out.append(
                {
                    "title": title,
                    "summary": "",
                    "publisher": source.strip(),
                    "published_utc": pub_date,
                    "url": link,
                    "source": "google-news-rss",
                    "lane": lane,
                }
            )
        return out
    except Exception:
        return []


def classify_lane(item: dict) -> str:
    haystack = " ".join([item.get("title") or "", item.get("summary") or ""]).lower()
    scores = {
        lane: sum(1 for keyword in keywords if keyword in haystack)
        for lane, keywords in LANE_KEYWORDS.items()
    }
    best_lane = max(scores, key=scores.get)
    return best_lane if scores[best_lane] > 0 else "macro_policy"


def collect_news() -> list[dict]:
    articles: list[dict] = []
    seen_titles: set[str] = set()

    for item in massive_news():
        title_key = re.sub(r"\s+", " ", (item.get("title") or "").strip().lower())
        if not title_key or title_key in seen_titles:
            continue
        item["lane"] = classify_lane(item)
        seen_titles.add(title_key)
        articles.append(item)

    lane_counts = {lane: 0 for lane in RSS_QUERIES}
    for item in articles:
        lane_counts[item["lane"]] = lane_counts.get(item["lane"], 0) + 1

    for lane, query in RSS_QUERIES.items():
        need = max(0, 3 - lane_counts.get(lane, 0))
        if need == 0:
            continue
        for item in google_news_lane(query, lane, limit=need + 1):
            title_key = re.sub(r"\s+", " ", (item.get("title") or "").strip().lower())
            if not title_key or title_key in seen_titles:
                continue
            seen_titles.add(title_key)
            articles.append(item)
            lane_counts[lane] = lane_counts.get(lane, 0) + 1

    def published_sort_key(item: dict) -> str:
        return item.get("published_utc") or ""

    articles.sort(key=published_sort_key, reverse=True)
    return articles[:12]


def pick(snapshot: dict, *path: str) -> dict:
    node = snapshot
    for key in path:
        if not isinstance(node, dict):
            return {}
        node = node.get(key, {})
    return node if isinstance(node, dict) else {}


def value_or_none(snapshot: dict, *path: str, field: str) -> float | None:
    node = pick(snapshot, *path)
    value = node.get(field)
    return value if isinstance(value, (int, float)) else None


def fmt_pct(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.{digits}f}%"


def fmt_num(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:.{digits}f}"


def thesis_label(snapshot: dict) -> tuple[str, str]:
    dgs10_5d = value_or_none(snapshot, "fred", "DGS10", field="chg_5d_abs")
    dxy_5d = value_or_none(snapshot, "panels", "fx", "DX-Y.NYB", field="chg_5d_pct")
    hyg_5d = value_or_none(snapshot, "panels", "credit", "HYG", field="chg_5d_pct")
    iwm_5d = value_or_none(snapshot, "panels", "equities", "IWM", field="chg_5d_pct")
    spy_5d = value_or_none(snapshot, "panels", "equities", "SPY", field="chg_5d_pct")
    breadth = value_or_none(snapshot, "panels", "breadth_proxy", field="pct_above_200d")
    smh_5d = value_or_none(snapshot, "panels", "internals", "SMH", field="chg_5d_pct")

    tightening = 0
    relief = 0

    if dgs10_5d is not None and dgs10_5d > 0.08:
        tightening += 1
    if dgs10_5d is not None and dgs10_5d < -0.08:
        relief += 1
    if dxy_5d is not None and dxy_5d > 0.4:
        tightening += 1
    if dxy_5d is not None and dxy_5d < -0.4:
        relief += 1
    if hyg_5d is not None and hyg_5d < -0.5:
        tightening += 1
    if hyg_5d is not None and hyg_5d > 0.5:
        relief += 1
    if breadth is not None and breadth < 50:
        tightening += 1
    if breadth is not None and breadth > 60:
        relief += 1
    if iwm_5d is not None and spy_5d is not None and iwm_5d > spy_5d + 0.4:
        relief += 1
    if smh_5d is not None and smh_5d > 1.0:
        relief += 1

    if tightening >= relief + 1:
        return (
            "Financial conditions are leaning tighter again.",
            "The working hypothesis is that higher real rates and a firmer dollar are taxing broad risk appetite faster than index-level price action admits.",
        )
    if relief >= tightening + 1:
        return (
            "The tape is still trading a falling-rate relief regime.",
            "The working hypothesis is that easier rates and better leadership under the surface are extending risk appetite, even if the move is not fully broad yet.",
        )
    return (
        "The market is in a selective rotation regime.",
        "The working hypothesis is that leadership is narrow enough that you need confirmation from credit and breadth before calling this a durable macro expansion.",
    )


def regime_confidence(snapshot: dict, data_limited: bool) -> tuple[str, str]:
    breadth = pick(snapshot, "panels", "breadth_proxy")
    ad_line = breadth.get("ad_line_1d")
    pct_above = breadth.get("pct_above_200d")
    hyg_5d = value_or_none(snapshot, "panels", "credit", "HYG", field="chg_5d_pct")
    dgs10_5d = value_or_none(snapshot, "fred", "DGS10", field="chg_5d_abs")

    if data_limited:
        return "Low", "Approaching Turning Point"
    if isinstance(pct_above, (int, float)) and pct_above > 60 and isinstance(hyg_5d, (int, float)) and hyg_5d > 0.5:
        return "High", "Trend Continuation"
    if isinstance(pct_above, (int, float)) and pct_above < 45 and isinstance(dgs10_5d, (int, float)) and abs(dgs10_5d) > 0.1:
        return "Low", "Approaching Turning Point"
    if isinstance(ad_line, (int, float)) and abs(ad_line) <= 2:
        return "Medium", "Approaching Turning Point"
    return "Medium", "Trend Continuation"


def evidence_anchor(name: str, change: str, lookback: str, timestamp: str, source: str) -> str:
    return f"- {name}: {change}; lookback {lookback}; timestamp {timestamp}; source {source}"


def render_review(snapshot: dict, news_items: list[dict]) -> str:
    generated_at = snapshot.get("generated_at_utc", datetime.now(timezone.utc).isoformat())
    dgs10 = pick(snapshot, "fred", "DGS10")
    dgs2 = pick(snapshot, "fred", "DGS2")
    dxy = pick(snapshot, "panels", "fx", "DX-Y.NYB")
    audjpy = pick(snapshot, "panels", "fx", "AUDJPY=X")
    spy = pick(snapshot, "panels", "equities", "SPY")
    iwm = pick(snapshot, "panels", "equities", "IWM")
    hyg = pick(snapshot, "panels", "credit", "HYG")
    lqd = pick(snapshot, "panels", "credit", "LQD")
    smh = pick(snapshot, "panels", "internals", "SMH")
    kre = pick(snapshot, "panels", "internals", "KRE")
    xly = pick(snapshot, "panels", "internals", "XLY")
    xlp = pick(snapshot, "panels", "internals", "XLP")
    breadth = pick(snapshot, "panels", "breadth_proxy")
    copper_gold = pick(snapshot, "ratios", "COPPER_GOLD")

    missing = []
    for label, node in [
        ("DGS10", dgs10),
        ("DGS2", dgs2),
        ("DXY", dxy),
        ("HYG", hyg),
        ("SPY", spy),
        ("breadth_proxy", breadth),
    ]:
        if not node or not node.get("ok", True):
            missing.append(label)

    lane_coverage = {item.get("lane") for item in news_items}
    if len(news_items) < 8:
        missing.append("news_coverage")
    if len(lane_coverage) < 3:
        missing.append("news_lane_coverage")

    data_limited = bool(missing)
    thesis_headline, thesis_body = thesis_label(snapshot)
    confidence, regime_status = regime_confidence(snapshot, data_limited)

    iwm_vs_spy = None
    if isinstance(iwm.get("chg_5d_pct"), (int, float)) and isinstance(spy.get("chg_5d_pct"), (int, float)):
        iwm_vs_spy = iwm["chg_5d_pct"] - spy["chg_5d_pct"]

    xly_vs_xlp = None
    if isinstance(xly.get("chg_5d_pct"), (int, float)) and isinstance(xlp.get("chg_5d_pct"), (int, float)):
        xly_vs_xlp = xly["chg_5d_pct"] - xlp["chg_5d_pct"]

    lines = [f"# Morning Review - {generated_at[:10]}", ""]

    if data_limited:
        lines.extend(
            [
                "DATA LIMITED",
                "",
                "Missing or thin inputs: " + ", ".join(sorted(set(missing))),
                "",
            ]
        )

    lines.extend(
        [
            "## Core Macro Thesis",
            thesis_headline,
            thesis_body,
            "",
            "## Market Truth",
            (
                f"Narrative: the overnight flow still leans toward headline-driven macro interpretation, "
                f"but the tape matters more here than the story."
            ),
            (
                f"Tape: SPY is {fmt_pct(spy.get('chg_1d_pct'))} on the day and {fmt_pct(spy.get('chg_5d_pct'))} over five sessions, "
                f"IWM is {fmt_pct(iwm.get('chg_5d_pct'))}, HYG is {fmt_pct(hyg.get('chg_5d_pct'))}, and DXY is {fmt_pct(dxy.get('chg_5d_pct'))}."
            ),
            (
                "Verdict: "
                + (
                    "Validated."
                    if not data_limited and confidence != "Low"
                    else "Pending because confirmation is incomplete."
                )
            ),
            "",
            "## Rates and FX Anchor",
            (
                f"The 10-year Treasury yield is {fmt_num(dgs10.get('value'))}% with a five-day change of "
                f"{fmt_num(dgs10.get('chg_5d_abs'))} points. The 2-year is {fmt_num(dgs2.get('value'))}% "
                f"with a five-day change of {fmt_num(dgs2.get('chg_5d_abs'))} points. "
                f"DXY is {fmt_pct(dxy.get('chg_5d_pct'))} over five sessions and AUDJPY is {fmt_pct(audjpy.get('chg_5d_pct'))}."
            ),
            (
                "If rates and the dollar are both firm while credit lags, the burden of proof is on equities. "
                "If rates ease and FX softens, the bar for a broader risk rally drops materially."
            ),
            "",
            "## Throughlines",
            (
                f"1. ["
                + ("Validates" if isinstance(dxy.get('chg_5d_pct'), (int, float)) and dxy['chg_5d_pct'] > 0 else "Nuances")
                + f"] Rates and FX versus credit: DXY at {fmt_pct(dxy.get('chg_5d_pct'))} and HYG at {fmt_pct(hyg.get('chg_5d_pct'))} "
                "tell you whether tighter conditions are actually biting. "
                + ("Status: Confirmed." if isinstance(hyg.get("chg_5d_pct"), (int, float)) and hyg["chg_5d_pct"] < 0 else "Status: Mixed.")
            ),
            (
                f"2. ["
                + ("Validates" if isinstance(iwm_vs_spy, (int, float)) and iwm_vs_spy > 0 else "Refutes")
                + f"] Small caps versus the index: IWM minus SPY is {fmt_pct(iwm_vs_spy)} over five sessions, "
                f"while SMH is {fmt_pct(smh.get('chg_5d_pct'))} and KRE is {fmt_pct(kre.get('chg_5d_pct'))}. "
                + (
                    "Status: Confirmed."
                    if isinstance(iwm_vs_spy, (int, float)) and iwm_vs_spy > 0 and isinstance(kre.get("chg_5d_pct"), (int, float)) and kre["chg_5d_pct"] > 0
                    else "Status: Mixed."
                )
            ),
            (
                f"3. ["
                + ("Validates" if isinstance(xly_vs_xlp, (int, float)) and xly_vs_xlp > 0 else "Nuances")
                + f"] Consumer and commodity impulse: XLY minus XLP is {fmt_pct(xly_vs_xlp)} and the copper-gold ratio is "
                f"{fmt_num(copper_gold.get('value'), 4) if copper_gold.get('ok') else 'n/a'}. "
                + (
                    "Status: Confirmed."
                    if isinstance(xly_vs_xlp, (int, float)) and xly_vs_xlp > 0 and copper_gold.get("ok")
                    else "Status: Mixed."
                )
            ),
            "",
            "## The Asymmetry",
            (
                "The asymmetric question is whether index resilience is outrunning confirmation from breadth and credit. "
                f"Breadth shows {fmt_num(breadth.get('pct_above_200d'))}% of the proxy universe above its 200-day average, "
                f"with an AD line of {breadth.get('ad_line_1d', 'n/a')}. If that improves alongside HYG and KRE, the bullish case hardens. "
                "If it does not, the index can still look fine while the underlying regime weakens."
            ),
            "",
            "## PM Desk Color",
            f"My current best bet: {thesis_headline}",
            (
                "Where I may be wrong first: if credit and breadth improve faster than rates normalize, "
                "the market can broaden before the macro tape looks comfortable."
            ),
            (
                "Crowding and pain-trade: the pain move is whichever side assumes yesterday's leadership can persist without "
                "confirmation from the rest of the tape."
            ),
            (
                "Friction: the biggest contradiction is the gap between index-level behavior and the quality of confirmation "
                "coming from breadth, credit, and small-cap participation."
            ),
            (
                "First validation signal I watch next session: whether HYG, IWM, and KRE confirm or reject the message from rates and DXY "
                "in the first half of the day."
            ),
            "",
            "## what_would_change_my_mind",
            f"- If DGS10 reverses by more than {fmt_num(abs(dgs10.get('chg_5d_abs') or 0.10))} points while DXY softens, the current macro weather changes.",
            "- If HYG and LQD both turn higher over the next few sessions, financial conditions are not biting the way the cautious case assumes.",
            "- If breadth improves meaningfully above the current proxy level and small caps start leading cleanly, the expansion case gains credibility.",
            "- If the news flow shifts from macro pressure toward broad earnings confirmation, the tape deserves a more constructive interpretation.",
            "",
            "## Regime Stability and Confidence",
            f"Regime Status: {regime_status}",
            f"Confidence: {confidence}",
            "",
            "## News Flow",
        ]
    )

    for item in news_items[:8]:
        lines.append(
            f"- [{item.get('lane', 'macro_policy')}] {item.get('title')} ({item.get('publisher', 'Unknown')}, {item.get('published_utc', 'n/a')})"
        )

    lines.extend(
        [
            "",
            "## data_timestamp",
            generated_at,
            "",
            "## Evidence Anchors",
            evidence_anchor(
                "US 10Y yield",
                f"{fmt_num(dgs10.get('value'))}% with 5-day move {fmt_num(dgs10.get('chg_5d_abs'))}",
                "5d",
                dgs10.get("date", generated_at),
                "FRED DGS10",
            ),
            evidence_anchor(
                "US 2Y yield",
                f"{fmt_num(dgs2.get('value'))}% with 5-day move {fmt_num(dgs2.get('chg_5d_abs'))}",
                "5d",
                dgs2.get("date", generated_at),
                "FRED DGS2",
            ),
            evidence_anchor(
                "DXY proxy",
                fmt_pct(dxy.get("chg_5d_pct")),
                "5d",
                dxy.get("asof_utc", generated_at),
                dxy.get("source", "unknown"),
            ),
            evidence_anchor(
                "HYG",
                fmt_pct(hyg.get("chg_5d_pct")),
                "5d",
                hyg.get("asof_utc", generated_at),
                hyg.get("source", "unknown"),
            ),
            evidence_anchor(
                "LQD",
                fmt_pct(lqd.get("chg_5d_pct")),
                "5d",
                lqd.get("asof_utc", generated_at),
                lqd.get("source", "unknown"),
            ),
            evidence_anchor(
                "SPY",
                fmt_pct(spy.get("chg_5d_pct")),
                "5d",
                spy.get("asof_utc", generated_at),
                spy.get("source", "unknown"),
            ),
            evidence_anchor(
                "IWM",
                fmt_pct(iwm.get("chg_5d_pct")),
                "5d",
                iwm.get("asof_utc", generated_at),
                iwm.get("source", "unknown"),
            ),
            evidence_anchor(
                "SMH",
                fmt_pct(smh.get("chg_5d_pct")),
                "5d",
                smh.get("asof_utc", generated_at),
                smh.get("source", "unknown"),
            ),
            evidence_anchor(
                "Breadth proxy",
                f"{fmt_num(breadth.get('pct_above_200d'))}% above 200d, AD line {breadth.get('ad_line_1d', 'n/a')}",
                "1d/structural",
                generated_at,
                breadth.get("method", "proxy-universe"),
            ),
            evidence_anchor(
                "Copper/Gold ratio",
                fmt_num(copper_gold.get("value"), 4) if copper_gold.get("ok") else "[EVIDENCE INSUFFICIENT: missing ratio]",
                "spot",
                generated_at,
                "derived",
            ),
            "",
            DISCLAIMER,
        ]
    )

    return "\n".join(lines).rstrip() + "\n"


def write_outputs(output_dir: Path, archive_dir: Path | None, snapshot: dict, news_items: list[dict], review_text: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "market-panels.json").write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output_dir / "news.json").write_text(json.dumps(news_items, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output_dir / "morning-review.md").write_text(review_text, encoding="utf-8")

    if archive_dir is None:
        return

    run_date = datetime.now(timezone.utc).date()
    dated_dir = archive_dir / f"{run_date:%Y}" / f"{run_date:%m}" / f"{run_date:%d}"
    dated_dir.mkdir(parents=True, exist_ok=True)
    (dated_dir / "market-panels.json").write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (dated_dir / "news.json").write_text(json.dumps(news_items, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (dated_dir / "morning-review.md").write_text(review_text, encoding="utf-8")


def main() -> int:
    args = parse_args()
    if maybe_skip_run(args.timezone, args.require_local_hour, args.force):
        print("Skipping run because the local scheduling gate did not match.")
        return 0

    snapshot = build_snapshot(pause_s=args.pause)
    news_items = collect_news()
    review_text = render_review(snapshot, news_items)
    write_outputs(
        Path(args.output_dir),
        Path(args.archive_dir) if args.archive_dir else None,
        snapshot,
        news_items,
        review_text,
    )
    print(review_text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
