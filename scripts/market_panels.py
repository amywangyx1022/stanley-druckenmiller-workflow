#!/usr/bin/env python3
"""Build market panels with Massive, public fallbacks, and local cache."""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import random
import time
import urllib.error
import urllib.parse
import urllib.request
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

USER_AGENT = "stanley-druckenmiller-workflow/2.0"
MASSIVE_BASE_URL = "https://api.massive.com"
MASSIVE_API_KEY = os.environ.get("MASSIVE_API_KEY", "").strip()
POLYGON_BASE_URL = "https://api.polygon.io"
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "").strip()

POLYGON_SYMBOL_MAP = {
    "SPY": "SPY",
    "IWM": "IWM",
    "HYG": "HYG",
    "LQD": "LQD",
    "XHB": "XHB",
    "ITB": "ITB",
    "IYT": "IYT",
    "XRT": "XRT",
    "SMH": "SMH",
    "KRE": "KRE",
    "XLY": "XLY",
    "XLP": "XLP",
    "GC=F": "C:XAUUSD",
    "HG=F": "C:XCUUSD",
    "CL=F": "C:XTIUSD",
}

SYMBOLS = {
    "rates": ["^IRX", "^FVX", "^TNX", "^TYX"],
    "fx": ["DX-Y.NYB", "AUDJPY=X"],
    "equities": ["^GSPC", "^IXIC", "^RUT", "^VIX", "SPY", "IWM"],
    "credit": ["HYG", "LQD"],
    "internals": ["XHB", "ITB", "IYT", "XRT", "SMH", "KRE", "XLY", "XLP"],
    "commodities": ["CL=F", "GC=F", "HG=F"],
}

MASSIVE_STOCK_SYMBOLS = {
    "SPY",
    "IWM",
    "HYG",
    "LQD",
    "XHB",
    "ITB",
    "IYT",
    "XRT",
    "SMH",
    "KRE",
    "XLY",
    "XLP",
}

FRED_SERIES = [
    "WALCL",
    "RRPONTSYD",
    "WTREGEN",
    "M2SL",
    "GDP",
    "DGS2",
    "DGS10",
    "T10Y2Y",
    "BAMLH0A0HYM2",
]

FRED_PROXY_MAP = {
    "^IRX": "DGS3MO",
    "^FVX": "DGS5",
    "^TNX": "DGS10",
    "^TYX": "DGS30",
    "^VIX": "VIXCLS",
    "DX-Y.NYB": "DTWEXBGS",
    "CL=F": "DCOILWTICO",
    "GC=F": "GOLDAMGBD228NLBM",
    "HG=F": "PCOPPUSDM",
}

FRED_TO_YAHOO_YIELD = {
    "DGS3MO": "^IRX",
    "DGS5": "^FVX",
    "DGS10": "^TNX",
    "DGS30": "^TYX",
}

MONTHLY_FRED_SERIES = {"PCOPPUSDM", "M2SL", "GDP"}

STOOQ_MAP = {
    "SPY": "spy.us",
    "IWM": "iwm.us",
    "HYG": "hyg.us",
    "LQD": "lqd.us",
    "XHB": "xhb.us",
    "ITB": "itb.us",
    "IYT": "iyt.us",
    "XRT": "xrt.us",
    "SMH": "smh.us",
    "KRE": "kre.us",
    "XLY": "xly.us",
    "XLP": "xlp.us",
    "^GSPC": "^spx",
    "^IXIC": "^ndq",
    "AUDJPY=X": "audjpy",
}

BREADTH_PROXY_UNIVERSE = [
    "SPY",
    "IWM",
    "XHB",
    "ITB",
    "IYT",
    "XRT",
    "SMH",
    "KRE",
    "XLY",
    "XLP",
    "HYG",
    "LQD",
]

REPO_ROOT = Path(__file__).resolve().parents[1]
CACHE_PATH = REPO_ROOT / ".cache" / "market-panels-cache.json"
CACHE_FRESH_HOURS = 96
CACHE_STALE_HOURS = 336


class HttpError(RuntimeError):
    pass


def http_get(url: str, timeout: int = 20, retries: int = 5, backoff: float = 0.6) -> str:
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            last_err = exc
            if exc.code not in (429, 500, 502, 503, 504) or attempt == retries - 1:
                break
            time.sleep(backoff * (2**attempt) + random.uniform(0.05, 0.25))
        except (urllib.error.URLError, TimeoutError) as exc:
            last_err = exc
            if attempt == retries - 1:
                break
            time.sleep(backoff * (2**attempt) + random.uniform(0.05, 0.25))
    raise HttpError(str(last_err))


def http_get_json(url: str, params: dict[str, str] | None = None) -> dict:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    return json.loads(http_get(url))


def compute_changes(points: list[tuple[int, float]]) -> tuple[float | None, float | None, float | None]:
    values = [value for _, value in points]
    latest = values[-1]

    def pct(old: float | None) -> float | None:
        if old in (None, 0):
            return None
        return ((latest - old) / old) * 100.0

    chg_1d = pct(values[-2]) if len(values) >= 2 else None
    chg_5d = pct(values[-6]) if len(values) >= 6 else None
    chg_20d = pct(values[-21]) if len(values) >= 21 else None
    return chg_1d, chg_5d, chg_20d


def massive_series(symbol: str) -> dict:
    if not MASSIVE_API_KEY:
        return {"ok": False, "symbol": symbol, "error": "missing MASSIVE_API_KEY"}
    if symbol not in MASSIVE_STOCK_SYMBOLS:
        return {"ok": False, "symbol": symbol, "error": "Massive not configured for symbol"}

    end_date = date.today()
    start_date = end_date - timedelta(days=400)
    encoded = urllib.parse.quote(symbol, safe="")
    url = f"{MASSIVE_BASE_URL}/v2/aggs/ticker/{encoded}/range/1/day/{start_date.isoformat()}/{end_date.isoformat()}"

    try:
        payload = http_get_json(
            url,
            {
                "adjusted": "true",
                "sort": "asc",
                "limit": "5000",
                "apiKey": MASSIVE_API_KEY,
            },
        )
        results = payload.get("results") or []
        points: list[tuple[int, float]] = []
        for row in results:
            raw_ts = row.get("t", row.get("timestamp"))
            raw_close = row.get("c", row.get("close"))
            if raw_ts is None or raw_close is None:
                continue
            ts = int(raw_ts)
            if ts > 10_000_000_000:
                ts //= 1000
            points.append((ts, float(raw_close)))

        if len(points) < 2:
            return {"ok": False, "symbol": symbol, "url": url, "error": "insufficient Massive data"}

        latest_ts, latest = points[-1]
        chg_1d, chg_5d, chg_20d = compute_changes(points)
        return {
            "ok": True,
            "symbol": symbol,
            "url": url,
            "source": "massive",
            "asof_utc": datetime.fromtimestamp(latest_ts, tz=timezone.utc).isoformat(),
            "latest": latest,
            "chg_1d_pct": chg_1d,
            "chg_5d_pct": chg_5d,
            "chg_20d_pct": chg_20d,
            "series_close": [value for _, value in points],
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "symbol": symbol, "url": url, "error": str(exc)}


def polygon_series(symbol: str) -> dict:
    if not POLYGON_API_KEY:
        return {"ok": False, "symbol": symbol, "error": "missing POLYGON_API_KEY"}
    polygon_symbol = POLYGON_SYMBOL_MAP.get(symbol)
    if not polygon_symbol:
        return {"ok": False, "symbol": symbol, "error": "Polygon mapping unavailable"}

    end_date = date.today()
    start_date = end_date - timedelta(days=400)
    encoded = urllib.parse.quote(polygon_symbol, safe="")
    url = (
        f"{POLYGON_BASE_URL}/v2/aggs/ticker/{encoded}/range/1/day/"
        f"{start_date.isoformat()}/{end_date.isoformat()}"
    )
    try:
        payload = http_get_json(
            url,
            {
                "adjusted": "true",
                "sort": "asc",
                "limit": "5000",
                "apiKey": POLYGON_API_KEY,
            },
        )
        results = payload.get("results") or []
        points: list[tuple[int, float]] = []
        for row in results:
            raw_ts = row.get("t")
            raw_close = row.get("c")
            if raw_ts is None or raw_close is None:
                continue
            ts = int(raw_ts)
            if ts > 10_000_000_000:
                ts //= 1000
            points.append((ts, float(raw_close)))

        if len(points) < 2:
            return {"ok": False, "symbol": symbol, "url": url, "error": "insufficient Polygon data"}

        latest_ts, latest = points[-1]
        chg_1d, chg_5d, chg_20d = compute_changes(points)
        return {
            "ok": True,
            "symbol": symbol,
            "url": url,
            "source": "polygon",
            "polygon_symbol": polygon_symbol,
            "asof_utc": datetime.fromtimestamp(latest_ts, tz=timezone.utc).isoformat(),
            "latest": latest,
            "chg_1d_pct": chg_1d,
            "chg_5d_pct": chg_5d,
            "chg_20d_pct": chg_20d,
            "series_close": [value for _, value in points],
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "symbol": symbol, "url": url, "error": str(exc)}


def yahoo_series(symbol: str, range_: str = "1y", interval: str = "1d") -> dict:
    encoded = urllib.parse.quote(symbol, safe="")
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}"
        f"?range={range_}&interval={interval}&includePrePost=false"
    )
    try:
        payload = json.loads(http_get(url))
        result = payload["chart"]["result"][0]
        timestamps = result.get("timestamp", [])
        closes = result["indicators"]["quote"][0].get("close", [])
        points = [(int(ts), float(close)) for ts, close in zip(timestamps, closes) if close is not None]
        if len(points) < 2:
            return {"ok": False, "symbol": symbol, "url": url, "error": "insufficient Yahoo data"}

        latest_ts, latest = points[-1]
        chg_1d, chg_5d, chg_20d = compute_changes(points)
        return {
            "ok": True,
            "symbol": symbol,
            "url": url,
            "source": "yahoo",
            "asof_utc": datetime.fromtimestamp(latest_ts, tz=timezone.utc).isoformat(),
            "latest": latest,
            "chg_1d_pct": chg_1d,
            "chg_5d_pct": chg_5d,
            "chg_20d_pct": chg_20d,
            "series_close": [value for _, value in points],
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "symbol": symbol, "url": url, "error": str(exc)}


def stooq_series(symbol: str) -> dict:
    stooq_symbol = STOOQ_MAP.get(symbol)
    if not stooq_symbol:
        return {"ok": False, "symbol": symbol, "error": "Stooq mapping unavailable"}

    url = f"https://stooq.com/q/d/l/?s={stooq_symbol}&i=d"
    try:
        text = http_get(url)
        reader = csv.DictReader(io.StringIO(text))
        points: list[tuple[int, float]] = []
        for row in reader:
            raw_date = (row.get("Date") or "").strip()
            raw_close = (row.get("Close") or "").strip()
            if not raw_date or not raw_close:
                continue
            try:
                ts = int(datetime.fromisoformat(raw_date).replace(tzinfo=timezone.utc).timestamp())
                points.append((ts, float(raw_close)))
            except ValueError:
                continue

        if len(points) < 2:
            return {"ok": False, "symbol": symbol, "url": url, "error": "insufficient Stooq data"}

        latest_ts, latest = points[-1]
        chg_1d, chg_5d, chg_20d = compute_changes(points)
        return {
            "ok": True,
            "symbol": symbol,
            "url": url,
            "source": "stooq",
            "asof_utc": datetime.fromtimestamp(latest_ts, tz=timezone.utc).isoformat(),
            "latest": latest,
            "chg_1d_pct": chg_1d,
            "chg_5d_pct": chg_5d,
            "chg_20d_pct": chg_20d,
            "series_close": [value for _, value in points],
            "stooq_symbol": stooq_symbol,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "symbol": symbol, "url": url, "error": str(exc)}


def fred_points(series_id: str) -> list[tuple[int, float]]:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    text = http_get(url)
    reader = csv.DictReader(io.StringIO(text))
    fields = reader.fieldnames or []
    if len(fields) < 2:
        print(
            f"[fred_points] {series_id}: unexpected CSV header fields={fields!r} "
            f"sample={text[:200]!r}",
            flush=True,
        )
        return []

    out: list[tuple[int, float]] = []
    skipped_tail: list[tuple[str, str]] = []
    for row in reader:
        raw_date = (row.get(fields[0]) or "").strip()
        raw_value = (row.get(fields[1]) or "").strip()
        if not raw_date or not raw_value or raw_value == ".":
            skipped_tail.append((raw_date, raw_value))
            if len(skipped_tail) > 5:
                skipped_tail.pop(0)
            continue
        try:
            ts = int(datetime.fromisoformat(raw_date).replace(tzinfo=timezone.utc).timestamp())
            out.append((ts, float(raw_value)))
        except ValueError:
            continue
    if not out:
        print(
            f"[fred_points] {series_id}: no numeric values parsed "
            f"(last-skipped={skipped_tail!r})",
            flush=True,
        )
    return out


def yahoo_yield_summary(fred_series_id: str) -> dict:
    """Fetch a Treasury yield via Yahoo's CBOE rate index and return in fred_summary shape.

    CBOE indices (^TNX, ^FVX, etc.) are historically quoted as yield*10; we defensively
    rescale when the latest value would imply an unrealistic > 20% rate.
    """
    yahoo_symbol = FRED_TO_YAHOO_YIELD.get(fred_series_id)
    if not yahoo_symbol:
        return {"ok": False, "series": fred_series_id, "error": "no yahoo yield mapping"}

    series = yahoo_series(yahoo_symbol)
    if not series.get("ok"):
        return {
            "ok": False,
            "series": fred_series_id,
            "error": f"yahoo {yahoo_symbol}: {series.get('error', 'unknown')}",
        }

    closes = series.get("series_close") or []
    if len(closes) < 2:
        return {"ok": False, "series": fred_series_id, "error": "insufficient yahoo yield data"}

    scale = 0.1 if closes[-1] > 20 else 1.0
    values = [v * scale for v in closes]
    latest = values[-1]
    chg_1d = latest - values[-2] if len(values) >= 2 else None
    chg_5d = latest - values[-6] if len(values) >= 6 else None
    chg_20d = latest - values[-21] if len(values) >= 21 else None

    asof = series.get("asof_utc", datetime.now(timezone.utc).isoformat())
    try:
        asof_date = datetime.fromisoformat(asof.replace("Z", "+00:00")).date().isoformat()
    except Exception:  # noqa: BLE001
        asof_date = datetime.now(timezone.utc).date().isoformat()

    return {
        "ok": True,
        "series": fred_series_id,
        "date": asof_date,
        "value": latest,
        "chg_1d_abs": chg_1d,
        "chg_5d_abs": chg_5d,
        "chg_20d_abs": chg_20d,
        "url": series.get("url", ""),
        "source": f"yahoo-yield:{yahoo_symbol}",
    }


def fred_proxy_series(symbol: str) -> dict:
    if symbol == "AUDJPY=X":
        try:
            jpy = fred_points("DEXJPUS")
            aud = fred_points("DEXUSAL")
            aud_by_ts = {ts: value for ts, value in aud}
            points = [(ts, jpy_value * aud_by_ts[ts]) for ts, jpy_value in jpy if ts in aud_by_ts]
            if len(points) < 2:
                return {"ok": False, "symbol": symbol, "error": "insufficient FRED FX proxy data"}
            latest_ts, latest = points[-1]
            chg_1d, chg_5d, chg_20d = compute_changes(points)
            return {
                "ok": True,
                "symbol": symbol,
                "url": "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXJPUS",
                "source": "fred-proxy",
                "asof_utc": datetime.fromtimestamp(latest_ts, tz=timezone.utc).isoformat(),
                "latest": latest,
                "chg_1d_pct": chg_1d,
                "chg_5d_pct": chg_5d,
                "chg_20d_pct": chg_20d,
                "series_close": [value for _, value in points],
                "proxy_series": ["DEXJPUS", "DEXUSAL"],
            }
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "symbol": symbol, "error": str(exc)}

    series_id = FRED_PROXY_MAP.get(symbol)
    if not series_id:
        return {"ok": False, "symbol": symbol, "error": "FRED proxy mapping unavailable"}

    try:
        points = fred_points(series_id)
        if len(points) < 2:
            return {"ok": False, "symbol": symbol, "error": "insufficient FRED proxy data"}

        latest_ts, latest = points[-1]
        is_monthly = series_id in MONTHLY_FRED_SERIES
        if is_monthly:
            chg_1d, chg_5d, chg_20d = None, None, None
        else:
            chg_1d, chg_5d, chg_20d = compute_changes(points)
        return {
            "ok": True,
            "symbol": symbol,
            "url": f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}",
            "source": "fred-proxy",
            "asof_utc": datetime.fromtimestamp(latest_ts, tz=timezone.utc).isoformat(),
            "latest": latest,
            "chg_1d_pct": chg_1d,
            "chg_5d_pct": chg_5d,
            "chg_20d_pct": chg_20d,
            "series_close": [value for _, value in points],
            "proxy_series": [series_id],
            "proxy_is_monthly": is_monthly,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "symbol": symbol, "error": str(exc)}


def fred_summary(series_id: str) -> dict:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    fred_err: str | None = None
    try:
        points = fred_points(series_id)
        if points:
            latest_ts, latest = points[-1]
            values = [value for _, value in points]
            chg_1d = latest - values[-2] if len(values) >= 2 else None
            chg_5d = latest - values[-6] if len(values) >= 6 else None
            chg_20d = latest - values[-21] if len(values) >= 21 else None
            return {
                "ok": True,
                "series": series_id,
                "date": datetime.fromtimestamp(latest_ts, tz=timezone.utc).date().isoformat(),
                "value": latest,
                "chg_1d_abs": chg_1d,
                "chg_5d_abs": chg_5d,
                "chg_20d_abs": chg_20d,
                "url": url,
                "source": "fred",
            }
        fred_err = "no numeric value"
    except Exception as exc:  # noqa: BLE001
        fred_err = str(exc)

    if series_id in FRED_TO_YAHOO_YIELD:
        yahoo = yahoo_yield_summary(series_id)
        if yahoo.get("ok"):
            yahoo["url"] = yahoo.get("url") or url
            yahoo["fred_error"] = fred_err
            return yahoo

    return {"ok": False, "series": series_id, "url": url, "error": fred_err or "unknown"}


def load_cache() -> dict[str, dict]:
    if not CACHE_PATH.exists():
        return {}
    try:
        payload = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        symbols = payload.get("symbols")
        if isinstance(symbols, dict):
            return symbols
    except Exception:
        return {}
    return {}


def cache_age_hours(asof_utc: str) -> float | None:
    try:
        ts = datetime.fromisoformat(asof_utc.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds() / 3600.0
    except Exception:
        return None


def cache_series(symbol: str, cache: dict[str, dict]) -> dict:
    item = deepcopy(cache.get(symbol) or {})
    if not item:
        return {"ok": False, "symbol": symbol, "error": "cache miss"}
    asof = item.get("asof_utc")
    if not asof:
        return {"ok": False, "symbol": symbol, "error": "cache missing asof_utc"}
    age_hours = cache_age_hours(asof)
    if age_hours is None or age_hours > CACHE_STALE_HOURS:
        return {"ok": False, "symbol": symbol, "error": "cache too old"}
    item["ok"] = True
    item["symbol"] = symbol
    item["source"] = f"cache:{item.get('source', 'unknown')}"
    item["cache_age_hours"] = round(age_hours, 2)
    item["stale"] = age_hours > CACHE_FRESH_HOURS
    return item


def save_cache(flat: dict[str, dict]) -> None:
    symbols: dict[str, dict] = {}
    for symbol, item in flat.items():
        if item.get("ok") and item.get("series_close"):
            symbols[symbol] = deepcopy(item)
    if not symbols:
        return
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "symbols": symbols,
    }
    CACHE_PATH.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def fetch_symbol_with_fallback(symbol: str, cache: dict[str, dict]) -> dict:
    attempts: list[str] = []

    primary = massive_series(symbol)
    if primary.get("ok"):
        return primary
    attempts.append(f"massive:{primary.get('error', 'unknown')}")

    polygon = polygon_series(symbol)
    if polygon.get("ok"):
        polygon["fallback_chain"] = attempts
        return polygon
    attempts.append(f"polygon:{polygon.get('error', 'unknown')}")

    yahoo = yahoo_series(symbol)
    if yahoo.get("ok"):
        yahoo["fallback_chain"] = attempts
        return yahoo
    attempts.append(f"yahoo:{yahoo.get('error', 'unknown')}")

    stooq = stooq_series(symbol)
    if stooq.get("ok"):
        stooq["fallback_chain"] = attempts
        return stooq
    attempts.append(f"stooq:{stooq.get('error', 'unknown')}")

    fred_proxy = fred_proxy_series(symbol)
    if fred_proxy.get("ok"):
        fred_proxy["fallback_chain"] = attempts
        return fred_proxy
    attempts.append(f"fred-proxy:{fred_proxy.get('error', 'unknown')}")

    cached = cache_series(symbol, cache)
    if cached.get("ok"):
        cached["fallback_chain"] = attempts
        return cached
    attempts.append(f"cache:{cached.get('error', 'unknown')}")

    return {
        "ok": False,
        "symbol": symbol,
        "url": primary.get("url") or polygon.get("url") or yahoo.get("url"),
        "error": "all sources failed",
        "fallback_chain": attempts,
    }


def safe_latest(flat: dict[str, dict], symbol: str) -> float | None:
    item = flat.get(symbol)
    if not item or not item.get("ok"):
        return None
    return item.get("latest")


def calc_breadth_proxy(flat: dict[str, dict]) -> dict:
    available: list[tuple[str, list[float]]] = []
    for symbol in BREADTH_PROXY_UNIVERSE:
        item = flat.get(symbol)
        closes = (item or {}).get("series_close") or []
        if item and item.get("ok") and len(closes) >= 210:
            available.append((symbol, closes))

    if not available:
        return {
            "ok": False,
            "error": "insufficient proxy breadth history",
            "universe": BREADTH_PROXY_UNIVERSE,
        }

    advancers = 0
    decliners = 0
    above_200d = 0
    new_highs_20d = 0
    new_lows_20d = 0

    for _, closes in available:
        latest = closes[-1]
        previous = closes[-2]
        ma200 = sum(closes[-200:]) / 200.0
        if latest > previous:
            advancers += 1
        elif latest < previous:
            decliners += 1
        if latest > ma200:
            above_200d += 1
        if latest >= max(closes[-20:]):
            new_highs_20d += 1
        if latest <= min(closes[-20:]):
            new_lows_20d += 1

    total = len(available)
    return {
        "ok": True,
        "method": "proxy-universe",
        "universe": [symbol for symbol, _ in available],
        "advancers_1d": advancers,
        "decliners_1d": decliners,
        "ad_line_1d": advancers - decliners,
        "pct_above_200d": (above_200d / total) * 100.0,
        "new_highs_20d": new_highs_20d,
        "new_lows_20d": new_lows_20d,
        "sample_size": total,
    }


def build_snapshot(pause_s: float = 0.25) -> dict:
    fallbacks = []
    if POLYGON_API_KEY:
        fallbacks.append("polygon_aggs")
    fallbacks.extend(["yahoo_chart_api_public", "stooq_csv_public", "fred_proxy_public", "local_cache"])

    out: dict = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "stock_primary": "massive_aggregates" if MASSIVE_API_KEY else (
                "polygon_aggs" if POLYGON_API_KEY else "yahoo_chart_api_public"
            ),
            "fallbacks": fallbacks,
            "macro_primary": "fredgraph_csv_public",
            "macro_fallback": "yahoo_cboe_yield_indices",
            "cache_path": str(CACHE_PATH),
        },
        "panels": {},
        "ratios": {},
        "fred": {},
        "derived": {},
    }

    cache = load_cache()
    flat: dict[str, dict] = {}

    for panel, symbols in SYMBOLS.items():
        bucket: dict[str, dict] = {}
        for symbol in symbols:
            bucket[symbol] = fetch_symbol_with_fallback(symbol, cache)
            flat[symbol] = bucket[symbol]
            time.sleep(pause_s)
        out["panels"][panel] = bucket

    for name, left, right in [
        ("IWM_SPY", "IWM", "SPY"),
        ("XLY_XLP", "XLY", "XLP"),
        ("XHB_SPY", "XHB", "SPY"),
        ("SMH_SPY", "SMH", "SPY"),
        ("KRE_SPY", "KRE", "SPY"),
        ("COPPER_GOLD", "HG=F", "GC=F"),
    ]:
        left_value = safe_latest(flat, left)
        right_value = safe_latest(flat, right)
        if left_value is None or right_value in (None, 0):
            out["ratios"][name] = {"ok": False, "a": left, "b": right}
        else:
            out["ratios"][name] = {"ok": True, "a": left, "b": right, "value": left_value / right_value}

    out["panels"]["breadth_proxy"] = calc_breadth_proxy(flat)
    save_cache(flat)

    for item in flat.values():
        item.pop("series_close", None)

    for series_id in FRED_SERIES:
        out["fred"][series_id] = fred_summary(series_id)
        time.sleep(0.1)

    fred = out["fred"]

    dgs2 = fred.get("DGS2", {})
    dgs10 = fred.get("DGS10", {})
    t10y2y = fred.get("T10Y2Y", {})
    if (
        not dgs2.get("ok")
        and dgs10.get("ok")
        and t10y2y.get("ok")
        and isinstance(dgs10.get("value"), (int, float))
        and isinstance(t10y2y.get("value"), (int, float))
    ):
        derived_value = dgs10["value"] - t10y2y["value"]
        def _derive(key: str) -> float | None:
            a = dgs10.get(key)
            b = t10y2y.get(key)
            if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                return a - b
            return None
        fred["DGS2"] = {
            "ok": True,
            "series": "DGS2",
            "date": dgs10.get("date") or t10y2y.get("date"),
            "value": derived_value,
            "chg_1d_abs": _derive("chg_1d_abs"),
            "chg_5d_abs": _derive("chg_5d_abs"),
            "chg_20d_abs": _derive("chg_20d_abs"),
            "url": "derived:DGS10-T10Y2Y",
            "source": "derived",
            "derivation": "DGS10 - T10Y2Y",
            "fred_error": dgs2.get("error"),
        }

    if fred.get("WALCL", {}).get("ok") and fred.get("RRPONTSYD", {}).get("ok") and fred.get("WTREGEN", {}).get("ok"):
        out["derived"]["net_liquidity_proxy_WALCL_minus_RRP_minus_TGA_mn_usd"] = (
            fred["WALCL"]["value"] - fred["RRPONTSYD"]["value"] - fred["WTREGEN"]["value"]
        )
    if fred.get("DGS10", {}).get("ok") and fred.get("DGS2", {}).get("ok"):
        out["derived"]["curve_10y_minus_2y_pct_pts"] = fred["DGS10"]["value"] - fred["DGS2"]["value"]
    if fred.get("M2SL", {}).get("ok") and fred.get("GDP", {}).get("ok") and fred["GDP"]["value"] != 0:
        out["derived"]["m2_to_gdp_ratio"] = fred["M2SL"]["value"] / fred["GDP"]["value"]

    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the morning market panel snapshot")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    parser.add_argument("--output", help="Write output JSON to a file path")
    parser.add_argument("--pause", type=float, default=0.25, help="Pause seconds between symbol calls")
    args = parser.parse_args()

    data = build_snapshot(pause_s=args.pause)
    text = json.dumps(data, ensure_ascii=False, indent=2 if args.pretty else None)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(text + ("\n" if not text.endswith("\n") else ""), encoding="utf-8")

    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
