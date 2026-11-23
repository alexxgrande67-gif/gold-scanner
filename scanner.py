
#!/usr/bin/env python3
"""GOLD SCANNER scheduled backend. Optimized OKX/Gate.io scanner."""

from __future__ import annotations

import json
import html
import math
import os
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter


# ============================================================
# CONFIGURATION
# ============================================================

OKX = "https://www.okx.com"
GATE = "https://api.gateio.ws/api/v4"

STATE_FILE = Path("state.json")

# Daily candle cache
DAILY_CACHE_FILE = Path("daily_cache.json")
DAILY_CACHE_LIMIT = 700
DAILY_CACHE_UPDATE = 30

_daily_cache = {}
_daily_cache_lock = threading.Lock()
_daily_cache_exchange = None
_daily_cache_saved_date = None
_daily_cache_force_save = False
_daily_cache_new_symbol = False

# False-breakout parameters
FB = {
    "swing": 1,
    "minSwingAtr": 1,
    "mergeAtr": 0.5,
    "leaveAtr": 0.5
}

# Minimum history required by the existing signal engine.
# fb_compute() requires at least swing * 2 + 20 candles.
MIN_DAILY_CANDLES = max(
    FB["swing"] * 2 + 20,
    15
)

MIN_5M_CANDLES = 30

TOP_UNIVERSE = 500
MAX_MONITORED = int(os.getenv("MAX_MONITORED", "500"))

# Parallel workers. Keep this moderate to avoid exchange limits.
SCAN_WORKERS = max(
    1,
    min(int(os.getenv("SCAN_WORKERS", "5")), 10)
)

HTTP_RETRIES = 3
HTTP_TIMEOUT = 25

# Minimum spacing between outgoing market-data requests.
REQUEST_INTERVAL = 0.06

EXCLUDE_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "EUR", "GBP",
    "AEUR", "BFUSD", "USDE", "USD0", "PYUSD"
}

ACTIVE_EXCHANGE = "okx"

_thread_local = threading.local()
_request_lock = threading.Lock()
_last_request_time = 0.0


# ============================================================
# HTTP
# ============================================================

def get_session():
    """Return one requests session per worker thread."""

    session = getattr(_thread_local, "session", None)

    if session is None:
        session = requests.Session()

        session.headers.update({
            "User-Agent": "GOLD-SCANNER/1.0",
            "Accept": "application/json"
        })

        adapter = HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=0
        )

        session.mount("https://", adapter)
        session.mount("http://", adapter)

        _thread_local.session = session

    return session


def request_spacing():
    """Apply a small global delay between market-data requests."""

    global _last_request_time

    with _request_lock:
        now = time.monotonic()
        wait = REQUEST_INTERVAL - (now - _last_request_time)

        if wait > 0:
            time.sleep(wait)

        _last_request_time = time.monotonic()


def get_json(url: str, params: dict | None = None, timeout: int = HTTP_TIMEOUT):
    """GET JSON with retry handling for transient network errors."""

    last_error = None

    for attempt in range(1, HTTP_RETRIES + 1):
        try:
            request_spacing()

            response = get_session().get(
                url,
                params=params,
                timeout=timeout
            )

            response.raise_for_status()
            return response.json()

        except (
            requests.RequestException,
            ValueError
        ) as exc:
            last_error = exc

            if attempt < HTTP_RETRIES:
                delay = min(1.0 * attempt, 3.0)
                time.sleep(delay)

    raise RuntimeError(
        f"HTTP request failed after {HTTP_RETRIES} attempts: "
        f"{type(last_error).__name__}: {last_error}"
    )


# ============================================================
# EXCHANGE CANDLES
# ============================================================

def _okx_rows(inst_id: str, bar: str, limit: int):
    rows_by_ts = {}
    cursor = None

    while len(rows_by_ts) < limit:
        count = min(300, limit - len(rows_by_ts))

        params = {
            "instId": inst_id,
            "bar": bar,
            "limit": str(count)
        }

        if cursor is not None:
            params["after"] = cursor

        data = get_json(
            OKX + "/api/v5/market/history-candles",
            params
        )

        if data.get("code") != "0":
            raise RuntimeError(
                f"OKX candles {inst_id}: {data.get('msg')}"
            )

        batch = data.get("data", [])

        if not batch:
            break

        for row in batch:
            rows_by_ts[int(row[0])] = row

        oldest = min(int(row[0]) for row in batch)

        if cursor == str(oldest) or len(batch) < 2:
            break

        cursor = str(oldest)

        if len(rows_by_ts) >= limit:
            break

        time.sleep(0.04)

    return [
        rows_by_ts[k]
        for k in sorted(rows_by_ts)
    ][-limit:]


def _gate_rows(contract: str, interval: str, limit: int):
    params = {
        "contract": contract,
        "interval": interval,
        "limit": min(limit, 2000)
    }

    rows = get_json(
        GATE + "/futures/usdt/candlesticks",
        params
    )

    return rows[-limit:]


# ============================================================
# SYMBOL NORMALIZATION
# ============================================================

def _canonical_from_okx(inst_id: str):
    return (
        inst_id[:-10] + "USDT"
        if inst_id.endswith("-USDT-SWAP")
        else None
    )


def _canonical_from_gate(name: str):
    return (
        name.replace("_", "")
        if name.endswith("_USDT")
        else None
    )


def _base(symbol: str):
    return (
        symbol[:-4]
        if symbol.endswith("USDT")
        else symbol
    )


# ============================================================
# MARKET UNIVERSE
# ============================================================

def _okx_universe():
    instruments = get_json(
        OKX + "/api/v5/public/instruments",
        {"instType": "SWAP"}
    )

    if instruments.get("code") != "0":
        raise RuntimeError(
            "OKX instruments: " + str(instruments.get("msg"))
        )

    supported = {}

    for item in instruments.get("data", []):
        inst_id = item.get("instId", "")

        if (
            item.get("state") == "live"
            and item.get("settleCcy") == "USDT"
            and inst_id.endswith("-USDT-SWAP")
        ):
            sym = _canonical_from_okx(inst_id)

            if sym and _base(sym) not in EXCLUDE_BASES:
                supported[sym] = inst_id

    tickers = get_json(
        OKX + "/api/v5/market/tickers",
        {"instType": "SWAP"}
    )

    if tickers.get("code") != "0":
        raise RuntimeError(
            "OKX tickers: " + str(tickers.get("msg"))
        )

    items = []

    for ticker in tickers.get("data", []):
        inst_id = ticker.get("instId", "")
        sym = _canonical_from_okx(inst_id)

        if not sym or sym not in supported:
            continue

        try:
            turnover = (
                float(ticker.get("volCcy24h") or 0)
                * float(ticker.get("last") or 0)
            )
        except (TypeError, ValueError):
            continue

        if turnover > 0:
            items.append((sym, turnover))

    items.sort(
        key=lambda x: x[1],
        reverse=True
    )

    return items[:TOP_UNIVERSE]


def _gate_universe():
    contracts = get_json(
        GATE + "/futures/usdt/contracts"
    )

    supported = {
        _canonical_from_gate(x.get("name", ""))
        for x in contracts
        if x.get("in_delisting") is False
    }

    tickers = get_json(
        GATE + "/futures/usdt/tickers"
    )

    items = []

    for ticker in tickers:
        sym = _canonical_from_gate(
            ticker.get("contract", "")
        )

        if (
            not sym
            or sym not in supported
            or _base(sym) in EXCLUDE_BASES
        ):
            continue

        try:
            turnover = float(
                ticker.get("volume_24h_quote") or 0
            )
        except (TypeError, ValueError):
            continue

        if turnover > 0:
            items.append((sym, turnover))

    items.sort(
        key=lambda x: x[1],
        reverse=True
    )

    return items[:TOP_UNIVERSE]


def build_stable_universe():
    global ACTIVE_EXCHANGE

    try:
        items = _okx_universe()
        ACTIVE_EXCHANGE = "okx"

    except Exception as exc:
        print(
            f"[WARN] OKX universe unavailable; "
            f"using Gate.io: {type(exc).__name__}: {exc}",
            flush=True
        )

        items = _gate_universe()
        ACTIVE_EXCHANGE = "gate"

    selected = items[:MAX_MONITORED]

    print(
        f"[INFO] ATR filter disabled. "
        f"BTC stability filter disabled. "
        f"Top universe={len(items)}; "
        f"monitoring={len(selected)}; "
        f"workers={SCAN_WORKERS}; "
        f"exchange={ACTIVE_EXCHANGE}",
        flush=True
    )

    return [
        (sym, quote_volume, None)
        for sym, quote_volume in selected
    ]


# ============================================================
# INDICATOR HELPERS
# ============================================================

def daily_returns(klines):
    out = {}

    for i in range(1, max(1, len(klines) - 1)):
        prev = float(klines[i - 1][4])
        close = float(klines[i][4])

        if prev > 0:
            out[int(klines[i][0])] = close / prev - 1

    return out


def stdev(values):
    if not values:
        return 0.0

    m = sum(values) / len(values)

    return math.sqrt(
        sum((x - m) ** 2 for x in values) / len(values)
    )


def corr(a, b):
    if not a or len(a) != len(b):
        return 0.0

    ma = sum(a) / len(a)
    mb = sum(b) / len(b)

    num = sum(
        (x - ma) * (y - mb)
        for x, y in zip(a, b)
    )

    da = sum((x - ma) ** 2 for x in a)
    db = sum((y - mb) ** 2 for y in b)

    return (
        num / math.sqrt(da * db)
        if da and db
        else 0.0
    )


def wilder_atr(highs, lows, closes, period=14):
    trs = []

    for i in range(1, len(highs)):
        trs.append(
            max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1])
            )
        )

    if len(trs) < period:
        return None

    atr = sum(trs[:period]) / period

    for tr in trs[period:]:
        atr = (
            (atr * (period - 1) + tr) / period
        )

    return atr


# ============================================================
# CANDLE DATA
# ============================================================

def _normalise_rows(rows, source):
    out = []

    for row in rows:
        if source == "okx":
            ts, op, hi, lo, cl, vol = row[:6]

        else:
            ts, op, hi, lo, cl, vol = (
                row["t"],
                row["o"],
                row["h"],
                row["l"],
                row["c"],
                row.get("sum", row.get("v", 0))
            )

        out.append([
            int(ts) if source == "gate" else int(ts) // 1000,
            float(op),
            float(hi),
            float(lo),
            float(cl),
            float(vol)
        ])

    return out


def get_bars(exchange, symbol, interval, limit):
    if exchange == "okx":
        inst_id = symbol.replace(
            "USDT",
            "-USDT-SWAP"
        )

        bar = (
            "5m"
            if interval == "5m"
            else "1D"
        )

        rows = _okx_rows(
            inst_id,
            bar,
            limit
        )

        return _normalise_rows(rows, "okx")

    contract = _base(symbol) + "_USDT"

    gate_interval = (
        "5m"
        if interval == "5m"
        else "1d"
    )

    rows = _gate_rows(
        contract,
        gate_interval,
        limit
    )

    return _normalise_rows(rows, "gate")


# ============================================================
# DAILY CANDLE CACHE
# ============================================================

def load_daily_cache(exchange):
    """
    Load persistent daily candle cache.

    Short histories are valid cache entries too.
    Even a symbol with fewer than MIN_DAILY_CANDLES
    is retained to avoid downloading the same history
    from scratch on every run.
    """

    global _daily_cache
    global _daily_cache_exchange
    global _daily_cache_saved_date
    global _daily_cache_force_save

    today = time.strftime(
        "%Y-%m-%d",
        time.gmtime()
    )

    _daily_cache = {}
    _daily_cache_exchange = exchange
    _daily_cache_saved_date = None
    _daily_cache_force_save = False

    if not DAILY_CACHE_FILE.exists():
        print(
            "[CACHE] Daily cache not found. "
            "Full history will be downloaded.",
            flush=True
        )

        _daily_cache_force_save = True
        return

    try:
        data = json.loads(
            DAILY_CACHE_FILE.read_text()
        )

        if data.get("version") != 1:
            raise ValueError("Unsupported cache version")

        if data.get("exchange") != exchange:
            raise ValueError(
                "Cached exchange does not match active exchange"
            )

        symbols = data.get("symbols", {})

        if not isinstance(symbols, dict):
            raise ValueError("Invalid symbols structure")

        valid_symbols = {}

        for symbol, rows in symbols.items():
            if not isinstance(rows, list):
                continue

            valid_rows = []

            for row in rows:
                if not isinstance(row, list) or len(row) != 6:
                    continue

                try:
                    valid_rows.append([
                        int(row[0]),
                        float(row[1]),
                        float(row[2]),
                        float(row[3]),
                        float(row[4]),
                        float(row[5])
                    ])
                except (TypeError, ValueError, OverflowError):
                    continue

            # Remove duplicate timestamps and keep newest 700.
            unique_rows = {
                row[0]: row
                for row in valid_rows
            }

            valid_rows = [
                unique_rows[ts]
                for ts in sorted(unique_rows)
            ][-DAILY_CACHE_LIMIT:]

            # Keep every non-empty history, including short ones.
            if valid_rows:
                valid_symbols[symbol] = valid_rows

        _daily_cache = valid_symbols
        _daily_cache_saved_date = data.get("saved_utc_date")

        print(
            f"[CACHE] Loaded {len(_daily_cache)} symbols; "
            f"saved date={_daily_cache_saved_date}",
            flush=True
        )

        if _daily_cache_saved_date != today:
            print(
                "[CACHE] New UTC day detected. "
                "Cache will be persisted after this scan.",
                flush=True
            )

    except Exception as exc:
        print(
            f"[CACHE] Cannot read daily cache: "
            f"{type(exc).__name__}: {exc}. "
            f"Full history will be downloaded.",
            flush=True
        )

        _daily_cache = {}
        _daily_cache_saved_date = None
        _daily_cache_force_save = True


def get_daily_bars(exchange, symbol):
    """
    Return up to 700 daily candles.

    New symbol:
        Download up to 700 candles.

    Cached symbol:
        Download the latest 30 candles and merge them
        with cached history.

    Short histories are retained and returned.
    The caller decides whether there are enough candles
    for the signal engine.
    """

    global _daily_cache_new_symbol

    with _daily_cache_lock:
        cached = _daily_cache.get(symbol)

        if cached:
            cached = list(cached)

    if cached:
        fetch_limit = DAILY_CACHE_UPDATE

        print(
            f"[CACHE] {symbol}: updating with "
            f"{fetch_limit} daily candles "
            f"(cached={len(cached)})",
            flush=True
        )

    else:
        fetch_limit = DAILY_CACHE_LIMIT

        print(
            f"[CACHE] {symbol}: downloading full "
            f"{fetch_limit}-candle history",
            flush=True
        )

    fresh_rows = get_bars(
        exchange,
        symbol,
        "1d",
        fetch_limit
    )

    if not fresh_rows:
        raise RuntimeError(
            f"No daily candles received for {symbol}"
        )

    # Merge by timestamp. Fresh data replaces old data
    # when the timestamp already exists. This also refreshes
    # the currently forming daily candle.
    merged = {}

    if cached:
        for row in cached:
            merged[int(row[0])] = row

    for row in fresh_rows:
        merged[int(row[0])] = row

    result = [
        merged[ts]
        for ts in sorted(merged)
    ][-DAILY_CACHE_LIMIT:]

    # Save the available history even when it is too short
    # for the signal engine.
    with _daily_cache_lock:
        _daily_cache[symbol] = result

        if not cached:
            _daily_cache_new_symbol = True

    # Report the actual beginning of available history.
    if len(result) < DAILY_CACHE_LIMIT:
        first_timestamp = result[0][0]

        first_date = time.strftime(
            "%Y-%m-%d",
            time.gmtime(first_timestamp)
        )

        age_days = max(
            0,
            int(
                (time.time() - first_timestamp) / 86400
            )
        )

        if len(result) < MIN_DAILY_CANDLES:
            status = (
                f"insufficient for signal calculation; "
                f"minimum={MIN_DAILY_CANDLES}"
            )
        else:
            status = (
                "enough for signal calculation; "
                "using available history"
            )

        print(
            f"[HISTORY] {symbol}: available={len(result)}/"
            f"{DAILY_CACHE_LIMIT}; "
            f"first candle={first_date}; "
            f"history span≈{age_days} days; "
            f"{status}",
            flush=True
        )

    return result


def save_daily_cache_if_needed():
    """
    Persist cache once per UTC day, on first initialization,
    or when a new symbol has been added.

    The in-memory cache is still refreshed on every scan.
    """

    global _daily_cache_saved_date
    global _daily_cache_force_save
    global _daily_cache_new_symbol

    today = time.strftime(
        "%Y-%m-%d",
        time.gmtime()
    )

    should_save = (
        _daily_cache_force_save
        or _daily_cache_new_symbol
        or _daily_cache_saved_date != today
    )

    if not should_save:
        print(
            "[CACHE] Persistent cache unchanged; "
            "skipping file write.",
            flush=True
        )
        return

    with _daily_cache_lock:
        data = {
            "version": 1,
            "exchange": _daily_cache_exchange,
            "saved_utc_date": today,
            "symbols": _daily_cache
        }

        temp_file = DAILY_CACHE_FILE.with_suffix(".tmp")

        temp_file.write_text(
            json.dumps(
                data,
                separators=(",", ":"),
                ensure_ascii=False
            )
        )

        temp_file.replace(DAILY_CACHE_FILE)

        symbol_count = len(_daily_cache)

    _daily_cache_saved_date = today
    _daily_cache_force_save = False
    _daily_cache_new_symbol = False

    size_mb = DAILY_CACHE_FILE.stat().st_size / (1024 * 1024)

    print(
        f"[CACHE] Saved {symbol_count} symbols; "
        f"file size={size_mb:.2f} MB; "
        f"UTC date={today}",
        flush=True
    )


# ============================================================
# FALSE BREAKOUT ENGINE
# ============================================================

def atr_series(days, period=14):
    out = [None] * len(days)
    prev = 0.0
    total = 0.0

    for i, d in enumerate(days):
        tr = (
            d["high"] - d["low"]
            if i == 0
            else max(
                d["high"] - d["low"],
                abs(d["high"] - days[i - 1]["close"]),
                abs(d["low"] - days[i - 1]["close"])
            )
        )

        if i < period:
            total += tr

            if i == period - 1:
                prev = total / period
                out[i] = prev

        else:
            prev = (
                prev * (period - 1) + tr
            ) / period

            out[i] = prev

    return out


def fb_events(days):
    n = len(days)
    L = FB["swing"]

    atr = atr_series(days, 14)
    ev = [None] * n

    for p in range(L, n - L - 1):
        c = p + L
        i = c + 1

        hp = days[p]["high"]
        lp = days[p]["low"]
        a = atr[p]

        if a is None:
            continue

        is_h = True
        is_l = True

        sw_lo = float("inf")
        sw_hi = float("-inf")

        max_a = float("-inf")
        min_a = float("inf")

        for k in range(p - L, c + 1):
            b = days[k]

            sw_lo = min(sw_lo, b["low"])
            sw_hi = max(sw_hi, b["high"])

            if k < p:
                if b["high"] >= hp:
                    is_h = False

                if b["low"] <= lp:
                    is_l = False

            elif k > p:
                if b["high"] > hp:
                    is_h = False

                if b["low"] < lp:
                    is_l = False

                max_a = max(max_a, b["high"])
                min_a = min(min_a, b["low"])

        strong_h = (
            is_h
            and hp - sw_lo >= a * FB["minSwingAtr"]
            and max_a < hp
        )

        strong_l = (
            is_l
            and sw_hi - lp >= a * FB["minSwingAtr"]
            and min_a > lp
        )

        if not strong_h and not strong_l:
            continue

        if ev[i] is None:
            ev[i] = {
                "atr": atr[c],
                "h": [],
                "l": []
            }

        if strong_h:
            ev[i]["h"].append({
                "price": hp,
                "time": days[p]["time"]
            })

        if strong_l:
            ev[i]["l"].append({
                "price": lp,
                "time": days[p]["time"]
            })

    return ev, atr


def fb_compute(days, m5, now_ms=None):
    n = len(days)
    L = FB["swing"]

    if n < L * 2 + 20:
        return {
            "levels": [],
            "events": []
        }

    ev, atr = fb_events(days)

    H = []
    Lo = []

    def add(arr, side, it, a):
        if any(
            abs(x["price"] - it["price"])
            <= a * FB["mergeAtr"]
            for x in arr
        ):
            return

        arr.append({
            "side": side,
            "price": it["price"],
            "time": it["time"],
            "st": 0,
            "touch": 0
        })

        if len(arr) > 240:
            arr.pop(0)

    def add_day(i):
        e = ev[i]

        if e:
            for x in e["h"]:
                add(H, "H", x, e["atr"] or 0)

            for x in e["l"]:
                add(Lo, "L", x, e["atr"] or 0)

    if m5:
        ds = (
            (m5[0]["time"] + 86399) // 86400
        ) * 86400

        k = next(
            (
                i for i, d in enumerate(days)
                if d["time"] >= ds
            ),
            -1
        )

        i_start = k if k >= 0 else n

    else:
        i_start = n

    if i_start > n - 1:
        i_start = n - 1

    for i in range(i_start):
        add_day(i)

        H = [
            x for x in H
            if days[i]["high"] < x["price"]
        ]

        Lo = [
            x for x in Lo
            if days[i]["low"] > x["price"]
        ]

    events = []
    cur = i_start

    add_day(cur)

    now_sec = (
        (now_ms or int(time.time() * 1000)) / 1000
    )

    for bi, b in enumerate(m5):
        if b["time"] < days[i_start]["time"]:
            continue

        while (
            cur + 1 < n
            and b["time"] >= days[cur + 1]["time"]
        ):
            cur += 1
            add_day(cur)

        closed = (
            bi < len(m5) - 1
            or b["time"] + 300 <= now_sec
        )

        D = FB["leaveAtr"] * (
            atr[cur - 1] or atr[cur] or 0
        )

        gone = False

        for x in H:
            if closed and b["close"] > x["price"]:
                x["st"] = 2
                gone = True
                continue

            if closed and b["high"] >= x["price"]:
                x["st"] = 1
                x["touch"] = b["time"]

                events.append({
                    "side": "H",
                    "price": x["price"],
                    "time": x["time"],
                    "touch": b["time"]
                })

                continue

            if (
                x["st"] == 1
                and x["touch"] < b["time"]
                and (
                    b["high"] >= x["price"] + D
                    or b["low"] <= x["price"] - D
                )
            ):
                x["st"] = 2
                gone = True

        for x in Lo:
            if closed and b["close"] < x["price"]:
                x["st"] = 2
                gone = True
                continue

            if closed and b["low"] <= x["price"]:
                x["st"] = 1
                x["touch"] = b["time"]

                events.append({
                    "side": "L",
                    "price": x["price"],
                    "time": x["time"],
                    "touch": b["time"]
                })

                continue

            if (
                x["st"] == 1
                and x["touch"] < b["time"]
                and (
                    b["low"] <= x["price"] - D
                    or b["high"] >= x["price"] + D
                )
            ):
                x["st"] = 2
                gone = True

        if gone:
            H = [
                x for x in H
                if x["st"] != 2
            ]

            Lo = [
                x for x in Lo
                if x["st"] != 2
            ]

    return {
        "levels": H + Lo,
        "events": events
    }


# ============================================================
# STATE
# ============================================================

def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(
                STATE_FILE.read_text()
            )
        except Exception as exc:
            print(
                f"[WARN] Cannot read state.json: {exc}",
                flush=True
            )

    return {
        "primed": False,
        "seen": [],
        "seeded_symbols": []
    }


# ============================================================
# TELEGRAM
# ============================================================

def telegram_post(message, timeout=20):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print(
            "[ERROR] Telegram credentials are missing",
            flush=True
        )
        return False

    url = (
        f"https://api.telegram.org/bot{token}/sendMessage"
    )

    try:
        response = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            },
            timeout=timeout
        )

        response.raise_for_status()
        result = response.json()

        if result.get("ok") is True:
            return True

        print(
            "[ERROR] Telegram API rejected message: "
            + str(result.get("description", "unknown error")),
            flush=True
        )

        return False

    except Exception as exc:
        print(
            f"[ERROR] Telegram delivery failed: "
            f"{type(exc).__name__}: {exc}",
            flush=True
        )

        return False


def send_telegram(sym, event, atr_pct):
    side = (
        "ЛОЖНЫЙ ПРОБОЙ ВВЕРХ 🔴"
        if event["side"] == "H"
        else "ЛОЖНЫЙ ПРОБОЙ ВНИЗ 🟢"
    )

    dt = time.strftime(
        "%d.%m.%Y %H:%M UTC",
        time.gmtime(event["touch"])
    )

    message = (
        "🟡 <b>GOLD SCANNER</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"Монета: <b>{html.escape(sym)}</b>\n"
        f"Сигнал: <b>{side}</b>\n"
        f"Уровень: <code>{event['price']:.12g}</code>\n"
        f"Время свечи: {dt}\n"
        f"ATR: {atr_pct:.2f}%\n"
        f"Биржа: {ACTIVE_EXCHANGE.upper()}\n"
        "━━━━━━━━━━━━━━"
    )

    if telegram_post(message):
        print(
            f"[TELEGRAM] Sent: {sym}",
            flush=True
        )
        return True

    return False


def send_telegram_test():
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print(
            "[TELEGRAM TEST] Missing bot token or chat ID",
            flush=True
        )
        return False

    message = (
        "🟡 <b>GOLD SCANNER — ТЕСТ УВЕДОМЛЕНИЯ</b>\n\n"
        "Telegram успешно подключён!\n"
        "Бот может отправлять уведомления.\n\n"
        "Это проверочное сообщение, не торговый сигнал."
    )

    if telegram_post(message, timeout=15):
        print(
            "[TELEGRAM TEST] Message sent successfully",
            flush=True
        )
        return True

    print(
        "[TELEGRAM TEST] Message was not delivered",
        flush=True
    )

    return False


# ============================================================
# PER-SYMBOL PROCESSING
# ============================================================

def process_symbol(exchange, sym, is_new_symbol, seen_snapshot):
    """
    Process one symbol independently.

    Returns:
        symbol, discovered events, seed keys, success, error
    """

    try:
        days_raw = get_daily_bars(
            exchange,
            sym
        )

        # Short daily history is not a fatal scanner error.
        # It is checked against the actual signal-engine minimum.
        if len(days_raw) < MIN_DAILY_CANDLES:
            return (
                sym,
                [],
                [],
                False,
                f"insufficient daily history "
                f"({len(days_raw)} available; "
                f"{MIN_DAILY_CANDLES} required)"
            )

        m5_raw = get_bars(
            exchange,
            sym,
            "5m",
            1000
        )

        days = [
            {
                "time": int(k[0]),
                "open": k[1],
                "high": k[2],
                "low": k[3],
                "close": k[4],
                "volume": k[5]
            }
            for k in days_raw
        ]

        m5 = [
            {
                "time": int(k[0]),
                "open": k[1],
                "high": k[2],
                "low": k[3],
                "close": k[4],
                "volume": k[5]
            }
            for k in m5_raw
        ]

        if len(m5) < MIN_5M_CANDLES:
            return (
                sym,
                [],
                [],
                False,
                f"insufficient 5m history "
                f"({len(m5)} available; "
                f"{MIN_5M_CANDLES} required)"
            )

        highs = [
            d["high"]
            for d in days[:-1]
        ]

        lows = [
            d["low"]
            for d in days[:-1]
        ]

        closes = [
            d["close"]
            for d in days[:-1]
        ]

        atr_value = wilder_atr(
            highs,
            lows,
            closes,
            14
        )

        last_close = days[-1]["close"]

        atr_pct = (
            atr_value / last_close * 100
            if atr_value and last_close > 0
            else 0.0
        )

        result = fb_compute(days, m5)

        discovered = []
        seed_keys = []

        for event in result["events"]:
            key = (
                f"{sym}|{event['side']}|{event['time']}|"
                f"{event['price']:.12g}|{event['touch']}"
            )

            if is_new_symbol:
                seed_keys.append(key)

            elif key not in seen_snapshot:
                discovered.append(
                    (sym, event, atr_pct, key)
                )

        return (
            sym,
            discovered,
            seed_keys,
            True,
            None
        )

    except Exception as exc:
        return (
            sym,
            [],
            [],
            False,
            f"{type(exc).__name__}: {exc}"
        )


# ============================================================
# MAIN
# ============================================================

def main():
    global ACTIVE_EXCHANGE

    started_at = time.time()

    print(
        "GOLD SCANNER: start",
        flush=True
    )

    print(
        f"[CONFIG] swing={FB['swing']}; "
        f"minSwingAtr={FB['minSwingAtr']}; "
        f"min_daily_candles={MIN_DAILY_CANDLES}; "
        f"min_5m_candles={MIN_5M_CANDLES}; "
        f"max_monitored={MAX_MONITORED}; "
        f"workers={SCAN_WORKERS}; "
        f"http_retries={HTTP_RETRIES}",
        flush=True
    )

    # Test is sent only on manual workflow run
    # when TELEGRAM_TEST=1.
    if os.getenv("TELEGRAM_TEST") == "1":
        test_ok = send_telegram_test()

        if not test_ok:
            raise RuntimeError(
                "Telegram test message was not delivered"
            )

        print(
            "[TELEGRAM TEST] Test completed; continuing scanner.",
            flush=True
        )

    symbols = build_stable_universe()
    exchange = ACTIVE_EXCHANGE

    # Load persistent daily cache before starting workers.
    load_daily_cache(exchange)

    print(
        f"Universe selected: {len(symbols)} symbols; "
        f"monitor cap={MAX_MONITORED}; "
        f"exchange={exchange}",
        flush=True
    )

    if not symbols:
        raise RuntimeError(
            "No symbols available in the selected universe"
        )

    state = load_state()

    seen = set(
        state.get("seen", [])
    )

    seeded = set(
        state.get("seeded_symbols", [])
    )

    # Snapshot is read-only for worker threads.
    seen_snapshot = frozenset(seen)

    discovered = []
    seed_keys = []

    successful_symbols = 0
    failed_symbols = 0

    total = len(symbols)
    completed = 0

    print(
        f"[INFO] Starting scan of {total} symbols "
        f"with {SCAN_WORKERS} workers",
        flush=True
    )

    with ThreadPoolExecutor(
        max_workers=SCAN_WORKERS
    ) as executor:

        futures = {}

        for sym, quote_volume, _ in symbols:
            is_new_symbol = sym not in seeded

            future = executor.submit(
                process_symbol,
                exchange,
                sym,
                is_new_symbol,
                seen_snapshot
            )

            futures[future] = sym

        for future in as_completed(futures):
            completed += 1
            sym = futures[future]

            try:
                (
                    result_sym,
                    symbol_events,
                    symbol_seed_keys,
                    success,
                    error
                ) = future.result()

                if success:
                    successful_symbols += 1

                    discovered.extend(
                        symbol_events
                    )

                    seed_keys.extend(
                        symbol_seed_keys
                    )

                    # Mark only successfully scanned symbols as seeded.
                    seeded.add(result_sym)

                else:
                    failed_symbols += 1

                    print(
                        f"[WARN] {sym}: {error}",
                        flush=True
                    )

            except Exception as exc:
                failed_symbols += 1

                print(
                    f"[WARN] {sym}: worker failed: "
                    f"{type(exc).__name__}: {exc}",
                    flush=True
                )

            if (
                completed % 10 == 0
                or completed == total
            ):
                print(
                    f"Progress: {completed}/{total}; "
                    f"successful={successful_symbols}; "
                    f"failed={failed_symbols}",
                    flush=True
                )

    # Persist daily cache after all worker threads finish.
    save_daily_cache_if_needed()

    if not state.get("primed"):
        state["primed"] = True

        print(
            "Initial run: historical signals seeded; "
            "no old alerts sent.",
            flush=True
        )

    # Historical events of newly monitored symbols are not sent.
    seen.update(seed_keys)

    sent_count = 0
    failed_count = 0

    print(
        f"[INFO] New signal candidates: {len(discovered)}",
        flush=True
    )

    # ========================================================
    # CHRONOLOGICAL SORTING (OLDEST FIRST, NEWEST LAST)
    # ========================================================

    discovered.sort(
        key=lambda item: item[1]["touch"]
    )

    for sym, event, atr_pct, key in discovered:
        side = (
            "ЛОЖНЫЙ ПРОБОЙ ВВЕРХ"
            if event["side"] == "H"
            else "ЛОЖНЫЙ ПРОБОЙ ВНИЗ"
        )

        dt = time.strftime(
            "%Y-%m-%d %H:%M UTC",
            time.gmtime(event["touch"])
        )

        print(
            f"SIGNAL | {sym} | {side} | "
            f"level={event['price']} | "
            f"candle={dt} | "
            f"ATR%={atr_pct:.2f}",
            flush=True
        )

        if send_telegram(sym, event, atr_pct):
            seen.add(key)
            sent_count += 1

        else:
            # Do not add failed notifications to seen.
            # They can be retried on the next scheduled scan.
            failed_count += 1

    state["seeded_symbols"] = sorted(seeded)
    state["seen"] = sorted(seen)[-10000:]

    STATE_FILE.write_text(
        json.dumps(
            state,
            ensure_ascii=False,
            indent=2
        ) + "\n"
    )

    elapsed = time.time() - started_at

    print(
        f"GOLD SCANNER: finished; "
        f"scanned={successful_symbols}; "
        f"symbol errors={failed_symbols}; "
        f"new candidates={len(discovered)}; "
        f"telegram sent={sent_count}; "
        f"telegram failed={failed_count}; "
        f"exchange={exchange}; "
        f"elapsed={elapsed:.1f}s",
        flush=True
    )


if __name__ == "__main__":
    main()
