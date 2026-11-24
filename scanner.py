#!/usr/bin/env python3
"""GOLD SCANNER scheduled backend. Optimized OKX/Gate.io scanner."""

from __future__ import annotations

import json
import html
import math
import os
import time
import threading
from io import BytesIO
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from PIL import Image, ImageDraw, ImageFont


# ============================================================
# CONFIGURATION
# ============================================================

OKX = "https://www.okx.com"
GATE = "https://api.gateio.ws/api/v4"

STATE_FILE = Path("state.json")

# Daily candles cache
DAILY_CACHE_FILE = Path("daily_cache.json")
DAILY_CACHE_VERSION = 1

_daily_cache = {
    "version": DAILY_CACHE_VERSION,
    "exchange": None,
    "symbols": {}
}
_daily_cache_lock = threading.Lock()

# 4H candles cache
FOUR_HOUR_CACHE_FILE = Path("4h_cache.json")
FOUR_HOUR_CACHE_VERSION = 1

_four_hour_cache = {
    "version": FOUR_HOUR_CACHE_VERSION,
    "exchange": None,
    "symbols": {}
}
_four_hour_cache_lock = threading.Lock()

# False-breakout parameters
FB = {
    "swing": 1,
    "minSwingAtr": 1,
    "mergeAtr": 0.5,
    "leaveAtr": 0.5
}

TOP_UNIVERSE = 1000
MAX_MONITORED = int(os.getenv("MAX_MONITORED", "1000"))

SCAN_WORKERS = max(
    1,
    min(int(os.getenv("SCAN_WORKERS", "5")), 10)
)

HTTP_RETRIES = 3
HTTP_TIMEOUT = 25
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
# CHART GENERATION
# ============================================================

CHART_WIDTH = 1400
CHART_HEIGHT = 800

CHART_BACKGROUND = (13, 17, 23)
CHART_GRID = (35, 42, 52)
CHART_UP = (38, 190, 120)
CHART_DOWN = (230, 75, 85)
CHART_LEVEL = (245, 195, 55)
CHART_HIGHLIGHT = (35, 220, 115)


def _chart_font(size):
    """Load a font available on GitHub Actions/Linux."""

    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf"
    ]

    for font_path in candidates:
        try:
            return ImageFont.truetype(font_path, size)
        except OSError:
            continue

    return ImageFont.load_default()


def make_signal_chart(event):
    """
    Create a clean candlestick chart for a signal.

    The chart contains:
    - 60 candles before the signal candle;
    - the signal candle itself;
    - a yellow dashed horizontal level;
    - a green outline around the signal candle.

    No additional captions, indicators or logos are drawn.
    """

    bars = event.get("_chart_bars")
    signal_index = event.get("_chart_index")

    if not bars or signal_index is None:
        raise ValueError("Chart candle data is missing")

    if signal_index < 0 or signal_index >= len(bars):
        raise ValueError("Invalid signal candle index")

    visible = bars[:signal_index + 1]

    if not visible:
        raise ValueError("No candles available for chart")

    image = Image.new(
        "RGB",
        (CHART_WIDTH, CHART_HEIGHT),
        CHART_BACKGROUND
    )

    draw = ImageDraw.Draw(image)

    # --------------------------------------------------------
    # Chart layout
    # --------------------------------------------------------

    plot_left = 80
    plot_right = 1300
    plot_top = 65
    plot_bottom = 735

    candle_area_right = 1050

    plot_height = plot_bottom - plot_top
    candle_area_width = candle_area_right - plot_left

    # --------------------------------------------------------
    # Price range and automatic scaling
    # --------------------------------------------------------

    level_price = float(event["price"])

    price_high = max(
        float(bar["high"])
        for bar in visible
    )

    price_low = min(
        float(bar["low"])
        for bar in visible
    )

    price_high = max(price_high, level_price)
    price_low = min(price_low, level_price)

    price_range = price_high - price_low

    if price_range <= 0:
        price_range = max(abs(price_high) * 0.01, 0.000001)

    padding = price_range * 0.08

    price_max = price_high + padding
    price_min = price_low - padding

    full_range = price_max - price_min

    if full_range <= 0:
        raise ValueError("Invalid chart price range")

    def price_to_y(price):
        return int(
            plot_top
            + (price_max - price) / full_range * plot_height
        )

    # --------------------------------------------------------
    # Subtle horizontal grid
    # --------------------------------------------------------

    for i in range(6):
        y = int(
            plot_top
            + i * plot_height / 5
        )

        draw.line(
            (plot_left, y, plot_right, y),
            fill=CHART_GRID,
            width=1
        )

    # --------------------------------------------------------
    # Candlesticks
    # --------------------------------------------------------

    count = len(visible)

    spacing = candle_area_width / max(count, 1)

    candle_width = max(
        5,
        min(12, int(spacing * 0.62))
    )

    signal_candle_box = None

    for i, bar in enumerate(visible):
        x = int(
            plot_left
            + spacing * (i + 0.5)
        )

        op = float(bar["open"])
        hi = float(bar["high"])
        lo = float(bar["low"])
        cl = float(bar["close"])

        color = (
            CHART_UP
            if cl >= op
            else CHART_DOWN
        )

        y_high = price_to_y(hi)
        y_low = price_to_y(lo)
        y_open = price_to_y(op)
        y_close = price_to_y(cl)

        # Candle wick
        draw.line(
            (x, y_high, x, y_low),
            fill=color,
            width=2
        )

        # Candle body
        body_top = min(y_open, y_close)
        body_bottom = max(y_open, y_close)

        if body_bottom - body_top < 2:
            body_bottom = body_top + 2

        draw.rectangle(
            (
                x - candle_width // 2,
                body_top,
                x + candle_width // 2,
                body_bottom
            ),
            fill=color,
            outline=color
        )

        # Save the signal candle coordinates.
        if i == signal_index:
            signal_candle_box = (
                x,
                y_high,
                y_low
            )

    # --------------------------------------------------------
    # Yellow dashed horizontal level
    # --------------------------------------------------------

    level_y = price_to_y(level_price)

    dash_length = 13
    dash_gap = 9

    x = plot_left

    while x < plot_right:
        x_end = min(
            x + dash_length,
            plot_right
        )

        draw.line(
            (x, level_y, x_end, level_y),
            fill=CHART_LEVEL,
            width=2
        )

        x += dash_length + dash_gap

    # --------------------------------------------------------
    # Price label on the right
    # --------------------------------------------------------

    font = _chart_font(21)

    price_text = f"{level_price:.12g}"

    text_bbox = draw.textbbox(
        (0, 0),
        price_text,
        font=font
    )

    text_width = text_bbox[2] - text_bbox[0]
    text_height = text_bbox[3] - text_bbox[1]

    label_padding_x = 12
    label_padding_y = 8

    label_left = 1080
    label_right = min(
        CHART_WIDTH - 20,
        label_left
        + text_width
        + label_padding_x * 2
    )

    label_top = level_y - text_height // 2 - label_padding_y
    label_bottom = level_y + text_height // 2 + label_padding_y

    # Keep the price label inside the image.
    if label_top < 10:
        shift = 10 - label_top
        label_top += shift
        label_bottom += shift

    if label_bottom > CHART_HEIGHT - 10:
        shift = label_bottom - (CHART_HEIGHT - 10)
        label_top -= shift
        label_bottom -= shift

    draw.rectangle(
        (
            label_left,
            label_top,
            label_right,
            label_bottom
        ),
        fill=CHART_BACKGROUND,
        outline=CHART_LEVEL,
        width=2
    )

    draw.text(
        (
            label_left + label_padding_x,
            label_top + label_padding_y - 2
        ),
        price_text,
        fill=CHART_LEVEL,
        font=font
    )

    # --------------------------------------------------------
    # Green outline around the signal candle
    # Includes the entire wick.
    # --------------------------------------------------------

    if signal_candle_box is None:
        raise ValueError("Signal candle was not drawn")

    signal_x, signal_high_y, signal_low_y = signal_candle_box

    highlight_padding_x = 10
    highlight_padding_y = 9

    box_left = signal_x - candle_width // 2 - highlight_padding_x
    box_right = signal_x + candle_width // 2 + highlight_padding_x

    box_top = signal_high_y - highlight_padding_y
    box_bottom = signal_low_y + highlight_padding_y

    draw.rectangle(
        (
            box_left,
            box_top,
            box_right,
            box_bottom
        ),
        outline=CHART_HIGHLIGHT,
        width=3
    )

    # --------------------------------------------------------
    # Return PNG in memory.
    # --------------------------------------------------------

    output = BytesIO()

    image.save(
        output,
        format="PNG",
        optimize=True
    )

    output.seek(0)

    return output


# ============================================================
# HTTP
# ============================================================

def get_session():
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
    global _last_request_time

    with _request_lock:
        now = time.monotonic()
        wait = REQUEST_INTERVAL - (now - _last_request_time)

        if wait > 0:
            time.sleep(wait)

        _last_request_time = time.monotonic()


def get_json(url: str, params: dict | None = None, timeout: int = HTTP_TIMEOUT):
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

        except (requests.RequestException, ValueError) as exc:
            last_error = exc

            if attempt < HTTP_RETRIES:
                time.sleep(min(1.0 * attempt, 3.0))

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

    items.sort(key=lambda x: x[1], reverse=True)
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

    items.sort(key=lambda x: x[1], reverse=True)
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
        atr = (atr * (period - 1) + tr) / period

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

        if interval == "5m":
            bar = "5m"
        elif interval == "4h":
            bar = "4H"
        else:
            bar = "1D"

        rows = _okx_rows(
            inst_id,
            bar,
            limit
        )

        return _normalise_rows(rows, "okx")

    contract = _base(symbol) + "_USDT"

    if interval == "5m":
        gate_interval = "5m"
    elif interval == "4h":
        gate_interval = "4h"
    else:
        gate_interval = "1d"

    rows = _gate_rows(
        contract,
        gate_interval,
        limit
    )

    return _normalise_rows(rows, "gate")


# ============================================================
# DAILY CANDLES CACHE
# ============================================================

def load_daily_cache(exchange):
    global _daily_cache

    if not DAILY_CACHE_FILE.exists():
        print(
            "[CACHE] daily_cache.json not found; "
            "full daily history will be downloaded.",
            flush=True
        )
        _daily_cache = {
            "version": DAILY_CACHE_VERSION,
            "exchange": exchange,
            "symbols": {}
        }
        return

    try:
        data = json.loads(
            DAILY_CACHE_FILE.read_text(encoding="utf-8")
        )

        if (
            data.get("version") != DAILY_CACHE_VERSION
            or data.get("exchange") != exchange
            or not isinstance(data.get("symbols"), dict)
        ):
            print(
                "[CACHE] Cache version or exchange mismatch; "
                "starting with an empty cache.",
                flush=True
            )
            _daily_cache = {
                "version": DAILY_CACHE_VERSION,
                "exchange": exchange,
                "symbols": {}
            }
            return

        valid_symbols = {}

        for symbol, rows in data["symbols"].items():
            if not isinstance(rows, list):
                continue

            valid_rows = []

            for row in rows:
                try:
                    if len(row) < 6:
                        continue

                    valid_rows.append([
                        int(row[0]),
                        float(row[1]),
                        float(row[2]),
                        float(row[3]),
                        float(row[4]),
                        float(row[5])
                    ])
                except (TypeError, ValueError):
                    continue

            valid_rows.sort(key=lambda x: x[0])

            if valid_rows:
                valid_symbols[symbol] = valid_rows[-700:]

        _daily_cache = {
            "version": DAILY_CACHE_VERSION,
            "exchange": exchange,
            "symbols": valid_symbols
        }

        print(
            f"[CACHE] Loaded daily history for "
            f"{len(valid_symbols)} symbols.",
            flush=True
        )

    except Exception as exc:
        print(
            f"[CACHE] Cannot read daily cache: "
            f"{type(exc).__name__}: {exc}",
            flush=True
        )

        _daily_cache = {
            "version": DAILY_CACHE_VERSION,
            "exchange": exchange,
            "symbols": {}
        }


def get_cached_daily_bars(exchange, symbol):
    with _daily_cache_lock:
        cached = _daily_cache["symbols"].get(symbol, [])
        cached = [row[:] for row in cached]

    download_limit = 30 if cached else 700

    try:
        fresh = get_bars(
            exchange,
            symbol,
            "1d",
            download_limit
        )

        merged = {
            int(row[0]): row
            for row in cached
        }

        for row in fresh:
            merged[int(row[0])] = row

        result = [
            merged[ts]
            for ts in sorted(merged)
        ][-700:]

        with _daily_cache_lock:
            _daily_cache["symbols"][symbol] = result

        return result

    except Exception as exc:
        if len(cached) >= 60:
            print(
                f"[CACHE] {symbol}: daily refresh failed; "
                f"using cached history. "
                f"{type(exc).__name__}: {exc}",
                flush=True
            )
            return cached

        raise


def save_daily_cache():
    with _daily_cache_lock:
        data = {
            "version": DAILY_CACHE_VERSION,
            "exchange": _daily_cache["exchange"],
            "symbols": _daily_cache["symbols"]
        }

        serialized = json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":")
        )

    temp_file = DAILY_CACHE_FILE.with_suffix(".tmp")

    temp_file.write_text(
        serialized,
        encoding="utf-8"
    )

    temp_file.replace(DAILY_CACHE_FILE)

    print(
        f"[CACHE] Saved daily history for "
        f"{len(data['symbols'])} symbols.",
        flush=True
    )


# ============================================================
# 4H CANDLES CACHE
# ============================================================

def load_4h_cache(exchange):
    global _four_hour_cache

    if not FOUR_HOUR_CACHE_FILE.exists():
        print(
            "[CACHE] 4h_cache.json not found; "
            "full 4H history will be downloaded.",
            flush=True
        )

        _four_hour_cache = {
            "version": FOUR_HOUR_CACHE_VERSION,
            "exchange": exchange,
            "symbols": {}
        }
        return

    try:
        data = json.loads(
            FOUR_HOUR_CACHE_FILE.read_text(encoding="utf-8")
        )

        if (
            data.get("version") != FOUR_HOUR_CACHE_VERSION
            or data.get("exchange") != exchange
            or not isinstance(data.get("symbols"), dict)
        ):
            print(
                "[CACHE] 4H cache version or exchange mismatch; "
                "starting with an empty cache.",
                flush=True
            )

            _four_hour_cache = {
                "version": FOUR_HOUR_CACHE_VERSION,
                "exchange": exchange,
                "symbols": {}
            }
            return

        valid_symbols = {}

        for symbol, rows in data["symbols"].items():
            if not isinstance(rows, list):
                continue

            valid_rows = []

            for row in rows:
                try:
                    if len(row) < 6:
                        continue

                    valid_rows.append([
                        int(row[0]),
                        float(row[1]),
                        float(row[2]),
                        float(row[3]),
                        float(row[4]),
                        float(row[5])
                    ])
                except (TypeError, ValueError):
                    continue

            valid_rows.sort(key=lambda x: x[0])

            if valid_rows:
                valid_symbols[symbol] = valid_rows[-700:]

        _four_hour_cache = {
            "version": FOUR_HOUR_CACHE_VERSION,
            "exchange": exchange,
            "symbols": valid_symbols
        }

        print(
            f"[CACHE] Loaded 4H history for "
            f"{len(valid_symbols)} symbols.",
            flush=True
        )

    except Exception as exc:
        print(
            f"[CACHE] Cannot read 4H cache: "
            f"{type(exc).__name__}: {exc}",
            flush=True
        )

        _four_hour_cache = {
            "version": FOUR_HOUR_CACHE_VERSION,
            "exchange": exchange,
            "symbols": {}
        }


def get_cached_4h_bars(exchange, symbol):
    with _four_hour_cache_lock:
        cached = _four_hour_cache["symbols"].get(symbol, [])
        cached = [row[:] for row in cached]

    download_limit = 30 if cached else 700

    try:
        fresh = get_bars(
            exchange,
            symbol,
            "4h",
            download_limit
        )

        merged = {
            int(row[0]): row
            for row in cached
        }

        for row in fresh:
            merged[int(row[0])] = row

        result = [
            merged[ts]
            for ts in sorted(merged)
        ][-700:]

        with _four_hour_cache_lock:
            _four_hour_cache["symbols"][symbol] = result

        return result

    except Exception as exc:
        if len(cached) >= 60:
            print(
                f"[CACHE] {symbol}: 4H refresh failed; "
                f"using cached history. "
                f"{type(exc).__name__}: {exc}",
                flush=True
            )
            return cached

        raise


def save_4h_cache():
    with _four_hour_cache_lock:
        data = {
            "version": FOUR_HOUR_CACHE_VERSION,
            "exchange": _four_hour_cache["exchange"],
            "symbols": _four_hour_cache["symbols"]
        }

        serialized = json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":")
        )

    temp_file = FOUR_HOUR_CACHE_FILE.with_suffix(".tmp")

    temp_file.write_text(
        serialized,
        encoding="utf-8"
    )

    temp_file.replace(FOUR_HOUR_CACHE_FILE)

    print(
        f"[CACHE] Saved 4H history for "
        f"{len(data['symbols'])} symbols.",
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


def fb_compute(days, m5, now_ms=None, interval_seconds=86400):
    """
    Build levels on the selected timeframe and detect their
    false-breakout/touch events using 5-minute candles.

    interval_seconds:
        86400 = daily levels
        14400 = 4H levels

    Historical invalidation is checked using the same timeframe
    as the level formation timeframe.
    """

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

    def add_bar(i):
        e = ev[i]

        if e:
            for x in e["h"]:
                add(H, "H", x, e["atr"] or 0)

            for x in e["l"]:
                add(Lo, "L", x, e["atr"] or 0)

    if m5:
        start_boundary = (
            (m5[0]["time"] + interval_seconds - 1)
            // interval_seconds
        ) * interval_seconds

        k = next(
            (
                i for i, d in enumerate(days)
                if d["time"] >= start_boundary
            ),
            -1
        )

        i_start = k if k >= 0 else n
    else:
        i_start = n

    if i_start > n - 1:
        i_start = n - 1

    # Reconstruct levels before the available 5m history.
    # Daily levels are invalidated by daily candles.
    # 4H levels are invalidated by 4H candles.
    for i in range(i_start):
        add_bar(i)

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

    add_bar(cur)

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
            add_bar(cur)

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

    url = f"https://api.telegram.org/bot{token}/sendMessage"

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


def telegram_post_photo(message, photo, timeout=30):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token or not chat_id:
        print(
            "[ERROR] Telegram credentials are missing",
            flush=True
        )
        return False

    url = f"https://api.telegram.org/bot{token}/sendPhoto"

    try:
        photo.seek(0)

        response = requests.post(
            url,
            data={
                "chat_id": chat_id,
                "caption": message,
                "parse_mode": "HTML"
            },
            files={
                "photo": (
                    "gold_scanner_signal.png",
                    photo,
                    "image/png"
                )
            },
            timeout=timeout
        )

        response.raise_for_status()
        result = response.json()

        if result.get("ok") is True:
            return True

        print(
            "[ERROR] Telegram rejected chart: "
            + str(result.get("description", "unknown error")),
            flush=True
        )

        return False

    except Exception as exc:
        print(
            f"[ERROR] Telegram chart delivery failed: "
            f"{type(exc).__name__}: {exc}",
            flush=True
        )

        return False


def send_telegram(sym, event, atr_pct):
    side = (
        "ШОРТ 🔴"
        if event["side"] == "H"
        else "ЛОНГ 🟢"
    )

    signal_time = datetime.fromtimestamp(
        event["touch"],
        tz=ZoneInfo("Asia/Tbilisi")
    )

    dt = signal_time.strftime("%d.%m.%Y %H:%M")

    level_age_days = max(
        0,
        int((event["touch"] - event["time"]) // 86400)
    )

    timeframe = event.get("timeframe", "1D")

    message = (
        "🟡 <b>GOLD SCANNER</b>\n"
        "━━━━━━━━━━━━━━\n"
        f"Монета: <b>{html.escape(sym)}</b>\n"
        f"Таймфрейм: <b>{timeframe}</b>\n"
        f"Сигнал: <b>{side}</b>\n"
        f"Уровень: <code>{event['price']:.12g}</code>\n"
        f"Давность уровня: <b>{level_age_days} дн.</b>\n"
        f"Время сигнала: {dt}\n"
        "━━━━━━━━━━━━━━"
    )

    # First try to send the chart with the signal caption.
    try:
        chart = make_signal_chart(event)

        if telegram_post_photo(message, chart):
            print(
                f"[TELEGRAM] Sent chart: {sym} | {timeframe}",
                flush=True
            )
            return True

    except Exception as exc:
        print(
            f"[CHART] Cannot create chart for {sym}: "
            f"{type(exc).__name__}: {exc}",
            flush=True
        )

    # If chart creation or photo delivery failed,
    # send the original text notification.
    if telegram_post(message):
        print(
            f"[TELEGRAM] Sent text fallback: {sym} | {timeframe}",
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
# CHART CANDLE ATTACHMENT
# ============================================================

def _attach_chart_candle(event, timeframe_bars, interval_seconds):
    """
    Find the daily or 4H candle containing the 5m signal time.

    Attach only the 60 preceding candles and the signal candle.
    This keeps event memory usage small.
    """

    if not timeframe_bars:
        return

    touch_time = int(event["touch"])

    signal_index = None

    for i, bar in enumerate(timeframe_bars):
        candle_start = int(bar["time"])
        candle_end = candle_start + interval_seconds

        if candle_start <= touch_time < candle_end:
            signal_index = i
            break

    # Fallback: use the latest candle that started before
    # the signal time. Never select a future candle.
    if signal_index is None:
        candidates = [
            i
            for i, bar in enumerate(timeframe_bars)
            if int(bar["time"]) <= touch_time
        ]

        if not candidates:
            return

        signal_index = candidates[-1]

    start_index = max(
        0,
        signal_index - 60
    )

    event["_chart_bars"] = [
        {
            "time": int(bar["time"]),
            "open": float(bar["open"]),
            "high": float(bar["high"]),
            "low": float(bar["low"]),
            "close": float(bar["close"])
        }
        for bar in timeframe_bars[start_index:signal_index + 1]
    ]

    event["_chart_index"] = (
        signal_index - start_index
    )


# ============================================================
# PER-SYMBOL PROCESSING
# ============================================================

def process_symbol(exchange, sym, is_new_symbol, seen_snapshot):
    """
    Process one symbol independently.

    Daily and 4H level systems are calculated independently.
    Both use 5-minute candles for false-breakout detection.

    Returns:
        symbol, discovered events, seed keys, success, error
    """

    try:
        days_raw = get_cached_daily_bars(
            exchange,
            sym
        )

        four_hours_raw = get_cached_4h_bars(
            exchange,
            sym
        )

        m5_raw = get_bars(
            exchange,
            sym,
            "5m",
            288
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

        four_hours = [
            {
                "time": int(k[0]),
                "open": k[1],
                "high": k[2],
                "low": k[3],
                "close": k[4],
                "volume": k[5]
            }
            for k in four_hours_raw
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

        if (
            len(days) < 60
            or len(four_hours) < 60
            or len(m5) < 30
        ):
            return (
                sym,
                [],
                [],
                False,
                f"insufficient candle history "
                f"(daily={len(days)}, "
                f"4H={len(four_hours)}, "
                f"5m={len(m5)})"
            )

        # ----------------------------------------------------
        # DAILY ATR
        # ----------------------------------------------------

        daily_highs = [
            d["high"]
            for d in days[:-1]
        ]

        daily_lows = [
            d["low"]
            for d in days[:-1]
        ]

        daily_closes = [
            d["close"]
            for d in days[:-1]
        ]

        daily_atr_value = wilder_atr(
            daily_highs,
            daily_lows,
            daily_closes,
            14
        )

        daily_last_close = days[-1]["close"]

        daily_atr_pct = (
            daily_atr_value / daily_last_close * 100
            if daily_atr_value and daily_last_close > 0
            else 0.0
        )

        # ----------------------------------------------------
        # 4H ATR
        # ----------------------------------------------------

        four_hour_highs = [
            d["high"]
            for d in four_hours[:-1]
        ]

        four_hour_lows = [
            d["low"]
            for d in four_hours[:-1]
        ]

        four_hour_closes = [
            d["close"]
            for d in four_hours[:-1]
        ]

        four_hour_atr_value = wilder_atr(
            four_hour_highs,
            four_hour_lows,
            four_hour_closes,
            14
        )

        four_hour_last_close = four_hours[-1]["close"]

        four_hour_atr_pct = (
            four_hour_atr_value / four_hour_last_close * 100
            if four_hour_atr_value and four_hour_last_close > 0
            else 0.0
        )

        # ----------------------------------------------------
        # DAILY LEVELS AND SIGNALS
        # Existing daily calculation remains unchanged.
        # ----------------------------------------------------

        daily_result = fb_compute(
            days,
            m5,
            interval_seconds=86400
        )

        # ----------------------------------------------------
        # 4H LEVELS AND SIGNALS
        # 4H levels are invalidated by subsequent 4H candles.
        # False-breakout detection still uses the 5m candles.
        # ----------------------------------------------------

        four_hour_result = fb_compute(
            four_hours,
            m5,
            interval_seconds=14400
        )

        discovered = []
        seed_keys = []

        # Process daily events.
        for event in daily_result["events"]:
            event["timeframe"] = "1D"

            key = (
                f"{sym}|1D|{event['side']}|{event['time']}|"
                f"{event['price']:.12g}|{event['touch']}"
            )

            if is_new_symbol:
                seed_keys.append(key)

            elif key not in seen_snapshot:
                _attach_chart_candle(
                    event,
                    days,
                    86400
                )

                discovered.append(
                    (sym, event, daily_atr_pct, key)
                )

        # Process 4H events.
        for event in four_hour_result["events"]:
            event["timeframe"] = "4H"

            key = (
                f"{sym}|4H|{event['side']}|{event['time']}|"
                f"{event['price']:.12g}|{event['touch']}"
            )

            if is_new_symbol:
                seed_keys.append(key)

            elif key not in seen_snapshot:
                _attach_chart_candle(
                    event,
                    four_hours,
                    14400
                )

                discovered.append(
                    (sym, event, four_hour_atr_pct, key)
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
        f"mergeAtr={FB['mergeAtr']}; "
        f"leaveAtr={FB['leaveAtr']}; "
        f"daily=1D; "
        f"additional=4H; "
        f"max_monitored={MAX_MONITORED}; "
        f"workers={SCAN_WORKERS}; "
        f"http_retries={HTTP_RETRIES}",
        flush=True
    )

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

    # Load daily and 4H candle caches.
    load_daily_cache(exchange)
    load_4h_cache(exchange)

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

    # Save updated daily and 4H caches after all workers finish.
    save_daily_cache()
    save_4h_cache()

    if not state.get("primed"):
        state["primed"] = True

        print(
            "Initial run: historical signals seeded; "
            "no old alerts sent.",
            flush=True
        )

    seen.update(seed_keys)

    sent_count = 0
    failed_count = 0

    print(
        f"[INFO] New signal candidates: {len(discovered)}",
        flush=True
    )

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
            f"SIGNAL | {sym} | {event.get('timeframe', '1D')} | "
            f"{side} | "
            f"level={event['price']} | "
            f"candle={dt} | "
            f"ATR%={atr_pct:.2f}",
            flush=True
        )

        if send_telegram(sym, event, atr_pct):
            seen.add(key)
            sent_count += 1

        else:
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
