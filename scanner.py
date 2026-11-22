
#!/usr/bin/env python3
"""GOLD SCANNER scheduled backend. Original false-breakout engine with OKX/Gate.io data."""
from __future__ import annotations

import json
import math
import os
import sys
import time
from pathlib import Path

import requests

OKX = "https://www.okx.com"
GATE = "https://api.gateio.ws/api/v4"
STATE_FILE = Path("state.json")
FB = {"swing": 12, "minSwingAtr": 2.5, "mergeAtr": 0.5, "leaveAtr": 0.5}
TOP_UNIVERSE = 200
MAX_MONITORED = int(os.getenv("MAX_MONITORED", "100"))

EXCLUDE_BASES = {
    "USDC", "FDUSD", "TUSD", "USDP", "DAI", "EUR", "GBP", "AEUR",
    "BFUSD", "USDE", "USD0", "PYUSD"
}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "GOLD-SCANNER/1.0",
    "Accept": "application/json"
})

ACTIVE_EXCHANGE = "okx"


def get_json(url: str, params: dict | None = None, timeout: int = 25):
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def _okx_rows(inst_id: str, bar: str, limit: int):
    """Fetch OKX candles."""
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

    return [rows_by_ts[k] for k in sorted(rows_by_ts)][-limit:]


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


def _canonical_from_okx(inst_id: str):
    # BTC-USDT-SWAP -> BTCUSDT
    return (
        inst_id[:-10] + "USDT"
        if inst_id.endswith("-USDT-SWAP")
        else None
    )


def _canonical_from_gate(name: str):
    return name.replace("_", "") if name.endswith("_USDT") else None


def _base(symbol: str):
    return symbol[:-4] if symbol.endswith("USDT") else symbol


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


def daily_returns(klines):
    # Exclude today's still-forming daily bar.
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

    return num / math.sqrt(da * db) if da and db else 0.0


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


def _daily_rows(symbol: str, limit: int):
    if ACTIVE_EXCHANGE == "okx":
        return _okx_rows(
            symbol.replace("USDT", "-USDT-SWAP"),
            "1D",
            limit
        )

    return _gate_rows(
        _base(symbol) + "_USDT",
        "1d",
        limit
    )


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


def build_stable_universe():
    """
    Build monitored universe.
    No ATR filter and no BTC stability filter.
    """
    global ACTIVE_EXCHANGE

    try:
        items = _okx_universe()
        ACTIVE_EXCHANGE = "okx"

    except Exception as exc:
        print(
            f"[WARN] OKX universe unavailable; using Gate.io: {exc}",
            flush=True
        )

        items = _gate_universe()
        ACTIVE_EXCHANGE = "gate"

    # No volatility filtering.
    selected = items[:MAX_MONITORED]

    print(
        f"[INFO] ATR filter disabled. "
        f"BTC stability filter disabled. "
        f"Top universe={len(items)}; "
        f"monitoring={len(selected)}",
        flush=True
    )

    return [
        (sym, quote_volume, None)
        for sym, quote_volume in selected
    ]


def get_bars(exchange, symbol, interval, limit):
    source = exchange

    if source == "okx":
        inst_id = symbol.replace(
            "USDT",
            "-USDT-SWAP"
        )

        bar = "5m" if interval == "5m" else "1D"

        rows = _okx_rows(
            inst_id,
            bar,
            limit
        )

        return _normalise_rows(rows, "okx")

    contract = _base(symbol) + "_USDT"

    gate_interval = (
        "5m" if interval == "5m" else "1d"
    )

    rows = _gate_rows(
        contract,
        gate_interval,
        limit
    )

    return _normalise_rows(rows, "gate")


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
            prev = (prev * (period - 1) + tr) / period
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

        is_h = is_l = True

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
        return {"levels": [], "events": []}

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
            (i for i, d in enumerate(days) if d["time"] >= ds),
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
            H = [x for x in H if x["st"] != 2]
            Lo = [x for x in Lo if x["st"] != 2]

    return {
        "levels": H + Lo,
        "events": events
    }


def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass

    return {
        "primed": False,
        "seen": [],
        "seeded_symbols": []
    }


def main():
    global ACTIVE_EXCHANGE

    print("GOLD SCANNER: start", flush=True)

    symbols = build_stable_universe()
    exchange = ACTIVE_EXCHANGE

    print(
        f"Universe selected: {len(symbols)} symbols; "
        f"monitor cap={MAX_MONITORED}; exchange={exchange}",
        flush=True
    )

    if not symbols:
        raise RuntimeError(
            "No symbols available in the selected universe"
        )

    state = load_state()
    seen = set(state.get("seen", []))
    seeded = set(state.get("seeded_symbols", []))
    discovered = []
    all_keys = []

    for idx, (sym, vol, _) in enumerate(symbols, 1):
        try:
            days_raw = get_bars(
                exchange, sym, "1d", 700
            )

            m5_raw = get_bars(
                exchange, sym, "5m", 1000
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

            if len(days) < 60 or len(m5) < 30:
                print(
                    f"[WARN] {sym}: insufficient candle history "
                    f"(daily={len(days)}, 5m={len(m5)})",
                    flush=True
                )
                continue

            # ATR is informational only; it does not filter symbols.
            highs = [d["high"] for d in days[:-1]]
            lows = [d["low"] for d in days[:-1]]
            closes = [d["close"] for d in days[:-1]]

            atr_value = wilder_atr(
                highs, lows, closes, 14
            )

            last_close = days[-1]["close"]

            atr_pct = (
                atr_value / last_close * 100
                if atr_value and last_close > 0
                else 0.0
            )

            result = fb_compute(days, m5)
            is_new_symbol = sym not in seeded

            for event in result["events"]:
                key = (
                    f"{sym}|{event['side']}|{event['time']}|"
                    f"{event['price']:.12g}|{event['touch']}"
                )

                all_keys.append(key)

                if key not in seen and not is_new_symbol:
                    discovered.append(
                        (sym, event, atr_pct, vol, key)
                    )

            if is_new_symbol:
                seeded.add(sym)

            if idx % 10 == 0 or idx == len(symbols):
                print(
                    f"Progress: {idx}/{len(symbols)}",
                    flush=True
                )

        except Exception as exc:
            print(
                f"[WARN] {sym}: {exc}",
                flush=True
            )

        time.sleep(0.06)

    if not state.get("primed"):
        state["primed"] = True

        print(
            "Initial run: historical signals seeded; "
            "no old alerts sent.",
            flush=True
        )

    for sym, event, atr_pct, vol, key in discovered:
        seen.add(key)

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
            f"level={event['price']} | candle={dt} | "
            f"ATR%={atr_pct:.2f}",
            flush=True
        )

    for key in all_keys:
        if key.split("|", 1)[0] in seeded:
            seen.add(key)

    state["seeded_symbols"] = sorted(seeded)
    state["seen"] = list(seen)[-10000:]

    STATE_FILE.write_text(
        json.dumps(state, ensure_ascii=False, indent=2) + "\n"
    )

    print(
        f"GOLD SCANNER: finished; "
        f"new candidates={len(discovered) if state.get('primed') else 0}; "
        f"exchange={exchange}",
        flush=True
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(
            f"FATAL: {exc}",
            file=sys.stderr,
            flush=True
        )
        raise
