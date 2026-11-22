#!/usr/bin/env python3
"""GOLD SCANNER scheduled backend. Ports the original false-breakout engine."""
from __future__ import annotations
import json, math, os, sys, time
from pathlib import Path
from typing import Any
import requests

BINANCE = "https://fapi.binance.com"
BYBIT = "https://api.bybit.com"
STATE_FILE = Path("state.json")
FB = {"swing": 12, "minSwingAtr": 2.5, "mergeAtr": 0.5, "leaveAtr": 0.5}
TOP_UNIVERSE = 200  # Original stable filter only accepts rank < 200.
MAX_MONITORED = int(os.getenv("MAX_MONITORED", "100"))
EXCLUDE_EXACT = {"USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "USDPUSDT", "DAIUSDT", "EURUSDT", "GBPUSDT", "AEURUSDT", "BFUSDUSDT"}
EXCLUDE_SUFFIXES = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "GOLD-SCANNER/1.0"})


def get_json(url: str, params: dict | None = None, timeout: int = 25):
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def parse_binance_klines(rows):
    return [{"time": int(k[0] // 1000), "open": float(k[1]), "high": float(k[2]),
             "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])} for k in rows]



def bybit_universe():
    """Build USDT perpetual universe from Bybit, ranked by 24h turnover."""
    supported = bybit_symbols()

    data = get_json(
        BYBIT + "/v5/market/tickers",
        {"category": "linear"}
    )
    if data.get("retCode") != 0:
        raise RuntimeError(
            "Bybit tickers: " + str(data.get("retMsg"))
        )

    items = []
    for ticker in data.get("result", {}).get("list", []):
        sym = ticker.get("symbol", "")
        if sym not in supported:
            continue
        if sym in EXCLUDE_EXACT or sym.endswith(EXCLUDE_SUFFIXES):
            continue

        try:
            turnover = float(ticker.get("turnover24h", 0))
        except (TypeError, ValueError):
            continue

        if turnover > 0:
            items.append((sym, turnover))

    items.sort(key=lambda x: x[1], reverse=True)
    return items[:TOP_UNIVERSE]


def daily_returns(klines):
    # Match original JS: only closed daily bars; exclude today's still-forming bar.
    out = {}
    for i in range(1, max(1, len(klines)-1)):
        prev = float(klines[i-1][4]); close = float(klines[i][4])
        if prev > 0: out[int(klines[i][0])] = close / prev - 1
    return out


def stdev(values):
    if not values: return 0.0
    m = sum(values) / len(values)
    return math.sqrt(sum((x-m)**2 for x in values) / len(values))


def corr(a, b):
    if not a or len(a) != len(b): return 0.0
    ma, mb = sum(a)/len(a), sum(b)/len(b)
    num = sum((x-ma)*(y-mb) for x,y in zip(a,b))
    da = sum((x-ma)**2 for x in a); db = sum((y-mb)**2 for y in b)
    return num / math.sqrt(da*db) if da and db else 0.0


def wilder_atr(highs, lows, closes, period=14):
    trs = []
    for i in range(1, len(highs)):
        trs.append(max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])))
    if len(trs) < period: return None
    atr = sum(trs[:period]) / period
    for tr in trs[period:]: atr = (atr*(period-1)+tr)/period
    return atr



def build_stable_universe():
    """Apply the original stability and ATR filters using Bybit data."""
    items = bybit_universe()

    btc_data = get_json(
        BYBIT + "/v5/market/kline",
        {
            "category": "linear",
            "symbol": "BTCUSDT",
            "interval": "D",
            "limit": 120
        }
    )

    if btc_data.get("retCode") != 0:
        raise RuntimeError(
            "Bybit BTC daily candles: " + str(btc_data.get("retMsg"))
        )

    btc_kl = list(reversed(
        btc_data.get("result", {}).get("list", [])
    ))

    btc_map = daily_returns(btc_kl)
    btc_sigma = stdev(list(btc_map.values()))

    stable = []

    for rank, (sym, quote_volume) in enumerate(items):
        try:
            data = get_json(
                BYBIT + "/v5/market/kline",
                {
                    "category": "linear",
                    "symbol": sym,
                    "interval": "D",
                    "limit": 120
                }
            )

            if data.get("retCode") != 0:
                raise RuntimeError(str(data.get("retMsg")))

            kl = list(reversed(
                data.get("result", {}).get("list", [])
            ))

            if len(kl) < 90:
                continue

            highs = [float(k[2]) for k in kl]
            lows = [float(k[3]) for k in kl]
            closes = [float(k[4]) for k in kl]

            atr = wilder_atr(
                highs[:-1], lows[:-1], closes[:-1], 14
            )

            if not atr or closes[-1] <= 0:
                continue

            atr_pct = atr / closes[-1] * 100
            rets = daily_returns(kl)
            own = list(rets.values())

            if not own:
                continue

            shared = [
                (v, btc_map[t])
                for t, v in rets.items()
                if t in btc_map
            ]

            aa = [x[0] for x in shared]
            bb = [x[1] for x in shared]

            sigma = stdev(own)
            max_move = max(abs(x) for x in own)
            correlation = corr(aa, bb) if len(aa) >= 30 else 0

            is_stable = (
                rank < 200
                and sigma <= 2 * btc_sigma
                and max_move <= 5 * btc_sigma
                and correlation >= 0.6
            )

            if is_stable and atr_pct <= 4:
                stable.append((sym, quote_volume, atr_pct))

        except Exception as e:
            print(f"[WARN] Bybit daily {sym}: {e}", flush=True)

        time.sleep(0.08)

    return stable[:MAX_MONITORED]


def bybit_symbols():
    symbols = set(); cursor = ""
    for _ in range(10):
        p = {"category":"linear", "limit":1000}
        if cursor: p["cursor"] = cursor
        j = get_json(BYBIT + "/v5/market/instruments-info", p)
        if j.get("retCode") != 0: raise RuntimeError("Bybit instruments: " + str(j.get("retMsg")))
        for s in j.get("result", {}).get("list", []):
            if s.get("status") == "Trading" and s.get("quoteCoin") == "USDT" and s.get("contractType") == "LinearPerpetual" and s.get("symbol", "").endswith("USDT"):
                symbols.add(s["symbol"])
        cursor = j.get("result", {}).get("nextPageCursor", "")
        if not cursor: break
    if not symbols: raise RuntimeError("Bybit returned no linear USDT perpetuals")
    return symbols


def get_bars(exchange, symbol, interval, limit):
    if exchange == "bybit":
        iv = "5" if interval == "5m" else "D"
        j = get_json(BYBIT + "/v5/market/kline", {"category":"linear", "symbol":symbol, "interval":iv, "limit":min(limit,1000)})
        if j.get("retCode") != 0: raise RuntimeError(f"Bybit kline {symbol}: {j.get('retMsg')}")
        rows = j.get("result",{}).get("list",[])
        return [{"time":int(k[0])//1000,"open":float(k[1]),"high":float(k[2]),"low":float(k[3]),"close":float(k[4]),"volume":float(k[5])} for k in reversed(rows)]
    rows = get_json(BINANCE + "/fapi/v1/klines", {"symbol":symbol,"interval":interval,"limit":min(limit,1000)})
    return parse_binance_klines(rows)


def atr_series(days, period=14):
    out=[None]*len(days); prev=0.0; total=0.0
    for i,d in enumerate(days):
        tr = d["high"]-d["low"] if i==0 else max(d["high"]-d["low"],abs(d["high"]-days[i-1]["close"]),abs(d["low"]-days[i-1]["close"]))
        if i<period:
            total += tr
            if i==period-1: prev=total/period; out[i]=prev
        else:
            prev=(prev*(period-1)+tr)/period; out[i]=prev
    return out


def fb_events(days):
    n=len(days); L=FB["swing"]; atr=atr_series(days,14); ev=[None]*n
    for p in range(L, n-L-1):
        c=p+L; i=c+1; hp=days[p]["high"]; lp=days[p]["low"]; a=atr[p]
        if a is None: continue
        is_h=is_l=True; sw_lo=float("inf"); sw_hi=float("-inf"); max_a=float("-inf"); min_a=float("inf")
        for k in range(p-L,c+1):
            b=days[k]; sw_lo=min(sw_lo,b["low"]); sw_hi=max(sw_hi,b["high"])
            if k<p:
                if b["high"]>=hp: is_h=False
                if b["low"]<=lp: is_l=False
            elif k>p:
                if b["high"]>hp: is_h=False
                if b["low"]<lp: is_l=False
                max_a=max(max_a,b["high"]); min_a=min(min_a,b["low"])
        strong_h=is_h and hp-sw_lo>=a*FB["minSwingAtr"] and max_a<hp
        strong_l=is_l and sw_hi-lp>=a*FB["minSwingAtr"] and min_a>lp
        if not strong_h and not strong_l: continue
        if ev[i] is None: ev[i]={"atr":atr[c],"h":[],"l":[]}
        if strong_h: ev[i]["h"].append({"price":hp,"time":days[p]["time"]})
        if strong_l: ev[i]["l"].append({"price":lp,"time":days[p]["time"]})
    return ev,atr


def fb_compute(days,m5,now_ms=None):
    n=len(days); L=FB["swing"]
    if n < L*2+20: return {"levels":[],"events":[]}
    ev,atr=fb_events(days); H=[]; Lo=[]
    def add(arr,side,it,a):
        if any(abs(x["price"]-it["price"])<=a*FB["mergeAtr"] for x in arr): return
        arr.append({"side":side,"price":it["price"],"time":it["time"],"st":0,"touch":0})
        if len(arr)>240: arr.pop(0)
    def add_day(i):
        e=ev[i]
        if e:
            for x in e["h"]: add(H,"H",x,e["atr"] or 0)
            for x in e["l"]: add(Lo,"L",x,e["atr"] or 0)
    if m5:
        ds=((m5[0]["time"]+86399)//86400)*86400
        k=next((i for i,d in enumerate(days) if d["time"]>=ds),-1)
        i_start=k if k>=0 else n
    else: i_start=n
    if i_start>n-1: i_start=n-1
    for i in range(i_start):
        add_day(i); H=[x for x in H if days[i]["high"]<x["price"]]; Lo=[x for x in Lo if days[i]["low"]>x["price"]]
    events=[]; cur=i_start; add_day(cur); now_sec=(now_ms or int(time.time()*1000))/1000
    for bi,b in enumerate(m5):
        if b["time"]<days[i_start]["time"]: continue
        while cur+1<n and b["time"]>=days[cur+1]["time"]:
            cur+=1; add_day(cur)
        closed=(bi<len(m5)-1 or b["time"]+300<=now_sec)
        D=FB["leaveAtr"]*(atr[cur-1] or atr[cur] or 0)
        gone=False
        for x in H:
            if closed and b["close"]>x["price"]: x["st"]=2; gone=True; continue
            if closed and b["high"]>=x["price"]:
                x["st"]=1; x["touch"]=b["time"]; events.append({"side":"H","price":x["price"],"time":x["time"],"touch":b["time"]}); continue
            if x["st"]==1 and x["touch"]<b["time"] and (b["high"]>=x["price"]+D or b["low"]<=x["price"]-D): x["st"]=2; gone=True
        for x in Lo:
            if closed and b["close"]<x["price"]: x["st"]=2; gone=True; continue
            if closed and b["low"]<=x["price"]:
                x["st"]=1; x["touch"]=b["time"]; events.append({"side":"L","price":x["price"],"time":x["time"],"touch":b["time"]}); continue
            if x["st"]==1 and x["touch"]<b["time"] and (b["low"]<=x["price"]-D or b["high"]>=x["price"]+D): x["st"]=2; gone=True
        if gone:
            H=[x for x in H if x["st"]!=2]; Lo=[x for x in Lo if x["st"]!=2]
    return {"levels":H+Lo,"events":events}


def load_state():
    if STATE_FILE.exists():
        try: return json.loads(STATE_FILE.read_text())
        except Exception: pass
    return {"primed":False,"seen":[],"seeded_symbols":[]}


def main():
    print("GOLD SCANNER: start", flush=True)
    symbols = build_stable_universe()
    print(f"Universe filtered: {len(symbols)} symbols; monitor cap={MAX_MONITORED}", flush=True)
    if not symbols:
        raise RuntimeError("No symbols passed the original stable/ATR filters")
    try:
        supported=bybit_symbols(); exchange="bybit"
        symbols=[x for x in symbols if x[0] in supported]
        if not symbols: raise RuntimeError("No selected symbols exist on Bybit")
    except Exception as e:
        print(f"[WARN] Bybit unavailable; switching this run to Binance: {e}", flush=True)
        exchange="binance"
    state=load_state(); seen=set(state.get("seen",[])); seeded=set(state.get("seeded_symbols",[])); discovered=[]; all_keys=[]
    for idx,(sym,vol,atr_pct) in enumerate(symbols,1):
        try:
            days=get_bars(exchange,sym,"1d",700)
            m5=get_bars(exchange,sym,"5m",1000)
            if len(days)<60 or len(m5)<30: continue
            result=fb_compute(days,m5)
            is_new_symbol = sym not in seeded
            for ev in result["events"]:
                key=f"{sym}|{ev['side']}|{ev['time']}|{ev['price']:.12g}|{ev['touch']}"
                all_keys.append(key)
                if key not in seen and not is_new_symbol:
                    discovered.append((sym,ev,atr_pct,vol,key))
            if is_new_symbol:
                seeded.add(sym)
            if idx%10==0: print(f"Progress: {idx}/{len(symbols)}",flush=True)
        except Exception as e:
            print(f"[WARN] {sym}: {e}",flush=True)
        time.sleep(0.08)
    seen.update(all_keys if not state.get("primed") else [])
    if not state.get("primed"):
        state["primed"]=True
        print("Initial run: historical signals seeded; no old alerts sent.",flush=True)
    for sym,ev,atr_pct,vol,key in discovered:
        seen.add(key)
        side="ЛОЖНЫЙ ПРОБОЙ ВВЕРХ" if ev["side"]=="H" else "ЛОЖНЫЙ ПРОБОЙ ВНИЗ"
        dt=time.strftime("%Y-%m-%d %H:%M UTC",time.gmtime(ev["touch"]))
        print(f"SIGNAL | {sym} | {side} | level={ev['price']} | candle={dt} | ATR%={atr_pct:.2f}",flush=True)
    # Seed any newly selected symbols without emitting their historical events.
    for key in all_keys:
        if key.split("|",1)[0] in seeded:
            seen.add(key)
    state["seeded_symbols"]=sorted(seeded)
    state["seen"]=list(seen)[-10000:]
    STATE_FILE.write_text(json.dumps(state,ensure_ascii=False,indent=2)+"\n")
    print(f"GOLD SCANNER: finished; new candidates={len(discovered) if state.get('primed') else 0}; exchange={exchange}",flush=True)

if __name__ == "__main__":
    try: main()
    except Exception as exc:
        print(f"FATAL: {exc}",file=sys.stderr,flush=True)
        raise
