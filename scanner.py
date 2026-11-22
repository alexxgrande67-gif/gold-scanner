
#!/usr/bin/env python3
"""GOLD SCANNER scheduled backend. Original false-breakout engine with OKX/Gate.io data."""
from __future__ import annotations

import json
import html
import math
import os
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import local

import requests

OKX = "https://www.okx.com"
GATE = "https://api.gateio.ws/api/v4"
STATE_FILE = Path("state.json")
FB = {"swing": 1, "minSwingAtr": 1, "mergeAtr": 0.5, "leaveAtr": 0.5}

# Expanded universe and monitoring capacity
TOP_UNIVERSE = 500
MAX_MONITORED = int(os.getenv("MAX_MONITORED", "500"))
SCAN_WORKERS = max(1, min(10, int(os.getenv("SCAN_WORKERS", "5"))))
HTTP_RETRIES = max(1, int(os.getenv("HTTP_RETRIES", "3")))
HTTP_THREAD_LOCAL = local()

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


def _thread_session():
    session = getattr(HTTP_THREAD_LOCAL, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({
            "User-Agent": "GOLD-SCANNER/1.0",
            "Accept": "application/json"
        })
        HTTP_THREAD_LOCAL.session = session
    return session



def get_json(url: str, params: dict | None = None, timeout: int = 25):
    last_error = None

    for attempt in range(HTTP_RETRIES):
        try:
            response = _thread_session().get(
                url, params=params, timeout=timeout
            )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt + 1 < HTTP_RETRIES:
                time.sleep(min(1.5 * (2 ** attempt), 6.0))

    raise RuntimeError(
        f"HTTP request failed after {HTTP_RETRIES} attempts: "
        f"{type(last_error).__name__}: {last_error}"
    )


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
