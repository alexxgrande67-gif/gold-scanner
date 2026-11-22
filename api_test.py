
import requests

URLS = {
    "OKX instruments":
        "https://www.okx.com/api/v5/public/instruments?instType=SWAP",

    "OKX BTC 5m candles":
        "https://www.okx.com/api/v5/market/candles?instId=BTC-USDT-SWAP&bar=5m&limit=2",

    "Gate instruments":
        "https://api.gateio.ws/api/v4/futures/usdt/contracts",

    "Gate BTC 5m candles":
        "https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract=BTC_USDT&interval=5m&limit=2",
}

for name, url in URLS.items():
    print("\n" + "=" * 45)
    print(name)
    try:
        r = requests.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=20
        )
        print("HTTP:", r.status_code)
        print("Response:", r.text[:700])
    except Exception as e:
        print("ERROR:", repr(e))

print("\nTEST FINISHED")
