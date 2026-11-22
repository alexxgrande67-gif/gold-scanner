
import requests
from datetime import datetime, timezone

URLS = {
    "Bybit server time":
        "https://api.bybit.com/v5/market/time",

    "Bybit instruments":
        "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=10",

    "Binance futures server time":
        "https://fapi.binance.com/fapi/v1/time",

    "Binance futures instruments":
        "https://fapi.binance.com/fapi/v1/exchangeInfo",
}

print("GOLD SCANNER API DIAGNOSTICS")
print("UTC:", datetime.now(timezone.utc).isoformat())

headers = {
    "User-Agent": "Mozilla/5.0 GOLD-SCANNER/1.0",
    "Accept": "application/json",
}

for name, url in URLS.items():
    print("\n" + "=" * 45)
    print(name)
    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=20
        )
        print("HTTP:", response.status_code)
        print("Response:", response.text[:500])
    except Exception as error:
        print("ERROR:", repr(error))

print("\nDIAGNOSTICS FINISHED")
