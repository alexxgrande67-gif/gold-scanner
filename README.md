# GOLD SCANNER — GitHub Actions prototype

This repository contains the existing GOLD SCANNER HTML interface and a scheduled Python scan runner.

## What runs

- GitHub Actions starts `scanner.py` on a 5-minute schedule (`2, 7, 12, ... 57` minutes each hour, UTC) and can also be started manually from **Actions → GOLD SCANNER - every 5 minutes → Run workflow**.
- The backend reuses the original scanner's default false-breakout parameters: swing 12, minimum swing size 2.5 daily ATR, merge distance 0.5 daily ATR, and leave distance 0.5 daily ATR.
- The universe is selected using the original Binance USDT-M Futures liquidity ranking and stability filter; up to 100 symbols are monitored by default (`MAX_MONITORED`).
- Bybit is the primary source for daily/5-minute candles; the run falls back to Binance if Bybit is unavailable.
- The first run seeds historical events without alerting. New events are printed in the Actions log and de-duplicated using `state.json`.

## Important limitations

GitHub Actions is a scheduled batch runner, not a permanently running server or WebSocket process. GitHub allows a minimum schedule interval of 5 minutes, but scheduled starts can be delayed or dropped under load. Public-repository scheduled workflows are disabled after 60 days without repository activity. The HTML page is a browser interface; it does not run the server-side scanner in the background.

This first deployment package validates the scheduled scanning and signal calculation. Apple Web Push delivery is a separate next step: it requires a PWA subscription from the iPhone and a secure way to store that subscription and VAPID credentials. No notification credentials are embedded in this public repository.

## Deploy

1. Create a **public** GitHub repository named `gold-scanner`.
2. Upload the contents of this folder to the repository root (not the folder itself).
3. Ensure the default branch is named `main`.
4. In **Settings → Pages**, choose **GitHub Actions** as the build/deploy source.
5. Open **Actions** and enable workflows if GitHub asks.
6. Run **Publish GOLD SCANNER website** once, then open the Pages URL shown in the workflow summary.
7. Open **Actions → GOLD SCANNER - every 5 minutes** and click **Run workflow** for the first scanner test.
8. Open the run and inspect the `Run GOLD SCANNER` step logs.

The first run may take several minutes because it builds the eligible universe and seeds historical signal keys. Subsequent runs should be faster only if the API response times and rate limits permit.

## Configuration

`MAX_MONITORED` defaults to 100. Change the workflow environment or script default if you intentionally want a different number. Do not put API keys, Telegram tokens, VAPID private keys, or push subscriptions in source files. Use GitHub Actions Secrets for credentials when the notification stage is added.
