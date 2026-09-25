# SolRadar v3

A 24/7 Solana smart-money tracker you can set up entirely from an iPhone.

- **Telegram alerts, 24/7:** a free Cloudflare server gets each trade from Helius the moment it happens and messages you within seconds, even when your phone is locked.
- **Auto-tracks famous traders:** every 6 hours it picks the top famous traders by 24h profit (Solana Tracker) and follows them. Wallets you add yourself stay until you remove them.
- **iPhone app** (GitHub Pages): markets, charts, a Leaders tab with 24h, 7-day and 30-day profit, trader profiles, a live feed and a watchlist.

## Files
| File | What it is |
|---|---|
| index.html, manifest.webmanifest, icon-180.png, icon-512.png | The iPhone app (GitHub Pages) |
| worker.js, wrangler.toml | The 24/7 server (Cloudflare Worker) |
| .github/workflows/deploy.yml | GitHub installs and updates the server for you |

## GitHub secrets (Settings → Secrets and variables → Actions)
CLOUDFLARE_API_TOKEN, HELIUS_API_KEY, SOLANA_TRACKER_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, APP_PASSCODE

Your keys live only in GitHub Secrets and Cloudflare (both encrypted). Nothing secret is in these files, so the repo can be public.

## Telegram commands
/top · /week · /list · /add · /remove · /trader · /auto on|off|15 · /min 0.5 · /pause · /resume · /status · /fix

## Free limits
- **Helius:** 1M credits a month (1 credit per trade event).
- **Solana Tracker:** 10,000 requests a month (the server caches results).
- **Cloudflare:** 100,000 requests and 1,000 storage writes a day. The server caps itself at 900 writes; alerts keep going above that.

## Updating later
Upload a new file with the same name to the repo. A new worker.js redeploys the server by itself.
