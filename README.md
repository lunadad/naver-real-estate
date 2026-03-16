# Naver Real Estate

Flask + Playwright app for browsing urgent listings from Naver Real Estate.

## Deploy to Render

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/lunadad/naver-real-estate)

## Notes

- Render deployment is configured with `render.yaml`.
- The app uses `DATABASE_URL` first and falls back to local SQLite via `DB_PATH`.
- The default Render setup in this repo provisions a managed Postgres database.
- Production crawling should run outside Render and write directly to the managed Postgres database.

## Production Safety (Important)

To prevent fake/demo listings from replacing real crawl data in production:

- `SEED_DEMO_DATA=false`
- `ALLOW_DEMO_FALLBACK=false`

With this setup:

- Live crawl failure is recorded as `failed` (not silent success)
- Demo data is not injected into production listings
- `/api/crawl` returns a truthful `status` (`success|degraded|failed`)

## Deploy Verification Checklist

After each Render deploy, run:

```bash
./scripts/verify_deploy.sh https://naver-real-estate.onrender.com
```

The script validates:

1. `/api/crawl-status`: `source=demo` must not be `status=success`
2. `/api/crawl`: success should come from `source=naver`
3. Re-check crawl status consistency
4. `/api/listings` sample endpoint health

## Recommended Production Crawl Setup

Render should serve the web app only. Run the crawler from your local Mac mini and write directly to Render Postgres.

1. Keep Render `ENABLE_SCHEDULER=false`
2. On the Mac mini, activate the project virtualenv and run:

```bash
python3 scripts/run_remote_crawl.py --database-url "$DATABASE_URL"
```

3. To install a daily macOS `launchd` job for 09:00:

```bash
python3 scripts/install_launchd_crawl.py --database-url "$DATABASE_URL" --install
launchctl unload ~/Library/LaunchAgents/com.lunadad.naver-real-estate-crawl.plist 2>/dev/null || true
launchctl load ~/Library/LaunchAgents/com.lunadad.naver-real-estate-crawl.plist
launchctl start com.lunadad.naver-real-estate-crawl
```

The generated logs go to:

- `logs/launchd-crawl.out.log`
- `logs/launchd-crawl.err.log`

## Postgres Migration

- Generate or provision a Postgres `DATABASE_URL`.
- Local development can stay on SQLite. Production should set `DATABASE_URL`.
- To copy existing SQLite data into Postgres:

```bash
python3 scripts/migrate_sqlite_to_postgres.py --sqlite-path real_estate.db --database-url "$DATABASE_URL" --truncate
```

## Mobile Push

- Generate VAPID keys with `python3 scripts/generate_vapid_keys.py`.
- Set `VAPID_PUBLIC_KEY`, `VAPID_PRIVATE_KEY`, and `VAPID_SUBJECT` in your deployment environment.
- Mobile Web Push requires HTTPS. On iPhone/iPad, users must install the PWA to the home screen before enabling notifications.
