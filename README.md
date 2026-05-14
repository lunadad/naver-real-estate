# Naver Real Estate

Flask + Playwright app for browsing urgent listings from Naver Real Estate.

## Current Production Architecture

The production web app is served by Vercel. The crawler runs on the Mac mini and
writes to Neon Postgres. Render is kept as a backup deployment target.

```text
Mac mini launchd -> Naver crawl -> Neon DB
Vercel web app   -> Neon DB
Render           -> backup web deployment
```

Primary production URL:

- Vercel: https://naver-real-estate-henna.vercel.app

Backup URL:

- Render: https://naver-real-estate.onrender.com

## GitHub Branches

- `main`: stable production branch for the current 1.x app.
- `v2.0`: separate development branch for the 2.0 version.

Keep production fixes on `main`. Start larger redesigns, architecture changes,
and experimental 2.0 work from `v2.0`.

## Deploy to Vercel + Neon

Vercel serves the dashboard and read-only API routes. It should not run the
Playwright crawler.

Required production environment variables:

```bash
DATABASE_URL=<Neon DATABASE_URL with sslmode=require>
ENABLE_SCHEDULER=false
ENABLE_CRAWL_ENDPOINT=false
SEED_DEMO_DATA=false
ALLOW_DEMO_FALLBACK=false
LOCAL_CRAWL_SCHEDULE_HOUR=9
LOCAL_CRAWL_SCHEDULE_MINUTE=0
DB_POOL_MIN_SIZE=0
DB_POOL_MAX_SIZE=3
DB_POOL_TIMEOUT=10
PGCONNECT_TIMEOUT=10
```

Optional push notification variables:

```bash
VAPID_PUBLIC_KEY=<public key>
VAPID_PRIVATE_KEY=<private key>
VAPID_SUBJECT=mailto:<email>
```

Deploy from a logged-in Vercel CLI:

```bash
vercel deploy --prod
```

Verify after deploy:

```bash
curl -fsSL https://naver-real-estate-henna.vercel.app/api/crawl-status
curl -fsSL 'https://naver-real-estate-henna.vercel.app/api/listings?page=1&per_page=5'
```

On Vercel, `/api/crawl` should return `403` because crawling belongs to the Mac
mini launchd job.

## Backup Deploy to Render + Neon

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/lunadad/naver-real-estate)

## Notes

- Render deployment is configured with `render.yaml` as a free backup web service.
- Create the Postgres database separately in Neon, then provide its connection string as Render's `DATABASE_URL` secret.
- The app uses `DATABASE_URL` first and falls back to local SQLite via `DB_PATH`.
- Production crawling should run outside Render and write directly to the same Neon database.
- Do not commit the real Neon URL. Use `.env.local` locally and Render secret env vars in hosting.

## Free Hosting Setup

This repo is set up for a low-cost/free hobby deployment shape:

1. Create a Neon Postgres project and copy the pooled or direct connection string.
2. Make sure the URL includes `sslmode=require`; the app and crawler also add it automatically if it is missing.
3. Click the Render deploy button above.
4. When Render prompts for `DATABASE_URL`, paste the Neon connection string.
5. Keep these production env vars as configured in `render.yaml`:

```bash
ENABLE_SCHEDULER=false
SEED_DEMO_DATA=false
ALLOW_DEMO_FALLBACK=false
DB_POOL_MIN_SIZE=0
DB_POOL_MAX_SIZE=3
```

Vercel serves the primary dashboard. Render is kept as a backup dashboard. The
Mac mini runs the crawler and writes fresh rows to Neon.

## Production Safety (Important)

To prevent fake/demo listings from replacing real crawl data in production:

- `SEED_DEMO_DATA=false`
- `ALLOW_DEMO_FALLBACK=false`

With this setup:

- Live crawl failure is recorded as `failed` (not silent success)
- Demo data is not injected into production listings
- `/api/crawl` returns a truthful `status` (`success|degraded|failed`)

## Deploy Verification Checklist

After each primary Vercel deploy, run:

```bash
curl -fsSL https://naver-real-estate-henna.vercel.app/api/crawl-status
curl -fsSL 'https://naver-real-estate-henna.vercel.app/api/listings?page=1&per_page=5'
curl -sS -i -X POST https://naver-real-estate-henna.vercel.app/api/crawl
```

The POST to `/api/crawl` should return `403`.

After each backup Render deploy, run:

```bash
./scripts/verify_deploy.sh https://naver-real-estate.onrender.com
```

The script validates:

1. `/api/crawl-status`: `source=demo` must not be `status=success`
2. `/api/crawl`: success should come from `source=naver`
3. Re-check crawl status consistency
4. `/api/listings` sample endpoint health

## Render CLI Deploy

After `render login`, you can deploy by service name. The script defaults to `naver-real-estate`:

```bash
scripts/deploy_render.sh
```

If your Render service name is different, set it once:

```bash
export RENDER_SERVICE_NAME=<your-render-service-name>
```

Deploy a specific commit:

```bash
scripts/deploy_render.sh <commit-sha>
```

If auto-discovery fails, set the service ID directly:

```bash
export RENDER_SERVICE_ID=<your-render-web-service-id>
scripts/deploy_render.sh
```

Push and deploy the current branch in one step:

```bash
scripts/publish.sh
```

Optional overrides:

```bash
REMOTE_NAME=origin BRANCH_NAME=main scripts/publish.sh
```

## Recommended Production Crawl Setup

Vercel and Render should serve the web app only. Run the crawler from your local Mac mini and write directly to Neon Postgres.

1. Keep Render `ENABLE_SCHEDULER=false`
2. On the Mac mini, set the same Neon URL:

```bash
cp .env.example .env.local
# edit .env.local and paste the real Neon DATABASE_URL
```

3. Activate the project virtualenv and run:

```bash
python3 scripts/run_remote_crawl.py --database-url "$DATABASE_URL"
```

4. To install a daily macOS job for 09:00:

```bash
sudo python3 scripts/install_launchd_crawl.py --database-url "$DATABASE_URL" --install --mode daemon
sudo chown root:wheel /Library/LaunchDaemons/com.lunadad.naver-real-estate-crawl.plist
sudo chmod 644 /Library/LaunchDaemons/com.lunadad.naver-real-estate-crawl.plist
sudo launchctl bootout system/com.lunadad.naver-real-estate-crawl 2>/dev/null || true
sudo launchctl bootstrap system /Library/LaunchDaemons/com.lunadad.naver-real-estate-crawl.plist
sudo launchctl kickstart -k system/com.lunadad.naver-real-estate-crawl
```

The plist now invokes `scripts/run_remote_crawl.sh`, which resolves a usable
Python interpreter at runtime (preferring `$CRAWL_PYTHON_BIN`, then
`.venv/bin/python3`, `.venv-migrate/bin/python3`, Homebrew, and finally
`/usr/bin/python3`). Renaming or rebuilding the venv no longer requires
regenerating the plist. The wrapper writes its own decision log to
`logs/run_remote_crawl.wrapper.log`.

### Troubleshooting: daily crawl silently stopped

If the dashboard shows the last successful crawl frozen on a past date, the
launchd job is almost certainly failing before Python starts. Check, in order:

**For LaunchAgent mode (user-bound):**
```bash
tail -n 50 ~/naver-real-estate/logs/run_remote_crawl.wrapper.log
tail -n 50 ~/naver-real-estate/logs/launchd-crawl.err.log
```

**For LaunchDaemon mode (system-wide):**
```bash
sudo tail -n 50 /var/log/naver-real-estate/launchd-crawl.err.log
sudo tail -n 50 /var/log/naver-real-estate/run_remote_crawl.wrapper.log
```

If the wrapper log is empty or missing, the plist is still pointing at an
interpreter that no longer exists. Reinstall it with the commands above —
once on the new wrapper-based plist, this failure mode is gone.

**Daemon permission issue:** If launchd logs show `deny(1) file-read-data` or
`Operation not permitted` when accessing logs, the daemon mode is trying to
write to the user's home directory. The install script automatically uses
`/var/log/naver-real-estate` for daemon mode and `./logs` for agent mode to
avoid this sandbox issue.

If you intentionally want a user-login-bound LaunchAgent instead:

```bash
python3 scripts/install_launchd_crawl.py --database-url "$DATABASE_URL" --install --mode agent --run-at-load
launchctl bootout gui/$(id -u) ~/Library/LaunchAgents/com.lunadad.naver-real-estate-crawl.plist 2>/dev/null || true
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.lunadad.naver-real-estate-crawl.plist
launchctl kickstart -k gui/$(id -u)/com.lunadad.naver-real-estate-crawl
```

The generated logs go to:

- `logs/launchd-crawl.out.log`
- `logs/launchd-crawl.err.log`
- `logs/run_remote_crawl.log`

## Postgres Migration

- Generate or provision a Neon/Postgres `DATABASE_URL`.
- Local development can stay on SQLite. Production should set `DATABASE_URL`.
- To copy existing SQLite data into Postgres:

```bash
python3 scripts/migrate_sqlite_to_postgres.py --sqlite-path real_estate.db --database-url "$DATABASE_URL" --truncate
```

## Mobile Push

- Generate VAPID keys with `python3 scripts/generate_vapid_keys.py`.
- Set `VAPID_PUBLIC_KEY`, `VAPID_PRIVATE_KEY`, and `VAPID_SUBJECT` in your deployment environment.
- Mobile Web Push requires HTTPS. On iPhone/iPad, users must install the PWA to the home screen before enabling notifications.
