# Naver Real Estate

Flask + Playwright app for browsing urgent commercial listings from Naver Real Estate: shops, offices, and land.

## Current Production Architecture

The production web app is served by Vercel. The crawler runs on the Mac mini and
writes to the `commercial_v2` schema in Neon Postgres.

```text
Mac mini launchd -> Naver crawl -> Neon DB (commercial_v2 schema)
Vercel web app   -> Neon DB (commercial_v2 schema)
```

Production URL:

- Vercel: https://naver-real-estate-v2.vercel.app

## GitHub Branches

- `main`: stable production branch for the current 1.x app.
- `version-2.0`: separate development branch for the 2.0 version.

Keep production fixes on `main`. Start larger redesigns, architecture changes,
and experimental 2.0 work from `version-2.0`.

## Local Worktree Layout

Local development uses separate folders so production maintenance and 2.0 work
do not fight over the same checkout:

```text
/Users/haluna/workspace/naver-real-estate     -> main         (crawler host: .venv-migrate, daily 09:00 launchd job)
/Users/haluna/workspace/naver-real-estate-v1  -> main-kakao    (1.x development and Vercel deploys)
/Users/haluna/workspace/naver-real-estate-v2  -> version-2.0   (commercial 2.0, daily 10:00 launchd job)
```

`naver-real-estate-v1` and `-v2` are git worktrees of the first folder, so they
share one object store.

Useful commands:

```bash
git worktree list

cd /Users/haluna/workspace/naver-real-estate
git status -sb

cd /Users/haluna/workspace/naver-real-estate-v2
git status -sb
```

The v2 folder has its own ignored `.env.local` that defaults to local SQLite and
demo data. This avoids accidental writes to production Neon while experimenting.
Copy a real Neon URL into that file only when v2 intentionally needs to read the
production Neon database through the isolated `commercial_v2` schema.

## Deploy to Vercel + Neon

Vercel serves the dashboard and read-only API routes. It should not run the
Playwright crawler.

Required production environment variables:

```bash
DATABASE_URL=<Neon DATABASE_URL with sslmode=require>
DB_SCHEMA=commercial_v2
ENABLE_SCHEDULER=false
ENABLE_CRAWL_ENDPOINT=false
SEED_DEMO_DATA=false
ALLOW_DEMO_FALLBACK=false
SKIP_STARTUP_BACKFILL=true
LOCAL_CRAWL_SCHEDULE_HOUR=10
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

## Notes

- The app uses `DATABASE_URL` first and falls back to local SQLite via `DB_PATH`.
- Production crawling runs outside Vercel and writes directly to the same Neon database.
- Do not commit the real Neon URL. Use `.env.local` locally and Vercel secret env vars in hosting.

## Free Hosting Setup (Vercel + Neon Free + Mac mini Crawler)

This repository is configured for a no-cost personal deployment pattern:

- **Vercel** serves the Flask web app and read-only API routes.
- **Neon Free Postgres** stores persistent app and crawl data through `DATABASE_URL`.
- **Mac mini launchd** runs the crawler outside Vercel and writes to the same Neon database.

### 1. Create Neon Free Postgres

1. Create a Neon project on the Free plan.
2. Copy the pooled or direct Postgres connection string.
3. If Neon provides a URL with `postgres://`, it is acceptable for this app.

### 2. Configure the Vercel project

Set these environment variables in the Vercel project settings:

```bash
DATABASE_URL=<your-neon-postgres-url>
DB_SCHEMA=commercial_v2
ENABLE_SCHEDULER=false
SEED_DEMO_DATA=false
ALLOW_DEMO_FALLBACK=false
SKIP_STARTUP_BACKFILL=true
```

`DB_SCHEMA=commercial_v2` is what keeps commercial 2.0 data isolated from the
1.x residential app that shares the same Neon database. Do not commit the Neon
connection string to git. For local testing, copy `.env.example` to `.env.local`
and put the real `DATABASE_URL` there; `.env.local` is ignored by git.

### 3. Point the Mac mini crawler at Neon

Use the same Neon `DATABASE_URL` when running or installing the launchd crawler:

```bash
python3 scripts/run_remote_crawl.py --database-url "$DATABASE_URL"
```

For the scheduled crawler, reinstall the launchd job with the Neon URL using the commands in the Recommended Production Crawl Setup section below.

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
curl -fsSL https://naver-real-estate-v2.vercel.app/api/crawl-status
curl -fsSL 'https://naver-real-estate-v2.vercel.app/api/listings?page=1&per_page=5'
curl -sS -i -X POST https://naver-real-estate-v2.vercel.app/api/crawl
```

The POST to `/api/crawl` should return `403`.

Or run the full check script, which defaults to the production URL:

```bash
./scripts/verify_deploy.sh
```

The script validates:

1. `/api/crawl-status`: `source=demo` must not be `status=success`
2. `/api/crawl`: a `403` is expected on the web host; if enabled, success must come from `source=naver`
3. Re-check crawl status consistency
4. `/api/listings` sample endpoint health

## Publishing

Push the current branch after the working tree is clean:

```bash
scripts/publish.sh
```

Optional overrides:

```bash
REMOTE_NAME=origin BRANCH_NAME=main scripts/publish.sh
```

## Recommended Production Crawl Setup

Vercel should serve the web app only. Run the crawler from your local Mac mini and write directly to Neon or another external Postgres database.

The Mac mini checkout path used by these commands is `~/workspace/naver-real-estate` (`/Users/haluna/workspace/naver-real-estate`). Reinstall the launchd job from this workspace path after moving the checkout so the generated plist uses the new `WorkingDirectory` and wrapper path.

1. Keep `ENABLE_SCHEDULER=false` on the web host
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
tail -n 50 ~/workspace/naver-real-estate/logs/run_remote_crawl.wrapper.log
tail -n 50 ~/workspace/naver-real-estate/logs/launchd-crawl.err.log
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
