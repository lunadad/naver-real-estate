# Naver Real Estate

Flask + Playwright app for browsing urgent listings from Naver Real Estate.

## Current Production Architecture

The production web app is served by Vercel. The crawler runs on the Mac mini and
writes to Neon Postgres.

```text
Mac mini launchd -> Naver crawl -> Neon DB
Vercel web app   -> Neon DB
```

Production URL:

- Vercel: https://naver-real-estate-henna.vercel.app

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
share one object store. Both `naver-real-estate` and `naver-real-estate-v1` are
linked to the same Vercel project — keep them on the same commit so a deploy
from either folder produces the same result.

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
shared production database.

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

## Notes

- The app uses `DATABASE_URL` first and falls back to local SQLite via `DB_PATH`.
- Production crawling runs outside the web host and writes directly to Neon.
- Do not commit the real Neon URL. Use `.env.local` locally and Vercel env vars in hosting.

## Free Hosting Setup

This repo is set up for a low-cost/free hobby deployment shape:

1. Create a Neon Postgres project and copy the pooled or direct connection string.
2. Make sure the URL includes `sslmode=require`; the app and crawler also add it automatically if it is missing.
3. Set that connection string as `DATABASE_URL` in the Vercel project settings.
4. Keep the production env vars listed above.

Vercel serves the dashboard. The Mac mini runs the crawler and writes fresh rows
to Neon.

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

Vercel should serve the web app only. Run the crawler from your local Mac mini and write directly to Neon Postgres.

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
