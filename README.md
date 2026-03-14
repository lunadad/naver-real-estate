# Naver Real Estate

Flask + Playwright app for browsing urgent listings from Naver Real Estate.

## Deploy to Render

[![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/lunadad/naver-real-estate)

## Notes

- Render deployment is configured with `render.yaml`.
- The app uses SQLite, so the Render service is configured with a persistent disk mounted at `/var/data`.
- The default Render setup in this repo disables demo seeding and keeps the in-app scheduler enabled.
