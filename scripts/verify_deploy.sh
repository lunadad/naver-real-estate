#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-https://naver-real-estate.onrender.com}"

echo "[1/4] GET /api/crawl-status"
STATUS_JSON=$(curl -fsSL "$BASE_URL/api/crawl-status")
echo "$STATUS_JSON" | jq . >/dev/null

LAST_SOURCE=$(echo "$STATUS_JSON" | jq -r '.last_crawl.source // ""')
LAST_STATUS=$(echo "$STATUS_JSON" | jq -r '.last_crawl.status // ""')

echo "last source=$LAST_SOURCE, status=$LAST_STATUS"
if [[ "$LAST_SOURCE" == "demo" && "$LAST_STATUS" == "success" ]]; then
  echo "❌ FAIL: demo source must not be marked success"
  exit 1
fi

echo "[2/4] POST /api/crawl"
CRAWL_JSON=$(curl -fsSL -X POST "$BASE_URL/api/crawl")
echo "$CRAWL_JSON" | jq . >/dev/null
CRAWL_SOURCE=$(echo "$CRAWL_JSON" | jq -r '.source // ""')
CRAWL_STATUS=$(echo "$CRAWL_JSON" | jq -r '.status // ""')
echo "crawl source=$CRAWL_SOURCE, status=$CRAWL_STATUS"

if [[ "$CRAWL_STATUS" == "success" && "$CRAWL_SOURCE" != "naver" ]]; then
  echo "❌ FAIL: success crawl must come from naver source"
  exit 1
fi

if [[ "$CRAWL_STATUS" == "failed" && "$CRAWL_SOURCE" == "demo" ]]; then
  echo "❌ FAIL: failed crawl should not silently switch to demo"
  exit 1
fi

echo "[3/4] GET /api/crawl-status (again)"
STATUS2_JSON=$(curl -fsSL "$BASE_URL/api/crawl-status")
echo "$STATUS2_JSON" | jq . >/dev/null
LAST2_SOURCE=$(echo "$STATUS2_JSON" | jq -r '.last_crawl.source // ""')
LAST2_STATUS=$(echo "$STATUS2_JSON" | jq -r '.last_crawl.status // ""')
echo "last source=$LAST2_SOURCE, status=$LAST2_STATUS"
if [[ "$LAST2_SOURCE" == "demo" && "$LAST2_STATUS" == "success" ]]; then
  echo "❌ FAIL: demo source must not be marked success"
  exit 1
fi

echo "[4/4] GET /api/listings?page=1&per_page=5"
LISTINGS_JSON=$(curl -fsSL "$BASE_URL/api/listings?page=1&per_page=5")
echo "$LISTINGS_JSON" | jq . >/dev/null
COUNT=$(echo "$LISTINGS_JSON" | jq -r '.listings | length')
echo "listings sample count=$COUNT"
if [[ "$COUNT" -eq 0 ]]; then
  echo "⚠️ WARN: no listings in sample"
fi

echo "✅ PASS: deploy verification checks completed"
