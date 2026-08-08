# 단지별 일별 매물수 추이 조회 (2단계) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `crawl_building_stats`에 쌓인 데이터를 매물 카드에서 클릭 한 번으로 조회할 수 있는 일별 매물수 추이 모달을 만든다.

**Architecture:** 매물 카드의 📈 아이콘 클릭 → `district`+`building_name`으로 신규 API(`/api/building-history`) 호출 → 기존에 존재하지만 어디서도 열리지 않던 `#modal-overlay`를 재활용해 히어로 미니 바 차트와 동일한 방식(순수 CSS/JS, 라이브러리 없음)으로 그린다. 백엔드는 `database.py`(원본 row 조회)와 `app.py`(날짜별 채움/가공 + 라우트)의 기존 책임 분리를 그대로 따른다.

**Tech Stack:** Flask, SQLite/Postgres (`ConnectionWrapper`를 통한 `?` placeholder), 바닐라 JS (빌드 도구/차트 라이브러리 없음).

## Global Constraints

- Base commit: `c35b041`
- 신규 의존성 추가 금지 (JS 차트 라이브러리 포함)
- 모든 SQL은 `?` placeholder만 사용 (SQLite/Postgres 양쪽에서 `ConnectionWrapper`가 변환)
- 건물 식별 키는 기존과 동일하게 **`district` + `building_name`**
- 조회 기간: 프런트엔드는 **14일 고정** 호출, API `days` 파라미터는 1~30으로 클램프
- 표시 지표: **매물 수(막대 차트) + 가격인하 건수(텍스트 요약)** 만 — 다른 지표 추가 금지
- 진입 경로는 **매물 카드의 📈 아이콘뿐** — 검색/자동완성 등 신규 UI 요소 금지
- 결과는 기존 `#modal-overlay`/`#modal-title`/`#modal-body` 재사용, 새 라우트/페이지 없음
- 전체 테스트 명령: `python3 -m unittest discover tests -v` (기존 24개 + 신규 테스트 모두 통과해야 함)
- 이 저장소에는 프런트엔드(JS) 자동화 테스트가 없다 — Task 3은 TDD가 아니라 구현 + 수동 검증(curl, 코드 리뷰)으로 마무리한다. 이는 계획의 누락이 아니라 저장소의 기존 상태를 반영한 것이다.

## File Structure

| File | Responsibility |
|---|---|
| `database.py` | (수정) `get_building_stats_history()` 추가 — 특정 건물의 성공/실제(non-demo) 크롤 세션 원본 row 조회 |
| `tests/test_building_snapshot.py` | (수정) `BuildingStatsHistoryTest` 클래스 추가 — Task 1 DB 메서드 테스트 |
| `app.py` | (수정) `build_building_history_series()` + `GET /api/building-history` 라우트 추가 |
| `tests/test_building_history.py` | (신규) Task 2 series builder + API 라우트 테스트 |
| `static/js/app.js` | (수정) 카드 템플릿에 추이 버튼, 클릭 델리게이터 확장, 모달 렌더러/오픈 함수, 모달 닫기 배선(close 버튼 + 오버레이 클릭) |
| `static/css/style.css` | (수정) `.building-trend-*` 클래스 추가 (히어로 미니 차트와 병렬 구조, 의도적 중복) |

---

### Task 1: DB 계층 — `get_building_stats_history()`

**Files:**
- Modify: `database.py` (get_trends() 메서드 뒤, 현재 1511~1610행)
- Test: `tests/test_building_snapshot.py`

**Interfaces:**
- Produces: `Database.get_building_stats_history(district: str, building_name: str, limit: int = 90) -> List[Dict]` — 각 dict는 `session_id`, `total_count`, `price_down_count`, `crawled_at` 키를 가짐. `crawled_at` 내림차순 정렬.

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_building_snapshot.py`의 `if __name__ == "__main__":` 블록 바로 위에 새 테스트 클래스를 추가한다:

```python
class BuildingStatsHistoryTest(unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = Database(
            db_path=os.path.join(tmpdir.name, "test.db"),
            skip_price_backfill=True,
        )
        self.addCleanup(self.db.close)

    def _insert_history(self, session_id, crawled_at, status="success", source="naver"):
        with self.db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO crawl_history
                (session_id, crawled_at, total_count, urgent_count, status, source)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (session_id, crawled_at, 10, 10, status, source),
            )

    def _insert_stats(self, session_id, district, building_name, total_count, price_down_count, created_at):
        with self.db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO crawl_building_stats
                (session_id, region, district, building_name, total_count, price_down_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (session_id, "서울특별시", district, building_name, total_count, price_down_count, created_at),
            )

    def test_returns_only_matching_district_and_building_name(self):
        self._insert_history("s1", "2026-08-06T09:00:00")
        self._insert_stats("s1", "서초구", "래미안원베일리", 5, 1, "2026-08-06T09:00:00")
        self._insert_stats("s1", "서초구", "반포자이", 3, 0, "2026-08-06T09:00:00")

        rows = self.db.get_building_stats_history("서초구", "래미안원베일리")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["total_count"], 5)

    def test_excludes_failed_and_demo_sessions(self):
        self._insert_history("s-failed", "2026-08-06T09:00:00", status="failed")
        self._insert_stats("s-failed", "서초구", "래미안원베일리", 5, 1, "2026-08-06T09:00:00")
        self._insert_history("s-demo", "2026-08-07T09:00:00", source="demo")
        self._insert_stats("s-demo", "서초구", "래미안원베일리", 6, 1, "2026-08-07T09:00:00")

        rows = self.db.get_building_stats_history("서초구", "래미안원베일리")
        self.assertEqual(rows, [])

    def test_orders_by_crawled_at_desc(self):
        self._insert_history("s-old", "2026-08-05T09:00:00")
        self._insert_stats("s-old", "서초구", "래미안원베일리", 4, 0, "2026-08-05T09:00:00")
        self._insert_history("s-new", "2026-08-06T09:00:00")
        self._insert_stats("s-new", "서초구", "래미안원베일리", 5, 1, "2026-08-06T09:00:00")

        rows = self.db.get_building_stats_history("서초구", "래미안원베일리")
        self.assertEqual([row["session_id"] for row in rows], ["s-new", "s-old"])

    def test_limit_caps_returned_rows(self):
        for i in range(3):
            session_id = f"s{i}"
            crawled_at = f"2026-08-0{i + 4}T09:00:00"
            self._insert_history(session_id, crawled_at)
            self._insert_stats(session_id, "서초구", "래미안원베일리", 5 + i, 0, crawled_at)

        rows = self.db.get_building_stats_history("서초구", "래미안원베일리", limit=2)
        self.assertEqual(len(rows), 2)
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `python3 -m unittest tests.test_building_snapshot.BuildingStatsHistoryTest -v`
Expected: FAIL — `AttributeError: 'Database' object has no attribute 'get_building_stats_history'`

- [ ] **Step 3: 최소 구현 작성**

`database.py`의 `get_trends()` 메서드(현재 1511행부터 시작, `return [dict(row) for row in rows]`로 끝나는 블록) 바로 뒤에 추가:

```python
    def get_building_stats_history(self, district: str, building_name: str, limit: int = 90):
        limit = max(1, int(limit or 90))
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT cb.session_id, cb.total_count, cb.price_down_count, ch.crawled_at
                FROM crawl_building_stats cb
                JOIN crawl_history ch ON ch.session_id = cb.session_id
                WHERE cb.district = ? AND cb.building_name = ?
                  AND ch.status = 'success'
                  AND COALESCE(ch.source, 'naver') <> 'demo'
                ORDER BY ch.crawled_at DESC
                LIMIT ?
                """,
                (district, building_name, limit),
            ).fetchall()
        return [dict(row) for row in rows]
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `python3 -m unittest tests.test_building_snapshot -v`
Expected: PASS (기존 8개 + 신규 4개 = 12개 전부 통과)

- [ ] **Step 5: 커밋**

```bash
git add database.py tests/test_building_snapshot.py
git commit -m "feat: add get_building_stats_history() for per-building daily trend lookup"
```

---

### Task 2: `app.py` — series builder + API 라우트

**Files:**
- Modify: `app.py` (`build_daily_crawl_series()` 뒤, 현재 344~384행 / `/api/crawl-daily-series` 라우트 뒤, 현재 682~685행)
- Test: `tests/test_building_history.py` (신규)

**Interfaces:**
- Consumes: `db.get_building_stats_history(district, building_name, limit) -> List[Dict]` (Task 1), `coerce_kst_datetime(value)` (app.py 기존, 264행), `KST` (app.py 기존 상수), `cacheable_json(payload, max_age)` (app.py 기존), `serialize_api_value(value)` (app.py 기존)
- Produces: `build_building_history_series(district: str, building_name: str, days: int = 14) -> List[Dict]` — 각 dict는 `date`, `label`, `total_count`(int 또는 None), `price_down_count`(int 또는 None). 길이는 항상 `days`. 라우트 `GET /api/building-history`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_building_history.py` 신규 생성:

```python
"""단지별 일별 매물수 추이 — app.py 계층 + API 라우트 테스트."""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


class BuildingHistoryAPITest(unittest.TestCase):
    """app.py를 임포트하기 전에 환경변수로 로컬 SQLite 모드를 강제한다."""

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmpdir.name, "building-history-test.db")

        os.environ["FORCE_LOCAL_SQLITE"] = "1"
        os.environ["DATABASE_URL"] = ""
        os.environ["DB_PATH"] = db_path
        os.environ["ENABLE_SCHEDULER"] = "false"
        os.environ["SEED_DEMO_DATA"] = "false"

        import app as app_module  # noqa: E402

        cls.app_module = app_module
        cls.client = app_module.app.test_client()
        cls.today = datetime.now(app_module.KST).date()

    @classmethod
    def tearDownClass(cls):
        cls.app_module.db.close()
        cls._tmpdir.cleanup()

    def setUp(self):
        # 각 테스트 전 통계 관련 테이블을 비워 테스트 간 데이터가 섞이지 않게 한다.
        with self.app_module.db.get_connection() as conn:
            conn.execute("DELETE FROM crawl_building_stats")
            conn.execute("DELETE FROM crawl_history")

    def _seed_day(self, session_id, days_ago, district, building_name, total_count, price_down_count):
        crawled_at = datetime.combine(
            self.today - timedelta(days=days_ago), datetime.min.time()
        ).replace(hour=9).isoformat()
        with self.app_module.db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO crawl_history
                (session_id, crawled_at, total_count, urgent_count, status, source)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (session_id, crawled_at, total_count, total_count, "success", "naver"),
            )
            conn.execute(
                """
                INSERT INTO crawl_building_stats
                (session_id, region, district, building_name, total_count, price_down_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (session_id, "서울특별시", district, building_name, total_count, price_down_count, crawled_at),
            )

    def test_series_fills_missing_days_with_none(self):
        self._seed_day("s-2days", 2, "서초구", "래미안원베일리", 5, 1)
        self._seed_day("s-today", 0, "서초구", "래미안원베일리", 7, 2)

        series = self.app_module.build_building_history_series("서초구", "래미안원베일리", days=5)

        self.assertEqual(len(series), 5)
        self.assertEqual(series[-1]["total_count"], 7)  # 오늘
        self.assertEqual(series[-3]["total_count"], 5)  # 2일 전
        self.assertIsNone(series[-2]["total_count"])    # 1일 전 데이터 없음
        self.assertIsNone(series[0]["total_count"])     # 4일 전 데이터 없음

    def test_series_picks_latest_session_within_same_day(self):
        self._seed_day("s-morning", 0, "서초구", "래미안원베일리", 5, 0)
        with self.app_module.db.get_connection() as conn:
            later = datetime.combine(self.today, datetime.min.time()).replace(hour=18).isoformat()
            conn.execute(
                """
                INSERT INTO crawl_history
                (session_id, crawled_at, total_count, urgent_count, status, source)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("s-evening", later, 8, 8, "success", "naver"),
            )
            conn.execute(
                """
                INSERT INTO crawl_building_stats
                (session_id, region, district, building_name, total_count, price_down_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("s-evening", "서울특별시", "서초구", "래미안원베일리", 8, 3, later),
            )

        series = self.app_module.build_building_history_series("서초구", "래미안원베일리", days=3)
        self.assertEqual(series[-1]["total_count"], 8)

    def test_series_for_unknown_building_is_all_none(self):
        series = self.app_module.build_building_history_series("서초구", "존재하지않는단지", days=5)
        self.assertEqual(len(series), 5)
        self.assertTrue(all(item["total_count"] is None for item in series))

    def test_route_requires_district_and_building_name(self):
        response = self.client.get("/api/building-history")
        self.assertEqual(response.status_code, 400)
        payload = response.get_json()
        self.assertEqual(payload["status"], "error")

    def test_route_returns_series_with_cache_header(self):
        self._seed_day("s-today", 0, "서초구", "래미안원베일리", 5, 1)

        response = self.client.get(
            "/api/building-history",
            query_string={"district": "서초구", "building_name": "래미안원베일리", "days": 5},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["district"], "서초구")
        self.assertEqual(payload["building_name"], "래미안원베일리")
        self.assertEqual(len(payload["days"]), 5)
        self.assertEqual(payload["days"][-1]["total_count"], 5)
        self.assertIn("max-age=300", response.headers.get("Cache-Control", ""))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `python3 -m unittest tests.test_building_history -v`
Expected: FAIL — `AttributeError: module 'app' has no attribute 'build_building_history_series'` (라우트 테스트는 404로 먼저 실패할 수 있음)

- [ ] **Step 3: 최소 구현 작성**

`app.py`의 `build_daily_crawl_series()` 함수(344~384행) 바로 뒤, `build_push_payload()` 함수 앞에 추가:

```python
def build_building_history_series(district, building_name, days=14):
    days = max(1, min(int(days or 14), 30))
    today = datetime.now(KST).date()
    rows = db.get_building_stats_history(district, building_name, limit=max(days * 6, 30))
    latest_by_day = {}

    for row in rows:
        crawled_at = coerce_kst_datetime(row.get("crawled_at"))
        if not crawled_at:
            continue
        day_key = crawled_at.date().isoformat()
        if day_key in latest_by_day:
            continue
        latest_by_day[day_key] = {
            "date": day_key,
            "label": f"{crawled_at.month}.{crawled_at.day}",
            "total_count": row.get("total_count"),
            "price_down_count": row.get("price_down_count"),
        }

    series = []
    for offset in range(days - 1, -1, -1):
        target = today - timedelta(days=offset)
        key = target.isoformat()
        series.append(
            latest_by_day.get(key)
            or {
                "date": key,
                "label": f"{target.month}.{target.day}",
                "total_count": None,
                "price_down_count": None,
            }
        )
    return series
```

`app.py`의 `/api/crawl-daily-series` 라우트(682~685행) 바로 뒤, `build_regions_payload()` 함수 앞에 추가:

```python
@app.route("/api/building-history")
def get_building_history():
    district = (request.args.get("district") or "").strip()
    building_name = (request.args.get("building_name") or "").strip()
    if not district or not building_name:
        return jsonify({"status": "error", "message": "district and building_name required"}), 400
    days = request.args.get("days", default=14, type=int)
    series = build_building_history_series(district, building_name, days)
    return cacheable_json(
        {
            "district": district,
            "building_name": building_name,
            "days": serialize_api_value(series),
        },
        max_age=300,
    )
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `python3 -m unittest tests.test_building_history -v`
Expected: PASS (5개 전부 통과)

Run: `python3 -m unittest discover tests -v`
Expected: PASS (기존 24 + Task 1의 4 + Task 2의 5 = 33개 전부 통과)

- [ ] **Step 5: 커밋**

```bash
git add app.py tests/test_building_history.py
git commit -m "feat: add build_building_history_series() and /api/building-history route"
```

---

### Task 3: 프런트엔드 — 카드 추이 버튼 + 모달

**Files:**
- Modify: `static/js/app.js` (카드 템플릿 ~827행, 클릭 델리게이터 ~1653~1658행, 키다운 리스너 ~1752행 부근에 모달 닫기 배선 추가, `renderHeroDailySeries` 함수 뒤 ~295행에 신규 함수 추가)
- Modify: `static/css/style.css` (`#modal-body` 규칙 뒤 ~1025행에 로딩/빈 상태 클래스, `.hero-mini-chart-day.missing` 규칙 뒤 ~1504행에 `.building-trend-*` 클래스)

**Interfaces:**
- Consumes: `GET /api/building-history?district=&building_name=&days=14` (Task 2), 기존 `api(path)` 헬퍼, 기존 `escHtml(str)`/`fmtNum(n)` 헬퍼, 기존 `#modal-overlay`/`#modal-title`/`#modal-body`/`#modal-close` DOM 엘리먼트
- Produces: 없음 (터미널 UI 기능 — 이후 태스크에서 소비되지 않음)

이 태스크는 이 저장소에 프런트엔드 자동화 테스트가 없으므로 TDD 사이클이 아니라
구현 → 로컬 실행 → 수동 검증 순서로 진행한다.

- [ ] **Step 1: 매물 카드에 추이 버튼 추가**

`static/js/app.js`에서 카드 템플릿의 다음 줄을 찾는다 (현재 827행):

```javascript
      <div class="card-name" title="${escHtml(l.building_name)}">${escHtml(l.building_name)}</div>
```

다음으로 교체:

```javascript
      <div class="card-name-row">
        <div class="card-name" title="${escHtml(l.building_name)}">${escHtml(l.building_name)}</div>
        ${l.district && l.building_name ? `<button type="button" class="card-trend-btn" data-district="${escHtml(l.district)}" data-building-name="${escHtml(l.building_name)}" title="일별 매물수 추이">📈</button>` : ''}
      </div>
```

(`district`/`building_name`이 비어 있으면 어차피 `crawl_building_stats`에 기록되지
않으므로 버튼 자체를 렌더링하지 않는다.)

- [ ] **Step 2: 클릭 델리게이터에서 추이 버튼을 먼저 가로채기**

같은 파일에서 다음 블록을 찾는다 (현재 1653~1658행):

```javascript
  document.getElementById('listings-grid').addEventListener('click', e => {
    const card = e.target.closest('.listing-card');
    if (!card) return;
    const url = card.dataset.naverUrl;
    if (url) window.open(url, '_blank', 'noopener,noreferrer');
  });
```

다음으로 교체:

```javascript
  document.getElementById('listings-grid').addEventListener('click', e => {
    const trendBtn = e.target.closest('.card-trend-btn');
    if (trendBtn) {
      e.stopPropagation();
      openBuildingTrendModal(trendBtn.dataset.district, trendBtn.dataset.buildingName);
      return;
    }
    const card = e.target.closest('.listing-card');
    if (!card) return;
    const url = card.dataset.naverUrl;
    if (url) window.open(url, '_blank', 'noopener,noreferrer');
  });
```

- [ ] **Step 3: 모달 닫기 배선 추가 (close 버튼 + 오버레이 바깥 클릭)**

같은 초기화 블록 안, Step 2에서 수정한 리스너 바로 뒤에 추가:

```javascript
  document.getElementById('modal-close')?.addEventListener('click', () => {
    document.getElementById('modal-overlay')?.classList.add('hidden');
  });
  document.getElementById('modal-overlay')?.addEventListener('click', e => {
    if (e.target.id === 'modal-overlay') e.target.classList.add('hidden');
  });
```

(기존 Escape 키 리스너, 현재 1752행 부근은 이미 `#modal-overlay`에 `hidden`을
추가하므로 그대로 둔다 — 세 가지 닫기 방법 모두 동일한 방식으로 수렴한다.)

- [ ] **Step 4: 모달 오픈 함수 + 렌더러 추가**

`renderHeroDailySeries()` 함수(현재 225~295행) 바로 뒤에 추가:

```javascript
async function openBuildingTrendModal(district, buildingName) {
  const overlay = document.getElementById('modal-overlay');
  const titleEl = document.getElementById('modal-title');
  const bodyEl = document.getElementById('modal-body');
  if (!overlay || !titleEl || !bodyEl) return;

  titleEl.textContent = buildingName;
  bodyEl.innerHTML = '<div class="building-trend-loading">불러오는 중…</div>';
  overlay.classList.remove('hidden');

  try {
    const data = await api(`/api/building-history?district=${encodeURIComponent(district)}&building_name=${encodeURIComponent(buildingName)}&days=14`);
    bodyEl.innerHTML = renderBuildingTrendBody(data.days || []);
  } catch (err) {
    bodyEl.innerHTML = '<div class="building-trend-empty">추이를 불러오지 못했습니다.</div>';
  }
}

function renderBuildingTrendBody(items) {
  const validItems = items.filter(item => Number.isFinite(Number(item.total_count)));

  if (!validItems.length) {
    return '<div class="building-trend-empty">데이터를 모으는 중입니다. 내일부터 표시됩니다.</div>';
  }

  const values = validItems.map(item => Number(item.total_count));
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const range = Math.max(maxValue - minValue, 1);
  const latest = validItems[validItems.length - 1];

  let previousValidValue = null;
  const bars = items.map(item => {
    const value = Number(item.total_count);
    const hasValue = Number.isFinite(value);
    let directionClass = 'flat';
    if (hasValue && previousValidValue != null) {
      if (value > previousValidValue) directionClass = 'up';
      else if (value < previousValidValue) directionClass = 'down';
    }
    const height = hasValue
      ? Math.max(18, Math.round(((value - minValue) / range) * 60) + 18)
      : 12;
    const tooltip = hasValue ? `${item.label} ${fmtNum(value)}건` : `${item.label} 데이터 없음`;
    if (hasValue) previousValidValue = value;
    return `
      <div class="building-trend-bar-wrap has-tooltip" data-tooltip="${escHtml(tooltip)}">
        <div class="building-trend-bar ${hasValue ? directionClass : 'missing'}" style="height:${height}px"></div>
      </div>
    `;
  }).join('');

  const labels = items.map(item => `<span class="building-trend-day">${escHtml(item.label || '')}</span>`).join('');

  return `
    <div class="building-trend-summary">
      현재 매물수 <strong>${fmtNum(Number(latest.total_count || 0))}건</strong>
      · 가격인하 <strong>${fmtNum(Number(latest.price_down_count || 0))}건</strong>
    </div>
    <div class="building-trend-chart">${bars}</div>
    <div class="building-trend-labels">${labels}</div>
  `;
}
```

- [ ] **Step 5: CSS 추가**

`static/css/style.css`의 `#modal-body { padding: 18px; overflow-y: auto; }` 규칙
(현재 1025행) 바로 뒤에 추가:

```css
.card-name-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}
.card-trend-btn {
  flex-shrink: 0;
  background: none;
  font-size: 13px;
  line-height: 1;
  padding: 4px 6px;
  border-radius: var(--radius);
  color: var(--text3);
  transition: all var(--transition);
}
.card-trend-btn:hover { background: var(--bg3); color: var(--text); }

.building-trend-loading,
.building-trend-empty {
  color: var(--text3);
  font-size: 12px;
  padding: 8px 0;
}

.building-trend-summary {
  font-size: 13px;
  color: var(--text);
  margin-bottom: 12px;
}
.building-trend-summary strong { font-family: var(--font-mono); }

.building-trend-chart {
  display: grid;
  grid-template-columns: repeat(14, minmax(0, 1fr));
  gap: 4px;
  align-items: end;
  min-height: 94px;
}
.building-trend-bar-wrap {
  display: grid;
  justify-items: center;
  align-content: end;
  min-height: 94px;
  position: relative;
}
.building-trend-bar {
  width: 100%;
  max-width: 16px;
  min-height: 12px;
  border-radius: 8px 8px 4px 4px;
  background: linear-gradient(180deg, color-mix(in srgb, var(--blue-soft) 88%, white 12%), color-mix(in srgb, var(--blue-soft) 34%, var(--bg4) 66%));
}
.building-trend-bar.up {
  background: linear-gradient(180deg, color-mix(in srgb, var(--green-soft) 88%, white 12%), color-mix(in srgb, var(--green-soft) 38%, var(--bg4) 62%));
}
.building-trend-bar.down {
  background: linear-gradient(180deg, color-mix(in srgb, var(--coral) 90%, white 10%), color-mix(in srgb, var(--coral) 42%, var(--bg4) 58%));
}
.building-trend-bar.missing {
  background: color-mix(in srgb, var(--border) 84%, transparent);
  opacity: 0.7;
}
.building-trend-labels {
  display: grid;
  grid-template-columns: repeat(14, minmax(0, 1fr));
  gap: 4px;
  margin-top: 6px;
}
.building-trend-day {
  text-align: center;
  font-size: 9px;
  color: var(--text3);
}
```

(히어로 미니 차트의 시각 언어를 그대로 따르되, 14개 막대를 좁은 모달 폭(560px)에
맞춰 막대 최대 폭만 28px→16px로 줄인다. 나머지 클래스가 `.hero-mini-*`와 겹치지
않도록 `.building-trend-*` 네임스페이스로 완전히 분리한다 — 설계 문서에서 명시한
의도적 중복.)

- [ ] **Step 6: 수동 검증**

로컬 서버 실행:

```bash
FORCE_LOCAL_SQLITE=1 python3 app.py
```

터미널에서 API 응답 형태 확인 (더미 데이터가 없다면 `days` 배열 항목이 전부
`total_count: null`인 것이 정상):

```bash
curl -s "http://127.0.0.1:5000/api/building-history?district=서초구&building_name=래미안원베일리&days=14" | python3 -m json.tool
```

브라우저에서 확인:
1. `http://127.0.0.1:5000` 접속, 매물 카드의 단지명 옆 📈 아이콘 클릭
2. 모달이 열리고 로딩 → (데이터가 없다면) "데이터를 모으는 중입니다" 메시지가
   뜨는지 확인
3. `#modal-close` 클릭, 모달 바깥 클릭, Escape 키 세 가지 방법 모두 모달이
   닫히는지 확인
4. 카드의 나머지 영역(추이 버튼이 아닌 곳) 클릭 시 기존처럼 네이버 매물 페이지가
   새 탭으로 열리는지 확인 (회귀 없음)

- [ ] **Step 7: 전체 테스트 스위트 재확인 (회귀 없음 확인)**

Run: `python3 -m unittest discover tests -v`
Expected: PASS (33개 전부 통과 — 프런트엔드만 수정했으므로 숫자 변화 없어야 함)

- [ ] **Step 8: 커밋**

```bash
git add static/js/app.js static/css/style.css
git commit -m "feat: add building daily trend modal triggered from listing cards"
```
