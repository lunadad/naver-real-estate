# 매물 태그 검색 필터 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `listings.tags`에 저장된 임의의 태그(역세권·신축·대단지 등)를 사이드바에서 다중 선택해 매물 목록과 지도 마커를 동시에 필터링할 수 있게 한다.

**Architecture:** 태그 목록은 최신 크롤 세션의 `tags` JSON 컬럼을 Python에서 파싱해 집계(`get_tag_counts()`)하고 `GET /api/tags`로 노출한다. 필터링은 `tags LIKE '%"태그"%'` 조건을 OR로 묶어 기존 `get_listings()` / `get_map_listings()` 조건 리스트에 AND로 추가한다. 프런트엔드는 `state.filters.tags` 배열을 콤마로 직렬화해 기존 필터 전파 경로(`buildQuery()`, `refreshMapListings()`)에 그대로 태운다.

**Tech Stack:** Python 3.14 / Flask 3.0 / SQLite(로컬)·PostgreSQL(운영) 듀얼 드라이버 / Vanilla JS + 카카오맵 SDK / 테스트는 표준 라이브러리 `unittest`

## Global Constraints

- **베이스 브랜치:** `main-kakao`, 베이스 커밋 `86291c4` ("Migrate map from Leaflet to Kakao Maps SDK"). `get_map_listings()` / `/api/map-listings` / `FORCE_LOCAL_SQLITE` 는 그 커밋에서 온다.
- **커밋 범위:** 각 태스크의 커밋에는 해당 태스크가 건드린 파일만 `git add` 한다 (`git add -A` 금지).
- **새 런타임 의존성 추가 금지.** 테스트는 표준 라이브러리 `unittest`만 사용한다 (저장소에 pytest 없음, venv 없음).
- **DB 드라이버 중립:** SQL은 `?` 플레이스홀더만 쓴다 (`ConnectionWrapper._convert_sql`이 Postgres용 `%s`로 변환). SQLite JSON1 함수 사용 금지 — 운영은 Postgres다.
- **태그 매칭은 반드시 따옴표를 포함**한 `%"역세권"%` 형태로 바인딩한다. `%역세권%`은 다른 태그의 부분 문자열과 오탐한다.
- **태그 화이트리스트 검증 없음** (YAGNI). 존재하지 않는 태그는 결과 0건이 될 뿐이며, 값은 파라미터 바인딩되므로 인젝션 위험 없음.
- **기존 `price_down_only` 필터는 유지**한다. 태그 필터와 독립적으로 동작하며 두 조건은 AND로 결합된다.
- 다중 태그 선택은 **OR 매칭** (선택한 태그 중 하나라도 있으면 노출).
- 모든 사용자 노출 문자열은 한국어.
- 테스트 실행은 항상 저장소 루트에서 `python3 -m unittest tests.test_tag_filter -v`.

---

## File Structure

| 파일 | 역할 | 변경 |
|---|---|---|
| `database.py` | `get_tag_counts()` 신규, `get_listings()`/`get_map_listings()`에 `tags` 파라미터 | 수정 |
| `app.py` | `GET /api/tags` 신규, 기존 두 라우트에 `tags` 쿼리 파싱 | 수정 |
| `static/js/app.js` | `state.filters.tags`, `buildQuery()` 배열 직렬화, 태그 섹션 렌더/토글, 지도·라벨 미러링 | 수정 |
| `templates/index.html` | 사이드바 "태그 필터" 섹션 마크업 | 수정 |
| `static/css/style.css` | 태그 pill·빈 상태 스타일 | 수정 |
| `tests/test_tag_filter.py` | DB 계층 + API 계층 unittest | 신규 |

---

### Task 1: `get_tag_counts()` — 태그 집계

**Files:**
- Create: `tests/test_tag_filter.py`
- Modify: `database.py` (import 블록 1~8행, `get_listings()` 바로 앞 1193행 부근)

**Interfaces:**
- Consumes: 없음 (첫 태스크)
- Produces:
  - `Database._parse_tags(raw) -> List[str]` — `staticmethod`. JSON 문자열/리스트/`None`을 받아 공백 아닌 태그 문자열 리스트 반환.
  - `Database.get_tag_counts() -> List[Dict[str, object]]` — `[{"tag": str, "count": int}, ...]`, `count` 내림차순 → 태그명 오름차순.
  - `tests/test_tag_filter.py`의 `make_listing(article_no, tags, **overrides) -> dict` 헬퍼와 `TagFilterDBTest.setUp()`의 픽스처(아래 Task 2가 그대로 재사용).

- [ ] **Step 1: 실패하는 테스트를 작성한다**

`tests/test_tag_filter.py` 를 새로 만든다:

```python
"""태그 검색 필터 — DB/API 계층 테스트 (표준 라이브러리 unittest)."""

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Database  # noqa: E402


def make_listing(article_no, tags, **overrides):
    """insert_listings()가 기대하는 형태의 매물 dict를 만든다.

    tags는 파이썬 리스트로 넘긴다 (insert_listings 내부에서 json.dumps 된다).
    """
    listing = {
        "article_no": article_no,
        "region": "서울특별시",
        "district": "강남구",
        "property_type": "아파트",
        "trade_type": "매매",
        "price": "10억",
        "area": "84㎡",
        "floor": "10/20",
        "building_name": f"테스트아파트{article_no}",
        "description": "테스트 설명",
        "is_urgent": 1,
        "tags": tags,
        "confirmed_date": "20260724",
        "latitude": 37.5,
        "longitude": 127.0,
        "naver_url": f"https://example.com/{article_no}",
    }
    listing.update(overrides)
    return listing


class TagFilterDBTest(unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = Database(
            db_path=os.path.join(tmpdir.name, "test.db"),
            skip_price_backfill=True,
        )
        self.addCleanup(self.db.close)

        listings = [
            make_listing("A1", ["역세권", "신축"]),
            make_listing("A2", ["역세권", "가격인하"]),
            make_listing("A3", ["대단지"], property_type="오피스텔"),
            make_listing("A4", []),
        ]
        self.db.insert_listings(listings, "session-1")
        self.db.log_crawl("session-1", len(listings), len(listings), "success", "naver")

    def test_tag_counts_sorted_by_count_then_name(self):
        counts = self.db.get_tag_counts()
        self.assertEqual(
            counts,
            [
                {"tag": "역세권", "count": 2},
                {"tag": "가격인하", "count": 1},
                {"tag": "대단지", "count": 1},
                {"tag": "신축", "count": 1},
            ],
        )

    def test_tag_counts_ignores_empty_tag_lists(self):
        tags = [entry["tag"] for entry in self.db.get_tag_counts()]
        self.assertNotIn("", tags)


if __name__ == "__main__":
    unittest.main()
```

기대 순서 근거: `count` 내림차순(역세권 2건) → 동률은 태그명 오름차순(가격인하 < 대단지 < 신축, 한글 코드포인트 순).

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

```bash
python3 -m unittest tests.test_tag_filter -v
```

Expected: FAIL — `AttributeError: 'Database' object has no attribute 'get_tag_counts'`

- [ ] **Step 3: 최소 구현을 작성한다**

`database.py` 상단 import에 `Counter`를 추가한다. 기존 3행 `import logging` 아래가 아니라, 표준 라이브러리 from-import 블록 첫 줄로 넣는다 — 즉 `from datetime import ...` 바로 위:

```python
from collections import Counter
from datetime import date, datetime, timedelta
```

`database.py`의 `def get_listings(` (약 1194행) **바로 앞**에 다음 두 메서드를 추가한다:

```python
    @staticmethod
    def _parse_tags(raw) -> List[str]:
        """tags 컬럼(JSON 문자열 또는 리스트)을 태그 문자열 리스트로 파싱한다."""
        if isinstance(raw, list):
            values = raw
        elif not raw:
            return []
        else:
            try:
                values = json.loads(raw)
            except (TypeError, ValueError):
                return []
            if not isinstance(values, list):
                return []
        return [str(value).strip() for value in values if str(value).strip()]

    def get_tag_counts(self):
        """최신 세션 매물의 태그별 등장 횟수를 count 내림차순으로 반환한다.

        SQLite JSON1 확장을 쓰지 않고(운영 DB는 Postgres) 애플리케이션 레벨에서 파싱한다.
        """
        with self.get_connection() as conn:
            latest_session = self._get_latest_visible_session_id(conn)
            if latest_session:
                rows = conn.execute(
                    "SELECT tags FROM listings WHERE crawl_session = ?",
                    (latest_session,),
                ).fetchall()
            else:
                rows = conn.execute("SELECT tags FROM listings").fetchall()

        counter = Counter()
        for row in rows:
            counter.update(self._parse_tags(row["tags"]))

        return [
            {"tag": tag, "count": count}
            for tag, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
        ]
```

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

```bash
python3 -m unittest tests.test_tag_filter -v
```

Expected: `Ran 2 tests` … `OK`

- [ ] **Step 5: 커밋한다**

```bash
git add tests/test_tag_filter.py database.py
git commit -m "feat: add get_tag_counts() for tag aggregation"
```

---

### Task 2: `get_listings()` / `get_map_listings()` 태그 필터 파라미터

**Files:**
- Modify: `database.py` (`get_listings()` 시그니처 및 조건 블록, `get_map_listings()` 시그니처 및 조건 블록)
- Test: `tests/test_tag_filter.py` (`TagFilterDBTest`에 테스트 추가)

**Interfaces:**
- Consumes: Task 1의 `TagFilterDBTest.setUp()` 픽스처(A1=역세권+신축, A2=역세권+가격인하, A3=대단지/오피스텔, A4=태그없음), `make_listing()`.
- Produces:
  - `Database.get_listings(..., price_down_only=False, tags=None)` — `tags: list[str] | None`, 마지막 키워드 인자.
  - `Database.get_map_listings(..., price_down_only=False, tags=None, limit=500)` — `tags`는 `price_down_only`와 `limit` 사이.

- [ ] **Step 1: 실패하는 테스트를 작성한다**

`tests/test_tag_filter.py`의 `TagFilterDBTest` 클래스 안, `test_tag_counts_ignores_empty_tag_lists` 아래에 추가한다:

```python
    def article_nos(self, result):
        return sorted(row["article_no"] for row in result["listings"])

    def test_single_tag_filters_listings(self):
        result = self.db.get_listings(tags=["역세권"])
        self.assertEqual(self.article_nos(result), ["A1", "A2"])
        self.assertEqual(result["total"], 2)

    def test_multiple_tags_use_or_matching(self):
        result = self.db.get_listings(tags=["역세권", "대단지"])
        self.assertEqual(self.article_nos(result), ["A1", "A2", "A3"])

    def test_empty_tag_list_returns_everything(self):
        result = self.db.get_listings(tags=[])
        self.assertEqual(self.article_nos(result), ["A1", "A2", "A3", "A4"])

    def test_unknown_tag_returns_no_rows(self):
        result = self.db.get_listings(tags=["존재하지않는태그"])
        self.assertEqual(result["listings"], [])
        self.assertEqual(result["total"], 0)

    def test_tag_matching_is_exact_not_substring(self):
        """'역'은 '역세권'의 부분 문자열이지만 매칭되면 안 된다."""
        result = self.db.get_listings(tags=["역"])
        self.assertEqual(result["listings"], [])

    def test_tags_combine_with_other_filters_as_and(self):
        result = self.db.get_listings(tags=["역세권", "대단지"], property_type="오피스텔")
        self.assertEqual(self.article_nos(result), ["A3"])

    def test_tags_combine_with_price_down_only(self):
        result = self.db.get_listings(tags=["역세권"], price_down_only=True)
        self.assertEqual(self.article_nos(result), ["A2"])

    def test_map_listings_filtered_by_tags(self):
        rows = self.db.get_map_listings(tags=["대단지"])
        self.assertEqual([row["article_no"] for row in rows], ["A3"])

    def test_map_listings_without_tags_returns_all_geocoded(self):
        rows = self.db.get_map_listings()
        self.assertEqual(len(rows), 4)
```

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

```bash
python3 -m unittest tests.test_tag_filter -v
```

Expected: FAIL — `TypeError: Database.get_listings() got an unexpected keyword argument 'tags'`

- [ ] **Step 3: 최소 구현을 작성한다**

`database.py` `get_listings()` 시그니처 마지막에 `tags=None`을 추가한다:

```python
    def get_listings(
        self,
        region="",
        district="",
        property_type="",
        trade_type="",
        urgent_only=False,
        search="",
        page=1,
        per_page=20,
        sort_by="urgent",
        price_down_only=False,
        tags=None,
    ):
```

같은 메서드의 `if price_down_only:` 블록과 `if search:` 블록 **사이**에 태그 조건을 넣는다:

```python
        if price_down_only:
            conditions.append("tags LIKE '%가격인하%'")
        if tags:
            conditions.append("(" + " OR ".join(["tags LIKE ?"] * len(tags)) + ")")
            params.extend(f'%"{tag}"%' for tag in tags)
        if search:
```

`get_map_listings()` 시그니처의 `price_down_only=False,` 아래에 `tags=None,`을 추가한다:

```python
        price_down_only=False,
        tags=None,
        limit=500,
    ):
```

`get_map_listings()`에도 동일 위치(`price_down_only` 블록과 `search` 블록 사이)에 같은 3줄을 넣는다:

```python
        if price_down_only:
            conditions.append("tags LIKE '%가격인하%'")
        if tags:
            conditions.append("(" + " OR ".join(["tags LIKE ?"] * len(tags)) + ")")
            params.extend(f'%"{tag}"%' for tag in tags)
        if search:
```

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

```bash
python3 -m unittest tests.test_tag_filter -v
```

Expected: `Ran 11 tests` … `OK`

- [ ] **Step 5: 커밋한다**

```bash
git add tests/test_tag_filter.py database.py
git commit -m "feat: filter listings and map markers by tags"
```

---

### Task 3: `/api/tags` 라우트 + 기존 라우트 태그 파라미터 전달

**Files:**
- Modify: `app.py` (`cacheable_json()` 아래 헬퍼 추가, `/api/listings` 504행 부근, `/api/map-listings` 527행 부근, `/api/region-stats` 위 신규 라우트)
- Test: `tests/test_tag_filter.py` (`TagFilterAPITest` 클래스 신규)

**Interfaces:**
- Consumes: `Database.get_tag_counts()`, `Database.get_listings(tags=...)`, `Database.get_map_listings(tags=...)` (Task 1·2).
- Produces:
  - `app.parse_tag_args(raw: str) -> list[str]` — 콤마 구분 문자열을 공백 제거·빈 항목 제외한 리스트로.
  - `GET /api/tags` → `{"tags": [{"tag": str, "count": int}, ...]}`, `Cache-Control: public, max-age=300`.
  - `GET /api/listings?tags=역세권,신축`, `GET /api/map-listings?tags=역세권,신축` 지원.

- [ ] **Step 1: 실패하는 테스트를 작성한다**

`tests/test_tag_filter.py` 맨 아래 `if __name__ == "__main__":` **위**에 추가한다 (새 import는 필요 없다 — `os`·`tempfile`·`unittest`·`make_listing` 모두 Task 1에서 이미 있다):

```python
class TagFilterAPITest(unittest.TestCase):
    """app.py를 임포트하기 전에 환경변수로 로컬 SQLite 모드를 강제한다."""

    @classmethod
    def setUpClass(cls):
        cls._tmpdir = tempfile.TemporaryDirectory()
        db_path = os.path.join(cls._tmpdir.name, "api-test.db")

        os.environ["FORCE_LOCAL_SQLITE"] = "1"
        os.environ["DATABASE_URL"] = ""
        os.environ["DB_PATH"] = db_path
        os.environ["ENABLE_SCHEDULER"] = "false"
        os.environ["SEED_DEMO_DATA"] = "false"

        import app as app_module  # noqa: E402

        cls.app_module = app_module
        cls.client = app_module.app.test_client()

        listings = [
            make_listing("B1", ["역세권", "신축"]),
            make_listing("B2", ["대단지"]),
        ]
        app_module.db.insert_listings(listings, "api-session")
        app_module.db.log_crawl("api-session", 2, 2, "success", "naver")

    @classmethod
    def tearDownClass(cls):
        cls.app_module.db.close()
        cls._tmpdir.cleanup()

    def test_parse_tag_args_strips_and_drops_blanks(self):
        self.assertEqual(
            self.app_module.parse_tag_args(" 역세권 , ,신축,"),
            ["역세권", "신축"],
        )
        self.assertEqual(self.app_module.parse_tag_args(""), [])

    def test_tags_endpoint_returns_counts(self):
        response = self.client.get("/api/tags")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(
            payload["tags"],
            [
                {"tag": "대단지", "count": 1},
                {"tag": "신축", "count": 1},
                {"tag": "역세권", "count": 1},
            ],
        )

    def test_listings_endpoint_accepts_tags_param(self):
        payload = self.client.get("/api/listings?tags=대단지").get_json()
        self.assertEqual([row["article_no"] for row in payload["listings"]], ["B2"])

    def test_listings_endpoint_ignores_blank_tags_param(self):
        payload = self.client.get("/api/listings?tags=").get_json()
        self.assertEqual(payload["total"], 2)

    def test_map_listings_endpoint_accepts_tags_param(self):
        payload = self.client.get("/api/map-listings?tags=역세권").get_json()
        self.assertEqual([row["article_no"] for row in payload["listings"]], ["B1"])
```

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

```bash
python3 -m unittest tests.test_tag_filter -v
```

Expected: FAIL — `AttributeError: module 'app' has no attribute 'parse_tag_args'` 및 `/api/tags` 404

- [ ] **Step 3: 최소 구현을 작성한다**

`app.py`의 `cacheable_json()` 정의(180~183행) **바로 아래**, `db = Database(...)` 줄 **위**에 헬퍼를 추가한다:

```python
def parse_tag_args(raw: str):
    """콤마로 구분된 tags 쿼리 파라미터를 태그 리스트로 변환한다."""
    return [tag.strip() for tag in (raw or "").split(",") if tag.strip()]
```

`/api/listings` 핸들러의 `db.get_listings(...)` 호출 마지막 인자 뒤에 태그를 넘긴다:

```python
        price_down_only=request.args.get("price_down_only", "false").lower() == "true",
        tags=parse_tag_args(request.args.get("tags", "")),
    )
```

`/api/map-listings` 핸들러의 `db.get_map_listings(...)` 호출에서 `price_down_only=` 줄 아래에 넣는다:

```python
        price_down_only=request.args.get("price_down_only", "false").lower() == "true",
        tags=parse_tag_args(request.args.get("tags", "")),
        limit=limit,
    )
```

`/api/region-stats` 라우트(659~662행의 주석 `# Crawl data changes once a day...` 아래) 옆에 신규 라우트를 추가한다 — 같은 캐싱 정책 그룹에 속하므로 `get_trends()` 정의 **아래**에 붙인다:

```python
@app.route("/api/tags")
def get_tags():
    return cacheable_json(serialize_api_value({"tags": db.get_tag_counts()}), max_age=300)
```

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

```bash
python3 -m unittest tests.test_tag_filter -v
```

Expected: `Ran 16 tests` … `OK`

- [ ] **Step 5: 커밋한다**

```bash
git add tests/test_tag_filter.py app.py
git commit -m "feat: expose /api/tags and accept tags query param"
```

---

### Task 4: 사이드바 태그 필터 UI + 목록 반영

**Files:**
- Modify: `templates/index.html` (사이드바 `alert-section` 다음, "급매 증가 지역" 섹션 앞 — 124행 부근)
- Modify: `static/js/app.js` (`state.filters` 11~18행, `buildQuery()` 61~66행, `loadSidebar()` 855행, `wireEvents()` 1656행 부근, `init()` 1688행 부근)
- Modify: `static/css/style.css` (`.alert-panel-inner` 규칙 뒤, 344행 부근)

**Interfaces:**
- Consumes: `GET /api/tags` → `{"tags": [{"tag", "count"}]}` (Task 3), `GET /api/listings?tags=a,b` (Task 3).
- Produces:
  - `state.filters.tags` — 선택된 태그 문자열 배열.
  - `state.tagCounts` — `/api/tags` 응답 배열 캐시.
  - `renderTagFilter()` — `#tag-filter-list` DOM 렌더 함수.
  - `toggleTagFilter(tag)` — 토글 후 `loadListings()` 호출.
  - DOM id: `#tag-filter-section`, `#tag-filter-toggle`, `#tag-filter-panel`, `#tag-filter-list`, `#tag-filter-clear`.

- [ ] **Step 1: 사이드바 마크업을 추가한다**

`templates/index.html`의 `alert-section` div가 닫히는 `</div>` (122행) 다음, `<div class="sidebar-section">`(급매 증가 지역, 124행) **앞**에 삽입한다:

```html
    <div class="sidebar-section" id="tag-filter-section">
      <div class="section-title tag-filter-title">
        <span class="alert-title-left">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path d="M20.6 13.4 12 22l-9-9V3h10l7.6 7.6a2 2 0 0 1 0 2.8z"/>
            <circle cx="7.5" cy="7.5" r="1.5"/>
          </svg>
          태그 필터
        </span>
        <button id="tag-filter-toggle" class="section-collapse-btn" title="접기/펼치기">
          <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
            <polyline points="18 15 12 9 6 15"/>
          </svg>
        </button>
      </div>
      <div class="alert-panel" id="tag-filter-panel">
        <div class="tag-filter-inner">
          <div id="tag-filter-list" class="tag-filter-list">
            <span class="tag-filter-empty">태그를 불러오는 중...</span>
          </div>
          <button id="tag-filter-clear" class="tag-filter-clear hidden">선택 해제</button>
        </div>
      </div>
    </div>
```

- [ ] **Step 2: 스타일을 추가한다**

`static/css/style.css`의 `.alert-panel-inner { ... }` 규칙(339~344행) **바로 아래**에 추가한다:

```css
/* 태그 필터 */
.section-title.tag-filter-title { color: var(--accent); cursor: default; }

.tag-filter-inner {
  padding: 0 12px 6px;
  display: flex;
  flex-direction: column;
  gap: 8px;
}
.tag-filter-list {
  display: flex;
  flex-wrap: wrap;
  gap: 5px;
}
.tag-filter-pill {
  padding: 4px 9px;
  border-radius: 99px;
  background: var(--pill-bg);
  color: var(--text2);
  font-size: 11px;
  font-weight: 500;
  border: 1px solid transparent;
  cursor: pointer;
  transition: all var(--transition);
  white-space: nowrap;
}
.tag-filter-pill:hover { border-color: var(--border); color: var(--text); }
.tag-filter-pill.active {
  background: var(--pill-active-bg);
  color: var(--pill-active-text);
  border-color: transparent;
}
.tag-filter-pill .tag-filter-count {
  margin-left: 4px;
  opacity: 0.65;
  font-size: 10px;
}
.tag-filter-empty {
  font-size: 11px;
  color: var(--text3);
}
.tag-filter-clear {
  align-self: flex-start;
  background: none;
  border: none;
  padding: 0;
  font-size: 11px;
  color: var(--text3);
  cursor: pointer;
  text-decoration: underline;
}
.tag-filter-clear:hover { color: var(--text); }
```

- [ ] **Step 3: `state`와 `buildQuery()`를 확장한다**

`static/js/app.js`의 `state.filters`에 `tags: []`를 추가하고, `state`에 `tagCounts: []`를 추가한다:

```js
  filters: {
    trade_type: '',
    property_type: '',
    search: '',
    district: '',
    sort_by: 'price-desc',
    price_down_only: false,
    tags: [],
  },
  tagCounts: [],
```

`buildQuery()`를 배열 대응으로 교체한다 (빈 배열은 파라미터 자체를 생략해야 한다 — 기존 `v !== '' && v !== false` 조건만으로는 `tags=`가 붙는다):

```js
function buildQuery(extra = {}) {
  const p = { ...state.filters, page: state.page, per_page: state.perPage, ...extra };
  const q = new URLSearchParams();
  Object.entries(p).forEach(([k, v]) => {
    if (Array.isArray(v)) {
      if (v.length) q.set(k, v.join(','));
      return;
    }
    if (v !== '' && v !== false) q.set(k, v);
  });
  return q.toString();
}
```

- [ ] **Step 4: 태그 렌더·토글 함수를 추가한다**

`static/js/app.js`의 `loadSidebar()` 정의(855행) **바로 앞**에 추가한다:

```js
// ── Tag filter ───────────────────────────────────────────────────────────────
async function loadTagFilter() {
  try {
    const data = await api('/api/tags');
    state.tagCounts = data.tags || [];
  } catch (e) {
    console.warn('Tag list load error:', e);
    state.tagCounts = [];
  }
  renderTagFilter();
}

function renderTagFilter() {
  const list = document.getElementById('tag-filter-list');
  const clearBtn = document.getElementById('tag-filter-clear');
  if (!list) return;

  if (!state.tagCounts.length) {
    list.innerHTML = '<span class="tag-filter-empty">태그 데이터가 없습니다.</span>';
    if (clearBtn) clearBtn.classList.add('hidden');
    return;
  }

  list.innerHTML = state.tagCounts.map(({ tag, count }) => {
    const active = state.filters.tags.includes(tag) ? ' active' : '';
    return `<button class="tag-filter-pill${active}" data-tag="${escHtml(tag)}">`
      + `${escHtml(tag)}<span class="tag-filter-count">${fmtNum(count)}</span></button>`;
  }).join('');

  if (clearBtn) clearBtn.classList.toggle('hidden', state.filters.tags.length === 0);
}

function toggleTagFilter(tag) {
  const index = state.filters.tags.indexOf(tag);
  if (index >= 0) state.filters.tags.splice(index, 1);
  else state.filters.tags.push(tag);

  state.page = 1;
  renderTagFilter();
  loadListings();
}

function clearTagFilters() {
  if (!state.filters.tags.length) return;
  state.filters.tags = [];
  state.page = 1;
  renderTagFilter();
  loadListings();
}
```

- [ ] **Step 5: 이벤트를 연결하고 초기 로드에 태운다**

`wireEvents()`의 "Alert section collapse toggle" 블록(1656~1668행) **바로 아래**에 추가한다:

```js
  // Tag filter: pill 토글 · 선택 해제 · 접기/펼치기
  document.getElementById('tag-filter-list').addEventListener('click', (event) => {
    const pill = event.target.closest('.tag-filter-pill');
    if (!pill) return;
    toggleTagFilter(pill.dataset.tag);
  });
  document.getElementById('tag-filter-clear').addEventListener('click', clearTagFilters);

  const tagToggleBtn = document.getElementById('tag-filter-toggle');
  const tagPanelBody = document.getElementById('tag-filter-panel');
  if (localStorage.getItem('tagSectionCollapsed') === 'true') {
    tagPanelBody.classList.add('collapsed');
    tagToggleBtn.classList.add('collapsed');
  }
  tagToggleBtn.addEventListener('click', () => {
    const isNowCollapsed = tagPanelBody.classList.toggle('collapsed');
    tagToggleBtn.classList.toggle('collapsed', isNowCollapsed);
    localStorage.setItem('tagSectionCollapsed', isNowCollapsed);
  });
```

`init()`의 `primaryLoads` 배열에 `loadTagFilter()`를 추가한다:

```js
  const primaryLoads = [
    loadCrawlStatus(),
    loadListings(),
    loadSidebar(),
    loadTagFilter(),
  ];
```

- [ ] **Step 6: 구문 오류가 없는지 확인한다**

```bash
node --check static/js/app.js && python3 -c "
import re, pathlib
html = pathlib.Path('templates/index.html').read_text(encoding='utf-8')
for dom_id in ['tag-filter-section','tag-filter-toggle','tag-filter-panel','tag-filter-list','tag-filter-clear']:
    assert f'id=\"{dom_id}\"' in html, dom_id
print('index.html ids OK')
"
```

Expected: 출력 `index.html ids OK` (node --check는 성공 시 무출력)

- [ ] **Step 7: 커밋한다**

```bash
git add templates/index.html static/css/style.css static/js/app.js
git commit -m "feat: add sidebar tag filter UI"
```

---

### Task 5: 지도 마커와 필터 요약 라벨에 태그 반영

**Files:**
- Modify: `static/js/app.js` (`buildCurrentFilterLabel()` 101~110행, `refreshMapListings()` 540~566행)

**Interfaces:**
- Consumes: `state.filters.tags` (Task 4), `GET /api/map-listings?tags=a,b` (Task 3).
- Produces: 없음 (마지막 코드 태스크).

- [ ] **Step 1: 필터 요약 라벨에 태그를 추가한다**

`buildCurrentFilterLabel()`의 `if (state.filters.price_down_only) parts.push('가격인하만');` **아래**, `return` **위**에 추가한다:

```js
  if (state.filters.tags.length) parts.push(`태그 ${state.filters.tags.join(',')}`);
```

- [ ] **Step 2: 지도 매물 조회에 태그를 전달한다**

`refreshMapListings()`의 `if (state.filters.price_down_only) q.set('price_down_only', 'true');` **아래**에 추가한다:

```js
  if (state.filters.tags.length) q.set('tags', state.filters.tags.join(','));
```

- [ ] **Step 3: 구문 오류가 없는지 확인한다**

```bash
node --check static/js/app.js && grep -n "state.filters.tags" static/js/app.js
```

Expected: `node --check` 무출력, grep이 `buildCurrentFilterLabel` / `refreshMapListings` / `renderTagFilter` / `toggleTagFilter` / `clearTagFilters` / `buildQuery` 경로에서 최소 6줄 이상 출력

- [ ] **Step 4: 전체 테스트가 여전히 통과하는지 확인한다**

```bash
python3 -m unittest tests.test_tag_filter -v
```

Expected: `Ran 16 tests` … `OK`

- [ ] **Step 5: 커밋한다**

```bash
git add static/js/app.js
git commit -m "feat: mirror tag filter to map markers and filter summary"
```

---

### Task 6: 로컬 실행 검증 (수동)

**Files:**
- Create: `/Users/haluna/.claude/jobs/618c458c/tmp/seed_tag_demo.py` (저장소 밖 임시 스크립트 — 커밋하지 않는다)

**Interfaces:**
- Consumes: 앞의 모든 태스크.
- Produces: 없음 (검증 전용).

- [ ] **Step 1: 검증용 SQLite DB를 시드한다**

`/Users/haluna/.claude/jobs/618c458c/tmp/seed_tag_demo.py` 를 만든다:

```python
import sys
sys.path.insert(0, "/Users/haluna/workspace/naver-real-estate-v1")

from database import Database

DB_PATH = "/Users/haluna/.claude/jobs/618c458c/tmp/tagdemo.db"
TAG_SETS = [
    ["역세권", "신축"],
    ["역세권", "대단지"],
    ["역세권", "가격인하"],
    ["대단지", "주차가능"],
    ["남향"],
    [],
]

db = Database(db_path=DB_PATH, skip_price_backfill=True)
listings = []
for index, tags in enumerate(TAG_SETS, start=1):
    listings.append({
        "article_no": f"D{index}",
        "region": "서울특별시",
        "district": "강남구",
        "property_type": "아파트",
        "trade_type": "매매",
        "price": f"{10 + index}억",
        "area": "84㎡",
        "floor": "10/20",
        "building_name": f"데모아파트{index}",
        "description": "검증용 데모 매물",
        "is_urgent": 1,
        "tags": tags,
        "confirmed_date": "20260724",
        "latitude": 37.4979 + index * 0.002,
        "longitude": 127.0276 + index * 0.002,
        "naver_url": "https://example.com",
    })

db.insert_listings(listings, "demo-session")
db.log_crawl("demo-session", len(listings), len(listings), "success", "naver")
print("tag counts:", db.get_tag_counts())
db.close()
```

실행:

```bash
python3 /Users/haluna/.claude/jobs/618c458c/tmp/seed_tag_demo.py
```

Expected: `tag counts: [{'tag': '역세권', 'count': 3}, {'tag': '대단지', 'count': 2}, {'tag': '가격인하', 'count': 1}, {'tag': '남향', 'count': 1}, {'tag': '신축', 'count': 1}, {'tag': '주차가능', 'count': 1}]`

- [ ] **Step 2: 앱을 로컬에서 띄운다**

```bash
cd /Users/haluna/workspace/naver-real-estate-v1 && \
FORCE_LOCAL_SQLITE=1 DATABASE_URL= DB_PATH=/Users/haluna/.claude/jobs/618c458c/tmp/tagdemo.db \
ENABLE_SCHEDULER=false SEED_DEMO_DATA=false PORT=5101 python3 app.py
```

(백그라운드로 실행하고 로그를 확인한다. `app.py`의 기본 포트도 5101이다.)

Expected: Flask 개발 서버가 뜨고 `Running on http://127.0.0.1:5101` 로그

- [ ] **Step 3: API 응답을 curl로 확인한다**

```bash
curl -s 'http://127.0.0.1:5101/api/tags' && echo && \
curl -s 'http://127.0.0.1:5101/api/listings?tags=%EC%97%AD%EC%84%B8%EA%B6%8C' | python3 -c "import json,sys; d=json.load(sys.stdin); print('listings total', d['total'], [l['article_no'] for l in d['listings']])" && \
curl -s 'http://127.0.0.1:5101/api/listings?tags=%EB%82%A8%ED%96%A5,%EC%97%AD%EC%84%B8%EA%B6%8C' | python3 -c "import json,sys; d=json.load(sys.stdin); print('OR total', d['total'])" && \
curl -s 'http://127.0.0.1:5101/api/map-listings?tags=%EB%82%A8%ED%96%A5' | python3 -c "import json,sys; d=json.load(sys.stdin); print('map count', d['count'])"
```

Expected:
- `/api/tags` → `{"tags":[{"count":3,"tag":"역세권"}, ...]}`
- `listings total 3 ['D3', 'D2', 'D1']` (정렬 순서는 다를 수 있음, 개수 3이 핵심)
- `OR total 4`
- `map count 1`

- [ ] **Step 4: 브라우저에서 UI를 확인한다**

`http://127.0.0.1:5101` 을 열고 다음을 확인한다:
1. 사이드바에 "태그 필터" 섹션이 보이고, 태그 pill이 개수와 함께 렌더된다.
2. `역세권` pill 클릭 → active 표시, 목록이 3건으로 줄고, hero 요약이 `... · 태그 역세권` 으로 바뀐다.
3. `남향` pill 추가 클릭 → 목록 4건 (OR 매칭).
4. "선택 해제" 클릭 → 전체 6건 복귀, pill active 모두 해제.
5. 섹션 헤더의 접기 버튼 클릭 → 패널이 접히고, 새로고침해도 접힌 상태가 유지된다.
6. 지도를 매물 티어까지 확대 → 태그 선택 시 마커 수가 목록과 일치한다.

- [ ] **Step 5: 서버를 내리고 임시 파일을 정리한다**

```bash
pkill -f "python3 app.py"
rm -f /Users/haluna/.claude/jobs/618c458c/tmp/tagdemo.db /Users/haluna/.claude/jobs/618c458c/tmp/seed_tag_demo.py
git status --short
```

Expected: `git status --short`에 카카오맵 관련 미커밋 변경(`.gitignore`, `app.py`, `database.py`, `static/css/style.css`, `static/js/app.js`, `templates/index.html`)만 남고, 태그 필터 변경은 모두 커밋되어 있어야 한다. `tests/` 는 untracked로 남지 않아야 한다.

---

## 미해결 사항 / 후속 논의

- `/api/tags` 는 `max_age=300` 캐시를 쓴다. 크롤이 하루 1회이므로 충분하지만, 크롤 직후 태그 목록이 최대 5분간 낡을 수 있다 (기존 `/api/region-stats`·`/api/trends`와 동일한 트레이드오프).
- 태그 pill이 매우 많아질 경우(20개 이상) 상위 N개만 노출하고 "더 보기"를 두는 방안은 이번 범위에서 제외했다 — 실제 태그 종류 수를 Task 6 Step 3의 `/api/tags` 응답으로 확인한 뒤 필요하면 별도 작업으로 다룬다.
