# 건물 단위 일별 매물 스냅샷 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 크롤이 돌 때마다 건물(단지)별 매물 수·가격인하 건수를 `crawl_building_stats` 테이블에 스냅샷으로 남겨, `listings` 테이블이 최신 2세션만 남기고 지우는 것과 무관하게 건물 단위 추이 데이터가 유실되지 않게 한다.

**Architecture:** `crawl_region_stats`와 같은 최소 패턴(매물 수 + 가격인하 수)을 따르되, 세션마다 지우고 다시 쓰는 `region_stats`와 달리 **누적** 테이블이다. `insert_listings()` 안에서 같은 트랜잭션으로 (1) 매물 2건 이상인 건물만 골라 INSERT·UPSERT하고 (2) 180일 지난 행을 삭제한다. API·프런트엔드는 건드리지 않는다 — 데이터만 쌓인다.

**Tech Stack:** Python 3.14 / SQLite(로컬)·PostgreSQL(운영) 듀얼 드라이버 / 표준 라이브러리 `unittest`

## Global Constraints

- **베이스 커밋:** 현재 `main` HEAD (`b006022`, 태그 검색 필터 병합 이후). 이 계획은 그 위에서 작업한다.
- **새 런타임 의존성 추가 금지.** 테스트는 표준 라이브러리 `unittest`만 사용한다 (저장소에 pytest 없음, venv 없음).
- **DB 드라이버 중립:** SQL은 `?` 플레이스홀더만 쓴다 (`ConnectionWrapper._convert_sql`이 Postgres용 `%s`로 변환).
- **건물 식별 키는 `district + building_name`.** 매물유형은 구분하지 않는다.
- **기록 지표는 매물 수(`total_count`) + 가격인하 건수(`price_down_count`)만.** 가격 등 추가 지표는 이번 범위 밖.
- **세션 내 매물 수가 2건 미만인 건물은 기록하지 않는다.**
- **보관 기간 180일 롤링.** `insert_listings()` 호출마다(=크롤마다) 자동으로 오래된 행을 지운다 — 별도 배치 작업 없음.
- **이번 단계는 API·프런트엔드 변경 없음.** `app.py`, `static/`, `templates/`는 건드리지 않는다.
- **커밋 범위:** 각 태스크의 커밋에는 해당 태스크가 건드린 파일만 `git add` 한다 (`git add -A` 금지).
- 테스트 실행은 항상 저장소 루트에서 `python3 -m unittest tests.test_building_snapshot -v`.

---

## File Structure

| 파일 | 역할 | 변경 |
|---|---|---|
| `database.py` | `crawl_building_stats` 스키마(SQLite+Postgres), `_build_building_stats_rows()`, `insert_listings()` 확장(INSERT/UPSERT + 180일 삭제) | 수정 |
| `tests/test_building_snapshot.py` | 스키마 스모크 테스트, 집계 함수 단위 테스트, `insert_listings()` 통합 테스트 | 신규 |

---

### Task 1: `crawl_building_stats` 테이블 스키마

**Files:**
- Create: `tests/test_building_snapshot.py`
- Modify: `database.py` (`_init_sqlite()` 306~315행 부근, `_init_postgres()` 404~414행·454행 부근)

**Interfaces:**
- Consumes: 없음 (첫 태스크)
- Produces:
  - 테이블 `crawl_building_stats(id, session_id, region, district, building_name, total_count, price_down_count, created_at)`, `UNIQUE(session_id, district, building_name)`.
  - `tests/test_building_snapshot.py`의 `make_listing(article_no, **overrides) -> dict` 헬퍼 (이후 태스크가 재사용).

- [ ] **Step 1: 실패하는 테스트를 작성한다**

`tests/test_building_snapshot.py`를 새로 만든다:

```python
"""건물 단위 일별 매물 스냅샷 — DB 계층 테스트 (표준 라이브러리 unittest)."""

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import Database  # noqa: E402


def make_listing(article_no, **overrides):
    """insert_listings()가 기대하는 형태의 매물 dict를 만든다.

    district·building_name 기본값이 같으므로, 같은 건물에 여러 매물을 만들려면
    article_no만 바꿔 호출하면 된다. 다른 건물을 만들려면 building_name을 override.
    """
    listing = {
        "article_no": article_no,
        "region": "서울특별시",
        "district": "서초구",
        "property_type": "아파트",
        "trade_type": "매매",
        "price": "10억",
        "area": "84㎡",
        "floor": "10/20",
        "building_name": "테스트단지",
        "description": "테스트 설명",
        "is_urgent": 1,
        "tags": [],
        "confirmed_date": "20260806",
        "latitude": 37.5,
        "longitude": 127.0,
        "naver_url": f"https://example.com/{article_no}",
    }
    listing.update(overrides)
    return listing


class BuildingSnapshotSchemaTest(unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = Database(
            db_path=os.path.join(tmpdir.name, "test.db"),
            skip_price_backfill=True,
        )
        self.addCleanup(self.db.close)

    def test_crawl_building_stats_table_accepts_expected_columns(self):
        with self.db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO crawl_building_stats
                (session_id, region, district, building_name, total_count, price_down_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("session-1", "서울특별시", "서초구", "래미안원베일리", 5, 2, "2026-08-06T09:00:00"),
            )
        with self.db.get_connection() as conn:
            row = conn.execute(
                """
                SELECT session_id, region, district, building_name, total_count, price_down_count
                FROM crawl_building_stats
                """
            ).fetchone()
        self.assertEqual(row["session_id"], "session-1")
        self.assertEqual(row["district"], "서초구")
        self.assertEqual(row["building_name"], "래미안원베일리")
        self.assertEqual(row["total_count"], 5)
        self.assertEqual(row["price_down_count"], 2)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

```bash
python3 -m unittest tests.test_building_snapshot -v
```

Expected: FAIL — `sqlite3.OperationalError: no such table: crawl_building_stats`

- [ ] **Step 3: 최소 구현을 작성한다**

`database.py`의 `_init_sqlite()` 안, `CREATE TABLE IF NOT EXISTS crawl_region_stats (...)` 블록(306~315행) **바로 다음**, `CREATE TABLE IF NOT EXISTS alert_rules (...)` **앞**에 추가한다:

```sql
            CREATE TABLE IF NOT EXISTS crawl_building_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                region TEXT NOT NULL,
                district TEXT NOT NULL,
                building_name TEXT NOT NULL,
                total_count INTEGER NOT NULL DEFAULT 0,
                price_down_count INTEGER NOT NULL DEFAULT 0,
                created_at TEXT,
                UNIQUE(session_id, district, building_name)
            );
```

같은 메서드의 인덱스 목록, `CREATE INDEX IF NOT EXISTS idx_crawl_region_stats_session ON crawl_region_stats(session_id);` **바로 다음**에 추가한다:

```sql
            CREATE INDEX IF NOT EXISTS idx_building_stats_lookup ON crawl_building_stats(district, building_name, created_at);
            CREATE INDEX IF NOT EXISTS idx_building_stats_created_at ON crawl_building_stats(created_at);
```

`_init_postgres()` 안, `CREATE TABLE IF NOT EXISTS crawl_region_stats (...)` 블록(404~414행) **바로 다음**, `CREATE TABLE IF NOT EXISTS alert_rules (...)` **앞**에 추가한다 (리스트의 새 문자열 원소):

```python
            """
            CREATE TABLE IF NOT EXISTS crawl_building_stats (
                id BIGSERIAL PRIMARY KEY,
                session_id TEXT NOT NULL,
                region TEXT NOT NULL,
                district TEXT NOT NULL,
                building_name TEXT NOT NULL,
                total_count INTEGER NOT NULL DEFAULT 0,
                price_down_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP,
                UNIQUE(session_id, district, building_name)
            )
            """,
```

같은 메서드의 인덱스 목록, `"CREATE INDEX IF NOT EXISTS idx_crawl_region_stats_session ON crawl_region_stats(session_id)",` **바로 다음**에 추가한다:

```python
            "CREATE INDEX IF NOT EXISTS idx_building_stats_lookup ON crawl_building_stats(district, building_name, created_at)",
            "CREATE INDEX IF NOT EXISTS idx_building_stats_created_at ON crawl_building_stats(created_at)",
```

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

```bash
python3 -m unittest tests.test_building_snapshot -v
```

Expected: `Ran 1 test` … `OK`

- [ ] **Step 5: 커밋한다**

```bash
git add tests/test_building_snapshot.py database.py
git commit -m "feat: add crawl_building_stats table schema"
```

---

### Task 2: `_build_building_stats_rows()` — 건물별 집계·임계값 필터

**Files:**
- Modify: `database.py` (`_build_region_stats_rows()` 정의 바로 다음, 615~644행 부근)
- Test: `tests/test_building_snapshot.py` (신규 클래스 `BuildingStatsRowsTest`)

**Interfaces:**
- Consumes: Task 1의 `make_listing()` 헬퍼.
- Produces:
  - `Database._build_building_stats_rows(session_id: str, listings: List[Dict], created_at: str) -> List[Dict]` — 각 dict는 `{"session_id", "region", "district", "building_name", "total_count", "price_down_count", "created_at"}`. `district`나 `building_name`이 빈 문자열인 매물은 집계에서 제외되고, 세션 내 매물 수가 2건 미만인 건물은 반환 목록에서 제외된다.

- [ ] **Step 1: 실패하는 테스트를 작성한다**

`tests/test_building_snapshot.py`의 `if __name__ == "__main__":` **위**, `BuildingSnapshotSchemaTest` 클래스 **다음**에 추가한다:

```python
class BuildingStatsRowsTest(unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = Database(
            db_path=os.path.join(tmpdir.name, "test.db"),
            skip_price_backfill=True,
        )
        self.addCleanup(self.db.close)

    def test_building_with_two_or_more_listings_is_included(self):
        listings = [
            make_listing("B1", building_name="래미안원베일리"),
            make_listing("B2", building_name="래미안원베일리"),
        ]
        rows = self.db._build_building_stats_rows("session-1", listings, "2026-08-06T09:00:00")
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["district"], "서초구")
        self.assertEqual(row["building_name"], "래미안원베일리")
        self.assertEqual(row["total_count"], 2)
        self.assertEqual(row["price_down_count"], 0)

    def test_building_with_single_listing_is_excluded(self):
        listings = [make_listing("B1", building_name="반포자이")]
        rows = self.db._build_building_stats_rows("session-1", listings, "2026-08-06T09:00:00")
        self.assertEqual(rows, [])

    def test_listing_missing_district_or_building_name_is_skipped(self):
        listings = [
            make_listing("B1", district="", building_name="반포자이"),
            make_listing("B2", district="서초구", building_name=""),
            make_listing("B3", building_name="반포자이"),
            make_listing("B4", building_name="반포자이"),
        ]
        rows = self.db._build_building_stats_rows("session-1", listings, "2026-08-06T09:00:00")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["building_name"], "반포자이")
        self.assertEqual(rows[0]["total_count"], 2)  # B3, B4만 집계됨

    def test_price_down_tag_is_counted(self):
        listings = [
            make_listing("B1", building_name="반포자이", tags=["가격인하"]),
            make_listing("B2", building_name="반포자이", tags=[]),
        ]
        rows = self.db._build_building_stats_rows("session-1", listings, "2026-08-06T09:00:00")
        self.assertEqual(rows[0]["price_down_count"], 1)


if __name__ == "__main__":
```

(마지막 줄은 기존 `if __name__ == "__main__": unittest.main()` 블록을 그대로 유지하되, 새 클래스가 그 위에 삽입되도록 순서를 맞춘다.)

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

```bash
python3 -m unittest tests.test_building_snapshot -v
```

Expected: FAIL — `AttributeError: 'Database' object has no attribute '_build_building_stats_rows'`

- [ ] **Step 3: 최소 구현을 작성한다**

`database.py`의 `_build_region_stats_rows()` 메서드(615~644행) **바로 다음**, `def replace_crawl_region_stats(` **앞**에 추가한다:

```python
    def _build_building_stats_rows(self, session_id: str, listings: List[Dict], created_at: str):
        grouped = {}
        for listing in listings:
            region = str(listing.get("region") or "").strip()
            district = str(listing.get("district") or "").strip()
            building_name = str(listing.get("building_name") or "").strip()
            if not district or not building_name:
                continue
            key = (district, building_name)
            entry = grouped.setdefault(
                key,
                {
                    "session_id": session_id,
                    "region": region,
                    "district": district,
                    "building_name": building_name,
                    "total_count": 0,
                    "price_down_count": 0,
                    "created_at": created_at,
                },
            )
            entry["total_count"] += 1
            tags = listing.get("tags") or []
            if isinstance(tags, str):
                try:
                    tags = json.loads(tags)
                except Exception:
                    tags = [tags]
            if "가격인하" in tags:
                entry["price_down_count"] += 1
        return [row for row in grouped.values() if row["total_count"] >= 2]
```

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

```bash
python3 -m unittest tests.test_building_snapshot -v
```

Expected: `Ran 5 tests` … `OK`

- [ ] **Step 5: 커밋한다**

```bash
git add tests/test_building_snapshot.py database.py
git commit -m "feat: add _build_building_stats_rows() aggregation"
```

---

### Task 3: `insert_listings()`에 스냅샷 저장 + 180일 보관 정책 연결

**Files:**
- Modify: `database.py` (`insert_listings()`, `crawl_region_stats` 저장 블록 1169~1193행 부근)
- Test: `tests/test_building_snapshot.py` (신규 클래스 `BuildingSnapshotInsertTest`)

**Interfaces:**
- Consumes: Task 1의 `make_listing()`, Task 2의 `Database._build_building_stats_rows()`.
- Produces: 없음 (마지막 코드 태스크). `insert_listings()` 호출 시 `crawl_building_stats`가 채워지고 180일 초과 행이 삭제된다는 부작용만 남긴다.

- [ ] **Step 1: 실패하는 테스트를 작성한다**

`tests/test_building_snapshot.py`의 `if __name__ == "__main__":` **위**, `BuildingStatsRowsTest` 클래스 **다음**에 추가한다:

```python
class BuildingSnapshotInsertTest(unittest.TestCase):
    def setUp(self):
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        self.db = Database(
            db_path=os.path.join(tmpdir.name, "test.db"),
            skip_price_backfill=True,
        )
        self.addCleanup(self.db.close)

    def _building_stats_rows(self):
        with self.db.get_connection() as conn:
            return conn.execute(
                """
                SELECT session_id, district, building_name, total_count, price_down_count, created_at
                FROM crawl_building_stats
                ORDER BY building_name
                """
            ).fetchall()

    def test_insert_listings_persists_stats_for_qualifying_buildings_only(self):
        listings = [
            make_listing("A1", building_name="래미안원베일리"),
            make_listing("A2", building_name="래미안원베일리"),
            make_listing("A3", building_name="반포자이"),  # 1건뿐 -> 제외되어야 함
        ]
        self.db.insert_listings(listings, "session-1")

        rows = self._building_stats_rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["building_name"], "래미안원베일리")
        self.assertEqual(rows[0]["total_count"], 2)

    def test_reinserting_same_session_upserts_instead_of_duplicating(self):
        listings_v1 = [
            make_listing("A1", building_name="래미안원베일리"),
            make_listing("A2", building_name="래미안원베일리"),
        ]
        self.db.insert_listings(listings_v1, "session-1")

        listings_v2 = [
            make_listing("A3", building_name="래미안원베일리"),
            make_listing("A4", building_name="래미안원베일리"),
            make_listing("A5", building_name="래미안원베일리"),
        ]
        self.db.insert_listings(listings_v2, "session-1")

        rows = self._building_stats_rows()
        self.assertEqual(len(rows), 1)  # 중복 행 없이 하나로 유지
        self.assertEqual(rows[0]["total_count"], 3)  # 최신 값으로 갱신

    def test_stats_older_than_180_days_are_pruned_on_next_insert(self):
        old_date = (datetime.now() - timedelta(days=200)).isoformat()
        with self.db.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO crawl_building_stats
                (session_id, region, district, building_name, total_count, price_down_count, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                ("old-session", "서울특별시", "서초구", "오래된단지", 3, 0, old_date),
            )

        listings = [
            make_listing("A1", building_name="래미안원베일리"),
            make_listing("A2", building_name="래미안원베일리"),
        ]
        self.db.insert_listings(listings, "session-new")

        rows = self._building_stats_rows()
        names = [row["building_name"] for row in rows]
        self.assertNotIn("오래된단지", names)
        self.assertIn("래미안원베일리", names)


if __name__ == "__main__":
```

(다시 한번, 마지막 줄은 기존 `if __name__ == "__main__": unittest.main()` 블록을 유지하며 새 클래스가 그 위에 삽입되도록 순서만 맞춘다.)

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

```bash
python3 -m unittest tests.test_building_snapshot -v
```

Expected: FAIL — 세 개의 새 테스트가 실패 (테이블에 행이 없거나, 오래된 행이 그대로 남아 있어서 `assertEqual(len(rows), ...)` / `assertNotIn(...)`이 어긋남).

- [ ] **Step 3: 최소 구현을 작성한다**

`database.py`의 `insert_listings()` 안, `crawl_region_stats` UPSERT 블록(1169~1193행) **바로 다음**, 메서드가 끝나는 지점(다음 메서드 `_parse_tags` 정의 **앞**)에 추가한다:

```python
            building_stats_rows = self._build_building_stats_rows(session_id, listings, now)
            building_payload = [
                (
                    row["session_id"],
                    row["region"],
                    row["district"],
                    row["building_name"],
                    row["total_count"],
                    row["price_down_count"],
                    row["created_at"],
                )
                for row in building_stats_rows
            ]
            if building_payload:
                conn.executemany(
                    """
                    INSERT INTO crawl_building_stats
                    (session_id, region, district, building_name, total_count, price_down_count, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(session_id, district, building_name) DO UPDATE SET
                        total_count = excluded.total_count,
                        price_down_count = excluded.price_down_count,
                        created_at = excluded.created_at
                    """,
                    building_payload,
                )

            building_stats_cutoff = (datetime.now() - timedelta(days=180)).isoformat()
            conn.execute(
                "DELETE FROM crawl_building_stats WHERE created_at < ?",
                (building_stats_cutoff,),
            )
```

이 블록은 `region_payload`를 저장하는 `if region_payload: conn.executemany(...)` 블록과 같은 들여쓰기 레벨(같은 `with self.get_connection() as conn:` 안)에 위치해 같은 트랜잭션으로 커밋된다.

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

```bash
python3 -m unittest tests.test_building_snapshot -v
```

Expected: `Ran 8 tests` … `OK`

- [ ] **Step 5: 커밋한다**

```bash
git add tests/test_building_snapshot.py database.py
git commit -m "feat: persist building-level daily snapshots with 180-day retention"
```

---

## 미해결 사항 / 후속 논의

- 이번 계획은 데이터 적재만 한다. 조회 API(`GET /api/buildings/<name>/history` 등)와 프런트엔드 시각화(모달 스파크라인 등)는 데이터가 실제로 쌓인 뒤, 필요성이 확인되면 별도 스펙·계획으로 진행한다.
- "빌라"처럼 여러 실제 건물이 하나의 `building_name`으로 뭉뚱그려지는 데이터 품질 문제는 이번 범위에서 다루지 않는다. 건물명 정규화가 필요해지면 별도 과제로 다룬다.
- 온라인 정리(pruning)는 크롤이 실행될 때만 일어난다. 크롤이 장기간 중단되면 그 기간 동안 180일 컷오프 삭제도 함께 멈춘다 — 별도 스케줄 작업이 아니므로 이는 의도된 트레이드오프다 (기존 `listings` 2세션 보관 정책과 동일한 성격).
