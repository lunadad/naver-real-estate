# 건물(단지) 단위 일별 매물 스냅샷 설계

## 배경

`listings` 테이블은 크롤이 돌 때마다 **최신 2개 세션만 남기고 이전 세션을 삭제**한다
(`insert_listings()`). 그 결과 "래미안원베일리 매물이 어제보다 늘었는지 줄었는지" 같은
건물(단지) 단위 추이는 하루만 지나도 영구히 사라진다. 구(district) 단위 추이는
`crawl_region_stats`가 세션마다 계속 누적돼 `get_trends()`로 조회 가능하지만, 건물
단위로는 이런 히스토리 테이블이 없다.

이 설계는 크롤 시점마다 건물별 매물 수·가격인하 건수를 별도 테이블에 스냅샷으로 남겨,
데이터가 더 이상 유실되지 않게 한다.

## 범위 (1단계)

- **백엔드 전용.** API 라우트·프런트엔드 변경은 이번 단계에 포함하지 않는다 — 화면은
  한 픽셀도 바뀌지 않는다.
- 건물 식별 키는 **`district + building_name`** (매물유형은 구분하지 않음).
- 기록 지표는 **매물 수 + 가격인하 건수**만 — `crawl_region_stats`와 동일한 최소 패턴.
- 세션 내 매물 수가 **2건 미만인 건물은 기록하지 않는다** (저장량 억제, 단발성 노이즈
  제거 목적. 데이터 품질 문제인 "빌라" 같은 뭉뚱그려진 건물명 정제는 별도 과제).
- 보관 기간은 **180일 롤링** — 그 이전 스냅샷은 크롤마다 자동 삭제된다.
- 조회 메서드나 프런트엔드는 만들지 않는다. 데이터는 쌓이기만 하고, 필요 시 DB에서
  직접 확인한다. (2단계에서 조회·UI 여부를 데이터가 쌓인 후 다시 판단한다.)

## 백엔드

### `database.py`

**신규 테이블 `crawl_building_stats`**

SQLite와 Postgres 양쪽 `CREATE TABLE` 블록에 `crawl_region_stats` 바로 아래 추가한다.
구조는 `crawl_region_stats`와 동일하되 `building_name` 컬럼이 추가되고, `UNIQUE` 제약이
`(session_id, district, building_name)`이다. `region_stats`처럼 세션 단위로 지우고
다시 쓰는 테이블이 아니라 **누적** 테이블이므로 비우는 로직이 없다 — 대신 오래된 행만
날짜 기준으로 삭제한다.

```sql
-- SQLite
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
CREATE INDEX IF NOT EXISTS idx_building_stats_lookup
    ON crawl_building_stats(district, building_name, created_at);
CREATE INDEX IF NOT EXISTS idx_building_stats_created_at
    ON crawl_building_stats(created_at);
```

```sql
-- Postgres
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
```
(인덱스는 Postgres용 인덱스 리스트에 동일하게 추가)

**`_build_building_stats_rows(session_id, listings, created_at)` (신규, staticmethod 아님 —
`_build_region_stats_rows`와 동일하게 인스턴스 메서드)**

- `_build_region_stats_rows()`와 같은 방식으로 `(district, building_name)` 기준 그룹핑
  (`UNIQUE(session_id, district, building_name)` 제약과 일치 — `region`은 키에 넣지 않는다).
  `district`나 `building_name`이 빈 문자열이면 스킵.
- 그룹당 `total_count` 증가, `tags`에 `"가격인하"` 있으면 `price_down_count` 증가
  (기존 `_build_region_stats_rows`의 태그 파싱 로직 그대로 재사용).
- **반환 직전에 `total_count < 2`인 그룹을 제외한다.**

**`insert_listings()` 확장**

`crawl_region_stats`를 저장하는 블록(1170행 부근) 바로 다음에 추가한다. 같은
트랜잭션·같은 커넥션을 쓴다 (별도 커밋 없음 — 원자성 유지).

```python
building_stats_rows = self._build_building_stats_rows(session_id, listings, now)
building_payload = [
    (row["session_id"], row["region"], row["district"], row["building_name"],
     row["total_count"], row["price_down_count"], row["created_at"])
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

cutoff = (datetime.now() - timedelta(days=180)).isoformat()
conn.execute("DELETE FROM crawl_building_stats WHERE created_at < ?", (cutoff,))
```

`ON CONFLICT ... DO UPDATE`는 같은 세션이 재실행(재크롤)되는 경우를 위한 안전장치다
(중복 삽입 대신 최신 값으로 갱신). 180일 컷오프 삭제는 매 크롤마다 실행되므로 별도
스케줄 작업이 필요 없다 — 기존 "세션 2개만 유지" 삭제 로직과 같은 자리에서 같은 방식으로
동작한다.

## 비범위 (2단계로 미룸)

- `GET /api/buildings/<name>/history` 같은 조회 API
- 프런트엔드 모달 스파크라인 등 시각화
- 건물명 정규화(예: "빌라"처럼 뭉뚱그려진 값 정제)
- 가격(최저가/최고가) 스냅샷

## 테스트 (TDD)

`tests/test_building_snapshot.py` 신규:

1. 매물 2건 이상인 건물만 `crawl_building_stats`에 기록된다.
2. 매물 1건뿐인 건물은 기록되지 않는다.
3. 같은 `session_id`로 `insert_listings()`를 다시 호출하면(재크롤 시뮬레이션) 행이
   중복되지 않고 `total_count`가 갱신된다 (UPSERT 확인).
4. `created_at`이 180일보다 오래된 행은 다음 `insert_listings()` 호출 시 삭제된다.
5. `district`/`building_name`이 빈 값인 매물은 스킵된다.
