# 매물 태그 검색 필터 설계

## 배경

현재 `listings.tags` 컬럼에는 Naver API가 제공하는 태그(예: `역세권`, `신축`, `대단지`,
`주차가능`, `남향`, `학세권`, `숲세권`)와 크롤러가 부여하는 `가격인하` 태그가 JSON 배열
문자열로 저장되어 있다. 카드 UI(`static/js/app.js`)는 이 태그를 이미 표시하고 있지만,
검색 조건으로는 `가격인하`(`price_down_only`) 하나만 하드코딩되어 있어 나머지 태그로는
필터링할 수 없다. 이 설계는 임의의 태그를 검색 조건으로 선택할 수 있는 기능을 추가한다.

## 범위

- 태그 목록은 DB에서 최신 크롤링 세션 기준으로 동적으로 집계한다 (고정 목록 아님).
- 다중 태그 선택 시 OR 매칭 (선택한 태그 중 하나라도 있으면 노출).
- UI는 사이드바에 새 섹션으로 추가한다.
- 기존 `price_down_only` 필터는 유지한다 (통계 카드·하위 호환 목적). 새 태그 필터와는
  독립적으로 동작하며 서로 배타적이지 않다.

## 백엔드

### `database.py`

**`get_tag_counts()` (신규 메서드)**
- 최신 `crawl_session`으로 스코프된 `SELECT tags FROM listings WHERE crawl_session = ?`
  실행 (다른 조회 메서드와 동일하게 `_get_latest_visible_session_id` 사용).
- Python에서 각 행의 `tags` JSON 문자열을 파싱해 태그별 등장 횟수를 `Counter`로 집계
  (SQLite에 JSON1 확장이 쓰이고 있지 않으므로 애플리케이션 레벨에서 파싱).
- `count` 내림차순, 동률이면 태그명 오름차순으로 정렬한 리스트
  `[{"tag": str, "count": int}, ...]` 반환.

**`get_listings()` / `get_map_listings()` (기존 메서드 확장)**
- 새 파라미터 `tags: list[str] | None = None` 추가.
- `tags`가 주어지면 `(tags LIKE ? OR tags LIKE ? OR ...)` 조건을 조건절에 추가하고,
  각 파라미터는 `%"<tag>"%` 형태로 바인딩한다 (따옴표 포함 매칭으로 `역` 이 `역세권`에
  부분 일치하는 것을 방지). 값은 파라미터 바인딩되므로 SQL 인젝션 우려 없음.
- 존재하지 않는 태그가 들어와도 검증 없이 그대로 조건에 사용한다 (결과가 0건이 될 뿐,
  별도 화이트리스트 검증은 두지 않는다 — YAGNI).

### `app.py`

- 신규 라우트 `GET /api/tags` → `db.get_tag_counts()` 결과를 JSON으로 반환.
- `GET /api/listings`, `GET /api/map-listings` 핸들러에서 `request.args.get("tags", "")`를
  콤마로 split, 빈 문자열 항목 제거 후 리스트로 만들어 `db.get_listings(...)` /
  `db.get_map_listings(...)` 에 전달.

## 프런트엔드 (`static/js/app.js`, `templates/index.html`, `static/css/style.css`)

- `state.filters.tags = []` 추가 (문자열 배열).
- `buildQuery()`: 배열 값은 `.join(',')`으로 직렬화하고, 빈 배열이면 파라미터 자체를
  생략하도록 분기 추가 (현재 로직은 `v !== '' && v !== false` 조건만 있어 빈 배열이
  `tags=`로 직렬화되는 문제를 방지해야 함).
- 앱 초기화 시 `/api/tags`를 한 번 호출해 태그 목록을 가져와 사이드바 "태그 필터" 섹션에
  렌더링. 각 태그는 클릭 시 토글되는 pill 버튼으로 표시하고, 태그명 옆에 개수를 작게
  표시한다 (예: `역세권 (128)`).
- "태그 필터" 섹션은 기존 "알림 구독" 섹션과 동일한 접기/펼치기 UI 패턴을 재사용한다.
- 태그 pill 클릭 시 `state.filters.tags` 배열에 추가/제거 후 목록 재조회(`loadListings`류
  함수) 및 active 클래스 갱신.
- `buildCurrentFilterLabel()`과 hero 필터 요약(`hero-filter-summary`)에 선택된 태그를
  `태그 역세권,신축` 형태로 반영.
- 지도 마커 조회(`/api/map-listings` 호출부)에도 동일한 `tags` 파라미터를 전달해 지도와
  목록 화면의 필터 결과가 일치하도록 한다 (기존에도 다른 필터들이 동일하게 미러링되어
  있는 패턴을 따름).

## 엣지 케이스

- 태그가 없는 매물(빈 배열 `[]` 또는 `null`)은 태그 필터를 하나라도 선택하면 자연히
  결과에서 제외된다.
- `/api/tags` 응답이 비어 있으면 (매물이 없거나 태그 데이터가 없는 경우) 사이드바 섹션은
  "태그 데이터가 없습니다" 같은 빈 상태를 보여준다.
- 태그 필터와 `price_down_only`(가격인하 통계 카드)를 동시에 켤 수 있으며, 두 조건은
  AND로 결합된다 (기존 조건 리스트에 추가되는 방식 그대로).

## 테스트 방침

- `database.py`의 `get_tag_counts()`와 `get_listings(tags=...)`에 대한 단위 테스트
  (기존 테스트 스위트 위치/스타일을 따름 — 저장소에 기존 테스트가 있으면 그 패턴 사용,
  없으면 수동 스크립트로 검증 후 브라우저에서 실제 동작 확인).
- 브라우저에서: 태그 1개 선택 → 목록/지도 필터링 확인, 태그 2개 선택 → OR 매칭 확인,
  태그 해제 → 원래 목록 복귀 확인.
