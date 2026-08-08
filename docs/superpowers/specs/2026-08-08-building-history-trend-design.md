# 단지별 일별 매물수 추이 조회 (2단계) 설계

## 배경

1단계(`docs/superpowers/specs/2026-08-06-building-daily-snapshot-design.md`)에서
`crawl_building_stats` 테이블을 만들어 크롤마다 건물(구+단지명) 단위 매물 수·가격인하
건수를 180일 롤링으로 누적 저장하기 시작했다. 이 데이터를 실제로 조회·시각화하는
2단계를 진행한다.

1단계는 "화면은 한 픽셀도 바뀌지 않는다"는 조건이었지만, 2단계는 사용자가 명시적으로
조회 기능을 요청했다. 다만 웹페이지가 복잡해지는 것은 여전히 원하지 않으므로, 새 페이지·
새 검색 UI·새 차트 라이브러리 없이 **기존에 이미 존재하지만 쓰이지 않는 모달**과 **기존
미니 바 차트 패턴**을 재활용한다.

- `templates/index.html`의 `#modal-overlay`/`#modal-title`/`#modal-body`는 CSS까지
  완성돼 있지만 현재 어디서도 열리지 않는 죽은 마크업이다 (매물 카드 클릭은 지금
  네이버 매물 페이지를 새 탭으로 여는 것으로 대체돼 있다).
- 대시보드 히어로 영역의 "최근 7일 급매 추이" 미니 바 차트(`/api/crawl-daily-series`
  + `renderHeroDailySeries()`)는 외부 라이브러리 없이 순수 CSS/JS로 막대를 그리는
  검증된 패턴이다.

**데이터 현황 참고**: `crawl_building_stats`는 2026-08-08 배포 시점부터 쌓이기
시작했다. 배포 전에 실행된 크롤은 반영되지 않았을 수 있어, 배포 직후에는 대부분의
단지가 0~1일치 데이터만 가질 수 있다. 이후 매일 하루씩 쌓인다. UI는 데이터가
부족한 초기 상태를 자연스러운 빈 상태로 처리해야 한다 (아래 프런트엔드 절 참고).

## 범위

- **진입 경로는 매물 카드뿐.** 사이드바 검색/자동완성 등 별도 단지 조회 UI는
  만들지 않는다 — 사용자가 보고 있는 매물 카드의 단지(구+단지명)에 대한 추이만
  볼 수 있다.
- 조회 기간은 **최근 14일** 고정 (API는 `days` 쿼리 파라미터를 받지만 프런트엔드는
  14로 고정 호출).
- 표시 지표는 1단계에서 기록한 것과 동일하게 **매물 수(막대 차트) + 현재 가격인하
  건수(텍스트 요약)** 만.
- 결과는 **모달**로만 표시한다. 별도 라우트(`/buildings/<name>`)나 URL 상태 변경은
  없다.
- 가격(최저가/최고가) 등 1단계에서 기록하지 않은 지표는 이번 단계에서도 다루지 않는다.

## 백엔드

### `database.py`

**`get_building_stats_history(district, building_name, limit=90)` (신규)**

`get_recent_successful_crawls()`와 동일한 책임 분리를 따른다 — 이 메서드는 원본
row만 반환하고, 날짜별로 채우고 빈 칸을 메우는 가공은 `app.py`의 몫이다.

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

기존 `idx_building_stats_lookup ON crawl_building_stats(district, building_name,
created_at)` 인덱스가 `WHERE cb.district = ? AND cb.building_name = ?`를 커버한다.
`crawl_history`는 세션당 한 행뿐인 작은 테이블이라 `session_id` 조인에 별도 인덱스를
추가하지 않는다 (YAGNI).

### `app.py`

**`build_building_history_series(district, building_name, days=14)` (신규)**

`build_daily_crawl_series()`와 완전히 동일한 패턴 — 하루 단위로 그 날짜의 가장 최신
row를 골라 채우고, 없는 날짜는 `total_count: None`으로 채운다.

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

`days`를 30으로 상한(`build_daily_crawl_series`의 14보다 넉넉하게)을 두는 이유는
프런트엔드가 14로 고정 호출하더라도 API 자체는 향후 재사용 가능하게 여지를 남기기
위함이다 (180일 보관 데이터를 낭비하지 않도록). 상한만 다를 뿐 나머지 로직은
동일하다.

**신규 라우트**

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
        {"district": district, "building_name": building_name, "days": serialize_api_value(series)},
        max_age=300,
    )
```

기존 에러 응답 패턴(`{"status": "error", "message": ...}`, 400)과 캐시 패턴
(`cacheable_json(..., max_age=300)`, 다른 통계 API와 동일)을 그대로 따른다.

## 프런트엔드

### 진입점 — 매물 카드에 추이 아이콘

`static/js/app.js`의 카드 템플릿, `card-name` 옆에 작은 버튼을 추가한다:

```html
<div class="card-name-row">
  <div class="card-name" title="${escHtml(l.building_name)}">${escHtml(l.building_name)}</div>
  <button class="card-trend-btn" data-district="${escHtml(l.district)}"
          data-building-name="${escHtml(l.building_name)}" title="일별 매물수 추이">📈</button>
</div>
```

`listings-grid`의 기존 클릭 델리게이터에서 `.card-trend-btn` 클릭을 카드 전체 클릭보다
먼저 검사해 `stopPropagation()` 후 모달을 연다 — 카드의 나머지 영역을 클릭하면 지금처럼
네이버 매물 페이지가 새 탭으로 열리는 동작은 그대로 유지된다.

### 모달

`openBuildingTrendModal(district, buildingName)`:

1. `GET /api/building-history?district=...&building_name=...&days=14` 호출
2. `#modal-title`에 단지명 표시, `#modal-overlay`의 `hidden` 클래스 제거
3. `#modal-body`를 문자열 템플릿으로 채운다 — 히어로 차트(`renderHeroDailySeries`)와
   동일한 막대 높이 계산 로직(최소/최대값 정규화, 결측일은 회색 빈 막대)을 재사용하되,
   전용 CSS 클래스(`building-trend-*`)로 분리해 히어로 차트와 독립적으로 유지보수한다.
   상단에 "현재 매물수 N건 · 가격인하 M건" 텍스트 요약을 넣는다 (최근 유효 데이터
   기준).
4. 유효 데이터가 하나도 없으면 히어로 차트의 빈 상태와 같은 톤의 메시지를 보여준다:
   "데이터를 모으는 중입니다. 내일부터 표시됩니다."
5. 모달 닫기: Escape 키는 이미 구현돼 있다 (`static/js/app.js`의 기존 keydown
   리스너가 `#modal-overlay`에 `hidden` 클래스를 추가). 다만 `#modal-close` 버튼
   클릭과 오버레이 바깥 클릭은 현재 어디에도 연결돼 있지 않으므로 이번 단계에서
   새로 연결한다 (둘 다 같은 `hidden` 클래스 토글 방식).

### CSS

`building-trend-*` 클래스 세트를 `hero-mini-chart-*`와 병렬로 추가한다 (막대 크기,
색상 등은 동일한 값으로 시작). 코드/스타일이 일부 중복되지만, 이번 저장소에서
반복적으로 채택해 온 결정과 동일하게 — 두 컴포넌트가 서로 다른 맥락(히어로 vs 모달)에서
독립적으로 진화할 수 있도록 공유 추상화 대신 중복을 허용한다.

## 비범위

- 단지 검색/자동완성 UI
- `/buildings/<name>` 같은 별도 라우트, URL 상태 반영, 브라우저 뒤로가기 지원
- 가격(최저가/최고가) 스냅샷 표시 (1단계에서 기록하지 않은 지표)
- 여러 단지 비교, CSV 내보내기 등 부가 기능

## 테스트 (TDD)

**`tests/test_building_snapshot.py` 확장** (또는 신규 `tests/test_building_history.py`):

1. `get_building_stats_history()` — 다른 구/단지명의 데이터가 섞여 있어도 정확히
   해당 `(district, building_name)`만 반환한다.
2. `get_building_stats_history()` — `status != 'success'`이거나 `source == 'demo'`인
   세션은 결과에서 제외된다.
3. `build_building_history_series()` — 하루에 여러 세션이 있으면 그날의 최신
   `crawled_at` row만 채택한다.
4. `build_building_history_series()` — 데이터가 없는 날짜는 `total_count: None`으로
   채워지고, 요청한 `days`만큼 길이가 유지된다.
5. `build_building_history_series()` — 전혀 데이터가 없는 신규 단지는 전부 `None`인
   시리즈를 반환한다 (빈 상태 처리 확인용).

**라우트 테스트** (기존 Flask 라우트 테스트 파일이 없으므로
`tests/test_building_history_route.py` 신규 생성):

6. `district`/`building_name` 파라미터 누락 시 400 + 에러 메시지.
7. 정상 요청 시 200, 응답에 `district`/`building_name`/`days` 키 포함, `Cache-Control`
   헤더 확인.
