# "오늘의 급매 온도" 브리핑 고도화 설계

## 배경

히어로 영역의 `hero-insight-banner`("오늘의 급매 온도")는 `updateHeroInsight()`
(`static/js/app.js`)가 두 줄 텍스트로 만든다. 지금 반영되는 데이터는:

- 전국 급매 총건수 / 가격인하 건수 (`updateStats()`가 채우는 `state.dashboard.total`,
  `priceDownCount`)
- 지역(구) 단위 1일 증감 (`/api/trends`)
- 서울 내 상위 변동 지역 3곳
- 등록된 알림 개수

최근에 추가된 단지 단위 데이터(`crawl_building_stats`, 단지별 급변 배지 판정 로직
`build_building_change_badge()`)와 이미 불러오고 있는 7일 전국 추이
(`/api/crawl-daily-series`)는 브리핑에 전혀 쓰이지 않고 있다. 이 설계는 브리핑에
아래 세 지표를 추가한다: **단지별 급변 하이라이트**, **가격인하 비율(%) 추세**,
**3일/7일 모멘텀**.

## 범위

- 기존 지역 증감 요약(1번째 줄)과 서울 변동 요약은 유지하되, 모멘텀/비율 문구를
  덧붙인다. 완전히 새 UI 섹션을 만드는 것은 단지 급변 하이라이트(칩 목록) 하나뿐이다.
- `/api/trends`는 사이드바 "급매 증가/감소 지역" 목록에서도 쓰이므로 응답 형태를
  바꾸지 않는다. 새 지표는 신규 엔드포인트 `/api/hero-insight`로 분리한다.
- 모멘텀(3일/7일)은 새 쿼리를 만들지 않는다. 이미 프런트엔드가 불러오는
  `/api/crawl-daily-series?days=7` 데이터(`state.dashboard.dailySeries`)를 그대로
  재사용해 클라이언트에서 계산한다.
- 대상은 전국 집계뿐이다. 사용자가 현재 보고 있는 필터/지역에 맞춘 개인화는
  이번 범위에 포함하지 않는다 (브리핑 질문에서 사용자가 선택하지 않음).

## 백엔드

### `database.py`

**리팩터: `_get_latest_and_prev_sessions(conn)` (신규 private 헬퍼)**

`get_trends()`에 있던 "가장 최근 성공/비데모 세션 + 그 전날 세션 찾기" 로직을 그대로
추출한다 (동작 변경 없음, 순수 추출):

```python
def _get_latest_and_prev_sessions(self, conn: ConnectionWrapper):
    latest_session_row = conn.execute(
        """
        SELECT session_id, DATE(crawled_at) AS crawl_date
        FROM crawl_history
        WHERE status = 'success' AND COALESCE(source, 'naver') <> 'demo'
        ORDER BY crawled_at DESC
        LIMIT 1
        """
    ).fetchone()
    latest_session = latest_session_row["session_id"] if latest_session_row else None
    latest_date = latest_session_row["crawl_date"] if latest_session_row else None
    if not latest_date:
        return None, None, None, None
    if isinstance(latest_date, datetime):
        latest_date = latest_date.date().isoformat()
    elif not isinstance(latest_date, str):
        latest_date = str(latest_date)

    prev_date = (date.fromisoformat(latest_date) - timedelta(days=1)).isoformat()
    prev_session_row = conn.execute(
        """
        SELECT session_id FROM crawl_history
        WHERE status = 'success' AND COALESCE(source, 'naver') <> 'demo'
          AND DATE(crawled_at) = ?
        ORDER BY crawled_at DESC
        LIMIT 1
        """,
        (prev_date,),
    ).fetchone()
    prev_session = prev_session_row["session_id"] if prev_session_row else None
    return latest_session, latest_date, prev_session, prev_date
```

`get_trends()`는 이 헬퍼를 호출하도록 바꾸되, 그 이후의 큰 SELECT(diff 계산)는
그대로 둔다. **동작은 기존과 완전히 동일해야 한다** — 지금 `get_trends()`를 직접
덮는 테스트가 없으므로, 이 리팩터가 회귀를 만들지 않았는지 확인할 최소 테스트를
아래 "테스트" 절에 추가한다.

**신규: `get_price_down_ratio_trend()`**

전국 가격인하 비율(%)의 오늘/어제 비교. `crawl_region_stats`는 세션별로 이미
`total_count`/`price_down_count`를 저장하고 있으므로 전국 합만 내면 된다.

```python
def get_price_down_ratio_trend(self):
    with self.get_connection() as conn:
        latest_session, _, prev_session, _ = self._get_latest_and_prev_sessions(conn)
        if not latest_session:
            return None

        def _ratio(session_id):
            row = conn.execute(
                """
                SELECT SUM(total_count) AS total, SUM(price_down_count) AS price_down
                FROM crawl_region_stats WHERE session_id = ?
                """,
                (session_id,),
            ).fetchone()
            total = int(row["total"] or 0)
            price_down = int(row["price_down"] or 0)
            return (price_down / total * 100) if total else None

        today_ratio = _ratio(latest_session)
        yesterday_ratio = _ratio(prev_session) if prev_session else None
        diff_pp = (
            today_ratio - yesterday_ratio
            if today_ratio is not None and yesterday_ratio is not None
            else None
        )
        return {
            "today_ratio": today_ratio,
            "yesterday_ratio": yesterday_ratio,
            "diff_pp": diff_pp,
        }
```

**신규: `get_top_building_movers()`**

기존 `_get_latest_building_changes(conn, buildings)`와 동일한 "최근 2개 라이브
세션" CTE를 재사용하되, 특정 단지 목록으로 좁히지 않고 그 두 세션에 존재하는
**모든** (구, 단지명) 쌍의 diff를 반환한다. 중복을 피하기 위해 공통 부분을
`_rank_recent_building_snapshots(conn, where_sql="", params=())`로 추출해
`_get_latest_building_changes()`와 `_get_top_building_movers()` 둘 다 이걸 쓰게
리팩터한다:

```python
def _rank_recent_building_snapshots(self, conn, where_sql="", params=()):
    """최근 2개 라이브 세션에 존재하는 단지들의 스냅샷을 (district, building_name)
    기준으로 그룹핑해 diff를 계산한다. where_sql이 비어 있으면 전체 단지를 반환한다."""
    rows = conn.execute(
        f"""
        WITH live_sessions AS (
            SELECT session_id, crawled_at,
                   ROW_NUMBER() OVER (ORDER BY crawled_at DESC) AS session_rank
            FROM crawl_history
            WHERE status = 'success' AND COALESCE(source, 'naver') <> 'demo'
        ),
        ranked AS (
            SELECT cb.district, cb.building_name, cb.total_count, cb.price_down_count,
                   cb.session_id, ls.crawled_at, ls.session_rank AS snapshot_rank
            FROM crawl_building_stats cb
            JOIN live_sessions ls ON ls.session_id = cb.session_id
            WHERE ls.session_rank <= 2 {where_sql}
        )
        SELECT * FROM ranked ORDER BY district, building_name, snapshot_rank
        """,
        params,
    ).fetchall()

    grouped = {}
    for row in rows:
        grouped.setdefault((row["district"], row["building_name"]), []).append(row)

    changes = {}
    for key, snapshots in grouped.items():
        if len(snapshots) < 2:
            continue
        current, previous = snapshots[0], snapshots[1]
        current_total = int(current["total_count"] or 0)
        previous_total = int(previous["total_count"] or 0)
        current_price_down = int(current["price_down_count"] or 0)
        previous_price_down = int(previous["price_down_count"] or 0)
        changes[key] = {
            "current_crawled_at": current["crawled_at"],
            "previous_crawled_at": previous["crawled_at"],
            "current_total": current_total,
            "previous_total": previous_total,
            "total_diff": current_total - previous_total,
            "current_price_down": current_price_down,
            "previous_price_down": previous_price_down,
            "price_down_diff": current_price_down - previous_price_down,
        }
    return changes

def _get_latest_building_changes(self, conn, buildings):
    keys = list(dict.fromkeys(
        (str(d or "").strip(), str(b or "").strip())
        for d, b in buildings if str(d or "").strip() and str(b or "").strip()
    ))
    if not keys:
        return {}
    where_sql = "AND (" + " OR ".join(["(cb.district = ? AND cb.building_name = ?)"] * len(keys)) + ")"
    params = [v for key in keys for v in key]
    return self._rank_recent_building_snapshots(conn, where_sql, params)

def get_top_building_movers(self):
    """최근 2개 라이브 세션 사이의 전체 단지 diff를 반환한다.
    '주목할 만한 변화' 판정은 app.py의 build_building_change_badge()가 담당한다."""
    with self.get_connection() as conn:
        changes = self._rank_recent_building_snapshots(conn)
    return [
        {"district": d, "building_name": b, **change}
        for (d, b), change in changes.items()
    ]
```

전국 단지 수가 크지 않고(현재 데이터 기준 수천 단지 이하), 최근 2개 세션으로만
필터링된 쿼리라 별도 LIMIT/페이지네이션은 두지 않는다 (YAGNI — 필요해지면 그때
추가). `idx_building_stats_lookup`은 `(district, building_name, created_at)`이라
이 쿼리(`session_id` 기준 스캔)에는 도움이 안 되지만, `crawl_building_stats`에는
이미 `session_id`를 참조하는 FK 성격의 컬럼이 있고 세션당 로우 수가 전체 단지
수 규모라 풀스캔 2회로 충분하다.

### `app.py`

**신규: `build_price_down_ratio_insight(trend)` / `build_top_building_movers(raw_changes, limit=5)`**

```python
def build_price_down_ratio_insight(trend):
    if not trend or trend.get("today_ratio") is None:
        return None
    return {
        "today_ratio": round(trend["today_ratio"], 1),
        "yesterday_ratio": (
            round(trend["yesterday_ratio"], 1) if trend.get("yesterday_ratio") is not None else None
        ),
        "diff_pp": round(trend["diff_pp"], 1) if trend.get("diff_pp") is not None else None,
    }


def build_top_building_movers(raw_changes, limit=5):
    movers = []
    for change in raw_changes:
        badge = build_building_change_badge(change)
        if not badge:
            continue
        movers.append({
            "district": change["district"],
            "building_name": change["building_name"],
            **badge,
        })
    movers.sort(key=lambda m: m["total_diff"] + m["price_down_diff"], reverse=True)
    return movers[:limit]
```

`build_building_change_badge()`는 이미 "연속 1일 스냅샷" 요구와 임계치(총량 +5 또는
+3&20%↑, 가격인하 +3) 판정을 갖고 있으므로 그대로 재사용한다 — 새 임계치를
만들지 않는다.

**신규 라우트**

```python
@app.route("/api/hero-insight")
def get_hero_insight():
    ratio_trend = db.get_price_down_ratio_trend()
    raw_movers = db.get_top_building_movers()
    return cacheable_json(
        {
            "price_down_ratio": serialize_api_value(build_price_down_ratio_insight(ratio_trend)),
            "building_movers": serialize_api_value(build_top_building_movers(raw_movers)),
        },
        max_age=300,
    )
```

기존 통계 API와 동일하게 `cacheable_json(..., max_age=300)` 패턴을 따른다.

## 프런트엔드

### 데이터 로드

`loadSidebar()`에 나머지 두 개(`dailySeriesPromise`, `regionStatsPromise`,
`trendsPromise`)와 병렬로 추가:

```js
const heroInsightPromise = api('/api/hero-insight')
  .then(data => {
    state.dashboard.priceDownRatio = data.price_down_ratio || null;
    state.dashboard.buildingMovers = data.building_movers || [];
    updateHeroInsight();
    renderHeroMovers(state.dashboard.buildingMovers);
  })
  .catch(e => {
    console.warn('Hero insight load error:', e);
  });
```

`Promise.all([...])` 목록에 합류시킨다 (기존 패턴과 동일).

### `updateHeroInsight()` 확장

1번째 줄(지역 증감 요약) 끝에 모멘텀 절을 덧붙인다. 신규 헬퍼:

```js
function computeMomentumText(dailySeries) {
  const items = (dailySeries || []).filter(hasNumericValue);
  if (items.length < 2) return '';

  const today = items[items.length - 1];
  const rest = items.slice(0, -1); // 오늘을 제외한 최대 6일
  const avg = rest.reduce((sum, i) => sum + Number(i.total_count), 0) / rest.length;
  const diff = Math.round(Number(today.total_count) - avg);
  const pct = avg ? Math.round((diff / avg) * 100) : null;
  const sign = diff > 0 ? '+' : '';

  let streak = 0;
  let dir = null;
  for (let i = items.length - 1; i > 0; i--) {
    const d = Number(items[i].total_count) - Number(items[i - 1].total_count);
    const curDir = d > 0 ? 'up' : d < 0 ? 'down' : null;
    if (!curDir || (dir && curDir !== dir)) break;
    dir = curDir;
    streak++;
  }
  const streakTag = streak >= 3 ? ` (${streak}일 연속 ${dir === 'up' ? '증가' : '감소'})` : '';

  return ` 최근 7일 평균 대비 오늘 ${sign}${fmtNum(diff)}건${pct != null ? `(${sign}${pct}%)` : ''}입니다.${streakTag}`;
}
```

`updateHeroInsight()`의 `!trends.length` 분기와 일반 분기 양쪽 `el.textContent`
끝에 `+ computeMomentumText(state.dashboard.dailySeries)`를 덧붙인다.

2번째 줄(서울 변동 + 가격인하 + 알림)에서 `현재 가격인하 매물은 N건입니다`를
비율 추세로 교체:

```js
function formatPriceDownRatioLine(ratio, fallbackCount) {
  if (!ratio || ratio.today_ratio == null) {
    return `현재 가격인하 매물은 ${fmtNum(fallbackCount)}건입니다.`;
  }
  const base = `가격인하 비중 ${ratio.today_ratio}%`;
  if (ratio.yesterday_ratio == null || ratio.diff_pp == null) return `${base}입니다.`;
  const arrow = ratio.diff_pp > 0 ? '▲' : ratio.diff_pp < 0 ? '▼' : '-';
  return `${base}(전일 ${ratio.yesterday_ratio}%, ${arrow}${Math.abs(ratio.diff_pp)}%p)입니다.`;
}
```

기존 `secondLine` 배열의 `현재 가격인하 매물은 ${fmtNum(priceDown)}건입니다.` 자리를
`formatPriceDownRatioLine(state.dashboard.priceDownRatio, priceDown)`로 교체한다
(두 분기 — trends 있음/없음 — 모두).

### 단지 급변 하이라이트 (신규 칩 목록)

```js
function renderHeroMovers(movers) {
  const el = document.getElementById('hero-insight-movers');
  if (!el) return;
  if (!movers || !movers.length) {
    el.classList.add('hidden');
    el.innerHTML = '';
    return;
  }
  el.classList.remove('hidden');
  el.innerHTML = movers.map(m => `
    <span class="hero-mover-chip hero-mover-chip-${escHtml(m.kind)}"
          title="${escHtml(m.district)} ${escHtml(m.building_name)}">
      ${escHtml(m.label)} · ${escHtml(m.building_name)}
    </span>
  `).join('');
}
```

`templates/index.html`의 `hero-insight-banner` 안, `hero-insight-copy` 다음에 추가:

```html
<div id="hero-insight-movers" class="hero-insight-movers hidden"></div>
```

### CSS

`static/css/style.css`에 `.hero-insight-movers`(flex-wrap 칩 컨테이너)와
`.hero-mover-chip` + `.hero-mover-chip-total/.hero-mover-chip-price_down/.hero-mover-chip-both`를
추가한다. 색상은 기존 `.badge-building-change-*`에서 쓰는 값을 그대로 재사용한다
(새 색상 팔레트를 만들지 않는다).

## 엣지 케이스

- 어제 라이브 세션이 없음(운영 첫날 등) → `get_price_down_ratio_trend()`가
  `yesterday_ratio: None` 반환 → 프런트는 "가격인하 비중 N%입니다."로 폴백(전일 비교
  없이).
- 라이브 세션 자체가 없음 → `get_price_down_ratio_trend()`가 `None` 반환 →
  `price_down_ratio` 응답이 `null` → 프런트는 기존 "현재 가격인하 매물은 N건입니다."
  문구로 완전 폴백.
- 오늘 주목할 단지 변동이 없음 → `building_movers: []` → 칩 줄 자체를 숨김
  (빈 상태 문구를 새로 만들지 않는다).
- `dailySeries`가 아직 로드되지 않았거나 유효 데이터가 2일 미만 → `computeMomentumText()`가
  빈 문자열 반환 → 1번째 줄은 모멘텀 절 없이 기존 그대로 보임.
- 전체 매물 0건 → `get_price_down_ratio_trend()`의 `_ratio()`가 0으로 나누기 대신
  `None` 반환 (이미 코드에 반영됨).

## 비범위

- 사용자 필터/관심 지역 기반 개인화 브리핑
- 단지 급변 하이라이트를 클릭했을 때 해당 단지의 추이 모달을 바로 여는 등 상호작용
  (이번 단계는 정보 표시까지만; 원하면 후속 단계에서 `openBuildingTrendModal()`과
  연결 가능)
- 알림 규칙과의 자동 연동(이미 등록된 알림과 하이라이트를 대조해 강조하는 것 등)

## 테스트 (TDD)

**`tests/test_hero_insight.py` (신규)**

1. `get_price_down_ratio_trend()` — 오늘/어제 세션 둘 다 있을 때 비율(%)과
   `diff_pp`가 정확히 계산된다.
2. `get_price_down_ratio_trend()` — 어제 세션이 없으면 `yesterday_ratio`/`diff_pp`는
   `None`, `today_ratio`는 값이 있다.
3. `get_price_down_ratio_trend()` — 라이브(성공/비데모) 세션이 전혀 없으면 `None`을
   반환한다.
4. `get_top_building_movers()` — 최근 2개 라이브 세션에 모두 존재하는 단지만 diff가
   계산되고, 한쪽 세션에만 있는 단지는 제외된다.
5. `build_top_building_movers()` — 임계치 미만인 변화는 걸러지고, 통과한 것들은
   심각도(총량diff+가격인하diff) 내림차순으로 정렬되며 `limit`이 적용된다.
6. `/api/hero-insight` 라우트 — 200, `price_down_ratio`/`building_movers` 키 존재,
   `Cache-Control` 헤더 확인.
7. `get_trends()` 리팩터 회귀 테스트 — 리팩터 전/후 동일 입력(동일 세션 데이터)에
   대해 반환값이 동일한지 확인 (지금까지 `get_trends()`를 직접 덮는 테스트가 없었으므로
   이번에 최소 1개 추가).

프런트엔드(`computeMomentumText`, `formatPriceDownRatioLine`, `renderHeroMovers`)는
이 저장소에 JS 테스트 인프라가 없으므로 기존 관례대로 로컬 서버 수동 스모크 테스트로
검증한다.
