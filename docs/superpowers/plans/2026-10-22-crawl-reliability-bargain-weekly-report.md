# 크롤링 안정성 · 진짜 급매 · 주간 리포트 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 크롤링이 실패해도 같은 날 자동 복구·텔레그램 알림이 되게 하고, 진짜 급매 점수와 주간 리포트를 추가한다.

**Architecture:** 크롤러는 페이지 단위 재시도 후 실패 조합 수를 결과에 싣는다. `run_remote_crawl.py`가 하루 3회 launchd 슬롯에서 "오늘 성공했으면 건너뛰기"와 알림 규칙을 판단하고, 로컬 상태 파일로 실패·미전송 알림을 기억한다. 진짜 급매 점수는 `insert_listings()` 때 계산해 컬럼으로 저장하고 API·사이트·주간 리포트가 같은 값을 읽는다.

**Tech Stack:** Python 3.14, Flask, psycopg/SQLite, Playwright, unittest, vanilla JS, launchd, Telegram Bot API(requests)

**Spec:** `docs/superpowers/specs/2026-10-22-crawl-reliability-bargain-weekly-report-design.md`

**Worktrees:** v1 = `~/workspace/naver-real-estate/.claude/worktrees/reliability` (`feature/reliability-bargain-report`), v2 = `~/workspace/naver-real-estate/.claude/worktrees/reliability-v2` (`feature/crawl-reliability-v2`). 테스트 실행: `/Users/haluna/workspace/naver-real-estate/.venv-migrate/bin/python3 -m unittest discover -s tests` (이하 `$PY -m unittest ...`).

## Global Constraints

- 진짜 급매 판정: `0.10 <= bargain_discount <= 0.40`. 비교 그룹 최소 3건.
- 실패 조합 비율 `failed_combos / total_combos > 0.2` → `status=failed`, 매물 저장 안 함. 정확히 0.2는 성공.
- 재시도 대기: 429 → 30, 60, 120초 / status 0·5xx → 2, 5, 15초 / 그 외 4xx → 재시도 없음.
- 슬롯: 주거용 09:00·12:00·15:00(final-hour 15), 상업용 10:00·13:00·16:00(final-hour 16). 주간 리포트 월요일 10:30.
- 네트워크·DB DNS 대기 최대 15분.
- 알림 접두어 `[주거용]` / `[상업용]`. 텔레그램 설정: env `TELEGRAM_BOT_TOKEN`/`TELEGRAM_CHAT_ID` 우선, 없으면 `~/.hermes/.env`의 `TELEGRAM_BOT_TOKEN`/`TELEGRAM_HOME_CHANNEL`/`TELEGRAM_HOME_CHANNEL_THREAD_ID`.
- 알림 전송은 절대 예외를 던지지 않는다. 정상 성공일에는 알림 없음.
- 기존 특성화 골든: 허용되는 변화는 크롤러 키와 `bargain_discount`/`bargain_basis`/`bargain_count` **추가**뿐.
- 사이트 URL: `https://naver-real-estate-henna.vercel.app`

## Review Focus

1. 크롤 도중 토큰 만료로 남은 조합이 전부 401 → 재시도 없이 실패 조합으로 집계되어 20% 초과 시 실패 처리, 다음 슬롯에서 재크롤 (Task 1 테스트)
2. 최종 슬롯에 인터넷이 없어 텔레그램 전송 실패 → 상태 파일에 남고 다음 실행 시작 때 먼저 전송 (Task 2·3 테스트)
3. "오늘 성공" 판정이 SQLite·Postgres 모두에서 같은 날짜 경계를 쓴다 — `DATE()` 대신 `[day 00:00, day+1 00:00)` 파라미터 비교 (Task 3 테스트)
4. 같은 평형 그룹이 정확히 3건 / 2건 / 면적 "84A"와 "" / `price_sort_value` NULL·0 / 월세·전세 섞임 (Task 4 테스트)
5. 7일 전 세션·단지 통계가 없는 주에도 리포트가 깨지지 않고 가용 정보로 전송 (Task 6 테스트)

---

### Task 1: 페이지 요청 재시도와 실패 조합 집계 (v1 crawler.py)

**Files:**
- Modify: `crawler.py` (`JS_FETCH_PAGE`, `_fetch_combo_all_pages`, `_crawl_with_playwright`, `crawl_all`)
- Create: `tests/test_crawler_retry.py`
- Modify: `tests/golden/refactor_characterization.json` (키 `crawler_fake_playwright`만), `tests/test_refactor_characterization.py` (`_FakePage.evaluate`가 새 응답 형식을 감싸 반환)

**Interfaces:**
- Produces:
  - `JS_FETCH_PAGE` 반환: `{"ok": True, "data": <json>}` | `{"ok": False, "status": int}` (네트워크/타임아웃/abort는 `status: 0`)
  - `NaverRealEstateCrawler.PAGE_RETRY_DELAYS = (2, 5, 15)`, `RATE_LIMIT_RETRY_DELAYS = (30, 60, 120)`, `MAX_FAILED_COMBO_RATIO = 0.2`
  - `_fetch_page(self, page, args: Dict) -> Optional[Dict]`: 성공 시 data(dict 또는 None), 재시도 소진/비재시도 오류 시 `raise PageFetchError(status)`
  - `class PageFetchError(Exception)` (속성 `status: int`), `crawler.py` 모듈 레벨
  - `_fetch_combo_all_pages(...) -> Tuple[List[Dict], bool]` — (articles, failed)
  - `_crawl_with_playwright()`가 `self.last_crawl_stats = {"failed_combos": int, "total_combos": int}` 설정 (토큰 실패로 조기 반환 시 `{0, 0}`)
  - `crawl_all()` 결과 dict에 항상 `failed_combos`, `total_combos` 포함

- [ ] **Step 1: Write the failing tests** — `tests/test_crawler_retry.py`, 가짜 Playwright는 `tests/test_refactor_characterization.py`의 `_FakePage`/`_FakePlaywright`/`_article`을 import해 재사용. 응답 시퀀스(같은 키에 리스트 = 호출 순서대로 소비)를 지원하도록 `_FakePage`를 확장.
  - `test_429_then_success_retries_with_rate_limit_delay`: 1페이지가 `{"ok": False, "status": 429}` 후 성공 → 매물 수집, `time.sleep` 호출 인자에 30 포함, `failed_combos == 0`
  - `test_5xx_exhausts_retries_and_marks_combo_failed`: 3회 재시도 후 실패 → sleep 2,5,15 순서, `failed == True`, 앞 페이지 매물은 유지
  - `test_401_is_not_retried`: sleep 미호출, 조합 실패
  - `test_evaluate_exception_counts_as_failure` (status 0 취급, 재시도함)
  - `test_ok_with_empty_article_list_is_normal_end`: 실패 아님
  - `test_crawl_all_fails_when_failed_ratio_exceeds_20_percent`: `_crawl_with_playwright`를 mock해 매물 반환 + `last_crawl_stats={"failed_combos": 3, "total_combos": 10}` → `status == "failed"`, `db.insert_listings` 미호출, `db.log_crawl(..., "failed", "naver")` 호출
  - `test_crawl_all_succeeds_at_exactly_20_percent`: `{2, 10}` → success, 결과에 `failed_combos == 2`, `total_combos == 10`
  - `test_token_expiry_mid_crawl` (Review Focus 1): 첫 조합 이후 모든 조합 401 → `failed_combos == total_combos - 1`, sleep 미호출

- [ ] **Step 2: Run** `$PY -m unittest tests.test_crawler_retry -v` → Expected: FAIL/ERROR (새 형식·속성 없음)

- [ ] **Step 3: Implement** 위 Interfaces대로. JS는 `r.ok`가 아니면 `{ok:false,status:r.status}`, catch에서 `{ok:false,status:0}`. 재시도 대상: status 0, 429, ≥500. 실패 조합은 `logger.warning("페이지 요청 실패로 조합 수집 중단 (%s/%s/%s page=%s status=%s)", ...)`. `crawl_all`의 비율 검사는 `persist_live_crawl` 안, `MIN_LIVE_CRAWL_RATIO` 검사 앞. 실패 시 `log_crawl(session_id, len, len, "failed", source)` 후 실패 결과 반환.

- [ ] **Step 4: Run** `$PY -m unittest tests.test_crawler_retry -v` → PASS. 이어서 `UPDATE_GOLDEN=1 $PY -m unittest tests.test_refactor_characterization.CrawlerCharacterizationTest`, `git diff tests/golden` 확인: `listings` 배열은 동일하고 바뀐 것이 `evaluate`가 실패 키(`("111","OPST","A1",1)` 예외)에 대해 재시도한 호출 수(`evaluate_count`)뿐인지 확인. 그 외 차이가 있으면 버그. 전체 `$PY -m unittest discover -s tests` → OK

- [ ] **Step 5: Commit** `git commit -m "fix(crawler): retry failed page requests and fail crawl when >20% combos fail"`

### Task 2: 텔레그램 전송과 알림 상태 파일 (v1 notifier.py)

**Files:**
- Create: `notifier.py`, `tests/test_notifier.py`

**Interfaces:**
- Produces:
  - `load_telegram_config(env: Mapping[str, str] = os.environ, hermes_env_path: Path = Path.home() / ".hermes/.env") -> Optional[Dict[str, str]]` → `{"token", "chat_id", "thread_id"(선택)}` 또는 None
  - `send_telegram(text: str, *, config: Optional[Dict] = None, post=requests.post) -> bool` — `config` None이면 `load_telegram_config()`. `https://api.telegram.org/bot{token}/sendMessage`에 `{"chat_id", "text", "disable_web_page_preview": True, ["message_thread_id"]}`, timeout 15. 설정 없음/예외/`ok` 아님 → WARNING 로그, False
  - `class AlertState(path: Path)`: `record_attempt(day: date, success: bool)`, `had_failure(day: date) -> bool`, `add_pending(text: str)`, `take_pending() -> List[str]`(꺼내면서 비움), 자동 저장. 손상된 JSON은 빈 상태로 시작(WARNING). 30일 지난 날짜 기록은 저장 시 정리.

- [ ] **Step 1: Write the failing tests** — `test_config_prefers_env_over_hermes`, `test_config_reads_hermes_home_channel_and_thread`, `test_config_none_when_missing`, `test_send_returns_false_and_never_raises_on_exception` (post가 ConnectionError), `test_send_returns_false_when_not_ok`, `test_send_posts_expected_payload`, `test_alert_state_round_trip_and_take_pending_clears`, `test_alert_state_tolerates_corrupt_file`, `test_alert_state_prunes_old_days`

- [ ] **Step 2: Run** `$PY -m unittest tests.test_notifier -v` → FAIL (모듈 없음)

- [ ] **Step 3: Implement** `notifier.py` (Hermes `.env` 파싱은 `app.load_env_file`과 같은 규칙: `#`·빈 줄 무시, 따옴표 제거. app을 import하지 말고 notifier 안에 작은 파서)

- [ ] **Step 4: Run** → PASS, 전체 테스트 OK

- [ ] **Step 5: Commit** `git commit -m "feat: add telegram notifier and local alert state"`

### Task 3: 하루 3회 재시도와 알림 규칙 (v1 scripts)

**Files:**
- Modify: `database.py` (`has_successful_live_crawl_on`), `scripts/run_remote_crawl.py`, `scripts/run_remote_crawl.sh`, `scripts/install_launchd_crawl.py`
- Create: `tests/test_run_remote_crawl.py`

**Interfaces:**
- Consumes: Task 1 결과 dict(`failed_combos`, `total_combos`), Task 2 `send_telegram`, `AlertState`
- Produces:
  - `Database.has_successful_live_crawl_on(day: date) -> bool` — `status='success' AND COALESCE(source,'naver') <> 'demo' AND crawled_at >= ? AND crawled_at < ?` (파라미터 `day.isoformat()` 00:00, 다음날 00:00 — SQLite는 ISO 문자열, Postgres는 TIMESTAMP 비교 모두 동작하도록 `datetime` 객체 전달)
  - `run_remote_crawl.py` 인자: `--skip-if-succeeded-today`, `--final-hour INT`(기본 15), `--label TEXT`(기본 `"주거용"`), `--alert-state PATH`(기본 `logs/crawl_alert_state.json`)
  - `decide_alerts(*, label: str, success: bool, now: datetime, final_hour: int, had_earlier_failure: bool, failed_combos: int, total_combos: int, error: Optional[str]) -> List[str]` — 순수 함수, 보낼 메시지 목록
  - `run(args, *, now_fn, db_factory, crawler_factory, send) -> int` — `main()`이 호출, 테스트 주입용. 반환값은 exit code
  - `install_launchd_crawl.py`: `--retry-hours "12,15"`, `--final-hour`, `--label`; `StartCalendarInterval`을 `[{Hour,Minute}, ...]` 배열로, `ProgramArguments`에 `--skip-if-succeeded-today --final-hour N --label X` 추가

- 메시지 문구 (고정):
  - 최종 실패: `"[{label}] ❌ 오늘 크롤링 최종 실패 ({HH:MM})\n원인: {error 또는 'status=failed'}\n이전 정상 데이터가 계속 표시됩니다."`
  - 복구: `"[{label}] ✅ 크롤링 복구 ({HH:MM}) — 앞선 시도 실패 후 성공"`
  - 부분 누락: `"[{label}] ⚠️ 일부 조합 수집 실패 {failed}/{total} — 저장은 완료"` (복구와 동시면 두 줄을 `\n`으로 합쳐 메시지 1개)

- [ ] **Step 1: Write the failing tests**
  - `test_has_successful_live_crawl_on_uses_day_bounds` (SQLite: 전날 23:59 성공·당일 failed·demo → False, 당일 00:00 success → True)
  - `decide_alerts` 표: 실패+final 이전 → `[]`; 실패+final 이후 → 최종 실패 1개; 성공+앞선 실패 → 복구 1개; 성공+누락 → 부분 누락; 성공+앞선 실패+누락 → 합친 1개; 정상 성공 → `[]`
  - `test_run_skips_when_already_succeeded_today`: crawler_factory 미호출, exit 0, 알림 없음
  - `test_run_db_connect_failure_at_final_hour_alerts_and_exits_1`
  - `test_run_failed_send_is_kept_pending_and_sent_next_run` (Review Focus 2): 1차 send False → 상태 파일에 pending, 2차 실행 시작 시 그 메시지를 먼저 send
  - `test_install_plist_has_three_slots_and_skip_args` (`build_plist` 계열 함수가 없다면 Step 3에서 `build_plist_payload(args) -> dict`로 분리해 테스트)

- [ ] **Step 2: Run** `$PY -m unittest tests.test_run_remote_crawl -v` → FAIL

- [ ] **Step 3: Implement** — 실행 순서: 시작 → pending 먼저 전송 → DB 연결(실패 시 예외 문자열을 error로) → skip 검사 → 크롤 → `AlertState.record_attempt` → `decide_alerts` → 전송(실패분 pending). 예외는 잡아 기록·알림 후 exit 1. `run_remote_crawl.sh`: 네트워크 대기 `max=180` × 5초, DB DNS 대기 `max=90` × 10초.

- [ ] **Step 4: Run** → PASS, 전체 테스트 OK. `bash -n scripts/run_remote_crawl.sh` OK

- [ ] **Step 5: Commit** `git commit -m "feat: retry crawl up to three times a day with telegram failure/recovery alerts"`

### Task 4: 진짜 급매 점수 저장과 API 필터 (v1)

**Files:**
- Create: `bargain.py`, `tests/test_bargain.py`, `scripts/backfill_bargain_scores.py`
- Modify: `database.py` (컬럼 추가, `insert_listings`, `_append_listing_filters`, `get_listings`, `backfill_bargain_scores`), `app.py` (`listing_filter_args`), `tests/golden/refactor_characterization.json`

**Interfaces:**
- Produces:
  - `bargain.py`: `BARGAIN_MIN = 0.10`, `BARGAIN_MAX = 0.40`, `MIN_GROUP = 3`; `area_number(area: Optional[str]) -> Optional[float]` (앞자리 숫자, "84A"→84.0, ""→None); `compute_bargain_scores(listings: Iterable[Mapping]) -> Dict[str, Tuple[float, str]]` (article_no → (discount, basis), basis ∈ `"same_area"`, `"complex_m2"`), 입력 키: `article_no, district, building_name, property_type, trade_type, area, price_sort_value`; `is_bargain(discount: Optional[float]) -> bool`
  - 계산 대상: `trade_type == "매매"`, `property_type in {"아파트","오피스텔"}`, 건물명 strip 후 `GENERIC_BUILDING_NAMES`에 없음·비어있지 않음, area 비어있지 않음, `price_sort_value > 0`. `GENERIC_BUILDING_NAMES`는 `database.py`에서 `bargain.py`로 옮기고 `database.py`는 import해서 재노출(기존 import 경로 유지)
  - discount = `1 - value / median` (같은 평형) 또는 `1 - (value/area) / median(㎡당)` (단지), 소수점 4자리 반올림
  - DB: `listings.bargain_discount` (SQLite REAL / Postgres DOUBLE PRECISION), `listings.bargain_basis TEXT`. 대상이 아니면 둘 다 NULL
  - `_append_listing_filters(..., bargain_only=False)` → `bargain_discount >= 0.1 AND bargain_discount <= 0.4`
  - `get_listings(..., bargain_only=False)` 응답에 `bargain_count` (현재 필터 범위 내 진짜 급매 수). `get_map_listings(..., bargain_only=False)`, SELECT에 `bargain_discount` 추가
  - `Database.backfill_bargain_scores(session_id: Optional[str] = None) -> int` (기본 최신 가시 세션, 갱신 행 수)
  - `app.listing_filter_args()`에 `"bargain_only": request_flag("bargain_only")`

- [ ] **Step 1: Write the failing tests** (`tests/test_bargain.py`)
  - `test_same_area_group_of_three_uses_price_median` (5억·5억·4억 → 4억 매물 discount 0.2, basis same_area)
  - `test_group_of_two_falls_back_to_complex_m2` / `test_no_score_when_complex_has_fewer_than_three`
  - `test_excludes_rent_generic_names_missing_area_and_nonpositive_price` (전세, "빌라", area "", price_sort_value None·0)
  - `test_area_number_parses_type_suffix` ("84A"→84.0, "113E"→113.0, ""/None→None)
  - `test_is_bargain_bounds` (0.0999 False, 0.10 True, 0.40 True, 0.4001 False, None False)
  - `test_insert_listings_stores_scores_and_filter` (SQLite: 저장 후 `get_listings(bargain_only=True)`가 해당 매물만, `bargain_count` 일치, 맵도 동일)
  - `test_listings_api_bargain_only_param`
  - `test_backfill_fills_existing_session`

- [ ] **Step 2: Run** `$PY -m unittest tests.test_bargain -v` → FAIL

- [ ] **Step 3: Implement.** 중앙값은 `statistics.median`. `insert_listings` 행 튜플에 두 값 추가(INSERT 컬럼·UPSERT SET 모두).

- [ ] **Step 4: Run** → PASS. `UPDATE_GOLDEN=1`로 골든 재생성 후 `git diff tests/golden`에 **추가된 줄만**(`bargain_discount`, `bargain_basis`, `bargain_count`) 있는지 확인 — `git diff -U0 tests/golden | grep '^-' | grep -v '^---'`가 비어 있어야 함(JSON 쉼표 변화로 인한 줄은 예외로 눈으로 확인). 전체 테스트 OK

- [ ] **Step 5: Commit** `git commit -m "feat: store bargain discount vs complex median and add bargain_only filter"`

### Task 5: 사이트 배지와 "진짜 급매" 통계 카드 (v1 frontend)

**Files:**
- Modify: `templates/index.html` (stats-bar에 카드 추가), `static/js/app.js` (state.filters.bargain_only, 목록·지도 쿼리, 카드 클릭, 배지 렌더, 요약 문구), `static/css/style.css` (`.badge-bargain`)

**Interfaces:**
- Consumes: Task 4 API (`bargain_only`, `bargain_count`, 매물 `bargain_discount`, `bargain_basis`)
- 카드: `<div class="stat-card stat-clickable" data-filter="bargain"><span class="stat-val" id="stat-bargain">—</span><span class="stat-lbl">진짜 급매</span></div>` (가격인하 카드 뒤)
- 배지: `0.10 <= d <= 0.40`일 때 `💎 단지 중앙값 −{Math.round(d*100)}%`, title은 basis가 `same_area`면 `"같은 단지·같은 평형 매매 중앙값 대비"`, `complex_m2`면 `"같은 단지 ㎡당 매매가 중앙값 대비"`
- 카드 클릭 동작은 가격인하 카드와 동일 패턴(다른 stat 필터와 상호 배타, `bargain_only` 리셋 포함), 필터 요약에 `'진짜 급매만'`

- [ ] **Step 1: 로컬 실행 준비** — 테스트용 SQLite DB에 데모 + 점수 있는 매물을 넣는 스크립트 없이, `FORCE_LOCAL_SQLITE=1 DB_PATH=<scratch>/ui.db` 로 앱을 띄우고 `tests/test_bargain.py`의 픽스처와 같은 매물을 `insert_listings`로 시드
- [ ] **Step 2: Implement** 위 항목
- [ ] **Step 3: Verify in browser** — 카드 숫자 = API `bargain_count`, 카드 클릭 시 목록이 진짜 급매만, 배지 문구/툴팁, 다시 클릭 시 해제, 가격인하 카드와 전환, 모바일 폭(390px)에서 카드 줄 깨짐 없음. 콘솔 오류 없음
- [ ] **Step 4: Run** 전체 테스트 OK, `node --check static/js/app.js`
- [ ] **Step 5: Commit** `git commit -m "feat(ui): bargain badge and stat-card filter"`

### Task 6: 주간 리포트 (v1)

**Files:**
- Create: `weekly_report.py`, `scripts/send_weekly_report.py`, `scripts/install_launchd_weekly_report.py`, `tests/test_weekly_report.py`
- Modify: `database.py` (리포트용 조회 메서드)

**Interfaces:**
- Consumes: Task 2 `send_telegram`, Task 4 `bargain_discount`, `is_bargain`
- Produces:
  - `Database.get_weekly_report_data(now: datetime) -> Optional[Dict]` → `{"latest": {session_id, crawled_at, total_count}, "previous": {...}|None, "ratio_now": float|None, "ratio_prev": float|None, "risers": [(district, building, prev, cur, diff)]×≤5, "fallers": [...]×≤5, "bargains": [row dict]×≤5, "bargain_total": int, "anomaly_count": int}` — previous = `crawled_at <= latest - 7일 + 12시간`인 가장 최근 라이브 성공, 없으면 가장 오래된 라이브 성공(최신과 다를 때)
  - `weekly_report.format_weekly_report(data: Dict, site_url: str) -> str` — 2026-10-22 스파이크와 같은 형식, 단 첫 줄 `"📊 주간 급매 리포트"`(테스트 접두어 없음), 진짜 급매 줄 `· {building} ({district}) {area}㎡ {floor}층 {price} (−{pct}%)` + 다음 줄 링크
  - `scripts/send_weekly_report.py --database-url ... [--dry-run]` → exit 0/1
  - `install_launchd_weekly_report.py`: label `com.lunadad.naver-real-estate-weekly-report`, `StartCalendarInterval {Weekday: 1, Hour: 10, Minute: 30}`, DB URL은 `--database-url` 또는 기존 크롤 plist에서 읽기

- [ ] **Step 1: Write the failing tests** — SQLite에 세션 3개(오늘, 7일 전, 14일 전) + building stats + 점수 매물 시드
  - `test_report_compares_with_session_seven_days_ago`
  - `test_report_lists_top_risers_and_fallers_limited_to_five`
  - `test_report_bargains_use_stored_discount_and_count_anomalies` (0.45 → anomaly_count, 목록 제외)
  - `test_report_without_previous_week_or_building_stats` (Review Focus 5: 세션 1개만 → 변동 줄 없이도 메시지 생성, 예외 없음)
  - `test_report_none_when_no_live_crawl`
  - `test_send_script_dry_run_does_not_send`
- [ ] **Step 2: Run** `$PY -m unittest tests.test_weekly_report -v` → FAIL
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run** → PASS, 전체 OK
- [ ] **Step 5: Commit** `git commit -m "feat: weekly telegram market report"`

### Task 7: 상업용(v2) 이식

**Files (v2 worktree):**
- Modify: `crawler.py`, `database.py`(`has_successful_live_crawl_on`), `scripts/run_remote_crawl.py`, `scripts/run_remote_crawl.sh`, `scripts/install_launchd_crawl.py`
- Create: `notifier.py`(v1과 동일 파일 복사), `tests/test_crawler_retry.py`, `tests/test_notifier.py`, `tests/test_run_remote_crawl.py`

**Interfaces:** Task 1~3과 동일한 이름·시그니처. v2 기본값: `--label 상업용`, `--final-hour 16`, 재시도 `13,16`. v2 `_fetch_combo_all_pages`의 상업용 필드 추출 로직은 그대로 두고 페이지 요청·재시도 부분만 교체.

- [ ] **Step 1:** v1 테스트 3개를 v2용으로 복사·조정(v2엔 특성화 골든 없음 — 가짜 Playwright 헬퍼를 `tests/test_crawler_retry.py` 안에 둔다)
- [ ] **Step 2: Run** v2에서 `$PY -m unittest discover -s tests -p 'test_*.py'` → 새 테스트 FAIL, 기존 통과
- [ ] **Step 3: Implement** (v1 변경을 v2 구조에 맞게 이식)
- [ ] **Step 4: Run** → 전체 PASS, `node --test tests/*.test.mjs` 기존 JS 테스트 PASS, `bash -n scripts/run_remote_crawl.sh`
- [ ] **Step 5: Commit** `git commit -m "feat: crawl retry, three daily slots and telegram alerts for commercial"`

### Task 8: 배포 (사용자 승인 완료 2026-10-22)

- [ ] **Step 1:** 두 브랜치 전체 테스트 재실행 후 병합: v1 `main` ← `feature/reliability-bargain-report` (ff), v2 `version-2.0` ← `feature/crawl-reliability-v2` (ff, `naver-real-estate-v2` 폴더에서). `git push origin main version-2.0`
- [ ] **Step 2:** 크롤 중이 아닌지 확인(`launchctl list | grep lunadad` PID 없음) 후 `~/workspace/naver-real-estate`를 `git checkout --detach main`
- [ ] **Step 3:** 두 크롤 plist 재설치(기존 DB URL 유지, `--mode agent`, 주거 9시+`12,15`, 상업 10시+`13,16`) → `plutil -p`로 슬롯 3개·인자 확인, `launchctl bootout/bootstrap`
- [ ] **Step 4:** 주간 리포트 plist 설치, `send_weekly_report.py --dry-run` 출력 확인
- [ ] **Step 5:** 운영 DB `backfill_bargain_scores.py` 실행 → 갱신 행 수 기록
- [ ] **Step 6:** v1 폴더에서 `vercel --prod`, 배포 URL에서 `/api/listings?bargain_only=true` 응답·`bargain_count` 확인, 브라우저로 카드/배지 확인
- [ ] **Step 7:** `notifier.send_telegram("[테스트] 크롤링 알림 경로 확인")` 1회 → True
