# 신규 기능 6종 설계 (단지 상세·가격 이력·사라진 매물·히트맵·맞춤 알림·텔레그램 봇)

작성일: 2026-10-22 · 대상: v1(주거용, `main`)만 · 근거: 사용자가 승인한 목업 캔버스 https://claude.ai/artifact/74STDzmFpFEC4NeT544RFk ("1~6 더 진행해")

## 왜
매일 쌓이는 급매 스냅샷과 진짜 급매 점수를 "어디가, 얼마나, 왜 싼지" 판단하는 도구로 바꾼다. 목업 6장이 화면의 기준이다. 목업의 예시 숫자는 실제 데이터로 대체한다.

## 공통 제약
- 기존 API 응답은 **추가만** 허용(특성화 골든 테스트 유지). 기존 동작 변경 금지.
- 새 라우트·DB 로직은 가능한 새 모듈에 둔다: Flask Blueprint `views/<feature>.py`, 순수 계산 `insights/<feature>.py`. `app.py`/`database.py`/`static/js/app.js`/`templates/index.html` 수정은 최소한의 연결부만.
- 새 페이지는 `templates/<page>.html` + `static/js/<page>.js`, 공용 `static/css/style.css` 재사용(새 CSS는 파일 끝 섹션에 추가), `asset_url()` 사용, 모바일 390px에서 가로 스크롤 없음.
- 진짜 급매 판정은 `bargain.is_bargain` (0.10~0.40) 하나만 쓴다.
- SQL은 SQLite·Postgres 공통(`?` 플레이스홀더, ISO 문자열 시각). 큰 IN 목록은 500개씩 나눈다.
- 데이터 범위: listings 테이블은 최근 2개 세션만 유지된다. 장기 데이터는 새 테이블에 쌓는다.

## ① 단지 상세 페이지
- `GET /complex?district=..&name=..` → 페이지, `GET /api/complex?district=..&building_name=..` → JSON:
  `{region, district, building_name, stats:{current_count, week_ago_count, bargain_count, price_down_ratio}, areas:[{area, median, prices:[{value, bargain}]}], history:<build_building_history_series 14일>, listings:[최신 세션 해당 단지 매물, 진짜 급매 우선·할인율 순]}`. 분포는 매매·`price_sort_value>0`만. week_ago_count는 `crawl_building_stats`에서 7일 전(±12h) 세션.
- 목록 카드와 히어로 "급매 증가/감소 단지" 행에서 단지 페이지로 가는 링크 추가.
- "이 단지 알림 받기" 버튼 = 기존 단지 변동 알림(`building_name`+`min_daily_change=3`) 생성. "관심 단지" 버튼은 만들지 않는다(내 판단: 기능 없는 버튼 금지).
- 없는 단지/파라미터 누락: 404/400 JSON, 페이지는 "데이터 없음" 표시.

## ② 매물 가격 이력
- 새 테이블 `listing_tracking(article_no PK, first_seen_at, last_seen_at, last_price, last_price_sort_value, price_change_count, price_cut_count)`, `listing_price_events(id, article_no, session_id, observed_at, price, price_sort_value, prev_price, prev_price_sort_value, change_pct)`; `UNIQUE(article_no, session_id)`.
- `insert_listings()`가 같은 트랜잭션에서 갱신: 처음 본 매물은 tracking 생성(+최초 이벤트), 가격 문자열이 바뀌면 이벤트 추가·카운트 증가(값이 줄면 cut). 90일 이상 안 보인 tracking/이벤트는 정리.
- `listings`에 `first_seen_at`, `price_cut_count` 컬럼 추가(응답 필드 추가). 카드에 "인하 N회" 배지(N≥1), "가격 이력" 버튼 → 모달(계단형 그래프 + 타임라인 + 협상 단서: 처음 발견 후 경과일, 인하 횟수, 누적 변화율, 마지막 변경 후 일수, 진짜 급매면 단지 중앙값 대비).
- `GET /api/listings/<article_no>/history`.
- "등록 후 경과"가 아니라 "처음 발견 후"로 표기(내 판단: 실제 등록일은 모름). 이력은 배포일부터 쌓인다.

## ③ 사라진 매물 추적
- 새 테이블 `listing_disappearances(id, article_no, session_id, disappeared_at, region, district, building_name, property_type, trade_type, area, floor, price, naver_url, first_seen_at, was_bargain, had_price_down, reappeared_at)`; `UNIQUE(article_no, session_id)`.
- `insert_listings(listings, session_id, incomplete_scopes=None)`: 새 세션 upsert 후 직전 세션 꼬리표가 남은 행 = 사라진 매물로 기록. 단 `incomplete_scopes`(크롤러가 실패한 (구, 매물유형, 거래유형) 조합)에 속한 행은 기록하지 않는다(가짜 소멸 방지). 크롤러는 `last_crawl_stats["failed_scopes"]`로 넘긴다.
- 새 세션에 다시 나타난 article_no는 `reappeared_at` 설정 → 화면에서 "철회 후 재등록".
- `GET /disappeared` 페이지, `GET /api/disappeared?days=1|7|30&limit=` (최근순), `GET /api/disappeared/fast-complexes?days=7` (crawl_building_stats로 7일 전 대비 감소율 상위 5; 주간 리포트와 같은 비교 규칙).

## ④ 지역 히트맵
- `GET /heatmap` 페이지, `GET /api/heatmap?region=서울특별시` → `{region, regions:[이름…], metric_defs, districts:[{district, total, bargain_count, bargain_ratio, price_down_ratio, weekly_change_pct}]}`. bargain_ratio = 진짜 급매 / 해당 구 전체 급매. price_down_ratio·weekly_change는 `crawl_region_stats`(최신 vs 7일 전).
- 서울은 목업의 타일 배치(코드에 고정 좌표), 그 외 지역은 이름순 격자. 지표 3종 전환, 5단계 색, 상위 5 목록, 타일 클릭 → 메인 목록을 그 구로 필터(`/?district=`) — 메인이 쿼리 파라미터를 읽지 못하면 읽도록 최소 연결.

## ⑤ 진짜 급매 맞춤 알림 (웹)
- `alert_rules`에 `bargain_only INTEGER DEFAULT 0`, `min_discount REAL`, `max_price INTEGER`(만원) 추가. `_collect_alert_matches`에 조건 반영(`bargain_only`면 `bargain_discount >= max(min_discount, 0.10) AND <= 0.40`, `max_price`면 `price_sort_value <= ?`). API 생성 파라미터 추가, 규칙 이름 자동 생성에 반영.
- `/api/listings`·`/api/map-listings`에 `max_price` 필터 추가.
- 사이드바 알림 영역에 "진짜 급매만", 최소 할인율(10/15/20/30%), 매매가 상한 입력 추가.
- 웹 푸시 발송은 지금 운영에서 돌지 않는다(Vercel 스케줄러 꺼짐, 맥미니는 VAPID 키 없음) — 범위 밖, 사용자에게 보고.

## ⑥ 텔레그램 질의 봇 (Hermes 플러그인)
- Hermes가 이미 같은 봇으로 메시지를 받으므로 별도 봇 금지. `ctx.register_command`로 슬래시 명령을 등록하는 Hermes 플러그인 `real-estate`를 저장소 `integrations/hermes/real-estate/`에 두고 `~/.hermes/plugins/real-estate`로 설치(심볼릭 링크), `hermes plugins enable real-estate` 후 게이트웨이 재시작.
- 명령(텔레그램 명령은 영문만 가능): `/deals <지역> [가격상한] [할인율%]` 진짜 급매 상위 5, `/complex <단지명> [구]` 단지 요약, `/weekly` 주간 리포트 즉시, `/dealalert add|list|del ...` 텔레그램 알림 규칙.
- 플러그인은 DB 비밀번호를 갖지 않는다: 공개 사이트 API(`https://naver-real-estate-henna.vercel.app`)만 호출. `/weekly`용 `GET /api/weekly-report`(텍스트) 추가.
- 텔레그램 알림 규칙은 공개 API가 아니라 맥미니 로컬 파일 `~/workspace/naver-real-estate/logs/telegram_alert_rules.json`에 저장(누구나 만든 규칙이 내 텔레그램으로 오는 걸 막기 위함). 크롤 성공 후 `run_remote_crawl.py`가 규칙별로 최신 세션의 조건 맞는 진짜 급매 중 아직 안 보낸 것을 모아 1개 메시지로 전송(보낸 article_no 기록, 30일 정리). 전송 실패는 기존 pending 큐.
- 인자 파싱: 지역은 `data/regions.json`의 시/구 이름 부분 일치, 가격 "20억", "15억5천", "9000"(만원), 할인 "15%".

## 비목표
- 상업용(v2) 적용, 실거래가 API 연동, 관심 단지 기능, 웹 푸시 발송 경로 복구.

## 배포
v1 `main` 병합·push → 크롤 폴더 `git checkout --detach main` → `vercel --prod` → Hermes 플러그인 설치·활성화·게이트웨이 재시작 → 텔레그램 `/deals` 동작 확인. 새 테이블은 앱/크롤러 시작 시 init_db가 만든다.
