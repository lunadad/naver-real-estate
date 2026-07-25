// app.js 특성화 시나리오 — 실행 결과를 baseline JSON과 비교한다.
// 재생성: node -e "import('./tests/helpers/scenarios.mjs').then(async m => console.log(JSON.stringify(await m.runScenarios(), null, 2)))" > tests/fixtures/app_js_baseline.json
import { loadApp } from './load_app.mjs';

export function runScenarios() {
  
  const { get, call, registry } = loadApp();
  const out = {};
  
  // ── 순수 유틸 ──
  out.fmtNum = {
    int: call(`fmtNum(1234567)`),
    str: call(`fmtNum('12')`),
    null: call(`fmtNum(null)`),
    undef: call(`fmtNum(undefined)`),
    float: call(`fmtNum(1.5)`),
    neg: call(`fmtNum(-3)`),
  };
  out.escHtml = {
    amp: call(`escHtml('a&b<c>d"e\\'f')`),
    plain: call(`escHtml('한글 텍스트')`),
    null: call(`escHtml(null)`),
    undef: call(`escHtml(undefined)`),
    num: call(`escHtml(42)`),
  };
  out.formatDate = {
    ok: call(`formatDate('20260315')`),
    short: call(`formatDate('2026')`),
    empty: call(`formatDate('')`),
    null: call(`formatDate(null)`),
  };
  out.parseTags = {
    arrayIdentity: call(`(() => { const a = ['x']; return parseTags(a) === a; })()`),
    json: call(`parseTags('["가격인하","급매"]')`),
    invalid: call(`parseTags('not json')`),
    null: call(`parseTags(null)`),
    jsonNull: call(`parseTags('null')`),
  };
  out.formatMetricValue = {
    num: call(`formatMetricValue(3.14159, '%')`),
    round: call(`formatMetricValue(3.15, '%')`),
    int: call(`formatMetricValue(1234, '㎡')`),
    null: call(`formatMetricValue(null)`),
    empty: call(`formatMetricValue('')`),
    undef: call(`formatMetricValue(undefined)`),
    text: call(`formatMetricValue('협의')`),
  };
  out.formatAreaM2 = {
    num: call(`formatAreaM2({ area_m2: 84.567 })`),
    zero: call(`formatAreaM2({ area_m2: 0, area: '10평' })`),
    none: call(`formatAreaM2({})`),
    strArea: call(`formatAreaM2({ area_m2: 'abc', area: '33㎡' })`),
  };
  out.inferPremiumLabel = {
    direct: call(`inferPremiumLabel({ premium_info: '무권리' }, [])`),
    noRights: call(`inferPremiumLabel({}, ['무권리금'])`),  // '무권리' 포함
    hasKeyword: call(`inferPremiumLabel({ description: '권리금 5000' }, [])`),
    none: call(`inferPremiumLabel({}, [])`),
    noneText: call(`inferPremiumLabel({ description: '권리금 없음 매물' }, [])`),
  };
  out.inferRoadLabel = {
    direct: call(`inferRoadLabel({ road_access: '코너각지' }, [])`),
    corner: call(`inferRoadLabel({ description: '코너 자리' }, [])`),
    main: call(`inferRoadLabel({}, ['대로변'])`),
    jeopdo: call(`inferRoadLabel({ description: '도로접함' }, [])`),
    none: call(`inferRoadLabel({}, [])`),
  };
  out.priceDropLabel = {
    nullTrue: call(`priceDropLabel({ price_drop_rate: null }, true)`),
    nullFalse: call(`priceDropLabel({ price_drop_rate: null }, false)`),
    num: call(`priceDropLabel({ price_drop_rate: 12.34 }, false)`),
    strNum: call(`priceDropLabel({ price_drop_rate: '5.5' }, false)`),
    nan: call(`priceDropLabel({ price_drop_rate: 'abc' }, true)`),
  };
  out.tradeBadgeClass = {
    buy: call(`tradeBadgeClass('매매')`),
    jeon: call(`tradeBadgeClass('전세')`),
    month: call(`tradeBadgeClass('월세')`),
    etc: call(`tradeBadgeClass('단기')`),
  };
  out.urgencyColor = {
    null: call(`urgencyColor(null)`),
    nan: call(`urgencyColor('x')`),
    low: call(`urgencyColor(19)`),
    mid: call(`urgencyColor(20)`),
    mid2: call(`urgencyColor(49)`),
    high: call(`urgencyColor(50)`),
  };
  out.urlBase64ToUint8Array = call(`Array.from(urlBase64ToUint8Array('AQID'))`);
  out.formatAreaRange = {
    both: call(`formatAreaRange(10, 50)`),
    min: call(`formatAreaRange(10, null)`),
    max: call(`formatAreaRange(null, 50)`),
    none: call(`formatAreaRange(null, null)`),
  };
  out.tradeScopeLabel = {
    sale: call(`tradeScopeLabel('sale')`),
    rent: call(`tradeScopeLabel('rent')`),
    etc: call(`tradeScopeLabel('')`),
  };
  out.hasAlertCondition = {
    empty: call(`hasAlertCondition({ keyword:'', district:'', property_type:'', trade_type:'', trade_scope:'', min_area_m2:null, max_area_m2:null, min_price_drop_rate:null })`),
    kw: call(`hasAlertCondition({ keyword:'강남', district:'', property_type:'', trade_type:'', trade_scope:'', min_area_m2:null, max_area_m2:null, min_price_drop_rate:null })`),
    area: call(`hasAlertCondition({ keyword:'', district:'', property_type:'', trade_type:'', trade_scope:'', min_area_m2:0, max_area_m2:null, min_price_drop_rate:null })`),
  };
  out.formatTrendInsightName = {
    display: call(`formatTrendInsightName({ display_name: '서울 강남구', region: 'x', district: 'y' })`),
    fallback: call(`formatTrendInsightName({ region: '서울특별시', district: '강남구' })`),
  };
  out.formatSeoulTrendSummary = call(`formatSeoulTrendSummary([{ district: '강남구', diff: 3 }, { district: '마포구', diff: -2 }])`);
  
  // ── state 의존 ──
  out.buildCurrentFilterLabel = {
    default: call(`buildCurrentFilterLabel()`),
    full: call(`(() => {
      Object.assign(state.filters, { search: '테헤란로', district: '강남구', property_type: '상가', trade_type: '매매', price_down_only: true });
      const r = buildCurrentFilterLabel();
      Object.assign(state.filters, { search: '', district: '', property_type: '', trade_type: '', price_down_only: false });
      return r;
    })()`),
  };
  out.buildQuery = {
    default: call(`buildQuery()`),
    extra: call(`buildQuery({ days: 7 })`),
    filtered: call(`(() => {
      Object.assign(state.filters, { search: '역삼', trade_type: '매매', price_down_only: true });
      state.page = 3;
      const r = buildQuery();
      Object.assign(state.filters, { search: '', trade_type: '', price_down_only: false });
      state.page = 1;
      return r;
    })()`),
  };
  
  // getListingCardProfile
  const landListing = `{ property_type: '토지', area_m2: 660.5, land_use_zone: '계획관리', land_category: '전', description: '코너 자리' }`;
  const officeListing = `{ property_type: '업무', area_m2: 84, floor: '5/10', trade_type: '매매', estimated_yield_rate: 4.5, price_drop_rate: 3.2 }`;
  const shopListing = `{ property_type: '상가', area_m2: 33, floor: '1/5', trade_type: '월세', description: '무권리 대로변', premium_info: null, price_drop_rate: null }`;
  out.profileLand = call(`JSON.stringify(getListingCardProfile(${landListing}, [], false))`);
  out.profileOffice = call(`JSON.stringify(getListingCardProfile(${officeListing}, [], true))`);
  out.profileShop = call(`JSON.stringify(getListingCardProfile(${shopListing}, ['가격인하'], true))`);
  
  // ── DOM 렌더 함수 ──
  const trendItems = `[
    { district: '강남구', region: '서울특별시', display_name: '서울 강남구', diff: 5, prev_cnt: 10, current_cnt: 15, price_down_count: 2 },
    { district: '해운대구', region: '부산광역시', diff: 3, prev_cnt: 1, current_cnt: 4, price_down_count: 0 },
  ]`;
  call(`renderTrendList('list-increasing', ${trendItems}, 'up')`);
  out.trendUp = registry.get('list-increasing').innerHTML;
  call(`renderTrendList('list-decreasing', [{ district: '마포구', region: '서울특별시', diff: -4, prev_cnt: 9, current_cnt: 5, price_down_count: 1 }], 'down')`);
  out.trendDown = registry.get('list-decreasing').innerHTML;
  call(`renderTrendList('list-price-down', ${trendItems}, 'price-down')`);
  out.trendPriceDown = registry.get('list-price-down').innerHTML;
  call(`renderTrendList('list-increasing', [], 'up')`);
  out.trendEmpty = registry.get('list-increasing').innerHTML;
  
  call(`renderRegionStats([
    { district: '강남구', region: '서울특별시', display_name: '서울 강남구', total: 120 },
    { district: '수원시', region: '경기도', total: 30 },
  ])`);
  out.regionStats = registry.get('list-region-stats').innerHTML;
  
  // renderListings
  const listingsData = `{
    total: 42, total_pages: 3, page: 2,
    listings: [
      {
        id: 1, article_no: 'A100', naver_url: 'https://new.land.naver.com/x?a=1&b=2',
        trade_type: '매매', property_type: '상가', region: '서울특별시', district: '강남구',
        building_name: '테스트<상가>&빌딩', price: '3억 5,000', confirmed_date: '20260701',
        tags: '["급매","가격인하","1층"]', description: '무권리 코너 "명당"',
        area_m2: 33.06, floor: '1/5', estimated_yield_rate: null, price_drop_rate: 8.5,
        raw_property_code: 'SG',
      },
      {
        id: 2, article_no: 'B200', naver_url: '',
        trade_type: '월세', property_type: '토지', region: '경기도', district: '수원시',
        building_name: null, price: null, confirmed_date: null,
        tags: null, description: null, area_m2: null,
        land_use_zone: null, land_category: null,
      },
    ],
  }`;
  call(`renderListings(${listingsData})`);
  out.listingsGrid = registry.get('listings-grid').innerHTML;
  out.pageInfo = registry.get('page-info').textContent;
  out.btnPrevDisabled = registry.get('btn-prev').disabled;
  out.btnNextDisabled = registry.get('btn-next').disabled;
  out.statePageAfter = call(`state.page`);
  out.stateTotalPagesAfter = call(`state.totalPages`);
  call(`renderListings({ total: 0, total_pages: 0, page: 1, listings: [] })`);
  out.listingsEmpty = registry.get('listings-grid').innerHTML;
  out.pageInfoEmpty = registry.get('page-info').textContent;
  call(`state.page = 1; state.totalPages = 1;`);
  
  // updateStats
  call(`updateStats({ total: 100, type_counts: { '상가': 60, '업무': 30 }, price_down_count: 7 })`);
  out.statTotal = registry.get('stat-total').textContent;
  out.statShop = registry.get('stat-shop').textContent;
  out.statOffice = registry.get('stat-office').textContent;
  out.statLand = registry.get('stat-land').textContent;
  out.statPriceDown = registry.get('stat-price-down').textContent;
  out.listingsSummaryAfterStats = registry.get('listings-summary').textContent;
  out.dashboardAfterStats = call(`JSON.stringify({ total: state.dashboard.total, priceDownCount: state.dashboard.priceDownCount })`);
  out.insightAfterStats = registry.get('hero-insight-text').textContent;
  out.insightSubAfterStats = registry.get('hero-insight-subtext').textContent;
  
  // updateHeroInsight with trends
  call(`state.dashboard.trends = [
    { region: '서울특별시', district: '강남구', display_name: '서울 강남구', diff: 5 },
    { region: '서울특별시', district: '마포구', diff: -3 },
    { region: '부산광역시', district: '해운대구', diff: 2 },
  ]; updateHeroInsight();`);
  out.insightWithTrends = registry.get('hero-insight-text').textContent;
  out.insightSubWithTrends = registry.get('hero-insight-subtext').textContent;
  call(`state.dashboard.trends = []; state.dashboard.total = 0; state.dashboard.priceDownCount = 0; updateHeroInsight();`);
  out.insightEmpty = registry.get('hero-insight-text').textContent;
  
  // renderHeroDailySeries
  call(`renderHeroDailySeries([
    { date: '2026-07-01', label: '7/1', total_count: 100 },
    { date: '2026-07-02', label: '7/2', total_count: 120 },
    { date: '2026-07-03', label: '7/3', total_count: null },
    { date: '2026-07-04', label: '7/4', total_count: 90 },
  ])`);
  out.dailyChart = registry.get('hero-daily-chart').innerHTML;
  out.dailyLabels = registry.get('hero-daily-labels').innerHTML;
  out.dailyCurrent = registry.get('hero-daily-current').textContent;
  out.dailyChange = registry.get('hero-daily-change').textContent;
  out.dailyEmptyHidden = registry.get('hero-daily-empty')._classes.has('hidden');
  call(`renderHeroDailySeries([])`);
  out.dailyChartEmpty = registry.get('hero-daily-chart').innerHTML;
  out.dailyCurrentEmpty = registry.get('hero-daily-current').textContent;
  out.dailyChangeEmpty = registry.get('hero-daily-change').textContent;
  out.dailyEmptyShown = !registry.get('hero-daily-empty')._classes.has('hidden');
  
  // renderAlertRules + renderHeroAlertPreview
  call(`state.alertRules = [
    { id: 1, name: '강남 상가', keyword: '테헤란로', district: '강남구', property_type: '상가', trade_type: '매매', trade_scope: 'sale', min_area_m2: 20, max_area_m2: null, min_price_drop_rate: 5 },
    { id: 2, name: '전체 감시', keyword: '', district: '', property_type: '', trade_type: '', trade_scope: '', min_area_m2: null, max_area_m2: null, min_price_drop_rate: null },
  ]; renderAlertRules();`);
  out.alertRulesList = registry.get('alert-rules-list').innerHTML;
  out.heroAlertPreview = registry.get('hero-alert-preview').innerHTML;
  call(`state.alertRules = []; renderAlertRules();`);
  out.alertRulesEmpty = registry.get('alert-rules-list').innerHTML;
  out.heroAlertPreviewEmpty = registry.get('hero-alert-preview').innerHTML;
  
  // refreshAlertDraftSummary
  call(`(() => {
    document.getElementById('alert-keyword').value = '역삼역';
    document.getElementById('alert-min-area').value = '30';
    document.getElementById('alert-trade-scope').value = 'rent';
    Object.assign(state.filters, { district: '강남구', property_type: '상가' });
    refreshAlertDraftSummary();
  })()`);
  out.alertDraftSummary = registry.get('alert-current-filters').textContent;
  out.heroFilterSummary = registry.get('hero-filter-summary').textContent;
  call(`(() => {
    document.getElementById('alert-keyword').value = '';
    document.getElementById('alert-min-area').value = '';
    document.getElementById('alert-trade-scope').value = '';
    Object.assign(state.filters, { district: '', property_type: '' });
    refreshAlertDraftSummary();
  })()`);
  out.alertDraftSummaryEmpty = registry.get('alert-current-filters').textContent;
  
  // getNotificationStatusMessage — Notification 미지원 환경
  out.notifStatusUnsupported = call(`JSON.stringify(getNotificationStatusMessage())`);
  
  // updateListingsSummary
  call(`updateListingsSummary(null)`);
  out.summaryLoading = registry.get('listings-summary').textContent;
  call(`updateListingsSummary(1234)`);
  out.summaryDone = registry.get('listings-summary').textContent;
  
  return out;
}
