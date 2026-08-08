/* ═══════════════════════════════════════════════════════════════════════════
   부동산 급매 알리미 — Frontend App
   ═══════════════════════════════════════════════════════════════════════════ */

const APP_NAME = '부동산 급매 알리미';
const ALERT_POLL_MS = 60000;

// ── State ──────────────────────────────────────────────────────────────────
const state = {
  theme: localStorage.getItem('theme') || 'light',
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
  activeStatFilter: '',  // tracks which stat card is active
  page: 1,
  perPage: 20,
  totalPages: 1,
  regionStats: [],
  mapMarkers: {},   // district → 마커 래퍼 (카카오 CustomOverlay)
  mapRegionMarkers: {},   // region(도·광역시) → 마커 래퍼
  mapListingMarkers: [],  // 매물 티어의 kakao.maps.Marker 목록
  mapClusterer: null,
  mapListingsToken: 0,    // 매물 응답 순서 역전 방지 토큰
  mapTier: 'region',      // 'region' | 'district' | 'listing'
  map: null,
  sidebarOpen: localStorage.getItem('sidebarOpen') !== 'false',
  clientId: '',
  alertRules: [],
  notificationPermission: typeof Notification === 'undefined' ? 'unsupported' : Notification.permission,
  swRegistration: null,
  pushConfigured: false,
  pushPublicKey: '',
  pushSubscribed: false,
  pushConfigLoaded: false,
  alertPollTimer: null,
  mobileSidebarOpen: false,
  mapExpanded: localStorage.getItem('mapExpanded') === null
    ? !window.matchMedia('(max-width: 900px)').matches
    : localStorage.getItem('mapExpanded') !== 'false',
  dashboard: {
    total: 0,
    priceDownCount: 0,
    crawlSummary: '',
    trends: [],
    dailySeries: [],
  },
};

// ── API helpers ─────────────────────────────────────────────────────────────
async function api(path, opts = {}) {
  const res = await fetch(path, opts);
  if (!res.ok) throw new Error(`API error ${res.status}`);
  return res.json();
}

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

function getClientId() {
  const key = 'real-estate-alert-client-id';
  let clientId = localStorage.getItem(key);
  if (!clientId) {
    clientId = window.crypto?.randomUUID?.() || `client-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    localStorage.setItem(key, clientId);
  }
  return clientId;
}

function isMobileViewport() {
  return window.matchMedia('(max-width: 900px)').matches;
}

function isLocalhost() {
  return ['localhost', '127.0.0.1'].includes(location.hostname);
}

function isPushSupported() {
  return 'serviceWorker' in navigator && 'PushManager' in window;
}

function canUsePushTransport() {
  return isPushSupported() && (window.isSecureContext || isLocalhost());
}

function urlBase64ToUint8Array(base64String) {
  const padding = '='.repeat((4 - (base64String.length % 4)) % 4);
  const base64 = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
  const rawData = window.atob(base64);
  return Uint8Array.from([...rawData].map(char => char.charCodeAt(0)));
}

function buildCurrentFilterLabel() {
  const parts = [];
  if (state.filters.search) parts.push(`검색 ${state.filters.search}`);
  if (state.filters.district) parts.push(`지역 ${state.filters.district}`);
  if (state.filters.property_type && state.filters.property_type !== '__OTHER__') parts.push(`유형 ${state.filters.property_type}`);
  if (state.filters.property_type === '__OTHER__') parts.push('유형 기타');
  if (state.filters.trade_type) parts.push(`거래 ${state.filters.trade_type}`);
  if (state.filters.price_down_only) parts.push('가격인하만');
  if (state.filters.tags.length) parts.push(`태그 ${state.filters.tags.join(',')}`);
  return parts.length ? parts.join(' · ') : '전체 급매';
}

function updateHeroAlertCount() {
  const el = document.getElementById('hero-alert-rule-count');
  if (!el) return;
  el.textContent = `${fmtNum(state.alertRules.length || 0)}개`;
}

function updateHeroFocusRegion() {
  const el = document.getElementById('hero-focus-region');
  if (!el) return;
  if (state.filters.district) {
    el.textContent = state.filters.district;
    updateHeroInsight();
    return;
  }
  if (state.filters.search) {
    el.textContent = state.filters.search;
    updateHeroInsight();
    return;
  }
  el.textContent = '전국';
  updateHeroInsight();
}

function updateHeroCrawlSummary(text) {
  const el = document.getElementById('hero-crawl-summary');
  if (el && text) {
    el.textContent = text;
    state.dashboard.crawlSummary = text;
    updateHeroInsight();
  }
}

function updateListingsSummary(total = null) {
  const el = document.getElementById('listings-summary');
  if (!el) return;
  const label = buildCurrentFilterLabel();
  if (total == null) {
    el.textContent = `${label} 조건의 급매를 불러오는 중입니다.`;
    return;
  }
  el.textContent = `${label} 조건 결과 ${fmtNum(total)}건`;
}

function updateHeroInsight() {
  const el = document.getElementById('hero-insight-text');
  const subEl = document.getElementById('hero-insight-subtext');
  if (!el || !subEl) return;

  const total = Number(state.dashboard.total || 0);
  const priceDown = Number(state.dashboard.priceDownCount || 0);
  const alertCount = Number(state.alertRules.length || 0);
  const trends = state.dashboard.trends || [];

  if (!total) {
    el.textContent = '전국 급매 흐름과 알림 현황을 계산 중입니다.';
    subEl.textContent = '서울 주요 지역과 가격인하 변화는 트렌드 집계 후 함께 보여드립니다.';
    return;
  }

  if (!trends.length) {
    const alertLabel = alertCount
      ? `알림 ${fmtNum(alertCount)}개가 새 매물을 감시 중입니다.`
      : '아직 등록된 알림은 없습니다.';
    el.textContent = `전국 급매 ${fmtNum(total)}건, 가격인하 ${fmtNum(priceDown)}건입니다.`;
    subEl.textContent = alertLabel;
    return;
  }

  const increasing = trends.filter(item => Number(item.diff) > 0);
  const decreasing = trends.filter(item => Number(item.diff) < 0);
  const topIncrease = [...increasing].sort((a, b) => Number(b.diff) - Number(a.diff))[0];
  const topDecrease = [...decreasing].sort((a, b) => Number(a.diff) - Number(b.diff))[0];
  const seoulMovers = trends
    .filter(item => item.region === '서울특별시' && Number(item.diff) !== 0)
    .sort((a, b) => Math.abs(Number(b.diff)) - Math.abs(Number(a.diff)))
    .slice(0, 3);
  const alertLabel = alertCount
    ? `알림 ${fmtNum(alertCount)}개가 새 매물을 감시 중입니다.`
    : '아직 등록된 알림은 없습니다.';
  const firstLine = [
    `1일 기준 ${fmtNum(increasing.length)}개 지역 증가, ${fmtNum(decreasing.length)}개 지역 감소입니다.`,
    topIncrease ? `증가폭 최대는 ${formatTrendInsightName(topIncrease)}(+${fmtNum(topIncrease.diff)})입니다.` : '',
    topDecrease ? `감소폭 최대는 ${formatTrendInsightName(topDecrease)}(${fmtNum(topDecrease.diff)})입니다.` : '',
  ].filter(Boolean).join(' ');
  const secondLine = [
    seoulMovers.length ? `서울에서는 ${formatSeoulTrendSummary(seoulMovers)} 변화가 컸습니다.` : '',
    `현재 가격인하 매물은 ${fmtNum(priceDown)}건입니다.`,
    alertLabel,
  ].filter(Boolean).join(' ');

  el.textContent = firstLine;
  subEl.textContent = secondLine;
}

function formatTrendInsightName(item) {
  return item.display_name || `${item.region} ${item.district}`;
}

function formatSeoulTrendSummary(items) {
  return items
    .map(item => `${item.district}(${Number(item.diff) > 0 ? '+' : ''}${fmtNum(item.diff)})`)
    .join(', ');
}

function hasNumericValue(item, field = 'total_count') {
  return item != null && item[field] != null && Number.isFinite(Number(item[field]));
}

function renderHeroDailySeries(series) {
  const chartEl = document.getElementById('hero-daily-chart');
  const labelsEl = document.getElementById('hero-daily-labels');
  const emptyEl = document.getElementById('hero-daily-empty');
  const currentEl = document.getElementById('hero-daily-current');
  const changeEl = document.getElementById('hero-daily-change');
  if (!chartEl || !labelsEl || !emptyEl || !currentEl || !changeEl) return;

  const items = Array.isArray(series) ? series.slice(-7) : [];
  const validItems = items.filter(item => hasNumericValue(item));

  if (!validItems.length) {
    chartEl.innerHTML = '';
    labelsEl.innerHTML = '';
    emptyEl.classList.remove('hidden');
    currentEl.textContent = '—';
    changeEl.textContent = '최근 7일 성공 크롤링 데이터가 없습니다.';
    return;
  }

  emptyEl.classList.add('hidden');

  const values = validItems.map(item => Number(item.total_count));
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const range = Math.max(maxValue - minValue, 1);
  const latestItem = validItems[validItems.length - 1];
  const previousItem = validItems.length > 1 ? validItems[validItems.length - 2] : null;
  const latestValue = Number(latestItem.total_count || 0);
  const delta = previousItem ? latestValue - Number(previousItem.total_count || 0) : null;
  const usesPreviousDay = previousItem && latestItem.date
    && previousItem.date
    && ((new Date(latestItem.date) - new Date(previousItem.date)) / 86400000 === 1);

  currentEl.textContent = `${fmtNum(latestValue)}건`;
  if (delta == null) {
    changeEl.textContent = '최근 7일 중 첫 성공 크롤링입니다.';
  } else {
    const prefix = delta > 0 ? '+' : '';
    changeEl.textContent = `${usesPreviousDay ? '전일' : '직전 성공일'} 대비 ${prefix}${fmtNum(delta)}건`;
  }

  let previousValidValue = null;
  chartEl.innerHTML = items.map((item, index) => {
    const hasValue = hasNumericValue(item);
    const value = hasValue ? Number(item.total_count) : NaN;
    const showTooltip = index < items.length - 1;
    let directionClass = 'flat';
    if (hasValue && previousValidValue != null) {
      if (value > previousValidValue) directionClass = 'up';
      else if (value < previousValidValue) directionClass = 'down';
    }
    const height = hasValue
      ? Math.max(18, Math.round(((value - minValue) / range) * 60) + 18)
      : 12;
    const tooltip = hasValue
      ? `${item.label} ${fmtNum(value)}건`
      : `${item.label} 성공 크롤링 없음`;
    if (hasValue) previousValidValue = value;
    return `
      <div class="hero-mini-bar-wrap ${index === items.length - 1 ? 'latest' : ''} ${showTooltip ? 'has-tooltip' : ''}"${showTooltip ? ` data-tooltip="${escHtml(tooltip)}"` : ''}>
        <div class="hero-mini-bar ${hasValue ? directionClass : 'missing'}" style="height:${height}px"></div>
      </div>
    `;
  }).join('');

  labelsEl.innerHTML = items.map(item => {
    const missing = !hasNumericValue(item);
    return `<span class="hero-mini-chart-day ${missing ? 'missing' : ''}">${escHtml(item.label || '')}</span>`;
  }).join('');
}

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
    console.warn('Building trend load error:', err);
    bodyEl.innerHTML = '<div class="building-trend-empty">추이를 불러오지 못했습니다.</div>';
  }
}

function renderBuildingTrendBody(items) {
  const validItems = items.filter(item => hasNumericValue(item));

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
    const hasValue = hasNumericValue(item);
    const value = hasValue ? Number(item.total_count) : NaN;
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

function applyMapVisibility() {
  const wrap = document.getElementById('map-wrap');
  const btn = document.getElementById('btn-map-toggle');
  const mobileBtn = document.getElementById('btn-mobile-map-fab');
  const legend = document.getElementById('map-legend');
  if (!wrap || !btn) return;
  wrap.classList.toggle('collapsed', !state.mapExpanded);
  btn.textContent = state.mapExpanded ? '지도 접기' : '지도 펼치기';
  if (mobileBtn) mobileBtn.textContent = state.mapExpanded ? '지도 이동' : '지도 보기';
  if (legend) legend.classList.toggle('hidden', !state.mapExpanded);
  localStorage.setItem('mapExpanded', state.mapExpanded);
  if (state.map && state.mapExpanded) {
    setTimeout(() => state.map.relayout(), 260);
  }
}

function toggleMap() {
  state.mapExpanded = !state.mapExpanded;
  applyMapVisibility();
}

function setMobileSidebar(open) {
  const sidebar = document.getElementById('sidebar');
  const dim = document.getElementById('mobile-dim');
  if (!sidebar || !dim) return;
  state.mobileSidebarOpen = open;
  sidebar.classList.toggle('mobile-open', open);
  dim.classList.toggle('hidden', !open);
  document.body.classList.toggle('sidebar-overlay-open', open);
}

// ── Map (Kakao Maps JS SDK) ─────────────────────────────────────────────────
function initMap() {
  const mapEl = document.getElementById('map');
  if (!mapEl) return;

  if (typeof kakao === 'undefined' || !kakao.maps) {
    mapEl.innerHTML = `
      <div class="map-unavailable">
        <strong>카카오맵을 불러올 수 없습니다</strong>
        <p>developers.kakao.com에서 발급한 <b>JavaScript 키</b>를 <code>.env.local</code>의
        <code>KAKAO_MAP_APP_KEY</code>에 넣고, 앱 플랫폼에 현재 도메인을 등록한 뒤 서버를 재시작하세요.</p>
      </div>`;
    return;
  }

  // autoload=false로 로드하므로 지도 모듈 준비 후 생성한다
  kakao.maps.load(() => {
    state.map = new kakao.maps.Map(mapEl, {
      center: new kakao.maps.LatLng(36.5, 127.8),
      level: 13, // 전국 뷰
    });
    // 줌·이동이 끝날 때마다 티어 갱신 (매물 티어에서는 화면 영역 재조회)
    kakao.maps.event.addListener(state.map, 'idle', () => applyMapTier());
    // SDK 준비 전에 지역 통계가 먼저 도착한 경우 여기서 마커를 그린다
    if (state.regionStats.length) renderMapMarkers(state.regionStats);
  });
}

// ── 줌 티어: region(도) → district(시·군·구) → listing(개별 매물) ─────────────
const MAP_TIER_LISTING_MAX_LEVEL = 7;   // 이 레벨 이하로 확대하면 개별 매물 표시
const MAP_TIER_DISTRICT_MAX_LEVEL = 10; // 이 레벨 이하는 시·군·구, 초과는 도 단위

function getMapTier(level) {
  if (level <= MAP_TIER_LISTING_MAX_LEVEL) return 'listing';
  if (level <= MAP_TIER_DISTRICT_MAX_LEVEL) return 'district';
  return 'region';
}

function applyMapTier(force = false) {
  if (!state.map) return;
  const tier = typeof state.map.getLevel === 'function'
    ? getMapTier(state.map.getLevel())
    : state.mapTier;

  if (!force && tier === state.mapTier) {
    // 티어는 그대로여도 매물 티어에서는 화면 이동 시 재조회가 필요하다
    if (tier === 'listing') refreshMapListingsDebounced();
    return;
  }

  state.mapTier = tier;
  hideMarkerBubble();
  Object.values(state.mapRegionMarkers).forEach(m => m.setVisible(tier === 'region'));
  Object.values(state.mapMarkers).forEach(m => m.setVisible(tier === 'district'));
  if (tier === 'listing') {
    refreshMapListingsDebounced();
  } else {
    clearListingMarkers();
  }
}

// 마커 클릭 시 표시되는 정보 말풍선 (한 번에 하나만)
let mapInfoOverlay = null;
function hideMarkerBubble() {
  if (mapInfoOverlay) {
    mapInfoOverlay.setMap(null);
    mapInfoOverlay = null;
  }
}
function showMarkerBubble(coords, displayName, total, color) {
  hideMarkerBubble();
  const el = document.createElement('div');
  el.className = 'map-info-bubble';
  el.innerHTML = `<strong>${escHtml(displayName)}</strong><br/>급매: <b style="color:${color}">${fmtNum(total)}개</b>`;
  mapInfoOverlay = new kakao.maps.CustomOverlay({
    position: new kakao.maps.LatLng(coords.lat, coords.lng),
    content: el,
    xAnchor: 0.5,
    yAnchor: 1.35,
    zIndex: 30,
  });
  mapInfoOverlay.setMap(state.map);
}

// 시·군·구 라벨 버블 — 지역명과 건수를 항상 표시해 가독성을 확보한다
function createDistrictMarker(district, data, coords, maxTotal) {
  const ratio = data.total / maxTotal;
  const color = urgencyColor(data.total);
  const displayName = data.display_name || `${data.region} ${data.district}`;

  const el = document.createElement('div');
  el.className = 'district-marker';
  el.style.borderColor = color;
  el.innerHTML = `<span class="district-marker-name">${escHtml(district)}</span><strong style="color:${color}">${fmtNum(data.total)}</strong>`;
  // 급매가 많은 지역일수록 버블을 조금 더 키운다
  el.style.fontSize = `${11 + Math.round(ratio * 3)}px`;
  el.title = `${displayName} · 급매 ${fmtNum(data.total)}개`;
  el.addEventListener('click', () => {
    selectDistrict(district);
    if (state.filters.district === district) {
      showMarkerBubble(coords, displayName, data.total, color);
    } else {
      hideMarkerBubble();
    }
  });

  const overlay = new kakao.maps.CustomOverlay({
    position: new kakao.maps.LatLng(coords.lat, coords.lng),
    content: el,
    xAnchor: 0.5,
    yAnchor: 0.5,
  });
  overlay.setMap(state.mapTier === 'district' ? state.map : null);

  return {
    remove: () => overlay.setMap(null),
    setVisible: v => overlay.setMap(v ? state.map : null),
    setEmphasis(selected, anySelected) {
      el.classList.toggle('selected', selected);
      el.classList.toggle('dimmed', anySelected && !selected);
    },
  };
}

// ── Region(도·광역시) 티어 마커 ──────────────────────────────────────────────
function shortRegionName(region) {
  return String(region || '')
    .replace('특별자치시', '').replace('특별자치도', '')
    .replace('특별시', '').replace('광역시', '');
}

function buildRegionMarkers(regionStats, coordMap) {
  const byRegion = {};
  regionStats.forEach(r => {
    const coords = coordMap[r.district];
    if (!coords) return;
    if (!byRegion[r.region]) byRegion[r.region] = { total: 0, latSum: 0, lngSum: 0, count: 0 };
    const agg = byRegion[r.region];
    agg.total += r.total;
    agg.latSum += coords.lat;
    agg.lngSum += coords.lng;
    agg.count += 1;
  });

  const maxTotal = Math.max(...Object.values(byRegion).map(r => r.total), 1);
  Object.entries(byRegion).forEach(([region, agg]) => {
    state.mapRegionMarkers[region] = createRegionMarker(region, agg, maxTotal);
  });
}

function createRegionMarker(region, agg, maxTotal) {
  const ratio = agg.total / maxTotal;
  const color = ratio >= 0.6 ? '#f85149' : ratio >= 0.25 ? '#d29922' : '#3fb950';
  const center = { lat: agg.latSum / agg.count, lng: agg.lngSum / agg.count };

  const el = document.createElement('div');
  el.className = 'region-marker';
  el.style.borderColor = color;
  el.innerHTML = `<strong>${escHtml(shortRegionName(region))}</strong><span style="color:${color}">${fmtNum(agg.total)}</span>`;
  el.title = `${region} · 급매 ${fmtNum(agg.total)}개 — 클릭해 확대`;
  el.addEventListener('click', () => {
    state.map.setLevel(MAP_TIER_DISTRICT_MAX_LEVEL - 1);
    state.map.panTo(new kakao.maps.LatLng(center.lat, center.lng));
  });

  const overlay = new kakao.maps.CustomOverlay({
    position: new kakao.maps.LatLng(center.lat, center.lng),
    content: el,
    xAnchor: 0.5,
    yAnchor: 0.5,
  });
  overlay.setMap(state.mapTier === 'region' ? state.map : null);

  return {
    remove: () => overlay.setMap(null),
    setVisible: v => overlay.setMap(v ? state.map : null),
  };
}

// ── Listing(개별 매물) 티어 ──────────────────────────────────────────────────
function ensureClusterer() {
  if (state.mapClusterer) return state.mapClusterer;
  if (!kakao.maps.MarkerClusterer) return null; // clusterer 라이브러리 미로드
  state.mapClusterer = new kakao.maps.MarkerClusterer({
    map: state.map,
    averageCenter: true,
    minLevel: 1, // 같은 건물·단지에 몰린 매물을 항상 묶는다
    disableClickZoom: true,
    gridSize: 60,
    styles: [{
      width: '46px',
      height: '46px',
      borderRadius: '23px',
      background: 'rgba(37, 99, 235, 0.92)',
      border: '2px solid #fff',
      boxShadow: '0 2px 8px rgba(0, 0, 0, 0.35)',
      color: '#fff',
      textAlign: 'center',
      lineHeight: '43px',
      fontSize: '14px',
      fontWeight: '700',
    }],
  });
  kakao.maps.event.addListener(state.mapClusterer, 'clusterclick', cluster => {
    const level = state.map.getLevel();
    if (level > 3) {
      state.map.setLevel(level - 2, { anchor: cluster.getCenter() });
    } else {
      // 최대 확대에서도 묶여 있으면(같은 단지) 목록 말풍선으로 보여준다
      showClusterBubble(cluster.getCenter(), cluster.getMarkers());
    }
  });
  return state.mapClusterer;
}

function clearListingMarkers() {
  if (state.mapClusterer) state.mapClusterer.clear();
  state.mapListingMarkers.forEach(m => m.setMap(null));
  state.mapListingMarkers = [];
}

async function refreshMapListings() {
  if (!state.map || state.mapTier !== 'listing') return;

  const bounds = state.map.getBounds();
  const sw = bounds.getSouthWest();
  const ne = bounds.getNorthEast();
  const q = new URLSearchParams({
    min_lat: sw.getLat(),
    max_lat: ne.getLat(),
    min_lng: sw.getLng(),
    max_lng: ne.getLng(),
  });
  ['trade_type', 'property_type', 'search', 'district'].forEach(key => {
    if (state.filters[key]) q.set(key, state.filters[key]);
  });
  if (state.filters.price_down_only) q.set('price_down_only', 'true');
  if (state.filters.tags.length) q.set('tags', state.filters.tags.join(','));

  const token = ++state.mapListingsToken;
  try {
    const data = await api(`/api/map-listings?${q.toString()}`);
    // 늦게 도착한 이전 응답이나 티어 이탈 후 응답은 무시
    if (token !== state.mapListingsToken || state.mapTier !== 'listing') return;
    renderListingMarkers(data.listings || []);
  } catch (e) {
    console.warn('Map listings load error:', e);
  }
}
const refreshMapListingsDebounced = debounce(refreshMapListings, 250);

function renderListingMarkers(listings) {
  const clusterer = ensureClusterer();
  clearListingMarkers();

  const markers = listings.map(l => {
    const marker = new kakao.maps.Marker({
      position: new kakao.maps.LatLng(l.latitude, l.longitude),
      title: `${l.building_name || '매물'} ${l.price || ''}`.trim(),
    });
    marker.__listing = l;
    kakao.maps.event.addListener(marker, 'click', () => showListingBubble(l));
    return marker;
  });

  state.mapListingMarkers = markers;
  if (clusterer) clusterer.addMarkers(markers);
  else markers.forEach(m => m.setMap(state.map));
}

function showListingBubble(l) {
  hideMarkerBubble();
  const tags = parseTags(l.tags);
  const hasPriceDown = tags.includes('가격인하');

  const el = document.createElement('div');
  el.className = 'map-info-bubble listing-bubble';
  el.innerHTML = `
    <button class="bubble-close" title="닫기">✕</button>
    <div class="listing-bubble-row">
      <span class="badge badge-urgent">${hasPriceDown ? '가격인하' : '급매'}</span>
      <span class="listing-bubble-type">[${escHtml(l.property_type || '')}/${escHtml(l.trade_type || '')}]</span>
    </div>
    <strong>${escHtml(l.building_name || '이름 없는 매물')}</strong>
    <div class="listing-bubble-price">${escHtml(l.price || '가격 확인 필요')}</div>
    ${l.naver_url ? `<a href="${escHtml(l.naver_url)}" target="_blank" rel="noopener noreferrer">네이버 부동산에서 보기 ↗</a>` : ''}
  `;
  el.querySelector('.bubble-close')?.addEventListener('click', hideMarkerBubble);

  mapInfoOverlay = new kakao.maps.CustomOverlay({
    position: new kakao.maps.LatLng(l.latitude, l.longitude),
    content: el,
    xAnchor: 0.5,
    yAnchor: 1.2,
    zIndex: 40,
  });
  mapInfoOverlay.setMap(state.map);
}

function showClusterBubble(center, markers) {
  hideMarkerBubble();
  const items = markers.map(m => m.__listing).filter(Boolean).slice(0, 6);

  const el = document.createElement('div');
  el.className = 'map-info-bubble cluster-bubble';
  el.innerHTML = `
    <button class="bubble-close" title="닫기">✕</button>
    <strong>이 위치 매물 ${fmtNum(markers.length)}건</strong>
    <ul>${items.map(l => `
      <li>${l.naver_url
        ? `<a href="${escHtml(l.naver_url)}" target="_blank" rel="noopener noreferrer">${escHtml(l.building_name || '매물')} · ${escHtml(l.price || '가격 미확인')}</a>`
        : `${escHtml(l.building_name || '매물')} · ${escHtml(l.price || '가격 미확인')}`}</li>`).join('')}
    </ul>
    ${markers.length > items.length ? `<div class="bubble-more">외 ${fmtNum(markers.length - items.length)}건</div>` : ''}
  `;
  el.querySelector('.bubble-close')?.addEventListener('click', hideMarkerBubble);

  mapInfoOverlay = new kakao.maps.CustomOverlay({
    position: center,
    content: el,
    xAnchor: 0.5,
    yAnchor: 1.1,
    zIndex: 40,
  });
  mapInfoOverlay.setMap(state.map);
}

function urgencyColor(total) {
  // 급매 수 기반 색상 (모든 매물이 급매이므로)
  if (total == null || isNaN(total)) return '#58a6ff';
  if (total < 20) return '#3fb950';
  if (total < 50) return '#d29922';
  return '#f85149';
}

// 좌표 데이터는 정적이므로 최초 1회만 요청하고 재사용한다.
let regionCoordMapPromise = null;
function getRegionCoordMap() {
  if (!regionCoordMapPromise) {
    regionCoordMapPromise = api('/api/regions').then(regions => {
      const coordMap = {};
      regions.forEach(r => {
        r.districts.forEach(d => { coordMap[d.name] = { lat: d.lat, lng: d.lng }; });
      });
      return coordMap;
    }).catch(e => {
      regionCoordMapPromise = null;
      throw e;
    });
  }
  return regionCoordMapPromise;
}

function renderMapMarkers(regionStats) {
  if (!state.map) return;

  Object.values(state.mapMarkers).forEach(m => m.remove());
  state.mapMarkers = {};
  Object.values(state.mapRegionMarkers).forEach(m => m.remove());
  state.mapRegionMarkers = {};

  // Group by district (sum totals if same district in different regions)
  const byDistrict = {};
  regionStats.forEach(r => {
    const key = r.district;
    if (!byDistrict[key]) byDistrict[key] = { ...r };
    else {
      byDistrict[key].total += r.total;
    }
  });

  hideMarkerBubble();

  getRegionCoordMap().then(coordMap => {
    const maxTotal = Math.max(...Object.values(byDistrict).map(d => d.total), 1);

    Object.entries(byDistrict).forEach(([district, data]) => {
      const coords = coordMap[district];
      if (!coords) return;
      state.mapMarkers[district] = createDistrictMarker(district, data, coords, maxTotal);
    });
    buildRegionMarkers(regionStats, coordMap);
    applyMapTier(true); // 마커 재생성 후 현재 티어에 맞는 세트만 표시
  }).catch(() => {});
}

function selectDistrict(district) {
  state.filters.district = district === state.filters.district ? '' : district;
  state.page = 1;

  const badge = document.getElementById('region-badge');
  const badgeName = document.getElementById('region-badge-name');

  if (state.filters.district) {
    badgeName.textContent = district;
    badge.classList.remove('hidden');
  } else {
    badge.classList.add('hidden');
  }

  Object.entries(state.mapMarkers).forEach(([d, m]) => {
    m.setEmphasis(d === state.filters.district, Boolean(state.filters.district));
  });
  if (!state.filters.district) hideMarkerBubble();

  document.querySelectorAll('.trend-item, .region-item').forEach(el => {
    el.classList.toggle('active', el.dataset.district === district);
  });

  refreshAlertDraftSummary();
  updateHeroFocusRegion();
  updateListingsSummary();
  if (state.mobileSidebarOpen) setMobileSidebar(false);
  loadListings();
}

// ── Listings ─────────────────────────────────────────────────────────────────
async function loadListings() {
  const grid = document.getElementById('listings-grid');
  grid.innerHTML = '<div class="loading-state"><div class="spinner"></div><p>불러오는 중...</p></div>';
  refreshAlertDraftSummary();
  updateListingsSummary();

  try {
    const data = await api(`/api/listings?${buildQuery()}`);
    renderListings(data);
    updateStats(data);
  } catch (e) {
    grid.innerHTML = '<div class="empty-state">데이터를 불러올 수 없습니다.</div>';
    updateListingsSummary(0);
  }

  // 필터가 바뀌면 매물 티어의 지도 마커도 같은 조건으로 갱신
  if (state.mapTier === 'listing') refreshMapListingsDebounced();
}

function tradeBadgeClass(trade) {
  if (trade === '매매') return 'badge-trade-buy';
  if (trade === '전세') return 'badge-trade-jeon';
  return 'badge-trade-month';
}

function formatDate(str) {
  if (!str || str.length !== 8) return str || '';
  return `${str.slice(0, 4)}.${str.slice(4, 6)}.${str.slice(6, 8)}`;
}

function parseTags(tagsRaw) {
  if (Array.isArray(tagsRaw)) return tagsRaw;
  try { return JSON.parse(tagsRaw) || []; } catch { return []; }
}

function renderListings(data) {
  const grid = document.getElementById('listings-grid');
  const { listings, total, total_pages, page } = data;

  state.totalPages = total_pages || 1;
  state.page = page;

  document.getElementById('btn-prev').disabled = page <= 1;
  document.getElementById('btn-next').disabled = page >= state.totalPages;
  document.getElementById('page-info').textContent = `페이지 ${page} / ${state.totalPages}`;

  if (!listings || listings.length === 0) {
    grid.innerHTML = '<div class="empty-state">조건에 맞는 급매가 없습니다.</div>';
    return;
  }

  grid.innerHTML = listings.map(l => {
    const tags = parseTags(l.tags);
    const hasPriceDown = tags.includes('가격인하');
    const compactRegion = `${l.region ? l.region.replace('특별시','').replace('광역시','').replace('특별자치시','') : ''} ${l.district}`.trim();
    const metaBits = [
      l.area ? `면적 ${l.area}` : '',
      l.floor ? `층 ${l.floor}` : '',
      l.trade_type || '',
    ].filter(Boolean);
    const visibleTags = tags.filter(tag => tag !== '급매').slice(0, 4);
    return `
    <div class="listing-card urgent-card-item"
         data-id="${escHtml(l.id)}"
         data-article-no="${escHtml(l.article_no)}"
         data-naver-url="${escHtml(l.naver_url || '')}"
         title="네이버 부동산에서 보기">
      <div class="card-topline">
        <div class="card-badges">
          <span class="badge badge-urgent">${hasPriceDown ? '가격인하' : '급매'}</span>
          <span class="badge ${tradeBadgeClass(l.trade_type)}">${escHtml(l.trade_type)}</span>
          <span class="badge badge-type">${escHtml(l.property_type)}</span>
        </div>
        <div class="card-date-chip">확인 ${formatDate(l.confirmed_date) || '—'}</div>
      </div>
      <div class="card-price-row">
        <div class="card-price-block">
          <div class="card-price">${escHtml(l.price || '—')}</div>
          <div class="card-price-caption">${escHtml(l.trade_type)} ${hasPriceDown ? '· 가격인하 감지' : '· 급매 포착'}</div>
        </div>
        <span class="naver-link-icon" title="네이버 부동산">네이버 보기</span>
      </div>
      <div class="card-name-row">
        <div class="card-name" title="${escHtml(l.building_name)}">${escHtml(l.building_name)}</div>
        ${l.district && l.building_name ? `<button type="button" class="card-trend-btn" data-district="${escHtml(l.district)}" data-building-name="${escHtml(l.building_name)}" title="일별 매물수 추이">📈</button>` : ''}
      </div>
      <div class="card-location-line">${escHtml(compactRegion)}</div>
      <div class="card-meta">
        ${metaBits.map(bit => `<span>${escHtml(bit)}</span>`).join('')}
      </div>
      ${l.description ? `<div class="card-desc">${escHtml(l.description)}</div>` : ''}
      <div class="card-tags">
        <span class="tag urgent-tag">${hasPriceDown ? '가격인하' : '급매'}</span>
        ${visibleTags.map(t => `<span class="tag ${t === '가격인하' ? 'price-down-tag' : ''}">${escHtml(t)}</span>`).join('')}
      </div>
    </div>`;
  }).join('');
}

function updateStats(data) {
  document.getElementById('stat-total').textContent = fmtNum(data.total);
  const tc = data.type_counts || {};
  document.getElementById('stat-apt').textContent = fmtNum(tc['아파트'] || 0);
  document.getElementById('stat-opst').textContent = fmtNum(tc['오피스텔'] || 0);
  document.getElementById('stat-villa').textContent = fmtNum(tc['빌라/연립'] || 0);
  const other = (tc['단독/다가구'] || 0) + (tc['상가/업무'] || 0) + (tc['토지'] || 0);
  document.getElementById('stat-other').textContent = fmtNum(other);

  // 가격인하 수 (서버에서 안 내려오면 0)
  const pdEl = document.getElementById('stat-price-down');
  if (pdEl) pdEl.textContent = fmtNum(data.price_down_count || 0);
  const heroTotal = document.getElementById('hero-total-count');
  if (heroTotal) heroTotal.textContent = fmtNum(data.total || 0);
  const heroPriceDown = document.getElementById('hero-price-down-count');
  if (heroPriceDown) heroPriceDown.textContent = fmtNum(data.price_down_count || 0);
  state.dashboard.total = data.total || 0;
  state.dashboard.priceDownCount = data.price_down_count || 0;
  updateHeroFocusRegion();
  updateListingsSummary(data.total || 0);
  updateHeroInsight();
}

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

// ── Sidebar ──────────────────────────────────────────────────────────────────
async function loadSidebar() {
  const dailySeriesPromise = api('/api/crawl-daily-series?days=7')
    .then(series => {
      state.dashboard.dailySeries = series;
      renderHeroDailySeries(series);
    })
    .catch(e => {
      console.warn('Daily series load error:', e);
    });

  const regionStatsPromise = api('/api/region-stats')
    .then(regionStats => {
      state.regionStats = regionStats;
      renderMapMarkers(regionStats);
      renderRegionStats(regionStats);
    })
    .catch(e => {
      console.warn('Region stats load error:', e);
    });

  const trendsPromise = api('/api/trends')
    .then(trends => {
      state.dashboard.trends = trends;
      renderTrends(trends);
      updateHeroInsight();
    })
    .catch(e => {
      console.warn('Trends load error:', e);
    });

  await Promise.allSettled([dailySeriesPromise, regionStatsPromise, trendsPromise]);
}

function renderTrends(trends) {
  const increasing = trends.filter(t => t.diff > 0).slice(0, 6);
  const decreasing = trends.filter(t => t.diff < 0).sort((a,b) => a.diff - b.diff).slice(0, 6);

  // 가격인하 매물이 많은 지역 (price_down_count 기준)
  const priceDown = [...trends]
    .filter(t => (t.price_down_count || 0) > 0)
    .sort((a,b) => (b.price_down_count||0) - (a.price_down_count||0))
    .slice(0, 6);

  renderTrendList('list-increasing', increasing, 'up');
  renderTrendList('list-decreasing', decreasing, 'down');
  renderTrendList('list-price-down', priceDown, 'price-down');
}

function renderTrendList(id, items, type) {
  const ul = document.getElementById(id);
  if (!ul) return;
  if (!items.length) {
    ul.innerHTML = '<li class="trend-empty">데이터 없음</li>';
    return;
  }
  ul.innerHTML = items.map((item, index) => {
    let badge, badgeClass, sub;
    if (type === 'up') {
      badge = `+${item.diff}`;
      badgeClass = 'up';
      sub = `전일 ${fmtNum(item.prev_cnt || 0)}개 → 오늘 ${fmtNum(item.current_cnt || 0)}개`;
    } else if (type === 'down') {
      badge = `${item.diff}`;
      badgeClass = 'down';
      sub = `전일 ${fmtNum(item.prev_cnt || 0)}개 → 오늘 ${fmtNum(item.current_cnt || 0)}개`;
    } else if (type === 'price-down') {
      badge = `${item.price_down_count || 0}개`;
      badgeClass = 'urgent';
      sub = `가격인하 ${fmtNum(item.price_down_count || 0)}개 · 급매 ${fmtNum(item.current_cnt || 0)}개`;
    } else {
      badge = fmtNum(item.current_cnt || 0);
      badgeClass = 'urgent';
      sub = `급매 ${fmtNum(item.current_cnt || 0)}개`;
    }
    const name = item.display_name || `${item.region} ${item.district}`;
    return `
    <li class="trend-item" data-district="${escHtml(item.district)}">
      <div class="trend-rank">${String(index + 1).padStart(2, '0')}</div>
      <div class="trend-copy">
        <div class="trend-name">${escHtml(name)}</div>
        <div class="trend-sub">${sub}</div>
      </div>
      <div class="trend-side">
        <span class="trend-badge ${badgeClass}">${badge}</span>
        <span class="trend-current">${fmtNum(item.current_cnt || 0)}건</span>
      </div>
    </li>`;
  }).join('');
}

function renderRegionStats(stats) {
  const ul = document.getElementById('list-region-stats');
  const maxTotal = Math.max(...stats.map(s => s.total), 1);
  ul.innerHTML = stats.slice(0, 20).map((s, index) => `
    <li class="region-item" data-district="${escHtml(s.district)}">
      <div class="region-rank">${String(index + 1).padStart(2, '0')}</div>
      <div class="region-bar-wrap">
        <div class="region-name">${escHtml(s.display_name || `${s.region} ${s.district}`)}</div>
        <div class="region-bar">
          <div class="region-bar-fill" style="width:${(s.total / maxTotal) * 100}%"></div>
        </div>
      </div>
      <span class="region-count">${s.total}</span>
    </li>`).join('');
}

function getNotificationStatusMessage() {
  if (!('Notification' in window)) return { text: '이 브라우저는 알림을 지원하지 않습니다.', cls: 'blocked' };
  if (state.notificationPermission === 'granted' && state.pushConfigured && state.pushSubscribed) {
    return { text: '모바일 푸시가 활성화되어 있습니다. 앱이 닫혀 있어도 새 급매를 보낼 수 있습니다.', cls: 'ready' };
  }
  if (state.notificationPermission === 'granted' && state.pushConfigured && !state.pushSubscribed) {
    return { text: '알림 권한은 허용됐지만 푸시 구독 연결이 아직 완료되지 않았습니다.', cls: 'blocked' };
  }
  if (state.notificationPermission === 'granted' && !state.pushConfigured) {
    return { text: '브라우저 알림은 활성화되어 있지만 서버 푸시는 아직 설정되지 않았습니다.', cls: 'ready' };
  }
  if (state.notificationPermission === 'granted') return { text: '브라우저 알림이 활성화되어 있습니다.', cls: 'ready' };
  if (state.notificationPermission === 'denied') return { text: '브라우저에서 알림이 차단되어 있습니다. 브라우저 설정에서 허용하세요.', cls: 'blocked' };
  return { text: '알림 권한이 필요합니다. 권한 요청 후 알림을 등록하세요.', cls: '' };
}

function updateHeroNotifBtn() {
  const btn = document.getElementById('btn-hero-notif');
  if (!btn) return;
  const perm = state.notificationPermission;
  if (perm === 'granted' && state.pushConfigured && state.pushSubscribed) {
    btn.textContent = '📲 모바일 푸시 연결됨';
    btn.disabled = true;
    btn.classList.add('notif-granted');
    btn.classList.remove('notif-denied');
  } else if (perm === 'granted') {
    btn.textContent = '🔔 알림 허용됨';
    btn.disabled = true;
    btn.classList.add('notif-granted');
    btn.classList.remove('notif-denied');
  } else if (perm === 'denied') {
    btn.textContent = '🔕 알림 차단됨';
    btn.disabled = false;
    btn.classList.add('notif-denied');
  } else {
    btn.textContent = '🔔 알림 허용';
    btn.disabled = false;
    btn.classList.remove('notif-granted', 'notif-denied');
  }
}

function updateNotificationStatus() {
  const el = document.getElementById('notification-status');
  if (!el) return;
  const { text, cls } = getNotificationStatusMessage();
  el.textContent = text;
  el.classList.remove('ready', 'blocked');
  if (cls) el.classList.add(cls);
}

function buildAlertDraft() {
  const keywordInput = document.getElementById('alert-keyword');
  const keyword = keywordInput?.value.trim() || state.filters.search;
  const propertyType = state.filters.property_type === '__OTHER__' ? '' : state.filters.property_type;
  return {
    client_id: state.clientId,
    keyword,
    district: state.filters.district,
    property_type: propertyType,
    trade_type: state.filters.trade_type,
  };
}

function refreshAlertDraftSummary() {
  const el = document.getElementById('alert-current-filters');
  const heroEl = document.getElementById('hero-filter-summary');
  if (!el) return;
  const draft = buildAlertDraft();
  const parts = [];
  if (draft.keyword) parts.push(`키워드: ${draft.keyword}`);
  if (draft.district) parts.push(`지역: ${draft.district}`);
  if (draft.property_type) parts.push(`유형: ${draft.property_type}`);
  if (draft.trade_type) parts.push(`거래: ${draft.trade_type}`);
  const summary = parts.length
    ? `저장될 조건: ${parts.join(' · ')}`
    : '검색어나 지역/유형/거래 필터를 먼저 선택하세요.';
  el.textContent = summary;
  if (heroEl) heroEl.textContent = `현재 조건: ${buildCurrentFilterLabel()}`;
  updateHeroFocusRegion();
}

function renderAlertRules() {
  const list = document.getElementById('alert-rules-list');
  if (!list) return;

  if (!state.alertRules.length) {
    list.innerHTML = '<li class="alert-empty">등록된 알림이 없습니다.</li>';
    updateHeroAlertCount();
    renderHeroAlertPreview();
    updateHeroInsight();
    return;
  }

  list.innerHTML = state.alertRules.map(rule => {
    const meta = [
      rule.keyword ? `키워드 ${rule.keyword}` : '',
      rule.district ? `지역 ${rule.district}` : '',
      rule.property_type ? `유형 ${rule.property_type}` : '',
      rule.trade_type ? `거래 ${rule.trade_type}` : '',
    ].filter(Boolean).join(' · ');

    return `
      <li class="alert-rule-item">
        <div>
          <div class="alert-rule-name">${escHtml(rule.name)}</div>
          <div class="alert-rule-meta">${escHtml(meta || '전체 조건')}</div>
        </div>
        <button class="alert-rule-remove" data-alert-id="${rule.id}">삭제</button>
      </li>
    `;
  }).join('');
  updateHeroAlertCount();
  renderHeroAlertPreview();
  updateHeroInsight();
}

function renderHeroAlertPreview() {
  const wrap = document.getElementById('hero-alert-preview');
  if (!wrap) return;

  if (!state.alertRules.length) {
    wrap.innerHTML = '<span class="hero-alert-chip muted">등록된 알림이 없습니다</span>';
    return;
  }

  wrap.innerHTML = state.alertRules.slice(0, 4).map(rule => {
    const meta = rule.district || rule.keyword || rule.property_type || rule.trade_type || '전체';
    return `<span class="hero-alert-chip">${escHtml(rule.name)} · ${escHtml(meta)}</span>`;
  }).join('');
}

async function loadAlertRules() {
  const data = await api(`/api/alert-rules?client_id=${encodeURIComponent(state.clientId)}`);
  state.alertRules = data.rules || [];
  renderAlertRules();
}

async function loadPushConfig(force = false) {
  if (state.pushConfigLoaded && !force) {
    return state.pushConfigured;
  }

  try {
    const data = await api('/api/push/public-key');
    state.pushConfigured = !!data.configured;
    state.pushPublicKey = data.public_key || '';
  } catch (e) {
    state.pushConfigured = false;
    state.pushPublicKey = '';
  }

  state.pushConfigLoaded = true;
  return state.pushConfigured;
}

async function syncPushSubscriptionWithServer(subscription) {
  await api('/api/push/subscribe', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      client_id: state.clientId,
      subscription: subscription.toJSON(),
    }),
  });
}

async function ensurePushSubscription(interactive = false) {
  state.pushSubscribed = false;

  if (!canUsePushTransport()) return false;
  if (!state.swRegistration) return false;

  await loadPushConfig();
  if (!state.pushConfigured || !state.pushPublicKey) return false;
  if (state.notificationPermission !== 'granted') return false;

  try {
    let subscription = await state.swRegistration.pushManager.getSubscription();
    if (!subscription && interactive) {
      subscription = await state.swRegistration.pushManager.subscribe({
        userVisibleOnly: true,
        applicationServerKey: urlBase64ToUint8Array(state.pushPublicKey),
      });
    }

    if (!subscription) return false;

    await syncPushSubscriptionWithServer(subscription);
    state.pushSubscribed = true;
    return true;
  } catch (e) {
    console.warn('Push subscription failed:', e);
    state.pushSubscribed = false;
    return false;
  }
}

async function ensureNotificationsReady(interactive = false) {
  if (!('Notification' in window)) {
    updateNotificationStatus();
    return false;
  }

  if ('serviceWorker' in navigator && !state.swRegistration) {
    try {
      state.swRegistration = await navigator.serviceWorker.register('/sw.js');
    } catch (e) {
      console.warn('Service worker registration failed:', e);
    }
  }

  if (interactive && Notification.permission !== 'granted') {
    state.notificationPermission = await Notification.requestPermission();
  } else {
    state.notificationPermission = Notification.permission;
  }

  if (state.notificationPermission === 'granted') {
    await ensurePushSubscription(interactive);
  } else {
    state.pushSubscribed = false;
  }

  updateNotificationStatus();
  updateHeroNotifBtn();
  return state.notificationPermission === 'granted';
}

async function saveAlertRule() {
  const draft = buildAlertDraft();
  if (!draft.keyword && !draft.district && !draft.property_type && !draft.trade_type) {
    showToast('검색어나 필터를 먼저 선택하세요.', 'error');
    return;
  }

  const ready = await ensureNotificationsReady(true);
  if (!ready) {
    showToast('브라우저 알림 권한이 필요합니다.', 'error');
    return;
  }

  const result = await api('/api/alert-rules', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(draft),
  });

  const keywordInput = document.getElementById('alert-keyword');
  if (keywordInput) keywordInput.value = '';
  await loadAlertRules();
  refreshAlertDraftSummary();
  showToast(`알림 등록: ${result.rule.name}`, 'success');
}

async function removeAlertRule(alertId) {
  await api(`/api/alert-rules/${alertId}?client_id=${encodeURIComponent(state.clientId)}`, {
    method: 'DELETE',
  });
  await loadAlertRules();
  showToast('알림이 삭제되었습니다.', 'success');
}

async function showAlertNotification(match) {
  const title = APP_NAME;
  const body = [
    (match.alert_names || []).join(', '),
    `[${match.property_type}/${match.trade_type}] ${match.building_name} ${match.price}`,
    `${match.region} ${match.district}`,
  ].filter(Boolean).join(' · ');

  const options = {
    body,
    data: { url: match.naver_url || '/' },
    tag: `listing-${match.article_no}`,
  };

  if (state.swRegistration?.showNotification) {
    await state.swRegistration.showNotification(title, options);
    return;
  }

  const notification = new Notification(title, options);
  notification.onclick = () => {
    if (match.naver_url) window.open(match.naver_url, '_blank', 'noopener,noreferrer');
  };
}

async function checkAlertMatches() {
  if (!state.alertRules.length || state.notificationPermission !== 'granted') return;

  try {
    const data = await api(`/api/alerts/check?client_id=${encodeURIComponent(state.clientId)}`);
    const matches = data.matches || [];
    if (!matches.length) return;

    for (const match of matches.slice(0, 5)) {
      await showAlertNotification(match);
    }

    if (matches.length > 5) {
      showToast(`새 알림 ${matches.length}건이 도착했습니다.`, 'success');
    }
  } catch (e) {
    console.warn('Alert check failed:', e);
  }
}

function startAlertPolling() {
  if (state.alertPollTimer) window.clearInterval(state.alertPollTimer);
  state.alertPollTimer = window.setInterval(checkAlertMatches, ALERT_POLL_MS);
}


// ── Crawl Status ─────────────────────────────────────────────────────────────
async function loadCrawlStatus() {
  try {
    const data = await api('/api/crawl-status');
    const last = data.last_crawl;
    const lastAttempt = data.last_attempt;
    const scheduleState = data.schedule_state || {};
    if (last) {
      const dt = new Date(last.crawled_at);
      const timeStr = dt.toLocaleString('ko-KR', {
        timeZone: 'Asia/Seoul',
        month: 'numeric',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
      });
      const label = scheduleState.stale || (lastAttempt && lastAttempt.session_id !== last.session_id)
        ? '마지막 정상 크롤링'
        : '마지막 크롤링';
      document.getElementById('info-last-crawl').textContent =
        `${label}: ${timeStr} (급매 ${last.total_count}개)`;
      const summary = `${timeStr} 기준 최신 급매 ${fmtNum(last.total_count || 0)}개`;
      updateHeroCrawlSummary(
        scheduleState.stale && scheduleState.message
          ? `${summary} · ${scheduleState.message}`
          : summary
      );

      if ((lastAttempt && lastAttempt.source === 'demo') || (lastAttempt && lastAttempt.status !== 'success')) {
        document.getElementById('demo-badge').classList.remove('hidden');
      }
    }
    if (data.next_crawl) {
      const dt = new Date(data.next_crawl);
      const timeStr = dt.toLocaleString('ko-KR', {
        timeZone: 'Asia/Seoul',
        month: 'numeric',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
      });
      const prefix = scheduleState.message
        ? `${scheduleState.message} · 다음 크롤링`
        : '다음 크롤링';
      document.getElementById('info-next-crawl').textContent = `${prefix}: ${timeStr}`;
    }
    updateHeroInsight();
  } catch (e) {
    console.warn('Status load error:', e);
  }
}

// ── Theme ─────────────────────────────────────────────────────────────────────
function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  const moon = document.querySelector('.icon-moon');
  const sun = document.querySelector('.icon-sun');
  if (theme === 'dark') {
    moon?.classList.remove('hidden');
    sun?.classList.add('hidden');
  } else {
    moon?.classList.add('hidden');
    sun?.classList.remove('hidden');
  }
  localStorage.setItem('theme', theme);
  state.theme = theme;
}

// ── Sidebar toggle ────────────────────────────────────────────────────────────
function toggleSidebar() {
  if (isMobileViewport()) {
    setMobileSidebar(!state.mobileSidebarOpen);
    return;
  }
  state.sidebarOpen = !state.sidebarOpen;
  const sidebar = document.getElementById('sidebar');
  const btn = document.getElementById('sidebar-toggle');
  const openBtn = document.getElementById('sidebar-open-btn');
  sidebar.classList.toggle('collapsed', !state.sidebarOpen);
  btn.textContent = state.sidebarOpen ? '◀' : '▶';
  openBtn.classList.toggle('visible', !state.sidebarOpen);
  localStorage.setItem('sidebarOpen', state.sidebarOpen);
  if (state.map) setTimeout(() => state.map.relayout(), 250);
}

// ── Mobile notification guide modal ──────────────────────────────────────────
function showMobileNotifGuide() {
  // Remove existing guide if any
  document.getElementById('notif-guide-overlay')?.remove();

  const isIOS = /iPhone|iPad|iPod/i.test(navigator.userAgent);
  const isAndroid = /Android/i.test(navigator.userAgent);
  const currentUrl = location.href.replace(location.pathname, '').replace(location.search, '');
  const isDenied = 'Notification' in window && Notification.permission === 'denied';

  let steps = '';
  if (isDenied) {
    if (isIOS) {
      steps = `
        <div class="guide-step"><span class="guide-num">1</span><span>Safari 주소창 왼쪽 <b>AA</b> 버튼 탭</span></div>
        <div class="guide-step"><span class="guide-num">2</span><span><b>웹사이트 설정</b> → <b>알림: 허용</b>으로 변경</span></div>
        <div class="guide-step"><span class="guide-num">3</span><span>페이지를 새로고침 후 다시 시도</span></div>`;
    } else {
      steps = `
        <div class="guide-step"><span class="guide-num">1</span><span>주소창 왼쪽 <b>자물쇠 🔒</b> 탭</span></div>
        <div class="guide-step"><span class="guide-num">2</span><span><b>권한</b> → <b>알림: 허용</b>으로 변경</span></div>
        <div class="guide-step"><span class="guide-num">3</span><span>페이지를 새로고침 후 다시 시도</span></div>`;
    }
  } else if (isIOS) {
    steps = `
      <p class="guide-note">📱 iPhone/iPad에서 모바일 푸시를 받으려면 <b>HTTPS</b>가 필요하고, 앱을 홈 화면에 추가해야 합니다.</p>
      <div class="guide-step"><span class="guide-num">1</span><span>PC에서 <code>brew install ngrok && ngrok http 5101</code> 실행 후 <b>https://…ngrok-free.app</b> 주소 복사</span></div>
      <div class="guide-step"><span class="guide-num">2</span><span>Safari에서 해당 HTTPS 주소로 접속</span></div>
      <div class="guide-step"><span class="guide-num">3</span><span>하단 <b>공유 버튼 □↑</b> → <b>홈 화면에 추가</b></span></div>
      <div class="guide-step"><span class="guide-num">4</span><span>홈 화면에 생긴 <b>급매 알리미</b> 앱 아이콘으로 실행</span></div>
      <div class="guide-step"><span class="guide-num">5</span><span>앱 안에서 <b>🔔 알림 허용</b> 버튼 다시 탭해 푸시를 연결</span></div>`;
  } else if (isAndroid) {
    steps = `
      <p class="guide-note">📱 Android에서 모바일 푸시를 받으려면 <b>HTTPS</b> 주소로 접속해야 합니다.</p>
      <div class="guide-step"><span class="guide-num">1</span><span>PC에서 <code>brew install ngrok && ngrok http 5101</code> 실행</span></div>
      <div class="guide-step"><span class="guide-num">2</span><span>출력된 <b>https://…ngrok-free.app</b> 주소를 폰으로 열기</span></div>
      <div class="guide-step"><span class="guide-num">3</span><span>Chrome 주소창 오른쪽 <b>⋮</b> → <b>홈 화면에 추가</b> (선택)</span></div>
      <div class="guide-step"><span class="guide-num">4</span><span>페이지에서 <b>🔔 알림 허용</b> 버튼 탭 → 팝업에서 <b>허용</b> → 푸시 연결 완료</span></div>`;
  } else {
    // Desktop but Notification not supported (e.g., Safari < 16)
    steps = `
      <p class="guide-note">이 브라우저는 알림을 지원하지 않습니다.</p>
      <div class="guide-step"><span class="guide-num">1</span><span><b>Chrome</b> 또는 <b>Edge</b> 브라우저로 접속하세요.</span></div>
      <div class="guide-step"><span class="guide-num">2</span><span>주소창에서 자물쇠 🔒 아이콘 → 알림 허용</span></div>`;
  }

  const overlay = document.createElement('div');
  overlay.id = 'notif-guide-overlay';
  overlay.innerHTML = `
    <div class="notif-guide-modal">
      <div class="notif-guide-header">
        <span>🔔 알림 설정 방법</span>
        <button class="notif-guide-close" onclick="document.getElementById('notif-guide-overlay').remove()">✕</button>
      </div>
      <div class="notif-guide-body">
        ${steps}
      </div>
      ${(!isDenied && (isIOS || isAndroid)) ? `
      <div class="notif-guide-footer">
        <p style="font-size:11px;color:var(--text3);margin:0;">현재 주소: <code>${currentUrl}</code></p>
      </div>` : ''}
    </div>`;
  overlay.addEventListener('click', e => { if (e.target === overlay) overlay.remove(); });
  document.body.appendChild(overlay);
}

// ── Toast notification ────────────────────────────────────────────────────────
function showToast(msg, type = 'info') {
  const el = document.createElement('div');
  el.style.cssText = `
    position:fixed; bottom:60px; right:16px; z-index:9999;
    padding:10px 16px; border-radius:8px; font-size:12px; font-weight:500;
    background:${type==='success' ? 'var(--up-bg)' : type==='error' ? 'var(--urgent-bg)' : 'var(--bg3)'};
    color:${type==='success' ? 'var(--up)' : type==='error' ? 'var(--urgent)' : 'var(--text)'};
    border:1px solid ${type==='success' ? 'var(--up)' : type==='error' ? 'var(--urgent)' : 'var(--border)'};
    box-shadow:var(--shadow); animation: fadeIn 0.2s ease;
    max-width:320px;
  `;
  el.textContent = msg;
  document.body.appendChild(el);
  setTimeout(() => el.remove(), 3500);
}

// ── Utilities ─────────────────────────────────────────────────────────────────
function fmtNum(n) {
  return Number(n).toLocaleString('ko-KR');
}
function escHtml(str) {
  return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ── Debounce ──────────────────────────────────────────────────────────────────
function debounce(fn, ms) {
  let t;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}

// ── Event Wiring ──────────────────────────────────────────────────────────────
function wireEvents() {
  // Theme toggle
  document.getElementById('theme-btn').addEventListener('click', () => {
    applyTheme(state.theme === 'dark' ? 'light' : 'dark');
  });

  // Sidebar toggle
  document.getElementById('sidebar-toggle').addEventListener('click', toggleSidebar);
  document.getElementById('sidebar-open-btn').addEventListener('click', toggleSidebar);
  document.getElementById('btn-mobile-sidebar').addEventListener('click', toggleSidebar);
  document.getElementById('btn-mobile-filter-fab').addEventListener('click', toggleSidebar);
  document.getElementById('mobile-dim').addEventListener('click', () => setMobileSidebar(false));
  document.getElementById('btn-map-toggle').addEventListener('click', toggleMap);
  document.getElementById('btn-mobile-map-fab').addEventListener('click', () => {
    if (!state.mapExpanded) {
      state.mapExpanded = true;
      applyMapVisibility();
    }
    document.getElementById('map-wrap')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
  document.getElementById('btn-hero-notif').addEventListener('click', async () => {
    // On HTTP (non-localhost), mobile browsers block Notification API — show guide
    const isHttp = location.protocol === 'http:' && location.hostname !== 'localhost' && location.hostname !== '127.0.0.1';
    const isMobile = /Android|iPhone|iPad|iPod/i.test(navigator.userAgent);
    const notifSupported = 'Notification' in window;

    if (!notifSupported || (isMobile && isHttp)) {
      showMobileNotifGuide();
      return;
    }

    // Desktop or HTTPS mobile — try requesting permission normally
    const ready = await ensureNotificationsReady(true);
    if (ready) {
      showToast(state.pushSubscribed ? '📲 모바일 푸시가 연결되었습니다.' : '✅ 브라우저 알림이 활성화되었습니다.', 'success');
      updateHeroNotifBtn();
    } else if (state.notificationPermission === 'denied') {
      showMobileNotifGuide();
    } else {
      showToast('알림 권한이 허용되지 않았습니다.', 'error');
    }
  });
  document.getElementById('btn-hero-alert').addEventListener('click', async () => {
    try {
      await saveAlertRule();
    } catch (e) {
      showToast('알림 등록 실패: ' + e.message, 'error');
    }
  });
  document.getElementById('btn-mobile-alert-fab').addEventListener('click', async () => {
    try {
      await saveAlertRule();
    } catch (e) {
      showToast('알림 등록 실패: ' + e.message, 'error');
    }
  });

  // Search
  const searchInput = document.getElementById('search-input');
  const searchClear = document.getElementById('search-clear');
  searchInput.addEventListener('input', debounce(e => {
    state.filters.search = e.target.value.trim();
    state.page = 1;
    searchClear.classList.toggle('hidden', !e.target.value);
    refreshAlertDraftSummary();
    loadListings();
  }, 350));
  searchClear.addEventListener('click', () => {
    searchInput.value = '';
    searchClear.classList.add('hidden');
    state.filters.search = '';
    state.page = 1;
    refreshAlertDraftSummary();
    loadListings();
  });

  // Pill filters
  document.querySelectorAll('.pill-group').forEach(group => {
    group.querySelectorAll('.pill').forEach(pill => {
      pill.addEventListener('click', () => {
        group.querySelectorAll('.pill').forEach(p => p.classList.remove('active'));
        pill.classList.add('active');
        const param = group.dataset.param;
        state.filters[param] = pill.dataset.value;
        state.page = 1;
        refreshAlertDraftSummary();
        loadListings();
      });
    });
  });

  // Stat card filters
  document.querySelectorAll('#stats-bar .stat-card[data-filter]').forEach(card => {
    card.addEventListener('click', () => {
      const filter = card.dataset.filter;
      const isActive = state.activeStatFilter === filter;

      // "전체 급매" or toggle off → clear all
      if (filter === 'all' || isActive) {
        state.activeStatFilter = '';
        state.filters.price_down_only = false;
        state.filters.property_type = '';
      } else {
        state.activeStatFilter = filter;
        state.filters.price_down_only = false;
        state.filters.property_type = '';

        if (filter === 'price-down') {
          state.filters.price_down_only = true;
        } else if (filter === '__OTHER__') {
          state.filters.property_type = '__OTHER__';
        } else {
          state.filters.property_type = filter;
        }
      }

      // Sync header property_type pills
      document.querySelectorAll('#type-filter .pill').forEach(p => {
        p.classList.toggle('active', p.dataset.value === state.filters.property_type);
      });
      if (!document.querySelector('#type-filter .pill.active')) {
        document.querySelector('#type-filter .pill[data-value=""]').classList.add('active');
      }

      // Update active styling on stat cards
      document.querySelectorAll('#stats-bar .stat-card[data-filter]').forEach(c => {
        c.classList.toggle('stat-active', c.dataset.filter === state.activeStatFilter);
      });

      state.page = 1;
      refreshAlertDraftSummary();
      loadListings();
    });
  });

  // Listing cards → Naver link (delegated: cards are re-rendered on every load)
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

  document.getElementById('modal-close')?.addEventListener('click', () => {
    document.getElementById('modal-overlay')?.classList.add('hidden');
  });
  document.getElementById('modal-overlay')?.addEventListener('click', e => {
    if (e.target.id === 'modal-overlay') e.target.classList.add('hidden');
  });

  // Trend / region lists → district filter (delegated)
  ['list-increasing', 'list-decreasing', 'list-price-down', 'list-region-stats'].forEach(id => {
    document.getElementById(id)?.addEventListener('click', e => {
      const item = e.target.closest('[data-district]');
      if (item) selectDistrict(item.dataset.district);
    });
  });

  // Sort
  document.getElementById('sort-select').addEventListener('change', e => {
    state.filters.sort_by = e.target.value;
    state.page = 1;
    loadListings();
  });

  // Pagination
  document.getElementById('btn-prev').addEventListener('click', () => {
    if (state.page > 1) { state.page--; loadListings(); }
  });
  document.getElementById('btn-next').addEventListener('click', () => {
    if (state.page < state.totalPages) { state.page++; loadListings(); }
  });

  // Region badge clear
  document.getElementById('region-badge-clear').addEventListener('click', () => {
    selectDistrict(state.filters.district);
  });

  // Alert controls
  document.getElementById('btn-alert-enable').addEventListener('click', async () => {
    const ready = await ensureNotificationsReady(true);
    showToast(
      ready
        ? (state.pushSubscribed ? '모바일 푸시가 연결되었습니다.' : '브라우저 알림이 활성화되었습니다.')
        : '알림 권한이 허용되지 않았습니다.',
      ready ? 'success' : 'error'
    );
  });
  document.getElementById('btn-alert-save').addEventListener('click', async () => {
    try {
      await saveAlertRule();
    } catch (e) {
      showToast('알림 등록 실패: ' + e.message, 'error');
    }
  });
  document.getElementById('alert-keyword').addEventListener('input', refreshAlertDraftSummary);
  document.getElementById('alert-rules-list').addEventListener('click', async (event) => {
    const button = event.target.closest('.alert-rule-remove');
    if (!button) return;
    try {
      await removeAlertRule(button.dataset.alertId);
    } catch (e) {
      showToast('알림 삭제 실패: ' + e.message, 'error');
    }
  });

  // Alert section collapse toggle
  const alertToggleBtn = document.getElementById('alert-section-toggle');
  const alertPanelBody = document.getElementById('alert-panel-body');
  const alertCollapsed = localStorage.getItem('alertSectionCollapsed') === 'true';
  if (alertCollapsed) {
    alertPanelBody.classList.add('collapsed');
    alertToggleBtn.classList.add('collapsed');
  }
  alertToggleBtn.addEventListener('click', () => {
    const isNowCollapsed = alertPanelBody.classList.toggle('collapsed');
    alertToggleBtn.classList.toggle('collapsed', isNowCollapsed);
    localStorage.setItem('alertSectionCollapsed', isNowCollapsed);
  });

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

  // ESC 키
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') document.getElementById('modal-overlay')?.classList.add('hidden');
    if (e.key === 'Escape' && state.mobileSidebarOpen) setMobileSidebar(false);
  });

  window.addEventListener('resize', () => {
    if (!isMobileViewport() && state.mobileSidebarOpen) {
      setMobileSidebar(false);
    }
    if (!isMobileViewport() && !state.mapExpanded) {
      state.mapExpanded = true;
      applyMapVisibility();
    }
  });
}

// ── Init ──────────────────────────────────────────────────────────────────────
async function init() {
  state.clientId = getClientId();
  applyTheme(state.theme);
  updateNotificationStatus();

  if (!state.sidebarOpen) {
    document.getElementById('sidebar').classList.add('collapsed');
    document.getElementById('sidebar-toggle').textContent = '▶';
    document.getElementById('sidebar-open-btn').classList.add('visible');
  }

  wireEvents();
  initMap();
  applyMapVisibility();
  refreshAlertDraftSummary();
  updateHeroAlertCount();
  updateListingsSummary();

  const primaryLoads = [
    loadCrawlStatus(),
    loadListings(),
    loadSidebar(),
    loadTagFilter(),
  ];

  const alertsBootstrap = (async () => {
    try {
      await loadPushConfig();
      await ensureNotificationsReady(false);
      updateHeroNotifBtn();
      await loadAlertRules();
      startAlertPolling();
      await checkAlertMatches();
    } catch (e) {
      console.warn('Alert bootstrap error:', e);
    }
  })();

  await Promise.allSettled(primaryLoads);
  void alertsBootstrap;
}

document.addEventListener('DOMContentLoaded', init);
