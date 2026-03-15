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
    sort_by: 'recent',
    price_down_only: false,
  },
  activeStatFilter: '',  // tracks which stat card is active
  page: 1,
  perPage: 20,
  totalPages: 1,
  regionStats: [],
  mapMarkers: {},   // district → Leaflet circle
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
  Object.entries(p).forEach(([k, v]) => { if (v !== '' && v !== false) q.set(k, v); });
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
  return parts.length ? parts.join(' · ') : '전체 급매';
}

function updateHeroAlertCount() {
  const el = document.getElementById('hero-alert-rule-count');
  if (el) el.textContent = `${fmtNum(state.alertRules.length || 0)}개`;

  // 모바일 하단 네비 배지
  const badge = document.getElementById('nav-alert-badge');
  if (badge) {
    const count = state.alertRules.length || 0;
    badge.textContent = count;
    badge.classList.toggle('hidden', count === 0);
  }
}

function updateHeroFocusRegion() {
  const el = document.getElementById('hero-focus-region');
  if (!el) return;
  if (state.filters.district) {
    el.textContent = state.filters.district;
    return;
  }
  if (state.filters.search) {
    el.textContent = state.filters.search;
    return;
  }
  el.textContent = '전국';
}

function updateHeroCrawlSummary(text) {
  const el = document.getElementById('hero-crawl-summary');
  if (el && text) el.textContent = text;
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

function applyMapVisibility() {
  const wrap = document.getElementById('map-wrap');
  const btn = document.getElementById('btn-map-toggle');
  const legend = document.getElementById('map-legend');
  if (!wrap || !btn) return;
  wrap.classList.toggle('collapsed', !state.mapExpanded);
  btn.textContent = state.mapExpanded ? '지도 접기' : '지도 펼치기';
  if (legend) legend.classList.toggle('hidden', !state.mapExpanded);
  localStorage.setItem('mapExpanded', state.mapExpanded);
  if (state.map && state.mapExpanded) {
    setTimeout(() => state.map.invalidateSize(), 260);
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

// ── Map ─────────────────────────────────────────────────────────────────────
function initMap() {
  state.map = L.map('map', { zoomControl: true, attributionControl: true }).setView([36.5, 127.8], 7);

  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '© OpenStreetMap',
    maxZoom: 18,
  }).addTo(state.map);
}

function urgencyColor(total) {
  // 급매 수 기반 색상 (모든 매물이 급매이므로)
  if (total == null || isNaN(total)) return '#58a6ff';
  if (total < 20) return '#3fb950';
  if (total < 50) return '#d29922';
  return '#f85149';
}

function renderMapMarkers(regionStats) {
  if (!state.map) return;

  Object.values(state.mapMarkers).forEach(m => m.remove());
  state.mapMarkers = {};

  // Group by district (sum totals if same district in different regions)
  const byDistrict = {};
  regionStats.forEach(r => {
    const key = r.district;
    if (!byDistrict[key]) byDistrict[key] = { ...r };
    else {
      byDistrict[key].total += r.total;
    }
  });

  api('/api/regions').then(regions => {
    const coordMap = {};
    regions.forEach(r => {
      r.districts.forEach(d => { coordMap[d.name] = { lat: d.lat, lng: d.lng }; });
    });

    const maxTotal = Math.max(...Object.values(byDistrict).map(d => d.total), 1);

    Object.entries(byDistrict).forEach(([district, data]) => {
      const coords = coordMap[district];
      if (!coords) return;

      const radius = 6 + (data.total / maxTotal) * 22;
      const color = urgencyColor(data.total);

      const circle = L.circleMarker([coords.lat, coords.lng], {
        radius,
        color,
        fillColor: color,
        fillOpacity: 0.45,
        weight: 2,
        opacity: 0.9,
      });

      const displayName = data.display_name || `${data.region} ${data.district}`;
      circle.bindPopup(`
        <div style="line-height:1.6">
          <strong>${displayName}</strong><br/>
          급매: <b style="color:${color}">${data.total}개</b>
        </div>
      `);

      circle.on('click', () => {
        selectDistrict(district);
      });

      circle.addTo(state.map);
      state.mapMarkers[district] = circle;
    });
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
    m.setStyle({ weight: d === state.filters.district ? 3 : 2, opacity: state.filters.district && d !== state.filters.district ? 0.4 : 0.9 });
  });

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
  grid.classList.add('loading');
  grid.innerHTML = '<div class="loading-state"><div class="spinner"></div><p>불러오는 중...</p></div>';
  refreshAlertDraftSummary();
  updateListingsSummary();

  try {
    const data = await api(`/api/listings?${buildQuery()}`);
    grid.classList.remove('loading');
    renderListings(data);
    updateStats(data);
  } catch (e) {
    grid.classList.remove('loading');
    grid.innerHTML = '<div class="empty-state">데이터를 불러올 수 없습니다.</div>';
    updateListingsSummary(0);
  }
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

  // 카드 입장 애니메이션 트리거
  grid.classList.remove('animating');
  void grid.offsetWidth; // reflow
  grid.classList.add('animating');

  grid.innerHTML = listings.map(l => {
    const tags = parseTags(l.tags);
    const hasPriceDown = tags.includes('가격인하');
    const compactRegion = `${l.region ? l.region.replace('특별시','').replace('광역시','').replace('특별자치시','') : ''} ${l.district}`.trim();
    return `
    <div class="listing-card urgent-card-item"
         onclick="openNaver('${l.article_no}')"
         data-id="${l.id}"
         data-article-no="${l.article_no}"
         data-naver-url="${l.naver_url || ''}"
         title="네이버 부동산에서 보기">
      <div class="card-badges">
        <span class="badge badge-urgent">${hasPriceDown ? '📉 가격인하' : '⚡ 급매'}</span>
        <span class="badge badge-type">${l.property_type}</span>
        <span class="badge ${tradeBadgeClass(l.trade_type)}">${l.trade_type}</span>
        <span class="badge badge-date">${formatDate(l.confirmed_date)}</span>
        <span class="naver-link-icon" title="네이버 부동산">네이버 보기</span>
      </div>
      <div class="card-main">
        <div class="card-price-block">
          <div class="card-price">${l.price || '—'}</div>
          <div class="card-location">${escHtml(compactRegion)}</div>
        </div>
        <div class="card-date-chip">확인 ${formatDate(l.confirmed_date) || '—'}</div>
      </div>
      <div class="card-name-row">
        <div class="card-name" title="${l.building_name}">${l.building_name}</div>
      </div>
      <div class="card-meta">
        <span>면적 ${l.area || '—'}</span>
        <span>층 ${l.floor || '—'}</span>
        <span>${l.trade_type}</span>
      </div>
      ${l.description ? `<div class="card-desc">${escHtml(l.description)}</div>` : ''}
      <div class="card-tags">
        ${tags.map(t => `<span class="tag ${t === '급매' ? 'urgent-tag' : t === '가격인하' ? 'price-down-tag' : ''}">${t}</span>`).join('')}
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
  updateHeroFocusRegion();
  updateListingsSummary(data.total || 0);
}

// ── Sidebar ──────────────────────────────────────────────────────────────────
async function loadSidebar() {
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
      renderTrends(trends);
    })
    .catch(e => {
      console.warn('Trends load error:', e);
    });

  await Promise.allSettled([regionStatsPromise, trendsPromise]);
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
    ul.innerHTML = '<li style="padding:5px 12px;color:var(--text3);font-size:11px;">데이터 없음</li>';
    return;
  }
  ul.innerHTML = items.map(item => {
    let badge, badgeClass, sub;
    if (type === 'up') {
      badge = `+${item.diff}`;
      badgeClass = 'up';
      sub = `급매 ${item.current_cnt}개`;
    } else if (type === 'down') {
      badge = `${item.diff}`;
      badgeClass = 'down';
      sub = `급매 ${item.current_cnt}개`;
    } else if (type === 'price-down') {
      badge = `${item.price_down_count || 0}개`;
      badgeClass = 'urgent';
      sub = `급매 ${item.current_cnt}개`;
    } else {
      badge = `${item.current_cnt || 0}`;
      badgeClass = 'urgent';
      sub = `급매 ${item.current_cnt}개`;
    }
    const name = item.display_name || `${item.region} ${item.district}`;
    return `
    <li class="trend-item" data-district="${item.district}" onclick="selectDistrict('${item.district}')">
      <div>
        <div class="trend-name">${name}</div>
        <div class="trend-sub">${sub}</div>
      </div>
      <span class="trend-badge ${badgeClass}">${badge}</span>
    </li>`;
  }).join('');
}

function renderRegionStats(stats) {
  const ul = document.getElementById('list-region-stats');
  const maxTotal = Math.max(...stats.map(s => s.total), 1);
  ul.innerHTML = stats.slice(0, 20).map(s => `
    <li class="region-item" data-district="${s.district}" onclick="selectDistrict('${s.district}')">
      <div class="region-bar-wrap">
        <div class="region-name">${s.display_name || `${s.region} ${s.district}`}</div>
        <div class="region-bar">
          <div class="region-bar-fill" style="width:${(s.total / maxTotal) * 100}%"></div>
        </div>
      </div>
      <span class="region-count">${s.total}</span>
    </li>`).join('');
}

// ── Naver Link ───────────────────────────────────────────────────────────────
function openNaver(articleNo) {
  const card = document.querySelector(`[data-article-no="${articleNo}"]`);
  const url = card?.dataset?.naverUrl;
  if (url) {
    window.open(url, '_blank', 'noopener,noreferrer');
  }
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
    if (last) {
      const dt = new Date(last.crawled_at);
      const timeStr = dt.toLocaleString('ko-KR', { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' });
      document.getElementById('info-last-crawl').textContent =
        `마지막 크롤링: ${timeStr} (급매 ${last.total_count}개)`;
      updateHeroCrawlSummary(`${timeStr} 기준 최신 급매 ${fmtNum(last.total_count || 0)}개`);

      if (last.source === 'demo') {
        document.getElementById('demo-badge').classList.remove('hidden');
      }
    }
    if (data.next_crawl) {
      const dt = new Date(data.next_crawl);
      const timeStr = dt.toLocaleString('ko-KR', { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' });
      document.getElementById('info-next-crawl').textContent = `다음 크롤링: ${timeStr}`;
    }
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
  if (state.map) setTimeout(() => state.map.invalidateSize(), 250);
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
  document.getElementById('mobile-dim').addEventListener('click', () => setMobileSidebar(false));
  document.getElementById('btn-map-toggle').addEventListener('click', toggleMap);
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

  // ── Mobile bottom navigation ──────────────────────────────────────────────
  function setMobileNavActive(tab) {
    document.querySelectorAll('.mobile-nav-btn').forEach(b => {
      b.classList.remove('active', 'nav-map-active-brand');
    });
    const btn = document.getElementById(`nav-btn-${tab}`);
    if (!btn) return;
    if (tab === 'map') btn.classList.add('nav-map-active-brand');
    else btn.classList.add('active');
  }

  document.getElementById('nav-btn-home')?.addEventListener('click', () => {
    setMobileNavActive('home');
    if (state.mobileSidebarOpen) setMobileSidebar(false);
    // 목록 상단으로 스크롤
    document.getElementById('main-content')?.scrollTo({ top: 0, behavior: 'smooth' });
  });

  document.getElementById('nav-btn-map')?.addEventListener('click', () => {
    if (state.mobileSidebarOpen) setMobileSidebar(false);
    // 지도가 접혀 있으면 펼치고, 지도 위치로 스크롤
    if (!state.mapExpanded) {
      state.mapExpanded = true;
      applyMapVisibility();
    }
    document.getElementById('map-wrap')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    setMobileNavActive('map');
    // 지도 탭 재클릭 시 접기 토글
    const btn = document.getElementById('nav-btn-map');
    btn.addEventListener('click', function onceMore() {
      btn.removeEventListener('click', onceMore);
      if (state.mapExpanded) {
        state.mapExpanded = false;
        applyMapVisibility();
        setMobileNavActive('home');
      }
    }, { once: true });
  });

  document.getElementById('nav-btn-alert')?.addEventListener('click', () => {
    setMobileNavActive('alert');
    // 알림 섹션 펼치기
    const panel = document.getElementById('alert-panel-body');
    const toggle = document.getElementById('alert-section-toggle');
    if (panel?.classList.contains('collapsed')) {
      panel.classList.remove('collapsed');
      toggle?.classList.remove('collapsed');
      localStorage.setItem('alertSectionCollapsed', 'false');
    }
    setMobileSidebar(true);
    setTimeout(() => {
      document.getElementById('alert-panel-body')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 280);
  });

  document.getElementById('nav-btn-filter')?.addEventListener('click', () => {
    setMobileNavActive('filter');
    setMobileSidebar(true);
    setTimeout(() => {
      document.getElementById('list-region-stats')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 280);
  });

  // 사이드바 닫힐 때 nav 상태 홈으로 복귀
  document.getElementById('mobile-dim')?.addEventListener('click', () => {
    setMobileNavActive('home');
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
