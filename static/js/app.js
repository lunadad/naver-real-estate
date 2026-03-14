/* ═══════════════════════════════════════════════════════════════════════════
   부동산 급매 터미널 — Frontend App (급매 전용)
   ═══════════════════════════════════════════════════════════════════════════ */

// ── State ──────────────────────────────────────────────────────────────────
const state = {
  theme: localStorage.getItem('theme') || 'dark',
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

  loadListings();
}

// ── Listings ─────────────────────────────────────────────────────────────────
async function loadListings() {
  const grid = document.getElementById('listings-grid');
  grid.innerHTML = '<div class="loading-state"><div class="spinner"></div><p>불러오는 중...</p></div>';

  try {
    const data = await api(`/api/listings?${buildQuery()}`);
    renderListings(data);
    updateStats(data);
  } catch (e) {
    grid.innerHTML = '<div class="empty-state">데이터를 불러올 수 없습니다.</div>';
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

  grid.innerHTML = listings.map(l => {
    const tags = parseTags(l.tags);
    const hasPriceDown = tags.includes('가격인하');
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
        <span class="naver-link-icon" title="네이버 부동산">🔗</span>
      </div>
      <div class="card-name" title="${l.building_name}">${l.building_name}</div>
      <div class="card-location">📍 ${l.region ? l.region.replace('특별시','').replace('광역시','').replace('특별자치시','') : ''} ${l.district}</div>
      <div class="card-price">${l.price || '—'}</div>
      <div class="card-meta">
        <span>📐 ${l.area || '—'}</span>
        <span>🏢 ${l.floor || '—'}층</span>
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
}

// ── Sidebar ──────────────────────────────────────────────────────────────────
async function loadSidebar() {
  try {
    const [trends, regionStats] = await Promise.all([
      api('/api/trends'),
      api('/api/region-stats'),
    ]);

    state.regionStats = regionStats;
    renderMapMarkers(regionStats);
    renderTrends(trends);
    renderRegionStats(regionStats);
  } catch (e) {
    console.warn('Sidebar load error:', e);
  }
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

// ── Crawl ─────────────────────────────────────────────────────────────────────
async function triggerCrawl() {
  const btn = document.getElementById('btn-crawl');
  const label = document.getElementById('crawl-label');

  btn.disabled = true;
  btn.classList.add('loading');
  label.textContent = '급매 크롤링 중...';

  try {
    const result = await api('/api/crawl', { method: 'POST' });
    label.textContent = '지금 크롤링';
    btn.classList.remove('loading');
    btn.disabled = false;

    const demoBadge = document.getElementById('demo-badge');
    if (result.source === 'demo') {
      demoBadge.classList.remove('hidden');
    } else {
      demoBadge.classList.add('hidden');
    }

    document.getElementById('info-last-crawl').textContent =
      `마지막 크롤링: 방금 (급매 ${result.total}개)`;

    await loadSidebar();
    state.page = 1;
    await loadListings();

    showToast(result.message || '업데이트 완료', 'success');
  } catch (e) {
    label.textContent = '지금 크롤링';
    btn.classList.remove('loading');
    btn.disabled = false;
    showToast('크롤링 실패: ' + e.message, 'error');
  }
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

      if (last.source === 'demo') {
        document.getElementById('demo-badge').classList.remove('hidden');
      }
    }
    if (data.next_crawl) {
      const dt = new Date(data.next_crawl);
      const timeStr = dt.toLocaleString('ko-KR', { month: 'numeric', day: 'numeric', hour: '2-digit', minute: '2-digit' });
      document.getElementById('info-next-crawl').textContent = `다음 크롤링: ${timeStr}`;
    }
    if (data.scheduled_hour !== undefined) {
      document.getElementById('schedule-hour').value = data.scheduled_hour;
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

  // Search
  const searchInput = document.getElementById('search-input');
  const searchClear = document.getElementById('search-clear');
  searchInput.addEventListener('input', debounce(e => {
    state.filters.search = e.target.value.trim();
    state.page = 1;
    searchClear.classList.toggle('hidden', !e.target.value);
    loadListings();
  }, 350));
  searchClear.addEventListener('click', () => {
    searchInput.value = '';
    searchClear.classList.add('hidden');
    state.filters.search = '';
    state.page = 1;
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

  // Crawl button
  document.getElementById('btn-crawl').addEventListener('click', triggerCrawl);

  // Save schedule
  document.getElementById('btn-save-schedule').addEventListener('click', async () => {
    const hour = parseInt(document.getElementById('schedule-hour').value, 10);
    try {
      const res = await api('/api/update-schedule', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ hour }),
      });
      showToast(`자동 크롤링 시간 변경: 매일 ${hour}시`, 'success');
      if (res.next_crawl) {
        const dt = new Date(res.next_crawl);
        document.getElementById('info-next-crawl').textContent =
          `다음 크롤링: ${dt.toLocaleString('ko-KR', { month:'numeric', day:'numeric', hour:'2-digit', minute:'2-digit' })}`;
      }
    } catch (e) {
      showToast('저장 실패', 'error');
    }
  });

  // ESC 키
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') document.getElementById('modal-overlay')?.classList.add('hidden');
  });
}

// ── Init ──────────────────────────────────────────────────────────────────────
async function init() {
  applyTheme(state.theme);

  if (!state.sidebarOpen) {
    document.getElementById('sidebar').classList.add('collapsed');
    document.getElementById('sidebar-toggle').textContent = '▶';
    document.getElementById('sidebar-open-btn').classList.add('visible');
  }

  wireEvents();
  initMap();
  await loadCrawlStatus();
  await loadSidebar();
  await loadListings();
}

document.addEventListener('DOMContentLoaded', init);
