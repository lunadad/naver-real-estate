/* 단지 상세 페이지 — /api/complex 결과를 그린다. */

const ALERT_THRESHOLD = 3;
const root = document.getElementById('complex-content');
const params = new URLSearchParams(location.search);
const district = (params.get('district') || '').trim();
const buildingName = (params.get('name') || params.get('building_name') || '').trim();

try {
  document.documentElement.setAttribute('data-theme', localStorage.getItem('theme') || 'light');
} catch (e) { /* 저장소 접근 불가 시 기본 테마 */ }

function escHtml(str) {
  return String(str ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
function fmtNum(n) { return Number(n).toLocaleString('ko-KR'); }

/** 만원 단위 → "10억 5,000만" */
function fmtPrice(manwon) {
  if (manwon == null) return '—';
  const v = Math.round(Number(manwon));
  const eok = Math.floor(v / 10000);
  const rest = v % 10000;
  if (eok && rest) return `${eok}억 ${fmtNum(rest)}만`;
  if (eok) return `${eok}억`;
  return `${fmtNum(rest)}만`;
}

function getClientId() {
  const key = 'real-estate-alert-client-id';
  let id = '';
  try { id = localStorage.getItem(key) || ''; } catch (e) { /* ignore */ }
  if (!id) {
    id = window.crypto?.randomUUID?.() || `client-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    try { localStorage.setItem(key, id); } catch (e) { /* ignore */ }
  }
  return id;
}

function showEmpty(message) {
  document.title = '단지 상세 · 부동산 급매 알리미';
  root.innerHTML = `<p class="cx-empty">${escHtml(message)}</p>`;
}

function renderStats(data) {
  const s = data.stats;
  let diffNote = '지난주 비교 데이터 없음';
  if (s.week_ago_count != null) {
    const diff = s.current_count - s.week_ago_count;
    diffNote = `지난주 대비 ${diff > 0 ? '+' : diff < 0 ? '−' : ''}${fmtNum(Math.abs(diff))}건`;
  }
  const topArea = [...data.areas].sort((a, b) => b.prices.length - a.prices.length)[0];
  return `
    <section class="cx-stats" aria-label="단지 요약">
      <div class="cx-card"><div class="cx-stat-label">현재 급매</div>
        <div class="cx-stat-value is-red">${fmtNum(s.current_count)}건</div>
        <div class="cx-stat-note">${escHtml(diffNote)}</div></div>
      <div class="cx-card"><div class="cx-stat-label">진짜 급매</div>
        <div class="cx-stat-value is-blue">${fmtNum(s.bargain_count)}건</div>
        <div class="cx-stat-note">단지 중앙값 대비 10~40% 저렴</div></div>
      <div class="cx-card"><div class="cx-stat-label">가격인하 비율</div>
        <div class="cx-stat-value">${Math.round((s.price_down_ratio || 0) * 100)}%</div>
        <div class="cx-stat-note">전체 급매 중 가격인하</div></div>
      <div class="cx-card"><div class="cx-stat-label">${topArea ? `${escHtml(topArea.area)} 중앙값` : '중앙값'}</div>
        <div class="cx-stat-value">${topArea ? escHtml(fmtPrice(topArea.median)) : '—'}</div>
        <div class="cx-stat-note">${topArea ? '가장 매물이 많은 평형' : '매매 호가 없음'}</div></div>
    </section>`;
}

/** 같은 위치에 겹치는 점은 레인을 나눠 위아래로 쌓는다. */
function layoutDots(prices, min, span) {
  const lanes = [];
  return prices.map(p => {
    const x = span ? ((p.value - min) / span) * 100 : 50;
    let lane = lanes.findIndex(last => x - last >= 2.4);
    if (lane === -1) { lane = lanes.length; lanes.push(x); } else { lanes[lane] = x; }
    return { ...p, x, lane };
  });
}

function renderDistribution(data) {
  if (!data.areas.length) {
    return `<section class="cx-panel"><h2 class="cx-panel-title">평형별 매매 호가 분포</h2><p class="cx-empty">매매 호가 데이터가 없습니다.</p></section>`;
  }
  const all = data.areas.flatMap(a => a.prices.map(p => p.value));
  let min = Math.min(...all);
  let max = Math.max(...all);
  const pad = (max - min) * 0.04 || max * 0.02 || 1;
  min -= pad; max += pad;
  const span = max - min;
  const rows = data.areas.map(a => {
    const dots = layoutDots(a.prices, min, span);
    const lanes = Math.max(...dots.map(d => d.lane)) + 1;
    const bargains = a.prices.filter(p => p.bargain).length;
    const medianX = ((a.median - min) / span) * 100;
    const label = `${a.area} 매물 ${a.prices.length}건, 중앙값 ${fmtPrice(a.median)}, 진짜 급매 ${bargains}건`;
    return `
      <div class="cx-dist-row" role="img" aria-label="${escHtml(label)}">
        <div class="cx-dist-label">${escHtml(a.area)}<small>${a.prices.length}건 · 중앙값 ${escHtml(fmtPrice(a.median))}</small></div>
        <div class="cx-strip" style="height:${lanes * 14 + 10}px">
          <span class="cx-median" style="left:${medianX}%"></span>
          ${dots.map(d => `<span class="cx-dot${d.bargain ? ' is-bargain' : ''}" style="left:${d.x}%;top:${d.lane * 14 + 4}px" title="${escHtml(fmtPrice(d.value))}${d.bargain ? ' · 진짜 급매' : ''}"></span>`).join('')}
        </div>
      </div>`;
  }).join('');
  return `
    <section class="cx-panel">
      <h2 class="cx-panel-title">평형별 매매 호가 분포</h2>
      ${rows}
      <div class="cx-axis" aria-hidden="true"><span>${escHtml(fmtPrice(Math.min(...all)))}</span><span>${escHtml(fmtPrice(Math.max(...all)))}</span></div>
      <div class="cx-legend">
        <span><i class="cx-dot is-bargain"></i>진짜 급매</span>
        <span><i class="cx-dot"></i>일반 매물</span>
        <span><i class="cx-median"></i>중앙값</span>
      </div>
    </section>`;
}

function renderHistory(data) {
  const days = data.history || [];
  const counts = days.map(d => d.total_count);
  const peak = Math.max(1, ...counts.filter(v => v != null));
  const bars = days.map((d, i) => {
    const has = d.total_count != null;
    const height = has ? Math.max(4, (d.total_count / peak) * 100) : 0;
    const cls = !has ? ' is-none' : i === days.length - 1 ? ' is-last' : '';
    return `<div class="cx-bar-col" title="${escHtml(d.label)} ${has ? d.total_count + '건' : '데이터 없음'}">
      ${has ? `<span class="cx-bar-val">${fmtNum(d.total_count)}</span>` : ''}
      <div class="cx-bar${cls}" style="height:${has ? height : 2}%"></div></div>`;
  }).join('');
  const summary = days.filter(d => d.total_count != null).map(d => `${d.label} ${d.total_count}건`).join(', ');
  return `
    <section class="cx-panel">
      <h2 class="cx-panel-title">최근 14일 급매 수</h2>
      <div class="cx-bars" role="img" aria-label="최근 14일 급매 수: ${escHtml(summary || '데이터 없음')}">${bars}</div>
      <div class="cx-bar-labels" aria-hidden="true">${days.map(d => `<span>${escHtml(d.label)}</span>`).join('')}</div>
    </section>`;
}

function renderDeals(data) {
  const deals = data.listings.filter(l => l.is_bargain);
  const body = deals.length
    ? `<ul class="cx-deals">${deals.map(l => {
        const tag = l.naver_url ? 'a' : 'div';
        const href = l.naver_url ? ` href="${escHtml(l.naver_url)}" target="_blank" rel="noopener noreferrer"` : '';
        const meta = [l.area, l.floor, l.confirmed_date ? `${l.confirmed_date} 확인` : ''].filter(Boolean).join(' · ');
        return `<li><${tag} class="cx-deal"${href}>
          <span><span class="cx-deal-price">${escHtml(l.price || fmtPrice(l.price_sort_value))}</span>
          <span class="cx-deal-meta"> ${escHtml(meta)}</span></span>
          <span class="cx-badge">중앙값 −${Math.round(l.bargain_discount * 100)}%</span></${tag}></li>`;
      }).join('')}</ul>`
    : '<p class="cx-empty">현재 진짜 급매가 없습니다.</p>';
  return `<section class="cx-panel"><h2 class="cx-panel-title">이 단지의 진짜 급매</h2>${body}</section>`;
}

async function registerAlert(button, statusEl) {
  button.disabled = true;
  statusEl.textContent = '';
  try {
    if (typeof Notification !== 'undefined' && Notification.permission === 'default') {
      await Notification.requestPermission();
    }
    const res = await fetch('/api/alert-rules', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        client_id: getClientId(),
        district,
        building_name: buildingName,
        min_daily_change: ALERT_THRESHOLD,
      }),
    });
    if (!res.ok) throw new Error(`API error ${res.status}`);
    const result = await res.json();
    statusEl.textContent = `알림을 등록했어요: ${result.rule?.name || buildingName}`;
  } catch (err) {
    statusEl.textContent = `알림 등록에 실패했어요: ${err.message}`;
  } finally {
    button.disabled = false;
  }
}

function render(data) {
  const type = data.listings[0]?.property_type || '';
  document.title = `${data.building_name} · 부동산 급매 알리미`;
  root.innerHTML = `
    <div class="cx-head">
      <div>
        <h1 class="cx-title">${escHtml(data.building_name)}</h1>
        <p class="cx-sub">${escHtml([data.district, type].filter(Boolean).join(' · '))}</p>
      </div>
      <button type="button" class="cx-btn" id="complex-alert-btn">이 단지 알림 받기</button>
    </div>
    <p class="cx-status" id="complex-alert-status" role="status"></p>
    ${renderStats(data)}${renderDistribution(data)}${renderHistory(data)}${renderDeals(data)}`;
  const btn = document.getElementById('complex-alert-btn');
  const statusEl = document.getElementById('complex-alert-status');
  btn.addEventListener('click', () => registerAlert(btn, statusEl));
}

async function load() {
  if (!district || !buildingName) { showEmpty('데이터 없음 — 단지 정보가 지정되지 않았습니다.'); return; }
  try {
    const res = await fetch(`/api/complex?district=${encodeURIComponent(district)}&building_name=${encodeURIComponent(buildingName)}`);
    if (res.status === 404) { showEmpty('데이터 없음 — 현재 수집된 매물이 없는 단지입니다.'); return; }
    if (!res.ok) throw new Error(`API error ${res.status}`);
    render(await res.json());
  } catch (err) {
    showEmpty(`불러오지 못했습니다: ${err.message}`);
  }
}

load();
