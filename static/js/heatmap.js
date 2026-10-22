/* 지역 히트맵 — /api/heatmap 결과를 타일 그리드로 그린다. */

const root = document.getElementById('heatmap-content');
const titleEl = document.getElementById('heatmap-title');
const DEFAULT_REGION = '서울특별시';

// 서울 타일 배치 [col, row]
const SEOUL_LAYOUT = {
  '은평구': [2, 1], '종로구': [3, 1], '성북구': [4, 1], '강북구': [5, 1], '도봉구': [6, 1],
  '강서구': [1, 2], '마포구': [2, 2], '서대문구': [3, 2], '중구': [4, 2], '동대문구': [5, 2], '노원구': [6, 2],
  '양천구': [1, 3], '영등포구': [2, 3], '용산구': [3, 3], '성동구': [4, 3], '중랑구': [5, 3], '광진구': [6, 3],
  '구로구': [1, 4], '동작구': [2, 4], '서초구': [3, 4], '강남구': [4, 4], '송파구': [5, 4], '강동구': [6, 4],
  '금천구': [1, 5], '관악구': [2, 5],
};

const view = {
  region: new URLSearchParams(location.search).get('region') || DEFAULT_REGION,
  metric: 'bargain_ratio',
  data: null,
};

try {
  document.documentElement.setAttribute('data-theme', localStorage.getItem('theme') || 'light');
} catch (e) { /* 기본 테마 */ }

function escHtml(str) {
  return String(str ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function metricValue(d, key) {
  const v = d[key];
  return v == null ? null : Number(v);
}

function fmtMetric(key, v) {
  if (v == null) return '—';
  if (key === 'weekly_change_pct') return `${v > 0 ? '+' : v < 0 ? '−' : ''}${Math.abs(v).toFixed(1)}%`;
  return `${Math.round(v * 100)}%`;
}

/** 값 → 0~4단계. 값이 있는 구들의 순위(분위)로 나눠 5단계를 고르게 쓴다. */
function buildScale(districts, key) {
  const values = districts.map(d => metricValue(d, key)).filter(v => v != null).sort((a, b) => a - b);
  if (!values.length) return () => null;
  const lo = values[0];
  const hi = values[values.length - 1];
  if (lo === hi) return () => 2;
  return v => Math.min(4, Math.floor(((v - lo) / (hi - lo)) * 5));
}

function tileHref(d) {
  // 중구·서구처럼 여러 시에 같은 이름이 있어 시/도도 함께 넘긴다
  const q = new URLSearchParams({ district: d.district });
  if (view.region) q.set('region', view.region);
  return `/?${q.toString()}`;
}

function tileHtml(d, key, scale, placed) {
  const v = metricValue(d, key);
  const step = v == null ? null : scale(v);
  const cls = step == null ? 'is-none' : `is-s${step}`;
  const pos = placed ? ` style="grid-column:${placed[0]};grid-row:${placed[1]}"` : '';
  const label = `${d.district} ${fmtMetric(key, v)}, 급매 ${d.total}건`;
  return `<a class="cx-tile ${cls}" href="${tileHref(d)}"${pos} aria-label="${escHtml(label)}">
    <span class="cx-tile-name">${escHtml(d.district)}</span>
    <span class="cx-tile-val">${escHtml(fmtMetric(key, v))}</span></a>`;
}

function renderTiles(data, key) {
  const scale = buildScale(data.districts, key);
  const isSeoul = data.region === DEFAULT_REGION;
  let list = data.districts;
  if (isSeoul) {
    list = data.districts
      .filter(d => SEOUL_LAYOUT[d.district])
      .sort((a, b) => SEOUL_LAYOUT[a.district][1] - SEOUL_LAYOUT[b.district][1] || SEOUL_LAYOUT[a.district][0] - SEOUL_LAYOUT[b.district][0]);
    return `<div class="cx-tiles is-seoul">${list.map(d => tileHtml(d, key, scale, SEOUL_LAYOUT[d.district])).join('')}</div>`;
  }
  list = [...data.districts].sort((a, b) => a.district.localeCompare(b.district, 'ko'));
  return `<div class="cx-tiles is-grid">${list.map(d => tileHtml(d, key, scale, null)).join('')}</div>`;
}

function renderTop(data, key) {
  const ranked = data.districts
    .filter(d => metricValue(d, key) != null && d.total > 0)
    .sort((a, b) => metricValue(b, key) - metricValue(a, key) || b.total - a.total || a.district.localeCompare(b.district, 'ko'))
    .slice(0, 5);
  const label = data.metric_defs.find(m => m.key === key)?.label || '';
  const body = ranked.length
    ? `<ol class="cx-top-list">${ranked.map((d, i) => `<li><a href="${tileHref(d)}">
        <span><span class="cx-top-rank">${i + 1}</span>${escHtml(d.district)}</span>
        <b>${escHtml(fmtMetric(key, metricValue(d, key)))}</b></a></li>`).join('')}</ol>`
    : '<p class="cx-empty">표시할 데이터가 없습니다.</p>';
  return `<aside class="cx-panel" aria-label="상위 5개 지역"><h2 class="cx-panel-title">${escHtml(label)} 상위 5</h2>${body}</aside>`;
}

function render() {
  const data = view.data;
  const key = view.metric;
  const hasData = data.districts.some(d => d.total > 0);
  titleEl.textContent = `${data.region} 급매 히트맵`;
  document.title = `${data.region} 급매 히트맵 · 부동산 급매 알리미`;

  const regionOptions = [...data.regions]
    .sort((a, b) => a.localeCompare(b, 'ko'))
    .map(r => `<option value="${escHtml(r)}"${r === data.region ? ' selected' : ''}>${escHtml(r)}</option>`)
    .join('');
  const metricButtons = data.metric_defs
    .map(m => `<button type="button" data-metric="${escHtml(m.key)}" aria-pressed="${m.key === key}">${escHtml(m.label)}</button>`)
    .join('');

  root.innerHTML = `
    <div class="cx-toolbar">
      <div class="cx-seg" role="group" aria-label="지표 선택">${metricButtons}</div>
      <label class="cx-field">지역 <select id="heatmap-region">${regionOptions}</select></label>
    </div>
    ${hasData ? '' : '<p class="cx-empty">아직 수집된 매물이 없는 지역입니다.</p>'}
    <div class="cx-heat-layout">
      <section class="cx-panel" aria-label="${escHtml(data.region)} 히트맵">
        ${renderTiles(data, key)}
        <div class="cx-scale" aria-hidden="true">낮음 <i class="is-s0"></i><i class="is-s1"></i><i class="is-s2"></i><i class="is-s3"></i><i class="is-s4"></i> 높음</div>
      </section>
      ${renderTop(data, key)}
    </div>`;

  root.querySelectorAll('[data-metric]').forEach(btn => btn.addEventListener('click', () => {
    view.metric = btn.dataset.metric;
    render();
    root.querySelector(`[data-metric="${view.metric}"]`)?.focus();
  }));
  document.getElementById('heatmap-region').addEventListener('change', async e => {
    await load(e.target.value);
    document.getElementById('heatmap-region')?.focus();
  });
}

async function load(region) {
  view.region = region;
  try {
    const res = await fetch(`/api/heatmap?region=${encodeURIComponent(region)}`);
    if (res.status === 404 && region !== DEFAULT_REGION) { await load(DEFAULT_REGION); return; }
    if (!res.ok) throw new Error(`API error ${res.status}`);
    view.data = await res.json();
    view.region = view.data.region;
    try { history.replaceState(null, '', `?region=${encodeURIComponent(view.region)}`); } catch (e) { /* ignore */ }
    render();
  } catch (err) {
    root.innerHTML = `<p class="cx-empty">불러오지 못했습니다: ${escHtml(err.message)}</p>`;
  }
}

load(view.region);
