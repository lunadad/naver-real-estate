// 매물 가격 이력 모달 — /api/listings/<article_no>/history
(function () {
  'use strict';

  const esc = (value) => String(value == null ? '' : value)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  let overlay = null;
  let opener = null;

  function ensureModal() {
    if (overlay) return overlay;
    overlay = document.createElement('div');
    overlay.id = 'ph-overlay';
    overlay.className = 'hidden';
    overlay.innerHTML = `
      <div id="ph-modal" role="dialog" aria-modal="true" aria-labelledby="ph-title" tabindex="-1">
        <div class="ph-head">
          <div id="ph-title" class="ph-title">가격 이력</div>
          <button type="button" id="ph-close" class="ph-close" aria-label="가격 이력 닫기">✕</button>
        </div>
        <div id="ph-body" class="ph-body" aria-live="polite"></div>
      </div>`;
    document.body.appendChild(overlay);
    overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });
    overlay.querySelector('#ph-close').addEventListener('click', close);
    overlay.addEventListener('keydown', onKeydown);
    return overlay;
  }

  function focusables() {
    return Array.from(overlay.querySelectorAll('button, a[href], [tabindex]:not([tabindex="-1"])'))
      .filter((el) => !el.disabled && el.offsetParent !== null);
  }

  function onKeydown(e) {
    if (e.key === 'Escape') {
      e.stopPropagation();
      close();
    } else if (e.key === 'Tab') {
      const items = focusables();
      if (!items.length) { e.preventDefault(); return; }
      const first = items[0];
      const last = items[items.length - 1];
      if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
      else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
    }
  }

  function close() {
    if (!overlay || overlay.classList.contains('hidden')) return;
    overlay.classList.add('hidden');
    if (opener && document.contains(opener)) opener.focus();
    opener = null;
  }

  function day(iso) {
    const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso || '');
    return m ? `${m[1]}.${m[2]}.${m[3]}` : '—';
  }

  function stepChart(events) {
    const points = events
      .map((e) => ({ t: Date.parse(e.observed_at), v: e.price_sort_value, label: e.price }))
      .filter((p) => Number.isFinite(p.t) && p.v != null);
    if (!points.length) return '<div class="ph-muted">그래프를 그릴 가격 데이터가 없습니다.</div>';
    const W = 320, H = 120, PAD_X = 14, PAD_Y = 16;
    const values = points.map((p) => p.v);
    const lo = Math.min(...values), hi = Math.max(...values);
    const tMin = points[0].t;
    const tMax = Math.max(points[points.length - 1].t, tMin + 86400000);
    const x = (t) => PAD_X + ((t - tMin) / (tMax - tMin)) * (W - PAD_X * 2);
    const y = (v) => (hi === lo ? H / 2 : PAD_Y + (1 - (v - lo) / (hi - lo)) * (H - PAD_Y * 2));
    let d = `M ${x(points[0].t).toFixed(1)} ${y(points[0].v).toFixed(1)}`;
    for (let i = 1; i < points.length; i += 1) {
      d += ` H ${x(points[i].t).toFixed(1)} V ${y(points[i].v).toFixed(1)}`;
    }
    d += ` H ${(W - PAD_X).toFixed(1)}`;
    const dots = points.map((p) =>
      `<circle cx="${x(p.t).toFixed(1)}" cy="${y(p.v).toFixed(1)}" r="3.5" class="ph-dot"></circle>`).join('');
    const hiLabel = points.find((p) => p.v === hi).label;
    const loLabel = points.find((p) => p.v === lo).label;
    const summary = points.map((p) => `${day(new Date(p.t).toISOString())} ${p.label}`).join(', ');
    return `
      <svg class="ph-chart" viewBox="0 0 ${W} ${H}" role="img" aria-label="가격 변화 계단 그래프: ${esc(summary)}">
        <path d="${d}" class="ph-line" fill="none"></path>${dots}
      </svg>
      <div class="ph-axis"><span>${esc(day(events[0].observed_at))}</span>
        <span>${hi === lo ? esc(hiLabel) : `최고 ${esc(hiLabel)} · 최저 ${esc(loLabel)}`}</span><span>지금</span></div>`;
  }

  function timeline(events) {
    return '<ol class="ph-timeline">' + events.slice().reverse().map((e, idx, arr) => {
      const first = idx === arr.length - 1;
      let change = '<span class="ph-chip">최초 발견</span>';
      if (!first && e.change_pct != null) {
        const down = e.change_pct < 0;
        change = `<span class="ph-chip ${down ? 'ph-down' : 'ph-up'}">${down ? '▼' : '▲'} ${Math.abs(e.change_pct).toFixed(1)}%</span>`;
      } else if (!first) {
        change = '<span class="ph-chip">변경</span>';
      }
      return `<li><span class="ph-date">${esc(day(e.observed_at))}</span>
        <span class="ph-price">${esc(e.price || '—')}</span>${change}</li>`;
    }).join('') + '</ol>';
  }

  function clues(insight) {
    const items = [];
    if (insight.days_since_first_seen != null) items.push(`처음 발견 후 <b>${insight.days_since_first_seen}일</b> 경과`);
    items.push(`가격 인하 <b>${insight.cut_count}회</b>`);
    if (insight.total_change_pct != null && insight.total_change_pct !== 0) {
      items.push(`누적 변화 <b>${insight.total_change_pct > 0 ? '+' : ''}${insight.total_change_pct.toFixed(1)}%</b>`);
    }
    if (insight.days_since_last_change != null) items.push(`마지막 변경 후 <b>${insight.days_since_last_change}일</b>`);
    if (insight.bargain_discount_pct != null) items.push(`단지 중앙값 대비 <b>−${insight.bargain_discount_pct}%</b> (진짜 급매)`);
    return `<div class="ph-clues"><div class="ph-clues-title">협상 단서</div>
      <ul>${items.map((i) => `<li>${i}</li>`).join('')}</ul>
      <p class="ph-muted">기준은 우리가 처음 발견한 날이며 실제 등록일이 아닙니다.</p></div>`;
  }

  async function open(articleNo, trigger) {
    ensureModal();
    opener = trigger || document.activeElement;
    const body = overlay.querySelector('#ph-body');
    overlay.querySelector('#ph-title').textContent = '가격 이력';
    body.innerHTML = '<div class="ph-muted">불러오는 중…</div>';
    overlay.classList.remove('hidden');
    overlay.querySelector('#ph-close').focus();
    try {
      const res = await fetch('/api/listings/' + encodeURIComponent(articleNo) + '/history');
      if (res.status === 404) {
        body.innerHTML = '<div class="ph-muted">아직 가격 이력이 없습니다. 이력은 배포일부터 쌓입니다.</div>';
        return;
      }
      if (!res.ok) throw new Error('API error ' + res.status);
      const data = await res.json();
      const l = data.listing || {};
      overlay.querySelector('#ph-title').textContent =
        [l.building_name, l.area, l.floor ? l.floor + '층' : ''].filter(Boolean).join(' · ') || '가격 이력';
      body.innerHTML = stepChart(data.events) + timeline(data.events) + clues(data.insight);
    } catch (e) {
      body.innerHTML = '<div class="ph-muted">가격 이력을 불러오지 못했습니다.</div>';
    }
  }

  window.openPriceHistory = open;
})();
