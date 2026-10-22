// 사라진 매물 페이지 — /api/disappeared, /api/disappeared/fast-complexes
(function () {
  'use strict';

  const esc = (value) => String(value == null ? '' : value)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const TAG_CLASS = {
    '진짜 급매였음': 'tr-tag-bargain',
    '인하 후 소진': 'tr-tag-down',
    '철회 후 재등록': 'tr-tag-back',
    '급매': 'tr-tag-plain',
  };
  const PERIOD_LABEL = { 1: '오늘', 7: '7일', 30: '30일' };

  async function getJson(path) {
    const res = await fetch(path);
    if (!res.ok) throw new Error('API error ' + res.status);
    return res.json();
  }

  function dateLabel(iso) {
    const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(iso || '');
    return m ? `${Number(m[2])}/${Number(m[3])}` : '—';
  }

  function renderList(data) {
    const list = document.getElementById('dis-list');
    document.getElementById('dis-summary').textContent =
      `${PERIOD_LABEL[data.days] || data.days + '일'} · ${data.total}건` +
      (data.bargain_total ? ` (진짜 급매 ${data.bargain_total}건)` : '');
    if (!data.items.length) {
      list.innerHTML = '<div class="tr-empty">이 기간에 사라진 매물 기록이 없습니다. 기록은 배포일부터 쌓입니다.</div>';
      return;
    }
    list.innerHTML = data.items.map((item) => {
      const meta = [item.area ? '면적 ' + item.area : '', item.floor ? '층 ' + item.floor : '', item.trade_type]
        .filter(Boolean).map(esc).join(' · ');
      const listed = item.days_listed == null ? '' : `게시 ${item.days_listed}일`;
      const name = item.naver_url
        ? `<a href="${esc(item.naver_url)}" target="_blank" rel="noopener noreferrer">${esc(item.building_name || '—')}</a>`
        : esc(item.building_name || '—');
      return `
      <article class="tr-row">
        <div class="tr-row-main">
          <div class="tr-row-name">${name} <span class="tr-muted">${esc(item.district)}</span></div>
          <div class="tr-row-meta">${meta}</div>
        </div>
        <div class="tr-row-side">
          <div class="tr-row-price">${esc(item.price || '—')}</div>
          <div class="tr-row-when tr-muted">${dateLabel(item.disappeared_at)} 사라짐${listed ? ' · ' + listed : ''}</div>
        </div>
        <span class="tr-tag ${TAG_CLASS[item.tag] || 'tr-tag-plain'}">${esc(item.tag)}</span>
      </article>`;
    }).join('');
  }

  function renderFast(data) {
    const box = document.getElementById('dis-fast');
    if (!data.items.length) {
      box.innerHTML = '<div class="tr-muted">비교할 지난 크롤 데이터가 아직 부족합니다.</div>';
      return;
    }
    box.innerHTML = data.items.map((item) => {
      const pct = Math.round(item.drop_ratio * 100);
      return `
      <div class="tr-bar-row">
        <div class="tr-bar-label"><span>${esc(item.building_name)} <span class="tr-muted">${esc(item.district)}</span></span>
          <span>${item.prev_count} → ${item.count}건 · −${pct}%</span></div>
        <div class="tr-bar" role="img" aria-label="${esc(item.building_name)} 급매 ${pct}% 감소"><span style="width:${Math.max(4, pct)}%"></span></div>
      </div>`;
    }).join('');
  }

  async function load(days) {
    document.querySelectorAll('.tr-period').forEach((btn) => {
      btn.setAttribute('aria-pressed', String(Number(btn.dataset.days) === days));
    });
    const list = document.getElementById('dis-list');
    list.innerHTML = '<div class="tr-muted">불러오는 중…</div>';
    try {
      renderList(await getJson('/api/disappeared?days=' + days + '&limit=100'));
    } catch (e) {
      list.innerHTML = '<div class="tr-empty">불러오지 못했습니다. 잠시 후 다시 시도하세요.</div>';
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('dis-periods').addEventListener('click', (e) => {
      const btn = e.target.closest('.tr-period');
      if (btn) load(Number(btn.dataset.days));
    });
    load(7);
    getJson('/api/disappeared/fast-complexes?days=7').then(renderFast).catch(() => {
      document.getElementById('dis-fast').innerHTML = '<div class="tr-muted">불러오지 못했습니다.</div>';
    });
  });
})();
