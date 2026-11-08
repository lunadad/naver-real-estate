// 관심 매물 페이지 — localStorage 찜 목록 + /api/watchlist/status
(function () {
  'use strict';

  const esc = (value) => String(value == null ? '' : value)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const CHANGE_KINDS = ['price_down', 'price_up', 'gone'];
  let view = 'changes';
  let lastData = null;

  function manwon(value) {
    const abs = Math.abs(value);
    const eok = Math.floor(abs / 10000);
    const rest = abs % 10000;
    const text = eok ? `${eok}억${rest ? ' ' + rest.toLocaleString() : ''}` : `${rest.toLocaleString()}만`;
    return (value < 0 ? '−' : '+') + text;
  }

  function changeTag(item) {
    const c = item.change || {};
    if (c.kind === 'price_down') return `<span class="tr-tag wl-tag-down">${esc(manwon(c.diff))} (${c.pct}%)</span>`;
    if (c.kind === 'price_up') return `<span class="tr-tag wl-tag-up">${esc(manwon(c.diff))} (+${c.pct}%)</span>`;
    if (c.kind === 'gone') return '<span class="tr-tag tr-tag-plain">사라짐</span>';
    if (item.status === 'unknown') return '<span class="tr-tag tr-tag-plain">정보 없음</span>';
    return '<span class="tr-tag tr-tag-bargain">변동 없음</span>';
  }

  function row(item, saved, { removable }) {
    const name = item.building_name || saved.building_name || '—';
    const url = item.naver_url || saved.naver_url;
    const link = url
      ? `<a href="${esc(url)}" target="_blank" rel="noopener noreferrer">${esc(name)}</a>`
      : esc(name);
    const meta = [item.area || saved.area, item.floor ? item.floor + '층' : saved.floor ? saved.floor + '층' : '', item.trade_type || saved.trade_type]
      .filter(Boolean).map(esc).join(' · ');
    const extra = [];
    if (item.median_gap_pct != null && item.median_gap_pct < 0) extra.push(`단지 중앙값보다 ${Math.abs(item.median_gap_pct)}% 낮음`);
    if (item.cut_count) extra.push(`인하 ${item.cut_count}회`);
    if (item.days_listed != null) extra.push(`${item.status === 'gone' ? '노출' : '발견 후'} ${item.days_listed}일`);
    const price = item.status === 'gone' ? (item.price || saved.saved_price) : (item.price || '—');
    const before = item.change && (item.change.kind === 'price_down' || item.change.kind === 'price_up')
      ? `<div class="tr-muted wl-before">찜할 때 ${esc(saved.saved_price)}</div>` : '';
    return `
      <article class="tr-row wl-row">
        <div class="tr-row-main">
          <div class="tr-row-name">${link} <span class="tr-muted">${esc(item.district || saved.district)}</span></div>
          <div class="tr-row-meta">${meta}</div>
          ${extra.length ? `<div class="tr-row-meta">${extra.map(esc).join(' · ')}</div>` : ''}
        </div>
        <div class="tr-row-side">
          <div class="tr-row-price">${esc(price || '—')}</div>
          ${before}
        </div>
        <div class="wl-row-actions">
          ${changeTag(item)}
          ${removable ? `<button type="button" class="wl-remove" data-article-no="${esc(item.article_no)}" aria-label="${esc(name)} 찜 해제">찜 해제</button>` : ''}
        </div>
      </article>`;
  }

  function render() {
    const list = document.getElementById('wl-list');
    const saved = window.WatchStore.list();
    const byNo = Object.fromEntries(saved.map((s) => [s.article_no, s]));
    const items = (lastData ? lastData.items : []).filter((i) => byNo[i.article_no]);
    const changed = items.filter((i) => CHANGE_KINDS.includes((i.change || {}).kind));
    document.getElementById('wl-change-count').textContent = changed.length;
    document.getElementById('wl-all-count').textContent = saved.length;
    document.querySelectorAll('#wl-tabs .tr-period').forEach((btn) => {
      btn.setAttribute('aria-pressed', String(btn.dataset.view === view));
    });
    document.getElementById('wl-summary').textContent = saved.length ? `${saved.length}개 · 변동 ${changed.length}건` : '';
    if (!saved.length) {
      list.innerHTML = '<div class="tr-empty">아직 찜한 매물이 없습니다. 급매 목록 카드의 ♡ 버튼으로 찜하세요.</div>';
      return;
    }
    const shown = view === 'changes' ? changed : items;
    if (!shown.length) {
      list.innerHTML = '<div class="tr-empty">찜한 뒤로 달라진 매물이 없습니다.</div>';
      return;
    }
    list.innerHTML = shown.map((item) => row(item, byNo[item.article_no], { removable: true })).join('');
  }

  function renderNew(data) {
    const box = document.getElementById('wl-new');
    if (!data.new_bargains.length) {
      box.innerHTML = '<div class="tr-muted">찜한 뒤 같은 단지에 새로 나온 진짜 급매가 없습니다.</div>';
      return;
    }
    box.innerHTML = data.new_bargains.map((item) => row(item, {}, { removable: false })).join('');
  }

  async function load() {
    const saved = window.WatchStore.list();
    if (!saved.length) {
      lastData = { items: [], new_bargains: [] };
      render();
      renderNew(lastData);
      return;
    }
    try {
      const res = await fetch('/api/watchlist/status', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ items: saved }),
      });
      if (!res.ok) throw new Error('API error ' + res.status);
      lastData = await res.json();
      render();
      renderNew(lastData);
    } catch (e) {
      document.getElementById('wl-list').innerHTML = '<div class="tr-empty">불러오지 못했습니다. 잠시 후 다시 시도하세요.</div>';
      document.getElementById('wl-new').innerHTML = '<div class="tr-muted">불러오지 못했습니다.</div>';
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    document.getElementById('wl-tabs').addEventListener('click', (e) => {
      const btn = e.target.closest('.tr-period');
      if (!btn) return;
      view = btn.dataset.view;
      render();
    });
    document.getElementById('wl-list').addEventListener('click', (e) => {
      const btn = e.target.closest('.wl-remove');
      if (!btn) return;
      window.WatchStore.remove(btn.dataset.articleNo);
      render();
    });
    load();
  });
})();
