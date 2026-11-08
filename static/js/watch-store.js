// 관심 매물(찜)·비교 목록 — 이 브라우저의 localStorage에만 저장한다 (로그인 없음).
(function () {
  'use strict';

  const WATCH_KEY = 'watchlist.v1';
  const COMPARE_KEY = 'compare.v1';
  const MAX_WATCH = 50;
  const MAX_COMPARE = 3;

  function read(key) {
    try {
      const value = JSON.parse(localStorage.getItem(key) || '[]');
      return Array.isArray(value) ? value : [];
    } catch (e) {
      return [];
    }
  }

  function write(key, value) {
    try { localStorage.setItem(key, JSON.stringify(value)); } catch (e) { /* 저장 불가 브라우저: 이번 화면에서만 유지 */ }
    window.dispatchEvent(new CustomEvent('watchstore:change', { detail: { key } }));
  }

  // listing: /api/listings 응답의 매물 한 건
  function snapshot(listing) {
    return {
      article_no: String(listing.article_no),
      saved_at: new Date().toISOString().slice(0, 19),
      saved_price: listing.price || '',
      saved_price_value: listing.price_sort_value || null,
      building_name: listing.building_name || '',
      district: listing.district || '',
      area: listing.area || '',
      floor: listing.floor || '',
      trade_type: listing.trade_type || '',
      naver_url: listing.naver_url || '',
    };
  }

  const WatchStore = {
    MAX_WATCH,
    MAX_COMPARE,
    list: () => read(WATCH_KEY),
    has: (articleNo) => read(WATCH_KEY).some((item) => item.article_no === String(articleNo)),
    // 반환: true = 찜함, false = 해제, null = 한도 초과
    toggle(listing) {
      const items = read(WATCH_KEY);
      const no = String(listing.article_no);
      const idx = items.findIndex((item) => item.article_no === no);
      if (idx >= 0) {
        items.splice(idx, 1);
        write(WATCH_KEY, items);
        return false;
      }
      if (items.length >= MAX_WATCH) return null;
      items.unshift(snapshot(listing));
      write(WATCH_KEY, items);
      return true;
    },
    remove(articleNo) {
      write(WATCH_KEY, read(WATCH_KEY).filter((item) => item.article_no !== String(articleNo)));
    },
    compareList: () => read(COMPARE_KEY).map(String).slice(0, MAX_COMPARE),
    inCompare: (articleNo) => read(COMPARE_KEY).map(String).includes(String(articleNo)),
    toggleCompare(articleNo) {
      const items = read(COMPARE_KEY).map(String);
      const no = String(articleNo);
      const idx = items.indexOf(no);
      if (idx >= 0) {
        items.splice(idx, 1);
        write(COMPARE_KEY, items);
        return false;
      }
      if (items.length >= MAX_COMPARE) return null;
      items.push(no);
      write(COMPARE_KEY, items);
      return true;
    },
    clearCompare: () => write(COMPARE_KEY, []),
  };

  window.WatchStore = WatchStore;
})();
