// app.js를 브라우저 없이 로드하기 위한 최소 스텁 환경.
// 특성화 테스트 전용 — index.html에 실제로 존재하는 ID만 등록해
// 브라우저에서의 getElementById 결과(null 여부)를 그대로 재현한다.
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import vm from 'node:vm';

const APP_JS_PATH = path.join(
  path.dirname(fileURLToPath(import.meta.url)), '..', '..', 'static', 'js', 'app.js',
);

// templates/index.html에 정적으로 존재하는 ID 목록 (여기 없는 ID는 null 반환)
const STATIC_IDS = [
  'app-header', 'search-input', 'search-clear', 'trade-filter', 'type-filter',
  'theme-btn', 'app-body', 'sidebar-open-btn', 'mobile-dim', 'sidebar',
  'sidebar-toggle', 'alert-section-title', 'alert-section-toggle', 'alert-toggle-icon',
  'alert-panel-body', 'notification-status', 'alert-keyword', 'alert-min-area',
  'alert-max-area', 'alert-trade-scope', 'alert-min-price-drop', 'alert-current-filters',
  'btn-alert-enable', 'btn-alert-save', 'alert-rules-list', 'list-increasing',
  'list-decreasing', 'list-price-down', 'list-region-stats', 'main-content',
  'hero-panel', 'hero-filter-summary', 'hero-crawl-summary', 'hero-insight-banner',
  'hero-insight-text', 'hero-insight-subtext', 'hero-daily-current', 'hero-daily-change',
  'hero-daily-chart', 'hero-daily-empty', 'hero-daily-labels', 'hero-alert-preview',
  'btn-mobile-sidebar', 'btn-map-toggle', 'btn-hero-notif', 'btn-hero-alert',
  'map-wrap', 'map', 'map-overlay-bar', 'region-badge', 'region-badge-name',
  'region-badge-clear', 'map-legend', 'stats-bar', 'stat-total', 'stat-shop',
  'stat-office', 'stat-land', 'stat-price-down', 'listings-toolbar',
  'listings-summary', 'sort-select', 'listings-wrap', 'listings-grid', 'pagination',
  'btn-prev', 'page-info', 'btn-next', 'mobile-quickbar', 'btn-mobile-filter-fab',
  'btn-mobile-map-fab', 'btn-mobile-alert-fab', 'status-bar', 'info-last-crawl',
  'info-next-crawl', 'demo-badge', 'modal-overlay', 'modal', 'modal-header',
  'modal-title', 'modal-close', 'modal-body',
];

function makeEl(id) {
  const classes = new Set();
  const listeners = {};
  return {
    id,
    innerHTML: '',
    textContent: '',
    value: '',
    disabled: false,
    dataset: {},
    style: {},
    title: '',
    className: '',
    classList: {
      add: (...cs) => cs.forEach(c => classes.add(c)),
      remove: (...cs) => cs.forEach(c => classes.delete(c)),
      toggle: (c, force) => {
        const on = force === undefined ? !classes.has(c) : Boolean(force);
        if (on) classes.add(c); else classes.delete(c);
        return on;
      },
      contains: c => classes.has(c),
    },
    addEventListener(evt, fn) { listeners[evt] = fn; },
    setAttribute() {},
    querySelector: () => makeEl('__query-result'),
    _classes: classes,
    _listeners: listeners,
  };
}

export function loadApp(overrides = {}) {
  const registry = new Map(STATIC_IDS.map(id => [id, makeEl(id)]));
  const storage = new Map();

  const localStorage = {
    getItem: k => (storage.has(k) ? storage.get(k) : null),
    setItem: (k, v) => storage.set(k, String(v)),
    removeItem: k => storage.delete(k),
  };

  const documentStub = {
    getElementById: id => registry.get(id) || null,
    querySelector: () => null,
    querySelectorAll: () => [],
    addEventListener() {},
    createElement: tag => makeEl(`__created-${tag}`),
    documentElement: makeEl('__html'),
    body: { appendChild() {}, classList: makeEl('__body').classList },
  };

  const windowStub = {
    matchMedia: () => ({ matches: false }),
    crypto: { randomUUID: () => 'test-uuid' },
    isSecureContext: true,
    atob: s => Buffer.from(s, 'base64').toString('binary'),
    setInterval: () => 0,
    clearInterval: () => {},
    addEventListener() {},
    open() {},
  };

  const sandbox = {
    console,
    setTimeout,
    clearTimeout,
    URLSearchParams,
    Uint8Array,
    localStorage,
    document: documentStub,
    window: windowStub,
    navigator: { userAgent: 'test' },
    location: { hostname: 'localhost', protocol: 'http:', href: 'http://localhost/', pathname: '/', search: '' },
    fetch: () => Promise.reject(new Error('fetch disabled in tests')),
    ...overrides,
  };
  sandbox.globalThis = sandbox;

  const ctx = vm.createContext(sandbox);
  vm.runInContext(readFileSync(APP_JS_PATH, 'utf8'), ctx, { filename: 'app.js' });

  // app.js 최상위 선언(function/const)에 접근하는 헬퍼
  const get = name => vm.runInContext(name, ctx);
  const call = (expr) => vm.runInContext(expr, ctx);
  return { ctx, get, call, registry, storage };
}
