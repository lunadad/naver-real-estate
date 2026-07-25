// 카카오맵 줌 티어(도 → 시·군·구 → 개별 매물) 테스트
// 시·군·구 마커는 지역명+건수 라벨 버블이며 색상 공식은 유지된다:
//   color = total<20 → #3fb950, <50 → #d29922, 그 외 → #f85149
import test from 'node:test';
import assert from 'node:assert/strict';
import { loadApp } from './helpers/load_app.mjs';

const REGIONS_FIXTURE = [
  {
    region: '서울특별시',
    districts: [
      { name: '강남구', lat: 37.5172, lng: 127.0473 },
      { name: '마포구', lat: 37.5663, lng: 126.9014 },
    ],
  },
  { region: '경기도', districts: [{ name: '수원시', lat: 37.2636, lng: 127.0286 }] },
];

const MAP_LISTINGS_FIXTURE = [
  { id: 1, article_no: 'A1', building_name: '역삼타워', price: '3억', latitude: 37.50, longitude: 127.03, property_type: '상가', trade_type: '매매', tags: '["급매"]', naver_url: 'https://naver.example/a1' },
  { id: 2, article_no: 'A2', building_name: '역삼타워', price: '5억', latitude: 37.50, longitude: 127.03, property_type: '업무', trade_type: '매매', tags: '["가격인하"]', naver_url: '' },
];

function makeKakaoStub() {
  const overlays = [];
  const markers = [];
  const listeners = []; // { target, event, handler }
  class LatLng {
    constructor(lat, lng) { this.lat = lat; this.lng = lng; }
  }
  class CustomOverlay {
    constructor(opts) { this.opts = opts; this.map = null; overlays.push(this); }
    setMap(m) { this.map = m; }
  }
  class Marker {
    constructor(opts) { this.opts = opts; this.map = null; markers.push(this); }
    setMap(m) { this.map = m; }
  }
  class KakaoMap {
    constructor(el, opts) {
      this.el = el; this.opts = opts;
      this.level = opts.level;
      this.panTarget = null;
    }
    getLevel() { return this.level; }
    setLevel(l) { this.level = l; }
    panTo(pos) { this.panTarget = pos; }
    relayout() {}
    getBounds() {
      return {
        getSouthWest: () => ({ getLat: () => 37.4, getLng: () => 126.9 }),
        getNorthEast: () => ({ getLat: () => 37.6, getLng: () => 127.1 }),
      };
    }
  }
  class MarkerClusterer {
    constructor(opts) { this.opts = opts; this.markers = []; }
    addMarkers(ms) { this.markers.push(...ms); }
    clear() { this.markers = []; }
  }
  const kakao = {
    maps: {
      LatLng, CustomOverlay, Marker, MarkerClusterer, Map: KakaoMap,
      load: cb => cb(),
      event: { addListener: (target, event, handler) => listeners.push({ target, event, handler }) },
    },
  };
  return { overlays, markers, listeners, kakao };
}

function setupMapEnv() {
  const stub = makeKakaoStub();
  const fetchLog = [];

  const fetchStub = (path) => {
    fetchLog.push(path);
    if (path.startsWith('/api/regions')) {
      return Promise.resolve({ ok: true, json: () => Promise.resolve(REGIONS_FIXTURE) });
    }
    if (path.startsWith('/api/map-listings')) {
      return Promise.resolve({ ok: true, json: () => Promise.resolve({ listings: MAP_LISTINGS_FIXTURE, count: MAP_LISTINGS_FIXTURE.length }) });
    }
    return Promise.resolve({ ok: true, json: () => Promise.resolve({}) });
  };

  const app = loadApp({ fetch: fetchStub, kakao: stub.kakao });
  return { app, ...stub, fetchLog };
}

const tick = () => new Promise(r => setTimeout(r, 0));
const byClass = (overlays, cls) => overlays.filter(o => o.opts.content.className === cls);

test('getMapTier: 줌 레벨 → 티어 매핑', () => {
  const { app } = setupMapEnv();
  assert.equal(app.call('getMapTier(1)'), 'listing');
  assert.equal(app.call('getMapTier(7)'), 'listing');
  assert.equal(app.call('getMapTier(8)'), 'district');
  assert.equal(app.call('getMapTier(10)'), 'district');
  assert.equal(app.call('getMapTier(11)'), 'region');
  assert.equal(app.call('getMapTier(13)'), 'region');
});

test('district 티어: 라벨 버블(지역명+건수)·색상·클릭 필터가 동작한다', async () => {
  const { app, overlays } = setupMapEnv();
  app.call(`state.map = { fake: true }; state.mapTier = 'district';`);
  app.call(`renderMapMarkers([
    { district: '강남구', region: '서울특별시', display_name: '서울 강남구', total: 60 },
    { district: '수원시', region: '경기도', total: 10 },
  ])`);
  await tick();

  const districtMarkers = byClass(overlays, 'district-marker');
  assert.equal(districtMarkers.length, 2);
  const gangnam = districtMarkers.find(o => o.opts.position.lat === 37.5172);
  const suwon = districtMarkers.find(o => o.opts.position.lat === 37.2636);

  // 라벨 버블: 지역명과 건수가 항상 보인다
  assert.ok(gangnam.opts.content.innerHTML.includes('강남구'));
  assert.ok(gangnam.opts.content.innerHTML.includes('60'));
  assert.equal(gangnam.opts.content.style.borderColor, '#f85149');
  assert.ok(suwon.opts.content.innerHTML.includes('수원시'));
  assert.equal(suwon.opts.content.style.borderColor, '#3fb950');
  // 급매 많은 지역은 폰트가 커진다 (11px + ratio*3)
  assert.equal(gangnam.opts.content.style.fontSize, '14px');
  assert.equal(suwon.opts.content.style.fontSize, '12px');
  assert.ok(gangnam.opts.content.title.includes('서울 강남구'));
  assert.ok(gangnam.map, 'district 티어에서는 구 마커가 지도에 붙는다');

  // region 마커는 만들어지되 숨김 상태
  const regionMarkers = byClass(overlays, 'region-marker');
  assert.ok(regionMarkers.length >= 1);
  assert.equal(regionMarkers[0].map, null);

  // 클릭 → 지역 필터 + 말풍선
  gangnam.opts.content._listeners.click();
  await tick();
  assert.equal(app.call('state.filters.district'), '강남구');
  assert.equal(byClass(overlays, 'map-info-bubble').length, 1);
});

test('region 티어: 도 단위 합산 마커가 표시되고 클릭하면 확대된다', async () => {
  const { app, overlays } = setupMapEnv();
  app.call(`state.regionStats = [
    { district: '강남구', region: '서울특별시', total: 60 },
    { district: '마포구', region: '서울특별시', total: 40 },
    { district: '수원시', region: '경기도', total: 10 },
  ]`);
  app.call('initMap()'); // level 13 → region 티어
  await tick();

  assert.equal(app.call('state.mapTier'), 'region');
  const regionMarkers = byClass(overlays, 'region-marker');
  assert.equal(regionMarkers.length, 2);

  const seoul = regionMarkers.find(o => o.opts.content.innerHTML.includes('서울'));
  assert.ok(seoul.opts.content.innerHTML.includes('100'), '서울 합산 60+40=100');
  // 중심 = 두 구 좌표 평균
  assert.ok(Math.abs(seoul.opts.position.lat - (37.5172 + 37.5663) / 2) < 1e-9);
  assert.ok(seoul.map, 'region 티어에서는 도 마커가 보인다');

  // district 마커는 숨김
  const districtMarkers = byClass(overlays, 'district-marker');
  assert.ok(districtMarkers.every(o => o.map === null));

  // 도 마커 클릭 → 구 티어 레벨로 확대 + 중심 이동
  seoul.opts.content._listeners.click();
  assert.equal(app.call('state.map.level'), 9);
  assert.ok(app.call('state.map.panTarget.lat') > 37);
});

test('listing 티어: 화면 영역 매물을 클러스터러에 렌더하고 말풍선을 띄운다', async () => {
  const { app, markers, overlays, fetchLog } = setupMapEnv();
  app.call('state.regionStats = [{ district: "강남구", region: "서울특별시", total: 60 }]');
  app.call('initMap()');
  await tick();

  // 매물 티어로 줌인
  app.call('state.map.setLevel(6); applyMapTier();');
  assert.equal(app.call('state.mapTier'), 'listing');
  await app.call('refreshMapListings()'); // 디바운스 우회 직접 호출
  await tick();

  const req = fetchLog.find(p => p.startsWith('/api/map-listings'));
  assert.ok(req.includes('min_lat=37.4') && req.includes('max_lng=127.1'), 'bounds 파라미터 포함');
  assert.equal(markers.length, 2, '매물 마커 2개 생성');
  assert.equal(app.call('state.mapClusterer.markers.length'), 2, '클러스터러에 등록');

  // 매물 말풍선 검증 (이름·가격·네이버 링크)
  app.call(`showListingBubble(${JSON.stringify(MAP_LISTINGS_FIXTURE[0])})`);
  const bubble = overlays.filter(o => String(o.opts.content.className).includes('listing-bubble')).pop();
  assert.ok(bubble.opts.content.innerHTML.includes('역삼타워'));
  assert.ok(bubble.opts.content.innerHTML.includes('3억'));
  assert.ok(bubble.opts.content.innerHTML.includes('naver.example/a1'));

  // 필터 파라미터 반영 확인
  app.call(`state.filters.property_type = '상가'; state.filters.price_down_only = true;`);
  await app.call('refreshMapListings()');
  const req2 = fetchLog.filter(p => p.startsWith('/api/map-listings')).pop();
  assert.ok(req2.includes('property_type=') && req2.includes('price_down_only=true'));

  // 티어 이탈 시 매물 마커 제거
  app.call('state.map.setLevel(13); applyMapTier();');
  assert.equal(app.call('state.mapTier'), 'region');
  assert.equal(app.call('state.mapClusterer.markers.length'), 0, '클러스터러 비움');
});

test('renderMapMarkers: 재호출 시 기존 마커 제거 + /api/regions 1회 캐시', async () => {
  const { app, overlays, fetchLog } = setupMapEnv();
  app.call(`state.map = { fake: true }; state.mapTier = 'district';`);
  app.call(`renderMapMarkers([{ district: '강남구', region: '서울특별시', total: 30 }])`);
  await tick();
  const first = byClass(overlays, 'district-marker')[0];
  assert.ok(first.map);

  app.call(`renderMapMarkers([{ district: '수원시', region: '경기도', total: 5 }])`);
  await tick();

  assert.equal(first.map, null, '이전 마커 제거됨');
  assert.equal(fetchLog.filter(p => p.startsWith('/api/regions')).length, 1, '좌표 캐시');
  assert.deepEqual(JSON.parse(JSON.stringify(app.call('Object.keys(state.mapMarkers)'))), ['수원시']);
});

test('state.map이 없으면 renderMapMarkers는 아무것도 하지 않는다', async () => {
  const { app, overlays, fetchLog } = setupMapEnv();
  app.call(`renderMapMarkers([{ district: '강남구', region: '서울특별시', total: 30 }])`);
  await tick();
  assert.equal(overlays.length, 0);
  assert.equal(fetchLog.length, 0);
});

test('initMap: SDK가 없으면 지도 영역에 설정 안내를 표시한다', () => {
  const app = loadApp(); // kakao 미정의
  app.call('initMap()');
  assert.ok(app.registry.get('map').innerHTML.includes('카카오맵을 불러올 수 없습니다'));
  assert.equal(app.call('state.map'), null);
});
