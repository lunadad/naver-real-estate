// app.js 특성화 테스트 (리팩토링 안전망)
// 리팩토링 전 관찰한 실제 동작(fixtures/app_js_baseline.json)과
// 현재 코드의 동작이 완전히 일치하는지 검증한다.
// 실행: node --test tests/*.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import { runScenarios } from './helpers/scenarios.mjs';

const baselinePath = path.join(
  path.dirname(fileURLToPath(import.meta.url)), 'fixtures', 'app_js_baseline.json',
);
const baseline = JSON.parse(readFileSync(baselinePath, 'utf8'));

// vm 컨텍스트에서 생성된 배열/객체는 realm이 달라 프로토타입 비교에 걸리므로 JSON으로 정규화
const actual = JSON.parse(JSON.stringify(runScenarios()));

test('시나리오 키 집합이 베이스라인과 동일하다', () => {
  assert.deepEqual(Object.keys(actual).sort(), Object.keys(baseline).sort());
});

for (const key of Object.keys(baseline)) {
  test(`특성화: ${key}`, () => {
    assert.deepEqual(actual[key], baseline[key]);
  });
}
