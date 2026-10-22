"""금액·지역 조건 파싱과 표기 (표준 라이브러리만 사용 — Hermes 플러그인에서도 import한다)."""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional

REGIONS_PATH = Path(__file__).resolve().parent / "data" / "regions.json"


def parse_price(text: Optional[str]) -> Optional[int]:
    """"20억", "15억5천", "15억 5,000", "9000", "9,000만" → 만원 단위 정수."""
    raw = re.sub(r"[\s,]", "", str(text or "")).replace("만원", "").replace("만", "")
    if not raw:
        return None
    match = re.fullmatch(r"(?:(\d+(?:\.\d+)?)억)?(?:(\d+)천)?(\d+)?", raw)
    if not match or not any(match.groups()):
        return None
    eok, cheon, rest = match.groups()
    total = 0.0
    if eok:
        total += float(eok) * 10000
    if cheon:
        total += int(cheon) * 1000
    if rest:
        total += int(rest)
    return int(round(total))


def format_manwon(value: int) -> str:
    """200000 → "20억", 155000 → "15억 5,000", 9000 → "9,000만"."""
    eok, rest = divmod(int(value), 10000)
    if not eok:
        return f"{rest:,}만"
    return f"{eok}억" + (f" {rest:,}" if rest else "")


def parse_discount(text: Optional[str]) -> Optional[float]:
    """"15%" 또는 "15" → 0.15. 범위 밖이면 None."""
    match = re.fullmatch(r"(\d+(?:\.\d+)?)%?", str(text or "").strip())
    if not match:
        return None
    value = float(match.group(1)) / 100
    return value if 0 < value <= 0.4 else None


def load_districts(path: Path = REGIONS_PATH) -> List[Dict[str, str]]:
    regions = json.loads(Path(path).read_text(encoding="utf-8"))
    return [
        {"region": region, "district": district}
        for region, info in regions.items()
        for district in info.get("districts", {})
    ]


def match_region(text: str, districts: List[Dict[str, str]]) -> Optional[str]:
    """"부산", "부산광역시", "경기" 같은 입력을 시/도 이름으로 맞춘다 (2글자 이상)."""
    needle = (text or "").strip()
    if len(needle) < 2:
        return None
    for region in dict.fromkeys(d["region"] for d in districts):
        if region == needle or region.startswith(needle):
            return region
    return None


def match_district(text: str, districts: List[Dict[str, str]],
                   region: Optional[str] = None) -> Optional[Dict[str, str]]:
    """"송파구", "송파", "분당" 같은 입력을 구/시 하나로 맞춘다. region이 있으면 그 안에서만 찾고,
    같은 이름(중구 등)이 여러 시에 있으면 첫 정확 일치를 고른다."""
    needle = (text or "").strip()
    if not needle:
        return None
    if region:
        districts = [d for d in districts if d["region"] == region]
    exact = [d for d in districts if d["district"] == needle]
    if exact:
        return exact[0]
    partial = [d for d in districts if needle in d["district"]]
    return partial[0] if partial else None


def parse_deal_args(raw: str, districts: List[Dict[str, str]]) -> Dict:
    """"/deals 송파구 20억 15%" 인자 → {district, region, max_price, min_discount, unknown}."""
    result = {"district": None, "region": None, "max_price": None, "min_discount": None,
              "unknown": [], "ambiguous": []}
    place_tokens = []
    for token in (raw or "").split():
        if token.endswith("%") and parse_discount(token) is not None:
            result["min_discount"] = parse_discount(token)
            continue
        # 단위 없는 작은 숫자(예: 15)는 15%인지 15억인지 알 수 없어 따로 알린다
        if re.fullmatch(r"\d{1,3}", token):
            result["ambiguous"].append(token)
            continue
        price = parse_price(token) if re.search(r"\d", token) else None
        if price is not None:
            result["max_price"] = price
            continue
        place_tokens.append(token)

    # 시/도 이름은 어디에 있든 먼저 골라 같은 이름의 구(중구·서구 등)를 가르는 데 쓴다.
    region_hint = None
    rest = []
    for token in place_tokens:
        region = match_region(token, districts) if region_hint is None else None
        is_district = any(d["district"] == token for d in districts)
        if region and not is_district:
            region_hint = region
        else:
            rest.append(token)
    for token in rest:
        found = match_district(token, districts, region_hint) if result["district"] is None else None
        if found:
            result.update(found)
        else:
            result["unknown"].append(token)
    return result
