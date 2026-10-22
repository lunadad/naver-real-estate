"""주간 텔레그램 시장 리포트 메시지 포맷."""

from typing import Dict, List

from bargain import BARGAIN_MIN


def _mover_lines(movers) -> List[str]:
    return [
        f"· {building} ({district}) {prev}→{cur}건 ({diff:+d})"
        for district, building, prev, cur, diff in movers
    ] or ["· 없음"]


def format_weekly_report(data: Dict, site_url: str) -> str:
    latest = data["latest"]
    previous = data.get("previous")
    total = latest["total_count"]

    lines = ["📊 주간 급매 리포트"]
    if previous:
        lines.append(f"{previous['crawled_at']:%m/%d} → {latest['crawled_at']:%m/%d}")
    else:
        lines.append(f"{latest['crawled_at']:%m/%d} 기준 (비교할 지난 데이터 없음)")
    lines.append("")

    if previous:
        diff = total - previous["total_count"]
        pct = diff / previous["total_count"] * 100 if previous["total_count"] else 0
        lines.append(f"급매 {total:,}건 ({diff:+,}, {pct:+.1f}%)")
    else:
        lines.append(f"급매 {total:,}건")

    ratio_now, ratio_prev = data.get("ratio_now"), data.get("ratio_prev")
    if ratio_now is not None and ratio_prev is not None:
        lines.append(f"가격인하 비율 {ratio_now:.1f}% ({ratio_now - ratio_prev:+.1f}%p)")
    elif ratio_now is not None:
        lines.append(f"가격인하 비율 {ratio_now:.1f}%")

    if data.get("risers") is not None and data.get("fallers") is not None:
        lines += ["", "📈 급매 늘어난 단지", *_mover_lines(data["risers"])]
        lines += ["", "📉 급매 줄어든 단지", *_mover_lines(data["fallers"])]

    bargains = data.get("bargains", [])
    lines += [
        "",
        f"💎 진짜 급매 (중앙값 대비 {BARGAIN_MIN:.0%}↓) — {data.get('bargain_total', 0):,}건 중 상위 {len(bargains)}",
        f"(40%↑ 차이 {data.get('anomaly_count', 0)}건은 지분·오류 의심으로 제외)",
    ]
    if not bargains:
        lines.append("· 없음")
    for b in bargains:
        floor = f" {b['floor']}층" if b.get("floor") else ""
        lines.append(
            f"· {b['building_name']} ({b['district']}) {b['area']}㎡{floor} {b['price']} (−{b['discount']:.0%})"
        )
        if b.get("naver_url"):
            lines.append(f"  {b['naver_url']}")

    lines += ["", site_url]
    return "\n".join(lines)
