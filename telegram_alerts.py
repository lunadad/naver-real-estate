"""텔레그램 진짜 급매 알림 규칙(맥미니 로컬 파일)과 크롤 후 발송.

규칙은 공개 API가 아니라 로컬 파일에 둔다 — 아무나 만든 규칙이 내 텔레그램으로
오지 않게 하기 위해서다. 표준 라이브러리만 쓴다(Hermes 플러그인이 import).
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, List, Optional

from bargain import BARGAIN_MIN, is_bargain
from deal_query import format_manwon

logger = logging.getLogger(__name__)

DEFAULT_RULES_PATH = Path(__file__).resolve().parent / "logs" / "telegram_alert_rules.json"
SITE_URL = "https://naver-real-estate-henna.vercel.app"
SENT_RETENTION_DAYS = 30
MAX_ITEMS_PER_MESSAGE = 5


def rule_label(rule: Dict) -> str:
    parts = [rule.get("district") or "전체"]
    if rule.get("max_price"):
        parts.append(f"{format_manwon(rule['max_price'])} 이하")
    parts.append(f"{round(max(rule.get('min_discount') or 0, BARGAIN_MIN) * 100)}%↓")
    return " · ".join(parts)


def format_deal_line(row: Dict) -> List[str]:
    floor = f" {row['floor']}층" if row.get("floor") else ""
    area = f" {row['area']}㎡" if row.get("area") else ""
    lines = [f"· {row.get('building_name') or '단지'} ({row.get('district')}){area}{floor} "
             f"{row.get('price')} (−{round(row['bargain_discount'] * 100)}%)"]
    if row.get("naver_url"):
        lines.append(f"  {row['naver_url']}")
    return lines


def select_bargains(rows: List[Dict], min_discount: Optional[float]) -> List[Dict]:
    floor = max(min_discount or 0, BARGAIN_MIN)
    picked = [r for r in rows if is_bargain(r.get("bargain_discount")) and r["bargain_discount"] >= floor]
    return sorted(picked, key=lambda r: r["bargain_discount"], reverse=True)


class TelegramRuleStore:
    def __init__(self, path: Path = DEFAULT_RULES_PATH):
        self.path = Path(path)
        self._data = {"next_id": 1, "rules": []}
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
                self._data["rules"] = list(loaded.get("rules") or [])
                self._data["next_id"] = int(loaded.get("next_id") or 1)
            except (OSError, ValueError, AttributeError, TypeError):
                logger.warning("텔레그램 알림 규칙 파일이 손상되어 비어 있는 것으로 봄: %s", self.path)

    def _save(self):
        tmp_path = self.path.with_name(self.path.name + ".tmp")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(tmp_path, self.path)
        except OSError as exc:
            logger.warning("텔레그램 알림 규칙 저장 실패 (%s): %s", self.path, exc)

    def list(self) -> List[Dict]:
        return [dict(rule) for rule in self._data["rules"]]

    def add(self, *, district: str, region: str = "", max_price: Optional[int] = None,
            min_discount: Optional[float] = None, now: Optional[datetime] = None) -> Dict:
        rule = {
            "id": self._data["next_id"],
            "district": district,
            "region": region,
            "max_price": max_price,
            "min_discount": min_discount,
            "created_at": (now or datetime.now()).isoformat(),
            "sent": {},
        }
        self._data["next_id"] += 1
        self._data["rules"].append(rule)
        self._save()
        return dict(rule)

    def remove(self, rule_id: int) -> bool:
        before = len(self._data["rules"])
        self._data["rules"] = [r for r in self._data["rules"] if r.get("id") != rule_id]
        if len(self._data["rules"]) == before:
            return False
        self._save()
        return True

    def mark_sent(self, rule_id: int, article_nos: List[str], now: datetime):
        cutoff = now - timedelta(days=SENT_RETENTION_DAYS)
        for rule in self._data["rules"]:
            if rule.get("id") != rule_id:
                continue
            sent = {
                no: at for no, at in (rule.get("sent") or {}).items()
                if _parse_time(at) and _parse_time(at) >= cutoff
            }
            for no in article_nos:
                sent[str(no)] = now.isoformat()
            rule["sent"] = sent
        self._save()

    def recently_sent(self, rule: Dict, now: datetime) -> set:
        cutoff = now - timedelta(days=SENT_RETENTION_DAYS)
        return {
            no for no, at in (rule.get("sent") or {}).items()
            if _parse_time(at) and _parse_time(at) >= cutoff
        }


def _parse_time(value) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def dispatch_telegram_deal_alerts(db, store: TelegramRuleStore, send: Callable[[str], bool],
                                  now: Optional[datetime] = None) -> List[str]:
    """규칙마다 아직 안 보낸 진짜 급매를 모아 1개 메시지로 보낸다. 못 보낸 메시지를 반환한다."""
    now = now or datetime.now()
    unsent = []
    for rule in store.list():
        result = db.get_listings(
            district=rule.get("district") or "",
            trade_type="매매",
            bargain_only=True,
            max_price=rule.get("max_price"),
            sort_by="recent",
            per_page=100,
        )
        already = store.recently_sent(rule, now)
        fresh = [r for r in select_bargains(result.get("listings") or [], rule.get("min_discount"))
                 if str(r.get("article_no")) not in already]
        if not fresh:
            continue
        lines = [f"[진짜 급매 알림] {rule_label(rule)} — 새 {len(fresh)}건"]
        for row in fresh[:MAX_ITEMS_PER_MESSAGE]:
            lines.extend(format_deal_line(row))
        if len(fresh) > MAX_ITEMS_PER_MESSAGE:
            lines.append(f"외 {len(fresh) - MAX_ITEMS_PER_MESSAGE}건 · {SITE_URL}")
        text = "\n".join(lines)
        if send(text):
            store.mark_sent(rule["id"], [str(r.get("article_no")) for r in fresh], now)
        else:
            unsent.append(text)
    return unsent
