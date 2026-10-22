"""텔레그램 알림 전송과 크롤링 알림 상태(로컬 파일).

알림은 부가 기능이라 어떤 경우에도 예외를 던지지 않는다. 상태는 DB가 아니라
로컬 파일에 둔다 — 인터넷·DB가 끊긴 상황에서도 실패를 기록해야 하기 때문이다.
"""

import json
import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List, Mapping, Optional

import requests

logger = logging.getLogger(__name__)

HERMES_ENV_PATH = Path.home() / ".hermes" / ".env"
STATE_RETENTION_DAYS = 30


def _read_env_file(path: Path) -> Dict[str, str]:
    values = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key:
            values[key] = value.strip().strip('"').strip("'")
    return values


def load_telegram_config(
    env: Mapping[str, str] = os.environ,
    hermes_env_path: Path = HERMES_ENV_PATH,
) -> Optional[Dict[str, str]]:
    """환경변수 TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID를 우선 쓰고, 없으면 Hermes 설정을 읽는다."""
    token = (env.get("TELEGRAM_BOT_TOKEN") or "").strip()
    chat_id = (env.get("TELEGRAM_CHAT_ID") or "").strip()
    if token and chat_id:
        return {"token": token, "chat_id": chat_id}

    hermes = _read_env_file(Path(hermes_env_path))
    token = hermes.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = hermes.get("TELEGRAM_HOME_CHANNEL", "")
    if not token or not chat_id:
        return None
    config = {"token": token, "chat_id": chat_id}
    thread_id = hermes.get("TELEGRAM_HOME_CHANNEL_THREAD_ID", "")
    if thread_id:
        config["thread_id"] = thread_id
    return config


def send_telegram(text: str, *, config: Optional[Dict] = None, post=requests.post) -> bool:
    """메시지를 보내고 성공 여부를 반환한다. 실패해도 예외를 던지지 않는다."""
    config = config or load_telegram_config()
    if not config:
        logger.warning("텔레그램 설정이 없어 알림을 보내지 못함")
        return False

    payload = {"chat_id": config["chat_id"], "text": text, "disable_web_page_preview": True}
    if config.get("thread_id"):
        payload["message_thread_id"] = config["thread_id"]
    try:
        response = post(
            f"https://api.telegram.org/bot{config['token']}/sendMessage",
            json=payload,
            timeout=15,
        )
        body = response.json()
    except Exception as exc:
        # 토큰이 URL에 들어가므로 예외 메시지 대신 종류만 남긴다.
        logger.warning("텔레그램 전송 실패: %s", type(exc).__name__)
        return False
    if not body.get("ok"):
        logger.warning("텔레그램 전송 거부: %s", body.get("description", "unknown"))
        return False
    return True


class AlertState:
    """날짜별 크롤링 시도 결과와 아직 못 보낸 알림을 JSON 파일에 보관한다."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._data = {"days": {}, "pending": []}
        if self.path.exists():
            try:
                loaded = json.loads(self.path.read_text(encoding="utf-8"))
                self._data["days"] = dict(loaded.get("days") or {})
                self._data["pending"] = list(loaded.get("pending") or [])
            except (OSError, ValueError, AttributeError):
                logger.warning("알림 상태 파일이 손상되어 새로 시작: %s", self.path)

    def _prune(self):
        parsed = {}
        for day, value in self._data["days"].items():
            try:
                parsed[day] = (date.fromisoformat(day), value)
            except (TypeError, ValueError):
                continue
        if parsed:
            cutoff = max(d for d, _ in parsed.values()) - timedelta(days=STATE_RETENTION_DAYS)
            parsed = {day: item for day, item in parsed.items() if item[0] >= cutoff}
        self._data["days"] = {day: value for day, (_, value) in parsed.items()}

    def _save(self):
        """임시 파일에 쓴 뒤 교체한다. 저장 실패는 크롤링을 막지 않도록 경고만 남긴다."""
        self._prune()
        tmp_path = self.path.with_name(self.path.name + ".tmp")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(tmp_path, self.path)
        except OSError as exc:
            logger.warning("알림 상태 파일 저장 실패 (%s): %s", self.path, exc)

    def record_attempt(self, day: date, success: bool):
        entry = self._data["days"].setdefault(day.isoformat(), {})
        entry["attempts"] = entry.get("attempts", 0) + 1
        key = "successes" if success else "failures"
        entry[key] = entry.get(key, 0) + 1
        self._save()

    def had_success(self, day: date) -> bool:
        return bool((self._data["days"].get(day.isoformat()) or {}).get("successes"))

    def had_failure(self, day: date) -> bool:
        return bool((self._data["days"].get(day.isoformat()) or {}).get("failures"))

    def add_pending(self, text: str):
        self._data["pending"].append(text)
        self._save()

    def take_pending(self) -> List[str]:
        pending = self._data["pending"]
        if not pending:
            return []
        self._data["pending"] = []
        self._save()
        return pending
