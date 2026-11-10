"""블루프린트 공용 헬퍼 — 현재 DB, 에러 응답, DB 기준 현재 시각."""

import os
from datetime import datetime, timedelta, timezone

from flask import current_app, jsonify

KST = timezone(timedelta(hours=9))


def get_db():
    """app.py가 등록한 GET_DB로 (테스트에서 교체되는) 현재 Database를 읽는다."""
    return current_app.config["GET_DB"]()


def error_response(message, status_code):
    return jsonify({"status": "error", "message": message}), status_code


def naive_now() -> datetime:
    """크롤러가 저장하는 naive 시각(datetime.now())과 같은 기준의 현재 시각.

    운영(DATABASE_URL 설정)은 맥미니 크롤러가 KST로 저장하므로 서버 시간대와 무관하게 KST를 쓴다.
    """
    if os.getenv("DATABASE_URL", "").strip():
        return datetime.now(KST).replace(tzinfo=None)
    return datetime.now()
