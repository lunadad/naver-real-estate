"""Hermes 플러그인: 부동산 급매 알리미 텔레그램 명령.

~/.hermes/plugins/real-estate 로 이 폴더를 심볼릭 링크해 설치한다. 실제 처리는 저장소의
telegram_commands.py가 하며, 이 파일은 명령 등록과 예외 차단만 맡는다.
"""

import asyncio
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

# integrations/hermes/real-estate → 저장소 루트 (심볼릭 링크를 따라간다)
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

ERROR_REPLY = "명령 처리 중 오류가 났습니다. 잠시 후 다시 시도해 주세요."


def _wrap(handler_name: str):
    async def handler(raw_args: str = ""):
        import telegram_commands

        try:
            return await asyncio.to_thread(getattr(telegram_commands, handler_name), raw_args or "")
        except Exception:
            logger.exception("real-estate /%s 처리 실패", handler_name)
            return ERROR_REPLY

    return handler


def register(ctx):
    ctx.register_command(
        "deals", _wrap("handle_deals"),
        description="진짜 급매 찾기: /deals 송파 20억 15%",
        args_hint="<지역> [가격상한] [최소할인%]",
    )
    ctx.register_command(
        "complex", _wrap("handle_complex"),
        description="단지 급매 요약: /complex 헬리오시티",
        args_hint="<단지명> [구]",
    )
    ctx.register_command(
        "weekly", _wrap("handle_weekly"),
        description="주간 급매 리포트 지금 받기",
    )
    ctx.register_command(
        "dealalert", _wrap("handle_dealalert"),
        description="진짜 급매 텔레그램 알림: add/list/del",
        args_hint="add <지역> [가격상한] [할인%] | list | del <번호>",
    )
