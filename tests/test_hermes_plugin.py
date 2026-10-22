"""Hermes 플러그인이 명령 4개를 등록하고 저장소 함수로 위임하는지 확인한다."""

import asyncio
import importlib.util
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
PLUGIN_DIR = ROOT / "integrations" / "hermes" / "real-estate"


def load_plugin():
    spec = importlib.util.spec_from_file_location("real_estate_plugin", PLUGIN_DIR / "__init__.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeCtx:
    def __init__(self):
        self.commands = {}

    def register_command(self, name, handler, description="", args_hint=""):
        self.commands[name] = (handler, description, args_hint)


class HermesPluginTest(unittest.TestCase):
    def test_manifest_and_registered_commands(self):
        manifest = (PLUGIN_DIR / "plugin.yaml").read_text(encoding="utf-8")
        self.assertIn("name: real-estate", manifest)
        ctx = FakeCtx()
        load_plugin().register(ctx)
        self.assertEqual(sorted(ctx.commands), ["complex", "dealalert", "deals", "weekly"])
        for handler, description, _ in ctx.commands.values():
            self.assertTrue(description)
            self.assertTrue(asyncio.iscoroutinefunction(handler))

    def test_handler_delegates_and_never_raises(self):
        plugin = load_plugin()
        ctx = FakeCtx()
        plugin.register(ctx)
        import telegram_commands

        with mock.patch.object(telegram_commands, "handle_weekly", return_value="리포트"):
            self.assertEqual(asyncio.run(ctx.commands["weekly"][0]("")), "리포트")
        with mock.patch.object(telegram_commands, "handle_deals", side_effect=RuntimeError("x")):
            reply = asyncio.run(ctx.commands["deals"][0]("송파"))
        self.assertIn("오류", reply)


if __name__ == "__main__":
    unittest.main()
