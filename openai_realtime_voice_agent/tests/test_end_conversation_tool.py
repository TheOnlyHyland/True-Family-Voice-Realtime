"""Offline tests for graceful Voice PE conversation closing."""

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock


ADDON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ADDON_ROOT))

from app.end_conversation_tool import (  # noqa: E402
    END_CONVERSATION_TOOL_NAME,
    create_end_conversation_tool_handler,
    get_end_conversation_tool_definition,
    register_end_conversation_tool,
)


class EndConversationToolTests(unittest.IsolatedAsyncioTestCase):
    def test_schema_has_no_arguments_and_describes_graceful_close(self):
        definition = get_end_conversation_tool_definition()

        self.assertEqual(definition["name"], END_CONVERSATION_TOOL_NAME)
        self.assertEqual(
            definition["parameters"],
            {
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": False,
            },
        )
        self.assertIn("final reply finish", definition["description"])
        self.assertIn("When uncertain", definition["description"])
        self.assertIn("Never call this alongside another tool", definition["description"])
        self.assertIn("You're welcome", definition["description"])
        self.assertIn("Never describe closing", definition["description"])

    async def test_control_is_sent_before_tool_result(self):
        events = []

        async def arm_graceful_close():
            events.append(("control", {"type": "suppress_followup"}))

        async def result_callback(result):
            events.append(("result", result))

        handler = create_end_conversation_tool_handler(arm_graceful_close)
        await handler(SimpleNamespace(arguments={}, result_callback=result_callback))

        self.assertEqual(events[0], ("control", {"type": "suppress_followup"}))
        self.assertEqual(events[1][0], "result")
        self.assertEqual(events[1][1]["status"], "closing_after_reply")
        self.assertIn("Do not ask a question", events[1][1]["instruction"])
        self.assertIn("You're welcome", events[1][1]["instruction"])
        self.assertIn("never describe closing", events[1][1]["instruction"])

    async def test_control_failure_still_completes_tool_call(self):
        async def fail_control():
            raise RuntimeError("test transport failure")

        callback = AsyncMock()
        handler = create_end_conversation_tool_handler(fail_control)

        with self.assertLogs("app.end_conversation_tool", level="ERROR"):
            await handler(SimpleNamespace(arguments=None, result_callback=callback))

        callback.assert_awaited_once()
        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["status"], "closing_reply_unconfirmed")
        self.assertIn("one brief sentence", result["instruction"])

    async def test_extra_arguments_do_not_arm_close(self):
        arm_graceful_close = AsyncMock()
        callback = AsyncMock()
        handler = create_end_conversation_tool_handler(arm_graceful_close)

        await handler(
            SimpleNamespace(
                arguments={"reason": "unsupported"},
                result_callback=callback,
            )
        )

        arm_graceful_close.assert_not_awaited()
        callback.assert_awaited_once()
        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["status"], "invalid_arguments")

    async def test_mixed_tool_call_does_not_arm_close(self):
        arm_graceful_close = AsyncMock()
        callback = AsyncMock()
        handler = create_end_conversation_tool_handler(
            arm_graceful_close,
            close_is_safe=lambda: False,
        )

        await handler(SimpleNamespace(arguments={}, result_callback=callback))

        arm_graceful_close.assert_not_awaited()
        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["status"], "other_tool_active")

    def test_handler_is_registered_for_dispatch(self):
        registrations = {}

        class Llm:
            def register_function(self, name, handler):
                registrations[name] = handler

        register_end_conversation_tool(Llm(), AsyncMock())

        self.assertEqual(set(registrations), {END_CONVERSATION_TOOL_NAME})
        self.assertTrue(callable(registrations[END_CONVERSATION_TOOL_NAME]))


if __name__ == "__main__":
    unittest.main()
