"""Offline tests for enforceable silent Voice PE conversation closing."""

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
    SilentCloseAuthorization,
    SilentCloseResultProperties,
    SpokenCloseVetoResultProperties,
    create_end_conversation_tool_handler,
    get_end_conversation_tool_definition,
    register_end_conversation_tool,
)


class EndConversationToolTests(unittest.IsolatedAsyncioTestCase):
    def test_schema_has_no_arguments_and_describes_silent_close(self):
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
        self.assertIn("no spoken reply before or after", definition["description"])
        self.assertIn("random or unrelated", definition["description"])
        self.assertIn("must never reopen", definition["description"])
        self.assertIn("Never call it alongside another tool", definition["description"])

    async def test_control_is_sent_before_tool_result(self):
        events = []

        async def close_silently():
            events.append(("control", {"type": "silent_close"}))

        async def result_callback(result, *, properties=None):
            events.append(("result", result, properties))

        handler = create_end_conversation_tool_handler(close_silently)
        await handler(
            SimpleNamespace(
                arguments={},
                tool_call_id="silent-close-call",
                result_callback=result_callback,
            )
        )

        self.assertEqual(events[0], ("control", {"type": "silent_close"}))
        self.assertEqual(events[1][0], "result")
        self.assertEqual(events[1][1]["status"], "closed_silently")
        self.assertIn("no spoken or textual reply", events[1][1]["instruction"])
        self.assertIsInstance(events[1][2], SilentCloseResultProperties)
        self.assertFalse(events[1][2].run_llm)

    async def test_control_failure_still_completes_tool_call(self):
        async def fail_control():
            raise RuntimeError("test transport failure")

        callback = AsyncMock()
        handler = create_end_conversation_tool_handler(fail_control)

        with self.assertLogs("app.end_conversation_tool", level="ERROR"):
            await handler(
                SimpleNamespace(
                    arguments=None,
                    tool_call_id="failed-close-call",
                    result_callback=callback,
                )
            )

        callback.assert_awaited_once()
        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["status"], "silent_close_unconfirmed")
        self.assertIn("no spoken or textual reply", result["instruction"])
        properties = cast(Any, callback.await_args).kwargs["properties"]
        self.assertFalse(properties.run_llm)

    async def test_extra_arguments_do_not_arm_close(self):
        arm_graceful_close = AsyncMock()
        callback = AsyncMock()
        handler = create_end_conversation_tool_handler(arm_graceful_close)

        await handler(
            SimpleNamespace(
                arguments={"reason": "unsupported"},
                tool_call_id="invalid-close-call",
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
            close_is_safe=lambda _tool_call_id: False,
        )

        await handler(
            SimpleNamespace(
                arguments={},
                tool_call_id="mixed-close-call",
                result_callback=callback,
            )
        )

        arm_graceful_close.assert_not_awaited()
        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["status"], "other_tool_active")

    async def test_async_terminal_authorizer_must_complete_before_close(self):
        terminal = AsyncMock(return_value=True)
        close_silently = AsyncMock()
        callback = AsyncMock()
        handler = create_end_conversation_tool_handler(
            close_silently,
            close_is_safe=terminal,
        )

        await handler(
            SimpleNamespace(
                arguments={},
                tool_call_id="terminal-close-call",
                result_callback=callback,
            )
        )

        terminal.assert_awaited_once_with("terminal-close-call")
        close_silently.assert_awaited_once_with()

    async def test_semantic_veto_requests_one_spoken_tool_free_response(self):
        close_silently = AsyncMock()
        callback = AsyncMock()
        handler = create_end_conversation_tool_handler(
            close_silently,
            close_is_safe=AsyncMock(
                return_value=SilentCloseAuthorization.SPOKEN_RESPONSE_REQUIRED
            ),
        )

        await handler(
            SimpleNamespace(
                arguments={},
                tool_call_id="vetoed-close-call",
                result_callback=callback,
            )
        )

        close_silently.assert_not_awaited()
        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["status"], "spoken_response_required")
        self.assertIn("one brief natural spoken response", result["instruction"])
        properties = cast(Any, callback.await_args).kwargs["properties"]
        self.assertIsInstance(properties, SpokenCloseVetoResultProperties)
        self.assertTrue(properties.run_llm)

    async def test_non_object_arguments_do_not_close(self):
        close_silently = AsyncMock()
        callback = AsyncMock()
        handler = create_end_conversation_tool_handler(close_silently)

        await handler(
            SimpleNamespace(
                arguments=[],
                tool_call_id="malformed-close-call",
                result_callback=callback,
            )
        )

        close_silently.assert_not_awaited()
        self.assertEqual(
            cast(Any, callback.await_args).args[0]["status"],
            "invalid_arguments",
        )

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
