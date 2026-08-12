"""Offline tests for serial model-directed Voice PE follow-up requests."""

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock


ADDON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ADDON_ROOT))

from app.request_follow_up_tool import (  # noqa: E402
    FollowUpReservationOutcome,
    REQUEST_FOLLOW_UP_TOOL_NAME,
    REQUEST_FOLLOW_UP_PURPOSE,
    create_request_follow_up_tool_handler,
    get_request_follow_up_tool_definition,
    register_request_follow_up_tool,
)

VALID_ARGUMENTS = {"purpose": REQUEST_FOLLOW_UP_PURPOSE}


class RequestFollowUpToolTests(unittest.IsolatedAsyncioTestCase):
    def test_schema_is_realtime_compatible_and_limits_use_to_one_question(self):
        definition = get_request_follow_up_tool_definition()

        self.assertEqual(definition["name"], REQUEST_FOLLOW_UP_TOOL_NAME)
        self.assertNotIn("strict", definition)
        self.assertEqual(
            definition["parameters"],
            {
                "type": "object",
                "properties": {
                    "purpose": {
                        "type": "string",
                        "enum": ["conversational_turn"],
                        "description": (
                            "Use before one question that expects the user's immediate "
                            "answer, including one step in a multi-question sequence."
                        ),
                    }
                },
                "required": ["purpose"],
                "additionalProperties": False,
            },
        )
        description = definition["description"]
        self.assertIn("Mandatory before any spoken or written question", description)
        self.assertIn("first question in a user-requested", description)
        self.assertIn("sole output", description)
        self.assertIn("no speech, text, or other tool", description)
        self.assertIn("exactly one short question", description)
        self.assertIn("Call again after each genuine relevant answer", description)

    async def test_handler_reserves_before_instructing_one_short_question(self):
        events = []

        async def reserve_follow_up(tool_call_id):
            events.append(("reserved", tool_call_id))
            return FollowUpReservationOutcome.RESERVED

        async def result_callback(result):
            events.append(result)

        def activate_follow_up(tool_call_id):
            events.append(("activated", tool_call_id))
            return True

        cancel_follow_up = Mock()
        handler = create_request_follow_up_tool_handler(
            reserve_follow_up,
            activate_follow_up,
            cancel_follow_up,
        )
        await handler(
            SimpleNamespace(
                arguments=VALID_ARGUMENTS,
                tool_call_id="follow-up-call",
                result_callback=result_callback,
            )
        )

        self.assertEqual(events[0], ("reserved", "follow-up-call"))
        self.assertEqual(events[1], ("activated", "follow-up-call"))
        result = events[2]
        self.assertEqual(result["status"], "follow_up_reserved")
        self.assertIn("exactly one short question", result["instruction"])
        self.assertIn("Do not mention this tool", result["instruction"])
        self.assertIn("microphone", result["instruction"])
        self.assertIn("timeout", result["instruction"])
        cancel_follow_up.assert_not_called()

    async def test_duplicate_reservation_is_reported_without_requesting_another(self):
        callback = AsyncMock()
        handler = create_request_follow_up_tool_handler(
            AsyncMock(return_value=FollowUpReservationOutcome.ALREADY_RESERVED),
            Mock(return_value=True),
            Mock(),
        )

        await handler(
            SimpleNamespace(
                arguments=VALID_ARGUMENTS,
                tool_call_id="duplicate-call",
                result_callback=callback,
            )
        )

        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["status"], "follow_up_already_reserved")
        self.assertIn("Ask no other question", result["instruction"])

    async def test_suppressed_no_wake_window_keeps_question_in_context(self):
        callback = AsyncMock()
        activate_follow_up = Mock()
        handler = create_request_follow_up_tool_handler(
            AsyncMock(return_value=FollowUpReservationOutcome.REQUIRES_WAKE),
            activate_follow_up,
            Mock(),
        )

        await handler(
            SimpleNamespace(
                arguments=VALID_ARGUMENTS,
                tool_call_id="wake-required-call",
                result_callback=callback,
            )
        )

        activate_follow_up.assert_not_called()
        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["status"], "follow_up_requires_wake")
        self.assertIn("conversation context", result["instruction"])
        self.assertIn("exactly one short question", result["instruction"])
        self.assertIn("Do not mention", result["instruction"])

    async def test_extra_arguments_are_rejected_without_reserving(self):
        reserve_follow_up = AsyncMock()
        activate_follow_up = Mock()
        cancel_follow_up = Mock()
        callback = AsyncMock()
        handler = create_request_follow_up_tool_handler(
            reserve_follow_up,
            activate_follow_up,
            cancel_follow_up,
        )

        await handler(
            SimpleNamespace(
                arguments={**VALID_ARGUMENTS, "seconds": 10},
                tool_call_id="invalid-call",
                result_callback=callback,
            )
        )

        reserve_follow_up.assert_not_awaited()
        activate_follow_up.assert_not_called()
        cancel_follow_up.assert_not_called()
        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["status"], "invalid_arguments")
        self.assertIn("without asking a question", result["instruction"])

    async def test_unknown_purpose_is_rejected_without_reserving(self):
        reserve_follow_up = AsyncMock()
        callback = AsyncMock()
        handler = create_request_follow_up_tool_handler(
            reserve_follow_up,
            Mock(return_value=True),
            Mock(),
        )

        await handler(
            SimpleNamespace(
                arguments={"purpose": "optional_offer"},
                tool_call_id="optional-call",
                result_callback=callback,
            )
        )

        reserve_follow_up.assert_not_awaited()
        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["status"], "invalid_arguments")

    async def test_non_object_arguments_are_rejected_even_when_empty(self):
        reserve_follow_up = AsyncMock()
        activate_follow_up = Mock()
        cancel_follow_up = Mock()
        callback = AsyncMock()
        handler = create_request_follow_up_tool_handler(
            reserve_follow_up,
            activate_follow_up,
            cancel_follow_up,
        )

        await handler(
            SimpleNamespace(
                arguments=[],
                tool_call_id="invalid-call",
                result_callback=callback,
            )
        )

        reserve_follow_up.assert_not_awaited()
        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["status"], "invalid_arguments")

    async def test_other_active_tool_is_rejected_without_reserving(self):
        reserve_follow_up = AsyncMock()
        callback = AsyncMock()
        handler = create_request_follow_up_tool_handler(
            reserve_follow_up,
            Mock(return_value=True),
            Mock(),
            follow_up_is_safe=lambda tool_call_id: tool_call_id != "blocked-call",
        )

        await handler(
            SimpleNamespace(
                arguments=VALID_ARGUMENTS,
                tool_call_id="blocked-call",
                result_callback=callback,
            )
        )

        reserve_follow_up.assert_not_awaited()
        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["status"], "other_tool_active")
        self.assertIn("without asking a question", result["instruction"])

    async def test_async_terminal_authorizer_completes_before_reservation(self):
        events = []

        async def terminal_authorizer(tool_call_id):
            events.append(("terminal", tool_call_id))
            return True

        async def reserve_follow_up(tool_call_id):
            events.append(("reserve", tool_call_id))
            return FollowUpReservationOutcome.RESERVED

        handler = create_request_follow_up_tool_handler(
            reserve_follow_up,
            Mock(return_value=True),
            Mock(),
            follow_up_is_safe=terminal_authorizer,
        )

        await handler(
            SimpleNamespace(
                arguments=VALID_ARGUMENTS,
                tool_call_id="terminal-follow-up-call",
                result_callback=AsyncMock(),
            )
        )

        self.assertEqual(
            events,
            [
                ("terminal", "terminal-follow-up-call"),
                ("reserve", "terminal-follow-up-call"),
            ],
        )

    async def test_unavailable_reservation_does_not_imply_listening(self):
        async def unavailable(_tool_call_id):
            raise RuntimeError("no connected device")

        callback = AsyncMock()
        handler = create_request_follow_up_tool_handler(
            unavailable,
            Mock(return_value=True),
            Mock(),
        )

        with self.assertLogs("app.request_follow_up_tool", level="WARNING"):
            await handler(
                SimpleNamespace(
                    arguments=VALID_ARGUMENTS,
                    tool_call_id="unavailable-call",
                    result_callback=callback,
                )
            )

        result = cast(Any, callback.await_args).args[0]
        self.assertEqual(result["status"], "follow_up_unavailable")
        self.assertIn("without asking a question", result["instruction"])
        self.assertNotIn("listening", result["instruction"].lower())
        self.assertNotIn("window", result["instruction"].lower())
        self.assertNotIn("timeout", result["instruction"].lower())

    async def test_result_delivery_failure_cancels_prepared_reservation(self):
        reserve_follow_up = AsyncMock(
            return_value=FollowUpReservationOutcome.RESERVED
        )
        activate_follow_up = Mock(return_value=True)
        cancel_follow_up = Mock()
        callback = AsyncMock(side_effect=RuntimeError("result queue failed"))
        handler = create_request_follow_up_tool_handler(
            reserve_follow_up,
            activate_follow_up,
            cancel_follow_up,
        )

        with self.assertRaisesRegex(RuntimeError, "result queue failed"):
            await handler(
                SimpleNamespace(
                    arguments=VALID_ARGUMENTS,
                    tool_call_id="result-failure-call",
                    result_callback=callback,
                )
            )

        cancel_follow_up.assert_called_once_with()
        activate_follow_up.assert_called_once_with("result-failure-call")

    def test_handler_is_registered_for_dispatch(self):
        registrations = {}

        class Llm:
            def register_function(self, name, handler):
                registrations[name] = handler

        register_request_follow_up_tool(
            Llm(),
            AsyncMock(),
            Mock(return_value=True),
            Mock(),
        )

        self.assertEqual(set(registrations), {REQUEST_FOLLOW_UP_TOOL_NAME})
        self.assertTrue(callable(registrations[REQUEST_FOLLOW_UP_TOOL_NAME]))


if __name__ == "__main__":
    unittest.main()
