"""Reserve one explicit Voice PE follow-up after the current reply."""

import logging
from enum import Enum
from typing import Any, Awaitable, Callable, Dict


logger = logging.getLogger(__name__)

REQUEST_FOLLOW_UP_TOOL_NAME = "request_follow_up"
REQUEST_FOLLOW_UP_PURPOSE = "conversational_turn"


class FollowUpReservationOutcome(str, Enum):
    """Backend-only result of reserving the current turn's follow-up authority."""

    RESERVED = "reserved"
    ALREADY_RESERVED = "already_reserved"
    REQUIRES_WAKE = "requires_wake"


def get_request_follow_up_tool_definition() -> Dict[str, Any]:
    """Return the Realtime-compatible schema for one conversational follow-up."""
    return {
        "type": "function",
        "name": REQUEST_FOLLOW_UP_TOOL_NAME,
        "description": (
            "Call as the sole tool immediately before asking exactly one short "
            "question whenever another user turn would be useful to continue, clarify, "
            "personalize, or naturally complete the active conversation. It may be "
            "called again after each genuine answer. Never call alongside another tool "
            "and never ask more than one question per call."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "purpose": {
                    "type": "string",
                    "enum": [REQUEST_FOLLOW_UP_PURPOSE],
                    "description": (
                        "Use when exactly one more user answer would make the active "
                        "conversation more useful."
                    ),
                }
            },
            "required": ["purpose"],
            "additionalProperties": False,
        },
    }


def create_request_follow_up_tool_handler(
    reserve_follow_up: Callable[[str], Awaitable[FollowUpReservationOutcome]],
    activate_follow_up: Callable[[str], bool],
    cancel_follow_up: Callable[[], None],
    follow_up_is_safe: Callable[[str], bool] = lambda _tool_call_id: True,
) -> Callable[[Any], Awaitable[None]]:
    """Create a handler that reserves, but does not send, one follow-up control."""

    async def request_follow_up_tool_handler(params: Any) -> None:
        if params.arguments != {"purpose": REQUEST_FOLLOW_UP_PURPOSE}:
            await params.result_callback(
                {
                    "status": "invalid_arguments",
                    "instruction": (
                        "Give the safest brief reply you can without asking a question. "
                        "Do not mention this tool or conversation controls."
                    ),
                }
            )
            return

        if not follow_up_is_safe(params.tool_call_id):
            await params.result_callback(
                {
                    "status": "other_tool_active",
                    "instruction": (
                        "Complete the other action and give a brief reply without asking "
                        "a question. Do not mention this tool or conversation controls."
                    ),
                }
            )
            return

        try:
            outcome = await reserve_follow_up(params.tool_call_id)
        except Exception as error:
            logger.warning("Could not reserve requested follow-up: %r", error)
            await params.result_callback(
                {
                    "status": "follow_up_unavailable",
                    "instruction": (
                        "Give the safest brief reply you can without asking a question. "
                        "Do not mention this tool or conversation controls."
                    ),
                }
            )
            return

        if outcome is FollowUpReservationOutcome.REQUIRES_WAKE:
            await params.result_callback(
                {
                    "status": "follow_up_requires_wake",
                    "instruction": (
                        "Ask exactly one short question, then stop. Keep "
                        "the question in conversation context so the user can answer "
                        "after a fresh wake. Do not mention this tool, the microphone, "
                        "a wake word, a listening window, or a timeout. Ask no other "
                        "question."
                    ),
                }
            )
            return

        if outcome not in (
            FollowUpReservationOutcome.RESERVED,
            FollowUpReservationOutcome.ALREADY_RESERVED,
        ):
            cancel_follow_up()
            await params.result_callback(
                {
                    "status": "follow_up_unavailable",
                    "instruction": (
                        "Give the safest brief reply you can without asking a question. "
                        "Do not mention this tool or conversation controls."
                    ),
                }
            )
            return

        newly_reserved = outcome is FollowUpReservationOutcome.RESERVED
        result = {
            "status": (
                "follow_up_reserved"
                if newly_reserved
                else "follow_up_already_reserved"
            ),
            "instruction": (
                "Now ask exactly one short question. Do not mention this "
                "tool, the microphone, a listening window, or a timeout. Ask no "
                "other question."
            ),
        }
        if newly_reserved:
            try:
                activated = activate_follow_up(params.tool_call_id)
            except BaseException:
                cancel_follow_up()
                raise
            if not activated:
                cancel_follow_up()
                result = {
                    "status": "follow_up_unavailable",
                    "instruction": (
                        "Give the safest brief reply you can without asking a question. "
                        "Do not mention this tool or conversation controls."
                    ),
                }
        try:
            await params.result_callback(result)
        except BaseException:
            cancel_follow_up()
            raise

    return request_follow_up_tool_handler


def register_request_follow_up_tool(
    llm,
    reserve_follow_up: Callable[[str], Awaitable[FollowUpReservationOutcome]],
    activate_follow_up: Callable[[str], bool],
    cancel_follow_up: Callable[[], None],
    follow_up_is_safe: Callable[[str], bool] = lambda _tool_call_id: True,
) -> None:
    """Register requested-follow-up dispatch on the active OpenAI service."""
    llm.register_function(
        REQUEST_FOLLOW_UP_TOOL_NAME,
        create_request_follow_up_tool_handler(
            reserve_follow_up,
            activate_follow_up,
            cancel_follow_up,
            follow_up_is_safe,
        ),
    )
