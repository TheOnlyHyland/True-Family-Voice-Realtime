"""Silently close an unrelated answer in a reopened Voice PE turn."""

import inspect
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Optional


logger = logging.getLogger(__name__)

END_CONVERSATION_TOOL_NAME = "end_conversation"


@dataclass
class SilentCloseResultProperties:
    """Pipecat-compatible result properties that forbid a model continuation."""

    run_llm: bool = False
    on_context_updated: Optional[Callable[[], Awaitable[None]]] = None


@dataclass
class SpokenCloseVetoResultProperties:
    """Pipecat-compatible result properties for one tool-disabled reply."""

    run_llm: bool = True
    on_context_updated: Optional[Callable[[], Awaitable[None]]] = None


class SilentCloseAuthorization(str, Enum):
    """A semantic answer may require speech instead of a silent close."""

    SPOKEN_RESPONSE_REQUIRED = "spoken_response_required"


def get_end_conversation_tool_definition() -> Dict[str, Any]:
    """Return the strict OpenAI function schema for silent conversation close."""
    return {
        "type": "function",
        "name": END_CONVERSATION_TOOL_NAME,
        "description": (
            "Call as the sole tool, with no spoken reply before or after it, only when "
            "the user's answer through a microphone reopened by request_follow_up is "
            "random or unrelated to the active conversation. This silently closes that "
            "conversation and must never reopen the microphone. Do not use it for an "
            "ordinary answer, a relevant answer, a goodbye, thanks, cancellation, or "
            "uncertainty. Never call it alongside another tool."
        ),
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    }


def create_end_conversation_tool_handler(
    close_silently: Callable[[], Awaitable[None]],
    close_is_safe: Callable[[str], Any] = lambda _tool_call_id: True,
) -> Callable[[Any], Awaitable[None]]:
    """Create a handler whose successful result cannot trigger model output."""

    async def end_conversation_tool_handler(params: Any) -> None:
        arguments = {} if params.arguments is None else params.arguments
        if not isinstance(arguments, dict) or arguments:
            await params.result_callback(
                {
                    "status": "invalid_arguments",
                    "instruction": "Continue naturally; do not mention this tool error.",
                }
            )
            return

        safe_to_close = close_is_safe(params.tool_call_id)
        if inspect.isawaitable(safe_to_close):
            safe_to_close = await safe_to_close
        if safe_to_close is SilentCloseAuthorization.SPOKEN_RESPONSE_REQUIRED:
            await params.result_callback(
                {
                    "status": "spoken_response_required",
                    "instruction": (
                        "Give one brief natural spoken response acknowledging the "
                        "user's completion or decision. Ask no question, call no tool, "
                        "and do not mention conversation controls."
                    ),
                },
                properties=SpokenCloseVetoResultProperties(),
            )
            return
        if safe_to_close is not True:
            await params.result_callback(
                {
                    "status": "other_tool_active",
                    "instruction": (
                        "Continue naturally without closing the conversation or "
                        "mentioning this tool."
                    ),
                }
            )
            return

        control_confirmed = True
        try:
            await close_silently()
        except Exception:
            control_confirmed = False
            logger.exception("Could not complete silent conversation close")

        await params.result_callback(
            {
                "status": (
                    "closed_silently"
                    if control_confirmed
                    else "silent_close_unconfirmed"
                ),
                "instruction": (
                    "Produce no spoken or textual reply. The conversation-control "
                    "decision is complete."
                ),
            },
            properties=SilentCloseResultProperties(),
        )

    return end_conversation_tool_handler


def register_end_conversation_tool(
    llm,
    close_silently: Callable[[], Awaitable[None]],
    close_is_safe: Callable[[str], Any] = lambda _tool_call_id: True,
) -> None:
    """Register silent-close dispatch on the active OpenAI service."""
    llm.register_function(
        END_CONVERSATION_TOOL_NAME,
        create_end_conversation_tool_handler(close_silently, close_is_safe),
    )
