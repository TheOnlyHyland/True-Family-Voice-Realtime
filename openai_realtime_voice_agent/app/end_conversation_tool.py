"""Gracefully close a Voice PE conversation after the current reply."""

import logging
from typing import Any, Awaitable, Callable, Dict


logger = logging.getLogger(__name__)

END_CONVERSATION_TOOL_NAME = "end_conversation"


def get_end_conversation_tool_definition() -> Dict[str, Any]:
    """Return the strict OpenAI function schema for graceful conversation close."""
    return {
        "type": "function",
        "name": END_CONVERSATION_TOOL_NAME,
        "description": (
            "Call immediately before your final short spoken reply only when the "
            "conversation is clearly complete and no answer is expected: for example, "
            "an explicit goodbye, 'that's all', a standalone thanks after a completed "
            "request, or a settled exchange with no pending question. This lets the "
            "final reply finish and then closes the follow-up "
            "microphone. Do not call merely because you can answer the current question, "
            "during an ordinary conversational pause, when the user may naturally "
            "continue, or when your reply asks a question. Never call this alongside "
            "another tool: finish all actions first, then call this as the sole tool in "
            "the next decision. When uncertain, leave the conversation open."
        ),
        "parameters": {
            "type": "object",
            "properties": {},
            "required": [],
            "additionalProperties": False,
        },
    }


def create_end_conversation_tool_handler(
    arm_graceful_close: Callable[[], Awaitable[None]],
    close_is_safe: Callable[[], bool] = lambda: True,
) -> Callable[[Any], Awaitable[None]]:
    """Create a handler that suppresses one follow-up without closing the socket."""

    async def end_conversation_tool_handler(params: Any) -> None:
        arguments = params.arguments or {}
        if arguments:
            await params.result_callback(
                {
                    "status": "invalid_arguments",
                    "instruction": "Continue naturally; do not mention this tool error.",
                }
            )
            return

        if not close_is_safe():
            await params.result_callback(
                {
                    "status": "other_tool_active",
                    "instruction": (
                        "Complete the other action and continue naturally. Do not close "
                        "the conversation or mention this tool."
                    ),
                }
            )
            return

        control_confirmed = True
        try:
            await arm_graceful_close()
        except Exception:
            control_confirmed = False
            logger.exception("Could not arm graceful conversation close")

        await params.result_callback(
            {
                "status": (
                    "closing_after_reply"
                    if control_confirmed
                    else "closing_reply_unconfirmed"
                ),
                "instruction": (
                    "Give at most one brief closing sentence. Do not ask a question or "
                    "mention this tool."
                ),
            }
        )

    return end_conversation_tool_handler


def register_end_conversation_tool(
    llm,
    arm_graceful_close: Callable[[], Awaitable[None]],
    close_is_safe: Callable[[], bool] = lambda: True,
) -> None:
    """Register graceful-close dispatch on the active OpenAI service."""
    llm.register_function(
        END_CONVERSATION_TOOL_NAME,
        create_end_conversation_tool_handler(arm_graceful_close, close_is_safe),
    )
