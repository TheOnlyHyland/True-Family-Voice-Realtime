"""Bounded complete-turn history for the OpenAI Realtime conversation."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


def _text_content(item: dict[str, Any]) -> str:
    parts = item.get("content") or []
    text = []
    for part in parts:
        value = part.get("text") or part.get("transcript")
        if value:
            text.append(value)
    return "".join(text).strip()


@dataclass
class ConversationTurn:
    """One user request and every item needed to understand its outcome."""

    user_item_id: str
    items: list[dict[str, Any]] = field(default_factory=list)
    transcript: str = ""
    complete: bool = False
    terminal_assistant: bool = False

    @property
    def replayable(self) -> bool:
        if not self.complete or not self.terminal_assistant or not self.transcript:
            return False
        calls = {
            item.get("call_id")
            for item in self.items
            if item.get("type") == "function_call"
        }
        results = {
            item.get("call_id")
            for item in self.items
            if item.get("type") == "function_call_output"
        }
        return None not in calls and calls == results


class ConversationWindow:
    """Track and bound Realtime history without splitting tool transactions."""

    def __init__(self, max_turns: int):
        self.max_turns = max(0, int(max_turns))
        self.turns: list[ConversationTurn] = []
        self.pending_context: list[dict[str, Any]] = []
        self.active_turn_id: Optional[str] = None
        self.call_turn_ids: dict[str, str] = {}

    def _turn(self, user_item_id: str) -> Optional[ConversationTurn]:
        return next(
            (turn for turn in self.turns if turn.user_item_id == user_item_id),
            None,
        )

    def has_user_turn(self, user_item_id: str) -> bool:
        """Return whether this server user item is already known history."""
        return self._turn(user_item_id) is not None

    def begin_user_turn(self, item: dict[str, Any]) -> ConversationTurn:
        item_id = item["id"]
        existing = self._turn(item_id)
        if existing:
            return existing
        turn = ConversationTurn(
            user_item_id=item_id,
            items=[*self.pending_context, item],
        )
        self.pending_context = []
        self.turns.append(turn)
        return turn

    def add_pending_context(self, item: dict[str, Any]) -> None:
        if self.active_turn_id:
            turn = self._turn(self.active_turn_id)
            if turn:
                turn.items = [
                    current
                    for current in turn.items
                    if not (
                        current.get("type") == "message"
                        and current.get("role") == "system"
                    )
                ]
                turn.items.insert(0, item)
                return
        self.pending_context = [item]

    def activate(self, user_item_id: str) -> None:
        if not self._turn(user_item_id):
            raise ValueError(f"unknown conversation turn: {user_item_id}")
        self.active_turn_id = user_item_id

    def detach_active_turn(self) -> Optional[str]:
        """Release admission while retaining an interrupted turn for cleanup."""
        active_turn_id = self.active_turn_id
        self.active_turn_id = None
        return active_turn_id

    def observe_item(self, item: dict[str, Any]) -> None:
        item_type = item.get("type")
        call_id = item.get("call_id")
        turn_id = self.active_turn_id
        if item_type == "function_call_output" and call_id:
            turn_id = self.call_turn_ids.get(call_id)
        if not turn_id:
            return
        turn = self._turn(turn_id)
        if not turn:
            return
        for index, current in enumerate(turn.items):
            if current.get("id") == item.get("id"):
                turn.items[index] = item
                return
        turn.items.append(item)
        if item_type == "function_call" and call_id:
            self.call_turn_ids[call_id] = turn.user_item_id

    def attach_transcript(self, item_id: str, transcript: str) -> bool:
        turn = self._turn(item_id)
        if turn:
            turn.transcript = transcript.strip()
            return True
        return False

    def finish_response(
        self,
        status: str,
        output: list[dict[str, Any]],
        continuation_pending: bool = False,
        continuable_call_ids: Optional[set[str]] = None,
    ) -> bool:
        """Record response output and return whether the user-led turn ended."""
        if not self.active_turn_id:
            return False
        turn = self._turn(self.active_turn_id)
        if not turn:
            return False
        for item in output:
            self.observe_item(item)
        has_function_call = any(item.get("type") == "function_call" for item in output)
        has_assistant = any(
            item.get("type") == "message"
            and item.get("role") == "assistant"
            and bool(_text_content(item))
            for item in output
        )
        calls = {
            item.get("call_id")
            for item in turn.items
            if item.get("type") == "function_call"
        }
        results = {
            item.get("call_id")
            for item in turn.items
            if item.get("type") == "function_call_output"
        }
        unresolved_calls = calls - results
        if (
            status == "completed"
            and not has_function_call
            and not unresolved_calls
            and not continuation_pending
        ):
            turn.complete = True
            turn.terminal_assistant = has_assistant
            self.active_turn_id = None
            return True
        recoverable_calls = unresolved_calls & (continuable_call_ids or set())
        if (
            status in {"cancelled", "failed", "incomplete"}
            and not recoverable_calls
            and not continuation_pending
        ):
            turn.complete = True
            self.active_turn_id = None
            return True
        return False

    def finish_silent_control(self, call_id: str, function_name: str) -> bool:
        """Finish a turn whose terminal model decision is a silent control."""
        if not self.active_turn_id or not call_id or not function_name:
            return False
        turn = self._turn(self.active_turn_id)
        if not turn:
            return False
        calls = [
            item.get("call_id")
            for item in turn.items
            if item.get("type") == "function_call"
        ]
        results = [
            item.get("call_id")
            for item in turn.items
            if item.get("type") == "function_call_output"
        ]
        matching_call = any(
            item.get("type") == "function_call"
            and item.get("call_id") == call_id
            and item.get("name") == function_name
            for item in turn.items
        )
        assistant_output_present = any(
            item.get("type") == "message"
            and item.get("role") == "assistant"
            and bool(_text_content(item))
            for item in turn.items
        )
        if (
            not matching_call
            or assistant_output_present
            or calls != [call_id]
            or results != [call_id]
        ):
            return False
        turn.complete = True
        # A silent control is itself the terminal assistant decision; no
        # synthetic assistant speech is added to replayable context.
        turn.terminal_assistant = True
        self.active_turn_id = None
        return True

    def turns_to_prune(self) -> list[ConversationTurn]:
        if self.max_turns == 0:
            return []
        excess = len(self.turns) - self.max_turns
        if excess <= 0:
            return []
        candidates = self.turns[:excess]
        if any(not turn.replayable for turn in candidates):
            raise RuntimeError("oldest conversation turn is not safely replayable")
        return candidates

    def remove_turns(self, turns: list[ConversationTurn]) -> None:
        remove_ids = {turn.user_item_id for turn in turns}
        self.turns = [turn for turn in self.turns if turn.user_item_id not in remove_ids]
        if self.active_turn_id in remove_ids:
            self.active_turn_id = None
        self.call_turn_ids = {
            call_id: turn_id
            for call_id, turn_id in self.call_turn_ids.items()
            if turn_id not in remove_ids
        }

    def replay_snapshot(self) -> list[ConversationTurn]:
        if self.active_turn_id:
            raise RuntimeError("conversation is not quiescent")
        if any(not turn.replayable for turn in self.turns):
            raise RuntimeError("retained conversation contains an incomplete turn")
        return list(self.turns)

    def clear(self) -> None:
        self.turns = []
        self.pending_context = []
        self.active_turn_id = None
        self.call_turn_ids = {}

    def replace_item_ids(self, replacements: dict[str, str]) -> None:
        for turn in self.turns:
            turn.user_item_id = replacements.get(turn.user_item_id, turn.user_item_id)
            for item in turn.items:
                item_id = item.get("id")
                if item_id in replacements:
                    item["id"] = replacements[item_id]
        self.call_turn_ids = {
            call_id: replacements.get(turn_id, turn_id)
            for call_id, turn_id in self.call_turn_ids.items()
        }
        for item in self.pending_context:
            item_id = item.get("id")
            if item_id in replacements:
                item["id"] = replacements[item_id]

    def context_messages(self, *, include_active_user: bool = False) -> list[dict[str, Any]]:
        """Project retained complete turns into Pipecat's shared context."""
        messages: list[dict[str, Any]] = []
        for turn in self.turns:
            include_active = (
                include_active_user
                and turn.user_item_id == self.active_turn_id
                and bool(turn.transcript)
            )
            if not turn.complete and not include_active:
                # The user aggregator owns the active transcription. Projecting
                # it during pruning would duplicate its delayed frame.
                continue
            for item in turn.items:
                item_type = item.get("type")
                role = item.get("role")
                if item_type == "message" and role == "user":
                    if turn.transcript:
                        messages.append({"role": "user", "content": turn.transcript})
                elif item_type == "message" and role == "assistant":
                    messages.append({"role": role, "content": _text_content(item)})
                elif item_type == "function_call":
                    messages.append(
                        {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [
                                {
                                    "id": item["call_id"],
                                    "type": "function",
                                    "function": {
                                        "name": item["name"],
                                        "arguments": item["arguments"],
                                    },
                                }
                            ],
                        }
                    )
                elif item_type == "function_call_output":
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": item["call_id"],
                            "content": item["output"],
                        }
                    )
        return messages

    @staticmethod
    def replay_item(item: dict[str, Any], transcript: str = "") -> dict[str, Any]:
        """Convert a server item to a compact item accepted during replay."""
        item_type = item.get("type")
        role = item.get("role")
        if item_type == "message" and role == "user":
            return {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": transcript}],
            }
        if item_type == "message" and role in {"assistant", "system"}:
            text = _text_content(item)
            return {
                "type": "message",
                "role": role,
                "content": [
                    {
                        "type": "output_text" if role == "assistant" else "input_text",
                        "text": text,
                    }
                ],
            }
        if item_type == "function_call":
            return {
                "type": "function_call",
                "call_id": item["call_id"],
                "name": item["name"],
                "arguments": item["arguments"],
            }
        if item_type == "function_call_output":
            return {
                "type": "function_call_output",
                "call_id": item["call_id"],
                "output": item["output"],
            }
        raise ValueError(f"unsupported conversation item: {item_type}/{role}")
