"""Tests for complete-turn context grouping and replay."""

import unittest

from app.conversation_window import ConversationWindow


def message(item_id, role, text=""):
    content_type = "input_audio" if role == "user" else "output_audio"
    return {
        "id": item_id,
        "type": "message",
        "role": role,
        "content": [{"type": content_type, "transcript": text}],
    }


class ConversationWindowTests(unittest.TestCase):
    def _complete_plain_turn(self, window, number):
        user_id = f"user-{number}"
        window.begin_user_turn(message(user_id, "user"))
        window.attach_transcript(user_id, f"request {number}")
        window.activate(user_id)
        ended = window.finish_response(
            "completed",
            [message(f"assistant-{number}", "assistant", f"reply {number}")],
        )
        self.assertTrue(ended)

    def test_prunes_only_whole_oldest_turns(self):
        window = ConversationWindow(max_turns=2)
        for number in range(3):
            self._complete_plain_turn(window, number)

        dropped = window.turns_to_prune()

        self.assertEqual([turn.user_item_id for turn in dropped], ["user-0"])
        window.remove_turns(dropped)
        self.assertEqual(
            [turn.user_item_id for turn in window.turns],
            ["user-1", "user-2"],
        )

    def test_tool_call_and_result_are_an_indivisible_replayable_turn(self):
        window = ConversationWindow(max_turns=12)
        window.begin_user_turn(message("user-1", "user"))
        window.attach_transcript("user-1", "turn on the kitchen light")
        window.activate("user-1")
        window.observe_item(
            {
                "id": "call-item",
                "type": "function_call",
                "call_id": "call-1",
                "name": "light_on",
                "arguments": '{"room":"kitchen"}',
            }
        )
        window.observe_item(
            {
                "id": "result-item",
                "type": "function_call_output",
                "call_id": "call-1",
                "output": '{"ok":true}',
            }
        )
        window.finish_response(
            "completed",
            [message("assistant-1", "assistant", "The kitchen light is on.")],
        )

        turn = window.replay_snapshot()[0]

        self.assertTrue(turn.replayable)
        self.assertEqual(
            [item["type"] for item in turn.items],
            ["message", "function_call", "function_call_output", "message"],
        )

    def test_orphaned_tool_call_blocks_replay(self):
        window = ConversationWindow(max_turns=12)
        window.begin_user_turn(message("user-1", "user"))
        window.attach_transcript("user-1", "turn on the light")
        window.activate("user-1")
        window.observe_item(
            {
                "id": "call-item",
                "type": "function_call",
                "call_id": "call-1",
                "name": "light_on",
                "arguments": "{}",
            }
        )
        window.finish_response(
            "completed",
            [message("assistant-1", "assistant", "Done.")],
        )

        with self.assertRaisesRegex(RuntimeError, "not quiescent"):
            window.replay_snapshot()

    def test_pending_system_context_stays_with_following_user_turn(self):
        window = ConversationWindow(max_turns=12)
        window.add_pending_context(
            {
                "id": "room-context",
                "type": "message",
                "role": "system",
                "content": [{"type": "input_text", "text": "Device room: Kitchen"}],
            }
        )

        turn = window.begin_user_turn(message("user-1", "user"))

        self.assertEqual(
            [item["id"] for item in turn.items],
            ["room-context", "user-1"],
        )
        self.assertNotIn("system", [message["role"] for message in window.context_messages()])

    def test_replay_converts_audio_messages_to_text(self):
        user = ConversationWindow.replay_item(
            message("user-1", "user"), transcript="what is next"
        )
        assistant = ConversationWindow.replay_item(
            message("assistant-1", "assistant", "The calendar is clear.")
        )

        self.assertEqual(user["content"], [{"type": "input_text", "text": "what is next"}])
        self.assertEqual(
            assistant["content"],
            [{"type": "output_text", "text": "The calendar is clear."}],
        )

    def test_out_of_order_transcript_attaches_by_item_id(self):
        window = ConversationWindow(max_turns=12)
        window.begin_user_turn(message("user-1", "user"))
        window.begin_user_turn(message("user-2", "user"))

        window.attach_transcript("user-2", "second")
        window.attach_transcript("user-1", "first")

        self.assertEqual(window.turns[0].transcript, "first")
        self.assertEqual(window.turns[1].transcript, "second")

    def test_context_projection_omits_active_user_even_after_transcription(self):
        window = ConversationWindow(max_turns=12)
        window.begin_user_turn(message("user-1", "user"))
        window.activate("user-1")

        self.assertEqual(window.context_messages(), [])

        window.attach_transcript("user-1", "hello")
        self.assertEqual(window.context_messages(), [])
        self.assertEqual(
            window.context_messages(include_active_user=True),
            [{"role": "user", "content": "hello"}],
        )

        window.finish_response(
            "completed",
            [message("assistant-1", "assistant", "Hi.")],
        )
        self.assertEqual(
            window.context_messages(),
            [
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "Hi."},
            ],
        )

    def test_pending_system_context_keeps_only_latest_wake_hint(self):
        window = ConversationWindow(max_turns=12)
        window.add_pending_context(message("system-1", "system", "first"))
        window.add_pending_context(message("system-2", "system", "second"))

        self.assertEqual(
            [item["id"] for item in window.pending_context],
            ["system-2"],
        )

    def test_context_projection_keeps_tool_call_next_to_its_result(self):
        window = ConversationWindow(max_turns=12)
        window.begin_user_turn(message("user-1", "user"))
        window.attach_transcript("user-1", "turn on the light")
        window.activate("user-1")
        window.observe_item(
            {
                "id": "call-item",
                "type": "function_call",
                "call_id": "call-1",
                "name": "light_on",
                "arguments": "{}",
            }
        )
        window.observe_item(
            {
                "id": "result-item",
                "type": "function_call_output",
                "call_id": "call-1",
                "output": '{"ok":true}',
            }
        )
        window.finish_response(
            "completed", [message("assistant-1", "assistant", "Done.")]
        )

        messages = window.context_messages()

        self.assertEqual([entry["role"] for entry in messages], ["user", "assistant", "tool", "assistant"])
        self.assertEqual(messages[1]["tool_calls"][0]["id"], messages[2]["tool_call_id"])

    def test_silent_completed_response_releases_turn_but_is_not_replayable(self):
        window = ConversationWindow(max_turns=12)
        window.begin_user_turn(message("user-1", "user"))
        window.attach_transcript("user-1", "background noise")
        window.activate("user-1")

        self.assertTrue(window.finish_response("completed", []))
        self.assertIsNone(window.active_turn_id)
        self.assertFalse(window.turns[0].replayable)

    def test_tool_result_stays_with_calling_turn_after_next_user_arrives(self):
        window = ConversationWindow(max_turns=12)
        window.begin_user_turn(message("user-1", "user"))
        window.attach_transcript("user-1", "do it")
        window.activate("user-1")
        window.observe_item(
            {
                "id": "call-item",
                "type": "function_call",
                "call_id": "call-1",
                "name": "do_it",
                "arguments": "{}",
            }
        )
        window.begin_user_turn(message("user-2", "user"))

        window.observe_item(
            {
                "id": "result-item",
                "type": "function_call_output",
                "call_id": "call-1",
                "output": "done",
            }
        )

        self.assertEqual(window.turns[0].items[-1]["id"], "result-item")
        self.assertEqual([item["id"] for item in window.turns[1].items], ["user-2"])

    def test_cancelled_tool_turn_waits_for_result_and_terminal_reply(self):
        window = ConversationWindow(max_turns=12)
        window.begin_user_turn(message("user-1", "user"))
        window.attach_transcript("user-1", "do it")
        window.activate("user-1")
        call = {
            "id": "call-item",
            "type": "function_call",
            "call_id": "call-1",
            "name": "do_it",
            "arguments": "{}",
        }
        window.observe_item(call)

        self.assertFalse(
            window.finish_response(
                "cancelled", [call], continuable_call_ids={"call-1"}
            )
        )
        self.assertEqual(window.active_turn_id, "user-1")

        window.observe_item(
            {
                "id": "result-item",
                "type": "function_call_output",
                "call_id": "call-1",
                "output": "done",
            }
        )
        self.assertTrue(
            window.finish_response(
                "completed", [message("assistant-1", "assistant", "Done.")]
            )
        )

    def test_queued_tool_continuation_keeps_turn_open(self):
        window = ConversationWindow(max_turns=12)
        window.begin_user_turn(message("user-1", "user"))
        window.attach_transcript("user-1", "do both")
        window.activate("user-1")

        self.assertFalse(
            window.finish_response(
                "completed",
                [message("assistant-1", "assistant", "First result.")],
                continuation_pending=True,
            )
        )
        self.assertEqual(window.active_turn_id, "user-1")


if __name__ == "__main__":
    unittest.main()
