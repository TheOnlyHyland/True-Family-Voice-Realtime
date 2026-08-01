"""Main application entry point using Pipecat."""
import os
import sys
import asyncio
import json
import logging
from collections import deque
from typing import Optional
import dotenv
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask
from pipecat.services.openai.realtime.llm import OpenAIRealtimeLLMService
from pipecat.transports.websocket.server import WebsocketServerTransport
from app.mcp_service import HomeAssistantMCPService
from app.phase_emitter import TURN_LIVENESS
from app.disconnect_tool import get_disconnect_tool_definition, create_disconnect_tool_handler
from app.web_search_tool import get_web_search_tool_definition, create_web_search_tool_handler
from app.calendar_tool import (
    CALENDAR_TOOL_NAME,
    get_calendar_tool_definition,
    register_calendar_tool,
)
from app.room_light_tool import (
    ROOM_LIGHT_TOOL_NAME,
    get_room_light_tool_definition,
    register_room_light_tool,
)
from app.end_conversation_tool import (
    END_CONVERSATION_TOOL_NAME,
    get_end_conversation_tool_definition,
    register_end_conversation_tool,
)
from app.audio_recording_service import AudioRecordingService
from app.session_manager import SessionManager
from app.websocket_handler import WebSocketHandler
from app.speaker_context import SpeakerProbe
from app.timers import TimerRegistry, get_timer_tool_definitions, register_timer_tools
from app.announce_http import start_announce_server
from app.openclaw_tool import (
    get_openclaw_tool_definition,
    get_recall_tool_definition,
    openclaw_url,
    register_openclaw_tool,
)
from app.voice_memory import (
    memory_instructions,
    get_memory_tool_definitions,
    register_memory_tools,
)
from app.enrollment import (
    EnrollmentRecorder,
    EnrollmentConductor,
    get_enrollment_tool_definition,
    create_enrollment_tool_handler,
    get_false_alarm_tool_definition,
    create_false_alarm_tool_handler,
)
from app.conversation_window import ConversationTurn, ConversationWindow

# Speaker context v1 (fork): set at startup when speaker names are configured.
# Module-level so SafeRealtimeLLMService.register_function can gate tools
# without threading state through pipecat.
SPEAKER_PROBE = None
MALE_ONLY_TOOLS: set = set()
NON_CLOSE_TOOL_CALLBACK = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Reduce verbosity of noisy loggers
logging.getLogger("aiortc").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("__main__").setLevel(logging.INFO)


def _resolve_choice(env_var: str, custom_env_var: str, default: str) -> str:
    """Resolve a dropdown option that supports a 'custom' escape hatch.

    The add-on UI renders these as a `list(...|custom)` dropdown plus a sibling
    free-text *_custom field. When the dropdown is set to "custom", use the
    custom field's value; otherwise use the dropdown value. Falls back to
    `default` if the resolved value is empty (e.g. "custom" picked but the custom
    field left blank).
    """
    choice = os.environ.get(env_var, default).strip()
    if choice.lower() == "custom":
        custom = os.environ.get(custom_env_var, "").strip()
        if custom:
            return custom
        logger.warning(
            f"⚠️ {env_var}=custom but {custom_env_var} is empty; falling back to {default!r}"
        )
        return default
    return choice or default

dotenv.load_dotenv()


class SafeRealtimeLLMService(OpenAIRealtimeLLMService):
    """OpenAIRealtimeLLMService with audio-truncation-on-interruption disabled.

    pipecat's `_truncate_current_audio_response()` (called by `_handle_interruption`
    on EVERY interruption — both our device "stop" AND pipecat's own server-VAD
    barge-in when the user wakes/speaks mid-reply) sends a
    `conversation.item.truncate` with `audio_end_ms = wall-clock ms since audio
    start`. But OpenAI BURSTS the reply faster than real-time, so that elapsed
    value massively overshoots the audio that actually exists, and OpenAI rejects
    it with `invalid_request_error("Audio content of N ms is already shorter than
    M ms")`. That errored truncate wedges the realtime session, so the user's very
    next turn gets NO response — the recurring "interrupt, then immediately ask
    again → silence" bug (confirmed in logs: session goes quiet right after
    `_truncate_current_audio_response`).

    The device stops playback authoritatively on its own, so server-side
    truncation buys us nothing. No-op it. (Cost: OpenAI's conversation history
    keeps the full assistant text the user may not have fully heard — purely
    cosmetic for context.)
    """

    async def _truncate_current_audio_response(self):  # type: ignore[override]
        return

    # Per-response cost accounting (fork). The API reports exact token usage in
    # every response.done; pipecat only pushes it as metrics frames. Log the
    # breakdown + estimated $ (measured 2026-07-12: warm turn ≈ $0.003-0.013,
    # cold session-first turn ≈ $0.019 — the 4.4k-token instruction+tool prefix
    # uncached) and publish a daily cost sensor to HA.
    _RATES = (  # $/1M tokens: (text_in, text_out, audio_in, audio_out, cached)
        (0.60, 2.40, 10.0, 20.0, 0.06)
        if "mini" in (os.environ.get("OPENAI_MODEL_CUSTOM") or os.environ.get("OPENAI_MODEL") or "")
        else (4.0, 24.0, 32.0, 64.0, 0.40)
    )

    SESSION_READY_TIMEOUT_S = 10.0
    CONVERSATION_ITEM_TIMEOUT_S = 5.0
    RESPONSE_FINISHED_TIMEOUT_S = 60.0
    TURN_TERMINAL_TIMEOUT_S = 180.0
    TOOL_EXECUTION_LOCK_TIMEOUT_S = 10.0

    def __init__(self, *args, **kwargs):
        max_context_turns = kwargs.pop("max_context_turns", 0)
        self._manual_response_gating = kwargs.pop("manual_response_gating", False)
        self._server_vad_response_ownership = kwargs.pop(
            "server_vad_response_ownership",
            False,
        )
        self._server_vad_interrupt_response = kwargs.pop(
            "server_vad_interrupt_response",
            False,
        )
        self._managed_context = max_context_turns > 0
        super().__init__(*args, **kwargs)
        self._session_ready_event = asyncio.Event()
        self._session_generation = 0
        self._ready_session_generation = None
        self._accept_session_ready = True
        self._recovery_active = False
        self._pending_tool_result_ids = set()
        self._pending_tool_results_drained = asyncio.Event()
        self._pending_tool_results_drained.set()
        self._conversation_window = ConversationWindow(max_context_turns)
        self._turn_terminal = asyncio.Event()
        self._turn_terminal.set()
        self._turn_gate = asyncio.Lock()
        self._tool_execution_lock = asyncio.Lock()
        self._turn_tasks = set()
        self._user_turn_tasks = {}
        self._replay_item_ids = set()
        self._replay_item_acks = {}
        self._response_finished = asyncio.Event()
        self._response_finished.set()
        self._response_gate = asyncio.Lock()
        self._history_lock = asyncio.Lock()
        self._prune_lock = asyncio.Lock()
        self._continuation_task = None
        self._continuation_reservations = 0
        self._continuation_requested = False
        self._discarded_tool_result_ids = set()
        self._interrupted_tool_result_ids = set()
        self._retired_aggregator_call_ids = set()
        self._discarded_user_item_ids = set()
        self._transcript_ready_events = {}
        self._pending_overlap_deletion_ids = set()
        self._running_tool_call_ids = set()
        self._running_tool_calls_drained = asyncio.Event()
        self._running_tool_calls_drained.set()
        self._scheduled_tool_call_ids = set()
        self._scheduled_tool_calls_drained = asyncio.Event()
        self._scheduled_tool_calls_drained.set()
        self._abandoned_running_tool_ids = set()
        self._tool_call_generations = {}
        self._tool_call_details = {}
        self._tool_result_callbacks = {}
        self._tool_output_item_ids = {}
        self._overlap_reservation_ids = set()
        self._overlap_deletions_drained = asyncio.Event()
        self._overlap_deletions_drained.set()
        self._pending_context_deletion_ids = set()
        self._context_deletions_drained = asyncio.Event()
        self._context_deletions_drained.set()
        self._interrupted_response_active = False
        self._interrupted_response_generation = None
        self._interrupted_item_ids = set()
        self._interrupted_turn_ids = set()
        self._interrupted_aggregation_drained = asyncio.Event()
        self._interrupted_aggregation_drained.set()
        self._interrupted_cleanup_drained = asyncio.Event()
        self._interrupted_cleanup_drained.set()
        self._interrupted_cleanup_task = None
        self._interrupt_generation = 0
        self._interrupt_cancel_pending = False
        self._interrupt_cancel_generation = None
        self._interrupt_cancel_event_generations = {}
        self._interrupt_cancel_settled = asyncio.Event()
        self._interrupt_cancel_settled.set()
        self._interrupt_input_clear_generation = None
        self._interrupt_clear_requests = deque()
        self._assistant_context_aggregator = None
        self._assistant_end_generations = deque()
        self._response_interrupt_generations = {}
        self._active_response_id = None
        self._post_interrupt_response_quarantine = False
        self._unmanaged_active_item_ids = set()
        self._managed_response_sent = False

    @staticmethod
    def _item_dict(item):
        if isinstance(item, dict):
            return dict(item)
        return item.model_dump(exclude_none=True)

    @classmethod
    def _payload_contains(cls, actual, expected) -> bool:
        if isinstance(expected, dict):
            return isinstance(actual, dict) and all(
                key in actual and cls._payload_contains(actual[key], value)
                for key, value in expected.items()
            )
        if isinstance(expected, list):
            return (
                isinstance(actual, list)
                and len(actual) == len(expected)
                and all(
                    cls._payload_contains(current, wanted)
                    for current, wanted in zip(actual, expected)
                )
            )
        return actual == expected

    def _track_turn_task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self._turn_tasks.add(task)

        def finish_turn_task(completed_task) -> None:
            self._turn_tasks.discard(completed_task)
            if not completed_task.cancelled():
                completed_task.exception()

        task.add_done_callback(finish_turn_task)
        return task

    def _track_user_turn_task(self, item_id: str, coroutine) -> None:
        task = self._track_turn_task(coroutine)
        self._user_turn_tasks[item_id] = task

        def remove_user_task(completed_task) -> None:
            if self._user_turn_tasks.get(item_id) is completed_task:
                self._user_turn_tasks.pop(item_id, None)

        task.add_done_callback(remove_user_task)

    def bind_context_aggregator(self, aggregator_pair) -> None:
        assistant = aggregator_pair.assistant()
        self._assistant_context_aggregator = assistant
        original_handle_end = getattr(assistant, "_handle_llm_end", None)
        if original_handle_end is None:
            return

        async def handle_end_and_acknowledge(frame) -> None:
            await original_handle_end(frame)
            interrupt_generation = (
                self._assistant_end_generations.popleft()
                if self._assistant_end_generations
                else None
            )
            await self.on_assistant_response_end_processed(interrupt_generation)

        assistant._handle_llm_end = handle_end_and_acknowledge
        original_calls_started = getattr(
            assistant,
            "_handle_function_calls_started",
            None,
        )
        if original_calls_started is not None:
            async def handle_calls_started(frame) -> None:
                await original_calls_started(frame)
                function_calls = getattr(
                    assistant,
                    "_function_calls_in_progress",
                    {},
                )
                for call_id in self._retired_aggregator_call_ids:
                    if call_id not in self._pending_tool_result_ids:
                        function_calls.pop(call_id, None)

            assistant._handle_function_calls_started = handle_calls_started
        original_call_in_progress = getattr(
            assistant,
            "_handle_function_call_in_progress",
            None,
        )
        if original_call_in_progress is not None:
            async def handle_call_in_progress(frame) -> None:
                call_id = getattr(frame, "tool_call_id", None)
                if (
                    call_id in self._retired_aggregator_call_ids
                    and call_id not in self._pending_tool_result_ids
                ):
                    return
                await original_call_in_progress(frame)

            assistant._handle_function_call_in_progress = handle_call_in_progress

    def _settle_interrupt_cancel(self, generation=None) -> None:
        if (
            generation is not None
            and generation != self._interrupt_cancel_generation
        ):
            return
        self._interrupt_cancel_pending = False
        self._interrupt_cancel_generation = None
        self._interrupt_cancel_event_generations = {
            event_id: event_generation
            for event_id, event_generation in self._interrupt_cancel_event_generations.items()
            if event_generation != generation
        }
        self._interrupt_cancel_settled.set()

    def note_interrupt_cancel_event(self, event_id: str, generation=None) -> None:
        if generation is None:
            generation = self._interrupt_cancel_generation
        if generation is None:
            return
        self._interrupt_cancel_event_generations[event_id] = generation

    def note_interrupt_input_clear(self, generation: int) -> None:
        self._interrupt_input_clear_generation = generation
        self._interrupt_clear_requests.append(generation)

    def note_unscoped_input_clear(self) -> None:
        self._interrupt_clear_requests.append(None)

    def cancel_unscoped_input_clear(self) -> None:
        for index in range(len(self._interrupt_clear_requests) - 1, -1, -1):
            if self._interrupt_clear_requests[index] is None:
                del self._interrupt_clear_requests[index]
                break

    def handle_interrupt_input_cleared(self) -> None:
        if not self._interrupt_clear_requests:
            return
        generation = self._interrupt_clear_requests.popleft()
        if generation == self._interrupt_input_clear_generation:
            self._interrupt_input_clear_generation = None

    async def fail_interrupt_input_clear(
        self,
        generation: int,
        error: Exception,
    ) -> None:
        try:
            self._interrupt_clear_requests.remove(generation)
        except ValueError:
            pass
        if generation != self._interrupt_input_clear_generation:
            return
        self._interrupt_input_clear_generation = None
        self.begin_recovery()
        await self.push_error(
            error_msg=f"interrupt input clear failed: {error!r}"
        )

    async def fail_interrupt_cancel(
        self,
        generation: int,
        error: Exception,
    ) -> None:
        if generation != self._interrupt_cancel_generation:
            return
        self.begin_recovery()
        await self.push_error(
            error_msg=f"interrupt response cancel failed: {error!r}"
        )

    async def _cancel_turn_tasks(self) -> None:
        current = asyncio.current_task()
        tasks = [task for task in self._turn_tasks if task is not current]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._turn_tasks.difference_update(tasks)
        continuation = self._continuation_task
        if continuation is not None and continuation is not current:
            continuation.cancel()
            await asyncio.gather(continuation, return_exceptions=True)
        self._continuation_task = None

    async def cleanup(self):  # type: ignore[override]
        self.begin_recovery()
        await self._cancel_turn_tasks()
        await super().cleanup()

    def begin_recovery(self) -> None:
        """Suppress responses before old tools and processor queues are drained."""
        self._recovery_active = True
        self._run_llm_when_api_session_ready = False
        self._llm_needs_conversation_setup = False
        current = asyncio.current_task()
        for task in list(self._turn_tasks):
            if task is not current and not task.done():
                task.cancel()
        continuation = self._continuation_task
        if continuation is not None and continuation is not current:
            continuation.cancel()

    async def discard_running_tool_results(self) -> None:
        """Tombstone tools that outlive the bounded recovery drain."""
        stale_ids = (
            self._scheduled_tool_call_ids
            | self._running_tool_call_ids
            | self._pending_tool_result_ids
        )
        self._discarded_tool_result_ids.update(stale_ids)
        self._completed_tool_calls.update(stale_ids)
        newly_abandoned = self._running_tool_call_ids - self._abandoned_running_tool_ids
        self._abandoned_running_tool_ids.update(newly_abandoned)
        for _call_id in newly_abandoned:
            TURN_LIVENESS.tool_finished()
        for call_id in newly_abandoned:
            callback = self._tool_result_callbacks.get(call_id)
            if callback is None:
                continue
            try:
                await asyncio.shield(
                    callback(
                        {
                            "error": (
                                "The tool result was discarded because the "
                                "conversation restarted."
                            )
                        }
                    )
                )
            except Exception as error:
                logger.warning(
                    "⚠️ could not finalize abandoned tool %s: %r",
                    call_id,
                    error,
                )

    async def send_client_event(self, event):  # type: ignore[override]
        """Disable server item-level truncation while turns are client-managed."""
        if getattr(event, "type", None) == "conversation.item.create":
            item = self._item_dict(getattr(event, "item", {}))
            if item.get("type") == "function_call_output" and item.get("call_id"):
                call_id = item["call_id"]
                item_id = item.get("id")
                if item_id:
                    self._tool_output_item_ids[call_id] = str(item_id)
                    if not self._managed_context:
                        self._unmanaged_active_item_ids.add(str(item_id))
                    if (
                        self._interrupted_response_active
                        or self._post_interrupt_response_quarantine
                        or call_id in self._interrupted_tool_result_ids
                    ):
                        self._interrupted_item_ids.add(str(item_id))
        if (
            (self._managed_context or self._server_vad_response_ownership)
            and getattr(event, "type", None) == "session.update"
        ):
            payload = event.model_dump(exclude_none=True)
            if self._managed_context:
                payload["session"]["truncation"] = "disabled"
            if self._server_vad_response_ownership:
                turn_detection = payload["session"]["audio"]["input"][
                    "turn_detection"
                ]
                turn_detection["create_response"] = True
                turn_detection["interrupt_response"] = (
                    self._server_vad_interrupt_response
                )
            await self._ws_send(payload)
            return
        await super().send_client_event(event)

    def _sync_local_context(self, *, include_active_user: bool = False) -> None:
        if self._context is not None and hasattr(self._context, "set_messages"):
            self._context.set_messages(
                self._conversation_window.context_messages(
                    include_active_user=include_active_user
                )
            )

    def _continuation_pending(self) -> bool:
        return (
            self._continuation_reservations > 0
            or
            self._continuation_task is not None
            and not self._continuation_task.done()
        )

    async def _confirm_item_deleted(self, item_id: str) -> None:
        try:
            await asyncio.wait_for(
                self.retrieve_conversation_item(item_id),
                timeout=self.CONVERSATION_ITEM_TIMEOUT_S,
            )
        except TimeoutError:
            raise RuntimeError(f"timed out confirming deletion of {item_id}")
        except Exception:
            # Pipecat only fails this future for item_retrieve_invalid_item_id.
            return
        raise RuntimeError(f"OpenAI retained deleted conversation item {item_id}")

    async def _discard_overlapping_user_item(self, item_id: str) -> None:
        """Remove a semantic fragment committed while another turn is active."""
        from pipecat.services.openai.realtime import events

        try:
            await self.send_client_event(
                events.ConversationItemDeleteEvent(item_id=item_id)
            )
            await self._confirm_item_deleted(item_id)
            logger.info("🧹 discarded overlapping semantic user fragment")
        except Exception as error:
            self.begin_recovery()
            await self.push_error(
                error_msg=f"context compaction failed: {error!r}"
            )
        finally:
            self._pending_overlap_deletion_ids.discard(item_id)
            if (
                not self._pending_overlap_deletion_ids
                and not self._overlap_reservation_ids
            ):
                self._overlap_deletions_drained.set()

    async def _expire_overlap_reservation(self, item_id: str) -> None:
        await asyncio.sleep(self.CONVERSATION_ITEM_TIMEOUT_S)
        if item_id not in self._overlap_reservation_ids:
            return
        self._overlap_reservation_ids.discard(item_id)
        if not self._pending_overlap_deletion_ids:
            self._overlap_deletions_drained.set()
        self.begin_recovery()
        await self.push_error(
            error_msg=(
                "context compaction failed: overlapping speech item did not "
                f"materialize: {item_id}"
            )
        )

    async def _delete_superseded_context_item(self, item_id: str) -> None:
        from pipecat.services.openai.realtime import events

        try:
            await self.send_client_event(
                events.ConversationItemDeleteEvent(item_id=item_id)
            )
            await self._confirm_item_deleted(item_id)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.begin_recovery()
            await self.push_error(
                error_msg=(
                    "context compaction failed: superseded context item could "
                    f"not be deleted: {error!r}"
                )
            )
        finally:
            self._discarded_user_item_ids.discard(item_id)
            self._pending_context_deletion_ids.discard(item_id)
            if not self._pending_context_deletion_ids:
                self._context_deletions_drained.set()

    def _schedule_server_item_deletion(self, item_id: str) -> None:
        if item_id in self._pending_context_deletion_ids:
            return
        self._pending_context_deletion_ids.add(item_id)
        self._context_deletions_drained.clear()
        self._track_turn_task(self._delete_superseded_context_item(item_id))

    async def _run_prune_transaction(self) -> None:
        try:
            async with self._prune_lock:
                await self._prune_complete_turns()
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.begin_recovery()
            self._response_finished.set()
            self._turn_terminal.set()
            await self.push_error(
                error_msg=f"context compaction failed: {error!r}"
            )
            raise

    async def _prune_complete_turns(self) -> None:
        from pipecat.services.openai.realtime import events

        async with self._history_lock:
            dropped = self._conversation_window.turns_to_prune()
        if not dropped:
            return
        for turn in dropped:
            # Results disappear before their calls, so no intermediate state has
            # an orphan tool result in the server conversation.
            for item in reversed(turn.items):
                item_id = item.get("id")
                if not item_id:
                    raise RuntimeError("retained conversation item has no server id")
                await self.send_client_event(
                    events.ConversationItemDeleteEvent(item_id=item_id)
                )
                await self._confirm_item_deleted(item_id)
        removed_call_ids = {
            item.get("call_id")
            for turn in dropped
            for item in turn.items
            if item.get("type") == "function_call"
        }
        async with self._history_lock:
            active_turn = next(
                (
                    turn
                    for turn in self._conversation_window.turns
                    if turn.user_item_id
                    == self._conversation_window.active_turn_id
                ),
                None,
            )
            include_active_user = bool(active_turn and active_turn.transcript)
            self._conversation_window.remove_turns(dropped)
            # Remove the local shadow before re-arming deleted call IDs. Otherwise
            # Pipecat sees their retained tool outputs as new and resends them.
            self._sync_local_context(include_active_user=include_active_user)
            self._completed_tool_calls.difference_update(
                removed_call_ids - self._discarded_tool_result_ids
            )
            for call_id in removed_call_ids:
                self._tool_call_generations.pop(call_id, None)
                self._tool_call_details.pop(call_id, None)
        logger.info(
            "✂️ pruned %s complete turn(s); %s retained",
            len(dropped),
            len(self._conversation_window.turns),
        )

    async def _start_user_turn(self, item_id: str) -> None:
        """Compact first, then request exactly one response for this user item."""
        from pipecat.services.openai.realtime import events

        async with self._turn_gate:
            target_generation = self._session_generation
            try:
                await asyncio.wait_for(
                    self._turn_terminal.wait(),
                    timeout=self.TURN_TERMINAL_TIMEOUT_S,
                )
                if self._recovery_active:
                    return
                await asyncio.wait_for(
                    self._interrupted_cleanup_drained.wait(),
                    timeout=self.RESPONSE_FINISHED_TIMEOUT_S,
                )
                async with self._history_lock:
                    active_turn = self._conversation_window.active_turn_id
                    if active_turn is None:
                        self._conversation_window.activate(item_id)
                    elif active_turn != item_id:
                        raise RuntimeError(
                            f"turn {item_id} lost admission to {active_turn}"
                        )
                self._turn_terminal.clear()
                prune_task = self._track_turn_task(self._run_prune_transaction())
                await asyncio.shield(prune_task)
                transcript_ready = self._transcript_ready_events.get(item_id)
                if transcript_ready is None:
                    raise RuntimeError(
                        f"missing transcript readiness gate for {item_id}"
                    )
                await asyncio.wait_for(
                    transcript_ready.wait(),
                    timeout=self.CONVERSATION_ITEM_TIMEOUT_S,
                )
                if not await self.wait_for_scheduled_tool_calls():
                    raise RuntimeError("interrupted scheduled tools did not drain")
                if not await self.wait_for_pending_tool_results():
                    raise RuntimeError("interrupted tool results did not drain")
                async with self._history_lock:
                    self._sync_local_context(include_active_user=True)
                    self._transcript_ready_events.pop(item_id, None)
                if not self._api_session_ready or self._websocket is None:
                    raise RuntimeError("OpenAI session is not ready for a bounded turn")
                async with self._response_gate:
                    await asyncio.wait_for(
                        self._interrupt_cancel_settled.wait(),
                        timeout=self.RESPONSE_FINISHED_TIMEOUT_S,
                    )
                    await asyncio.wait_for(
                        self._response_finished.wait(),
                        timeout=self.RESPONSE_FINISHED_TIMEOUT_S,
                    )
                    await asyncio.wait_for(
                        self._overlap_deletions_drained.wait(),
                        timeout=self.CONVERSATION_ITEM_TIMEOUT_S,
                    )
                    await asyncio.wait_for(
                        self._context_deletions_drained.wait(),
                        timeout=self.CONVERSATION_ITEM_TIMEOUT_S,
                    )
                    if (
                        self._recovery_active
                        or target_generation != self._session_generation
                    ):
                        return
                    self._post_interrupt_response_quarantine = False
                    self._managed_response_sent = False
                    self._response_finished.clear()
                    await self.send_client_event(
                        events.ResponseCreateEvent(
                            response=events.ResponseProperties(
                                output_modalities=self._get_enabled_modalities()
                            )
                        )
                    )
                    self._managed_response_sent = True
            except Exception as error:
                recovery_was_active = self._recovery_active
                self.begin_recovery()
                self._response_finished.set()
                self._turn_terminal.set()
                logger.error(f"❌ bounded conversation gate failed: {error!r}")
                if not recovery_was_active:
                    await self.push_error(
                        error_msg=f"context compaction failed: {error!r}"
                    )

    async def _handle_evt_conversation_item_added(self, evt):  # type: ignore[override]
        item = self._item_dict(evt.item)
        item_id = item.get("id")
        replayed = item_id in self._replay_item_ids
        if replayed:
            self._messages_added_manually.pop(item_id, None)
            acknowledgement = self._replay_item_acks.get(item_id)
            if acknowledgement is not None and not acknowledgement.done():
                acknowledgement.set_result(item)
            return
        if self._recovery_active:
            if (
                item.get("type") == "message"
                and item.get("role") == "user"
                and item_id
            ):
                self._discarded_user_item_ids.add(str(item_id))
            logger.info("🔇 old-session conversation item suppressed during recovery")
            return
        manually_added = item_id in self._messages_added_manually
        if item.get("type") == "function_call" and item.get("call_id"):
            self._tool_call_generations[item["call_id"]] = self._session_generation
            self._pending_function_calls.setdefault(item["call_id"], evt.item)
        await self._call_event_handler(
            "on_conversation_item_created",
            item_id,
            evt.item,
        )
        async with self._history_lock:
            if self._messages_added_manually.get(item_id):
                del self._messages_added_manually[item_id]
            elif (
                item.get("type") == "message"
                and item.get("role") == "assistant"
                and not (
                    self._recovery_active
                    or self._interrupted_response_active
                    or self._post_interrupt_response_quarantine
                )
            ):
                from pipecat.frames.frames import LLMFullResponseStartFrame

                self._current_assistant_response = evt.item
                await self.push_frame(LLMFullResponseStartFrame())
            if (
                self._recovery_active
                or self._interrupted_response_active
                or self._post_interrupt_response_quarantine
            ) and (
                item.get("type") == "function_call"
                or (
                    item.get("type") == "function_call_output"
                    and item.get("call_id") in self._interrupted_tool_result_ids
                )
                or (
                    item.get("type") == "message"
                    and item.get("role") == "assistant"
                )
            ):
                if item_id:
                    self._interrupted_item_ids.add(str(item_id))
                call_id = item.get("call_id")
                if call_id:
                    self._pending_function_calls.pop(call_id, None)
                    self._discarded_tool_result_ids.add(call_id)
                    self._completed_tool_calls.add(call_id)
                return
            if (
                not manually_added
                and (
                    item.get("type") == "function_call"
                    or (
                        item.get("type") == "message"
                        and item.get("role") == "assistant"
                    )
                )
            ):
                self._response_finished.clear()
            if (
                item.get("type") == "message"
                and item.get("role") == "user"
                and self._interrupt_input_clear_generation is not None
            ):
                self._discarded_user_item_ids.add(str(item_id))
                self._pending_overlap_deletion_ids.add(str(item_id))
                self._overlap_deletions_drained.clear()
                self._track_turn_task(
                    self._discard_overlapping_user_item(str(item_id))
                )
                return
            if manually_added or not self._managed_context:
                if not self._managed_context and (
                    item.get("type") == "function_call"
                    or (
                        item.get("type") == "message"
                        and item.get("role") == "user"
                    )
                ):
                    self._unmanaged_active_item_ids.add(str(item_id))
                    self._post_interrupt_response_quarantine = False
                return
            if item.get("type") == "message" and item.get("role") == "system":
                if self._conversation_window.active_turn_id:
                    active_turn = next(
                        (
                            turn
                            for turn in self._conversation_window.turns
                            if turn.user_item_id
                            == self._conversation_window.active_turn_id
                        ),
                        None,
                    )
                    existing_context_ids = [
                        current.get("id")
                        for current in (active_turn.items if active_turn else [])
                        if current.get("type") == "message"
                        and current.get("role") == "system"
                    ]
                else:
                    existing_context_ids = [
                        current.get("id")
                        for current in self._conversation_window.pending_context
                    ]
                self._conversation_window.add_pending_context(item)
                for existing_id in existing_context_ids:
                    if existing_id and existing_id != item_id:
                        self._schedule_server_item_deletion(str(existing_id))
            elif item.get("type") == "message" and item.get("role") == "user":
                reserved_overlap = str(item_id) in self._overlap_reservation_ids
                if reserved_overlap:
                    self._overlap_reservation_ids.discard(str(item_id))
                if reserved_overlap or self._conversation_window.active_turn_id:
                    self._discarded_user_item_ids.add(str(item_id))
                    self._pending_overlap_deletion_ids.add(str(item_id))
                    self._overlap_deletions_drained.clear()
                    self._track_turn_task(
                        self._discard_overlapping_user_item(str(item_id))
                    )
                    return
                self._conversation_window.begin_user_turn(item)
                self._conversation_window.activate(str(item_id))
                self._transcript_ready_events[str(item_id)] = asyncio.Event()
                if self._manual_response_gating and item_id:
                    self._track_user_turn_task(
                        str(item_id),
                        self._start_user_turn(str(item_id)),
                    )
            else:
                self._conversation_window.observe_item(item)

    async def _handle_evt_conversation_item_done(self, evt):  # type: ignore[override]
        item = self._item_dict(evt.item)
        if (
            item.get("id") in self._replay_item_ids
            or item.get("id") in self._discarded_user_item_ids
        ):
            return
        async with self._history_lock:
            interrupted_item = item.get("id") in self._interrupted_item_ids
            if (
                self._managed_context
                and not interrupted_item
                and not (
                    item.get("type") == "message"
                    and item.get("role") in {"user", "system"}
                )
            ):
                self._conversation_window.observe_item(item)
            await super()._handle_evt_conversation_item_done(evt)

    async def handle_evt_input_audio_transcription_completed(self, evt):  # type: ignore[override]
        if evt.item_id in self._discarded_user_item_ids:
            return
        async with self._history_lock:
            if self._managed_context:
                if not evt.transcript.strip():
                    self.begin_recovery()
                    await self.push_error(
                        error_msg=(
                            "context compaction failed: input transcription was "
                            f"blank for item {evt.item_id}"
                        )
                    )
                    return
                attached = self._conversation_window.attach_transcript(
                    evt.item_id,
                    evt.transcript,
                )
                if not attached:
                    logger.info(
                        "🔇 transcript for unknown historical item suppressed: %s",
                        evt.item_id,
                    )
                    return
                transcript_ready = self._transcript_ready_events.get(evt.item_id)
                if transcript_ready is not None:
                    transcript_ready.set()
                return
            await super().handle_evt_input_audio_transcription_completed(evt)

    async def _handle_evt_speech_stopped(self, evt):  # type: ignore[override]
        if self._managed_context and self._conversation_window.active_turn_id:
            item_id = str(evt.item_id)
            self._overlap_reservation_ids.add(item_id)
            self._overlap_deletions_drained.clear()
            self._track_turn_task(self._expire_overlap_reservation(item_id))
        await super()._handle_evt_speech_stopped(evt)

    async def _handle_evt_function_call_arguments_done(self, evt):  # type: ignore[override]
        if (
            self._interrupted_response_active
            or self._post_interrupt_response_quarantine
        ):
            self._pending_function_calls.pop(evt.call_id, None)
            self._discarded_tool_result_ids.add(evt.call_id)
            self._completed_tool_calls.add(evt.call_id)
            logger.info(
                "🔇 interrupted response tool call suppressed: %s",
                evt.call_id,
            )
            return
        if self._recovery_active:
            self._pending_function_calls.pop(evt.call_id, None)
            logger.info(
                "🔇 historical tool call suppressed during OpenAI recovery: %s",
                evt.call_id,
            )
            return
        try:
            arguments = json.loads(evt.arguments)
        except Exception as error:
            await self._fail_tool_dispatch(
                evt.call_id,
                f"malformed function arguments: {error!r}",
            )
            return
        function_call_item = self._pending_function_calls.get(evt.call_id)
        function_name = getattr(function_call_item, "name", None)
        if function_call_item is None or not function_name:
            await self._fail_tool_dispatch(evt.call_id, "untracked function call")
            return
        if function_name not in self._functions and None not in self._functions:
            self._pending_function_calls.pop(evt.call_id, None)
            await self._fail_tool_dispatch(
                evt.call_id,
                f"unregistered function: {function_name}",
            )
            return
        self._tool_call_details[evt.call_id] = (function_name, arguments)
        self._scheduled_tool_call_ids.add(evt.call_id)
        self._scheduled_tool_calls_drained.clear()
        await super()._handle_evt_function_call_arguments_done(evt)
        self._track_turn_task(self._verify_tool_dispatch_started(evt.call_id))

    def _remove_scheduled_tool_call(self, call_id: str) -> None:
        self._scheduled_tool_call_ids.discard(call_id)
        if not self._scheduled_tool_call_ids:
            self._scheduled_tool_calls_drained.set()

    async def _fail_tool_dispatch(self, call_id: str, reason: str) -> None:
        self._pending_function_calls.pop(call_id, None)
        self._remove_scheduled_tool_call(call_id)
        self._discarded_tool_result_ids.add(call_id)
        self._completed_tool_calls.add(call_id)
        self.begin_recovery()
        await self.push_error(
            error_msg=f"context compaction failed: tool dispatch failed: {reason}"
        )

    async def suppress_function_call_after_interrupt(self, call_id: str) -> None:
        """Tombstone a post-stop function item before arguments can dispatch."""
        self._pending_function_calls.pop(call_id, None)
        self._discarded_tool_result_ids.add(call_id)
        self._interrupted_tool_result_ids.add(call_id)
        self._retired_aggregator_call_ids.add(call_id)
        self._completed_tool_calls.add(call_id)

    async def suppress_tools_at_interrupt(self) -> int:
        """Quarantine the current turn without splitting an active tool action."""
        self._interrupt_generation += 1
        interrupt_generation = self._interrupt_generation
        self._overlap_reservation_ids.clear()
        if not self._pending_overlap_deletion_ids:
            self._overlap_deletions_drained.set()
        pending_ids = set(self._pending_function_calls)
        active_ids = (
            self._scheduled_tool_call_ids
            | self._running_tool_call_ids
            | self._pending_tool_result_ids
        )
        continuation_active = (
            self._continuation_requested
            or self._continuation_task is not None
            and not self._continuation_task.done()
        )
        response_active = not self._response_finished.is_set()
        active_turn_id = self._conversation_window.active_turn_id
        self._interrupted_item_ids.update(self._unmanaged_active_item_ids)
        if (
            not pending_ids
            and not active_ids
            and not continuation_active
            and not active_turn_id
            and not self._unmanaged_active_item_ids
            and not response_active
        ):
            return interrupt_generation
        self._interrupted_response_active = True
        self._interrupted_response_generation = interrupt_generation
        if self._active_response_id:
            self._response_interrupt_generations[
                self._active_response_id
            ] = interrupt_generation
        self._interrupted_cleanup_drained.clear()
        if response_active:
            self._interrupt_cancel_pending = True
            self._interrupt_cancel_generation = interrupt_generation
            self._interrupt_cancel_settled.clear()
            self._interrupted_aggregation_drained.clear()
        else:
            self._interrupted_aggregation_drained.set()
        for call_id in pending_ids:
            self._pending_function_calls.pop(call_id, None)
            self._discarded_tool_result_ids.add(call_id)
            self._completed_tool_calls.add(call_id)
        self._discarded_tool_result_ids.update(active_ids)
        self._interrupted_tool_result_ids.update(pending_ids | active_ids)
        self._retired_aggregator_call_ids.update(pending_ids | active_ids)
        newly_abandoned = (
            self._running_tool_call_ids - self._abandoned_running_tool_ids
        )
        self._abandoned_running_tool_ids.update(newly_abandoned)
        for _call_id in newly_abandoned:
            TURN_LIVENESS.tool_finished()
        for call_id in list(active_ids & self._scheduled_tool_call_ids):
            self._remove_scheduled_tool_call(call_id)

        continuation = self._continuation_task
        if continuation is not None and continuation is not asyncio.current_task():
            continuation.cancel()
        self._continuation_requested = False

        if active_turn_id:
            user_task = self._user_turn_tasks.pop(active_turn_id, None)
            if user_task is not None and user_task is not asyncio.current_task():
                user_task.cancel()
                await asyncio.gather(user_task, return_exceptions=True)
                self._turn_tasks.discard(user_task)
            async with self._history_lock:
                turn = next(
                    (
                        current
                        for current in self._conversation_window.turns
                        if current.user_item_id == active_turn_id
                    ),
                    None,
                )
                if turn is not None:
                    self._interrupted_turn_ids.add(turn.user_item_id)
                    self._interrupted_item_ids.update(
                        str(item["id"])
                        for item in turn.items
                        if item.get("id")
                    )
                    for item in turn.items:
                        call_id = item.get("call_id")
                        output_item_id = self._tool_output_item_ids.get(call_id)
                        if output_item_id:
                            self._interrupted_item_ids.add(output_item_id)
                self._conversation_window.detach_active_turn()
                self._discarded_user_item_ids.add(active_turn_id)
                self._transcript_ready_events.pop(active_turn_id, None)
                self._turn_terminal.set()

        if not response_active:
            await self._retire_interrupted_aggregator_state()
            self._schedule_interrupted_cleanup()
        return interrupt_generation

    def _schedule_interrupted_cleanup(self) -> None:
        if (
            self._interrupted_cleanup_task is not None
            and not self._interrupted_cleanup_task.done()
        ):
            return
        self._interrupted_cleanup_task = self._track_turn_task(
            self._finish_interrupted_cleanup()
        )

    async def _finish_interrupted_cleanup(self) -> None:
        cancelled = False
        try:
            while True:
                cleanup_generation = self._interrupt_generation
                await self._interrupted_aggregation_drained.wait()
                await self._pending_tool_results_drained.wait()
                if cleanup_generation != self._interrupt_generation:
                    continue
                async with self._history_lock:
                    if cleanup_generation != self._interrupt_generation:
                        continue
                    interrupted_turns = [
                        turn
                        for turn in self._conversation_window.turns
                        if turn.user_item_id in self._interrupted_turn_ids
                    ]
                    self._conversation_window.remove_turns(interrupted_turns)
                    active_turn = next(
                        (
                            turn
                            for turn in self._conversation_window.turns
                            if turn.user_item_id
                            == self._conversation_window.active_turn_id
                        ),
                        None,
                    )
                    self._sync_local_context(
                        include_active_user=bool(active_turn and active_turn.transcript)
                    )
                    self._interrupted_turn_ids.clear()
                for item_id in self._interrupted_item_ids:
                    self._schedule_server_item_deletion(item_id)
                self._interrupted_item_ids.clear()
                self._interrupted_response_active = False
                self._interrupted_response_generation = None
                self._unmanaged_active_item_ids.clear()
                break
        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            self._interrupted_cleanup_task = None
            if not cancelled:
                if self._interrupted_response_active:
                    self._interrupted_cleanup_drained.clear()
                    if self._interrupted_aggregation_drained.is_set():
                        self._schedule_interrupted_cleanup()
                else:
                    self._interrupted_cleanup_drained.set()

    async def _retire_interrupted_aggregator_state(self) -> None:
        if self._assistant_context_aggregator is not None:
            function_calls = getattr(
                self._assistant_context_aggregator,
                "_function_calls_in_progress",
                {},
            )
            for call_id in self._retired_aggregator_call_ids:
                if call_id not in self._pending_tool_result_ids:
                    function_calls.pop(call_id, None)
            if getattr(self._assistant_context_aggregator, "_started", 0) < 0:
                self._assistant_context_aggregator._started = 0
            await self._assistant_context_aggregator.reset()

    async def on_assistant_response_end_processed(
        self,
        interrupt_generation,
    ) -> None:
        """Complete interrupted cleanup after Pipecat consumes the end frame."""
        if (
            not self._interrupted_response_active
            or interrupt_generation != self._interrupted_response_generation
        ):
            return
        await self._retire_interrupted_aggregator_state()
        self._interrupted_aggregation_drained.set()
        self._schedule_interrupted_cleanup()

    async def mark_interrupted_response(self) -> None:
        """Quarantine every item in the response currently being cancelled."""
        if not self._interrupted_response_active:
            await self.suppress_tools_at_interrupt()
        if not self._interrupt_cancel_pending:
            self._interrupt_generation += 1
            self._interrupt_cancel_generation = self._interrupt_generation
        self._interrupted_response_active = True
        self._interrupted_response_generation = self._interrupt_generation
        self._interrupted_cleanup_drained.clear()
        self._interrupted_aggregation_drained.clear()
        self._interrupt_cancel_pending = True
        self._interrupt_cancel_settled.clear()
        self._response_finished.clear()

    async def _verify_tool_dispatch_started(self, call_id: str) -> None:
        await asyncio.sleep(0.1)
        if call_id not in self._scheduled_tool_call_ids:
            return
        try:
            await self._finalize_scheduled_tool_call(call_id)
        except Exception as error:
            logger.error(
                "❌ scheduled tool finalization failed (%s): %r",
                call_id,
                error,
            )
        self.begin_recovery()
        await self.push_error(
            error_msg=(
                "context compaction failed: scheduled tool handler did not start: "
                f"{call_id}"
            )
        )

    async def _finalize_scheduled_tool_call(self, call_id: str) -> None:
        from pipecat.frames.frames import FunctionCallResultFrame

        details = self._tool_call_details.get(call_id)
        self._remove_scheduled_tool_call(call_id)
        self._discarded_tool_result_ids.add(call_id)
        self._completed_tool_calls.add(call_id)
        if details is None:
            return
        function_name, arguments = details
        self._pending_tool_result_ids.add(call_id)
        self._pending_tool_results_drained.clear()
        await self.broadcast_frame(
            FunctionCallResultFrame,
            function_name=function_name,
            tool_call_id=call_id,
            arguments=arguments,
            result={
                "error": "The tool was discarded because the conversation restarted."
            },
            run_llm=True,
        )

    async def wait_for_scheduled_tool_calls(self) -> bool:
        if not self._scheduled_tool_call_ids:
            return True
        try:
            await asyncio.wait_for(
                self._scheduled_tool_calls_drained.wait(),
                timeout=self.CONVERSATION_ITEM_TIMEOUT_S,
            )
            return True
        except TimeoutError:
            for call_id in list(self._scheduled_tool_call_ids):
                await self._finalize_scheduled_tool_call(call_id)
            return False

    async def _handle_evt_session_updated(self, evt):  # type: ignore[override]
        """Publish readiness only for the active receive-loop generation."""
        receive_task = self._receive_task
        current_receive_task = asyncio.current_task()
        accepts_readiness = (
            self._accept_session_ready
            and receive_task is not None
            and current_receive_task is receive_task
        )
        suppress_response = self._recovery_active
        if suppress_response:
            self._run_llm_when_api_session_ready = False
            self._llm_needs_conversation_setup = False

        await super()._handle_evt_session_updated(evt)

        if suppress_response:
            # Guard against the parent implementation changing either flag while
            # processing session.updated. Recovery must never create a response.
            self._run_llm_when_api_session_ready = False
            self._llm_needs_conversation_setup = False

        receive_alive = receive_task is not None and (
            not hasattr(receive_task, "done") or not receive_task.done()
        )
        if (
            accepts_readiness
            and self._api_session_ready
            and self._websocket is not None
            and receive_alive
        ):
            self._ready_session_generation = self._session_generation
            self._session_ready_event.set()

    async def _create_response(self):  # type: ignore[override]
        """Coalesce post-tool continuations behind the current response.done."""
        if self._recovery_active:
            self._run_llm_when_api_session_ready = False
            logger.info("🔇 response creation suppressed during OpenAI recovery")
            return
        if (
            self._interrupted_response_active
            or self._post_interrupt_response_quarantine
        ):
            logger.info("🔇 response creation suppressed for interrupted response")
            return
        if not self._managed_context and not self._server_vad_response_ownership:
            await super()._create_response()
            return
        if not self._api_session_ready:
            await super()._create_response()
            return
        if (
            self._continuation_task is not None
            and not self._continuation_task.done()
        ):
            self._continuation_requested = True
            return
        self._continuation_requested = False
        generation = self._session_generation
        task = asyncio.create_task(self._run_tool_continuation(generation))
        self._continuation_task = task

    async def _run_tool_continuation(self, generation: int) -> None:
        from pipecat.services.openai.realtime import events

        try:
            async with self._response_gate:
                await asyncio.wait_for(
                    self._response_finished.wait(),
                    timeout=self.RESPONSE_FINISHED_TIMEOUT_S,
                )
                while True:
                    while (
                        (
                            self._scheduled_tool_call_ids
                            | self._running_tool_call_ids
                        )
                        - self._discarded_tool_result_ids
                    ) and not self._recovery_active:
                        await asyncio.sleep(0.05)
                    if not await self.wait_for_pending_tool_results():
                        self.begin_recovery()
                        await self.push_error(
                            error_msg=(
                                "context compaction failed: tool result queue timed out"
                            )
                        )
                        return
                    await asyncio.sleep(0)
                    if not (
                        (
                            self._scheduled_tool_call_ids
                            | self._running_tool_call_ids
                        )
                        - self._discarded_tool_result_ids
                    ) and not self._pending_tool_result_ids:
                        await asyncio.wait_for(
                            self._overlap_deletions_drained.wait(),
                            timeout=self.CONVERSATION_ITEM_TIMEOUT_S,
                        )
                        await asyncio.wait_for(
                            self._context_deletions_drained.wait(),
                            timeout=self.CONVERSATION_ITEM_TIMEOUT_S,
                        )
                        if self._continuation_requested:
                            self._continuation_requested = False
                            continue
                        if (
                            self._recovery_active
                            or generation != self._session_generation
                        ):
                            return
                        self._response_finished.clear()
                        await self.send_client_event(
                            events.ResponseCreateEvent(
                                response=events.ResponseProperties(
                                    output_modalities=self._get_enabled_modalities()
                                )
                            )
                        )
                        break
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.begin_recovery()
            self._response_finished.set()
            await self.push_error(
                error_msg=f"context compaction failed: {error!r}"
            )
        finally:
            if self._continuation_task is asyncio.current_task():
                self._continuation_task = None
                if self._continuation_requested and not self._recovery_active:
                    self._continuation_requested = False
                    await self._create_response()

    async def _handle_context(self, context):  # type: ignore[override]
        """Consume old tool results without allowing a post-recovery reply."""
        context_generation = self._interrupt_generation
        matching_pending_results = {
            message.get("tool_call_id")
            for message in context.get_messages()
            if isinstance(message, dict)
            and message.get("tool_call_id") in self._pending_tool_result_ids
            and message.get("content") != "IN_PROGRESS"
        }
        self._completed_tool_calls.update(self._discarded_tool_result_ids)
        if (
            self._recovery_active
            or self._interrupted_response_active
            or bool(matching_pending_results & self._discarded_tool_result_ids)
        ):
            # The old tool may have changed the home, but its result belongs to a
            # dead conversation generation. Drain it locally and fail the active
            # turn fresh rather than sending an orphan output to the new session.
            self._context = context
        else:
            if matching_pending_results:
                self._continuation_reservations += 1
            try:
                await super()._handle_context(context)
            finally:
                if matching_pending_results:
                    self._continuation_reservations = max(
                        0,
                        self._continuation_reservations - 1,
                    )
            if context_generation != self._interrupt_generation:
                self._discarded_tool_result_ids.update(matching_pending_results)
                self._interrupted_tool_result_ids.update(matching_pending_results)
                self._completed_tool_calls.update(matching_pending_results)
                for call_id in matching_pending_results:
                    item_id = self._tool_output_item_ids.get(call_id)
                    if item_id:
                        self._interrupted_item_ids.add(item_id)
        if matching_pending_results:
            self._pending_tool_result_ids.difference_update(matching_pending_results)
            if not self._pending_tool_result_ids:
                self._pending_tool_results_drained.set()

    async def wait_for_pending_tool_results(self) -> bool:
        """Wait until queued tool results cross the processor queues."""
        while self._pending_tool_result_ids:
            try:
                await asyncio.wait_for(
                    self._pending_tool_results_drained.wait(),
                    timeout=self.CONVERSATION_ITEM_TIMEOUT_S,
                )
            except TimeoutError:
                logger.warning(
                    "⚠️ old tool results did not drain before recovery; "
                    "discarding their conversation state"
                )
                timed_out = set(self._pending_tool_result_ids)
                self._discarded_tool_result_ids.update(timed_out)
                self._completed_tool_calls.update(timed_out)
                self._pending_tool_result_ids.clear()
                self._pending_tool_results_drained.set()
                return False
        return True

    def mark_recovery_complete(self) -> None:
        """Re-enable model responses after the ready session has no old tools."""
        self._run_llm_when_api_session_ready = False
        self._llm_needs_conversation_setup = False
        self._recovery_active = False
        self._response_finished.set()

    async def _handle_evt_response_done(self, evt):  # type: ignore[override]
        if self._recovery_active:
            response_id = getattr(evt.response, "id", None)
            self._response_interrupt_generations.pop(response_id, None)
            if response_id == self._active_response_id:
                self._active_response_id = None
            self._response_finished.set()
            self._managed_response_sent = False
            logger.info("🔇 old-session response completion suppressed during recovery")
            return
        unreplayable_terminal = False
        response_id = getattr(evt.response, "id", None)
        response_was_active = (
            response_id is None
            or response_id == self._active_response_id
        )
        interrupt_generation = self._response_interrupt_generations.pop(
            response_id,
            None,
        )
        if response_id is None and self._interrupted_response_active:
            interrupt_generation = self._interrupted_response_generation
        interrupted_response = interrupt_generation is not None
        if response_id == self._active_response_id:
            self._active_response_id = None
        process_aggregator_end = response_was_active or (
            interrupted_response
            and self._interrupted_response_active
            and interrupt_generation == self._interrupted_response_generation
        )
        if (
            process_aggregator_end
            and (
            self._managed_context or self._server_vad_response_ownership
            )
        ) and not any(
            self._item_dict(item).get("role") == "assistant"
            for item in evt.response.output
        ):
            # Managed response.create bypasses Pipecat's client-side start frame.
            # Assistant messages provide their own start on item.added, but a
            # tool-call-only response does not. Balance response.done's end frame
            # so the assistant aggregator cannot go negative and lose the final
            # spoken tool reply.
            from pipecat.frames.frames import LLMFullResponseStartFrame

            await self.push_frame(LLMFullResponseStartFrame())
        async with self._history_lock:
            turn_ended = (
                self._conversation_window.finish_response(
                    evt.response.status,
                    [self._item_dict(item) for item in evt.response.output],
                    continuation_pending=self._continuation_pending(),
                    continuable_call_ids=(
                        self._running_tool_call_ids
                        | self._scheduled_tool_call_ids
                        | self._pending_tool_result_ids
                    ),
                )
                if (
                    self._managed_context
                    and response_was_active
                    and not interrupted_response
                )
                else False
            )
            if turn_ended and self._conversation_window.turns:
                unreplayable_terminal = not self._conversation_window.turns[-1].replayable
            try:
                u = evt.response.usage
                itd = getattr(u, "input_token_details", None)
                otd = getattr(u, "output_token_details", None)
                ctd = getattr(itd, "cached_tokens_details", None)
                in_text = getattr(itd, "text_tokens", 0) or 0
                in_audio = getattr(itd, "audio_tokens", 0) or 0
                cached = getattr(itd, "cached_tokens", 0) or 0
                c_text = getattr(ctd, "text_tokens", 0) or 0
                c_audio = getattr(ctd, "audio_tokens", 0) or 0
                out_text = getattr(otd, "text_tokens", 0) or 0
                out_audio = getattr(otd, "audio_tokens", 0) or 0
                ti, to, ai, ao, ca = self._RATES
                cost = (max(0, in_text - c_text) * ti + max(0, in_audio - c_audio) * ai
                        + cached * ca + out_text * to + out_audio * ao) / 1e6
                logger.info(
                    f"💰 usage: in {in_text}txt+{in_audio}aud (cached {cached}) "
                    f"out {out_text}txt+{out_audio}aud ≈ ${cost:.4f}"
                )
                from .ha_sensors import PUBLISHER
                asyncio.get_running_loop().create_task(PUBLISHER.usage(cost, {
                    "in_text": in_text, "in_audio": in_audio, "cached": cached,
                    "out_text": out_text, "out_audio": out_audio}))
            except Exception as e:
                logger.debug(f"usage accounting failed: {e!r}")
            try:
                if (
                    process_aggregator_end
                    and self._assistant_context_aggregator is not None
                ):
                    self._assistant_end_generations.append(
                        interrupt_generation if interrupted_response else None
                    )
                if process_aggregator_end:
                    await super()._handle_evt_response_done(evt)
            finally:
                if turn_ended:
                    self._turn_terminal.set()
                if response_was_active:
                    self._response_finished.set()
                    self._managed_response_sent = False
        if interrupted_response:
            for item in evt.response.output:
                item_id = self._item_dict(item).get("id")
                if item_id:
                    self._interrupted_item_ids.add(str(item_id))
            self._post_interrupt_response_quarantine = True
            self._settle_interrupt_cancel(interrupt_generation)
            if self._assistant_context_aggregator is None:
                await self.on_assistant_response_end_processed(
                    interrupt_generation
                )
            return
        if not self._managed_context and response_was_active:
            self._unmanaged_active_item_ids.update(
                str(item_id)
                for item in evt.response.output
                if (item_id := self._item_dict(item).get("id"))
            )
        if unreplayable_terminal:
            self.begin_recovery()
            await self.push_error(
                error_msg=(
                    "context compaction failed: terminal turn was not safely replayable"
                )
            )
        if response_was_active and not interrupted_response and not (
            self._continuation_pending()
            or self._running_tool_call_ids
            or self._scheduled_tool_call_ids
            or self._pending_tool_result_ids
            or any(
                self._item_dict(item).get("type") == "function_call"
                for item in evt.response.output
            )
        ):
            self._unmanaged_active_item_ids.clear()

    async def _replay_history(
        self,
        turns: list[ConversationTurn],
        pending_context: list[dict],
    ) -> None:
        """Populate a fresh server conversation without generating a reply."""
        from pipecat.services.openai.realtime import events

        replacements = {}
        replay_call_ids = set()
        attempted_ids = set()
        replay_succeeded = False
        try:
            self._pending_function_calls.clear()
            replay_entries = []
            for turn in turns:
                replay_entries.extend(
                    (
                        item,
                        turn.transcript if item.get("id") == turn.user_item_id else "",
                    )
                    for item in turn.items
                )
            replay_entries.extend((item, "") for item in pending_context)

            for original, transcript in replay_entries:
                payload = self._conversation_window.replay_item(original, transcript)
                item = events.ConversationItem(**payload)
                old_id = original.get("id")
                if not old_id:
                    raise RuntimeError("replay item has no original server id")
                replacements[old_id] = item.id
                if item.type == "function_call" and item.call_id:
                    replay_call_ids.add(item.call_id)
                self._replay_item_ids.add(item.id)
                attempted_ids.add(item.id)
                acknowledgement = asyncio.get_running_loop().create_future()
                self._replay_item_acks[item.id] = acknowledgement
                await self.send_client_event(events.ConversationItemCreateEvent(item=item))
                acknowledged = await asyncio.wait_for(
                    acknowledgement,
                    timeout=self.CONVERSATION_ITEM_TIMEOUT_S,
                )
                expected = {"id": item.id, **payload}
                if not self._payload_contains(acknowledged, expected):
                    raise RuntimeError(
                        f"OpenAI acknowledged different replay content for {item.id}"
                    )
                retrieved = await asyncio.wait_for(
                    self.retrieve_conversation_item(item.id),
                    timeout=self.CONVERSATION_ITEM_TIMEOUT_S,
                )
                if not self._payload_contains(self._item_dict(retrieved), expected):
                    raise RuntimeError(
                        f"OpenAI read-back differed for replay item {item.id}"
                    )
                self._replay_item_acks.pop(item.id, None)

            self._pending_function_calls.clear()
            self._completed_tool_calls.update(replay_call_ids)
            self._conversation_window.replace_item_ids(replacements)
            self._sync_local_context()
            if replay_entries:
                logger.info(
                    "♻️ silently replayed %s complete turn(s) across OpenAI reconnect",
                    len(turns),
                )
            replay_succeeded = True
        except Exception as error:
            async with self._history_lock:
                self._conversation_window.clear()
                if self._context is not None and hasattr(self._context, "set_messages"):
                    self._context.set_messages([])
            await self._disconnect()
            raise RuntimeError(
                f"OpenAI conversation replay failed closed: {error!r}"
            ) from error
        finally:
            if not replay_succeeded:
                self._replay_item_ids.clear()
            for item_id in attempted_ids:
                acknowledgement = self._replay_item_acks.pop(item_id, None)
                if acknowledgement is not None and not acknowledgement.done():
                    acknowledgement.cancel()

    async def reset_conversation(self):  # type: ignore[override]
        """Reconnect and wait for authoritative API-session readiness.

        pipecat's reset_conversation() (used by ConnectionRecovery on a 60-min cap
        / keepalive drop) reconnects and leaves `_llm_needs_conversation_setup =
        True`. The collision: if a turn was mid-flight when the WS dropped,
        `_create_response()` had already set `_run_llm_when_api_session_ready =
        True` (because `_api_session_ready` went False on disconnect). After the
        reconnect, the `session.updated` handler sees that flag and fires
        `_create_response()` — but under semantic_vad (`create_response=true`) the
        SERVER also auto-creates a response for the user's next turn. Two
        response.create events collide → `conversation_already_has_active_response`,
        and that turn gets no answer (observed: first turn right after a reconnect
        fails, ~1 in 20 reconnects — whenever the user happens to speak in the few
        seconds just after a reconnect).

        Recovery suppresses response creation, waits for generation-bound session
        readiness, then silently rebuilds the bounded complete-turn history with
        conversation.item.create. It never runs historical tools or asks the model
        to answer during replay.
        """
        self.begin_recovery()
        await self._cancel_turn_tasks()
        self._turn_terminal.set()
        self._response_finished.set()
        async with self._history_lock:
            try:
                retained_turns = (
                    self._conversation_window.replay_snapshot()
                    if self._managed_context
                    else []
                )
                pending_context = (
                    list(self._conversation_window.pending_context)
                    if self._managed_context
                    else []
                )
                self._sync_local_context()
            except RuntimeError as error:
                logger.warning(
                    "⚠️ conversation cannot be replayed safely; starting fresh: %s",
                    error,
                )
                retained_turns = []
                pending_context = []
                self._conversation_window.clear()
                if self._context is not None and hasattr(self._context, "set_messages"):
                    self._context.set_messages([])

            self._accept_session_ready = False
            self._session_ready_event.clear()
            self._ready_session_generation = None

            # Snapshot and disconnect share one history barrier, so no late event
            # can enter the retained ledger after the snapshot was taken.
            await self._disconnect()
            if self._assistant_context_aggregator is not None:
                function_calls = getattr(
                    self._assistant_context_aggregator,
                    "_function_calls_in_progress",
                    {},
                )
                function_calls.clear()
                self._assistant_context_aggregator._started = 0
                await self._assistant_context_aggregator.reset()
            self._replay_item_ids.clear()
            self._replay_item_acks.clear()
            self._discarded_user_item_ids.clear()
            self._transcript_ready_events.clear()
            self._pending_overlap_deletion_ids.clear()
            self._overlap_reservation_ids.clear()
            self._overlap_deletions_drained.set()
            self._pending_context_deletion_ids.clear()
            self._context_deletions_drained.set()
            self._interrupted_response_active = False
            self._interrupted_response_generation = None
            self._post_interrupt_response_quarantine = False
            self._unmanaged_active_item_ids.clear()
            self._active_response_id = None
            self._response_interrupt_generations.clear()
            self._assistant_end_generations.clear()
            self._interrupted_item_ids.clear()
            self._interrupted_turn_ids.clear()
            self._interrupted_tool_result_ids.intersection_update(
                self._running_tool_call_ids
            )
            self._interrupted_aggregation_drained.set()
            self._interrupted_cleanup_drained.set()
            self._interrupted_cleanup_task = None
            self._settle_interrupt_cancel()
            self._interrupt_cancel_event_generations.clear()
            self._interrupt_input_clear_generation = None
            self._interrupt_clear_requests.clear()
            self._user_turn_tasks.clear()
            self._managed_response_sent = False
            self._continuation_requested = False
            self._continuation_reservations = 0
            self._scheduled_tool_call_ids.clear()
            self._scheduled_tool_calls_drained.set()
            self._running_tool_call_ids.clear()
            self._running_tool_calls_drained.set()
            self._tool_call_details.clear()
            self._tool_result_callbacks.clear()
            self._tool_output_item_ids.clear()

        if self._context is None:
            from pipecat.processors.aggregators.llm_context import LLMContext
            self._context = LLMContext()

        self._llm_needs_conversation_setup = False
        await self._process_completed_function_calls(send_new_results=False)

        self._session_generation += 1
        target_generation = self._session_generation
        self._api_session_ready = False
        self._accept_session_ready = True
        await self._connect()

        # Pipecat 0.0.97 converts connection failures to ErrorFrame and returns,
        # so reset_conversation() completing does not prove that a socket exists.
        if self._websocket is None or self._receive_task is None:
            raise RuntimeError("OpenAI Realtime reconnect did not create a receive loop")

        try:
            await asyncio.wait_for(
                self._session_ready_event.wait(),
                timeout=self.SESSION_READY_TIMEOUT_S,
            )
        except TimeoutError as error:
            raise RuntimeError(
                "OpenAI Realtime reconnect timed out before session.updated"
            ) from error

        receive_task = self._receive_task
        if (
            not self._api_session_ready
            or self._ready_session_generation != target_generation
            or self._websocket is None
            or receive_task is None
            or (hasattr(receive_task, "done") and receive_task.done())
        ):
            raise RuntimeError("OpenAI Realtime receive loop died before readiness")

        await self._replay_history(retained_turns, pending_context)

        self._run_llm_when_api_session_ready = False
        self._llm_needs_conversation_setup = False

    # Error codes that must NOT kill the realtime session. pipecat 0.0.97's
    # _receive_task_handler does `_handle_evt_error(evt); return` on EVERY
    # error event — the reader task dies, the in-flight reply cuts off
    # mid-sentence and the session is deaf until the next connection death.
    # Observed live (2026-06-10): semantic_vad split one utterance into two
    # turns, the server's auto-created second response collided with the
    # first → conversation_already_has_active_response → the playing reply
    # stopped at 4.4 s and the session wedged. These codes are harmless
    # protocol races; the right move is to keep reading.
    BENIGN_ERROR_CODES = {
        # The server auto-created a response while one was still active
        # (VAD split a sentence into two turns). The active response keeps
        # streaming — nothing is broken.
        "conversation_already_has_active_response",
        # response.cancel landed without an active response. The reader stays
        # alive long enough to enter the explicit reconnect path below.
        "response_cancel_not_active",
        # input_audio_buffer.commit raced our input_audio_buffer.clear (device
        # "stop"): an empty commit is exactly the outcome we wanted.
        "input_audio_buffer_commit_empty",
    }

    async def _maybe_handle_evt_retrieve_conversation_item_error(self, evt):  # type: ignore[override]
        """Generic benign-error filter, hooked into pipecat's receive loop.

        pipecat's `_receive_task_handler` treats a True return from this
        method as "error handled — keep the receive loop alive"; every other
        error event kills the reader task (`_handle_evt_error` + `return`).
        It is the ONLY surviving path, so besides the original retrieve-item
        case (super()), we declare our benign protocol races handled here
        instead of letting them cut off live audio and wedge the session.
        """
        if await super()._maybe_handle_evt_retrieve_conversation_item_error(evt):
            return True
        code = getattr(getattr(evt, "error", None), "code", None)
        if code in self.BENIGN_ERROR_CODES:
            if code == "response_cancel_not_active":
                client_event_id = getattr(
                    getattr(evt, "error", None),
                    "event_id",
                    None,
                )
                generation = self._interrupt_cancel_event_generations.pop(
                    client_event_id,
                    None,
                )
                if generation == self._interrupt_cancel_generation:
                    self._settle_interrupt_cancel(generation)
                    self._response_finished.set()
                    self.begin_recovery()
                    await self._retire_interrupted_aggregator_state()
                    self._interrupted_aggregation_drained.set()
                    self._schedule_interrupted_cleanup()
                    await self.push_error(
                        error_msg=(
                            "context compaction failed: interrupt response "
                            "ownership became ambiguous; "
                            "reconnecting before the replacement turn"
                        )
                    )
                    logger.warning(
                        "⚠️ response cancellation had no active owner; reconnecting"
                    )
                    return True
            logger.warning(
                f"⚠️ benign realtime error ignored (session stays alive): {code}"
            )
            return True
        return False

    def register_function(self, function_name, handler, start_callback=None, *,
                          cancel_on_interruption: bool = True):  # type: ignore[override]
        """Force cancel_on_interruption=False for every tool registration.

        pipecat cancels in-flight function-call tasks on EVERY user-speech
        interruption — and semantic_vad fires one per utterance fragment, so
        merely continuing your own sentence kills the tool call your previous
        fragment started. By then the HTTP request to Home Assistant has
        usually already been SENT: the action executes, but its result never
        reaches the model, which then tells the user it failed (observed
        live: the lights turned ON while the assistant claimed they
        wouldn't). Our tools are all short-lived (HA service calls, one web
        search), so letting them finish and report the truth always beats
        killing them halfway. This single override covers every registration
        path (MCP tools via pipecat's MCPClient, web_search, disconnect).

        The handler is also wrapped to tick TURN_LIVENESS around its run, so
        the PhaseEmitter's thinking-watchdog knows a tool is in flight and a
        slow tool (web search: 10-20 s of pipeline silence) is never mistaken
        for a dead turn. All our handlers use the single-param
        FunctionCallParams signature, so the wrapper does too (pipecat
        inspects the signature to pick the calling convention).
        """
        async def liveness_tracked(params):
            self._remove_scheduled_tool_call(params.tool_call_id)
            original_result_callback = params.result_callback
            result_reported = False

            async def generation_tracked_result(result, *, properties=None):
                nonlocal result_reported
                if result_reported:
                    logger.info(
                        "🔇 duplicate tool result suppressed: %s",
                        params.tool_call_id,
                    )
                    return
                if params.tool_call_id in self._interrupted_tool_result_ids:
                    result_reported = True
                    self._completed_tool_calls.add(params.tool_call_id)
                    return
                # Pipecat's callback queues a FunctionCallResultFrame and returns
                # before the context aggregator consumes it. Track every queued
                # result so recovery cannot miss the pre-reset boundary race.
                self._pending_tool_result_ids.add(params.tool_call_id)
                self._pending_tool_results_drained.clear()
                try:
                    await original_result_callback(result, properties=properties)
                    result_reported = True
                except Exception:
                    self._pending_tool_result_ids.discard(params.tool_call_id)
                    if not self._pending_tool_result_ids:
                        self._pending_tool_results_drained.set()
                    raise

            params.result_callback = generation_tracked_result
            self._tool_result_callbacks[params.tool_call_id] = generation_tracked_result
            call_generation = self._tool_call_generations.get(params.tool_call_id)
            if (
                self._recovery_active
                or params.tool_call_id in self._discarded_tool_result_ids
                or call_generation is None
                or call_generation != self._session_generation
            ):
                self._discarded_tool_result_ids.add(params.tool_call_id)
                logger.info(
                    "🔇 stale tool execution suppressed during OpenAI recovery: %s",
                    params.tool_call_id,
                )
                try:
                    await params.result_callback(
                        {
                            "error": (
                                "The tool was discarded because the "
                                "conversation restarted."
                            )
                        }
                    )
                finally:
                    self._completed_tool_calls.add(params.tool_call_id)
                    self._tool_result_callbacks.pop(params.tool_call_id, None)
                    self._interrupted_tool_result_ids.discard(params.tool_call_id)
                return
            # Speaker gate (fork): tools listed in male_only_tools only execute
            # when the last voice-type verdict is "male". Enforced HERE — below
            # the model — so prompt tricks can't bypass it. Fails closed on
            # uncertain/stale/absent verdicts. This is convenience gating on a
            # voice-type heuristic, not biometric auth.
            if MALE_ONLY_TOOLS and function_name in MALE_ONLY_TOOLS:
                speaker = SPEAKER_PROBE.gate_speaker() if SPEAKER_PROBE else "unknown"
                if speaker != "male":
                    owner = (SPEAKER_PROBE.male_name if SPEAKER_PROBE else "") or "the owner"
                    logger.info(f"⛔ speaker gate blocked '{function_name}' (speaker={speaker})")
                    await params.result_callback({
                        "error": (
                            f"Not available: this capability is reserved for {owner}, "
                            f"and the current speaker's voice was not recognized as {owner}. "
                            f"Relay this politely."
                        )
                    })
                    self._tool_result_callbacks.pop(params.tool_call_id, None)
                    return
            self._running_tool_call_ids.add(params.tool_call_id)
            self._running_tool_calls_drained.clear()
            is_non_close_tool = function_name != END_CONVERSATION_TOOL_NAME
            if is_non_close_tool:
                TURN_LIVENESS.non_close_tool_started()
            TURN_LIVENESS.tool_started()
            try:
                try:
                    await asyncio.wait_for(
                        self._tool_execution_lock.acquire(),
                        timeout=self.TOOL_EXECUTION_LOCK_TIMEOUT_S,
                    )
                except asyncio.TimeoutError:
                    if not result_reported:
                        await params.result_callback(
                            {
                                "error": (
                                    "A previous home action is still completing. "
                                    "Ask the user to try this action again shortly."
                                )
                            }
                        )
                    return
                try:
                    if (
                        self._recovery_active
                        or params.tool_call_id in self._discarded_tool_result_ids
                        or self._tool_call_generations.get(params.tool_call_id)
                        != self._session_generation
                    ):
                        if not result_reported:
                            await params.result_callback(
                                {
                                    "error": (
                                        "The tool was stopped before its action began."
                                    )
                                }
                            )
                        return
                    if is_non_close_tool and NON_CLOSE_TOOL_CALLBACK is not None:
                        await NON_CLOSE_TOOL_CALLBACK()
                    if (
                        self._recovery_active
                        or params.tool_call_id in self._discarded_tool_result_ids
                        or self._tool_call_generations.get(params.tool_call_id)
                        != self._session_generation
                    ):
                        if not result_reported:
                            await params.result_callback(
                                {
                                    "error": (
                                        "The tool was stopped before its action began."
                                    )
                                }
                            )
                        return
                    result = await handler(params)
                    if not result_reported:
                        await params.result_callback(
                            {
                                "error": (
                                    "The requested tool returned without a result. "
                                    "Tell the user briefly that the action could not be completed."
                                )
                            }
                        )
                    return result
                finally:
                    self._tool_execution_lock.release()
            except asyncio.CancelledError:
                if not result_reported:
                    try:
                        await params.result_callback(
                            {
                                "error": (
                                    "The tool was cancelled because the "
                                    "conversation restarted."
                                )
                            }
                        )
                    except Exception as callback_error:
                        logger.warning(
                            "⚠️ cancelled tool could not be finalized (%s): %r",
                            params.tool_call_id,
                            callback_error,
                        )
                raise
            except Exception as error:
                logger.error(
                    "❌ tool handler failed (%s): %r",
                    function_name,
                    error,
                )
                if not result_reported:
                    try:
                        await params.result_callback(
                            {
                                "error": (
                                    "The requested tool failed before returning a result. "
                                    "Tell the user briefly that the action could not be completed."
                                )
                            }
                        )
                    except Exception as callback_error:
                        self.begin_recovery()
                        await self.push_error(
                            error_msg=(
                                "context compaction failed: tool error result could not "
                                f"be queued: {callback_error!r}"
                            )
                        )
            finally:
                if params.tool_call_id in self._abandoned_running_tool_ids:
                    self._abandoned_running_tool_ids.discard(params.tool_call_id)
                else:
                    TURN_LIVENESS.tool_finished()
                self._running_tool_call_ids.discard(params.tool_call_id)
                self._interrupted_tool_result_ids.discard(params.tool_call_id)
                if not self._running_tool_call_ids:
                    self._running_tool_calls_drained.set()
                self._tool_result_callbacks.pop(params.tool_call_id, None)
                if params.tool_call_id not in self._discarded_tool_result_ids:
                    self._tool_call_generations.pop(params.tool_call_id, None)
                    self._tool_call_details.pop(params.tool_call_id, None)

        super().register_function(
            function_name, liveness_tracked, start_callback, cancel_on_interruption=False
        )

    async def _receive_task_handler(self):  # type: ignore[override]
        """Surface OpenAI reader death as an ErrorFrame so recovery can act.

        pipecat's receive loop can end without producing ANY ErrorFrame: a
        silent server-side close ends the `async for` normally, and a network
        drop raises ConnectionClosed, which the task manager merely LOGS
        ("unexpected exception"). Nothing reaches ConnectionRecovery either
        way, so the session sat deaf for HOURS until the next user utterance
        hit the dead socket — losing that utterance (observed live twice).
        Wrap the loop and report its end; ConnectionRecovery treats the
        message as a reconnect trigger.
        """
        from pipecat.services.openai.realtime import events

        try:
            async for message in self._websocket:
                evt = events.parse_server_event(message)
                if evt.type == "session.created":
                    await self._handle_evt_session_created(evt)
                elif evt.type == "session.updated":
                    await self._handle_evt_session_updated(evt)
                elif evt.type == "input_audio_buffer.cleared":
                    self.handle_interrupt_input_cleared()
                elif evt.type == "response.output_audio.delta":
                    if not (
                        self._recovery_active
                        or self._interrupted_response_active
                        or self._post_interrupt_response_quarantine
                    ):
                        await self._handle_evt_audio_delta(evt)
                elif evt.type == "response.output_audio.done":
                    await self._handle_evt_audio_done(evt)
                elif evt.type == "response.created":
                    self._managed_response_sent = True
                    self._response_finished.clear()
                    response_id = getattr(evt.response, "id", None)
                    self._active_response_id = response_id
                    quarantine_response = (
                        self._recovery_active
                        or self._interrupt_cancel_pending
                        or self._post_interrupt_response_quarantine
                    )
                    if quarantine_response:
                        await self.mark_interrupted_response()
                        if response_id:
                            self._response_interrupt_generations[
                                response_id
                            ] = self._interrupted_response_generation
                        cancel_event = events.ResponseCancelEvent()
                        self.note_interrupt_cancel_event(cancel_event.event_id)
                        await self.send_client_event(cancel_event)
                    elif response_id:
                        self._response_interrupt_generations[response_id] = None
                elif evt.type == "conversation.item.added":
                    await self._handle_evt_conversation_item_added(evt)
                elif evt.type == "conversation.item.done":
                    await self._handle_evt_conversation_item_done(evt)
                elif evt.type == "conversation.item.input_audio_transcription.delta":
                    await self._handle_evt_input_audio_transcription_delta(evt)
                elif evt.type == "conversation.item.input_audio_transcription.completed":
                    await self.handle_evt_input_audio_transcription_completed(evt)
                elif evt.type == "conversation.item.input_audio_transcription.failed":
                    known_turn = any(
                        turn.user_item_id == evt.item_id
                        for turn in self._conversation_window.turns
                    )
                    if (
                        not self._managed_context
                        or evt.item_id in self._discarded_user_item_ids
                        or not known_turn
                    ):
                        logger.info(
                            "🔇 optional or discarded transcription failure ignored: %s",
                            evt.item_id,
                        )
                        continue
                    self.begin_recovery()
                    await self.push_error(
                        error_msg=(
                            "context compaction failed: input transcription failed "
                            f"for item {evt.item_id}"
                        )
                    )
                    return
                elif evt.type == "conversation.item.retrieved":
                    await self._handle_conversation_item_retrieved(evt)
                elif evt.type == "response.done":
                    await self._handle_evt_response_done(evt)
                elif evt.type == "input_audio_buffer.speech_started":
                    await self._handle_evt_speech_started(evt)
                elif evt.type == "input_audio_buffer.speech_stopped":
                    await self._handle_evt_speech_stopped(evt)
                elif evt.type == "response.output_text.delta":
                    if not (
                        self._recovery_active
                        or self._interrupted_response_active
                        or self._post_interrupt_response_quarantine
                    ):
                        await self._handle_evt_text_delta(evt)
                elif evt.type == "response.output_audio_transcript.delta":
                    if not (
                        self._recovery_active
                        or self._interrupted_response_active
                        or self._post_interrupt_response_quarantine
                    ):
                        await self._handle_evt_audio_transcript_delta(evt)
                elif evt.type == "response.function_call_arguments.done":
                    await self._handle_evt_function_call_arguments_done(evt)
                elif evt.type == "error":
                    if not await self._maybe_handle_evt_retrieve_conversation_item_error(evt):
                        await self._handle_evt_error(evt)
                        await self.push_error(
                            error_msg=(
                                "realtime receive loop ended after OpenAI error"
                            )
                        )
                        return
        except asyncio.CancelledError:
            raise  # our own disconnect/reset tearing the task down — not a death
        except Exception as e:
            await self.push_error(error_msg=f"realtime receive loop died: {e!r}")
            return
        # Loop ended without an exception: a clean server-side close, or the
        # fatal-error path (which already pushed its own ErrorFrame —
        # duplicates collapse in ConnectionRecovery's cooldown/guard).
        await self.push_error(error_msg="realtime receive loop ended — connection closed")


class Application:
    """Main application class using Pipecat."""
    
    def __init__(self):
        """Initialize application."""
        self.pipeline: Optional[Pipeline] = None
        self.runner: Optional[PipelineRunner] = None
        self.websocket_handler: Optional[WebSocketHandler] = None
        self.websocket_transport: Optional[WebsocketServerTransport] = None
        self.openai_service: Optional[OpenAIRealtimeLLMService] = None
        self.mcp_service: Optional[HomeAssistantMCPService] = None
        self.audio_recording_service: Optional[AudioRecordingService] = None
        self.session_manager: Optional[SessionManager] = None
        self.current_task: Optional[PipelineTask] = None
        self._pipeline_lock: Optional[asyncio.Lock] = None
        self.ha_access_token = ""
        
    async def initialize(self) -> None:
        """Initialize all components."""
        # Get configuration from environment
        openai_api_key = os.environ.get("OPENAI_API_KEY")
        websocket_port = int(os.environ.get("WEBSOCKET_PORT", "8080"))
        websocket_host = os.environ.get("WEBSOCKET_HOST", "0.0.0.0")
        
        # Get turn detection settings with defaults
        vad_threshold = float(os.environ.get("VAD_THRESHOLD", "0.5"))
        vad_prefix_padding_ms = int(os.environ.get("VAD_PREFIX_PADDING_MS", "300"))
        vad_silence_duration_ms = int(os.environ.get("VAD_SILENCE_DURATION_MS", "800"))

        # Turn detection mode. "semantic_vad" is OpenAI's recommended mode for
        # natural conversation: it detects a *semantic* end-of-utterance instead
        # of a fixed silence window, so it doesn't cut the user off on a pause
        # and is more resistant to speaker->mic echo. "server_vad" is the classic
        # silence-based detector tuned by the vad_* values above.
        turn_detection_type = os.environ.get("TURN_DETECTION_TYPE", "semantic_vad").strip().lower()
        if turn_detection_type not in ("semantic_vad", "server_vad"):
            logger.warning(f"⚠️ Unknown TURN_DETECTION_TYPE '{turn_detection_type}', falling back to semantic_vad")
            turn_detection_type = "semantic_vad"
        # semantic_vad eagerness: "low" waits longest before deciding the user is
        # done (fewest mid-sentence cut-offs). low | medium | high | auto.
        vad_eagerness = os.environ.get("VAD_EAGERNESS", "low").strip().lower()
        if vad_eagerness not in ("low", "medium", "high", "auto"):
            logger.warning(f"⚠️ Unknown VAD_EAGERNESS '{vad_eagerness}', falling back to low")
            vad_eagerness = "low"
        # Whether detected user speech may interrupt the assistant's reply
        # (handsfree barge-in). With imperfect device-side AEC, set this false so
        # speaker echo can't cut replies short; interrupt then only via the
        # device "stop" wake word / center button.
        interrupt_response = os.environ.get("INTERRUPT_RESPONSE", "false").strip().lower() == "true"
        # Who creates the OpenAI response each user turn (semantic_vad only).
        # TRUE (default) = the server creates a response on every detected
        # end-of-turn. This is REQUIRED for multi-turn: Pipecat 0.0.97's realtime
        # service only auto-creates a response for the FIRST context (turn 1) and
        # after tool results; plain 2nd/3rd user turns get NO response unless the
        # server makes it. FALSE reproduces the old single-turn-only behaviour
        # (turn 1 answers, turn 2 hangs in "thinking"). See _ensure_openai_service.
        semantic_vad_create_response = os.environ.get("SEMANTIC_VAD_CREATE_RESPONSE", "true").strip().lower() == "true"
        # Expose the `disconnect_client` tool to the model. DEFAULT FALSE: on the
        # Voice PE the device owns its own session lifecycle (wake word starts a
        # turn, the no-speech watchdog / idle phase ends it), so a model-driven
        # disconnect just tears down the persistent WebSocket mid-conversation —
        # it was seen closing the socket DURING the first reply ("conversation_ended").
        # Only enable if your device relies on the backend to hang up.
        enable_disconnect_tool = os.environ.get("ENABLE_DISCONNECT_TOOL", "false").strip().lower() == "true"
        speaker_male_name = os.environ.get("SPEAKER_MALE_NAME", "").strip()
        speaker_female_name = os.environ.get("SPEAKER_FEMALE_NAME", "").strip()
        male_only_tools = {t.strip() for t in os.environ.get("MALE_ONLY_TOOLS", "").split(",") if t.strip()}
        # Pin the input-transcription language (ISO code, e.g. "nl"). Empty = let
        # the model auto-detect. Helps stop the model drifting to another
        # language; pair it with an explicit language lock in `instructions`.
        transcription_language = os.environ.get("TRANSCRIPTION_LANGUAGE", "").strip()
        # Model that transcribes the user's speech to TEXT (the transcript shown
        # in logs + put in the context). NOTE: this is NOT what gpt-realtime-2
        # uses to understand you — the main model hears the audio natively; this
        # only affects the side-channel transcript. Default "gpt-4o-transcribe".
        # Alternatives: "gpt-4o-mini-transcribe", "whisper-1", and the newer
        # streaming "gpt-realtime-whisper" (purpose-built for the Realtime API,
        # faster/cheaper). If the API rejects a value, transcription silently
        # falls back; check the logs.
        transcription_model = _resolve_choice(
            "TRANSCRIPTION_MODEL", "TRANSCRIPTION_MODEL_CUSTOM", "gpt-4o-transcribe"
        )

        # Get instructions with default
        instructions = os.environ.get("INSTRUCTIONS", "You are the Home Assistant Voice Agent and can control the Smart Home.")

        # OpenAI Realtime model + voice. These are dropdowns in the add-on UI with
        # a "custom" sentinel + a sibling *_CUSTOM free-text field; _resolve_choice
        # returns the custom value when the dropdown is "custom", else the dropdown.
        openai_model = _resolve_choice("OPENAI_MODEL", "OPENAI_MODEL_CUSTOM", "gpt-realtime-2.1")
        openai_voice = _resolve_choice("OPENAI_VOICE", "OPENAI_VOICE_CUSTOM", "marin")

        # Playback speed (post-generation rate): 0.25-1.5, 1.0 = normal. Clamped.
        try:
            openai_speed = float(os.environ.get("OPENAI_SPEED", "1.0"))
        except (TypeError, ValueError):
            openai_speed = 1.0
        openai_speed = max(0.25, min(1.5, openai_speed))
        # Max reply length in output tokens. 0 = unlimited (API default). Caps a
        # runaway monologue + bounds per-response output-token cost.
        try:
            max_output_tokens = int(os.environ.get("MAX_OUTPUT_TOKENS", "0"))
        except (TypeError, ValueError):
            max_output_tokens = 0
        # Pass None when 0/unset so SessionProperties omits it (API default "inf").
        max_output_tokens = max_output_tokens if max_output_tokens > 0 else None
        # Input noise reduction: "near_field" | "far_field" | "" (off). Anything
        # else is treated as off so a typo can't reach the API.
        noise_reduction = os.environ.get("NOISE_REDUCTION", "").strip().lower()
        if noise_reduction not in ("near_field", "far_field"):
            noise_reduction = ""

        # Optional allow-list to trim the (large) ha-mcp tool set exposed to the
        # model. Comma-separated tool names; empty means expose all.
        mcp_tool_allowlist = [t.strip() for t in os.environ.get("MCP_TOOL_ALLOWLIST", "").split(",") if t.strip()]
        
        # Web search: let the assistant look things up online (weather, news,
        # facts). ON by default; existing installs keep their saved option, so an
        # Update won't silently flip it. When on, a `web_search` function tool
        # calls OpenAI's Responses web_search built-in tool server-side (using
        # OPENAI_API_KEY) and returns a short spoken answer. The model is
        # configurable so a different price/quality — or a renamed model — needs
        # no code change.
        enable_web_search = os.environ.get("ENABLE_WEB_SEARCH", "true").lower() == "true"
        web_search_model = _resolve_choice(
            "WEB_SEARCH_MODEL", "WEB_SEARCH_MODEL_CUSTOM", "gpt-5.5"
        )

        # Get recording setting (optional, defaults to false)
        enable_recording = os.environ.get("ENABLE_RECORDING", "false").lower() == "true"
        
        # Post-reply follow-up window: how many seconds the device keeps the mic
        # open after the assistant finishes so the user can answer back without
        # re-saying the wake word. Sent to the device in the `hello` handshake as
        # follow_up_ms; the device opens the mic (after its TTS tail drains) and
        # shows the listening LED for that long. 0 disables (turn-based).
        try:
            follow_up_listen_seconds = int(os.environ.get("FOLLOW_UP_LISTEN_SECONDS", "8"))
        except (TypeError, ValueError):
            follow_up_listen_seconds = 8
        follow_up_listen_seconds = max(0, min(60, follow_up_listen_seconds))
        follow_up_ms = follow_up_listen_seconds * 1000
        # Delay (ms) before the follow-up mic opens, bridging the device speaker's
        # hardware tail so the mic doesn't catch the reply's own end. Sent to the
        # device in `hello`; lower = snappier, higher = safer against echo.
        try:
            follow_up_open_delay_ms = int(os.environ.get("FOLLOW_UP_OPEN_DELAY_MS", "700"))
        except (TypeError, ValueError):
            follow_up_open_delay_ms = 700
        follow_up_open_delay_ms = max(0, min(5000, follow_up_open_delay_ms))
        # Same idea at the WAKE boundary: delay (ms) after the wake chime before
        # the mic opens, so the chime's own hardware tail doesn't leak into the
        # fresh mic and become a ghost turn (the wake-path twin of
        # follow_up_open_delay_ms — the yaml wake handler reads it via a lambda).
        try:
            wake_open_delay_ms = int(os.environ.get("WAKE_OPEN_DELAY_MS", "700"))
        except (TypeError, ValueError):
            wake_open_delay_ms = 700
        wake_open_delay_ms = max(0, min(5000, wake_open_delay_ms))
        # Playback jitter buffer (ms): the device holds incoming TTS until this
        # much has accumulated before playing, so a brief network hiccup doesn't
        # dry out the speaker chain mid-word (audible crackle). Sent in `hello`.
        try:
            playback_prebuffer_ms = int(os.environ.get("PLAYBACK_PREBUFFER_MS", "150"))
        except (TypeError, ValueError):
            playback_prebuffer_ms = 150
        playback_prebuffer_ms = max(0, min(2000, playback_prebuffer_ms))

        # Get session reuse timeout and initialize session manager
        session_reuse_timeout = float(os.environ.get("SESSION_REUSE_TIMEOUT_SECONDS", "300"))
        # Complete user-led turns retained in both the live OpenAI conversation
        # and reconnect replay. Tool calls and outputs are never split.
        try:
            max_context_messages = int(os.environ.get("MAX_CONTEXT_MESSAGES", "12"))
        except (TypeError, ValueError):
            max_context_messages = 12
        max_context_messages = max(0, max_context_messages)
        self.session_manager = SessionManager(
            reuse_timeout=session_reuse_timeout,
            max_restored_messages=max_context_messages,
        )
        logger.info(
            f"Session reuse timeout: {session_reuse_timeout} seconds, "
            f"max complete context turns: {max_context_messages or 'disabled'}"
        )
        
        if not openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
        
        # Initialize Home Assistant MCP Service
        mcp_client = None
        ha_access_token = os.environ.get("LONGLIVED_TOKEN") or os.environ.get("SUPERVISOR_TOKEN", "")
        try:
            ha_mcp_url = os.environ.get("HA_MCP_URL", "http://supervisor/core/api/mcp")
            if ha_access_token:
                logger.info("Loading Home Assistant MCP tools...")
                self.mcp_service = HomeAssistantMCPService(url=ha_mcp_url, access_token=ha_access_token)
                mcp_client = await self.mcp_service.initialize()
                logger.info("✅ Home Assistant MCP Client initialized")
            else:
                logger.warning("⚠️ SUPERVISOR_TOKEN not set, skipping Home Assistant MCP integration")
        except Exception as e:
            logger.warning(f"⚠️ Failed to initialize Home Assistant MCP Client: {e}")
        
        # Initialize WebSocket handler
        self.websocket_handler = WebSocketHandler(
            host=websocket_host,
            port=websocket_port,
            session_manager=self.session_manager,
            audio_recording_service=self.audio_recording_service,
            follow_up_ms=follow_up_ms,
            follow_up_open_delay_ms=follow_up_open_delay_ms,
            wake_open_delay_ms=wake_open_delay_ms,
            playback_prebuffer_ms=playback_prebuffer_ms,
        )
        global NON_CLOSE_TOOL_CALLBACK
        NON_CLOSE_TOOL_CALLBACK = self.websocket_handler.cancel_graceful_close
        logger.info(
            f"🔁 Follow-up window: {follow_up_listen_seconds}s "
            f"({'enabled' if follow_up_ms > 0 else 'disabled — turn-based'}), "
            f"mic-open delay {follow_up_open_delay_ms}ms, "
            f"wake-open delay {wake_open_delay_ms}ms, "
            f"playback prebuffer {playback_prebuffer_ms}ms"
        )
        # Speaker context v1 (fork): enabled when at least one name is set.
        global SPEAKER_PROBE, MALE_ONLY_TOOLS
        if speaker_male_name or speaker_female_name:
            SPEAKER_PROBE = SpeakerProbe(speaker_male_name, speaker_female_name)
            MALE_ONLY_TOOLS = male_only_tools
            self.websocket_handler.speaker_probe = SPEAKER_PROBE
            logger.info(
                f"🗣️ Speaker context enabled: male={speaker_male_name or '-'} "
                f"female={speaker_female_name or '-'}"
                f"{f', male-only tools: {sorted(male_only_tools)}' if male_only_tools else ''}"
            )
        elif male_only_tools:
            logger.warning("⚠️ male_only_tools set but no speaker names configured — gate inactive")

        # Surface enrolled voice prints in HA from boot (not just after builds).
        try:
            from .ha_sensors import PUBLISHER as _PUB
            asyncio.get_running_loop().create_task(_PUB.voice_prints())
        except Exception:
            pass

        # Voice timers: backend-owned registry, device rings via TIMER_RING_ENTITY.
        self.timer_registry = TimerRegistry()

        # Voice enrollment (fork): guided on-device voice capture, always available.
        self.enrollment_recorder = EnrollmentRecorder()
        self.websocket_handler.enrollment_recorder = self.enrollment_recorder
        self.enrollment_conductor = EnrollmentConductor(
            self.enrollment_recorder,
            self.websocket_handler.broadcast_json,
            self.websocket_handler.broadcast_bytes,
            openai_api_key,
            phrase=os.environ.get("ENROLLMENT_PHRASE", "").strip(),
            tts_voice=os.environ.get("ENROLLMENT_TTS_VOICE", "fable").strip() or "fable",
        )
        self.websocket_handler.enrollment_conductor = self.enrollment_conductor

        # Auto-build the voice print when enrollment finishes (fork, 0.16.5):
        # recording alone used to require a manual `python3 -m app.build_voiceprint`
        # step that most users never found — enrollments silently did nothing.
        # Now: build in a worker thread, tell the user out loud, and warn when
        # the enrolled name isn't in the speaker_*_name options (recognition
        # stays inactive until it is).
        async def _auto_build_voiceprint(info):
            person, path = (info.get("person") or "").strip(), info.get("path")
            if not person or not path or (info.get("seconds") or 0) < 20:
                return
            from .build_voiceprint import build
            from .ha_sensors import PUBLISHER
            try:
                result = await asyncio.to_thread(build, person, [path])
            except Exception as e:
                logger.warning(f"⚠️ voice-print auto-build failed: {e!r}")
                return
            if not result["ok"]:
                logger.warning(f"⚠️ voice-print for '{person}': {result['error']}")
                await self.enrollment_conductor._say(
                    "I couldn't build a reliable voice print from that session — "
                    "there wasn't enough clear speech. Say 'train my voice' to try again.")
                return
            logger.info(f"🪪 voice print built for '{person}' ({result['chunks']} chunks) → {result['path']}")
            known = {n.strip().lower() for n in (
                os.environ.get("SPEAKER_MALE_NAME", ""), os.environ.get("SPEAKER_FEMALE_NAME", "")) if n.strip()}
            if person.lower() in known:
                await self.enrollment_conductor._say(
                    f"Your voice print is ready, {person.capitalize()}. I'll recognize you from now on.")
            else:
                logger.warning(
                    f"⚠️ '{person}' is enrolled but not in speaker_male_name/speaker_female_name — "
                    "recognition inactive until the add-on configuration names this person")
                await self.enrollment_conductor._say(
                    f"Your voice print is built, {person.capitalize()} — one more step: add your name "
                    "to the speaker settings in the add-on configuration, then restart it.")
            await PUBLISHER.voice_prints()
        self.enrollment_conductor.on_finished = _auto_build_voiceprint
        # Timers: personalized spoken expiry via the conductor's TTS lane,
        # owner from the live speaker verdict, wake-ack from the serializer.
        async def _guarded_say(text):
            # Suppress inbound mic audio while the announcement plays (+ tail)
            # so the assistant can't hear itself and reply.
            ser = self.websocket_handler._serializer
            import time as _t
            if ser is not None:
                ser.suppress_inbound_until = _t.monotonic() + 3600
            try:
                await self.enrollment_conductor._say(text)
            finally:
                if ser is not None:
                    ser.suppress_inbound_until = _t.monotonic() + 1.2
        self.timer_registry.announcer = _guarded_say
        self.timer_registry.get_owner = (
            lambda: SPEAKER_PROBE.name_for(SPEAKER_PROBE.gate_speaker()) if SPEAKER_PROBE else None
        )
        self.timer_registry.last_wake = (
            lambda: max(
                getattr(self.websocket_handler._serializer, "_last_wake_mono", 0.0),
                getattr(self.websocket_handler._serializer, "_last_button_mono", 0.0),
            ) if self.websocket_handler._serializer else 0.0
        )

        # Announce endpoint (fork): a LAN route back to the device so the
        # household's agent can speak results of long-running work. Reuses the
        # guarded announcer above; off unless both port and token are set.
        announce_port = int(os.environ.get("ANNOUNCE_PORT", "0") or 0)
        announce_token = os.environ.get("ANNOUNCE_TOKEN", "").strip()
        if announce_port and announce_token:
            await start_announce_server(
                announce_port, announce_token, _guarded_say,
                lambda: self.websocket_handler._serializer is not None,
            )
        elif announce_port or announce_token:
            logger.warning("⚠️ announce endpoint needs BOTH announce_port and announce_token — disabled")

        self.websocket_transport = self.websocket_handler.create_transport()
        
        # Store configuration for session creation
        self.openai_api_key = openai_api_key
        self.vad_threshold = vad_threshold
        self.vad_prefix_padding_ms = vad_prefix_padding_ms
        self.vad_silence_duration_ms = vad_silence_duration_ms
        self.turn_detection_type = turn_detection_type
        self.vad_eagerness = vad_eagerness
        self.interrupt_response = interrupt_response
        self.semantic_vad_create_response = semantic_vad_create_response
        self.enable_disconnect_tool = enable_disconnect_tool
        self.transcription_language = transcription_language
        self.transcription_model = transcription_model
        self.max_context_messages = max_context_messages
        self.instructions = instructions
        self.model = openai_model
        self.voice = openai_voice
        self.openai_speed = openai_speed
        self.max_output_tokens = max_output_tokens
        self.noise_reduction = noise_reduction
        self.mcp_tool_allowlist = mcp_tool_allowlist
        self.mcp_client = mcp_client
        self.ha_access_token = ha_access_token
        self.enable_web_search = enable_web_search
        self.web_search_model = web_search_model

        # Initialize audio recording service (optional)
        self.audio_recording_service = AudioRecordingService(
            enable_recording=enable_recording,
            sample_rate=24000,
            chunk_duration_seconds=30,
            output_dir="recordings"
        )
        
        logger.info("✅ Application initialized - ready to accept WebSocket connections")
    
    def _build_pipeline_for_transport(self, transport: WebsocketServerTransport, client_id: str):
        """
        Build pipeline for a WebSocket transport connection.
        
        Args:
            transport: The WebSocket transport instance
            client_id: Unique identifier for the client device
        """
        # Ensure OpenAI service exists
        if self.openai_service is None:
            raise RuntimeError("OpenAI service must be created before building pipeline")
        
        # Use WebSocket handler to build pipeline
        self.pipeline, self.runner, self.current_task = self.websocket_handler.build_pipeline(
            transport=transport,
            openai_service=self.openai_service,
            client_id=client_id,
            activity_callback=self._update_session_activity
        )
    
    def _update_session_activity(self):
        """Update session activity timestamp (called by SessionActivityTracker)."""
        pass
    
    async def _ensure_openai_service(self):
        """Create the single OpenAI service used by the live pipeline."""
        if self._pipeline_lock is None:
            self._pipeline_lock = asyncio.Lock()
        
        async with self._pipeline_lock:
            if self.openai_service is not None:
                return self.openai_service

            logger.info("🆕 Creating authoritative OpenAI Session...")
            
            # Create session properties with audio configuration
            from pipecat.services.openai.realtime.events import (
                SessionProperties,
                AudioConfiguration,
                AudioInput,
                AudioOutput,
                TurnDetection,
                SemanticTurnDetection,
                InputAudioTranscription,
                InputAudioNoiseReduction,
            )
            
            # Collect all tool definitions for session properties. The
            # disconnect_client tool is opt-in (see enable_disconnect_tool): by
            # default we do NOT expose it, so the model can't hang up the device
            # mid-conversation.
            all_tools = []
            if self.enable_disconnect_tool:
                all_tools.append(get_disconnect_tool_definition())

            # Web search tool (optional). Lets the model look things up online via
            # a secondary OpenAI Responses web_search call in the handler.
            if self.enable_web_search:
                all_tools.append(get_web_search_tool_definition())

            # Bounded, read-only reads from the approved Home Assistant calendars.
            all_tools.append(get_calendar_tool_definition())

            # Authoritative ON sequences for approved mixed Zigbee room groups.
            all_tools.append(get_room_light_tool_definition())

            # Graceful model-selected conversation close. Unlike disconnect_client,
            # this only suppresses the next post-reply mic window after audio drains.
            all_tools.append(get_end_conversation_tool_definition())

            # Voice enrollment tool (fork): guided voice-training capture.
            all_tools.append(get_enrollment_tool_definition())
            all_tools.append(get_false_alarm_tool_definition())
            all_tools.extend(get_timer_tool_definitions())
            all_tools.extend(get_memory_tool_definitions())
            # Direct OpenClaw escalation (fork): with OPENCLAW_URL set the tool
            # is native (no HA-MCP 60s cap); the same-named MCP tool is skipped
            # below so the model sees exactly one ask_openclaw.
            if openclaw_url():
                all_tools.append(get_openclaw_tool_definition())
                all_tools.append(get_recall_tool_definition())

            # Get MCP tool definitions if available
            mcp_tools_schema = None
            if self.mcp_client:
                try:
                    logger.info("🔧 Fetching MCP tool definitions...")
                    mcp_tools_schema = await self.mcp_client.get_tools_schema()
                    
                    # Convert MCP tool schemas to OpenAI format, applying the
                    # optional allow-list so the realtime session isn't flooded
                    # with ha-mcp's 80+ tools.
                    exposed = 0
                    for function_schema in mcp_tools_schema.standard_tools:
                        if function_schema.name in (
                            CALENDAR_TOOL_NAME,
                            ROOM_LIGHT_TOOL_NAME,
                            END_CONVERSATION_TOOL_NAME,
                        ):
                            continue
                        if self.mcp_tool_allowlist and function_schema.name not in self.mcp_tool_allowlist:
                            continue
                        if openclaw_url() and function_schema.name == "ask_openclaw":
                            continue
                        openai_tool = {
                            "type": "function",
                            "name": function_schema.name,
                            "description": function_schema.description,
                            "parameters": {
                                "type": "object",
                                "properties": function_schema.properties,
                                "required": function_schema.required
                            }
                        }
                        all_tools.append(openai_tool)
                        exposed += 1

                    if self.mcp_tool_allowlist:
                        logger.info(f"✅ Fetched {len(mcp_tools_schema.standard_tools)} MCP tools, exposing {exposed} per allow-list")
                    else:
                        logger.info(f"✅ Fetched {len(mcp_tools_schema.standard_tools)} MCP tools")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to fetch MCP tool definitions: {e}")
            
            # Turn detection: semantic_vad (recommended — semantic end-of-turn,
            # echo-resistant, doesn't cut the user off) or classic server_vad.
            if self.max_context_messages > 0 and self.turn_detection_type != "semantic_vad":
                logger.warning(
                    "⚠️ bounded complete-turn context requires semantic_vad; "
                    "disabling managed history for legacy server_vad"
                )
                self.max_context_messages = 0
            if self.turn_detection_type == "semantic_vad":
                manual_response_gating = self.max_context_messages > 0
                turn_detection = SemanticTurnDetection(
                    eagerness=self.vad_eagerness,
                    # Strict context bounding requires response creation to wait
                    # until expired complete turns are deleted. The service sends
                    # one response.create after that barrier. With bounding off,
                    # retain the configured server-driven behaviour.
                    create_response=(
                        False if manual_response_gating
                        else self.semantic_vad_create_response
                    ),
                    interrupt_response=self.interrupt_response,
                )
            else:
                manual_response_gating = False
                turn_detection = TurnDetection(
                    type="server_vad",
                    threshold=self.vad_threshold,
                    prefix_padding_ms=self.vad_prefix_padding_ms,
                    silence_duration_ms=self.vad_silence_duration_ms,
                )

            # Bounded reconnect replay needs a compact representation of native
            # user audio. A blank language means automatic language detection;
            # it no longer disables transcription while context bounding is on.
            transcription = (
                InputAudioTranscription(
                    model=self.transcription_model,
                    language=self.transcription_language or None,
                )
                if self.max_context_messages > 0 or self.transcription_language
                else None
            )

            # Optional near/far-field input noise reduction (helps the VAD reject
            # background noise / residual speaker leak). None = off (default).
            noise_reduction = (
                InputAudioNoiseReduction(type=self.noise_reduction)
                if self.noise_reduction
                else None
            )

            session_properties = SessionProperties(
                # Voice-instructed memory: standing household notes are folded
                # into the instructions at every session creation.
                instructions=self.instructions + memory_instructions(),
                # Cap the reply length: bounds runaway monologues + per-response
                # output-token cost. None = unlimited (the API default "inf").
                max_output_tokens=self.max_output_tokens,
                audio=AudioConfiguration(
                    input=AudioInput(
                        turn_detection=turn_detection,
                        transcription=transcription,
                        noise_reduction=noise_reduction,
                    ),
                    # speed is a post-generation playback rate (0.25-1.5, 1.0 = normal).
                    output=AudioOutput(voice=self.voice, speed=self.openai_speed)
                ),
                tools=all_tools
            )

            if self.turn_detection_type == "semantic_vad":
                logger.info(
                    f"🎚️ Turn detection: semantic_vad (eagerness={self.vad_eagerness}, "
                    f"create_response={not manual_response_gating and self.semantic_vad_create_response}, "
                    f"interrupt_response={self.interrupt_response})"
                    + (
                        f", transcription={self.transcription_model} "
                        f"(lang={self.transcription_language or 'auto'})"
                        if transcription else " (transcription off)"
                    )
                )
            else:
                logger.info(
                    f"🎚️ Turn detection: server_vad (threshold={self.vad_threshold}, "
                    f"silence_duration_ms={self.vad_silence_duration_ms})"
                    + (f", transcription={self.transcription_model} (lang={self.transcription_language})" if self.transcription_language else " (transcription off)")
                )

            logger.info(f"🔧 Creating session with {len(all_tools)} tools: {[tool.get('name', 'unknown') for tool in all_tools]}")
            
            # Create new service instance
            self.openai_service = SafeRealtimeLLMService(
                api_key=self.openai_api_key,
                model=self.model,
                session_properties=session_properties,
                start_audio_paused=False,
                max_context_turns=self.max_context_messages,
                manual_response_gating=manual_response_gating,
                server_vad_response_ownership=(
                    self.turn_detection_type == "server_vad"
                ),
                server_vad_interrupt_response=self.interrupt_response,
            )
            logger.info(f"✅ OpenAI Service created: {type(self.openai_service).__name__}")
            
            # Register disconnect tool handler (only when the tool is exposed)
            if self.enable_disconnect_tool:
                disconnect_tool_handler = create_disconnect_tool_handler(self.websocket_transport)
                self.openai_service.register_function("disconnect_client", disconnect_tool_handler)
                logger.info("✅ Registered disconnect tool handler")

            # Register web search tool handler (only when the tool is exposed)
            if self.enable_web_search:
                self.openai_service.register_function(
                    "web_search",
                    create_web_search_tool_handler(self.openai_api_key, self.web_search_model),
                )
                logger.info(f"✅ Registered web_search tool handler (model={self.web_search_model})")
            
            # Register voice enrollment tool handler (fork). The speaker-name
            # getter lets the tool default to the voice-identified person.
            def _current_speaker_name():
                if SPEAKER_PROBE is None:
                    return None
                return SPEAKER_PROBE.name_for(SPEAKER_PROBE.gate_speaker())

            self.openai_service.register_function(
                "voice_enrollment",
                create_enrollment_tool_handler(self.enrollment_conductor, _current_speaker_name),
            )
            logger.info("✅ Registered voice_enrollment tool handler")
            self.openai_service.register_function(
                "mark_false_wake", create_false_alarm_tool_handler()
            )
            register_timer_tools(self.openai_service, self.timer_registry)
            register_memory_tools(self.openai_service, _current_speaker_name)
            if openclaw_url():
                register_openclaw_tool(self.openai_service)
                logger.info("✅ Registered DIRECT ask_openclaw tool (bypassing HA MCP 60s cap)")
            logger.info("✅ Registered timer + memory tools")

            # Register MCP tool handlers if available
            if self.mcp_client and mcp_tools_schema:
                try:
                    await self.mcp_client.register_tools_schema(mcp_tools_schema, self.openai_service)
                    logger.info(f"✅ Registered {len(mcp_tools_schema.standard_tools)} MCP tool handlers")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to register MCP tool handlers: {e}")
            # MUST come AFTER register_tools_schema: pipecat registers a handler
            # for EVERY MCP tool (our allow-list/dedup only trims the definitions
            # sent to the model, not handler registration), so a same-named
            # ask_openclaw script silently rebinds the tool back onto the HA MCP
            # path and its 60s cap. Observed live 2026-07-13: "It failed. I
            # couldn't send the text" at exactly 60s — while the text sent fine.
            if openclaw_url():
                register_openclaw_tool(self.openai_service)
                logger.info("✅ DIRECT ask_openclaw re-registered after MCP handlers (wins)")
            # Register after MCP because Pipecat registers every MCP handler,
            # including schemas not exposed to the model. Native implementations
            # must remain authoritative if names ever collide.
            register_calendar_tool(self.openai_service, self.ha_access_token)
            logger.info("✅ Registered read-only get_calendar_events tool")
            register_room_light_tool(self.openai_service, self.ha_access_token)
            logger.info("✅ Registered authoritative turn_on_room_lights tool")
            register_end_conversation_tool(
                self.openai_service,
                self.websocket_handler.request_graceful_close,
                lambda: TURN_LIVENESS.in_flight == 1,
            )
            logger.info("✅ Registered graceful end_conversation tool")
            
            logger.info("✅ New OpenAI Session created")
            return self.openai_service
    
    async def run(self) -> None:
        """Run the application."""
        await self.initialize()
        
        # Create the authoritative OpenAI service before the pipeline captures it.
        await self._ensure_openai_service()
        
        # Build pipeline - based on pipecat-examples, one pipeline handles all connections
        # The transport manages multiple connections internally
        self._build_pipeline_for_transport(self.websocket_transport, "server")

        # Consume pipecat's FIRST-context auto-response ONCE at startup — SILENTLY.
        # WHY: pipecat 0.0.97's OpenAIRealtimeLLMService._handle_context does
        # `if not self._context: ... await self._create_response()` — i.e. the
        # very first context it ever sees triggers a real response. With
        # the bounded turn gate also creates a response on every user turn, so the
        # user's first transcription must not trigger Pipecat's one-time automatic
        # response as well. We previously consumed that path with a throwaway LLMRunFrame
        # kickoff — but an LLMRunFrame runs `_create_response()`, producing a REAL
        # (audible, tool-calling) reply. The old comment assumed it "goes to no
        # device" because nothing is connected at startup; WRONG: when the user
        # updates the add-on the device auto-reconnects within seconds and lands
        # mid-kickoff (and its post-tool follow-up), so the device plays a
        # spontaneous "answer" nobody asked for (observed: "Ik vond geen
        # betrouwbare lamp in de gang" right after a restart).
        #
        # Fix: pre-set `self._context` to an empty LLMContext instead. Now the
        # first REAL user turn hits the ELSE branch of _handle_context (no
        # _create_response), and the bounded gate creates exactly one response.
        # The empty sentinel is harmlessly overwritten by the real context on the
        # first turn (both branches do `self._context = context`).
        if self.openai_service is not None and (
            getattr(self, "max_context_messages", 0) > 0
            or getattr(self, "turn_detection_type", "semantic_vad") == "server_vad"
            or getattr(self, "semantic_vad_create_response", True)
        ):
            try:
                from pipecat.processors.aggregators.llm_context import LLMContext
                if self.openai_service is not None and getattr(self.openai_service, "_context", None) is None:
                    self.openai_service._context = LLMContext()
                    # Also mark pipecat's one-time "conversation setup" as already
                    # done. pipecat runs it on the FIRST _create_response: it
                    # re-sends the context's messages as ConversationItemCreate
                    # events, then flips _llm_needs_conversation_setup False. On a
                    # fresh realtime session OpenAI already builds the conversation
                    # from the live audio + tool-call flow, so that one-time setup
                    # re-injects items OpenAI already has — which made the first
                    # post-tool reply come out as a meaningless filler ("Ik ben
                    # klaar om verder te gaan met het gesprek."). Instructions are
                    # sent independently via _update_settings() on session.created,
                    # so clearing this flag is safe and makes the first real turn a
                    # normal reply.
                    if hasattr(self.openai_service, "_llm_needs_conversation_setup"):
                        self.openai_service._llm_needs_conversation_setup = False
                    logger.info("🌱 Pre-seeded empty context + marked conversation setup done (no startup speech, no first-turn filler)")
                else:
                    logger.info("🌱 Startup context already set; skipping pre-seed")
            except Exception as e:
                logger.warning(f"⚠️ Could not pre-seed startup context (turn-1 double may occur): {e}")

        # Setup WebSocket event handlers
        async def on_client_connected(client_id: str):
            """Handle new client connection."""
            if self.openai_service is None:
                raise RuntimeError("OpenAI service is unavailable for client connection")
            if self.session_manager:
                self.session_manager.set_current_service(client_id, self.openai_service)
            if self.audio_recording_service:
                self.audio_recording_service.start_new_session(client_id)
        
        def on_client_disconnected(client_id: str):
            """Handle client disconnection."""
            if self.session_manager:
                self.session_manager.handle_client_disconnect(client_id, self.openai_service)
            if self.audio_recording_service:
                self.audio_recording_service.stop_recording()
        
        # Function to get OpenAI service for a client
        def get_openai_service_for_client(client_id: str) -> Optional[OpenAIRealtimeLLMService]:
            """Get OpenAI service for a specific client."""
            if self.session_manager:
                service = self.session_manager.get_current_service(client_id)
                if service is not None:
                    return service
            return self.openai_service
        
        self.websocket_handler.setup_event_handlers(
            transport=self.websocket_transport,
            on_client_connected_callback=on_client_connected,
            on_client_disconnected_callback=on_client_disconnected,
            openai_service_getter=get_openai_service_for_client
        )
        
        try:
            # Start the pipeline runner - this will start the WebSocket server
            # Based on pipecat-examples: PipelineRunner.run() starts the transport server
            logger.info("✅ Starting WebSocket server and pipeline...")
            await self.runner.run(self.current_task)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt")
        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
            raise
        finally:
            await self.cleanup()
    
    async def cleanup(self) -> None:
        """Cleanup resources."""
        logger.info("Cleaning up application...")
        
        if self.runner:
            try:
                await self.runner.cancel()
            except Exception as e:
                logger.warning(f"⚠️ Error cancelling runner: {e}")
        
        if self.websocket_handler:
            try:
                await self.websocket_handler.cleanup()
            except Exception as e:
                logger.warning(f"⚠️ Error cleaning up WebSocket handler: {e}")
        
        if self.audio_recording_service:
            self.audio_recording_service.cleanup()
        
        logger.info("✅ Application cleanup complete")


async def main() -> None:
    """Main entry point."""
    app = Application()
    
    try:
        await app.run()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
