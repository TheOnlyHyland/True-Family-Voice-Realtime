"""Offline lifecycle tests for the single-device voice pipeline."""

import asyncio
import json
import sys
import types
import unittest
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, Mock, patch


ADDON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ADDON_ROOT))


def _stub_module(name, **attributes):
    """Install a small import stub without requiring runtime dependencies."""
    parts = name.split(".")
    for index in range(1, len(parts) + 1):
        module_name = ".".join(parts[:index])
        module = sys.modules.get(module_name)
        if module is None:
            module = types.ModuleType(module_name)
            sys.modules[module_name] = module
        if index > 1:
            parent = sys.modules[".".join(parts[: index - 1])]
            setattr(parent, parts[index - 1], module)

    module = sys.modules[name]
    for key, value in attributes.items():
        setattr(module, key, value)
    return module


class _FrameProcessor:
    def __init__(self, *args, **kwargs):
        pass

    async def process_frame(self, frame, direction):
        pass

    async def push_frame(self, frame, direction=None):
        pass

    async def cleanup(self):
        pass


class _Frame:
    pass


class _FrameDirection:
    DOWNSTREAM = object()
    UPSTREAM = object()


class _OpenAIRealtimeLLMService:
    def __init__(self, *args, **kwargs):
        self._api_session_ready = False
        self._run_llm_when_api_session_ready = False
        self._llm_needs_conversation_setup = True
        self._websocket = None
        self._receive_task = None
        self._connect_hook: Any = None
        self._context = None
        self._pending_function_calls = {}
        self._completed_tool_calls = set()
        self._messages_added_manually = {}
        self._functions = {}
        self._sent_client_events = []

    async def _create_response(self):
        pass

    async def _handle_context(self, context):
        self._context = context

    def register_function(self, function_name, handler, start_callback=None, **kwargs):
        self._registered_function = (function_name, handler)
        self._functions[function_name] = handler

    async def broadcast_frame(self, _frame_type, **_kwargs):
        pass

    async def push_frame(self, _frame, _direction=None):
        pass

    async def _handle_evt_session_updated(self, _evt):
        self._api_session_ready = True
        if self._run_llm_when_api_session_ready:
            self._run_llm_when_api_session_ready = False
            await self._create_response()

    async def _handle_evt_response_done(self, _evt):
        pass

    async def _handle_evt_conversation_item_added(self, _evt):
        pass

    async def _handle_evt_conversation_item_done(self, _evt):
        pass

    async def handle_evt_input_audio_transcription_completed(self, _evt):
        pass

    async def _handle_evt_speech_stopped(self, _evt):
        pass

    async def _handle_evt_function_call_arguments_done(self, _evt):
        pass

    async def _call_event_handler(self, *_args):
        pass

    async def _maybe_handle_evt_retrieve_conversation_item_error(self, _evt):
        return False

    async def cleanup(self):
        pass

    async def _disconnect(self):
        self._api_session_ready = False
        self._websocket = None
        self._receive_task = None

    async def _connect(self):
        if self._connect_hook is not None:
            await self._connect_hook()

    async def _process_completed_function_calls(self, send_new_results):
        if self._context is None:
            raise RuntimeError("missing context")

    async def send_client_event(self, _event):
        self._sent_client_events.append(_event)

    def _get_enabled_modalities(self):
        return ["audio"]


class _Placeholder:
    def __init__(self, *args, **kwargs):
        pass


class _TurnLiveness:
    in_flight = 0
    last_non_close_tool_start = 0.0
    non_close_tool_generation = 0

    def tool_started(self):
        pass

    def tool_finished(self):
        pass

    def non_close_tool_started(self):
        self.non_close_tool_generation += 1


_stub_module("dotenv", load_dotenv=lambda: None)
_stub_module("pipecat.pipeline.pipeline", Pipeline=_Placeholder)
_stub_module("pipecat.pipeline.runner", PipelineRunner=_Placeholder)
_stub_module("pipecat.pipeline.task", PipelineTask=_Placeholder)
_stub_module(
    "pipecat.transports.websocket.server",
    WebsocketServerTransport=_Placeholder,
    WebsocketServerParams=_Placeholder,
)
_stub_module(
    "pipecat.services.openai.realtime.llm",
    OpenAIRealtimeLLMService=_OpenAIRealtimeLLMService,
)
_stub_module(
    "pipecat.processors.frame_processor",
    FrameProcessor=_FrameProcessor,
    FrameDirection=_FrameDirection,
)
_stub_module(
    "pipecat.frames.frames",
    Frame=_Frame,
    InputAudioRawFrame=type("InputAudioRawFrame", (_Frame,), {}),
    OutputAudioRawFrame=type("OutputAudioRawFrame", (_Frame,), {}),
    StartFrame=type("StartFrame", (_Frame,), {}),
    EndFrame=type("EndFrame", (_Frame,), {}),
    ErrorFrame=type("ErrorFrame", (_Frame,), {}),
    FunctionCallResultFrame=type("FunctionCallResultFrame", (_Frame,), {}),
    LLMFullResponseStartFrame=type("LLMFullResponseStartFrame", (_Frame,), {}),
    LLMFullResponseEndFrame=type("LLMFullResponseEndFrame", (_Frame,), {}),
)
_stub_module("pipecat.audio.utils", create_stream_resampler=lambda: _Placeholder())
_stub_module("pipecat.services.openai.realtime.events")


class _RealtimeEvent:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.type = getattr(self, "type", "")


class _ConversationItem(_RealtimeEvent):
    _counter = 0

    def __init__(self, **kwargs):
        type(self)._counter += 1
        kwargs.setdefault("id", f"replay-{type(self)._counter}")
        super().__init__(**kwargs)

    def model_dump(self, exclude_none=False):
        return {
            key: value
            for key, value in self.__dict__.items()
            if not exclude_none or value is not None
        }


class _ConversationItemCreateEvent(_RealtimeEvent):
    def __init__(self, item):
        super().__init__(type="conversation.item.create", item=item)


class _ConversationItemDeleteEvent(_RealtimeEvent):
    def __init__(self, item_id):
        super().__init__(type="conversation.item.delete", item_id=item_id)


class _ResponseProperties(_RealtimeEvent):
    pass


class _ResponseCreateEvent(_RealtimeEvent):
    def __init__(self, response=None):
        super().__init__(type="response.create", response=response)


_event_module = sys.modules["pipecat.services.openai.realtime.events"]
setattr(_event_module, "ConversationItem", _ConversationItem)
setattr(_event_module, "ConversationItemCreateEvent", _ConversationItemCreateEvent)
setattr(_event_module, "ConversationItemDeleteEvent", _ConversationItemDeleteEvent)
setattr(_event_module, "ResponseProperties", _ResponseProperties)
setattr(_event_module, "ResponseCreateEvent", _ResponseCreateEvent)
setattr(_event_module, "parse_server_event", lambda message: message)


class _LLMContext:
    def __init__(self, messages=None):
        self._messages = messages or []

    def get_messages(self):
        return self._messages

    def set_messages(self, messages):
        self._messages[:] = messages


_stub_module(
    "pipecat.processors.aggregators.llm_context",
    LLMContext=_LLMContext,
)

import app  # noqa: E402

_stub_module("app.raw_audio_serializer", RawAudioSerializer=_Placeholder)
_stub_module("app.session_manager", SessionManager=_Placeholder)
_stub_module("app.audio_recording_service", AudioRecordingService=_Placeholder)
_stub_module(
    "app.phase_emitter",
    PhaseEmitter=_Placeholder,
    TURN_LIVENESS=_TurnLiveness(),
)
_stub_module("app.transcript_logger", TranscriptLogger=_Placeholder)
_stub_module("app.mcp_service", HomeAssistantMCPService=_Placeholder)
_stub_module(
    "app.disconnect_tool",
    get_disconnect_tool_definition=lambda: {},
    create_disconnect_tool_handler=lambda transport: None,
)
_stub_module(
    "app.web_search_tool",
    get_web_search_tool_definition=lambda: {},
    create_web_search_tool_handler=lambda *args: None,
)
_stub_module("app.speaker_context", SpeakerProbe=_Placeholder)
_stub_module(
    "app.timers",
    TimerRegistry=_Placeholder,
    get_timer_tool_definitions=lambda: [],
    register_timer_tools=lambda *args: None,
)
_stub_module("app.announce_http", start_announce_server=AsyncMock())
_stub_module(
    "app.openclaw_tool",
    get_openclaw_tool_definition=lambda: {},
    get_recall_tool_definition=lambda: {},
    openclaw_url=lambda: "",
    register_openclaw_tool=lambda service: None,
)
_stub_module(
    "app.voice_memory",
    memory_instructions=lambda: "",
    get_memory_tool_definitions=lambda: [],
    register_memory_tools=lambda *args: None,
)
_stub_module(
    "app.enrollment",
    EnrollmentRecorder=_Placeholder,
    EnrollmentConductor=_Placeholder,
    get_enrollment_tool_definition=lambda: {},
    create_enrollment_tool_handler=lambda *args: None,
    get_false_alarm_tool_definition=lambda: {},
    create_false_alarm_tool_handler=lambda: None,
)

from app import main  # noqa: E402
from app import websocket_handler  # noqa: E402


class _FakePhaseEmitter:
    def __init__(self, *args, **kwargs):
        pass

    def set_kill_window_handlers(self, **kwargs):
        pass


class _FakeOpenAIService:
    def event_handler(self, event_name):
        return lambda callback: callback


class _FakeTransport:
    def input(self):
        return object()

    def output(self):
        return object()


class _FakeSessionManager:
    def __init__(self):
        self.current_services = {}

    def set_current_service(self, client_id, service):
        self.current_services[client_id] = service

    def get_current_service(self, client_id):
        return self.current_services.get(client_id)


class _FakeRecordingService:
    def __init__(self):
        self.started = []

    def start_new_session(self, client_id):
        self.started.append(client_id)


class _FakeWebSocketHandler:
    def setup_event_handlers(self, **callbacks):
        self.callbacks = callbacks


class PipelineLifecycleTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _message(item_id, role, text=""):
        content_type = "input_audio" if role == "user" else "output_audio"
        return {
            "id": item_id,
            "type": "message",
            "role": role,
            "content": [{"type": content_type, "transcript": text}],
        }

    def _complete_turn(self, service, number):
        user_id = f"user-{number}"
        service._conversation_window.begin_user_turn(
            self._message(user_id, "user")
        )
        service._conversation_window.attach_transcript(user_id, f"request {number}")
        service._conversation_window.activate(user_id)
        service._conversation_window.finish_response(
            "completed",
            [self._message(f"assistant-{number}", "assistant", f"reply {number}")],
        )

    async def test_bounded_turn_deletes_old_items_before_response_create(self):
        service = main.SafeRealtimeLLMService(
            max_context_turns=1,
            manual_response_gating=True,
        )
        service._context = _LLMContext()
        self._complete_turn(service, 1)
        service._conversation_window.begin_user_turn(
            self._message("user-2", "user")
        )
        service._conversation_window.attach_transcript("user-2", "request 2")
        service._transcript_ready_events["user-2"] = asyncio.Event()
        service._transcript_ready_events["user-2"].set()
        service._api_session_ready = True
        service._websocket = object()
        sent = []

        async def send(event):
            sent.append(event)

        service.send_client_event = send
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )

        await service._start_user_turn("user-2")

        self.assertEqual(
            [event.type for event in sent],
            [
                "conversation.item.delete",
                "conversation.item.delete",
                "response.create",
            ],
        )
        self.assertEqual(
            [event.item_id for event in sent[:2]],
            ["assistant-1", "user-1"],
        )
        self.assertEqual(
            [turn.user_item_id for turn in service._conversation_window.turns],
            ["user-2"],
        )

    async def test_pruning_tool_turn_removes_local_result_before_rearming_call_id(self):
        service = main.SafeRealtimeLLMService(max_context_turns=1)
        window = service._conversation_window
        window.begin_user_turn(self._message("user-1", "user"))
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
        window.observe_item(
            {
                "id": "result-item",
                "type": "function_call_output",
                "call_id": "call-1",
                "output": "done",
            }
        )
        window.finish_response(
            "completed", [self._message("assistant-1", "assistant", "Done.")]
        )
        window.begin_user_turn(self._message("user-2", "user"))
        service._context = _LLMContext(
            messages=[
                {"role": "user", "content": "do it"},
                {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "type": "function",
                            "function": {"name": "do_it", "arguments": "{}"},
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call-1", "content": "done"},
            ]
        )
        service._completed_tool_calls = {"call-1"}
        service.send_client_event = AsyncMock()
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )

        await service._prune_complete_turns()

        self.assertNotIn(
            "call-1",
            {
                message.get("tool_call_id")
                for message in service._context.get_messages()
            },
        )
        self.assertNotIn("call-1", service._completed_tool_calls)

    async def test_local_sync_keeps_one_already_aggregated_active_user(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._context = _LLMContext()
        service._conversation_window.begin_user_turn(
            self._message("user-1", "user")
        )
        service._conversation_window.attach_transcript("user-1", "hello")
        service._conversation_window.activate("user-1")
        service._context.set_messages([{"role": "user", "content": "hello"}])

        service._sync_local_context(include_active_user=True)

        self.assertEqual(
            service._context.get_messages(),
            [{"role": "user", "content": "hello"}],
        )

    async def test_superseded_system_hint_is_deleted_from_server(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.send_client_event = AsyncMock()
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )

        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(
                item={
                    "id": "system-1",
                    "type": "message",
                    "role": "system",
                    "content": [],
                }
            )
        )
        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(
                item={
                    "id": "system-2",
                    "type": "message",
                    "role": "system",
                    "content": [],
                }
            )
        )
        await asyncio.sleep(0)

        service.send_client_event.assert_awaited_once()
        delete_event = cast(Any, service.send_client_event).await_args_list[0].args[0]
        self.assertEqual(delete_event.item_id, "system-1")
        self.assertEqual(
            [item["id"] for item in service._conversation_window.pending_context],
            ["system-2"],
        )

    async def test_replay_creates_items_without_response_or_tool_execution(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._context = _LLMContext()
        self._complete_turn(service, 1)
        turns = service._conversation_window.replay_snapshot()
        sent = []

        async def send(event):
            sent.append(event)
            if event.type == "conversation.item.create":
                await service._handle_evt_conversation_item_added(
                    types.SimpleNamespace(item=event.item)
                )

        async def retrieve(item_id):
            return next(event.item for event in sent if getattr(event, "item", None) and event.item.id == item_id)

        service.send_client_event = send
        service.retrieve_conversation_item = retrieve

        await service._replay_history(turns, [])

        self.assertEqual(
            [event.type for event in sent],
            ["conversation.item.create", "conversation.item.create"],
        )
        self.assertNotIn("response.create", [event.type for event in sent])
        self.assertEqual(service._pending_function_calls, {})
        self.assertNotEqual(
            service._conversation_window.turns[0].user_item_id,
            "user-1",
        )
        retained_count = len(service._conversation_window.turns[0].items)
        await service._handle_evt_conversation_item_done(
            types.SimpleNamespace(item=sent[-1].item)
        )
        self.assertEqual(
            len(service._conversation_window.turns[0].items),
            retained_count,
        )

    async def test_post_tool_response_waits_for_function_response_done(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._api_session_ready = True
        service._response_finished.clear()
        service.send_client_event = AsyncMock()

        create_task = asyncio.create_task(service._create_response())
        await asyncio.sleep(0)
        service.send_client_event.assert_not_awaited()
        first_continuation = service._continuation_task
        await service._create_response()
        self.assertIs(service._continuation_task, first_continuation)

        service._response_finished.set()
        await create_task
        continuation_task = service._continuation_task
        self.assertIsNotNone(continuation_task)
        await cast(Any, continuation_task)

        service.send_client_event.assert_awaited_once()
        response_event = cast(Any, service.send_client_event).await_args_list[0].args[0]
        self.assertEqual(
            response_event.type,
            "response.create",
        )

    async def test_tool_continuation_waits_for_overlap_deletion_barrier(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._api_session_ready = True
        service._overlap_deletions_drained.clear()
        service.send_client_event = AsyncMock()

        await service._create_response()
        continuation_task = service._continuation_task
        self.assertIsNotNone(continuation_task)
        await asyncio.sleep(0)
        service.send_client_event.assert_not_awaited()

        service._overlap_deletions_drained.set()
        await cast(Any, continuation_task)

        service.send_client_event.assert_awaited_once()

    async def test_tool_result_reserves_continuation_before_parent_await(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._api_session_ready = True
        service.send_client_event = AsyncMock()
        service._conversation_window.begin_user_turn(
            self._message("user-1", "user")
        )
        service._conversation_window.activate("user-1")
        service._pending_tool_result_ids.add("call-1")
        service._pending_tool_results_drained.clear()
        context = types.SimpleNamespace(
            get_messages=lambda: [
                {
                    "role": "tool",
                    "tool_call_id": "call-1",
                    "content": "done",
                }
            ]
        )

        async def parent_handle_context(_self, _context):
            await service._handle_evt_response_done(
                types.SimpleNamespace(
                    response=types.SimpleNamespace(
                        status="completed",
                        output=[
                            types.SimpleNamespace(
                                model_dump=lambda exclude_none: {
                                    "type": "function_call",
                                    "id": "fc-1",
                                    "call_id": "call-1",
                                    "name": "test_tool",
                                    "arguments": "{}",
                                }
                            )
                        ],
                    )
                )
            )
            self.assertEqual(
                service._conversation_window.active_turn_id,
                "user-1",
            )
            await service._create_response()

        with patch.object(
            _OpenAIRealtimeLLMService,
            "_handle_context",
            new=parent_handle_context,
        ):
            await service._handle_context(context)

        continuation_task = service._continuation_task
        self.assertIsNotNone(continuation_task)
        await cast(Any, continuation_task)
        service.send_client_event.assert_awaited_once()

    async def test_tool_only_response_balances_managed_assistant_aggregator(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_frame = AsyncMock()
        tool_item = types.SimpleNamespace(
            model_dump=lambda exclude_none: {
                "id": "call-item",
                "type": "function_call",
                "call_id": "call-1",
                "name": "test_tool",
                "arguments": "{}",
            }
        )

        await service._handle_evt_response_done(
            types.SimpleNamespace(
                response=types.SimpleNamespace(
                    status="completed",
                    output=[tool_item],
                )
            )
        )

        service.push_frame.assert_awaited_once()

    async def test_unreplayable_terminal_response_fails_fresh_immediately(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_frame = AsyncMock()
        service.push_error = AsyncMock()
        service._conversation_window.begin_user_turn(
            self._message("user-cancelled", "user")
        )
        service._conversation_window.attach_transcript(
            "user-cancelled",
            "do something",
        )
        service._conversation_window.activate("user-cancelled")

        await service._handle_evt_response_done(
            types.SimpleNamespace(
                response=types.SimpleNamespace(
                    status="cancelled",
                    output=[],
                )
            )
        )

        self.assertTrue(service._recovery_active)
        service.push_error.assert_awaited_once()

    async def test_tool_continuation_waits_for_scheduled_parallel_call(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._api_session_ready = True
        service.send_client_event = AsyncMock()
        service._scheduled_tool_call_ids.add("call-2")

        await service._create_response()
        continuation_task = service._continuation_task
        self.assertIsNotNone(continuation_task)
        await asyncio.sleep(0.01)
        service.send_client_event.assert_not_awaited()

        service._scheduled_tool_call_ids.remove("call-2")
        await cast(Any, continuation_task)
        service.send_client_event.assert_awaited_once()

    async def test_recovery_cancels_blocked_response_send(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._api_session_ready = True
        send_started = asyncio.Event()
        send_completed = asyncio.Event()

        async def blocked_send(event):
            _ = event
            send_started.set()
            await asyncio.Event().wait()
            send_completed.set()

        service.send_client_event = blocked_send
        await service._create_response()
        continuation_task = service._continuation_task
        self.assertIsNotNone(continuation_task)
        await send_started.wait()

        service.begin_recovery()
        await asyncio.gather(cast(Any, continuation_task), return_exceptions=True)

        self.assertFalse(send_completed.is_set())
        self.assertIsNone(service._continuation_task)

    async def test_late_continuation_request_runs_after_current_response_done(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._api_session_ready = True
        first_send_started = asyncio.Event()
        release_first_send = asyncio.Event()
        send_count = 0

        async def controlled_send(event):
            nonlocal send_count
            _ = event
            send_count += 1
            if send_count == 1:
                first_send_started.set()
                await release_first_send.wait()

        service.send_client_event = controlled_send
        await service._create_response()
        first_task = service._continuation_task
        await first_send_started.wait()

        await service._create_response()
        release_first_send.set()
        await cast(Any, first_task)
        second_task = service._continuation_task
        self.assertIsNotNone(second_task)
        self.assertIsNot(second_task, first_task)

        service._response_finished.set()
        await cast(Any, second_task)
        self.assertEqual(send_count, 2)

    async def test_overlapping_user_item_is_deleted_not_queued_behind_active_turn(self):
        service = main.SafeRealtimeLLMService(
            max_context_turns=12,
            manual_response_gating=True,
        )
        service._conversation_window.begin_user_turn(
            self._message("user-1", "user")
        )
        service._conversation_window.activate("user-1")
        service._discard_overlapping_user_item = AsyncMock()

        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(item=self._message("user-2", "user"))
        )
        await asyncio.sleep(0)

        await service._handle_evt_conversation_item_done(
            types.SimpleNamespace(item=self._message("user-2", "user"))
        )

        self.assertEqual(
            [item["id"] for item in service._conversation_window.turns[0].items],
            ["user-1"],
        )
        self.assertEqual(len(service._conversation_window.turns), 1)
        service._discard_overlapping_user_item.assert_awaited_once_with("user-2")

    async def test_first_user_is_active_before_second_item_can_be_admitted(self):
        service = main.SafeRealtimeLLMService(
            max_context_turns=12,
            manual_response_gating=True,
        )
        service._start_user_turn = AsyncMock()
        service._discard_overlapping_user_item = AsyncMock()

        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(item=self._message("user-1", "user"))
        )
        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(item=self._message("user-2", "user"))
        )
        await asyncio.sleep(0)

        self.assertEqual(
            [turn.user_item_id for turn in service._conversation_window.turns],
            ["user-1"],
        )
        service._start_user_turn.assert_awaited_once_with("user-1")
        service._discard_overlapping_user_item.assert_awaited_once_with("user-2")

    async def test_overlap_reservation_discards_item_after_old_turn_finishes(self):
        service = main.SafeRealtimeLLMService(
            max_context_turns=12,
            manual_response_gating=True,
        )
        service._conversation_window.begin_user_turn(
            self._message("user-1", "user")
        )
        service._conversation_window.activate("user-1")
        service._discard_overlapping_user_item = AsyncMock()

        await service._handle_evt_speech_stopped(
            types.SimpleNamespace(item_id="user-2")
        )
        service._conversation_window.active_turn_id = None
        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(item=self._message("user-2", "user"))
        )
        await asyncio.sleep(0)

        self.assertEqual(
            [turn.user_item_id for turn in service._conversation_window.turns],
            ["user-1"],
        )
        service._discard_overlapping_user_item.assert_awaited_once_with("user-2")

    async def test_missing_overlap_item_expires_fail_fresh(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.CONVERSATION_ITEM_TIMEOUT_S = 0.01
        service.push_error = AsyncMock()
        service._conversation_window.begin_user_turn(
            self._message("user-1", "user")
        )
        service._conversation_window.activate("user-1")

        await service._handle_evt_speech_stopped(
            types.SimpleNamespace(item_id="missing-user")
        )
        await asyncio.sleep(0.02)

        self.assertTrue(service._recovery_active)
        self.assertNotIn("missing-user", service._overlap_reservation_ids)
        service.push_error.assert_awaited_once()

    async def test_unknown_delayed_transcript_is_not_added_to_local_context(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        with patch.object(
            _OpenAIRealtimeLLMService,
            "handle_evt_input_audio_transcription_completed",
            new=AsyncMock(),
        ) as parent_transcript:
            await service.handle_evt_input_audio_transcription_completed(
                types.SimpleNamespace(item_id="old-user", transcript="private words")
            )

        parent_transcript.assert_not_awaited()

    async def test_blank_managed_transcript_fails_fresh(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_error = AsyncMock()
        service._conversation_window.begin_user_turn(
            self._message("user-blank", "user")
        )
        service._conversation_window.activate("user-blank")

        await service.handle_evt_input_audio_transcription_completed(
            types.SimpleNamespace(item_id="user-blank", transcript="   ")
        )

        self.assertTrue(service._recovery_active)
        service.push_error.assert_awaited_once()

    async def test_managed_transcript_writes_one_canonical_local_user(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._context = _LLMContext()
        service._conversation_window.begin_user_turn(
            self._message("user-1", "user")
        )
        service._conversation_window.activate("user-1")
        service._transcript_ready_events["user-1"] = asyncio.Event()
        with patch.object(
            _OpenAIRealtimeLLMService,
            "handle_evt_input_audio_transcription_completed",
            new=AsyncMock(),
        ) as parent_transcript:
            await service.handle_evt_input_audio_transcription_completed(
                types.SimpleNamespace(item_id="user-1", transcript="hello")
            )

        parent_transcript.assert_not_awaited()
        self.assertTrue(service._transcript_ready_events["user-1"].is_set())
        self.assertEqual(service._context.get_messages(), [])
        service._sync_local_context(include_active_user=True)
        self.assertEqual(
            service._context.get_messages(),
            [{"role": "user", "content": "hello"}],
        )

    async def test_late_duplicate_transcript_never_rewrites_active_tool_context(self):
        messages = [
            {"role": "user", "content": "turn it on"},
            {
                "role": "assistant",
                "tool_calls": [{"id": "call-1", "function": {"name": "light"}}],
            },
            {
                "role": "tool",
                "tool_call_id": "call-1",
                "content": "IN_PROGRESS",
            },
        ]
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._context = _LLMContext(messages=list(messages))
        service._conversation_window.begin_user_turn(
            self._message("user-1", "user")
        )
        service._conversation_window.activate("user-1")

        await service.handle_evt_input_audio_transcription_completed(
            types.SimpleNamespace(item_id="user-1", transcript="turn it on")
        )

        self.assertEqual(service._context.get_messages(), messages)

    async def test_replay_ack_timeout_discards_history_and_disconnects(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.CONVERSATION_ITEM_TIMEOUT_S = 0.01
        service._context = _LLMContext()
        self._complete_turn(service, 1)
        service.send_client_event = AsyncMock()
        service._disconnect = AsyncMock()

        with self.assertRaisesRegex(RuntimeError, "failed closed"):
            await service._replay_history(
                service._conversation_window.replay_snapshot(),
                [],
            )

        self.assertEqual(service._conversation_window.turns, [])
        self.assertEqual(service._context.get_messages(), [])
        service._disconnect.assert_awaited_once()

    async def test_managed_session_disables_server_item_truncation(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._ws_send = AsyncMock()

        class SessionUpdate:
            type = "session.update"

            @staticmethod
            def model_dump(exclude_none=False):
                return {"type": "session.update", "session": {"instructions": "test"}}

        await service.send_client_event(SessionUpdate())

        payload = service._ws_send.await_args.args[0]
        self.assertEqual(payload["session"]["truncation"], "disabled")

    async def test_realtime_reset_waits_for_session_updated_without_response(self):
        service = main.SafeRealtimeLLMService()
        service._create_response = AsyncMock()
        deliver_ready = asyncio.Event()
        keep_receive_loop_alive = asyncio.Event()

        async def receive_loop():
            await deliver_ready.wait()
            await service._handle_evt_session_updated(object())
            await keep_receive_loop_alive.wait()

        async def connect():
            service._websocket = object()
            service._receive_task = asyncio.create_task(receive_loop())

        service._connect_hook = connect

        reset_task = asyncio.create_task(service.reset_conversation())
        await asyncio.sleep(0)
        self.assertFalse(reset_task.done())

        service._run_llm_when_api_session_ready = True
        deliver_ready.set()
        await reset_task

        self.assertTrue(service._api_session_ready)
        self.assertTrue(service._session_ready_event.is_set())
        self.assertFalse(service._run_llm_when_api_session_ready)
        self.assertFalse(service._llm_needs_conversation_setup)
        service._create_response.assert_not_awaited()
        service._receive_task.cancel()
        await asyncio.gather(service._receive_task, return_exceptions=True)

    async def test_realtime_reset_rejects_false_socket_success(self):
        service = main.SafeRealtimeLLMService()

        with self.assertRaisesRegex(RuntimeError, "did not create a receive loop"):
            await service.reset_conversation()

    async def test_realtime_reset_times_out_without_session_updated(self):
        service = main.SafeRealtimeLLMService()
        service.SESSION_READY_TIMEOUT_S = 0.01

        async def connect():
            service._websocket = object()
            service._receive_task = asyncio.create_task(asyncio.sleep(10))

        service._connect_hook = connect

        with self.assertRaisesRegex(RuntimeError, "timed out before session.updated"):
            await service.reset_conversation()
        service._receive_task.cancel()
        await asyncio.gather(service._receive_task, return_exceptions=True)

    async def test_realtime_ignores_old_receive_loop_readiness(self):
        service = main.SafeRealtimeLLMService()
        service._websocket = object()
        service._receive_task = asyncio.current_task()
        service._accept_session_ready = False
        service._recovery_active = True

        await service._handle_evt_session_updated(object())

        self.assertFalse(service._session_ready_event.is_set())
        self.assertIsNone(service._ready_session_generation)

    async def test_realtime_suppresses_tool_response_during_recovery(self):
        service = main.SafeRealtimeLLMService()
        service._recovery_active = True

        with patch.object(
            _OpenAIRealtimeLLMService,
            "_create_response",
            new=AsyncMock(),
        ) as parent_create_response:
            await service._create_response()

        parent_create_response.assert_not_awaited()

    async def test_realtime_drains_pre_recovery_tool_result_before_reenabling(self):
        service = main.SafeRealtimeLLMService()

        async def tool_handler(params):
            await params.result_callback({"status": "done"})

        service.register_function("test_tool", tool_handler)
        _, wrapped_handler = service._registered_function
        original_result_callback = AsyncMock()
        params = types.SimpleNamespace(
            tool_call_id="call-1",
            arguments={},
            result_callback=original_result_callback,
        )
        service._session_generation = 1
        service._tool_call_generations["call-1"] = 1

        await wrapped_handler(params)

        self.assertEqual(service._pending_tool_result_ids, {"call-1"})
        self.assertFalse(service._pending_tool_results_drained.is_set())

        service._recovery_active = True
        context = _LLMContext(
            messages=[
                {
                    "role": "tool",
                    "tool_call_id": "call-1",
                    "content": {"status": "done"},
                }
            ]
        )
        await service._handle_context(context)
        await service.wait_for_pending_tool_results()

        self.assertEqual(service._pending_tool_result_ids, set())
        self.assertTrue(service._pending_tool_results_drained.is_set())
        original_result_callback.assert_awaited_once_with(
            {"status": "done"},
            properties=None,
        )

    async def test_tool_handler_exception_returns_error_result(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)

        async def failing_handler(_params):
            raise RuntimeError("test failure")

        service.register_function("failing_tool", failing_handler)
        _, wrapped_handler = service._registered_function
        result_callback = AsyncMock()
        params = types.SimpleNamespace(
            tool_call_id="call-fail",
            arguments={},
            result_callback=result_callback,
        )
        service._tool_call_generations["call-fail"] = 0

        await wrapped_handler(params)

        result_callback.assert_awaited_once()
        callback_result = cast(Any, result_callback).await_args_list[0].args[0]
        self.assertIn("error", callback_result)
        self.assertEqual(service._running_tool_call_ids, set())

    async def test_stop_during_pre_handler_await_prevents_side_effect(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_error = AsyncMock()
        handler = AsyncMock()
        service.register_function("guarded_tool", handler)
        _, wrapped_handler = service._registered_function
        result_callback = AsyncMock()
        params = types.SimpleNamespace(
            tool_call_id="call-guarded",
            arguments={},
            result_callback=result_callback,
        )
        service._tool_call_generations["call-guarded"] = 0
        callback_started = asyncio.Event()
        release_callback = asyncio.Event()

        async def before_tool():
            callback_started.set()
            await release_callback.wait()

        original_callback = main.NON_CLOSE_TOOL_CALLBACK
        main.NON_CLOSE_TOOL_CALLBACK = before_tool
        try:
            tool_task = asyncio.create_task(wrapped_handler(params))
            await callback_started.wait()
            await service.suppress_tools_at_interrupt()
            release_callback.set()
            await tool_task
        finally:
            main.NON_CLOSE_TOOL_CALLBACK = original_callback

        handler.assert_not_awaited()
        result_callback.assert_not_awaited()

    async def test_interrupt_allows_started_mutation_to_finish_atomically(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.send_client_event = AsyncMock()
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        handler_started = asyncio.Event()
        handler_release = asyncio.Event()
        completed_steps = []

        async def atomic_handler(params):
            handler_started.set()
            completed_steps.append("first")
            await handler_release.wait()
            completed_steps.append("second")
            await params.result_callback({"ok": True})

        service.register_function("atomic_tool", atomic_handler)
        _, wrapped_handler = service._registered_function
        result_callback = AsyncMock()
        params = types.SimpleNamespace(
            tool_call_id="call-atomic",
            arguments={},
            result_callback=result_callback,
        )
        service._tool_call_generations["call-atomic"] = 0
        service._conversation_window.begin_user_turn(
            self._message("atomic-user", "user")
        )
        service._conversation_window.activate("atomic-user")
        service._managed_response_sent = True
        service._response_finished.clear()

        tool_task = asyncio.create_task(wrapped_handler(params))
        await handler_started.wait()
        generation = await service.suppress_tools_at_interrupt()
        self.assertFalse(tool_task.cancelled())

        handler_release.set()
        await tool_task
        self.assertEqual(completed_steps, ["first", "second"])
        result_callback.assert_not_awaited()
        self.assertIn("call-atomic", service._retired_aggregator_call_ids)
        service._settle_interrupt_cancel(generation)
        service._response_finished.set()
        await service.on_assistant_response_end_processed(
            service._interrupted_response_generation
        )
        await service._interrupted_cleanup_drained.wait()
        await service._context_deletions_drained.wait()

    async def test_malformed_tool_arguments_fail_fresh_without_scheduling(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_error = AsyncMock()

        await service._handle_evt_function_call_arguments_done(
            types.SimpleNamespace(call_id="call-bad", arguments="{")
        )

        self.assertTrue(service._recovery_active)
        self.assertEqual(service._scheduled_tool_call_ids, set())
        service.push_error.assert_awaited_once()

    async def test_post_interrupt_tool_call_is_tombstoned_before_dispatch(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_error = AsyncMock()
        service._pending_function_calls["call-stop"] = object()

        await service.suppress_function_call_after_interrupt("call-stop")

        self.assertFalse(service._recovery_active)
        self.assertNotIn("call-stop", service._pending_function_calls)
        self.assertIn("call-stop", service._discarded_tool_result_ids)
        service.push_error.assert_not_awaited()

    async def test_interrupt_tombstones_tool_item_created_before_stop(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_error = AsyncMock()
        service._pending_function_calls["call-before-stop"] = object()

        await service.suppress_tools_at_interrupt()

        self.assertFalse(service._recovery_active)
        self.assertNotIn("call-before-stop", service._pending_function_calls)
        self.assertIn("call-before-stop", service._discarded_tool_result_ids)
        service.push_error.assert_not_awaited()

    async def test_interrupt_cancels_turn_waiting_for_transcript(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_error = AsyncMock()
        service.send_client_event = AsyncMock()
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        service._conversation_window.begin_user_turn(
            self._message("user-waiting", "user")
        )
        service._conversation_window.activate("user-waiting")

        await service.suppress_tools_at_interrupt()
        await asyncio.sleep(0)

        self.assertFalse(service._recovery_active)
        self.assertIsNone(service._conversation_window.active_turn_id)
        service.push_error.assert_not_awaited()

        service._start_user_turn = AsyncMock()
        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(item=self._message("user-fresh", "user"))
        )
        self.assertEqual(
            [turn.user_item_id for turn in service._conversation_window.turns],
            ["user-fresh"],
        )

    async def test_fresh_turn_survives_cancelled_plain_response_cleanup(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._context = _LLMContext(
            messages=[{"role": "user", "content": "old request"}]
        )
        service.send_client_event = AsyncMock()
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        service._conversation_window.begin_user_turn(
            self._message("old-user", "user")
        )
        service._conversation_window.attach_transcript("old-user", "old request")
        service._conversation_window.activate("old-user")
        service._managed_response_sent = True
        service._response_finished.clear()

        await service.suppress_tools_at_interrupt()
        self.assertTrue(service._interrupted_response_active)
        self.assertFalse(service._recovery_active)

        service._start_user_turn = AsyncMock()
        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(item=self._message("fresh-user", "user"))
        )
        service._conversation_window.attach_transcript(
            "fresh-user",
            "fresh request",
        )
        service._sync_local_context(include_active_user=True)
        old_assistant = self._message("old-assistant", "assistant", "old reply")
        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(item=old_assistant)
        )
        self.assertEqual(
            service._conversation_window.active_turn_id,
            "fresh-user",
        )
        self.assertEqual(
            service._context.get_messages(),
            [{"role": "user", "content": "fresh request"}],
        )

        await service._handle_evt_response_done(
            types.SimpleNamespace(
                response=types.SimpleNamespace(
                    status="cancelled",
                    output=[
                        types.SimpleNamespace(
                            model_dump=lambda exclude_none: old_assistant
                        )
                    ],
                )
            )
        )
        await asyncio.sleep(0.06)

        self.assertFalse(service._interrupted_response_active)
        self.assertFalse(service._recovery_active)
        self.assertEqual(
            service._conversation_window.active_turn_id,
            "fresh-user",
        )

    async def test_interrupt_cancels_owned_tool_continuation(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_error = AsyncMock()
        async def wait_forever():
            await asyncio.Event().wait()

        continuation = asyncio.create_task(wait_forever())
        service._continuation_task = continuation

        await service.suppress_tools_at_interrupt()
        await asyncio.gather(continuation, return_exceptions=True)

        self.assertTrue(continuation.cancelled())
        self.assertFalse(service._recovery_active)

    async def test_interrupt_does_not_cancel_server_cleanup_tasks(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.send_client_event = AsyncMock()
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        cleanup_release = asyncio.Event()

        async def server_cleanup():
            await cleanup_release.wait()

        cleanup_task = service._track_turn_task(server_cleanup())
        service._conversation_window.begin_user_turn(
            self._message("user-stop", "user")
        )
        service._conversation_window.activate("user-stop")

        await service.suppress_tools_at_interrupt()
        await service._context_deletions_drained.wait()

        self.assertFalse(cleanup_task.cancelled())
        cleanup_release.set()
        await cleanup_task

    async def test_interrupt_during_response_create_releases_fresh_turn_gate(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_error = AsyncMock()
        service.send_client_event = AsyncMock()
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        service._conversation_window.begin_user_turn(
            self._message("user-create", "user")
        )
        service._conversation_window.activate("user-create")
        service._response_finished.clear()

        async def blocked_response_create():
            await asyncio.Event().wait()

        create_task = service._track_turn_task(blocked_response_create())
        service._user_turn_tasks["user-create"] = create_task

        generation = await service.suppress_tools_at_interrupt()

        self.assertTrue(create_task.cancelled())
        self.assertFalse(service._response_finished.is_set())
        self.assertTrue(service._interrupted_response_active)
        self.assertFalse(service._interrupt_cancel_settled.is_set())
        self.assertFalse(service._recovery_active)

        service.note_interrupt_cancel_event("cancel-create", generation)
        handled = await service._maybe_handle_evt_retrieve_conversation_item_error(
            types.SimpleNamespace(
                error=types.SimpleNamespace(
                    code="response_cancel_not_active",
                    event_id="cancel-create",
                )
            )
        )
        await service._interrupted_cleanup_drained.wait()
        await service._context_deletions_drained.wait()
        self.assertTrue(handled)
        self.assertTrue(service._interrupt_cancel_settled.is_set())
        self.assertTrue(service._response_finished.is_set())
        self.assertFalse(service._interrupted_response_active)
        self.assertTrue(service._recovery_active)
        self.assertFalse(service._post_interrupt_response_quarantine)

    async def test_late_cancelled_response_done_cannot_finish_fresh_turn(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._conversation_window.begin_user_turn(
            self._message("fresh-user", "user")
        )
        service._conversation_window.activate("fresh-user")
        service._active_response_id = "fresh-response"
        service._response_finished.clear()
        service._response_interrupt_generations["old-response"] = 9
        service._post_interrupt_response_quarantine = True
        old_response = types.SimpleNamespace(
            id="old-response",
            status="completed",
            output=[self._message("old-assistant", "assistant")],
            usage=None,
        )

        await service._handle_evt_response_done(
            types.SimpleNamespace(response=old_response)
        )

        self.assertFalse(service._response_finished.is_set())
        self.assertEqual(
            [item["id"] for item in service._conversation_window.turns[-1].items],
            ["fresh-user"],
        )
        self.assertIn("old-assistant", service._interrupted_item_ids)

    async def test_stale_normal_response_done_cannot_finish_fresh_turn(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._conversation_window.begin_user_turn(
            self._message("fresh-normal-user", "user")
        )
        service._conversation_window.activate("fresh-normal-user")
        service._active_response_id = "fresh-normal-response"
        service._response_finished.clear()
        service._response_interrupt_generations["old-normal-response"] = None
        old_response = types.SimpleNamespace(
            id="old-normal-response",
            status="completed",
            output=[self._message("old-normal-assistant", "assistant")],
            usage=None,
        )

        await service._handle_evt_response_done(
            types.SimpleNamespace(response=old_response)
        )

        self.assertFalse(service._response_finished.is_set())
        self.assertEqual(
            [item["id"] for item in service._conversation_window.turns[-1].items],
            ["fresh-normal-user"],
        )

    async def test_stale_normal_done_keeps_fresh_unmanaged_ownership(self):
        service = main.SafeRealtimeLLMService(max_context_turns=0)
        service._unmanaged_active_item_ids.add("fresh-unmanaged-user")
        service._active_response_id = "fresh-unmanaged-response"
        service._response_finished.clear()
        service._response_interrupt_generations["old-unmanaged-response"] = None
        old_response = types.SimpleNamespace(
            id="old-unmanaged-response",
            status="completed",
            output=[self._message("old-unmanaged-assistant", "assistant")],
            usage=None,
        )

        await service._handle_evt_response_done(
            types.SimpleNamespace(response=old_response)
        )

        self.assertEqual(
            service._unmanaged_active_item_ids,
            {"fresh-unmanaged-user"},
        )

    async def test_old_session_user_item_is_suppressed_during_recovery(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.begin_recovery()

        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(
                item=self._message("stale-recovery-user", "user")
            )
        )

        self.assertFalse(service._conversation_window.turns)
        self.assertIn(
            "stale-recovery-user",
            service._discarded_user_item_ids,
        )

    async def test_old_session_response_done_is_not_added_during_recovery(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._conversation_window.begin_user_turn(
            self._message("recovery-user", "user")
        )
        service._conversation_window.attach_transcript(
            "recovery-user",
            "request",
        )
        service._conversation_window.activate("recovery-user")
        service._active_response_id = "old-recovery-response"
        service.begin_recovery()
        old_response = types.SimpleNamespace(
            id="old-recovery-response",
            status="completed",
            output=[self._message("unheard-assistant", "assistant")],
            usage=None,
        )

        await service._handle_evt_response_done(
            types.SimpleNamespace(response=old_response)
        )

        turn = service._conversation_window.turns[-1]
        self.assertFalse(turn.complete)
        self.assertEqual(
            [item["id"] for item in turn.items],
            ["recovery-user"],
        )

    async def test_stale_cancel_error_cannot_settle_newer_interrupt(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_error = AsyncMock()
        service._response_finished.clear()

        generation_a = await service.suppress_tools_at_interrupt()
        service.note_interrupt_cancel_event("cancel-a", generation_a)
        generation_b = await service.suppress_tools_at_interrupt()
        service.note_interrupt_cancel_event("cancel-b", generation_b)

        await service._maybe_handle_evt_retrieve_conversation_item_error(
            types.SimpleNamespace(
                error=types.SimpleNamespace(
                    code="response_cancel_not_active",
                    event_id="cancel-a",
                )
            )
        )
        self.assertFalse(service._interrupt_cancel_settled.is_set())
        self.assertEqual(service._interrupt_cancel_generation, generation_b)

        await service._maybe_handle_evt_retrieve_conversation_item_error(
            types.SimpleNamespace(
                error=types.SimpleNamespace(
                    code="response_cancel_not_active",
                    event_id="cancel-b",
                )
            )
        )
        await service._interrupted_cleanup_drained.wait()
        self.assertTrue(service._interrupt_cancel_settled.is_set())

    async def test_pre_clear_stop_item_is_discarded_before_replacement(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.send_client_event = AsyncMock()
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        service.note_interrupt_input_clear(7)

        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(item=self._message("stop-fragment", "user"))
        )
        await service._overlap_deletions_drained.wait()
        self.assertIsNone(service._conversation_window.active_turn_id)

        service.handle_interrupt_input_cleared()
        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(item=self._message("replacement", "user"))
        )
        self.assertEqual(
            service._conversation_window.active_turn_id,
            "replacement",
        )

    async def test_pre_clear_stop_item_is_deleted_in_server_vad_mode(self):
        service = main.SafeRealtimeLLMService(max_context_turns=0)
        service.send_client_event = AsyncMock()
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        service.note_interrupt_input_clear(11)

        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(item=self._message("legacy-stop", "user"))
        )
        await service._overlap_deletions_drained.wait()

        service.send_client_event.assert_awaited_once()
        self.assertIsNone(service._conversation_window.active_turn_id)

    async def test_unscoped_clear_ack_cannot_consume_interrupt_clear(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.note_unscoped_input_clear()
        service.note_interrupt_input_clear(12)

        service.handle_interrupt_input_cleared()
        self.assertEqual(service._interrupt_input_clear_generation, 12)

        service.handle_interrupt_input_cleared()
        self.assertIsNone(service._interrupt_input_clear_generation)

    async def test_failed_newest_unscoped_clear_preserves_fifo_order(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.note_unscoped_input_clear()
        service.note_interrupt_input_clear(13)
        service.note_unscoped_input_clear()

        service.cancel_unscoped_input_clear()
        service.handle_interrupt_input_cleared()
        self.assertEqual(service._interrupt_input_clear_generation, 13)

        service.handle_interrupt_input_cleared()
        self.assertIsNone(service._interrupt_input_clear_generation)

    async def test_standalone_racing_response_detaches_originating_turn(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.send_client_event = AsyncMock()
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        service._conversation_window.begin_user_turn(
            self._message("dangling-user", "user")
        )
        service._conversation_window.activate("dangling-user")
        service._response_finished.clear()

        await service.mark_interrupted_response()

        self.assertIsNone(service._conversation_window.active_turn_id)
        self.assertIn("dangling-user", service._interrupted_turn_ids)
        generation = service._interrupt_cancel_generation
        service._settle_interrupt_cancel(generation)
        service._response_finished.set()
        await service.on_assistant_response_end_processed(
            service._interrupted_response_generation
        )
        await service._interrupted_cleanup_drained.wait()
        await service._context_deletions_drained.wait()
        self.assertEqual(service._conversation_window.turns, [])

    async def test_interrupted_tool_output_is_owned_by_cleanup(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._interrupted_response_active = True
        service._interrupted_tool_result_ids.add("call-late")
        event = types.SimpleNamespace(
            type="conversation.item.create",
            item={
                "id": "output-late",
                "type": "function_call_output",
                "call_id": "call-late",
                "output": "{}",
            },
        )

        await service.send_client_event(event)

        self.assertEqual(
            service._tool_output_item_ids["call-late"],
            "output-late",
        )
        self.assertIn("output-late", service._interrupted_item_ids)

    async def test_tool_output_known_before_stop_is_deleted_with_turn(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        output_event = types.SimpleNamespace(
            type="conversation.item.create",
            item={
                "id": "known-output",
                "type": "function_call_output",
                "call_id": "known-call",
                "output": "{}",
            },
        )
        await service.send_client_event(output_event)
        service._conversation_window.begin_user_turn(
            self._message("known-user", "user")
        )
        service._conversation_window.activate("known-user")
        service._conversation_window.observe_item(
            {
                "id": "known-call-item",
                "type": "function_call",
                "call_id": "known-call",
                "name": "known_tool",
                "arguments": "{}",
            }
        )

        await service.suppress_tools_at_interrupt()
        await service._interrupted_cleanup_drained.wait()
        await service._context_deletions_drained.wait()

        deleted_ids = {
            getattr(event, "item_id", None)
            for event in service._sent_client_events
            if getattr(event, "type", None) == "conversation.item.delete"
        }
        self.assertIn("known-output", deleted_ids)

    async def test_post_response_running_tool_ownership_is_retired_on_stop(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.send_client_event = AsyncMock()
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        assistant = types.SimpleNamespace(
            _function_calls_in_progress={"old-call": object()},
            _started=0,
            reset=AsyncMock(),
        )
        service._assistant_context_aggregator = assistant
        service._conversation_window.begin_user_turn(
            self._message("old-tool-user", "user")
        )
        service._conversation_window.activate("old-tool-user")
        service._running_tool_call_ids.add("old-call")

        await service.suppress_tools_at_interrupt()
        await service._interrupted_cleanup_drained.wait()
        await service._context_deletions_drained.wait()

        self.assertEqual(assistant._function_calls_in_progress, {})

    async def test_tool_execution_lock_prevents_mutation_interleaving(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        first_started = asyncio.Event()
        first_release = asyncio.Event()
        second_started = asyncio.Event()

        async def first_handler(params):
            first_started.set()
            await first_release.wait()
            await params.result_callback({"first": True})

        async def second_handler(params):
            second_started.set()
            await params.result_callback({"second": True})

        service.register_function("first_tool", first_handler)
        _, first_wrapper = service._registered_function
        service.register_function("second_tool", second_handler)
        _, second_wrapper = service._registered_function
        service._tool_call_generations.update(
            {
                "first-call": 0,
                "second-call": 0,
            }
        )
        first_params = types.SimpleNamespace(
            tool_call_id="first-call",
            arguments={},
            result_callback=AsyncMock(),
        )
        second_params = types.SimpleNamespace(
            tool_call_id="second-call",
            arguments={},
            result_callback=AsyncMock(),
        )

        first_task = asyncio.create_task(first_wrapper(first_params))
        await first_started.wait()
        second_task = asyncio.create_task(second_wrapper(second_params))
        await asyncio.sleep(0.01)
        self.assertFalse(second_started.is_set())

        first_release.set()
        await asyncio.gather(first_task, second_task)
        self.assertTrue(second_started.is_set())

    async def test_tool_execution_lock_timeout_returns_retryable_error(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.TOOL_EXECUTION_LOCK_TIMEOUT_S = 0.01
        await service._tool_execution_lock.acquire()
        handler = AsyncMock()
        service.register_function("blocked_tool", handler)
        _, wrapper = service._registered_function
        service._tool_call_generations["blocked-call"] = 0
        callback = AsyncMock()
        params = types.SimpleNamespace(
            tool_call_id="blocked-call",
            arguments={},
            result_callback=callback,
        )

        try:
            await wrapper(params)
        finally:
            service._tool_execution_lock.release()

        handler.assert_not_awaited()
        callback.assert_awaited_once()
        result = callback.await_args_list[0][0][0]
        self.assertIn("previous home action", result["error"])

    async def test_unmanaged_dangling_user_is_deleted_with_response(self):
        service = main.SafeRealtimeLLMService(max_context_turns=0)
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        service._unmanaged_active_item_ids.update(
            {
                "dangling-user-one",
                "dangling-user-two",
            }
        )
        service._response_finished.clear()

        generation = await service.suppress_tools_at_interrupt()
        service._settle_interrupt_cancel(generation)
        service._response_finished.set()
        service._interrupted_aggregation_drained.set()
        service._schedule_interrupted_cleanup()
        await service._interrupted_cleanup_drained.wait()
        await service._context_deletions_drained.wait()

        deleted_ids = {
            getattr(event, "item_id", None)
            for event in service._sent_client_events
            if getattr(event, "type", None) == "conversation.item.delete"
        }
        self.assertIn("dangling-user-one", deleted_ids)
        self.assertIn("dangling-user-two", deleted_ids)

    async def test_unmanaged_tool_response_keeps_items_until_final_reply(self):
        service = main.SafeRealtimeLLMService(max_context_turns=0)
        service._unmanaged_active_item_ids.update(
            {
                "legacy-user",
                "legacy-call-item",
            }
        )
        await service.send_client_event(
            types.SimpleNamespace(
                type="conversation.item.create",
                item={
                    "id": "legacy-output",
                    "type": "function_call_output",
                    "call_id": "legacy-call",
                    "output": "{}",
                },
            )
        )
        tool_response = types.SimpleNamespace(
            id=None,
            status="completed",
            output=[
                self._message("legacy-preamble", "assistant"),
                {
                    "id": "legacy-call-item",
                    "type": "function_call",
                    "call_id": "legacy-call",
                    "name": "legacy_tool",
                    "arguments": "{}",
                }
            ],
            usage=None,
        )

        await service._handle_evt_response_done(
            types.SimpleNamespace(response=tool_response)
        )

        self.assertEqual(
            service._unmanaged_active_item_ids,
            {
                "legacy-user",
                "legacy-call-item",
                "legacy-output",
                "legacy-preamble",
            },
        )
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        await service.suppress_tools_at_interrupt()
        await service._interrupted_cleanup_drained.wait()
        await service._context_deletions_drained.wait()
        deleted_ids = {
            getattr(event, "item_id", None)
            for event in service._sent_client_events
            if getattr(event, "type", None) == "conversation.item.delete"
        }
        self.assertIn("legacy-output", deleted_ids)
        self.assertIn("legacy-preamble", deleted_ids)

    async def test_detached_prune_failure_enters_recovery(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_error = AsyncMock()
        service._prune_complete_turns = AsyncMock(
            side_effect=RuntimeError("delete confirmation failed")
        )

        prune_task = service._track_turn_task(service._run_prune_transaction())
        with self.assertRaisesRegex(RuntimeError, "delete confirmation failed"):
            await prune_task

        self.assertTrue(service._recovery_active)
        service.push_error.assert_awaited_once()

    async def test_interrupt_cannot_cancel_shielded_prune_transaction(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.send_client_event = AsyncMock()
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        service._context = _LLMContext()
        prune_started = asyncio.Event()
        prune_release = asyncio.Event()
        prune_completed = asyncio.Event()

        async def blocked_prune():
            prune_started.set()
            await prune_release.wait()
            prune_completed.set()

        service._run_prune_transaction = blocked_prune
        service._conversation_window.begin_user_turn(
            self._message("prune-user", "user")
        )
        service._conversation_window.activate("prune-user")
        service._transcript_ready_events["prune-user"] = asyncio.Event()
        service._track_user_turn_task(
            "prune-user",
            service._start_user_turn("prune-user"),
        )
        await prune_started.wait()

        await service.suppress_tools_at_interrupt()
        prune_release.set()
        await prune_completed.wait()
        await service._interrupted_cleanup_drained.wait()
        await service._context_deletions_drained.wait()

        self.assertTrue(prune_completed.is_set())

    async def test_bound_assistant_end_hook_acknowledges_after_aggregation(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        original_end = AsyncMock()
        assistant = types.SimpleNamespace(_handle_llm_end=original_end)
        pair = types.SimpleNamespace(assistant=lambda: assistant)
        service.on_assistant_response_end_processed = AsyncMock()
        frame = object()

        service.bind_context_aggregator(pair)
        service._assistant_end_generations.append(12)
        await assistant._handle_llm_end(frame)

        original_end.assert_awaited_once_with(frame)
        service.on_assistant_response_end_processed.assert_awaited_once_with(12)

    async def test_stale_assistant_end_cannot_release_newer_interrupt(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        assistant = types.SimpleNamespace(
            _function_calls_in_progress={},
            _started=1,
            reset=AsyncMock(),
        )
        service._assistant_context_aggregator = assistant
        service._interrupted_response_active = True
        service._interrupted_response_generation = 22
        service._interrupted_aggregation_drained.clear()

        await service.on_assistant_response_end_processed(21)

        self.assertFalse(service._interrupted_aggregation_drained.is_set())
        assistant.reset.assert_not_awaited()

    async def test_late_in_progress_frame_cannot_recreate_interrupted_call(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        original_in_progress = AsyncMock()
        assistant = types.SimpleNamespace(
            _handle_llm_end=AsyncMock(),
            _handle_function_call_in_progress=original_in_progress,
        )
        service.bind_context_aggregator(
            types.SimpleNamespace(assistant=lambda: assistant)
        )
        service._retired_aggregator_call_ids.add("retired-call")
        frame = types.SimpleNamespace(tool_call_id="retired-call")

        await assistant._handle_function_call_in_progress(frame)

        original_in_progress.assert_not_awaited()

    async def test_fresh_response_waits_for_interrupt_cancel_settlement(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._context = _LLMContext()
        service._api_session_ready = True
        service._websocket = object()
        service.send_client_event = AsyncMock()
        service._conversation_window.begin_user_turn(
            self._message("fresh-gated", "user")
        )
        service._conversation_window.attach_transcript(
            "fresh-gated",
            "fresh gated request",
        )
        service._conversation_window.activate("fresh-gated")
        transcript_ready = asyncio.Event()
        transcript_ready.set()
        service._transcript_ready_events["fresh-gated"] = transcript_ready
        service._interrupt_cancel_pending = True
        service._interrupt_cancel_settled.clear()

        fresh_task = asyncio.create_task(service._start_user_turn("fresh-gated"))
        await asyncio.sleep(0.01)
        service.send_client_event.assert_not_awaited()

        service._settle_interrupt_cancel()
        await fresh_task
        service.send_client_event.assert_awaited_once()

    async def test_tool_barge_in_keeps_replacement_turn_admissible(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._conversation_window.begin_user_turn(
            self._message("tool-user", "user")
        )
        service._conversation_window.activate("tool-user")
        service._pending_function_calls["call-stop"] = object()
        service._managed_response_sent = True
        service._response_finished.clear()

        await service.suppress_tools_at_interrupt()
        self.assertFalse(service._recovery_active)
        self.assertNotIn("call-stop", service._pending_function_calls)

        service._start_user_turn = AsyncMock()
        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(item=self._message("replacement-user", "user"))
        )
        self.assertEqual(
            service._conversation_window.active_turn_id,
            "replacement-user",
        )

    async def test_interrupt_resets_pending_assistant_aggregation(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.send_client_event = AsyncMock()
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        assistant_aggregator = types.SimpleNamespace(reset=AsyncMock())
        service._assistant_context_aggregator = assistant_aggregator
        service._conversation_window.begin_user_turn(
            self._message("user-reset", "user")
        )
        service._conversation_window.activate("user-reset")
        service._managed_response_sent = True
        service._response_finished.clear()

        await service.suppress_tools_at_interrupt()
        assistant_aggregator.reset.assert_not_awaited()

        await service.on_assistant_response_end_processed(
            service._interrupted_response_generation
        )
        await service._interrupted_cleanup_drained.wait()
        await service._context_deletions_drained.wait()

        assistant_aggregator.reset.assert_awaited_once()

    async def test_unregistered_tool_fails_fresh_without_runner(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_error = AsyncMock()
        service._pending_function_calls["call-unknown"] = types.SimpleNamespace(
            name="missing_tool"
        )

        await service._handle_evt_function_call_arguments_done(
            types.SimpleNamespace(call_id="call-unknown", arguments="{}")
        )

        self.assertTrue(service._recovery_active)
        self.assertNotIn("call-unknown", service._pending_function_calls)
        service.push_error.assert_awaited_once()

    async def test_scheduled_tool_without_runner_is_finalized_and_recovered(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_error = AsyncMock()
        service.broadcast_frame = AsyncMock()
        service.register_function("test_tool", AsyncMock())
        service._pending_function_calls["call-no-runner"] = types.SimpleNamespace(
            name="test_tool"
        )

        await service._handle_evt_function_call_arguments_done(
            types.SimpleNamespace(call_id="call-no-runner", arguments="{}")
        )
        await asyncio.sleep(0.12)

        self.assertTrue(service._recovery_active)
        self.assertNotIn("call-no-runner", service._scheduled_tool_call_ids)
        service.broadcast_frame.assert_awaited_once()
        service.push_error.assert_awaited_once()

    async def test_tool_queued_before_recovery_never_executes_after_recovery(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        handler = AsyncMock()
        service.register_function("stale_tool", handler)
        _, wrapped_handler = service._registered_function
        result_callback = AsyncMock()
        params = types.SimpleNamespace(
            tool_call_id="call-stale",
            arguments={},
            result_callback=result_callback,
        )
        service._tool_call_generations["call-stale"] = 0
        service.begin_recovery()

        await wrapped_handler(params)

        handler.assert_not_awaited()
        result_callback.assert_awaited_once()
        self.assertIn("call-stale", service._discarded_tool_result_ids)

    async def test_tombstoned_tool_cannot_execute_after_reset_generation_clear(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        handler = AsyncMock()
        service.register_function("stale_tool", handler)
        _, wrapped_handler = service._registered_function
        result_callback = AsyncMock()
        params = types.SimpleNamespace(
            tool_call_id="old-call",
            arguments={},
            result_callback=result_callback,
        )
        service._discarded_tool_result_ids.add("old-call")

        await wrapped_handler(params)

        handler.assert_not_awaited()
        result_callback.assert_awaited_once()

    async def test_server_vad_session_update_has_explicit_response_ownership(self):
        service = main.SafeRealtimeLLMService(
            server_vad_response_ownership=True,
            server_vad_interrupt_response=False,
        )
        sent = []
        service._ws_send = AsyncMock(side_effect=lambda payload: sent.append(payload))
        event = types.SimpleNamespace(
            type="session.update",
            model_dump=lambda exclude_none: {
                "type": "session.update",
                "session": {
                    "audio": {
                        "input": {
                            "turn_detection": {"type": "server_vad"}
                        }
                    }
                },
            },
        )

        await service.send_client_event(event)

        turn_detection = sent[0]["session"]["audio"]["input"]["turn_detection"]
        self.assertTrue(turn_detection["create_response"])
        self.assertFalse(turn_detection["interrupt_response"])

    async def test_transcription_failure_immediately_fails_fresh(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_error = AsyncMock()
        service._conversation_window.begin_user_turn(
            self._message("user-1", "user")
        )
        service._conversation_window.activate("user-1")

        async def messages():
            yield types.SimpleNamespace(
                type="conversation.item.input_audio_transcription.failed",
                item_id="user-1",
            )

        service._websocket = messages()
        await service._receive_task_handler()

        self.assertTrue(service._recovery_active)
        service.push_error.assert_awaited_once()

    async def test_recovery_drains_tool_result_without_sending_orphan_output(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._recovery_active = True
        service._pending_tool_result_ids = {"call-1"}
        service._pending_tool_results_drained.clear()
        context = _LLMContext(
            messages=[
                {
                    "role": "tool",
                    "tool_call_id": "call-1",
                    "content": {"status": "done"},
                }
            ]
        )

        with patch.object(
            _OpenAIRealtimeLLMService,
            "_handle_context",
            new=AsyncMock(),
        ) as parent_handle_context:
            await service._handle_context(context)

        parent_handle_context.assert_not_awaited()
        self.assertEqual(service._pending_tool_result_ids, set())
        self.assertTrue(service._pending_tool_results_drained.is_set())

    async def test_connection_recovery_retries_until_ready(self):
        service = types.SimpleNamespace(
            reset_conversation=AsyncMock(
                side_effect=[
                    RuntimeError("first failure"),
                    RuntimeError("second failure"),
                    None,
                ]
            )
        )
        phase_emitter = types.SimpleNamespace(force_idle=AsyncMock())
        recovery = websocket_handler.ConnectionRecovery(
            openai_service=service,
            phase_emitter=phase_emitter,
        )
        connected_at = recovery._connected_at
        delays = []

        async def record_sleep(delay):
            delays.append(delay)

        with patch.object(websocket_handler.asyncio, "sleep", side_effect=record_sleep):
            await recovery._recover("test disconnect")

        self.assertEqual(service.reset_conversation.await_count, 3)
        self.assertEqual(delays, [1.0, 2.0])
        self.assertGreaterEqual(recovery._connected_at, connected_at)
        self.assertFalse(recovery._reconnecting)
        phase_emitter.force_idle.assert_awaited_once()

    async def test_connection_recovery_suppresses_then_drains_before_reset(self):
        order = []

        class Service:
            def begin_recovery(self):
                order.append("begin")

            async def wait_for_pending_tool_results(self):
                order.append("drain")

            async def reset_conversation(self):
                order.append("reset")

            def mark_recovery_complete(self):
                order.append("complete")

        recovery = websocket_handler.ConnectionRecovery(openai_service=Service())
        recovery.set_recovery_complete_callback(
            lambda: order.append("kill-clear")
        )

        await recovery._recover("test ordering")

        self.assertEqual(
            order,
            ["begin", "drain", "reset", "complete", "kill-clear"],
        )

    async def test_compaction_failure_during_cooldown_schedules_delayed_recovery(self):
        service = types.SimpleNamespace(
            begin_recovery=Mock(),
            wait_for_pending_tool_results=AsyncMock(return_value=True),
            reset_conversation=AsyncMock(),
            mark_recovery_complete=Mock(),
        )
        recovery = websocket_handler.ConnectionRecovery(openai_service=service)
        recovery._last_attempt = websocket_handler.time.monotonic()
        recovery.push_frame = AsyncMock()
        frame = websocket_handler.ErrorFrame()
        frame.error = "context compaction failed: test"

        await recovery.process_frame(frame, None)

        self.assertTrue(recovery._reconnecting)
        self.assertIsNotNone(recovery._recovery_task)
        service.reset_conversation.assert_not_awaited()
        await recovery.cleanup()

    async def test_forced_dangling_reconnect_bypasses_cooldown(self):
        service = types.SimpleNamespace(
            begin_recovery=Mock(),
            wait_for_pending_tool_results=AsyncMock(return_value=True),
            reset_conversation=AsyncMock(),
            mark_recovery_complete=Mock(),
        )
        recovery = websocket_handler.ConnectionRecovery(openai_service=service)
        recovery._last_attempt = websocket_handler.time.monotonic()
        recovery._reconnecting = True
        recovery._recovery_delayed = True
        recovery._recovery_task = asyncio.create_task(asyncio.sleep(60))

        await recovery.force_reconnect(
            "dangling server VAD boundary",
            bypass_cooldown=True,
        )

        for _ in range(3):
            await asyncio.sleep(0)
        service.reset_conversation.assert_awaited_once()

    async def test_connection_recovery_caps_retry_backoff(self):
        service = types.SimpleNamespace(
            reset_conversation=AsyncMock(
                side_effect=[
                    RuntimeError("failure 1"),
                    RuntimeError("failure 2"),
                    RuntimeError("failure 3"),
                    RuntimeError("failure 4"),
                    RuntimeError("failure 5"),
                    RuntimeError("failure 6"),
                    None,
                ]
            )
        )
        recovery = websocket_handler.ConnectionRecovery(openai_service=service)
        delays = []

        async def record_sleep(delay):
            delays.append(delay)

        with patch.object(websocket_handler.asyncio, "sleep", side_effect=record_sleep):
            await recovery._recover("test prolonged outage")

        self.assertEqual(delays, [1.0, 2.0, 4.0, 8.0, 15.0, 15.0])
        self.assertFalse(recovery._reconnecting)

    async def test_connection_recovery_rejects_wake_until_ready(self):
        phase_emitter = types.SimpleNamespace(force_idle=AsyncMock())
        recovery = websocket_handler.ConnectionRecovery(
            openai_service=types.SimpleNamespace(),
            phase_emitter=phase_emitter,
        )

        self.assertFalse(await recovery.reject_wake_while_recovering())
        phase_emitter.force_idle.assert_not_awaited()

        recovery._reconnecting = True
        self.assertTrue(await recovery.reject_wake_while_recovering())
        phase_emitter.force_idle.assert_awaited_once_with(
            "wake during OpenAI recovery",
            force_delivery=True,
        )

    async def test_connection_recovery_drops_audio_until_ready(self):
        recovery = websocket_handler.ConnectionRecovery(
            openai_service=types.SimpleNamespace()
        )
        recovery._refresh_task = Mock()
        recovery._reconnecting = True
        recovery.push_frame = AsyncMock()

        await recovery.process_frame(
            websocket_handler.InputAudioRawFrame(),
            None,
        )

        recovery.push_frame.assert_not_awaited()

    async def test_connection_recovery_cleanup_cancels_owned_tasks(self):
        recovery = websocket_handler.ConnectionRecovery(
            openai_service=types.SimpleNamespace()
        )
        recovery._reconnecting = True
        recovery_any = cast(Any, recovery)
        recovery_task = asyncio.create_task(asyncio.sleep(10))
        refresh_task = asyncio.create_task(asyncio.sleep(10))
        recovery_any._recovery_task = recovery_task
        recovery_any._refresh_task = refresh_task

        await recovery.cleanup()

        self.assertTrue(recovery_task.cancelled())
        self.assertTrue(refresh_task.cancelled())
        self.assertFalse(recovery._reconnecting)

    async def test_control_broadcast_is_compact_and_keeps_socket_open(self):
        class WebSocket:
            def __init__(self):
                self.send = AsyncMock()
                self.close = AsyncMock()

        websocket = cast(Any, WebSocket())
        handler = websocket_handler.WebSocketHandler()
        handler._websockets.add(websocket)

        async def acknowledge():
            while websocket.send.await_count == 0:
                await asyncio.sleep(0)
            prepared = json.loads(websocket.send.await_args_list[0].args[0])
            handler._handle_graceful_close_ack(
                {
                    "token": prepared["token"],
                    "stage": "prepared",
                    "accepted": True,
                }
            )
            while websocket.send.await_count < 2:
                await asyncio.sleep(0)
            committed = json.loads(websocket.send.await_args_list[1].args[0])
            handler._handle_graceful_close_ack(
                {
                    "token": committed["token"],
                    "stage": "committed",
                    "accepted": True,
                }
            )

        ack_task = asyncio.create_task(acknowledge())
        await handler.arm_graceful_close()
        await ack_task

        self.assertEqual(websocket.send.await_count, 2)
        websocket.close.assert_not_awaited()
        prepared = json.loads(websocket.send.await_args_list[0].args[0])
        committed = json.loads(websocket.send.await_args_list[1].args[0])
        self.assertEqual(prepared["type"], "prepare_suppress_followup")
        self.assertEqual(committed["type"], "commit_suppress_followup")
        self.assertEqual(prepared["token"], committed["token"])

    async def test_graceful_close_fails_without_connected_firmware(self):
        handler = websocket_handler.WebSocketHandler()

        with self.assertRaisesRegex(RuntimeError, "No Voice PE is connected"):
            await handler.arm_graceful_close()

    async def test_graceful_close_accepts_ack_after_previous_one_second_deadline(self):
        class WebSocket:
            def __init__(self):
                self.send = AsyncMock()

        websocket = cast(Any, WebSocket())
        handler = websocket_handler.WebSocketHandler()
        handler._websockets.add(websocket)

        async def acknowledge_after_delay():
            while websocket.send.await_count == 0:
                await asyncio.sleep(0)
            prepared = json.loads(websocket.send.await_args_list[0].args[0])
            await asyncio.sleep(1.05)
            handler._handle_graceful_close_ack(
                {
                    "token": prepared["token"],
                    "stage": "prepared",
                    "accepted": True,
                }
            )
            while websocket.send.await_count < 2:
                await asyncio.sleep(0)
            committed = json.loads(websocket.send.await_args_list[1].args[0])
            handler._handle_graceful_close_ack(
                {
                    "token": committed["token"],
                    "stage": "committed",
                    "accepted": True,
                }
            )

        ack_task = asyncio.create_task(acknowledge_after_delay())
        await handler.arm_graceful_close()
        await ack_task

        self.assertEqual(websocket.send.await_count, 2)

    async def test_graceful_close_fails_when_firmware_rejects_turn(self):
        class WebSocket:
            def __init__(self):
                self.send = AsyncMock()

        websocket = cast(Any, WebSocket())
        handler = websocket_handler.WebSocketHandler()
        handler._websockets.add(websocket)

        async def reject():
            while websocket.send.await_count == 0:
                await asyncio.sleep(0)
            prepared = json.loads(websocket.send.await_args.args[0])
            handler._handle_graceful_close_ack(
                {
                    "token": prepared["token"],
                    "stage": "prepared",
                    "accepted": False,
                }
            )

        reject_task = asyncio.create_task(reject())
        with self.assertRaisesRegex(RuntimeError, "rejected graceful close"):
            await handler.arm_graceful_close()
        await reject_task

    async def test_new_tool_after_prepare_cancels_instead_of_committing(self):
        class WebSocket:
            def __init__(self):
                self.send = AsyncMock()

        websocket = cast(Any, WebSocket())
        handler = websocket_handler.WebSocketHandler()
        handler._websockets.add(websocket)
        original_generation = websocket_handler.TURN_LIVENESS.non_close_tool_generation

        async def acknowledge_then_start_tool():
            while websocket.send.await_count == 0:
                await asyncio.sleep(0)
            prepared = json.loads(websocket.send.await_args_list[0].args[0])
            handler._handle_graceful_close_ack(
                {
                    "token": prepared["token"],
                    "stage": "prepared",
                    "accepted": True,
                }
            )
            websocket_handler.TURN_LIVENESS.non_close_tool_generation += 1

        ack_task = asyncio.create_task(acknowledge_then_start_tool())
        try:
            await handler.arm_graceful_close(original_generation)
            await ack_task
        finally:
            websocket_handler.TURN_LIVENESS.non_close_tool_generation = original_generation

        message_types = [
            json.loads(call.args[0])["type"]
            for call in websocket.send.await_args_list
        ]
        self.assertEqual(
            message_types,
            ["prepare_suppress_followup", "cancel_suppress_followup"],
        )

    async def test_lost_commit_ack_retains_token_for_later_cancellation(self):
        class WebSocket:
            def __init__(self):
                self.send = AsyncMock()

        websocket = cast(Any, WebSocket())
        handler = websocket_handler.WebSocketHandler()
        handler.GRACEFUL_CLOSE_ACK_TIMEOUT_S = 0.05
        handler._websockets.add(websocket)

        async def acknowledge_prepare_only():
            while websocket.send.await_count == 0:
                await asyncio.sleep(0)
            prepared = json.loads(websocket.send.await_args_list[0].args[0])
            handler._handle_graceful_close_ack(
                {
                    "token": prepared["token"],
                    "stage": "prepared",
                    "accepted": True,
                }
            )

        ack_task = asyncio.create_task(acknowledge_prepare_only())
        with self.assertRaisesRegex(RuntimeError, "did not acknowledge.*committed"):
            await handler.arm_graceful_close()
        await ack_task

        committed = json.loads(websocket.send.await_args_list[1].args[0])
        self.assertEqual(handler._graceful_close_committed_token, committed["token"])

        await handler.cancel_graceful_close()

        cancelled = json.loads(websocket.send.await_args_list[2].args[0])
        self.assertEqual(cancelled["type"], "cancel_suppress_followup")
        self.assertEqual(cancelled["token"], committed["token"])
        self.assertIsNone(handler._graceful_close_committed_token)

    async def test_failed_cancel_keeps_token_and_blocks_caller(self):
        class WebSocket:
            async def send(self, _message):
                raise RuntimeError("test send failure")

        handler = websocket_handler.WebSocketHandler()
        handler._websockets.add(WebSocket())
        handler._graceful_close_committed_token = 42

        with self.assertRaisesRegex(RuntimeError, "No Voice PE accepted"):
            await handler.cancel_graceful_close()

        self.assertEqual(handler._graceful_close_committed_token, 42)

    def test_graceful_close_ignores_stale_ack_token(self):
        handler = websocket_handler.WebSocketHandler()
        handler._graceful_close_pending_token = 42

        handler._handle_graceful_close_ack(
            {"token": 41, "stage": "committed", "accepted": True}
        )

        self.assertFalse(handler._graceful_close_ack.is_set())

    async def test_later_tool_generation_cancels_deferred_close(self):
        handler = websocket_handler.WebSocketHandler()
        original_generation = websocket_handler.TURN_LIVENESS.non_close_tool_generation
        handler.arm_graceful_close = AsyncMock()
        try:
            await handler.request_graceful_close()
            websocket_handler.TURN_LIVENESS.non_close_tool_generation += 1

            await handler._arm_requested_graceful_close()

            handler.arm_graceful_close.assert_not_awaited()
            self.assertIsNone(handler._graceful_close_requested_generation)
        finally:
            websocket_handler.TURN_LIVENESS.non_close_tool_generation = original_generation

    async def test_unchanged_tool_generation_arms_at_bot_stop_boundary(self):
        handler = websocket_handler.WebSocketHandler()
        handler.arm_graceful_close = AsyncMock()

        await handler.request_graceful_close()
        await handler._arm_requested_graceful_close()

        handler.arm_graceful_close.assert_awaited_once_with(
            websocket_handler.TURN_LIVENESS.non_close_tool_generation
        )

    async def test_build_pipeline_does_not_start_runner(self):
        handler = websocket_handler.WebSocketHandler()
        runner = AsyncMock()
        pipeline = object()
        task = object()

        with (
            patch.object(websocket_handler, "ConnectionRecovery", _Placeholder),
            patch.object(websocket_handler, "InputResampler", _Placeholder),
            patch.object(websocket_handler, "SessionActivityTracker", _Placeholder),
            patch.object(websocket_handler, "TranscriptLogger", _Placeholder),
            patch.object(websocket_handler, "PhaseEmitter", _FakePhaseEmitter),
            patch.object(websocket_handler, "Pipeline", return_value=pipeline),
            patch.object(websocket_handler, "PipelineRunner", return_value=runner),
            patch.object(websocket_handler, "PipelineTask", return_value=task),
            patch.object(websocket_handler.asyncio, "create_task") as create_task,
        ):
            result = handler.build_pipeline(
                transport=_FakeTransport(),
                openai_service=_FakeOpenAIService(),
                client_id="server",
            )

        self.assertEqual(result, (pipeline, runner, task))
        create_task.assert_not_called()
        runner.run.assert_not_called()

    async def test_run_owns_runner_and_client_reuses_pipeline_service(self):
        application = cast(Any, main.Application())
        handler = _FakeWebSocketHandler()
        session_manager = _FakeSessionManager()
        recording_service = _FakeRecordingService()
        authoritative_service = object()
        detached_service = object()
        transport = object()
        task = object()

        async def initialize():
            application.websocket_handler = handler
            application.websocket_transport = transport
            application.session_manager = session_manager
            application.audio_recording_service = recording_service
            application.turn_detection_type = "server_vad"
            application.semantic_vad_create_response = False

        async def ensure_service(*args, **kwargs):
            if application.openai_service is None:
                application.openai_service = authoritative_service
            else:
                application.openai_service = detached_service
            client_id = kwargs.get("client_id")
            if client_id:
                session_manager.set_current_service(client_id, application.openai_service)
            return application.openai_service

        runner = AsyncMock()

        async def run_pipeline(current_task):
            self.assertIs(current_task, task)
            await handler.callbacks["on_client_connected_callback"]("voice-pe")

        runner.run.side_effect = run_pipeline

        def build_pipeline(current_transport, client_id):
            self.assertIs(current_transport, transport)
            self.assertEqual(client_id, "server")
            application.runner = runner
            application.current_task = task

        application.initialize = AsyncMock(side_effect=initialize)
        application._ensure_openai_service = AsyncMock(side_effect=ensure_service)
        application._build_pipeline_for_transport = Mock(side_effect=build_pipeline)
        application.cleanup = AsyncMock()

        await application.run()

        application._ensure_openai_service.assert_awaited_once_with()
        runner.run.assert_awaited_once_with(task)
        self.assertIs(application.openai_service, authoritative_service)
        self.assertIs(
            session_manager.get_current_service("voice-pe"), authoritative_service
        )
        self.assertIs(
            handler.callbacks["openai_service_getter"]("voice-pe"),
            authoritative_service,
        )
        self.assertEqual(recording_service.started, ["voice-pe"])


if __name__ == "__main__":
    unittest.main()
