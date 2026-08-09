"""Offline lifecycle tests for the single-device voice pipeline."""

import asyncio
import base64
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


class _TTSAudioRawFrame(_Frame):
    _next_id = 0

    def __init__(self, audio=b"", sample_rate=24000, num_channels=1):
        type(self)._next_id += 1
        self.id = type(self)._next_id
        self.audio = audio
        self.sample_rate = sample_rate
        self.num_channels = num_channels


class _CurrentAudioResponse:
    def __init__(self, item_id, content_index, start_time_ms):
        self.item_id = item_id
        self.content_index = content_index
        self.start_time_ms = start_time_ms
        self.total_size = 0


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

    async def _handle_evt_text_delta(self, evt):
        if evt.delta:
            await self.push_frame(("text", evt.delta))

    async def _handle_evt_audio_transcript_delta(self, evt):
        if evt.delta:
            await self.push_frame(("audio_transcript", evt.delta))

    async def _handle_evt_audio_done(self, _evt):
        pass

    async def _handle_evt_conversation_item_added(self, _evt):
        pass

    async def _handle_evt_conversation_item_done(self, _evt):
        pass

    async def handle_evt_input_audio_transcription_completed(self, _evt):
        pass

    async def _handle_evt_speech_stopped(self, _evt):
        pass

    async def _handle_evt_speech_started(self, _evt):
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
        self.in_flight += 1

    def tool_finished(self):
        self.in_flight = max(0, self.in_flight - 1)

    def non_close_tool_started(self):
        self.non_close_tool_generation += 1


_stub_module("dotenv", load_dotenv=lambda: None)
_stub_module("loguru", logger=Mock())
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
    CurrentAudioResponse=_CurrentAudioResponse,
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
    TTSAudioRawFrame=_TTSAudioRawFrame,
    TTSStartedFrame=type("TTSStartedFrame", (_Frame,), {}),
    TTSStoppedFrame=type("TTSStoppedFrame", (_Frame,), {}),
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


class _InputAudioBufferClearEvent(_RealtimeEvent):
    _counter = 0

    def __init__(self):
        type(self)._counter += 1
        super().__init__(
            type="input_audio_buffer.clear",
            event_id=f"clear-{type(self)._counter}",
        )


class _ResponseCancelEvent(_RealtimeEvent):
    _counter = 0

    def __init__(self):
        type(self)._counter += 1
        super().__init__(
            type="response.cancel",
            event_id=f"cancel-{type(self)._counter}",
        )


_event_module = sys.modules["pipecat.services.openai.realtime.events"]
setattr(_event_module, "ConversationItem", _ConversationItem)
setattr(_event_module, "ConversationItemCreateEvent", _ConversationItemCreateEvent)
setattr(_event_module, "ConversationItemDeleteEvent", _ConversationItemDeleteEvent)
setattr(_event_module, "ResponseProperties", _ResponseProperties)
setattr(_event_module, "ResponseCreateEvent", _ResponseCreateEvent)
setattr(_event_module, "InputAudioBufferClearEvent", _InputAudioBufferClearEvent)
setattr(_event_module, "ResponseCancelEvent", _ResponseCancelEvent)
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
    "app.false_alarm_tool",
    get_false_alarm_tool_definition=lambda: {},
    create_false_alarm_tool_handler=lambda: None,
)
_stub_module("app.tts_announcer", DeviceAnnouncer=_Placeholder)

from app import main  # noqa: E402
from app import websocket_handler  # noqa: E402


class _FakePhaseEmitter:
    def __init__(self, *args, **kwargs):
        pass

    def set_kill_window_handlers(self, **kwargs):
        pass

    def note_wake(self):
        pass


class _FakeOpenAIService:
    def event_handler(self, event_name):
        return lambda callback: callback

    def set_request_follow_up_event_handlers(self, **callbacks):
        self.request_follow_up_callbacks = callbacks

    async def clear_input_audio_buffer_authoritatively(self, generation):
        self.authoritative_input_clear_generations = getattr(
            self,
            "authoritative_input_clear_generations",
            [],
        )
        self.authoritative_input_clear_generations.append(generation)


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


class _FakeDeviceWebSocket:
    def __init__(self, send=None):
        self.send = send or AsyncMock()
        self.close = AsyncMock()


class PipelineLifecycleTests(unittest.IsolatedAsyncioTestCase):
    TEST_SESSION_NONCE = 123456789

    def _admit(self, handler, websocket, nonce=None):
        session_nonce = nonce or self.TEST_SESSION_NONCE
        handler._websockets = {websocket}
        handler._active_session_nonce = session_nonce
        handler._clear_device_input = AsyncMock()
        return session_nonce

    async def _reserve(self, handler, tool_call_id="request-call"):
        if (
            handler._device_wake_generation == 0
            and len(handler._websockets) == 1
            and handler._active_session_nonce is not None
        ):
            handler.note_device_wake()
        return await handler.reserve_request_follow_up(tool_call_id)

    def _activate(self, handler, tool_call_id="request-call"):
        return handler.activate_request_follow_up(tool_call_id)

    def _qualify_response(
        self,
        handler,
        response_id="question-response",
        tool_call_id="request-call",
    ):
        self.assertTrue(
            handler.arm_request_follow_up_continuation({tool_call_id})
        )
        handler.bind_request_follow_up_response(response_id)
        handler.note_request_follow_up_response_audio(response_id)
        handler.note_request_follow_up_playback_started()
        handler.note_request_follow_up_response_done(response_id, "completed")

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

    def _prepare_decision_service(self):
        service = main.SafeRealtimeLLMService(
            max_context_turns=12,
            authorized_tool_names=(main.END_CONVERSATION_TOOL_NAME,),
        )
        service._context = _LLMContext()
        service._current_audio_response = None
        service.stop_ttfb_metrics = AsyncMock()
        pushed = []
        service.push_frame = AsyncMock(side_effect=lambda frame, *_args: pushed.append(frame))
        service._assistant_output_frame_created = Mock(return_value=True)
        service.set_silent_close_runtime_authorizer(lambda: True)
        service._conversation_window.begin_user_turn(
            self._message("answer-user", "user")
        )
        service._conversation_window.attach_transcript(
            "answer-user",
            "unrelated answer",
        )
        service._conversation_window.activate("answer-user")
        service._confirmed_follow_up_answer_identity = ("answer-user", 4)
        service._active_response_id = "decision-response"
        service._output_response_generation = 1
        service._active_output_response_context = ("decision-response", 1)
        service._begin_decision_output_hold("decision-response", 1)
        return service, pushed

    @staticmethod
    def _audio_delta(response_id="decision-response", audio=b"held-audio"):
        return types.SimpleNamespace(
            response_id=response_id,
            item_id="assistant-audio",
            content_index=0,
            delta=base64.b64encode(audio).decode("ascii"),
        )

    @staticmethod
    def _response_done(output, status="completed"):
        return types.SimpleNamespace(
            response=types.SimpleNamespace(
                id="decision-response",
                status=status,
                output=output,
            )
        )

    @staticmethod
    def _text_delta(event_type, text):
        return types.SimpleNamespace(
            response_id="decision-response",
            delta=text,
            type=event_type,
        )

    @staticmethod
    def _graceful_ack(handler, payload, stage, accepted):
        return {
            "type": "suppress_followup_ack",
            "token": payload["token"],
            "session_nonce": handler._active_session_nonce,
            "wake_generation": handler._device_wake_generation,
            "stage": stage,
            "accepted": accepted,
        }

    async def _ack_requested_follow_up(
        self,
        handler,
        websocket,
        *,
        accepted=True,
    ):
        reservation = cast(Any, handler._request_follow_up_reservation)
        payload = None
        while payload is None:
            for call in reversed(websocket.send.await_args_list):
                candidate = json.loads(call.args[0])
                if (
                    candidate.get("type") == "request_follow_up"
                    and candidate.get("token") == reservation.token
                ):
                    payload = candidate
                    break
            if payload is None:
                await asyncio.sleep(0)
        handler._handle_request_follow_up_ack(
            {
                "type": "request_follow_up_ack",
                "token": cast(dict, payload)["token"],
                "session_nonce": cast(dict, payload)["session_nonce"],
                "accepted": accepted,
            }
        )
        return cast(dict, payload)

    async def _ack_cancel_requested_follow_up(
        self,
        handler,
        websocket,
        *,
        accepted=True,
        cleared=True,
    ):
        while True:
            for call in websocket.send.await_args_list:
                payload = json.loads(call.args[0])
                if payload.get("type") == "cancel_request_follow_up":
                    handler._handle_cancel_request_follow_up_ack(
                        {
                            "type": "cancel_request_follow_up_ack",
                            "token": payload["token"],
                            "session_nonce": payload["session_nonce"],
                            "accepted": accepted,
                            "cleared": cleared,
                        }
                    )
                    return payload
            await asyncio.sleep(0)

    async def _ready_and_commit_requested_follow_up(
        self,
        handler,
        websocket,
        *,
        accepted=True,
    ):
        reservation = cast(Any, handler._request_follow_up_reservation)
        ready_nonce = 987654321 - len(handler._seen_ready_nonces)
        while ready_nonce in {reservation.token, reservation.session_nonce}:
            ready_nonce -= 1
        await handler._handle_device_control_message(
            {
                "type": "follow_up_ready",
                "token": reservation.token,
                "session_nonce": reservation.session_nonce,
                "ready_nonce": ready_nonce,
            },
            websocket,
        )
        commit = None
        for _ in range(1000):
            for call in reversed(websocket.send.await_args_list):
                candidate = json.loads(call.args[0])
                if (
                    candidate.get("type") == "commit_follow_up"
                    and candidate.get("token") == reservation.token
                ):
                    commit = candidate
                    break
            if commit is not None:
                break
            await asyncio.sleep(0)
        self.assertIsNotNone(commit)
        await handler._handle_device_control_message(
            {
                **cast(dict, commit),
                "type": "commit_follow_up_ack",
                "accepted": accepted,
            },
            websocket,
        )
        await handler._await_request_follow_up_settlements()
        return cast(dict, commit)

    async def _open_requested_follow_up(self, handler, websocket):
        ack_task = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        request = await ack_task
        commit = await self._ready_and_commit_requested_follow_up(
            handler,
            websocket,
        )
        return request, commit

    async def _open_and_confirm_answer(
        self,
        handler,
        websocket,
        tool_call_id="request-call",
    ):
        self.assertEqual(
            await self._reserve(handler, tool_call_id),
            websocket_handler.FollowUpReservationOutcome.RESERVED,
        )
        self._activate(handler, tool_call_id)
        self._qualify_response(
            handler,
            f"{tool_call_id}-response",
            tool_call_id,
        )
        await self._open_requested_follow_up(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        self.assertTrue(handler.bind_request_follow_up_answer("answer-item", 1))
        self.assertTrue(
            handler.confirm_request_follow_up_answer(
                "answer-item",
                1,
                "relevant answer",
            )
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

    async def test_tool_continuation_drains_response_a_before_follow_up_and_response_b(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._api_session_ready = True
        service._continuation_result_call_ids.add("call-1")
        service._tool_call_output_contexts["call-1"] = ("response-a", 4)
        order = []

        async def drain(response_id, response_generation):
            self.assertEqual((response_id, response_generation), ("response-a", 4))
            order.append("drain-a")
            return True

        def arm(call_ids):
            self.assertEqual(call_ids, {"call-1"})
            order.append("arm-follow-up")
            return True

        async def send(event):
            self.assertEqual(event.type, "response.create")
            order.append("create-b")

        service.set_assistant_output_event_handlers(
            on_before_tool_continuation=drain,
        )
        service.set_request_follow_up_event_handlers(
            on_continuation_arm=arm,
        )
        service.send_client_event = send

        await service._run_tool_continuation(service._session_generation)

        self.assertEqual(order, ["drain-a", "arm-follow-up", "create-b"])
        self.assertNotIn("call-1", service._tool_call_output_contexts)

    async def test_failed_audio_drain_never_creates_response_b_or_reexecutes_tool(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        executions = 0

        async def tool_handler(params):
            nonlocal executions
            executions += 1
            await params.result_callback({"status": "done"})

        service.register_function("one_shot_tool", tool_handler)
        _, wrapped_handler = service._registered_function
        params = types.SimpleNamespace(
            tool_call_id="call-once",
            arguments={},
            result_callback=AsyncMock(),
        )
        service._tool_call_generations[params.tool_call_id] = 0
        await wrapped_handler(params)
        self.assertEqual(executions, 1)

        service._pending_tool_result_ids.clear()
        service._pending_tool_results_drained.set()
        service._continuation_result_call_ids.add(params.tool_call_id)
        service._tool_call_output_contexts[params.tool_call_id] = (
            "response-a",
            4,
        )
        service._api_session_ready = True
        service.send_client_event = AsyncMock()
        service.push_error = AsyncMock()
        service.set_assistant_output_event_handlers(
            on_before_tool_continuation=AsyncMock(return_value=False),
        )

        await service._run_tool_continuation(service._session_generation)

        self.assertEqual(executions, 1)
        service.send_client_event.assert_not_awaited()
        service.push_error.assert_awaited_once()
        self.assertTrue(service._recovery_active)

    async def test_ordinary_non_tool_response_never_runs_audio_drain_barrier(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        drain = AsyncMock(return_value=True)
        service.set_assistant_output_event_handlers(
            on_before_tool_continuation=drain,
        )
        service._active_response_id = "ordinary-response"
        service._active_output_response_context = ("ordinary-response", 2)
        service._response_interrupt_generations["ordinary-response"] = None
        service.push_frame = AsyncMock()

        await service._handle_evt_response_done(
            types.SimpleNamespace(
                response=types.SimpleNamespace(
                    id="ordinary-response",
                    status="completed",
                    output=[
                        types.SimpleNamespace(
                            model_dump=lambda exclude_none: self._message(
                                "assistant-ordinary",
                                "assistant",
                                "done",
                            )
                        )
                    ],
                    usage=types.SimpleNamespace(),
                )
            )
        )

        drain.assert_not_awaited()
        self.assertIsNone(service._continuation_task)

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
        answer_confirmed = Mock()
        service = main.SafeRealtimeLLMService(
            max_context_turns=12,
            request_follow_up_answer_confirmed=answer_confirmed,
        )
        with patch.object(
            _OpenAIRealtimeLLMService,
            "handle_evt_input_audio_transcription_completed",
            new=AsyncMock(),
        ) as parent_transcript:
            await service.handle_evt_input_audio_transcription_completed(
                types.SimpleNamespace(item_id="old-user", transcript="private words")
            )

        parent_transcript.assert_not_awaited()
        answer_confirmed.assert_not_called()

    async def test_blank_managed_transcript_fails_fresh(self):
        answer_confirmed = Mock()
        service = main.SafeRealtimeLLMService(
            max_context_turns=12,
            request_follow_up_answer_confirmed=answer_confirmed,
        )
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
        answer_confirmed.assert_not_called()

    async def test_managed_transcript_writes_one_canonical_local_user(self):
        answer_confirmed = Mock(return_value=True)
        service = main.SafeRealtimeLLMService(
            max_context_turns=12,
            request_follow_up_answer_confirmed=answer_confirmed,
        )
        service._context = _LLMContext()
        service._conversation_window.begin_user_turn(
            self._message("user-1", "user")
        )
        service._conversation_window.activate("user-1")
        service._transcript_ready_events["user-1"] = asyncio.Event()
        service._follow_up_answer_item_sequences["user-1"] = 7
        service._seen_input_speech_items["user-1"] = (100, 7)
        with patch.object(
            _OpenAIRealtimeLLMService,
            "handle_evt_input_audio_transcription_completed",
            new=AsyncMock(),
        ) as parent_transcript:
            await service.handle_evt_input_audio_transcription_completed(
                types.SimpleNamespace(item_id="user-1", transcript="hello")
            )

        parent_transcript.assert_not_awaited()
        answer_confirmed.assert_called_once_with("user-1", 7, "hello")
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

    async def test_end_conversation_result_forbids_model_continuation(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._finalize_silent_control_result = AsyncMock()
        captured_properties = None

        async def result_callback(_result, *, properties=None):
            nonlocal captured_properties
            captured_properties = properties
            await cast(Any, properties).on_context_updated()

        async def end_handler(params):
            await params.result_callback(
                {"status": "closed_silently"},
                properties=main.SilentCloseResultProperties(),
            )

        service.register_function(main.END_CONVERSATION_TOOL_NAME, end_handler)
        _, wrapped_handler = service._registered_function
        params = types.SimpleNamespace(
            tool_call_id="silent-result-call",
            arguments={},
            result_callback=result_callback,
        )
        service._tool_call_generations[params.tool_call_id] = 0

        await wrapped_handler(params)

        self.assertIsInstance(
            captured_properties,
            main.SilentCloseResultProperties,
        )
        self.assertFalse(cast(Any, captured_properties).run_llm)
        service._finalize_silent_control_result.assert_awaited_once_with(
            params.tool_call_id,
            0,
        )
        self.assertIsNone(service._continuation_task)
        service._pending_tool_result_ids.clear()
        service._pending_tool_results_drained.set()

    async def test_silent_control_result_finishes_replayable_turn_without_response(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service._context = _LLMContext(
            [
                {
                    "role": "tool",
                    "tool_call_id": "silent-call",
                    "content": '{"status":"closed_silently"}',
                }
            ]
        )
        service._conversation_window.begin_user_turn(
            self._message("silent-user", "user")
        )
        service._conversation_window.attach_transcript(
            "silent-user",
            "purple elephant",
        )
        service._conversation_window.activate("silent-user")
        service._conversation_window.observe_item(
            {
                "id": "silent-call-item",
                "type": "function_call",
                "call_id": "silent-call",
                "name": main.END_CONVERSATION_TOOL_NAME,
                "arguments": "{}",
            }
        )
        service._pending_tool_result_ids.add("silent-call")
        service._pending_tool_results_drained.clear()
        service._turn_terminal.clear()
        service.push_error = AsyncMock()
        service._create_response = AsyncMock()

        async def send_result(call_id, content):
            self.assertEqual(call_id, "silent-call")
            self.assertIn("closed_silently", content)
            service._conversation_window.observe_item(
                {
                    "id": "silent-result-item",
                    "type": "function_call_output",
                    "call_id": call_id,
                    "output": content,
                }
            )
            service._silent_tool_output_events[call_id].set()

        service._send_tool_result = send_result

        await service._finalize_silent_control_result("silent-call", 0)

        self.assertTrue(service._conversation_window.replay_snapshot()[0].replayable)
        self.assertEqual(service._pending_tool_result_ids, set())
        self.assertTrue(service._pending_tool_results_drained.is_set())
        self.assertTrue(service._turn_terminal.is_set())
        service._create_response.assert_not_awaited()
        service.push_error.assert_not_awaited()

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

    async def test_stop_during_pre_handler_await_finalizes_without_side_effect(self):
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
        result_callback.assert_awaited_once()
        self.assertIn("error", cast(Any, result_callback.await_args).args[0])
        self.assertNotIn("call-guarded", service._tool_result_callbacks)
        self.assertNotIn("call-guarded", service._interrupted_tool_result_ids)

    async def test_direct_cancellation_during_pre_handler_releases_ownership(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        handler = AsyncMock()
        service.register_function("cancelled_guarded_tool", handler)
        _, wrapped_handler = service._registered_function
        result_callback = AsyncMock()
        params = types.SimpleNamespace(
            tool_call_id="call-direct-cancel",
            arguments={},
            result_callback=result_callback,
        )
        service._tool_call_generations["call-direct-cancel"] = 0
        callback_started = asyncio.Event()

        async def before_tool():
            callback_started.set()
            await asyncio.Event().wait()

        original_callback = main.NON_CLOSE_TOOL_CALLBACK
        main.NON_CLOSE_TOOL_CALLBACK = before_tool
        try:
            tool_task = asyncio.create_task(wrapped_handler(params))
            await callback_started.wait()
            tool_task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await tool_task
        finally:
            main.NON_CLOSE_TOOL_CALLBACK = original_callback

        handler.assert_not_awaited()
        result_callback.assert_awaited_once()
        self.assertNotIn("call-direct-cancel", service._tool_result_callbacks)
        self.assertNotIn("call-direct-cancel", service._tool_call_generations)

    async def test_cancellation_inside_result_delivery_settles_exactly_once(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        delivery_started = asyncio.Event()
        callback_calls = 0
        downstream_calls = 0
        upstream_calls = 0

        async def result_callback(_result, *, properties=None):
            nonlocal callback_calls, downstream_calls, upstream_calls
            self.assertIsNone(properties)
            callback_calls += 1
            downstream_calls += 1
            delivery_started.set()
            await asyncio.Event().wait()
            upstream_calls += 1

        async def tool_handler(params):
            await params.result_callback({"status": "done"})

        service.register_function("cancel_during_result", tool_handler)
        _, wrapped_handler = service._registered_function
        params = types.SimpleNamespace(
            tool_call_id="call-result-cancel",
            arguments={},
            result_callback=result_callback,
        )
        service._tool_call_generations[params.tool_call_id] = 0

        tool_task = asyncio.create_task(wrapped_handler(params))
        await delivery_started.wait()
        tool_task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await tool_task

        await params.result_callback({"status": "duplicate"})
        self.assertEqual(callback_calls, 1)
        self.assertEqual(downstream_calls, 1)
        self.assertEqual(upstream_calls, 0)
        self.assertNotIn(params.tool_call_id, service._pending_tool_result_ids)
        self.assertTrue(service._pending_tool_results_drained.is_set())
        self.assertNotIn(params.tool_call_id, service._tool_result_callbacks)
        self.assertIn(params.tool_call_id, service._completed_tool_calls)
        self.assertIn(params.tool_call_id, service._retired_aggregator_call_ids)

    async def test_result_delivery_error_is_not_retried_and_signals_recovery(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.push_error = AsyncMock()
        result_callback = AsyncMock(side_effect=RuntimeError("delivery failed"))

        async def tool_handler(params):
            await params.result_callback({"status": "done"})

        service.register_function("failed_result_delivery", tool_handler)
        _, wrapped_handler = service._registered_function
        params = types.SimpleNamespace(
            tool_call_id="call-result-error",
            arguments={},
            result_callback=result_callback,
        )
        service._tool_call_generations[params.tool_call_id] = 0

        with self.assertLogs("app.main", level="ERROR"):
            await wrapped_handler(params)
        await params.result_callback({"status": "duplicate"})

        result_callback.assert_awaited_once()
        self.assertTrue(service._recovery_active)
        service.push_error.assert_awaited_once()
        self.assertNotIn(params.tool_call_id, service._pending_tool_result_ids)
        self.assertTrue(service._pending_tool_results_drained.is_set())
        self.assertIn(params.tool_call_id, service._completed_tool_calls)

    async def test_recovery_during_pre_handler_releases_ownership(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        handler = AsyncMock()
        service.register_function("recovery_guarded_tool", handler)
        _, wrapped_handler = service._registered_function
        result_callback = AsyncMock()
        params = types.SimpleNamespace(
            tool_call_id="call-pre-recovery",
            arguments={},
            result_callback=result_callback,
        )
        service._tool_call_generations["call-pre-recovery"] = 0

        async def before_tool():
            service._recovery_active = True

        original_callback = main.NON_CLOSE_TOOL_CALLBACK
        main.NON_CLOSE_TOOL_CALLBACK = before_tool
        try:
            await wrapped_handler(params)
        finally:
            main.NON_CLOSE_TOOL_CALLBACK = original_callback

        handler.assert_not_awaited()
        result_callback.assert_awaited_once()
        self.assertNotIn("call-pre-recovery", service._tool_result_callbacks)
        self.assertNotIn("call-pre-recovery", service._tool_call_generations)

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

    async def test_input_clear_ack_settles_generation_fifo(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.note_interrupt_input_clear(11)
        service.note_interrupt_input_clear(12)

        service.handle_interrupt_input_cleared()
        self.assertEqual(service._interrupt_input_clear_generation, 12)

        service.handle_interrupt_input_cleared()
        self.assertIsNone(service._interrupt_input_clear_generation)

    async def test_authoritative_input_clear_waits_for_openai_receipt(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        sent = asyncio.Event()

        async def send_client_event(event):
            sent.set()

        service.send_client_event = send_client_event
        clear = asyncio.create_task(
            service.clear_input_audio_buffer_authoritatively(21)
        )
        await sent.wait()

        self.assertFalse(clear.done())
        self.assertEqual(service._interrupt_input_clear_generation, 21)
        self.assertTrue(service._post_interrupt_response_quarantine)
        service.handle_interrupt_input_cleared()
        await clear
        self.assertIsNone(service._interrupt_input_clear_generation)
        self.assertFalse(service._interrupt_clear_requests)

    async def test_empty_openai_buffer_authoritatively_settles_exact_clear(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        sent_event = None

        async def send_client_event(event):
            nonlocal sent_event
            sent_event = event

        service.send_client_event = send_client_event
        clear = asyncio.create_task(
            service.clear_input_audio_buffer_authoritatively(22)
        )
        while sent_event is None:
            await asyncio.sleep(0)

        self.assertTrue(
            await service._maybe_handle_evt_retrieve_conversation_item_error(
                types.SimpleNamespace(
                    error=types.SimpleNamespace(
                        code="input_audio_buffer_clear_empty",
                        event_id=sent_event.event_id,
                    )
                )
            )
        )
        await clear
        self.assertIsNone(service._interrupt_input_clear_generation)
        self.assertFalse(service._interrupt_clear_requests)

    async def test_failed_authoritative_input_clear_enters_recovery(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        service.send_client_event = AsyncMock(
            side_effect=RuntimeError("clear send failed")
        )
        service.push_error = AsyncMock()

        with self.assertRaisesRegex(RuntimeError, "did not settle"):
            await service.clear_input_audio_buffer_authoritatively(24)

        self.assertTrue(service._recovery_active)
        error_message = service.push_error.await_args.kwargs["error_msg"]
        self.assertIn("context compaction failed", error_message)
        self.assertIn("OpenAI input clear did not settle", error_message)

    async def test_response_racing_client_revoke_clear_gets_no_output_grant(self):
        service = main.SafeRealtimeLLMService(max_context_turns=12)
        sent_events = []
        output_created = Mock()
        service.set_assistant_output_event_handlers(
            on_response_created=output_created,
        )

        async def send_client_event(event):
            sent_events.append(event)

        service.send_client_event = send_client_event
        clear = asyncio.create_task(
            service.clear_input_audio_buffer_authoritatively(23)
        )
        while not sent_events:
            await asyncio.sleep(0)

        async def messages():
            yield types.SimpleNamespace(
                type="response.created",
                response=types.SimpleNamespace(id="racing-response"),
            )

        service._websocket = messages()
        service.mark_interrupted_response = AsyncMock()
        service.push_error = AsyncMock()
        await service._receive_task_handler()

        output_created.assert_not_called()
        self.assertTrue(
            any(event.type == "response.cancel" for event in sent_events)
        )
        service.handle_interrupt_input_cleared()
        await clear

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
        service = main.SafeRealtimeLLMService(
            max_context_turns=12,
            authorized_tool_names={"test_tool"},
        )
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

    async def test_registered_or_wildcard_unexposed_tool_fails_before_dispatch(self):
        for use_wildcard in (False, True):
            with self.subTest(use_wildcard=use_wildcard):
                service = main.SafeRealtimeLLMService(
                    max_context_turns=12,
                    authorized_tool_names={"allowed_tool"},
                )
                service.push_error = AsyncMock()
                handler = AsyncMock()
                if use_wildcard:
                    service._functions[None] = handler
                else:
                    service.register_function("hallucinated_tool", handler)
                service._pending_function_calls["call-unexposed"] = (
                    types.SimpleNamespace(name="hallucinated_tool")
                )

                await service._handle_evt_function_call_arguments_done(
                    types.SimpleNamespace(
                        call_id="call-unexposed",
                        arguments="{}",
                    )
                )

                self.assertTrue(service._recovery_active)
                self.assertEqual(service._scheduled_tool_call_ids, set())
                self.assertNotIn("call-unexposed", service._pending_function_calls)
                handler.assert_not_awaited()
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

    async def test_ordinary_reply_idle_emits_no_follow_up_control(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)

        await handler._before_reply_idle()

        websocket.send.assert_not_awaited()

    async def test_three_serial_follow_ups_rearm_only_after_each_answer(self):
        websocket = _FakeDeviceWebSocket()
        media_check = AsyncMock(
            return_value=websocket_handler.MediaActivity.CLEAR
        )
        handler = websocket_handler.WebSocketHandler(
            follow_up_ms=0,
            media_activity_check=media_check,
        )
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake())

        tokens = []
        for number in range(1, 4):
            tool_call_id = f"request-{number}"
            self.assertEqual(
                await self._reserve(handler, tool_call_id),
                websocket_handler.FollowUpReservationOutcome.RESERVED,
            )
            self._activate(handler, tool_call_id)
            self._qualify_response(
                handler,
                f"question-{number}",
                tool_call_id,
            )
            request, _commit = await self._open_requested_follow_up(
                handler,
                websocket,
            )
            tokens.append(request["token"])
            handler.note_request_follow_up_turn_boundary()
            self.assertTrue(handler._request_follow_up_budget_spent)
            item_id = f"answer-item-{number}"
            self.assertTrue(
                handler.bind_request_follow_up_answer(item_id, number)
            )
            self.assertTrue(
                handler.confirm_request_follow_up_answer(
                    item_id,
                    number,
                    f"answer {number}",
                )
            )
            self.assertFalse(handler._request_follow_up_budget_spent)

        self.assertEqual(len(set(tokens)), 3)
        self.assertEqual(
            await self._reserve(handler, "request-4"),
            websocket_handler.FollowUpReservationOutcome.RESERVED,
        )
        self.assertEqual(media_check.await_count, 7)
        handler.cancel_request_follow_up()

    async def test_successor_reservation_keeps_previous_open_token_until_replying(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(
            follow_up_ms=0,
            media_activity_check=AsyncMock(
                return_value=websocket_handler.MediaActivity.CLEAR
            ),
        )
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake())

        previous_token = None
        for number in range(1, 4):
            tool_call_id = f"request-{number}"
            stale_context = (
                handler.capture_phase_authorization_context()
                if previous_token is not None
                else None
            )
            self.assertEqual(
                await self._reserve(handler, tool_call_id),
                websocket_handler.FollowUpReservationOutcome.RESERVED,
            )
            reservation = cast(Any, handler._request_follow_up_reservation)
            self.assertNotEqual(reservation.token, previous_token)

            if previous_token is not None:
                self.assertIsNotNone(stale_context)
                stale_context = cast(Any, stale_context)
                self.assertEqual(stale_context.follow_up_token, previous_token)
                self.assertLess(stale_context.follow_up_epoch, reservation.epoch)
                sent_before_stale = websocket.send.await_count
                self.assertFalse(
                    await handler.broadcast_phase("replying", stale_context)
                )
                self.assertEqual(websocket.send.await_count, sent_before_stale)

            self._activate(handler, tool_call_id)
            response_id = f"question-{number}"
            self.assertTrue(
                handler.arm_request_follow_up_continuation({tool_call_id})
            )
            handler.bind_request_follow_up_response(response_id)
            handler.note_request_follow_up_response_audio(response_id)
            handler.note_assistant_playback_started()

            if previous_token is not None:
                replying_context = handler.capture_phase_authorization_context()
                self.assertIsNotNone(replying_context)
                replying_context = cast(Any, replying_context)
                self.assertEqual(replying_context.follow_up_token, previous_token)
                self.assertEqual(replying_context.follow_up_epoch, reservation.epoch)
                self.assertTrue(
                    await handler.broadcast_phase("replying", replying_context)
                )
                replying = json.loads(websocket.send.await_args_list[-1].args[0])
                self.assertEqual(replying["type"], "phase")
                self.assertEqual(replying["value"], "replying")
                self.assertEqual(replying["token"], previous_token)
                self.assertNotEqual(replying["token"], reservation.token)
                self.assertIsNone(handler._open_follow_up_phase_grant)

            handler.note_request_follow_up_response_done(response_id, "completed")
            request, _commit = await self._open_requested_follow_up(
                handler,
                websocket,
            )
            self.assertEqual(request["token"], reservation.token)
            handler.note_request_follow_up_turn_boundary()
            item_id = f"answer-{number}"
            self.assertTrue(
                handler.bind_request_follow_up_answer(item_id, number)
            )
            self.assertTrue(
                handler.confirm_request_follow_up_answer(
                    item_id,
                    number,
                    f"answer {number}",
                )
            )
            previous_token = reservation.token

        handler.invalidate_request_follow_up_turn(send_cancel=False)

    async def test_media_denial_preserves_previous_open_token_for_replying(self):
        websocket = _FakeDeviceWebSocket()
        media_check = AsyncMock(return_value=websocket_handler.MediaActivity.CLEAR)
        handler = websocket_handler.WebSocketHandler(
            follow_up_ms=0,
            media_activity_check=media_check,
        )
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake())

        await self._open_and_confirm_answer(handler, websocket)
        phase_grant = cast(Any, handler._open_follow_up_phase_grant)
        self.assertIsNotNone(phase_grant)
        media_check.return_value = websocket_handler.MediaActivity.ACTIVE

        self.assertEqual(
            await self._reserve(handler, "denied-successor"),
            websocket_handler.FollowUpReservationOutcome.REQUIRES_WAKE,
        )
        self.assertIsNone(handler._request_follow_up_reservation)
        self.assertIsNone(handler._request_follow_up_answer_grant)
        self.assertIs(handler._open_follow_up_phase_grant, phase_grant)

        handler.note_assistant_playback_started()
        context = handler.capture_phase_authorization_context()
        self.assertIsNotNone(context)
        context = cast(Any, context)
        self.assertEqual(context.follow_up_token, phase_grant.token)
        self.assertTrue(await handler.broadcast_phase("replying", context))
        replying = json.loads(websocket.send.await_args_list[-1].args[0])
        self.assertEqual(replying["token"], phase_grant.token)
        self.assertIsNone(handler._open_follow_up_phase_grant)

    async def test_terminal_idle_consumes_open_phase_grant_without_a_token(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake())
        await self._open_and_confirm_answer(handler, websocket)

        self.assertIsNotNone(handler._open_follow_up_phase_grant)
        terminal = handler.capture_terminal_idle_phase_authorization_context()
        self.assertIsNotNone(terminal)
        terminal = cast(Any, terminal)
        self.assertTrue(terminal.terminal_idle)
        self.assertIsNone(terminal.follow_up_token)
        self.assertTrue(await handler.broadcast_phase("idle"))
        payload = json.loads(websocket.send.await_args_list[-1].args[0])
        self.assertEqual(
            payload,
            {
                "type": "phase",
                "value": "idle",
                "session_nonce": handler._active_session_nonce,
                "wake_generation": handler._device_wake_generation,
            },
        )
        self.assertIsNone(handler._open_follow_up_phase_grant)

    async def test_stale_terminal_idle_cannot_cross_successor_reservation_epoch(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake())
        await self._open_and_confirm_answer(handler, websocket)

        terminal = handler.capture_terminal_idle_phase_authorization_context()
        self.assertIsNotNone(terminal)
        self.assertEqual(
            await self._reserve(handler, "successor"),
            websocket_handler.FollowUpReservationOutcome.RESERVED,
        )
        sent_before = websocket.send.await_count

        self.assertFalse(await handler.broadcast_phase("idle", terminal))
        self.assertEqual(websocket.send.await_count, sent_before)
        handler.invalidate_request_follow_up_turn(send_cancel=False)

    async def test_recovery_revokes_open_phase_grant_before_idle_delivery(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake())
        await self._open_and_confirm_answer(handler, websocket)
        self.assertIsNotNone(handler._open_follow_up_phase_grant)

        handler._on_connection_recovery_started()

        self.assertIsNone(handler._open_follow_up_phase_grant)
        websocket.send.side_effect = RuntimeError("idle delivery failed")
        self.assertFalse(await handler.broadcast_phase("idle"))
        self.assertIsNone(handler._open_follow_up_phase_grant)

    async def test_client_revoke_clears_open_phase_grant(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        session_nonce = self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake())
        await self._open_and_confirm_answer(handler, websocket)
        self.assertIsNotNone(handler._open_follow_up_phase_grant)

        await handler._handle_device_control_message(
            {
                "type": "client_revoke",
                "session_nonce": session_nonce,
                "wake_generation": handler._device_wake_generation,
                "reason": "malformed_phase",
            },
            websocket,
        )

        self.assertIsNone(handler._open_follow_up_phase_grant)
        self.assertIsNone(handler._request_follow_up_answer_grant)
        context = handler.capture_phase_authorization_context()
        self.assertIsNotNone(context)
        self.assertIsNone(cast(Any, context).follow_up_token)

    async def test_client_revoke_clears_phase_grant_before_output_settlement(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        session_nonce = self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake())
        await self._open_and_confirm_answer(handler, websocket)
        self.assertIsNotNone(handler._open_follow_up_phase_grant)
        settlement_started = asyncio.Event()
        release_settlement = asyncio.Event()

        async def blocked_settlement(_grant):
            settlement_started.set()
            await release_settlement.wait()

        handler._retire_assistant_output_grant = Mock(return_value=object())
        handler._settle_retired_assistant_output = AsyncMock(
            side_effect=blocked_settlement
        )

        revoke = asyncio.create_task(
            handler._handle_device_control_message(
                {
                    "type": "client_revoke",
                    "session_nonce": session_nonce,
                    "wake_generation": handler._device_wake_generation,
                    "reason": "malformed_phase",
                },
                websocket,
            )
        )
        await settlement_started.wait()

        self.assertIsNone(handler._open_follow_up_phase_grant)
        self.assertIsNone(handler._request_follow_up_answer_grant)
        self.assertTrue(handler._request_follow_up_budget_spent)
        self.assertFalse(handler.silent_close_is_allowed())
        with self.assertRaises(RuntimeError):
            await handler.reserve_request_follow_up("racing-tool")

        release_settlement.set()
        await revoke

    async def test_bound_socket_retirement_revokes_locally_before_failed_settlement(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        session_nonce = self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake())
        await self._open_and_confirm_answer(handler, websocket)
        handler.transport = types.SimpleNamespace(
            retire_output_audio_generation=Mock(),
            settle_output_audio_generation=AsyncMock(
                side_effect=RuntimeError("settlement failed")
            ),
        )
        handler._assistant_output_grant = websocket_handler._AssistantOutputGrant(
            websocket=websocket,
            session_nonce=session_nonce,
            wake_generation=handler._device_wake_generation,
            response_id="retired-response",
            response_generation=1,
            authority_epoch=handler._assistant_output_authority_epoch,
        )

        await handler._retire_bound_socket(websocket, session_nonce)

        self.assertNotIn(websocket, handler._websockets)
        self.assertIsNone(handler._active_session_nonce)
        self.assertIsNone(handler._request_follow_up_answer_grant)
        self.assertIsNone(handler._open_follow_up_phase_grant)
        self.assertFalse(handler._binary_audio_is_admitted(websocket))
        self.assertGreaterEqual(websocket.close.await_count, 1)

    async def test_client_revoke_closes_socket_when_output_settlement_fails(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        session_nonce = self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake())
        await self._open_and_confirm_answer(handler, websocket)
        handler.transport = types.SimpleNamespace(
            retire_output_audio_generation=Mock(),
            settle_output_audio_generation=AsyncMock(
                side_effect=RuntimeError("settlement failed")
            ),
        )
        handler._assistant_output_grant = websocket_handler._AssistantOutputGrant(
            websocket=websocket,
            session_nonce=session_nonce,
            wake_generation=handler._device_wake_generation,
            response_id="revoked-response",
            response_generation=1,
            authority_epoch=handler._assistant_output_authority_epoch,
        )

        await handler._handle_device_control_message(
            {
                "type": "client_revoke",
                "session_nonce": session_nonce,
                "wake_generation": handler._device_wake_generation,
                "reason": "malformed_phase",
            },
            websocket,
        )

        self.assertNotIn(websocket, handler._websockets)
        self.assertIsNone(handler._active_session_nonce)
        self.assertIsNone(handler._open_follow_up_phase_grant)
        self.assertFalse(handler._binary_audio_is_admitted(websocket))
        websocket.close.assert_awaited_once()

    async def test_cleanup_clears_open_phase_grant(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake())
        await self._open_and_confirm_answer(handler, websocket)
        self.assertIsNotNone(handler._open_follow_up_phase_grant)

        await handler.cleanup()

        self.assertIsNone(handler._open_follow_up_phase_grant)

    async def test_cleanup_revokes_locally_before_failed_output_settlement(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake())
        await self._open_and_confirm_answer(handler, websocket)
        session_nonce = cast(int, handler._active_session_nonce)
        handler.transport = types.SimpleNamespace(
            retire_output_audio_generation=Mock(),
            settle_output_audio_generation=AsyncMock(
                side_effect=RuntimeError("settlement failed")
            ),
        )
        handler._assistant_output_grant = websocket_handler._AssistantOutputGrant(
            websocket=websocket,
            session_nonce=session_nonce,
            wake_generation=handler._device_wake_generation,
            response_id="cleanup-response",
            response_generation=1,
            authority_epoch=handler._assistant_output_authority_epoch,
        )

        await handler.cleanup()

        self.assertIsNone(handler._request_follow_up_answer_grant)
        self.assertIsNone(handler._open_follow_up_phase_grant)
        self.assertFalse(handler._binary_audio_is_admitted(websocket))
        self.assertGreaterEqual(websocket.close.await_count, 1)

    async def test_follow_up_answer_confirmation_requires_exact_fresh_item_identity(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        initial_context = handler.capture_phase_authorization_context()
        await self._reserve(handler)
        handler._device_audio_generation = 91
        sent_before_stale_initial = websocket.send.await_count
        await handler.broadcast_phase("thinking", initial_context)
        self.assertEqual(websocket.send.await_count, sent_before_stale_initial)
        self.assertEqual(handler._device_audio_generation, 91)
        self._activate(handler)
        self._qualify_response(handler, "identity-question")
        await self._open_requested_follow_up(handler, websocket)
        handler.note_request_follow_up_turn_boundary()

        self.assertTrue(handler.bind_request_follow_up_answer("fresh-item", 8))
        self.assertFalse(
            handler.confirm_request_follow_up_answer(
                "historical-item",
                7,
                "delayed historical transcript",
            )
        )
        self.assertFalse(
            handler.confirm_request_follow_up_answer(
                "fresh-item",
                7,
                "wrong sequence",
            )
        )
        self.assertTrue(
            handler.confirm_request_follow_up_answer(
                "fresh-item",
                8,
                "current answer",
            )
        )
        self.assertFalse(
            handler.confirm_request_follow_up_answer(
                "fresh-item",
                8,
                "duplicate transcript",
            )
        )

    async def test_repeated_open_phases_carry_only_the_current_answer_token(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "token-question")
        request, _commit = await self._open_requested_follow_up(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        answer_context = handler.capture_phase_authorization_context()
        self.assertIsNotNone(answer_context)

        await handler.broadcast_phase("thinking", answer_context)
        phase = json.loads(websocket.send.await_args_list[-1].args[0])
        self.assertEqual(
            phase,
            {
                "type": "phase",
                "value": "thinking",
                "token": request["token"],
                "session_nonce": handler._active_session_nonce,
                "wake_generation": handler._device_wake_generation,
            },
        )
        self.assertTrue(handler.bind_request_follow_up_answer("answer-item", 1))
        self.assertTrue(
            handler.confirm_request_follow_up_answer(
                "answer-item",
                1,
                "answer",
            )
        )
        handler.note_assistant_playback_started()
        replying_context = handler.capture_phase_authorization_context()
        await handler.broadcast_phase("replying", replying_context)
        self.assertEqual(
            json.loads(websocket.send.await_args_list[-1].args[0])["token"],
            request["token"],
        )

        stale_context = replying_context
        await self._reserve(handler, "next-round")
        sent_before_stale = websocket.send.await_count
        await handler.broadcast_phase("thinking", stale_context)
        self.assertEqual(websocket.send.await_count, sent_before_stale)
        handler.cancel_request_follow_up(send_cancel=False)

    async def test_physical_wake_deadline_blocks_audio_output_and_new_round(self):
        websocket = _FakeDeviceWebSocket()
        media_check = AsyncMock(return_value=websocket_handler.MediaActivity.CLEAR)
        handler = websocket_handler.WebSocketHandler(
            follow_up_ms=0,
            media_activity_check=media_check,
        )
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake())
        handler._physical_wake_deadline = websocket_handler.time.monotonic() - 0.001

        self.assertFalse(handler._binary_audio_is_admitted(websocket))
        self.assertFalse(
            await handler.bind_assistant_output_response("late-response", 1)
        )
        self.assertEqual(
            await handler.reserve_request_follow_up("late-follow-up"),
            websocket_handler.FollowUpReservationOutcome.REQUIRES_WAKE,
        )
        media_check.assert_not_awaited()
        websocket.send.assert_not_awaited()

    async def test_stop_mute_and_recovery_paths_never_rearm_consumed_answer(self):
        for message_type in (
            "interrupt",
            "flush",
            "button_cancel",
            "false_flag",
        ):
            with self.subTest(message_type=message_type):
                websocket = _FakeDeviceWebSocket()
                handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
                session_nonce = self._admit(handler, websocket)
                handler.note_device_wake()
                await self._open_and_confirm_answer(handler, websocket)

                payload = {
                    "type": message_type,
                    "session_nonce": session_nonce,
                    "wake_generation": handler._device_wake_generation,
                }
                if message_type == "interrupt":
                    payload["reason"] = "test_stop"
                handled = await handler._handle_device_control_message(
                    payload,
                    websocket,
                )

                self.assertTrue(handled)
                self.assertTrue(handler._request_follow_up_budget_spent)
                self.assertIsNone(handler._request_follow_up_answer_grant)
                self.assertFalse(
                    handler.confirm_request_follow_up_answer(
                        "late-item",
                        99,
                        "late transcript",
                    )
                )

        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._open_and_confirm_answer(handler, websocket)
        handler._on_connection_recovery_started()
        self.assertTrue(handler._request_follow_up_budget_spent)
        self.assertIsNone(handler._request_follow_up_answer_grant)

    async def test_competing_tool_silent_reply_and_disconnect_revoke_rearm(self):
        for path in ("competing_tool", "silent_reply", "disconnect"):
            with self.subTest(path=path):
                websocket = _FakeDeviceWebSocket()
                handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
                session_nonce = self._admit(handler, websocket)
                handler.note_device_wake()
                await self._open_and_confirm_answer(handler, websocket)

                if path == "competing_tool":
                    await handler.cancel_deferred_conversation_controls()
                elif path == "silent_reply":
                    await handler._before_reply_idle()
                else:
                    await handler._retire_bound_socket(websocket, session_nonce)

                self.assertTrue(handler._request_follow_up_budget_spent)
                self.assertIsNone(handler._request_follow_up_answer_grant)
                self.assertFalse(
                    handler.confirm_request_follow_up_answer(
                        "late-item",
                        99,
                        "late transcript",
                    )
                )

    async def test_media_denial_consumes_rearmed_answer_authority(self):
        websocket = _FakeDeviceWebSocket()
        media_check = AsyncMock(
            side_effect=(
                websocket_handler.MediaActivity.CLEAR,
                websocket_handler.MediaActivity.CLEAR,
                websocket_handler.MediaActivity.ACTIVE,
            )
        )
        handler = websocket_handler.WebSocketHandler(
            follow_up_ms=0,
            media_activity_check=media_check,
        )
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._open_and_confirm_answer(handler, websocket)

        self.assertEqual(
            await self._reserve(handler, "media-denied-round"),
            websocket_handler.FollowUpReservationOutcome.REQUIRES_WAKE,
        )
        self.assertTrue(handler._request_follow_up_budget_spent)
        self.assertIsNone(handler._request_follow_up_answer_grant)
        self.assertFalse(
            handler.confirm_request_follow_up_answer(
                "replayed-item",
                99,
                "replayed answer",
            )
        )

    async def test_reconnect_recovery_and_replay_boundaries_do_not_reset_budget(self):
        first = _FakeDeviceWebSocket()
        replacement = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, first)
        handler.note_device_wake()
        await self._reserve(handler)
        wake_generation = handler._device_wake_generation
        handler.invalidate_request_follow_up_turn(send_cancel=False)

        # Recovery and replay can rebuild OpenAI turns, but neither is a device
        # wake and neither may mint another no-wake budget.
        handler.note_request_follow_up_turn_boundary()
        self.assertTrue(handler._request_follow_up_budget_spent)
        self.assertEqual(handler._device_wake_generation, wake_generation)

        self._admit(handler, replacement, nonce=self.TEST_SESSION_NONCE + 1)
        handler.invalidate_request_follow_up_turn(send_cancel=False)
        handler.note_request_follow_up_turn_boundary()
        self.assertTrue(handler._request_follow_up_budget_spent)
        self.assertEqual(handler._device_wake_generation, wake_generation)
        with self.assertRaisesRegex(RuntimeError, "genuine device wake"):
            await handler.reserve_request_follow_up("stale-replay-call")

        handler.note_device_wake()
        self.assertFalse(handler._request_follow_up_budget_spent)
        self.assertEqual(handler._device_wake_generation, wake_generation + 1)

    async def test_active_or_uncertain_media_suppresses_initial_window(self):
        for media_status in (
            websocket_handler.MediaActivity.ACTIVE,
            websocket_handler.MediaActivity.UNCERTAIN,
        ):
            with self.subTest(media_status=media_status):
                websocket = _FakeDeviceWebSocket()
                handler = websocket_handler.WebSocketHandler(
                    follow_up_ms=0,
                    media_activity_check=AsyncMock(return_value=media_status),
                )
                self._admit(handler, websocket)
                handler.note_device_wake()

                outcome = await self._reserve(handler)
                await handler._before_reply_idle()

                self.assertEqual(
                    outcome,
                    websocket_handler.FollowUpReservationOutcome.REQUIRES_WAKE,
                )
                self.assertIsNone(handler._request_follow_up_reservation)
                self.assertEqual(handler._websockets, {websocket})
                websocket.send.assert_not_awaited()

    async def test_media_is_rechecked_at_firmware_ready_boundary(self):
        websocket = _FakeDeviceWebSocket()
        media_check = AsyncMock(
            side_effect=(
                websocket_handler.MediaActivity.CLEAR,
                websocket_handler.MediaActivity.ACTIVE,
            )
        )
        handler = websocket_handler.WebSocketHandler(
            follow_up_ms=0,
            media_activity_check=media_check,
        )
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "media-race-response")

        prepare_ack = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        await prepare_ack
        cancel_ack = asyncio.create_task(
            self._ack_cancel_requested_follow_up(handler, websocket)
        )
        reservation = cast(Any, handler._request_follow_up_reservation)
        await handler._handle_device_control_message(
            {
                "type": "follow_up_ready",
                "token": reservation.token,
                "session_nonce": reservation.session_nonce,
                "ready_nonce": 987654321,
            },
            websocket,
        )
        await cancel_ack
        await handler._await_request_follow_up_settlements()

        self.assertEqual(media_check.await_count, 2)
        self.assertIsNone(handler._request_follow_up_reservation)
        self.assertEqual(handler._websockets, {websocket})
        self.assertEqual(
            [json.loads(call.args[0])["type"] for call in websocket.send.await_args_list],
            ["request_follow_up", "cancel_request_follow_up"],
        )

    async def test_media_timeout_fails_closed_without_losing_context_socket(self):
        async def media_timeout():
            await asyncio.sleep(10)
            return websocket_handler.MediaActivity.CLEAR

        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(
            follow_up_ms=0,
            media_activity_check=media_timeout,
        )
        handler.FOLLOW_UP_MEDIA_CHECK_TIMEOUT_S = 0.001
        self._admit(handler, websocket)
        handler.note_device_wake()

        with self.assertLogs("app.websocket_handler", level="WARNING"):
            outcome = await self._reserve(handler)

        self.assertEqual(
            outcome,
            websocket_handler.FollowUpReservationOutcome.REQUIRES_WAKE,
        )
        self.assertEqual(handler._websockets, {websocket})
        websocket.close.assert_not_awaited()

    async def test_final_media_timeout_suppresses_the_prepared_window(self):
        checks = 0

        async def media_check():
            nonlocal checks
            checks += 1
            if checks == 1:
                return websocket_handler.MediaActivity.CLEAR
            await asyncio.sleep(10)
            return websocket_handler.MediaActivity.CLEAR

        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(
            follow_up_ms=0,
            media_activity_check=media_check,
        )
        handler.FOLLOW_UP_MEDIA_CHECK_TIMEOUT_S = 0.001
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "final-timeout-response")

        prepare_ack = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        await prepare_ack
        cancel_ack = asyncio.create_task(
            self._ack_cancel_requested_follow_up(handler, websocket)
        )
        reservation = cast(Any, handler._request_follow_up_reservation)
        with self.assertLogs("app.websocket_handler", level="WARNING"):
            await handler._handle_device_control_message(
                {
                    "type": "follow_up_ready",
                    "token": reservation.token,
                    "session_nonce": reservation.session_nonce,
                    "ready_nonce": 987654321,
                },
                websocket,
            )
            await cancel_ack
            await handler._await_request_follow_up_settlements()

        self.assertEqual(checks, 2)
        self.assertIsNone(handler._request_follow_up_reservation)
        self.assertEqual(handler._websockets, {websocket})

    async def test_stop_during_final_media_check_cannot_open_microphone(self):
        final_check_started = asyncio.Event()
        release_final_check = asyncio.Event()
        checks = 0

        async def media_check():
            nonlocal checks
            checks += 1
            if checks == 1:
                return websocket_handler.MediaActivity.CLEAR
            final_check_started.set()
            await release_final_check.wait()
            return websocket_handler.MediaActivity.CLEAR

        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(
            follow_up_ms=0,
            media_activity_check=media_check,
        )
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "stopped-at-final-check")

        prepare_ack = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        await prepare_ack
        reservation = cast(Any, handler._request_follow_up_reservation)
        await handler._handle_device_control_message(
            {
                "type": "follow_up_ready",
                "token": reservation.token,
                "session_nonce": reservation.session_nonce,
                "ready_nonce": 987654321,
            },
            websocket,
        )
        await final_check_started.wait()
        await handler._handle_device_control_message(
            {
                "type": "client_revoke",
                "session_nonce": handler._active_session_nonce,
                "wake_generation": handler._device_wake_generation,
                "reason": "stop",
            },
            websocket,
        )
        release_final_check.set()
        await handler._await_request_follow_up_settlements()

        self.assertEqual(websocket.send.await_count, 1)
        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_control_is_never_sent_before_bound_playback_starts_and_stops(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        handler.arm_request_follow_up_continuation({"request-call"})
        handler.bind_request_follow_up_response("question-response")
        handler.note_request_follow_up_response_audio("question-response")
        handler.note_request_follow_up_response_done(
            "question-response",
            "completed",
        )

        websocket.send.assert_not_awaited()
        await handler._before_reply_idle()
        websocket.send.assert_not_awaited()
        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_requested_follow_up_binds_exact_response_audio_and_ack(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()

        self.assertEqual(
            await self._reserve(handler),
            websocket_handler.FollowUpReservationOutcome.RESERVED,
        )
        # Generic/unbound playback before activation cannot qualify.
        handler.note_request_follow_up_response_audio("response-before-result")
        self.assertTrue(self._activate(handler))
        self._qualify_response(handler)
        control, commit = await self._open_requested_follow_up(handler, websocket)

        self.assertEqual(control["type"], "request_follow_up")
        self.assertGreater(control["token"], 0)
        reservation = cast(Any, handler._request_follow_up_reservation)
        self.assertTrue(reservation.ack_accepted)
        self.assertTrue(reservation.commit_ack_accepted)
        self.assertEqual(
            reservation.stage,
            websocket_handler._FollowUpStage.OPEN,
        )
        self.assertEqual(commit["token"], control["token"])
        self.assertEqual(reservation.response_id, "question-response")
        self.assertIsNone(handler._user_turn_non_close_generation)
        handler._handle_request_follow_up_ack(
            {
                "type": "request_follow_up_ack",
                "token": control["token"],
                "session_nonce": control["session_nonce"],
                "accepted": False,
            }
        )
        self.assertTrue(reservation.ack_accepted)

    async def test_wrong_response_audio_never_sends_follow_up(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)
        self._activate(handler)
        handler.arm_request_follow_up_continuation({"request-call"})
        handler.bind_request_follow_up_response("question-response")

        handler.note_request_follow_up_response_audio("unrelated-response")
        await handler._before_reply_idle()

        websocket.send.assert_not_awaited()
        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_competing_response_created_cannot_steal_follow_up(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler, "owned-tool-call")
        self._activate(handler, "owned-tool-call")

        self.assertFalse(
            handler.bind_request_follow_up_response("competing-response")
        )
        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_only_exact_tool_continuation_can_arm_follow_up(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler, "owned-tool-call")
        self._activate(handler, "owned-tool-call")

        self.assertFalse(
            handler.arm_request_follow_up_continuation(
                {"owned-tool-call", "other-tool-call"}
            )
        )
        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_failed_continuation_send_cancels_follow_up(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler, "owned-tool-call")
        self._activate(handler, "owned-tool-call")
        self.assertTrue(
            handler.arm_request_follow_up_continuation({"owned-tool-call"})
        )

        handler.fail_request_follow_up_continuation({"owned-tool-call"})

        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_response_done_without_matching_audio_cancels_follow_up(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)
        self._activate(handler)
        handler.arm_request_follow_up_continuation({"request-call"})
        handler.bind_request_follow_up_response("silent-response")

        handler.note_request_follow_up_response_done(
            "silent-response",
            "completed",
        )
        await handler._before_reply_idle()

        websocket.send.assert_not_awaited()
        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_openai_events_authoritatively_bind_follow_up_response(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)
        self._activate(handler)

        service = main.SafeRealtimeLLMService(
            max_context_turns=1,
            manual_response_gating=True,
        )
        service.set_request_follow_up_event_handlers(
            on_response_created=handler.bind_request_follow_up_response,
            on_response_audio=handler.note_request_follow_up_response_audio,
            on_response_done=handler.note_request_follow_up_response_done,
            on_response_failed=handler.note_request_follow_up_response_failed,
            on_continuation_arm=handler.arm_request_follow_up_continuation,
            on_continuation_failed=handler.fail_request_follow_up_continuation,
        )
        service.set_assistant_output_event_handlers(
            on_response_created=handler.bind_assistant_output_response,
        )
        service._api_session_ready = True
        service._continuation_result_call_ids.add("request-call")
        service.send_client_event = AsyncMock()
        await service._run_tool_continuation(service._session_generation)
        service.send_client_event.assert_awaited_once()
        reservation = cast(Any, handler._request_follow_up_reservation)
        self.assertTrue(reservation.continuation_armed)

        service._handle_evt_audio_delta = AsyncMock()
        service._handle_evt_response_done = AsyncMock()
        service.push_error = AsyncMock()

        async def messages():
            yield types.SimpleNamespace(
                type="response.created",
                response=types.SimpleNamespace(id="bound-response"),
            )
            yield types.SimpleNamespace(
                type="response.output_audio.delta",
                response_id="bound-response",
            )
            yield types.SimpleNamespace(
                type="response.done",
                response=types.SimpleNamespace(
                    id="bound-response",
                    status="completed",
                ),
            )

        service._websocket = messages()
        await service._receive_task_handler()
        reservation = cast(Any, handler._request_follow_up_reservation)
        self.assertEqual(reservation.response_id, "bound-response")
        self.assertTrue(reservation.question_audio_started)
        self.assertTrue(reservation.response_completed)
        self.assertTrue(
            await handler._authorize_output_audio(("bound-response", 1), websocket)
        )
        self.assertFalse(
            await handler._authorize_output_audio(("bound-response", 2), websocket)
        )

    async def test_wrong_and_rejected_follow_up_acks_fail_closed(self):
        for accepted in (True, False):
            with self.subTest(accepted=accepted):
                websocket = _FakeDeviceWebSocket()
                handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
                handler.REQUEST_FOLLOW_UP_ACK_TIMEOUT_S = 0.05
                self._admit(handler, websocket)
                handler.note_request_follow_up_turn_boundary()
                await self._reserve(handler)
                self._activate(handler)
                self._qualify_response(handler, "ack-response")

                send_task = asyncio.create_task(handler._before_reply_idle())
                while websocket.send.await_count == 0:
                    await asyncio.sleep(0)
                request = json.loads(cast(Any, websocket.send.await_args).args[0])
                token = request["token"] if not accepted else request["token"] + 1
                handler._handle_request_follow_up_ack(
                    {
                        "type": "request_follow_up_ack",
                        "token": token,
                        "session_nonce": request["session_nonce"],
                        "accepted": accepted,
                    }
                )
                if accepted:
                    cancel_ack_task = asyncio.create_task(
                        self._ack_cancel_requested_follow_up(
                            handler,
                            websocket,
                            accepted=True,
                            cleared=True,
                        )
                    )
                    await send_task
                    cancel = await cancel_ack_task
                    self.assertEqual(cancel["type"], "cancel_request_follow_up")
                    self.assertEqual(cancel["token"], request["token"])
                else:
                    await send_task
                    self.assertEqual(websocket.send.await_count, 1)

                self.assertIsNone(handler._request_follow_up_reservation)

    async def test_wrong_session_request_ack_is_ignored(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        handler.REQUEST_FOLLOW_UP_ACK_TIMEOUT_S = 0.02
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "wrong-session-response")

        send_task = asyncio.create_task(handler._before_reply_idle())
        while websocket.send.await_count == 0:
            await asyncio.sleep(0)
        request = json.loads(cast(Any, websocket.send.await_args).args[0])
        with self.assertLogs("app.websocket_handler", level="WARNING"):
            handler._handle_request_follow_up_ack(
                {
                    "type": "request_follow_up_ack",
                    "token": request["token"],
                    "session_nonce": request["session_nonce"] + 1,
                    "accepted": True,
                }
            )
        cancel_ack_task = asyncio.create_task(
            self._ack_cancel_requested_follow_up(handler, websocket)
        )
        await send_task
        await cancel_ack_task

        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_cancel_ack_failures_retire_bound_socket(self):
        cases = (
            ("contradictory-rejected", False, True, False),
            ("rejected-already-cleared", False, False, False),
            ("not-cleared", True, False, False),
            ("malformed", "yes", True, False),
            ("wrong-session", True, True, True),
            ("missing", True, True, None),
        )
        for name, accepted, cleared, wrong_session in cases:
            with self.subTest(name=name):
                websocket = _FakeDeviceWebSocket()
                handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
                handler.REQUEST_FOLLOW_UP_ACK_TIMEOUT_S = 0.01
                self._admit(handler, websocket)
                handler.note_request_follow_up_turn_boundary()
                await self._reserve(handler)
                self._activate(handler)
                self._qualify_response(handler, f"cancel-{name}")
                request_ack_task = asyncio.create_task(
                    self._ack_requested_follow_up(handler, websocket)
                )
                await handler._before_reply_idle()
                request = await request_ack_task

                handler.invalidate_request_follow_up_turn()
                while websocket.send.await_count < 2:
                    await asyncio.sleep(0)
                cancel = json.loads(websocket.send.await_args_list[1].args[0])
                if wrong_session is not None:
                    if wrong_session:
                        with self.assertLogs(
                            "app.websocket_handler",
                            level="WARNING",
                        ):
                            handler._handle_cancel_request_follow_up_ack(
                                {
                                    "type": "cancel_request_follow_up_ack",
                                    "token": cancel["token"],
                                    "session_nonce": cancel["session_nonce"] + 1,
                                    "accepted": accepted,
                                    "cleared": cleared,
                                }
                            )
                    else:
                        handler._handle_cancel_request_follow_up_ack(
                            {
                                "type": "cancel_request_follow_up_ack",
                                "token": cancel["token"],
                                "session_nonce": cancel["session_nonce"],
                                "accepted": accepted,
                                "cleared": cleared,
                            }
                        )
                with self.assertLogs("app.websocket_handler", level="WARNING"):
                    await handler._await_request_follow_up_settlements()

                self.assertEqual(cancel["token"], request["token"])
                websocket.close.assert_awaited_once_with()
                self.assertEqual(handler._websockets, set())
                self.assertIsNone(handler._active_session_nonce)

    async def test_matching_idempotent_cancel_ack_confirms_prior_revocation(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "already-revoked-response")
        request_ack_task = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        await request_ack_task

        cancel_ack_task = asyncio.create_task(
            self._ack_cancel_requested_follow_up(
                handler,
                websocket,
                accepted=True,
                cleared=True,
            )
        )
        handler.invalidate_request_follow_up_turn()
        await cancel_ack_task
        await handler._await_request_follow_up_settlements()

        self.assertEqual(handler._websockets, {websocket})
        self.assertIsNotNone(handler._active_session_nonce)
        websocket.close.assert_not_awaited()

    async def test_idle_barrier_waits_for_cancel_ack(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "idle-barrier-response")
        request_ack_task = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        await request_ack_task

        handler.invalidate_request_follow_up_turn()
        while websocket.send.await_count < 2:
            await asyncio.sleep(0)
        idle_barrier = asyncio.create_task(handler._before_reply_idle())
        await asyncio.sleep(0)
        self.assertFalse(idle_barrier.done())

        cancel = json.loads(websocket.send.await_args_list[1].args[0])
        handler._handle_cancel_request_follow_up_ack(
            {
                "type": "cancel_request_follow_up_ack",
                "token": cancel["token"],
                "session_nonce": cancel["session_nonce"],
                "accepted": True,
                "cleared": True,
            }
        )
        await idle_barrier
        websocket.close.assert_not_awaited()

    async def test_overlapping_speech_does_not_rebase_tool_generation(self):
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, _FakeDeviceWebSocket())
        original = websocket_handler.TURN_LIVENESS.non_close_tool_generation
        try:
            handler.note_request_follow_up_turn_boundary()
            websocket_handler.TURN_LIVENESS.non_close_tool_generation += 1
            handler.note_request_follow_up_turn_boundary()
            self.assertEqual(handler._user_turn_non_close_generation, original)

            wake_generation = handler._device_wake_generation
            handler._request_follow_up_budget_spent = True
            handler.note_request_follow_up_turn_boundary(force=True)
            self.assertEqual(
                handler._user_turn_non_close_generation,
                original + 1,
            )
            self.assertEqual(handler._device_wake_generation, wake_generation)
            self.assertTrue(handler._request_follow_up_budget_spent)
        finally:
            websocket_handler.TURN_LIVENESS.non_close_tool_generation = original

    async def test_accepted_window_is_consumed_by_fresh_answer_without_cancel(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "answer-question")
        await self._open_requested_follow_up(handler, websocket)

        handler.note_request_follow_up_turn_boundary()

        self.assertIsNone(handler._request_follow_up_reservation)
        self.assertIsNotNone(handler._user_turn_non_close_generation)
        self.assertTrue(handler._request_follow_up_budget_spent)
        self.assertTrue(handler.bind_request_follow_up_answer("fresh-item", 1))
        self.assertTrue(
            handler.confirm_request_follow_up_answer(
                "fresh-item",
                1,
                "fresh answer",
            )
        )
        self.assertFalse(handler._request_follow_up_budget_spent)
        self.assertEqual(websocket.send.await_count, 2)

    async def test_openai_speech_can_atomically_bind_before_phase_emitter(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "atomic-answer-question")
        await self._open_requested_follow_up(handler, websocket)
        open_epoch = handler._request_follow_up_epoch

        self.assertTrue(handler.bind_request_follow_up_answer("fresh-item", 1))
        self.assertIsNone(handler._request_follow_up_reservation)
        grant = handler._request_follow_up_answer_grant
        self.assertIsNotNone(grant)
        self.assertEqual(handler._request_follow_up_epoch, open_epoch + 1)

        handler.note_request_follow_up_turn_boundary()
        self.assertIs(handler._request_follow_up_answer_grant, grant)
        self.assertEqual(handler._request_follow_up_epoch, open_epoch + 1)
        self.assertTrue(
            handler.confirm_request_follow_up_answer(
                "fresh-item",
                1,
                "fresh answer",
            )
        )
        self.assertFalse(handler._request_follow_up_budget_spent)

        handler._check_nearby_media_activity = AsyncMock(
            return_value=websocket_handler.MediaActivity.CLEAR
        )
        self.assertEqual(
            await handler.reserve_request_follow_up("next-question"),
            websocket_handler.FollowUpReservationOutcome.RESERVED,
        )
        handler._check_nearby_media_activity.assert_awaited_once_with()

    async def test_malformed_identity_cannot_consume_open_answer(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "malformed-answer-question")
        await self._open_requested_follow_up(handler, websocket)
        reservation = handler._request_follow_up_reservation
        open_epoch = handler._request_follow_up_epoch

        self.assertFalse(handler.bind_request_follow_up_answer("", 1))
        self.assertFalse(handler.bind_request_follow_up_answer("fresh-item", 0))
        self.assertIs(handler._request_follow_up_reservation, reservation)
        self.assertEqual(handler._request_follow_up_epoch, open_epoch)
        self.assertIsNone(handler._request_follow_up_answer_grant)

    async def test_microphone_audio_is_blocked_until_exact_commit_ack(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "two-phase-question")

        prepare_ack = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        request = await prepare_ack
        self.assertFalse(handler._binary_audio_is_admitted(websocket))
        self.assertEqual(websocket.send.await_count, 1)

        reservation = cast(Any, handler._request_follow_up_reservation)
        await handler._handle_device_control_message(
            {
                "type": "follow_up_ready",
                "token": request["token"],
                "session_nonce": request["session_nonce"],
                "ready_nonce": 987654321,
            },
            websocket,
        )
        while websocket.send.await_count < 2:
            await asyncio.sleep(0)
        commit = json.loads(websocket.send.await_args_list[1].args[0])
        self.assertEqual(commit["type"], "commit_follow_up")
        self.assertEqual(
            reservation.stage,
            websocket_handler._FollowUpStage.COMMITTING,
        )
        self.assertFalse(handler._binary_audio_is_admitted(websocket))

        await handler._handle_device_control_message(
            {**commit, "type": "commit_follow_up_ack", "accepted": True},
            websocket,
        )
        await handler._await_request_follow_up_settlements()
        self.assertTrue(handler._binary_audio_is_admitted(websocket))
        self.assertEqual(
            reservation.stage,
            websocket_handler._FollowUpStage.OPEN,
        )

    async def test_thinking_closes_pcm_but_preserves_logical_wake_ownership(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake(1))
        self.assertTrue(handler._binary_audio_is_admitted(websocket))
        await handler.bind_assistant_output_response("old-response", 1)

        await handler.broadcast_phase("thinking")
        phase = json.loads(cast(Any, websocket.send.await_args).args[0])
        self.assertEqual(
            phase,
            {
                "type": "phase",
                "value": "thinking",
                "session_nonce": handler._active_session_nonce,
                "wake_generation": 1,
            },
        )
        self.assertFalse(handler._binary_audio_is_admitted(websocket))
        self.assertEqual(handler._device_wake_generation, 1)
        self.assertEqual(handler._wake_session_socket, websocket)
        self.assertFalse(handler._request_follow_up_budget_spent)
        self.assertIsNone(handler._assistant_output_grant)

        await handler.broadcast_phase("replying")
        self.assertFalse(handler._binary_audio_is_admitted(websocket))

        self.assertTrue(handler.note_device_wake(2))
        self.assertTrue(handler._binary_audio_is_admitted(websocket))
        await handler._handle_device_control_message(
            {
                "type": "flush",
                "session_nonce": handler._active_session_nonce,
                "wake_generation": 2,
            },
            websocket,
        )
        self.assertFalse(handler._binary_audio_is_admitted(websocket))

    def test_ready_deadline_covers_final_firmware_physical_bounds(self):
        handler = websocket_handler.WebSocketHandler(
            follow_up_ms=0,
            follow_up_open_delay_ms=5000,
        )
        timeout_s = handler._request_follow_up_ready_timeout_s()
        ring_drain_s = (
            handler.FIRMWARE_AUDIO_RING_BYTES
            / handler.FIRMWARE_OUTPUT_BYTES_PER_SECOND
        )
        physical_bound_s = (
            ring_drain_s
            + handler.FIRMWARE_PLAYBACK_PREBUFFER_MAX_S
            + handler.FIRMWARE_SPEAKER_DRAIN_TIMEOUT_S
            + handler.FIRMWARE_MIC_SEND_BARRIER_TIMEOUT_S
            + handler.FIRMWARE_FOLLOW_UP_CHIME_WAIT_TIMEOUT_S
            + 5.0
        )

        self.assertEqual(timeout_s, 59.0)
        self.assertGreater(timeout_s, physical_bound_s)
        self.assertGreater(
            handler.REQUEST_FOLLOW_UP_COMMIT_ACK_TIMEOUT_S,
            handler.FIRMWARE_FOLLOW_UP_COMMIT_TIMEOUT_S,
        )

    async def test_stale_bound_phase_is_not_sent_for_a_new_wake_generation(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake(1))
        session_nonce = cast(int, handler._active_session_nonce)
        self.assertTrue(handler.note_device_wake(2))

        sent_before = websocket.send.await_count
        self.assertFalse(
            await handler._send_phase_for_context(
                websocket,
                "idle",
                session_nonce,
                1,
            )
        )
        self.assertEqual(websocket.send.await_count, sent_before)
        self.assertEqual(
            handler._build_phase_control("idle"),
            {"type": "phase", "value": "idle"},
        )
        for value in ("listening", "thinking", "replying", "idle"):
            self.assertEqual(
                handler._build_phase_control(
                    value,
                    session_nonce=session_nonce,
                    wake_generation=2,
                ),
                {
                    "type": "phase",
                    "value": value,
                    "session_nonce": session_nonce,
                    "wake_generation": 2,
                },
            )
        for value in ("listening", "thinking", "replying"):
            self.assertEqual(
                handler._build_phase_control(
                    value,
                    session_nonce=session_nonce,
                    wake_generation=2,
                    follow_up_token=77,
                ),
                {
                    "type": "phase",
                    "value": value,
                    "session_nonce": session_nonce,
                    "wake_generation": 2,
                    "token": 77,
                },
            )
        with self.assertRaisesRegex(ValueError, "terminal idle"):
            handler._build_phase_control(
                "idle",
                session_nonce=session_nonce,
                wake_generation=2,
                follow_up_token=77,
            )

    async def test_phase_send_completes_before_a_new_wake_can_advance_ownership(self):
        send_started = asyncio.Event()
        release_send = asyncio.Event()

        async def send(_message):
            send_started.set()
            await release_send.wait()

        websocket = _FakeDeviceWebSocket(send=AsyncMock(side_effect=send))
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        session_nonce = self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake(1))

        phase_send = asyncio.create_task(handler.broadcast_phase("thinking"))
        await send_started.wait()
        new_wake = asyncio.create_task(
            handler._handle_device_control_message(
                {
                    "type": "wake",
                    "session_nonce": session_nonce,
                    "wake_generation": 2,
                },
                websocket,
            )
        )
        await asyncio.sleep(0)
        self.assertFalse(new_wake.done())
        self.assertEqual(handler._device_wake_generation, 1)

        release_send.set()
        await phase_send
        self.assertTrue(await new_wake)
        self.assertEqual(handler._device_wake_generation, 2)

    async def test_phase_revalidates_after_output_settlement(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake(1))
        context = handler.capture_phase_authorization_context()
        handler._device_audio_generation = 73
        handler._retire_assistant_output_grant = Mock(return_value=object())

        async def settle_and_advance(_grant):
            handler._request_follow_up_epoch += 1
            return True

        handler._settle_retired_assistant_output = AsyncMock(
            side_effect=settle_and_advance
        )

        self.assertFalse(await handler.broadcast_phase("thinking", context))
        self.assertEqual(handler._device_audio_generation, 73)
        self.assertEqual(websocket.send.await_count, 0)

    async def test_client_revoke_clear_settles_before_next_wake_admission(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        session_nonce = self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake(1))
        clear_started = asyncio.Event()
        release_clear = asyncio.Event()

        async def clear_input(reason, generation=None):
            clear_started.set()
            await release_clear.wait()

        handler._clear_device_input = clear_input
        revoke = asyncio.create_task(
            handler._handle_device_control_message(
                {
                    "type": "client_revoke",
                    "session_nonce": session_nonce,
                    "wake_generation": 1,
                    "reason": "mic_send_failed",
                },
                websocket,
            )
        )
        await clear_started.wait()
        new_wake = asyncio.create_task(
            handler._handle_device_control_message(
                {
                    "type": "wake",
                    "session_nonce": session_nonce,
                    "wake_generation": 2,
                },
                websocket,
            )
        )
        await asyncio.sleep(0)
        self.assertFalse(new_wake.done())

        release_clear.set()
        await revoke
        self.assertTrue(await new_wake)
        self.assertEqual(handler._device_wake_generation, 2)

    async def test_failed_client_revoke_clear_retires_the_physical_socket(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        session_nonce = self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake(1))
        handler._clear_device_input = AsyncMock(
            side_effect=TimeoutError("clear receipt missing")
        )

        await handler._handle_device_control_message(
            {
                "type": "client_revoke",
                "session_nonce": session_nonce,
                "wake_generation": 1,
                "reason": "mic_send_failed",
            },
            websocket,
        )

        self.assertNotIn(websocket, handler._websockets)
        self.assertIsNone(handler._active_session_nonce)
        self.assertTrue(handler._input_clear_fail_closed)
        self.assertFalse(
            await handler._handle_device_control_message(
                {
                    "type": "wake",
                    "session_nonce": session_nonce,
                    "wake_generation": 2,
                },
                websocket,
            )
        )
        self.assertEqual(handler._device_wake_generation, 1)
        websocket.close.assert_awaited_once()

    async def test_transmitted_wake_generations_are_gap_free_across_revoke_and_wrap(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        session_nonce = self._admit(handler, websocket)

        self.assertTrue(
            await handler._handle_device_control_message(
                {
                    "type": "wake",
                    "session_nonce": session_nonce,
                    "wake_generation": 41,
                },
                websocket,
            )
        )
        await handler._handle_device_control_message(
            {
                "type": "client_revoke",
                "session_nonce": session_nonce,
                "wake_generation": 41,
                "reason": "wake_commit_race",
            },
            websocket,
        )
        self.assertIsNone(handler._device_audio_generation)
        self.assertTrue(
            await handler._handle_device_control_message(
                {
                    "type": "wake",
                    "session_nonce": session_nonce,
                    "wake_generation": 42,
                },
                websocket,
            )
        )
        with self.assertLogs("app.websocket_handler", level="WARNING"):
            self.assertFalse(
                await handler._handle_device_control_message(
                    {
                        "type": "wake",
                        "session_nonce": session_nonce,
                        "wake_generation": 42,
                    },
                    websocket,
                )
            )
        self.assertEqual(handler._device_audio_generation, 42)

        handler._device_wake_generation = 0x7FFFFFFF
        handler._device_audio_generation = None
        self.assertTrue(
            await handler._handle_device_control_message(
                {
                    "type": "wake",
                    "session_nonce": session_nonce,
                    "wake_generation": 1,
                },
                websocket,
            )
        )
        self.assertEqual(handler._device_audio_generation, 1)

    async def test_stale_ready_cannot_commit_a_fresh_reservation(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler, "first")
        first = cast(Any, handler._request_follow_up_reservation)
        handler.invalidate_request_follow_up_turn(send_cancel=False)
        handler.note_device_wake()
        await self._reserve(handler, "second")
        second = handler._request_follow_up_reservation

        with self.assertLogs("app.websocket_handler", level="WARNING"):
            await handler._handle_device_control_message(
                {
                    "type": "follow_up_ready",
                    "token": first.token,
                    "session_nonce": first.session_nonce,
                    "ready_nonce": 987654321,
                },
                websocket,
            )

        self.assertIs(handler._request_follow_up_reservation, second)
        websocket.send.assert_not_awaited()
        handler.cancel_request_follow_up(send_cancel=False)

    async def test_accepted_prepare_is_revoked_if_local_generation_changes(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "prepare-race")

        async def accept_then_invalidate():
            while websocket.send.await_count < 1:
                await asyncio.sleep(0)
            request = json.loads(websocket.send.await_args_list[0].args[0])
            await handler._handle_device_control_message(
                {**request, "type": "request_follow_up_ack", "accepted": True},
                websocket,
            )
            websocket_handler.TURN_LIVENESS.non_close_tool_started()
            while websocket.send.await_count < 2:
                await asyncio.sleep(0)
            cancel = json.loads(websocket.send.await_args_list[1].args[0])
            await handler._handle_device_control_message(
                {
                    **cancel,
                    "type": "cancel_request_follow_up_ack",
                    "accepted": True,
                    "cleared": True,
                },
                websocket,
            )

        receipt = asyncio.create_task(accept_then_invalidate())
        await handler._before_reply_idle()
        await receipt

        sent_types = [
            json.loads(call.args[0])["type"]
            for call in websocket.send.await_args_list
        ]
        self.assertEqual(
            sent_types,
            ["request_follow_up", "cancel_request_follow_up"],
        )
        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_accepted_commit_is_revoked_if_local_generation_changes(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "commit-race")
        prepare_ack = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        request = await prepare_ack
        await handler._handle_device_control_message(
            {
                "type": "follow_up_ready",
                "token": request["token"],
                "session_nonce": request["session_nonce"],
                "ready_nonce": 987654321,
            },
            websocket,
        )
        while websocket.send.await_count < 2:
            await asyncio.sleep(0)
        commit = json.loads(websocket.send.await_args_list[1].args[0])

        await handler._handle_device_control_message(
            {**commit, "type": "commit_follow_up_ack", "accepted": True},
            websocket,
        )
        websocket_handler.TURN_LIVENESS.non_close_tool_started()
        self.assertFalse(handler._binary_audio_is_admitted(websocket))
        while websocket.send.await_count < 3:
            await asyncio.sleep(0)
        cancel = json.loads(websocket.send.await_args_list[2].args[0])
        await handler._handle_device_control_message(
            {
                **cancel,
                "type": "cancel_request_follow_up_ack",
                "accepted": True,
                "cleared": True,
            },
            websocket,
        )
        await handler._await_request_follow_up_settlements()

        self.assertEqual(cancel["type"], "cancel_request_follow_up")
        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_late_assistant_audio_revokes_prepared_follow_up(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "late-audio-question")
        prepare_ack = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        await prepare_ack
        cancel_ack = asyncio.create_task(
            self._ack_cancel_requested_follow_up(handler, websocket)
        )

        self.assertFalse(await handler._authorize_output_audio())
        await cancel_ack
        await handler._await_request_follow_up_settlements()
        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_assistant_pcm_requires_exact_socket_wake_and_response_generation(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake(1))
        await handler.bind_assistant_output_response("response-a", 7)

        self.assertTrue(
            await handler._authorize_output_audio(("response-a", 7), websocket)
        )
        self.assertFalse(
            await handler._authorize_output_audio(("response-a", 6), websocket)
        )
        self.assertFalse(
            await handler._authorize_output_audio(("response-b", 7), websocket)
        )

        replacement = _FakeDeviceWebSocket()
        handler._websockets = {replacement}
        self.assertFalse(
            await handler._authorize_output_audio(("response-a", 7), websocket)
        )

    async def test_audio_drain_wait_never_holds_socket_transition_lock(self):
        entered = asyncio.Event()
        release = asyncio.Event()

        class DrainTransport:
            async def bind_output_audio_generation(self, _context, _websocket):
                return True

            async def gracefully_finish_output_audio_generation(
                self,
                _context,
                _websocket,
                ownership_is_current,
                *,
                timeout_s,
            ):
                self.assert_timeout = timeout_s
                entered.set()
                await release.wait()
                return ownership_is_current()

            def retire_output_audio_generation(self, _context=None):
                return True

        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.transport = DrainTransport()
        self.assertTrue(handler.note_device_wake(1))
        self.assertTrue(
            await handler.bind_assistant_output_response("response-a", 7)
        )

        finish = asyncio.create_task(
            handler.finish_assistant_output_response("response-a", 7)
        )
        await entered.wait()
        await asyncio.wait_for(handler._socket_transition_lock.acquire(), timeout=0.1)
        handler._socket_transition_lock.release()
        release.set()

        self.assertTrue(await finish)

    async def test_failed_finish_settles_before_clearing_output_grant(self):
        settlement_started = asyncio.Event()
        release_settlement = asyncio.Event()

        class FailedFinishTransport:
            def __init__(self, websocket):
                self.admitted_websocket = websocket

            async def bind_output_audio_generation(self, _context, _websocket):
                return True

            async def gracefully_finish_output_audio_generation(
                self,
                _context,
                _websocket,
                _ownership_is_current,
                *,
                timeout_s,
            ):
                _ = timeout_s
                return False

            async def settle_output_audio_generation(self, _context=None):
                settlement_started.set()
                await release_settlement.wait()
                return True

            def retire_output_audio_generation(self, _context=None):
                return True

        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.transport = FailedFinishTransport(websocket)
        self.assertTrue(handler.note_device_wake(1))
        self.assertTrue(
            await handler.bind_assistant_output_response("response-a", 8)
        )
        grant = handler._assistant_output_grant

        finish = asyncio.create_task(
            handler.finish_assistant_output_response("response-a", 8)
        )
        await settlement_started.wait()
        self.assertIs(handler._assistant_output_grant, grant)
        release_settlement.set()

        self.assertFalse(await finish)
        self.assertIsNone(handler._assistant_output_grant)

    async def test_stop_disconnect_and_replacement_revoke_audio_drain_owner(self):
        for path in ("stop", "disconnect", "replacement"):
            with self.subTest(path=path):
                entered = asyncio.Event()

                class DrainTransport:
                    async def bind_output_audio_generation(
                        self,
                        _context,
                        _websocket,
                    ):
                        return True

                    async def gracefully_finish_output_audio_generation(
                        self,
                        _context,
                        _websocket,
                        ownership_is_current,
                        *,
                        timeout_s,
                    ):
                        _ = timeout_s
                        entered.set()
                        while ownership_is_current():
                            await asyncio.sleep(0)
                        return False

                    def retire_output_audio_generation(self, _context=None):
                        return True

                    async def settle_output_audio_generation(self, _context=None):
                        return True

                websocket = _FakeDeviceWebSocket()
                handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
                session_nonce = self._admit(handler, websocket)
                handler.transport = DrainTransport()
                self.assertTrue(handler.note_device_wake(1))
                self.assertTrue(
                    await handler.bind_assistant_output_response(
                        "response-a",
                        7,
                    )
                )
                finish = asyncio.create_task(
                    handler.finish_assistant_output_response("response-a", 7)
                )
                await entered.wait()

                if path == "stop":
                    self.assertTrue(
                        await handler._handle_device_control_message(
                            {
                                "type": "interrupt",
                                "session_nonce": session_nonce,
                                "wake_generation": 1,
                                "reason": "stop",
                            },
                            websocket,
                        )
                    )
                elif path == "disconnect":
                    async with handler._socket_transition_lock:
                        handler._retire_assistant_output_grant()
                        handler._mark_socket_retired(websocket)
                        handler._websockets.clear()
                        handler._active_session_nonce = None
                else:
                    replacement = _FakeDeviceWebSocket()
                    async with handler._socket_transition_lock:
                        handler._websockets = {replacement}

                self.assertFalse(await finish)
                self.assertIsNone(handler._assistant_output_grant)

    async def test_retired_output_grant_cancels_only_its_exact_response(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake(1))
        await handler.bind_assistant_output_response("response-a", 7)
        handler._cancel_assistant_output_callback = AsyncMock()

        grant = handler._retire_assistant_output_grant()
        await handler._cancel_retired_assistant_output(grant)

        handler._cancel_assistant_output_callback.assert_awaited_once_with(
            "response-a",
            7,
        )
        self.assertFalse(
            await handler._authorize_output_audio(("response-a", 7), websocket)
        )

    async def test_old_reply_finalizer_cannot_spend_or_clear_fresh_wake(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "old-finalizer")
        old_context = handler.capture_reply_finalizer_context()

        self.assertTrue(handler.note_device_wake())
        fresh_generation = handler._device_wake_generation
        fresh_baseline = handler._user_turn_non_close_generation
        self.assertFalse(handler._request_follow_up_budget_spent)

        self.assertFalse(await handler._before_reply_idle(old_context))
        self.assertEqual(handler._device_wake_generation, fresh_generation)
        self.assertEqual(handler._user_turn_non_close_generation, fresh_baseline)
        self.assertFalse(handler._request_follow_up_budget_spent)
        websocket.send.assert_not_awaited()

    async def test_protocol_histories_and_task_sets_fail_closed_at_bounds(self):
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        issued = set(range(1, handler.PROTOCOL_HISTORY_LIMIT + 1))
        with self.assertRaisesRegex(RuntimeError, "history is full"):
            handler._new_protocol_id(issued)

        handler._request_follow_up_tasks = {
            cast(Any, object()) for _ in range(handler.MAX_FOLLOW_UP_TASKS)
        }
        coroutine = asyncio.sleep(0)
        with self.assertRaisesRegex(RuntimeError, "task limit"):
            handler._track_request_follow_up_task(coroutine)
        self.assertTrue(handler._follow_up_fail_closed)

        for index in range(handler.MAX_UNCERTAIN_SOCKETS + 2):
            handler._remember_uncertain_socket(f"socket-{index}")
        self.assertEqual(
            len(handler._uncertain_retired_sockets),
            handler.MAX_UNCERTAIN_SOCKETS,
        )

    async def test_requested_follow_up_requires_zero_mode_and_exactly_one_socket(self):
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        with self.assertRaisesRegex(RuntimeError, "exactly one"):
            await self._reserve(handler)

        first = _FakeDeviceWebSocket()
        second = _FakeDeviceWebSocket()
        handler._websockets.update((first, second))
        with self.assertRaisesRegex(RuntimeError, "exactly one"):
            await self._reserve(handler)

        automatic = websocket_handler.WebSocketHandler(follow_up_ms=8000)
        self._admit(automatic, first)
        with self.assertRaisesRegex(RuntimeError, "automatic mode"):
            await self._reserve(automatic)

    async def test_duplicate_requested_follow_up_coalesces(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()

        self.assertEqual(
            await self._reserve(handler, "first-request"),
            websocket_handler.FollowUpReservationOutcome.RESERVED,
        )
        original = cast(Any, handler._request_follow_up_reservation)
        self.assertTrue(self._activate(handler, "first-request"))
        self.assertEqual(
            await self._reserve(handler, "first-request"),
            websocket_handler.FollowUpReservationOutcome.ALREADY_RESERVED,
        )
        self.assertIs(handler._request_follow_up_reservation, original)
        self.assertTrue(original.active)
        self._qualify_response(
            handler,
            "first-response",
            "first-request",
        )
        ack_task = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        await ack_task

        self.assertEqual(websocket.send.await_count, 1)
        self.assertEqual(
            json.loads(cast(Any, websocket.send.await_args).args[0])["token"],
            original.token,
        )

    async def test_graceful_close_conflict_cancels_or_rejects_follow_up(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()

        await self._reserve(handler)
        await handler.request_graceful_close()
        self.assertIsNone(handler._request_follow_up_reservation)

        with self.assertRaisesRegex(RuntimeError, "conflicting graceful close"):
            await self._reserve(handler)

    async def test_stale_tool_generation_cancels_requested_follow_up(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        original_generation = websocket_handler.TURN_LIVENESS.non_close_tool_generation

        try:
            await self._reserve(handler)
            self._activate(handler)
            self._qualify_response(handler, "stale-response")
            websocket_handler.TURN_LIVENESS.non_close_tool_generation += 1
            await handler._before_reply_idle()
        finally:
            websocket_handler.TURN_LIVENESS.non_close_tool_generation = original_generation

        websocket.send.assert_not_awaited()
        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_tool_started_before_request_is_rejected_for_current_user_turn(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        original_generation = websocket_handler.TURN_LIVENESS.non_close_tool_generation
        handler.note_device_wake()

        try:
            websocket_handler.TURN_LIVENESS.non_close_tool_generation += 1
            with self.assertRaisesRegex(RuntimeError, "already ran"):
                await self._reserve(handler)
        finally:
            websocket_handler.TURN_LIVENESS.non_close_tool_generation = original_generation

        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_changed_socket_cancels_requested_follow_up(self):
        original = _FakeDeviceWebSocket()
        replacement = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, original)
        handler.note_request_follow_up_turn_boundary()

        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "changed-socket-response")
        self._admit(handler, replacement, nonce=self.TEST_SESSION_NONCE + 1)
        await handler._before_reply_idle()

        original.send.assert_not_awaited()
        replacement.send.assert_not_awaited()
        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_expired_requested_follow_up_is_never_sent(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()

        await self._reserve(handler)
        reservation = cast(Any, handler._request_follow_up_reservation)
        reservation.expires_at = websocket_handler.time.monotonic() - 1.0
        await handler._expire_request_follow_up(reservation)
        await handler._before_reply_idle()

        websocket.send.assert_not_awaited()
        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_requested_follow_up_send_failure_is_not_retried(self):
        websocket = _FakeDeviceWebSocket(
            AsyncMock(side_effect=RuntimeError("test send failure"))
        )
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()

        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "send-failure-response")
        with self.assertLogs("app.websocket_handler", level="WARNING"):
            await handler._before_reply_idle()
        await handler._before_reply_idle()

        # One request plus one best-effort cancel; neither is retried.
        self.assertEqual(websocket.send.await_count, 2)
        websocket.close.assert_awaited_once_with()
        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_hello_keeps_zero_follow_up_and_socket_boundaries_cancel(self):
        class EventTransport:
            def __init__(self):
                self.handlers = {}

            def event_handler(self, name):
                def register(callback):
                    self.handlers[name] = callback
                    return callback

                return register

        class WebSocket:
            def __init__(self, host):
                self.client = types.SimpleNamespace(host=host)
                self.send = AsyncMock()
                self.close = AsyncMock()

        transport = EventTransport()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        handler._clear_device_input = AsyncMock()
        connected = AsyncMock()
        disconnected = Mock()
        handler.setup_event_handlers(
            transport,
            connected,
            disconnected,
        )
        first = WebSocket("first")
        replacement = WebSocket("replacement")

        await transport.handlers["on_client_connected"](transport, first)
        hello_message = cast(Any, first.send.await_args).args[0]
        hello = json.loads(hello_message)
        self.assertEqual(hello["follow_up_ms"], 0)
        self.assertGreater(hello["nonce"], 0)
        self.assertIn('"follow_up_ms":0', hello_message)
        self.assertNotIn(": ", hello_message)
        self.assertNotIn(first, handler._websockets)
        connected.assert_not_awaited()

        await handler._handle_device_control_message(
            {**hello, "type": "hello_ack", "accepted": True},
            first,
        )
        self.assertIn(first, handler._websockets)
        connected.assert_awaited_once_with("first")

        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)
        await transport.handlers["on_client_disconnected"](transport, first)
        self.assertIsNone(handler._request_follow_up_reservation)
        disconnected.assert_called_once_with("first")

        await transport.handlers["on_client_connected"](transport, replacement)
        replacement_hello = json.loads(
            cast(Any, replacement.send.await_args).args[0]
        )
        await handler._handle_device_control_message(
            {**replacement_hello, "type": "hello_ack", "accepted": True},
            replacement,
        )
        self.assertEqual(handler._websockets, {replacement})

    async def test_hello_waits_for_reconnect_clear_before_wake_or_output(self):
        class EventTransport:
            def __init__(self):
                self.handlers = {}

            def event_handler(self, name):
                def register(callback):
                    self.handlers[name] = callback
                    return callback

                return register

        class WebSocket:
            def __init__(self):
                self.client = types.SimpleNamespace(host="device")
                self.send = AsyncMock()
                self.close = AsyncMock()

        clear_started = asyncio.Event()
        clear_release = asyncio.Event()

        async def clear_device_input(reason, generation=None):
            self.assertEqual(reason, "device reconnect")
            self.assertIsNone(generation)
            clear_started.set()
            await clear_release.wait()

        transport = EventTransport()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        handler._clear_device_input = clear_device_input
        handler.setup_event_handlers(transport, AsyncMock())
        websocket = WebSocket()

        await transport.handlers["on_client_connected"](transport, websocket)
        hello = json.loads(cast(Any, websocket.send.await_args).args[0])
        hello_send_count = websocket.send.await_count
        ack_task = asyncio.create_task(
            handler._handle_hello_ack(
                {**hello, "type": "hello_ack", "accepted": True},
                websocket,
            )
        )
        await asyncio.wait_for(clear_started.wait(), timeout=0.1)

        self.assertFalse(ack_task.done())
        self.assertTrue(handler._input_clear_fail_closed)
        self.assertFalse(await handler._authorize_output_audio())
        await handler.broadcast_bytes(b"blocked")
        self.assertEqual(websocket.send.await_count, hello_send_count)
        wake_task = asyncio.create_task(
            handler._handle_device_control_message(
                {
                    "type": "wake",
                    "session_nonce": hello["nonce"],
                    "wake_generation": 1,
                },
                websocket,
            )
        )
        await asyncio.sleep(0)
        self.assertFalse(wake_task.done())

        clear_release.set()
        await ack_task
        self.assertFalse(handler._input_clear_fail_closed)
        self.assertTrue(await wake_task)

    def test_session_nonces_and_follow_up_tokens_are_not_reused(self):
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        with patch.object(
            websocket_handler.secrets,
            "randbits",
            side_effect=(101, 101, 102, 201, 201, 202),
        ):
            self.assertEqual(
                handler._new_protocol_id(handler._issued_hello_nonces),
                101,
            )
            self.assertEqual(
                handler._new_protocol_id(handler._issued_hello_nonces),
                102,
            )
            self.assertEqual(
                handler._new_protocol_id(
                    handler._issued_request_follow_up_tokens
                ),
                201,
            )
            self.assertEqual(
                handler._new_protocol_id(
                    handler._issued_request_follow_up_tokens
                ),
                202,
            )

        with patch.object(
            websocket_handler.secrets,
            "randbits",
            side_effect=(301, 302),
        ):
            self.assertEqual(
                handler._new_protocol_id(
                    handler._issued_request_follow_up_tokens,
                    forbidden=frozenset({301}),
                ),
                302,
            )

    async def test_hello_nonce_cannot_reuse_any_follow_up_token(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        handler._issued_request_follow_up_tokens.add(401)
        admitted = AsyncMock()
        with patch.object(
            websocket_handler.secrets,
            "randbits",
            side_effect=(401, 402),
        ):
            self.assertTrue(await handler._start_hello(websocket, "device", admitted))
        self.assertEqual(cast(Any, handler._hello_transaction).nonce, 402)
        handler._clear_hello_transaction()

    async def test_follow_up_token_cannot_reuse_any_hello_nonce(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        handler._issued_hello_nonces.add(501)
        with patch.object(
            websocket_handler.secrets,
            "randbits",
            side_effect=(501, 502),
        ):
            self.assertEqual(
                await self._reserve(handler),
                websocket_handler.FollowUpReservationOutcome.RESERVED,
            )
        self.assertEqual(cast(Any, handler._request_follow_up_reservation).token, 502)
        handler.cancel_request_follow_up(send_cancel=False)

    async def test_transport_without_owner_contract_rejects_challenger(self):
        events = []

        class EventTransport:
            def __init__(self):
                self.handlers = {}

            def event_handler(self, name):
                def register(callback):
                    self.handlers[name] = callback
                    return callback

                return register

        class WebSocket:
            def __init__(self, host):
                self.client = types.SimpleNamespace(host=host)
                self.messages = []

            async def send(self, message):
                events.append(f"send:{self.client.host}")
                self.messages.append(message)

            async def close(self):
                events.append(f"close:{self.client.host}")

        transport = EventTransport()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        handler._clear_device_input = AsyncMock()
        handler.setup_event_handlers(transport, AsyncMock())
        old = WebSocket("old")
        new = WebSocket("new")

        await transport.handlers["on_client_connected"](transport, old)
        old_hello = json.loads(old.messages[0])
        await handler._handle_hello_ack(
            {**old_hello, "type": "hello_ack", "accepted": True},
            old,
        )
        events.clear()

        await transport.handlers["on_client_connected"](transport, new)
        self.assertEqual(events, ["close:new"])
        self.assertEqual(handler._websockets, {old})
        self.assertIsNone(handler._hello_transaction)

    async def test_fallback_transport_never_closes_owner_for_challenger(self):
        class EventTransport:
            def __init__(self):
                self.handlers = {}

            def event_handler(self, name):
                def register(callback):
                    self.handlers[name] = callback
                    return callback

                return register

        class WebSocket:
            def __init__(self, host, close_error=None):
                self.client = types.SimpleNamespace(host=host)
                self.send = AsyncMock()
                self.close = AsyncMock(side_effect=close_error)

        transport = EventTransport()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        handler._clear_device_input = AsyncMock()
        handler.setup_event_handlers(transport, AsyncMock())
        old = WebSocket("old", RuntimeError("close uncertain"))
        new = WebSocket("new")
        later = WebSocket("later")

        await transport.handlers["on_client_connected"](transport, old)
        old_hello = json.loads(cast(Any, old.send.await_args).args[0])
        await handler._handle_hello_ack(
            {**old_hello, "type": "hello_ack", "accepted": True},
            old,
        )

        with self.assertLogs("app.websocket_handler", level="WARNING"):
            await transport.handlers["on_client_connected"](transport, new)
        with self.assertLogs("app.websocket_handler", level="WARNING"):
            await transport.handlers["on_client_connected"](transport, later)

        self.assertEqual(old.close.await_count, 0)
        new.close.assert_awaited_once_with()
        later.close.assert_awaited_once_with()
        self.assertEqual(new.send.await_count, 0)
        self.assertEqual(later.send.await_count, 0)
        self.assertEqual(handler._websockets, {old})
        self.assertIsNotNone(handler._active_session_nonce)
        self.assertEqual(handler._uncertain_retired_sockets, {new, later})

    async def test_second_raw_candidate_cannot_replace_pending_hello(self):
        class EventTransport:
            def __init__(self):
                self.handlers = {}

            def event_handler(self, name):
                def register(callback):
                    self.handlers[name] = callback
                    return callback

                return register

        class WebSocket:
            def __init__(self, host):
                self.client = types.SimpleNamespace(host=host)
                self.send = AsyncMock()
                self.close = AsyncMock()

        transport = EventTransport()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        handler._clear_device_input = AsyncMock()
        connected = AsyncMock()
        handler.setup_event_handlers(transport, connected)
        old = WebSocket("old")
        new = WebSocket("new")

        await transport.handlers["on_client_connected"](transport, old)
        old_hello = json.loads(cast(Any, old.send.await_args).args[0])
        with self.assertLogs("app.websocket_handler", level="WARNING"):
            await transport.handlers["on_client_connected"](transport, new)
        new.close.assert_awaited_once_with()
        new.send.assert_not_awaited()

        await handler._handle_hello_ack(
            {**old_hello, "type": "hello_ack", "accepted": True},
            old,
        )
        self.assertEqual(handler._websockets, {old})
        connected.assert_awaited_once_with("old")

    async def test_wrong_or_rejected_hello_ack_never_admits_socket(self):
        class EventTransport:
            def __init__(self):
                self.handlers = {}

            def event_handler(self, name):
                def register(callback):
                    self.handlers[name] = callback
                    return callback

                return register

        class WebSocket:
            def __init__(self):
                self.client = types.SimpleNamespace(host="stale")
                self.send = AsyncMock()
                self.close = AsyncMock()

        transport = EventTransport()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        connected = AsyncMock()
        handler.setup_event_handlers(transport, connected)
        websocket = WebSocket()

        await transport.handlers["on_client_connected"](transport, websocket)
        hello = json.loads(cast(Any, websocket.send.await_args).args[0])
        self.assertNotIn(websocket, handler._websockets)

        with self.assertLogs("app.websocket_handler", level="WARNING"):
            await handler._handle_device_control_message(
                {
                    **hello,
                    "type": "hello_ack",
                    "nonce": hello["nonce"] + 1,
                    "accepted": True,
                },
                websocket,
            )
        self.assertIsNotNone(handler._hello_transaction)
        websocket.close.assert_not_awaited()

        with self.assertLogs("app.websocket_handler", level="WARNING"):
            await handler._handle_device_control_message(
                {**hello, "type": "hello_ack", "accepted": 1},
                websocket,
            )
        self.assertIsNotNone(handler._hello_transaction)
        websocket.close.assert_not_awaited()

        with self.assertLogs("app.websocket_handler", level="WARNING"):
            await handler._handle_device_control_message(
                {**hello, "type": "hello_ack", "accepted": False},
                websocket,
            )

        self.assertNotIn(websocket, handler._websockets)
        websocket.close.assert_awaited_once_with()
        connected.assert_not_awaited()
        with self.assertRaisesRegex(RuntimeError, "exactly one"):
            await self._reserve(handler)

    async def test_duplicate_old_disconnect_cannot_clear_new_socket_audio_gate(self):
        class EventTransport:
            def __init__(self):
                self.handlers = {}

            def event_handler(self, name):
                def register(callback):
                    self.handlers[name] = callback
                    return callback

                return register

        class WebSocket:
            def __init__(self, host):
                self.client = types.SimpleNamespace(host=host)
                self.send = AsyncMock()
                self.close = AsyncMock()

        transport = EventTransport()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        handler._clear_device_input = AsyncMock()
        audio_admission = Mock()
        cast(Any, handler)._serializer = types.SimpleNamespace(
            set_audio_admitted=audio_admission
        )
        handler.setup_event_handlers(transport, AsyncMock())
        cancel_output = AsyncMock()
        handler._cancel_assistant_output_callback = cancel_output
        old = WebSocket("old")
        new = WebSocket("new")

        await transport.handlers["on_client_connected"](transport, old)
        old_hello = json.loads(cast(Any, old.send.await_args).args[0])
        await handler._handle_hello_ack(
            {**old_hello, "type": "hello_ack", "accepted": True},
            old,
        )
        self.assertTrue(handler.note_device_wake(1))
        await handler.bind_assistant_output_response("old-response", 1)
        await transport.handlers["on_client_disconnected"](transport, old)
        cancel_output.assert_awaited_once_with(
            "old-response",
            1,
        )
        await transport.handlers["on_client_connected"](transport, new)
        new_hello = json.loads(cast(Any, new.send.await_args).args[0])
        await handler._handle_hello_ack(
            {**new_hello, "type": "hello_ack", "accepted": True},
            new,
        )
        self.assertTrue(handler.note_device_wake(2))
        await handler.bind_assistant_output_response("new-response", 2)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)
        cancel_output.reset_mock()

        await transport.handlers["on_client_disconnected"](transport, old)

        self.assertEqual(handler._websockets, {new})
        self.assertIsNotNone(handler._request_follow_up_reservation)
        grant = cast(Any, handler._assistant_output_grant)
        self.assertIsNotNone(grant)
        self.assertEqual(cast(Any, grant).response_id, "new-response")
        cancel_output.assert_not_awaited()
        self.assertTrue(audio_admission.call_args.args[0])

    async def test_missing_hello_ack_times_out_without_admission(self):
        class EventTransport:
            def __init__(self):
                self.handlers = {}

            def event_handler(self, name):
                def register(callback):
                    self.handlers[name] = callback
                    return callback

                return register

        class WebSocket:
            def __init__(self):
                self.client = types.SimpleNamespace(host="timeout")
                self.close = AsyncMock()
                self.send = AsyncMock()

        transport = EventTransport()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        handler.HELLO_ACK_TIMEOUT_S = 0.001
        connected = AsyncMock()
        handler.setup_event_handlers(transport, connected)
        websocket = WebSocket()

        await transport.handlers["on_client_connected"](transport, websocket)
        with self.assertLogs("app.websocket_handler", level="WARNING"):
            await asyncio.sleep(0.01)

        self.assertNotIn(websocket, handler._websockets)
        websocket.close.assert_awaited_once_with()
        connected.assert_not_awaited()

    async def test_cancel_and_cleanup_gather_follow_up_expiry_tasks(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()

        await self._reserve(handler)
        cancelled_task = cast(Any, handler._request_follow_up_expiry_task)
        handler.cancel_request_follow_up()
        await asyncio.gather(cancelled_task, return_exceptions=True)
        await asyncio.sleep(0)

        self.assertTrue(cancelled_task.done())
        self.assertNotIn(cancelled_task, handler._request_follow_up_tasks)

        handler.note_device_wake()
        await self._reserve(handler)
        cleanup_task = cast(Any, handler._request_follow_up_expiry_task)
        await handler.cleanup()

        self.assertTrue(cleanup_task.done())
        self.assertEqual(handler._request_follow_up_tasks, set())

    async def test_cancellation_winning_before_guarded_send_emits_nothing(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "cancel-before-send")

        send_task = asyncio.create_task(handler._before_reply_idle())
        handler.invalidate_request_follow_up_turn()
        await send_task

        websocket.send.assert_not_awaited()

    async def test_cancellation_during_send_revokes_same_token(self):
        class WebSocket:
            def __init__(self):
                self.entered_send = asyncio.Event()
                self.release_send = asyncio.Event()
                self.messages = []
                self.close = AsyncMock()

            async def send(self, message):
                self.messages.append(message)
                self.entered_send.set()
                await self.release_send.wait()

        websocket = WebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "cancel-during-send")

        send_task = asyncio.create_task(handler._before_reply_idle())
        await websocket.entered_send.wait()
        handler.invalidate_request_follow_up_turn()
        websocket.release_send.set()
        while len(websocket.messages) < 2:
            await asyncio.sleep(0)
        cancel = json.loads(websocket.messages[1])
        handler._handle_cancel_request_follow_up_ack(
            {
                "type": "cancel_request_follow_up_ack",
                "token": cancel["token"],
                "session_nonce": cancel["session_nonce"],
                "accepted": True,
                "cleared": True,
            }
        )
        await send_task

        self.assertEqual(len(websocket.messages), 2)
        request = json.loads(websocket.messages[0])
        self.assertEqual(request["type"], "request_follow_up")
        self.assertEqual(cancel["type"], "cancel_request_follow_up")
        self.assertEqual(cancel["token"], request["token"])

    async def test_cancellation_after_ack_revokes_same_token(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "cancel-after-ack")

        ack_task = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        request = await ack_task
        cancel_ack_task = asyncio.create_task(
            self._ack_cancel_requested_follow_up(
                handler,
                websocket,
                accepted=True,
                cleared=True,
            )
        )
        handler.invalidate_request_follow_up_turn()
        cancel = await cancel_ack_task
        await handler._await_request_follow_up_settlements()

        self.assertEqual(cancel["type"], "cancel_request_follow_up")
        self.assertEqual(cancel["token"], request["token"])

    async def test_firmware_revoke_then_fresh_wake_needs_no_cancel_settlement(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "fresh-wake-response")
        request_ack_task = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        request = await request_ack_task

        await handler._handle_device_control_message(
            {
                "type": "client_revoke",
                "session_nonce": handler._active_session_nonce,
                "wake_generation": handler._device_wake_generation,
                "reason": "new_wake",
            },
            websocket,
        )
        self.assertTrue(handler.note_device_wake())
        await handler._await_request_follow_up_settlements()

        self.assertGreater(request["token"], 0)
        self.assertEqual(handler._websockets, {websocket})
        self.assertFalse(handler._request_follow_up_budget_spent)
        self.assertEqual(handler._request_follow_up_cancellations, {})
        self.assertEqual(websocket.send.await_count, 1)
        websocket.close.assert_not_awaited()
        self.assertEqual(
            await self._reserve(handler, "fresh-wake-request"),
            websocket_handler.FollowUpReservationOutcome.RESERVED,
        )
        handler.cancel_request_follow_up(send_cancel=False)

    async def test_client_revoke_settles_exact_current_reservation(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "local-close-response")
        request_ack_task = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        request = await request_ack_task

        await handler._handle_device_control_message(
            {
                "type": "client_revoke",
                "session_nonce": request["session_nonce"],
                "wake_generation": handler._device_wake_generation,
                "reason": "follow_up_timeout",
            },
            websocket,
        )

        self.assertIsNone(handler._request_follow_up_reservation)
        self.assertTrue(handler._request_follow_up_budget_spent)
        self.assertEqual(handler._websockets, {websocket})
        websocket.close.assert_not_awaited()

    async def test_client_revoke_does_not_emit_a_redundant_cancel(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "local-close-cancel-response")
        request_ack_task = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        request = await request_ack_task

        await handler._handle_device_control_message(
            {
                "type": "client_revoke",
                "session_nonce": request["session_nonce"],
                "wake_generation": handler._device_wake_generation,
                "reason": "mute",
            },
            websocket,
        )
        await handler._await_request_follow_up_settlements()

        self.assertEqual(handler._request_follow_up_cancellations, {})
        self.assertEqual(handler._websockets, {websocket})
        self.assertEqual(websocket.send.await_count, 1)
        websocket.close.assert_not_awaited()

    async def test_late_old_cancel_ack_does_not_touch_new_reservation(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler, "request-a")
        self._activate(handler, "request-a")
        self._qualify_response(handler, "response-a", "request-a")
        request_ack_task = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        request_a = await request_ack_task

        handler.invalidate_request_follow_up_turn()
        while websocket.send.await_count < 2:
            await asyncio.sleep(0)
        self.assertTrue(handler.note_device_wake())
        self.assertEqual(
            await self._reserve(handler, "request-b"),
            websocket_handler.FollowUpReservationOutcome.RESERVED,
        )
        reservation_b = handler._request_follow_up_reservation

        handler._handle_cancel_request_follow_up_ack(
            {
                "type": "cancel_request_follow_up_ack",
                "token": request_a["token"],
                "session_nonce": request_a["session_nonce"],
                "accepted": True,
                "cleared": True,
            }
        )
        await handler._await_request_follow_up_settlements()

        self.assertIs(handler._request_follow_up_reservation, reservation_b)
        self.assertEqual(handler._websockets, {websocket})
        websocket.close.assert_not_awaited()
        handler.cancel_request_follow_up(send_cancel=False)

    async def test_late_old_ready_does_not_touch_new_reservation(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler, "request-a")
        self._activate(handler, "request-a")
        self._qualify_response(handler, "response-a", "request-a")
        request_ack_task = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        request_a = await request_ack_task
        await handler._handle_device_control_message(
            {
                "type": "client_revoke",
                "session_nonce": request_a["session_nonce"],
                "wake_generation": handler._device_wake_generation,
                "reason": "new_wake",
            },
            websocket,
        )

        self.assertTrue(handler.note_device_wake())
        self.assertEqual(
            await self._reserve(handler, "request-b"),
            websocket_handler.FollowUpReservationOutcome.RESERVED,
        )
        reservation_b = handler._request_follow_up_reservation
        with self.assertLogs("app.websocket_handler", level="WARNING"):
            await handler._handle_device_control_message(
                {
                    "type": "follow_up_ready",
                    "token": request_a["token"],
                    "session_nonce": request_a["session_nonce"],
                    "ready_nonce": 987654320,
                },
                websocket,
            )

        self.assertIs(handler._request_follow_up_reservation, reservation_b)
        self.assertEqual(handler._websockets, {websocket})
        handler.cancel_request_follow_up(send_cancel=False)

    async def test_nonce_and_generation_bound_mute_revoke_closes_window(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        session_nonce = self._admit(handler, websocket)
        handler.note_device_wake()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "mute-response")
        request_ack_task = asyncio.create_task(
            self._ack_requested_follow_up(handler, websocket)
        )
        await handler._before_reply_idle()
        request = await request_ack_task

        await handler._handle_device_control_message(
            {
                "type": "client_revoke",
                "session_nonce": session_nonce,
                "wake_generation": handler._device_wake_generation,
                "reason": "mute",
            },
            websocket,
        )

        self.assertGreater(request["token"], 0)
        self.assertIsNone(handler._request_follow_up_reservation)
        self.assertTrue(handler._request_follow_up_budget_spent)
        self.assertEqual(websocket.send.await_count, 1)
        websocket.close.assert_not_awaited()

    async def test_wake_speech_stop_recovery_and_cleanup_cancel_reservation(self):
        class Serializer:
            def __init__(self):
                self.callbacks = {}

            def set_interrupt_handler(self, callback):
                self.callbacks["interrupt"] = callback

            def set_session_start_handler(self, callback):
                self.callbacks["start"] = callback

            def set_mic_flush_handler(self, callback):
                self.callbacks["flush"] = callback

            def set_wake_handler(self, callback):
                self.callbacks["wake"] = callback

            def set_button_cancel_handler(self, callback):
                self.callbacks["button"] = callback

            def set_first_audio_handler(self, callback):
                self.callbacks["first_audio"] = callback

        class CapturingPhaseEmitter:
            instance = None

            def __init__(self, *args, **kwargs):
                type(self).instance = self
                self.before_idle = kwargs["before_idle"]
                self.on_bot_started = kwargs.get("on_bot_started")
                self.on_real_speech = None
                self.last_vad_mono = websocket_handler.time.monotonic()

            def set_kill_window_handlers(self, **kwargs):
                self.on_real_speech = kwargs["on_real_speech"]

            def note_wake(self):
                pass

            async def force_idle(self, *args, **kwargs):
                pass

        class InputAudioBufferClearEvent:
            pass

        class ResponseCancelEvent:
            event_id = "cancel-event"

        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        serializer = Serializer()
        handler_any = cast(Any, handler)
        handler_any._serializer = serializer
        service = cast(Any, _FakeOpenAIService())
        service.send_client_event = AsyncMock()

        with (
            patch.object(websocket_handler, "PhaseEmitter", CapturingPhaseEmitter),
            patch.object(websocket_handler, "InputResampler", _Placeholder),
            patch.object(websocket_handler, "SessionActivityTracker", _Placeholder),
            patch.object(websocket_handler, "TranscriptLogger", _Placeholder),
            patch.object(websocket_handler, "Pipeline", _Placeholder),
            patch.object(websocket_handler, "PipelineRunner", _Placeholder),
            patch.object(websocket_handler, "PipelineTask", _Placeholder),
            patch.object(
                websocket_handler.openai_rt_events,
                "InputAudioBufferClearEvent",
                InputAudioBufferClearEvent,
                create=True,
            ),
            patch.object(
                websocket_handler.openai_rt_events,
                "ResponseCancelEvent",
                ResponseCancelEvent,
                create=True,
            ),
        ):
            handler.build_pipeline(_FakeTransport(), service, "server")
            phase = cast(Any, CapturingPhaseEmitter.instance)
            self.assertTrue(callable(phase.on_bot_started))
            self.assertEqual(
                set(service.request_follow_up_callbacks),
                {
                    "on_response_created",
                    "on_response_audio",
                    "on_response_done",
                    "on_response_failed",
                    "on_continuation_arm",
                    "on_continuation_failed",
                },
            )

            await serializer.callbacks["start"]()
            await serializer.callbacks["flush"]()
            self.assertEqual(
                service.authoritative_input_clear_generations,
                [-1, -2],
            )

            handler.note_request_follow_up_turn_boundary()
            await self._reserve(handler)
            wake_generation = handler._device_wake_generation + 1
            self.assertTrue(
                await handler._handle_device_control_message(
                    {
                        "type": "wake",
                        "session_nonce": handler._active_session_nonce,
                        "wake_generation": wake_generation,
                    },
                    websocket,
                )
            )
            await serializer.callbacks["wake"]()
            self.assertIsNone(handler._request_follow_up_reservation)
            self.assertEqual(
                handler._user_turn_non_close_generation,
                websocket_handler.TURN_LIVENESS.non_close_tool_generation,
            )

            await self._reserve(handler)
            phase.on_real_speech()
            self.assertIsNotNone(handler._request_follow_up_reservation)
            self.assertEqual(
                handler._user_turn_non_close_generation,
                websocket_handler.TURN_LIVENESS.non_close_tool_generation,
            )

            handler.cancel_request_follow_up()
            await self._reserve(handler)
            self.assertTrue(
                await handler._handle_device_control_message(
                    {
                        "type": "interrupt",
                        "session_nonce": handler._active_session_nonce,
                        "wake_generation": handler._device_wake_generation,
                        "reason": "stop",
                    },
                    websocket,
                )
            )
            await serializer.callbacks["interrupt"]()
            self.assertIsNone(handler._request_follow_up_reservation)
            self.assertIsNone(handler._user_turn_non_close_generation)
            self.assertEqual(
                service.authoritative_input_clear_generations,
                [-1, -2, 0],
            )

            handler.note_request_follow_up_turn_boundary()
            await self._reserve(handler)
            cast(Any, handler._connection_recovery)._notify_recovery_started()
            self.assertIsNone(handler._request_follow_up_reservation)
            self.assertIsNone(handler._user_turn_non_close_generation)

            handler.note_request_follow_up_turn_boundary()
            await self._reserve(handler)
            await handler.cleanup()
            self.assertIsNone(handler._request_follow_up_reservation)

    async def test_reconnect_and_flush_clear_failures_retire_until_recovery(self):
        class Serializer:
            def __init__(self):
                self.callbacks = {}

            def set_interrupt_handler(self, callback):
                self.callbacks["interrupt"] = callback

            def set_session_start_handler(self, callback):
                self.callbacks["start"] = callback

            def set_mic_flush_handler(self, callback):
                self.callbacks["flush"] = callback

            def set_wake_handler(self, callback):
                self.callbacks["wake"] = callback

            def set_button_cancel_handler(self, callback):
                self.callbacks["button"] = callback

            def set_first_audio_handler(self, callback):
                self.callbacks["first_audio"] = callback

            def set_audio_admitted(self, _admitted):
                pass

        class Recovery:
            instance = None

            def __init__(self, *args, **kwargs):
                type(self).instance = self
                self.complete_callback = None

            def set_recovery_complete_callback(self, callback):
                self.complete_callback = callback

            async def cleanup(self):
                pass

        async def exercise(callback_name, recovery_before_failure=False):
            websocket = _FakeDeviceWebSocket()
            handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
            session_nonce = self._admit(handler, websocket)
            self.assertTrue(handler.note_device_wake(1))
            serializer = Serializer()
            cast(Any, handler)._serializer = serializer
            retire_client = AsyncMock(return_value=True)
            handler.transport = types.SimpleNamespace(retire_client=retire_client)
            service = cast(Any, _FakeOpenAIService())

            async def fail_clear(_generation):
                if recovery_before_failure:
                    recovery = cast(Any, Recovery.instance)
                    self.assertIsNotNone(recovery.complete_callback)
                    recovery.complete_callback()
                    self.assertTrue(handler._input_clear_fail_closed)
                    self.assertFalse(handler._input_clear_settled.is_set())
                raise TimeoutError("clear receipt missing")

            service.clear_input_audio_buffer_authoritatively = AsyncMock(
                side_effect=fail_clear
            )

            with (
                patch.object(websocket_handler, "ConnectionRecovery", Recovery),
                patch.object(websocket_handler, "InputResampler", _Placeholder),
                patch.object(websocket_handler, "SessionActivityTracker", _Placeholder),
                patch.object(websocket_handler, "TranscriptLogger", _Placeholder),
                patch.object(websocket_handler, "PhaseEmitter", _FakePhaseEmitter),
                patch.object(websocket_handler, "Pipeline", _Placeholder),
                patch.object(websocket_handler, "PipelineRunner", _Placeholder),
                patch.object(websocket_handler, "PipelineTask", _Placeholder),
            ):
                handler.build_pipeline(_FakeTransport(), service, "server")
                with self.assertRaisesRegex(TimeoutError, "clear receipt missing"):
                    await serializer.callbacks[callback_name]()

            service.clear_input_audio_buffer_authoritatively.assert_awaited_once_with(-1)
            self.assertNotIn(websocket, handler._websockets)
            retire_client.assert_awaited_once_with(websocket)
            self.assertFalse(
                await handler._handle_device_control_message(
                    {
                        "type": "wake",
                        "session_nonce": session_nonce,
                        "wake_generation": 2,
                    },
                    websocket,
                )
            )
            self.assertEqual(handler._device_wake_generation, 1)
            recovery = cast(Any, Recovery.instance)
            self.assertIsNotNone(recovery.complete_callback)
            if recovery_before_failure:
                self.assertFalse(handler._input_clear_fail_closed)
            else:
                self.assertTrue(handler._input_clear_fail_closed)
                recovery.complete_callback()
            self.assertFalse(handler._input_clear_fail_closed)

        await exercise("start")
        await exercise("flush", recovery_before_failure=True)

    async def test_other_tool_start_cancels_requested_follow_up(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)

        service = main.SafeRealtimeLLMService()

        async def other_tool(params):
            await params.result_callback({"ok": True})

        service.register_function("other_tool", other_tool)
        _, wrapped_handler = service._registered_function
        params = types.SimpleNamespace(
            tool_call_id="other-call",
            arguments={},
            result_callback=AsyncMock(),
        )
        service._tool_call_generations["other-call"] = 0
        original_callback = main.NON_CLOSE_TOOL_CALLBACK
        original_generation = main.TURN_LIVENESS.non_close_tool_generation
        main.NON_CLOSE_TOOL_CALLBACK = handler.cancel_deferred_conversation_controls
        try:
            await wrapped_handler(params)
        finally:
            main.NON_CLOSE_TOOL_CALLBACK = original_callback
            main.TURN_LIVENESS.non_close_tool_generation = original_generation

        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_speaker_gate_rejection_still_invalidates_follow_up(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)

        service = main.SafeRealtimeLLMService()
        blocked_handler = AsyncMock()
        service.register_function("restricted_tool", blocked_handler)
        _, wrapped_handler = service._registered_function
        in_flight_during_gate = []

        async def result_callback(_result, **_kwargs):
            in_flight_during_gate.append(main.TURN_LIVENESS.in_flight)
        params = types.SimpleNamespace(
            tool_call_id="restricted-call",
            arguments={},
            result_callback=result_callback,
        )
        service._tool_call_generations["restricted-call"] = 0
        original_callback = main.NON_CLOSE_TOOL_CALLBACK
        original_generation = main.TURN_LIVENESS.non_close_tool_generation
        original_tools = main.MALE_ONLY_TOOLS
        original_probe = main.SPEAKER_PROBE
        main.NON_CLOSE_TOOL_CALLBACK = handler.cancel_deferred_conversation_controls
        main.MALE_ONLY_TOOLS = {"restricted_tool"}
        main.SPEAKER_PROBE = types.SimpleNamespace(
            gate_speaker=lambda: "unknown",
            male_name="owner",
        )
        try:
            await wrapped_handler(params)
        finally:
            main.NON_CLOSE_TOOL_CALLBACK = original_callback
            main.MALE_ONLY_TOOLS = original_tools
            main.SPEAKER_PROBE = original_probe
            main.TURN_LIVENESS.non_close_tool_generation = original_generation

        blocked_handler.assert_not_awaited()
        self.assertEqual(in_flight_during_gate, [1])
        self.assertEqual(main.TURN_LIVENESS.in_flight, 0)
        self.assertIsNone(handler._request_follow_up_reservation)

    async def test_speaker_gate_exception_balances_tool_liveness(self):
        service = main.SafeRealtimeLLMService()
        blocked_handler = AsyncMock()
        service.register_function("restricted_tool", blocked_handler)
        _, wrapped_handler = service._registered_function
        params = types.SimpleNamespace(
            tool_call_id="restricted-call",
            arguments={},
            result_callback=AsyncMock(),
        )
        service._tool_call_generations["restricted-call"] = 0
        original_tools = main.MALE_ONLY_TOOLS
        original_probe = main.SPEAKER_PROBE
        main.MALE_ONLY_TOOLS = {"restricted_tool"}

        def fail_gate():
            raise RuntimeError("speaker probe failed")

        main.SPEAKER_PROBE = types.SimpleNamespace(gate_speaker=fail_gate)
        try:
            with self.assertRaisesRegex(RuntimeError, "speaker probe failed"):
                await wrapped_handler(params)
        finally:
            main.MALE_ONLY_TOOLS = original_tools
            main.SPEAKER_PROBE = original_probe

        blocked_handler.assert_not_awaited()
        self.assertEqual(main.TURN_LIVENESS.in_flight, 0)

    async def test_request_follow_up_control_preserves_its_own_generation(self):
        service = main.SafeRealtimeLLMService()

        async def request_control(params):
            await params.result_callback({"reserved": True})

        service.register_function(main.REQUEST_FOLLOW_UP_TOOL_NAME, request_control)
        _, wrapped_handler = service._registered_function
        params = types.SimpleNamespace(
            tool_call_id="follow-up-call",
            arguments={},
            result_callback=AsyncMock(),
        )
        service._tool_call_generations["follow-up-call"] = 0
        original_generation = main.TURN_LIVENESS.non_close_tool_generation
        original_callback = main.NON_CLOSE_TOOL_CALLBACK
        callback = AsyncMock()
        main.NON_CLOSE_TOOL_CALLBACK = callback
        try:
            await wrapped_handler(params)
        finally:
            main.NON_CLOSE_TOOL_CALLBACK = original_callback

        self.assertEqual(
            main.TURN_LIVENESS.non_close_tool_generation,
            original_generation,
        )
        callback.assert_not_awaited()

    def test_follow_up_safety_requires_only_its_exact_tool_call(self):
        service = main.SafeRealtimeLLMService()
        original_in_flight = main.TURN_LIVENESS.in_flight
        try:
            main.TURN_LIVENESS.in_flight = 1
            service._running_tool_call_ids = {"follow-up-call"}
            self.assertTrue(
                service.request_follow_up_is_sole_tool("follow-up-call")
            )

            service._scheduled_tool_call_ids.add("other-call")
            self.assertFalse(
                service.request_follow_up_is_sole_tool("follow-up-call")
            )
            service._scheduled_tool_call_ids.clear()
            service._pending_tool_result_ids.add("other-call")
            self.assertFalse(
                service.request_follow_up_is_sole_tool("follow-up-call")
            )
            service._pending_tool_result_ids.clear()
            service._running_tool_call_ids.add("other-call")
            main.TURN_LIVENESS.in_flight = 2
            self.assertFalse(
                service.request_follow_up_is_sole_tool("follow-up-call")
            )
        finally:
            main.TURN_LIVENESS.in_flight = original_in_flight

    def test_zero_mode_exposes_and_registers_both_native_controls(self):
        application = cast(Any, main.Application())
        application.follow_up_ms = 0
        application.request_follow_up_supported = True
        application.websocket_handler = types.SimpleNamespace(
            reserve_request_follow_up=AsyncMock(),
            activate_request_follow_up=Mock(return_value=True),
            cancel_request_follow_up=Mock(),
            request_silent_close=AsyncMock(),
            silent_close_is_allowed=Mock(return_value=True),
        )
        functions = {
            main.REQUEST_FOLLOW_UP_TOOL_NAME: object(),
            main.END_CONVERSATION_TOOL_NAME: object(),
        }

        def register_function(name, handler):
            functions[name] = handler

        application.openai_service = types.SimpleNamespace(
            _functions=functions,
            register_function=register_function,
            request_follow_up_is_sole_tool=Mock(return_value=True),
        )

        definition = application._get_conversation_control_tool_definition()
        application._register_conversation_control_tool()

        self.assertEqual(definition["name"], main.REQUEST_FOLLOW_UP_TOOL_NAME)
        self.assertEqual(
            set(functions),
            {
                main.REQUEST_FOLLOW_UP_TOOL_NAME,
                main.END_CONVERSATION_TOOL_NAME,
            },
        )

    def test_closed_default_is_exported_through_addon_configuration(self):
        config = (ADDON_ROOT / "config.yaml").read_text(encoding="utf-8")
        run_script = (ADDON_ROOT / "root" / "run.sh").read_text(
            encoding="utf-8"
        )
        main_source = (ADDON_ROOT / "app" / "main.py").read_text(
            encoding="utf-8"
        )

        self.assertIn("follow_up_listen_seconds: 0", config)
        self.assertIn(
            "FOLLOW_UP_LISTEN_SECONDS=$(bashio::config "
            "'follow_up_listen_seconds')",
            run_script,
        )
        self.assertIn("export FOLLOW_UP_LISTEN_SECONDS", run_script)
        self.assertIn(
            'if [ "$FOLLOW_UP_LISTEN_SECONDS" != "0" ]; then',
            run_script,
        )
        self.assertIn('nearby_media_players: ""', config)
        self.assertIn('nearby_media_power_entity: ""', config)
        self.assertIn("nearby_media_power_entity: str?", config)
        self.assertIn("max_output_tokens: 1200", config)
        self.assertIn("enable_voice_memory: false", config)
        self.assertIn(
            "conversational turn would usefully continue",
            config,
        )
        self.assertIn(
            "this again after each genuine answer",
            config,
        )
        self.assertIn(
            "call end_conversation as the sole tool and say nothing before or after it",
            config,
        )
        self.assertNotIn("Ask no optional", config)
        self.assertIn(
            "NEARBY_MEDIA_PLAYERS=$(bashio::config 'nearby_media_players')",
            run_script,
        )
        self.assertIn("export NEARBY_MEDIA_PLAYERS", run_script)
        self.assertIn('NEARBY_MEDIA_POWER_ENTITY=""', run_script)
        self.assertIn(
            "if bashio::config.has_value 'nearby_media_power_entity'; then",
            run_script,
        )
        self.assertIn(
            "NEARBY_MEDIA_POWER_ENTITY=$(bashio::config "
            "'nearby_media_power_entity')",
            run_script,
        )
        self.assertIn("export NEARBY_MEDIA_POWER_ENTITY", run_script)
        self.assertIn(
            'os.environ.get("NEARBY_MEDIA_POWER_ENTITY", "")',
            main_source,
        )
        self.assertIn(
            "power_entity_id=nearby_media_power_entity",
            main_source,
        )
        self.assertIn(
            "ENABLE_VOICE_MEMORY=$(bashio::config 'enable_voice_memory')",
            run_script,
        )
        self.assertIn("export ENABLE_VOICE_MEMORY", run_script)
        self.assertIn('if [ -z "$NEARBY_MEDIA_PLAYERS" ]; then', run_script)

    def test_rapid_pilot_policy_suffix_preserves_saved_instructions(self):
        saved = "Keep my exact household style."
        combined = main.append_rapid_pilot_policy(saved)

        self.assertTrue(combined.startswith(saved + "\n\n"))
        self.assertIn(main.RAPID_PILOT_POLICY_MARKER, combined)
        self.assertIn("MUST first call request_follow_up", combined)
        self.assertIn("only that function call", combined)
        self.assertIn("first question in a user-requested multi-question", combined)
        self.assertIn("never ask that first question directly", combined)
        self.assertIn("repeat the\ntool-only call before asking the next question", combined)
        self.assertIn("end_conversation as the sole tool", combined)
        self.assertIn("produce no spoken reply", combined)
        self.assertIn("Never claim that the microphone is open", combined)
        self.assertEqual(main.append_rapid_pilot_policy(combined), combined)
        incomplete_marker = f"{saved}\n\n{main.RAPID_PILOT_POLICY_MARKER}"
        self.assertTrue(
            main.append_rapid_pilot_policy(incomplete_marker).endswith(
                main.RAPID_PILOT_POLICY_SUFFIX
            )
        )

    def test_rapid_pilot_rejects_nonzero_or_malformed_saved_mode(self):
        self.assertEqual(main.parse_rapid_pilot_follow_up_seconds("0"), 0)
        for value in ("8", 1, -1, 0.5, False, "0.0", "automatic"):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "must be 0"):
                    main.parse_rapid_pilot_follow_up_seconds(value)

    async def test_application_startup_rejects_saved_nonzero_mode_before_io(self):
        application = main.Application()
        with patch.dict(
            main.os.environ,
            {
                "FOLLOW_UP_LISTEN_SECONDS": "8",
                "OPENAI_API_KEY": "not-used-before-mode-check",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "legacy automatic"):
                await application.initialize()

    async def test_application_startup_rejects_empty_media_scope_before_io(self):
        application = main.Application()
        with patch.dict(
            main.os.environ,
            {
                "FOLLOW_UP_LISTEN_SECONDS": "0",
                "OPENAI_API_KEY": "not-used-before-media-check",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "nearby_media_players"):
                await application.initialize()

    async def test_application_startup_rejects_invalid_power_entity_before_io(self):
        application = main.Application()
        with patch.dict(
            main.os.environ,
            {
                "FOLLOW_UP_LISTEN_SECONDS": "0",
                "NEARBY_MEDIA_PLAYERS": "media_player.living_room_tv",
                "NEARBY_MEDIA_POWER_ENTITY": "switch.Living_Room",
                "OPENAI_API_KEY": "not-used-before-power-check",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "nearby_media_power_entity"):
                await application.initialize()

    def test_mcp_allowlist_is_exact_and_empty_fails_closed(self):
        allowlist = main.parse_mcp_tool_allowlist(
            " HassTurnOn,GetLiveContext,HassTurnOn "
        )
        self.assertEqual(allowlist, {"HassTurnOn", "GetLiveContext"})
        self.assertFalse(
            main.mcp_tool_is_explicitly_allowed(
                "HassTurnOn",
                frozenset(),
                direct_openclaw_enabled=False,
            )
        )
        self.assertTrue(
            main.mcp_tool_is_explicitly_allowed(
                "HassTurnOn",
                allowlist,
                direct_openclaw_enabled=False,
            )
        )
        self.assertFalse(
            main.mcp_tool_is_explicitly_allowed(
                "HassTurnOn",
                allowlist,
                direct_openclaw_enabled=False,
                native_tool_names=frozenset({"HassTurnOn"}),
            )
        )
        self.assertFalse(
            main.mcp_tool_is_explicitly_allowed(
                "hassturnon",
                allowlist,
                direct_openclaw_enabled=False,
            )
        )
        for blocked in (
            "voice_enrollment",
            "remember",
            "request_follow_up",
            "end_conversation",
        ):
            self.assertFalse(
                main.mcp_tool_is_explicitly_allowed(
                    blocked,
                    frozenset({blocked}),
                    direct_openclaw_enabled=False,
                )
            )

    def test_persistent_memory_schema_and_context_are_opt_in(self):
        application = main.Application()
        with (
            patch.object(main, "get_memory_tool_definitions", return_value=[{"name": "remember"}]) as definitions,
            patch.object(main, "memory_instructions", return_value="secret memory") as instructions,
        ):
            self.assertEqual(application._get_memory_tool_definitions(), [])
            self.assertEqual(application._get_memory_instructions(), "")
            definitions.assert_not_called()
            instructions.assert_not_called()

            application.enable_voice_memory = True
            self.assertEqual(
                application._get_memory_tool_definitions(),
                [{"name": "remember"}],
            )
            self.assertEqual(application._get_memory_instructions(), "secret memory")

    def test_rapid_pilot_prerequisites_match_tool_exposure(self):
        main.validate_rapid_pilot_prerequisites("semantic_vad", True, 12)
        for values in (
            ("server_vad", True, 12),
            ("semantic_vad", False, 12),
            ("semantic_vad", True, 0),
        ):
            with self.subTest(values=values):
                with self.assertRaises(ValueError):
                    main.validate_rapid_pilot_prerequisites(*values)
    def test_server_owned_zero_mode_exposes_no_request_follow_up(self):
        application = cast(Any, main.Application())
        application.follow_up_ms = 0
        application.request_follow_up_supported = False
        application.websocket_handler = types.SimpleNamespace()
        functions = {
            main.REQUEST_FOLLOW_UP_TOOL_NAME: object(),
            main.END_CONVERSATION_TOOL_NAME: object(),
        }
        application.openai_service = types.SimpleNamespace(_functions=functions)

        self.assertIsNone(
            application._get_conversation_control_tool_definition()
        )
        application._register_conversation_control_tool()

        self.assertEqual(functions, {})

    def test_nonzero_legacy_mode_cannot_expose_a_conversation_control(self):
        application = cast(Any, main.Application())
        application.follow_up_ms = 8000
        application.websocket_handler = types.SimpleNamespace(
            reserve_request_follow_up=AsyncMock(),
            activate_request_follow_up=Mock(return_value=True),
            cancel_request_follow_up=Mock(),
            request_silent_close=AsyncMock(),
        )
        functions = {
            main.REQUEST_FOLLOW_UP_TOOL_NAME: object(),
            main.END_CONVERSATION_TOOL_NAME: object(),
        }

        def register_function(name, handler):
            functions[name] = handler

        application.openai_service = types.SimpleNamespace(
            _functions=functions,
            register_function=register_function,
        )

        definition = application._get_conversation_control_tool_definition()
        application._register_conversation_control_tool()

        self.assertIsNone(definition)
        self.assertEqual(functions, {})

    async def test_control_broadcast_is_compact_and_keeps_socket_open(self):
        class WebSocket:
            def __init__(self):
                self.send = AsyncMock()
                self.close = AsyncMock()

        websocket = cast(Any, WebSocket())
        handler = websocket_handler.WebSocketHandler()
        self._admit(handler, websocket)
        handler.note_device_wake()

        async def acknowledge():
            while websocket.send.await_count == 0:
                await asyncio.sleep(0)
            prepared = json.loads(websocket.send.await_args_list[0].args[0])
            handler._handle_graceful_close_ack(
                self._graceful_ack(handler, prepared, "prepared", True)
            )
            while websocket.send.await_count < 2:
                await asyncio.sleep(0)
            committed = json.loads(websocket.send.await_args_list[1].args[0])
            handler._handle_graceful_close_ack(
                self._graceful_ack(handler, committed, "committed", True)
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

    async def test_silent_close_commits_idle_and_blocks_all_assistant_pcm(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        session_nonce = self._admit(handler, websocket)
        handler.note_device_wake()
        await self._open_and_confirm_answer(handler, websocket)
        wake_generation = handler._device_wake_generation
        self.assertTrue(handler.silent_close_is_allowed())
        self.assertTrue(
            await handler.bind_assistant_output_response("close-response", 1)
        )
        queued_listening = handler.capture_phase_authorization_context()

        close_task = asyncio.create_task(handler.request_silent_close())

        async def acknowledge(message_type, stage):
            payload = None
            while payload is None:
                for call in reversed(websocket.send.await_args_list):
                    candidate = json.loads(call.args[0])
                    if candidate.get("type") == message_type:
                        payload = candidate
                        break
                if payload is None:
                    await asyncio.sleep(0)
            await handler._handle_device_control_message(
                self._graceful_ack(handler, payload, stage, True),
                websocket,
            )

        await acknowledge("prepare_suppress_followup", "prepared")
        await acknowledge("commit_suppress_followup", "committed")
        await close_task

        controls = [
            json.loads(call.args[0]) for call in websocket.send.await_args_list
        ]
        self.assertEqual(
            [control["type"] for control in controls[-3:]],
            ["prepare_suppress_followup", "commit_suppress_followup", "phase"],
        )
        self.assertEqual(controls[-1]["value"], "idle")
        self.assertEqual(controls[-1]["session_nonce"], session_nonce)
        self.assertEqual(controls[-1]["wake_generation"], wake_generation)
        self.assertNotIn("token", controls[-1])
        self.assertTrue(handler._request_follow_up_budget_spent)
        self.assertIsNone(handler._request_follow_up_answer_grant)
        self.assertFalse(handler.silent_close_is_allowed())
        self.assertFalse(
            await handler._authorize_output_audio(
                ("close-response", 1),
                websocket,
            )
        )
        self.assertFalse(
            await handler.bind_assistant_output_response("forbidden-response", 2)
        )
        sent_before_stale_phase = websocket.send.await_count
        self.assertFalse(
            await handler.broadcast_phase("listening", queued_listening)
        )
        self.assertEqual(websocket.send.await_count, sent_before_stale_phase)
        self.assertIsNone(handler._device_audio_generation)
        websocket.close.assert_not_awaited()

        self.assertTrue(handler.note_device_wake(wake_generation + 1))
        self.assertFalse(handler._silent_close_is_current())
        self.assertTrue(
            await handler.bind_assistant_output_response("fresh-response", 3)
        )

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
        self._admit(handler, websocket)
        handler.note_device_wake()

        async def acknowledge_after_delay():
            while websocket.send.await_count == 0:
                await asyncio.sleep(0)
            prepared = json.loads(websocket.send.await_args_list[0].args[0])
            await asyncio.sleep(1.05)
            handler._handle_graceful_close_ack(
                self._graceful_ack(handler, prepared, "prepared", True)
            )
            while websocket.send.await_count < 2:
                await asyncio.sleep(0)
            committed = json.loads(websocket.send.await_args_list[1].args[0])
            handler._handle_graceful_close_ack(
                self._graceful_ack(handler, committed, "committed", True)
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
        self._admit(handler, websocket)
        handler.note_device_wake()

        async def reject():
            while websocket.send.await_count == 0:
                await asyncio.sleep(0)
            prepared = json.loads(websocket.send.await_args.args[0])
            handler._handle_graceful_close_ack(
                self._graceful_ack(handler, prepared, "prepared", False)
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
        self._admit(handler, websocket)
        handler.note_device_wake()
        original_generation = websocket_handler.TURN_LIVENESS.non_close_tool_generation

        async def acknowledge_then_start_tool():
            while websocket.send.await_count == 0:
                await asyncio.sleep(0)
            prepared = json.loads(websocket.send.await_args_list[0].args[0])
            handler._handle_graceful_close_ack(
                self._graceful_ack(handler, prepared, "prepared", True)
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
        self._admit(handler, websocket)
        handler.note_device_wake()

        async def acknowledge_prepare_only():
            while websocket.send.await_count == 0:
                await asyncio.sleep(0)
            prepared = json.loads(websocket.send.await_args_list[0].args[0])
            handler._handle_graceful_close_ack(
                self._graceful_ack(handler, prepared, "prepared", True)
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

    async def test_silent_close_waits_for_exact_terminal_ledger_and_discards_pcm(self):
        service, pushed = self._prepare_decision_service()
        call_id = "silent-close-call"
        call_item = {
            "id": "silent-close-item",
            "type": "function_call",
            "call_id": call_id,
            "name": main.END_CONVERSATION_TOOL_NAME,
            "arguments": "{}",
        }
        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(item=call_item)
        )
        service._tool_call_details[call_id] = (
            main.END_CONVERSATION_TOOL_NAME,
            {},
        )
        service._scheduled_tool_call_ids.add(call_id)
        close_silently = AsyncMock()
        result_callback = AsyncMock()
        main.register_end_conversation_tool(
            service,
            close_silently,
            service.end_conversation_is_sole_terminal_tool,
        )
        _name, wrapped_handler = service._registered_function
        tool_task = asyncio.create_task(
            wrapped_handler(
                types.SimpleNamespace(
                    arguments={},
                    tool_call_id=call_id,
                    result_callback=result_callback,
                )
            )
        )
        await service._handle_evt_audio_delta(self._audio_delta())
        await service._handle_evt_text_delta(
            self._text_delta("response.text.delta", "discarded text")
        )
        await service._handle_evt_audio_transcript_delta(
            self._text_delta(
                "response.audio_transcript.delta",
                "discarded transcript",
            )
        )
        await service._handle_evt_audio_done(
            types.SimpleNamespace(response_id="decision-response")
        )
        await asyncio.sleep(0)
        self.assertFalse(tool_task.done())
        close_silently.assert_not_awaited()
        self.assertFalse(any(isinstance(frame, _TTSAudioRawFrame) for frame in pushed))
        self.assertFalse(any(isinstance(frame, tuple) for frame in pushed))

        await service._handle_evt_response_done(
            self._response_done([call_item])
        )
        await tool_task

        close_silently.assert_awaited_once_with()
        result_callback.assert_awaited_once()
        properties = cast(Any, result_callback.await_args).kwargs["properties"]
        self.assertFalse(properties.run_llm)
        self.assertFalse(any(isinstance(frame, _TTSAudioRawFrame) for frame in pushed))
        self.assertFalse(any(isinstance(frame, tuple) for frame in pushed))
        self.assertEqual(main.TURN_LIVENESS.in_flight, 0)
        service.begin_recovery()

    async def test_pending_mixed_function_call_rejects_close_and_releases_pcm(self):
        service, pushed = self._prepare_decision_service()
        close_call = {
            "id": "close-item",
            "type": "function_call",
            "call_id": "close-call",
            "name": main.END_CONVERSATION_TOOL_NAME,
            "arguments": "{}",
        }
        other_call = {
            "id": "other-item",
            "type": "function_call",
            "call_id": "other-call",
            "name": "other_tool",
            "arguments": "{}",
        }
        for item in (close_call, other_call):
            await service._handle_evt_conversation_item_added(
                types.SimpleNamespace(item=item)
            )
        service._tool_call_details["close-call"] = (
            main.END_CONVERSATION_TOOL_NAME,
            {},
        )
        service._running_tool_call_ids.add("close-call")
        original_in_flight = main.TURN_LIVENESS.in_flight
        main.TURN_LIVENESS.in_flight = 1
        try:
            await service._handle_evt_audio_delta(self._audio_delta())
            await service._handle_evt_audio_transcript_delta(
                self._text_delta(
                    "response.audio_transcript.delta",
                    "released transcript",
                )
            )
            await service._handle_evt_audio_done(
                types.SimpleNamespace(response_id="decision-response")
            )
            self.assertFalse(
                await service.end_conversation_is_sole_terminal_tool(
                    "close-call"
                )
            )
            await service._handle_evt_response_done(
                self._response_done([close_call, other_call])
            )
        finally:
            main.TURN_LIVENESS.in_flight = original_in_flight

        self.assertTrue(any(isinstance(frame, _TTSAudioRawFrame) for frame in pushed))
        self.assertIn(("audio_transcript", "released transcript"), pushed)
        self.assertTrue(any(type(frame).__name__ == "TTSStartedFrame" for frame in pushed))
        self.assertTrue(any(type(frame).__name__ == "TTSStoppedFrame" for frame in pushed))
        service.begin_recovery()

    async def test_unverified_terminal_close_call_releases_pcm(self):
        service, pushed = self._prepare_decision_service()
        call_item = {
            "id": "unverified-close-item",
            "type": "function_call",
            "call_id": "unverified-close-call",
            "name": main.END_CONVERSATION_TOOL_NAME,
            "arguments": "{}",
        }
        await service._handle_evt_conversation_item_added(
            types.SimpleNamespace(item=call_item)
        )
        await service._handle_evt_audio_delta(self._audio_delta())

        await service._handle_evt_response_done(
            self._response_done([call_item])
        )

        self.assertTrue(any(isinstance(frame, _TTSAudioRawFrame) for frame in pushed))
        service.begin_recovery()

    async def test_decision_audio_hold_is_bounded_and_initial_audio_is_immediate(self):
        service, pushed = self._prepare_decision_service()
        service.DECISION_AUDIO_HOLD_TIMEOUT_S = 0.001
        await service._handle_evt_audio_delta(self._audio_delta(audio=b"bounded"))
        self.assertFalse(any(isinstance(frame, _TTSAudioRawFrame) for frame in pushed))
        await asyncio.sleep(0.01)
        self.assertTrue(any(isinstance(frame, _TTSAudioRawFrame) for frame in pushed))
        service.begin_recovery()

    async def test_decision_output_hold_has_an_independent_event_count_bound(self):
        service, pushed = self._prepare_decision_service()
        cast(Any, service).DECISION_OUTPUT_HOLD_MAX_EVENTS = 2
        for text in ("first", "second"):
            await service._handle_evt_text_delta(
                self._text_delta("response.text.delta", text)
            )

        self.assertIn(("text", "first"), pushed)
        self.assertIn(("text", "second"), pushed)
        self.assertTrue(cast(Any, service._decision_output_hold).released)
        service.begin_recovery()

    async def test_stale_decision_output_callbacks_cannot_mutate_current_hold(self):
        service, pushed = self._prepare_decision_service()
        await service._handle_evt_audio_delta(self._audio_delta())
        await service._handle_evt_text_delta(
            types.SimpleNamespace(response_id="old-response", delta="old text")
        )
        await service._handle_evt_audio_transcript_delta(
            types.SimpleNamespace(
                response_id="old-response",
                delta="old transcript",
            )
        )
        await service._handle_evt_audio_done(
            types.SimpleNamespace(response_id="old-response")
        )

        hold = service._decision_output_hold
        self.assertIsNotNone(hold)
        self.assertFalse(cast(Any, hold).audio_done)
        self.assertEqual(cast(Any, hold).text_events, [])
        self.assertEqual(pushed, [])
        await service._handle_evt_audio_done(
            types.SimpleNamespace(response_id="decision-response")
        )
        self.assertTrue(cast(Any, hold).audio_done)
        service.begin_recovery()

        ordinary = main.SafeRealtimeLLMService()
        ordinary._current_audio_response = None
        ordinary.stop_ttfb_metrics = AsyncMock()
        ordinary._active_output_response_context = ("ordinary-response", 1)
        ordinary._assistant_output_frame_created = Mock(return_value=True)
        ordinary_pushed = []
        ordinary.push_frame = AsyncMock(
            side_effect=lambda frame, *_args: ordinary_pushed.append(frame)
        )
        await ordinary._handle_evt_audio_delta(
            self._audio_delta(
                response_id="ordinary-response",
                audio=b"ordinary",
            )
        )
        self.assertTrue(
            any(isinstance(frame, _TTSAudioRawFrame) for frame in ordinary_pushed)
        )

    async def test_recovery_start_immediately_retires_output_generation(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake()
        retire = Mock(return_value=True)
        settle = AsyncMock(return_value=True)
        handler.transport = types.SimpleNamespace(
            retire_output_audio_generation=retire,
            settle_output_audio_generation=settle,
        )
        self.assertTrue(
            await handler.bind_assistant_output_response("response-a", 1)
        )
        cast(Any, handler).MAX_WEDGE_TASKS = 0

        handler._on_connection_recovery_started()
        await asyncio.sleep(0)

        self.assertIsNone(handler._assistant_output_grant)
        self.assertIsNone(handler._device_audio_generation)
        retire.assert_called_with(("response-a", 1))
        settle.assert_awaited_once_with(("response-a", 1))

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

    async def test_phase_authority_expires_at_physical_wake_ceiling(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        self.assertTrue(handler.note_device_wake(1))
        progress_context = handler.capture_phase_authorization_context()
        terminal_context = (
            handler.capture_terminal_idle_phase_authorization_context()
        )
        handler._device_audio_generation = None
        handler._physical_wake_deadline = 0.0

        self.assertIsNone(handler.capture_phase_authorization_context())
        self.assertFalse(
            await handler.broadcast_phase("listening", progress_context)
        )
        self.assertFalse(await handler.broadcast_phase("idle", terminal_context))
        self.assertEqual(websocket.send.await_count, 0)
        self.assertIsNone(handler._device_audio_generation)

    async def test_slow_tool_thinking_preserves_response_a_output_owner(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_device_wake(1)
        retire = Mock(return_value=True)
        settle = AsyncMock(return_value=True)
        handler.transport = types.SimpleNamespace(
            bind_output_audio_generation=AsyncMock(return_value=True),
            retire_output_audio_generation=retire,
            settle_output_audio_generation=settle,
        )
        self.assertTrue(
            await handler.bind_assistant_output_response("response-a", 1)
        )
        grant = cast(Any, handler._assistant_output_grant)
        context = handler.capture_phase_authorization_context()
        original_in_flight = websocket_handler.TURN_LIVENESS.in_flight
        websocket_handler.TURN_LIVENESS.in_flight = 1
        try:
            self.assertTrue(
                await handler.broadcast_phase(
                    "thinking",
                    context,
                    preserve_output=True,
                )
            )
        finally:
            websocket_handler.TURN_LIVENESS.in_flight = original_in_flight

        self.assertIs(handler._assistant_output_grant, grant)
        self.assertTrue(handler._assistant_output_grant_is_current(grant))
        retire.assert_not_called()
        settle.assert_not_awaited()
        self.assertIsNone(handler._device_audio_generation)
        phase = json.loads(cast(Any, websocket.send.await_args).args[0])
        self.assertEqual(phase["value"], "thinking")

    async def test_recovery_racing_transport_bind_cannot_restore_output_grant(self):
        bind_started = asyncio.Event()
        release_bind = asyncio.Event()
        retired = []

        class RacingTransport:
            async def bind_output_audio_generation(self, context, _websocket):
                bind_started.set()
                await release_bind.wait()
                self.bound = context
                return True

            def retire_output_audio_generation(self, context=None):
                retired.append(context)
                return True

            async def settle_output_audio_generation(self, _context=None):
                return True

        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.transport = RacingTransport()
        handler.note_device_wake(1)

        binding = asyncio.create_task(
            handler.bind_assistant_output_response("response-racing", 2)
        )
        await bind_started.wait()
        handler._on_connection_recovery_started()
        release_bind.set()

        self.assertFalse(await binding)
        self.assertTrue(handler._connection_recovery_active)
        self.assertIsNone(handler._assistant_output_grant)
        self.assertIn(("response-racing", 2), retired)
        self.assertFalse(
            await handler._authorize_output_audio(
                ("response-racing", 2),
                websocket,
            )
        )

    async def test_realtime_speech_start_rearms_before_delayed_phase_callback(self):
        websocket = _FakeDeviceWebSocket()
        handler = websocket_handler.WebSocketHandler(follow_up_ms=0)
        self._admit(handler, websocket)
        handler.note_request_follow_up_turn_boundary()
        await self._reserve(handler)
        self._activate(handler)
        self._qualify_response(handler, "production-order-question")
        await self._open_requested_follow_up(handler, websocket)

        service = main.SafeRealtimeLLMService(
            max_context_turns=12,
            request_follow_up_answer_started=handler.bind_request_follow_up_answer,
            request_follow_up_answer_confirmed=(
                handler.confirm_request_follow_up_answer
            ),
        )
        service._input_speech_ledger_generation = service._session_generation
        await service._handle_evt_speech_started(
            types.SimpleNamespace(item_id="fresh-user", audio_start_ms=100)
        )
        grant = handler._request_follow_up_answer_grant
        self.assertIsNotNone(grant)
        self.assertEqual(grant.user_item_id, "fresh-user")

        # The real PhaseEmitter receives its queued frame after the OpenAI event
        # handler returns. Its callback must not consume or replace the grant.
        handler.note_request_follow_up_turn_boundary()
        self.assertIs(handler._request_follow_up_answer_grant, grant)

        service._conversation_window.begin_user_turn(
            self._message("fresh-user", "user")
        )
        service._conversation_window.activate("fresh-user")
        await service.handle_evt_input_audio_transcription_completed(
            types.SimpleNamespace(
                item_id="fresh-user",
                transcript="fresh answer",
            )
        )
        self.assertFalse(handler._request_follow_up_budget_spent)

        handler._check_nearby_media_activity = AsyncMock(
            return_value=websocket_handler.MediaActivity.CLEAR
        )
        self.assertEqual(
            await handler.reserve_request_follow_up("next-question"),
            websocket_handler.FollowUpReservationOutcome.RESERVED,
        )
        handler._check_nearby_media_activity.assert_awaited_once_with()

    async def test_historical_speech_pair_after_recovery_cannot_rearm(self):
        answer_started = Mock(return_value=True)
        answer_confirmed = Mock(return_value=True)
        service = main.SafeRealtimeLLMService(
            max_context_turns=12,
            request_follow_up_answer_started=answer_started,
            request_follow_up_answer_confirmed=answer_confirmed,
        )
        service._conversation_window.begin_user_turn(
            self._message("old-user", "user")
        )
        service._conversation_window.attach_transcript("old-user", "old answer")
        service._conversation_window.activate("old-user")
        self.assertTrue(
            service._conversation_window.finish_response(
                "completed",
                [self._message("old-assistant", "assistant", "old reply")],
            )
        )
        service._conversation_window.replace_item_ids(
            {
                "old-user": "replayed-user",
                "old-assistant": "replayed-assistant",
            }
        )
        old_generation = service._session_generation
        service._session_generation += 1
        service._input_speech_ledger_generation = service._session_generation
        service._seen_input_speech_items.clear()
        service._last_input_speech_start_ms = -1

        token = main._CURRENT_REALTIME_SESSION_GENERATION.set(old_generation)
        try:
            await service._handle_evt_speech_started(
                types.SimpleNamespace(item_id="old-user", audio_start_ms=100)
            )
            await service.handle_evt_input_audio_transcription_completed(
                types.SimpleNamespace(item_id="old-user", transcript="old answer")
            )
        finally:
            main._CURRENT_REALTIME_SESSION_GENERATION.reset(token)

        await service._handle_evt_speech_started(
            types.SimpleNamespace(item_id="replayed-user", audio_start_ms=200)
        )
        await service.handle_evt_input_audio_transcription_completed(
            types.SimpleNamespace(
                item_id="replayed-user",
                transcript="old answer",
            )
        )
        answer_started.assert_not_called()
        answer_confirmed.assert_not_called()
        self.assertEqual(service._follow_up_answer_item_sequences, {})

        await service._handle_evt_speech_started(
            types.SimpleNamespace(item_id="fresh-user", audio_start_ms=201)
        )
        service._conversation_window.begin_user_turn(
            self._message("fresh-user", "user")
        )
        service._conversation_window.activate("fresh-user")
        await service.handle_evt_input_audio_transcription_completed(
            types.SimpleNamespace(
                item_id="fresh-user",
                transcript="fresh answer",
            )
        )

        answer_started.assert_called_once_with("fresh-user", 1)
        answer_confirmed.assert_called_once_with("fresh-user", 1, "fresh answer")
        self.assertEqual(len(service._seen_input_speech_items), 1)


if __name__ == "__main__":
    unittest.main()
