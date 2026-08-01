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


class _OpenAIRealtimeLLMService:
    def __init__(self, *args, **kwargs):
        self._api_session_ready = False
        self._run_llm_when_api_session_ready = False
        self._llm_needs_conversation_setup = True
        self._websocket = None
        self._receive_task = None
        self._connect_hook: Any = None
        self._context = None

    async def _create_response(self):
        pass

    async def _handle_context(self, context):
        self._context = context

    def register_function(self, function_name, handler, start_callback=None, **kwargs):
        self._registered_function = (function_name, handler)

    async def _handle_evt_session_updated(self, _evt):
        self._api_session_ready = True
        if self._run_llm_when_api_session_ready:
            self._run_llm_when_api_session_ready = False
            await self._create_response()

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
    FrameDirection=_Placeholder,
)
_stub_module(
    "pipecat.frames.frames",
    Frame=_Frame,
    InputAudioRawFrame=type("InputAudioRawFrame", (_Frame,), {}),
    OutputAudioRawFrame=type("OutputAudioRawFrame", (_Frame,), {}),
    StartFrame=type("StartFrame", (_Frame,), {}),
    EndFrame=type("EndFrame", (_Frame,), {}),
    ErrorFrame=type("ErrorFrame", (_Frame,), {}),
)
_stub_module("pipecat.audio.utils", create_stream_resampler=lambda: _Placeholder())
_stub_module("pipecat.services.openai.realtime.events")


class _LLMContext:
    def __init__(self, messages=None):
        self._messages = messages or []

    def get_messages(self):
        return self._messages


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
