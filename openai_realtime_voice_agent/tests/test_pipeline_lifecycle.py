"""Offline lifecycle tests for the single-device voice pipeline."""

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


class _Frame:
    pass


class _OpenAIRealtimeLLMService:
    pass


class _Placeholder:
    def __init__(self, *args, **kwargs):
        pass


class _TurnLiveness:
    def tool_started(self):
        pass

    def tool_finished(self):
        pass


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
