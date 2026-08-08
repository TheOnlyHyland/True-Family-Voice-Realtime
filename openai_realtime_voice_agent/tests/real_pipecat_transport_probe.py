"""Fresh-process transport checks against the actual pinned Pipecat package."""

import asyncio
import base64
import enum
import importlib.metadata
import json
import unittest
from pathlib import Path
import sys
import types
from unittest.mock import AsyncMock


ADDON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ADDON_ROOT))

from app.main import Application, SafeRealtimeLLMService  # noqa: E402
from app.timers import TimerRegistry  # noqa: E402
from pipecat.adapters.schemas.function_schema import FunctionSchema  # noqa: E402
from pipecat.adapters.schemas.tools_schema import ToolsSchema  # noqa: E402
from pipecat.transports.websocket.server import WebsocketServerParams  # noqa: E402
from pipecat.frames.frames import (  # noqa: E402
    CancelFrame,
    LLMFullResponseEndFrame,
    OutputAudioRawFrame,
    StartFrame,
    TTSAudioRawFrame,
    TTSTextFrame,
)
from pipecat.processors.frame_processor import FrameDirection  # noqa: E402

from app.single_owner_websocket import (  # noqa: E402
    SingleOwnerWebsocketServerTransport,
    current_output_audio_context,
)
from app.raw_audio_serializer import RawAudioSerializer  # noqa: E402
from app.websocket_handler import WebSocketHandler  # noqa: E402
from app.transcript_logger import TranscriptLogger  # noqa: E402


class _State(enum.IntEnum):
    OPEN = 1
    CLOSED = 3


class _Socket:
    def __init__(self):
        self.state = _State.OPEN
        self.close_count = 0
        self.client = types.SimpleNamespace(host="probe")
        self.messages = []

    async def send(self, message):
        self.messages.append(message)

    async def close(self):
        self.close_count += 1
        self.state = _State.CLOSED

    async def wait_closed(self):
        if self.state is not _State.CLOSED:
            raise RuntimeError("socket is not closed")


class _UncertainSocket(_Socket):
    async def close(self):
        await asyncio.Event().wait()


class _BlockingSendSocket(_Socket):
    def __init__(self):
        super().__init__()
        self.send_entered = asyncio.Event()
        self.release_send = asyncio.Event()

    async def send(self, message):
        self.send_entered.set()
        await self.release_send.wait()
        self.messages.append(message)


class _InspectingRawAudioSerializer(RawAudioSerializer):
    def __init__(self):
        super().__init__()
        self.serialized_audio = []

    async def serialize(self, frame):
        if isinstance(frame, OutputAudioRawFrame):
            self.serialized_audio.append(
                (
                    frame.id,
                    hasattr(frame, "_true_family_output_context"),
                )
            )
        return await super().serialize(frame)


class _MCPClient:
    def __init__(self):
        self.registered = []
        self._schema = ToolsSchema(
            standard_tools=[
                FunctionSchema(
                    name=name,
                    description=name,
                    properties={},
                    required=[],
                )
                for name in ("allowed_mcp", "hidden_mcp", "mark_false_wake")
            ]
        )

    async def get_tools_schema(self):
        return self._schema

    async def register_tools_schema(self, tools_schema, llm):
        async def handler(_params):
            return None

        for function_schema in tools_schema.standard_tools:
            self.registered.append(function_schema.name)
            llm.register_function(function_schema.name, handler)


class RealPipecatTransportTests(unittest.IsolatedAsyncioTestCase):
    def _transport(self, *, serializer=None, audio_out_10ms_chunks=4):
        self.assertEqual(importlib.metadata.version("pipecat-ai"), "0.0.97")
        transport = SingleOwnerWebsocketServerTransport(
            params=WebsocketServerParams(
                serializer=serializer,
                audio_out_enabled=True,
                audio_out_sample_rate=24000,
                audio_out_channels=1,
                audio_out_10ms_chunks=audio_out_10ms_chunks,
            ),
        )
        transport.output()
        return transport

    async def test_application_registers_only_the_exact_exposed_mcp_subset(self):
        application = Application()
        application.openai_api_key = "probe"
        application.model = "gpt-realtime"
        application.instructions = "Probe"
        application.voice = "marin"
        application.openai_speed = 1.0
        application.max_output_tokens = 1200
        application.max_context_messages = 12
        application.turn_detection_type = "semantic_vad"
        application.vad_eagerness = "low"
        application.semantic_vad_create_response = False
        application.interrupt_response = False
        application.transcription_model = "gpt-4o-transcribe"
        application.transcription_language = ""
        application.noise_reduction = ""
        application.enable_disconnect_tool = False
        application.enable_web_search = False
        application.enable_voice_memory = False
        application.web_search_model = "gpt-5.5"
        application.request_follow_up_supported = True
        application.mcp_tool_allowlist = frozenset(
            {"allowed_mcp", "mark_false_wake"}
        )
        mcp_client = _MCPClient()
        application.mcp_client = mcp_client
        application.ha_access_token = "probe"
        application.websocket_handler = WebSocketHandler(follow_up_ms=0)
        application.websocket_transport = None
        application.timer_registry = TimerRegistry()

        service = await application._ensure_openai_service()
        if service is None:
            self.fail("application did not create the realtime service")

        self.assertEqual(mcp_client.registered, ["allowed_mcp"])
        self.assertIn("allowed_mcp", service._functions)
        self.assertNotIn("hidden_mcp", service._functions)
        self.assertIn("mark_false_wake", service._functions)

    async def test_actual_application_handler_completes_after_hello_send(self):
        transport = self._transport()
        handler = WebSocketHandler(follow_up_ms=0)
        admitted_clients = []

        async def on_admitted(client_id):
            admitted_clients.append(client_id)

        handler.setup_event_handlers(transport, on_admitted)
        candidate = _Socket()

        self.assertTrue(await transport._on_client_connected(candidate))
        self.assertEqual(len(candidate.messages), 1)
        self.assertEqual(json.loads(candidate.messages[0])["type"], "hello")
        transaction = handler._hello_transaction
        if transaction is None:
            self.fail("application handler did not create a hello transaction")
        self.assertIs(transaction.websocket, candidate)
        self.assertEqual(admitted_clients, [])

        timeout_task = handler._hello_timeout_task
        handler._clear_hello_transaction()
        if timeout_task is not None:
            await asyncio.gather(timeout_task, return_exceptions=True)
        await transport.reject_candidate(candidate)
        await transport.cleanup()

    async def test_actual_openai_pcm_registers_exact_response_generation(self):
        service = SafeRealtimeLLMService(api_key="probe")
        service._active_output_response_context = ("response-a", 4)
        service._current_audio_response = None
        service.stop_ttfb_metrics = AsyncMock()
        observed = []
        registered = []

        def register_source(frame, response_id, response_generation):
            registered.append(
                (
                    frame,
                    (response_id, response_generation),
                    hasattr(frame, "_true_family_output_context"),
                )
            )
            return True

        service.set_assistant_output_event_handlers(on_audio_frame=register_source)

        async def capture_frame(frame):
            observed.append(
                (
                    current_output_audio_context(),
                    hasattr(frame, "_true_family_output_context"),
                )
            )

        service.push_frame = AsyncMock(side_effect=capture_frame)
        event = types.SimpleNamespace(
            response_id="response-a",
            item_id="item-a",
            content_index=0,
            delta=base64.b64encode(b"\x00\x00").decode("ascii"),
        )

        await service._handle_evt_audio_delta(event)

        self.assertEqual(
            [(context, private) for _frame, context, private in registered],
            [(("response-a", 4), False)],
        )
        self.assertEqual(
            observed[-1],
            (None, False),
        )
        registrations_before_stale = len(registered)
        event.response_id = "response-old"
        await service._handle_evt_audio_delta(event)
        self.assertEqual(len(registered), registrations_before_stale)

    async def test_actual_chunker_preserves_exact_owner_and_drops_replacement_audio(self):
        serializer = _InspectingRawAudioSerializer()
        transport = self._transport(
            serializer=serializer,
            audio_out_10ms_chunks=1,
        )
        output = transport.output()
        old = _Socket()
        replacement = _Socket()
        authorizations = []

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, websocket):
            owner_transport.complete_candidate_handler(websocket, True)

        async def authorize(context, websocket):
            authorizations.append((context, websocket))
            return True

        def create_task(coroutine, *_args, **_kwargs):
            return asyncio.create_task(coroutine)

        async def cancel_task(task, *_args, **_kwargs):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        output.create_task = create_task
        output.cancel_task = cancel_task
        output.push_frame = AsyncMock()
        transport.set_output_audio_authorizer(authorize)
        await output.start(StartFrame())
        sender = output._media_senders[None]
        real_chunker = sender._true_family_handle_audio_frame
        self.assertEqual(
            real_chunker.__func__.__module__,
            "pipecat.transports.base_output",
        )
        self.assertEqual(
            real_chunker.__func__.__qualname__,
            "BaseOutputTransport.MediaSender.handle_audio_frame",
        )
        self.assertTrue(await transport._on_client_connected(old))
        self.assertTrue(await transport.admit_client(old))
        self.assertTrue(
            await transport.bind_output_audio_generation(("response-old", 5), old)
        )

        first = TTSAudioRawFrame(
            audio=b"\x01\x00" * 480,
            sample_rate=24000,
            num_channels=1,
        )
        self.assertTrue(
            transport.register_output_audio_source(
                first,
                ("response-old", 5),
                old,
            )
        )
        await output.process_frame(first, FrameDirection.DOWNSTREAM)
        for _ in range(100):
            if len(old.messages) == 2:
                break
            await asyncio.sleep(0.001)
        self.assertEqual(len(old.messages), 2)
        self.assertEqual(old.messages, [b"\x01\x00" * 240] * 2)
        self.assertEqual(
            authorizations,
            [(("response-old", 5), old), (("response-old", 5), old)],
        )
        self.assertEqual(len(serializer.serialized_audio), 2)
        self.assertTrue(
            all(frame_id != first.id for frame_id, _private in serializer.serialized_audio)
        )
        self.assertTrue(
            all(not private for _frame_id, private in serializer.serialized_audio)
        )

        await sender._cancel_audio_task()
        queued_old = TTSAudioRawFrame(
            audio=b"\x02\x00" * 480,
            sample_rate=24000,
            num_channels=1,
        )
        self.assertTrue(
            transport.register_output_audio_source(
                queued_old,
                ("response-old", 5),
                old,
            )
        )
        await output.process_frame(queued_old, FrameDirection.DOWNSTREAM)
        self.assertEqual(sender._audio_queue.qsize(), 2)
        partial_old = TTSAudioRawFrame(
            audio=b"\x06\x00" * 120,
            sample_rate=24000,
            num_channels=1,
        )
        self.assertTrue(
            transport.register_output_audio_source(
                partial_old,
                ("response-old", 5),
                old,
            )
        )
        await output.process_frame(partial_old, FrameDirection.DOWNSTREAM)
        self.assertEqual(len(sender._audio_buffer), 240)

        self.assertTrue(await transport.retire_client(old))
        self.assertTrue(await transport._on_client_connected(replacement))
        self.assertTrue(await transport.admit_client(replacement))
        self.assertTrue(
            await transport.bind_output_audio_generation(
                ("response-new", 6),
                replacement,
            )
        )
        self.assertEqual(sender._audio_queue.qsize(), 0)
        self.assertEqual(sender._audio_buffer, bytearray())

        self.assertFalse(
            transport.register_output_audio_source(
                queued_old,
                ("response-old", 5),
                old,
            )
        )
        await output.process_frame(queued_old, FrameDirection.DOWNSTREAM)
        self.assertEqual(sender._audio_queue.qsize(), 0)

        sender._create_audio_task()
        fresh = TTSAudioRawFrame(
            audio=b"\x03\x00" * 480,
            sample_rate=24000,
            num_channels=1,
        )
        self.assertTrue(
            transport.register_output_audio_source(
                fresh,
                ("response-new", 6),
                replacement,
            )
        )
        await output.process_frame(fresh, FrameDirection.DOWNSTREAM)
        for _ in range(100):
            if len(replacement.messages) == 2:
                break
            await asyncio.sleep(0.001)

        self.assertEqual(replacement.messages, [b"\x03\x00" * 240] * 2)
        self.assertEqual(old.messages, [b"\x01\x00" * 240] * 2)
        self.assertEqual(
            authorizations[-2:],
            [
                (("response-new", 6), replacement),
                (("response-new", 6), replacement),
            ],
        )
        self.assertFalse(hasattr(fresh, "_true_family_output_context"))
        self.assertEqual(len(serializer.serialized_audio), 4)
        self.assertTrue(
            all(not private for _frame_id, private in serializer.serialized_audio)
        )
        await output.cancel(CancelFrame())
        await transport.cleanup_uncertain_sockets()

    async def test_assistant_completion_log_never_retains_or_emits_text(self):
        processor = TranscriptLogger(capture="assistant")
        processor.push_frame = AsyncMock()

        with self.assertLogs("app.transcript_logger", level="INFO") as logs:
            await processor.process_frame(
                TTSTextFrame(
                    text="private spoken reply",
                    aggregated_by="sentence",
                ),
                FrameDirection.DOWNSTREAM,
            )
            await processor.process_frame(
                LLMFullResponseEndFrame(),
                FrameDirection.DOWNSTREAM,
            )

        rendered = "\n".join(logs.output)
        self.assertNotIn("private spoken reply", rendered)
        self.assertIn("20 characters", rendered)
        self.assertFalse(hasattr(processor, "_assistant_buf"))

    async def test_actual_chunk_write_settles_before_new_generation_binds(self):
        serializer = RawAudioSerializer()
        transport = self._transport(
            serializer=serializer,
            audio_out_10ms_chunks=1,
        )
        output = transport.output()
        websocket = _BlockingSendSocket()

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, candidate):
            owner_transport.complete_candidate_handler(candidate, True)

        def create_task(coroutine, *_args, **_kwargs):
            return asyncio.create_task(coroutine)

        async def cancel_task(task, *_args, **_kwargs):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        output.create_task = create_task
        output.cancel_task = cancel_task
        output.push_frame = AsyncMock()
        transport.set_output_audio_authorizer(AsyncMock(return_value=True))
        await output.start(StartFrame())
        self.assertTrue(await transport._on_client_connected(websocket))
        self.assertTrue(await transport.admit_client(websocket))
        self.assertTrue(
            await transport.bind_output_audio_generation(
                ("response-old", 10),
                websocket,
            )
        )

        old = TTSAudioRawFrame(
            audio=b"\x04\x00" * 240,
            sample_rate=24000,
            num_channels=1,
        )
        self.assertTrue(
            transport.register_output_audio_source(
                old,
                ("response-old", 10),
                websocket,
            )
        )
        await output.process_frame(old, FrameDirection.DOWNSTREAM)
        await websocket.send_entered.wait()

        bind_new = asyncio.create_task(
            transport.bind_output_audio_generation(
                ("response-new", 11),
                websocket,
            )
        )
        await asyncio.sleep(0)
        self.assertFalse(bind_new.done())
        websocket.release_send.set()
        self.assertTrue(await bind_new)
        self.assertEqual(websocket.messages, [b"\x04\x00" * 240])

        fresh = TTSAudioRawFrame(
            audio=b"\x05\x00" * 240,
            sample_rate=24000,
            num_channels=1,
        )
        self.assertTrue(
            transport.register_output_audio_source(
                fresh,
                ("response-new", 11),
                websocket,
            )
        )
        await output.process_frame(fresh, FrameDirection.DOWNSTREAM)
        for _ in range(100):
            if len(websocket.messages) == 2:
                break
            await asyncio.sleep(0.001)
        self.assertEqual(
            websocket.messages,
            [b"\x04\x00" * 240, b"\x05\x00" * 240],
        )
        await output.cancel(CancelFrame())
        await transport.cleanup_uncertain_sockets()

    async def test_actual_scheduled_handler_must_explicitly_complete_admission(self):
        transport = self._transport()
        candidate = _Socket()
        entered = asyncio.Event()
        release = asyncio.Event()

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, websocket):
            entered.set()
            await release.wait()
            owner_transport.complete_candidate_handler(websocket, True)

        admission = asyncio.create_task(transport._on_client_connected(candidate))
        await entered.wait()
        await asyncio.sleep(0)
        self.assertFalse(admission.done())
        release.set()

        self.assertTrue(await admission)
        self.assertTrue(await transport.admit_client(candidate))
        self.assertIs(transport.admitted_websocket, candidate)
        await transport.cleanup()

    async def test_actual_handler_rejection_closes_without_stranding_candidate(self):
        transport = self._transport()
        candidate = _Socket()

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, websocket):
            owner_transport.complete_candidate_handler(websocket, False)
            raise RuntimeError("expected probe failure")

        self.assertFalse(await transport._on_client_connected(candidate))
        self.assertIsNone(transport.candidate_websocket)
        self.assertEqual(candidate.state, _State.CLOSED)
        await transport.cleanup()

    async def test_actual_handler_timeout_rejects_and_releases_event_task(self):
        transport = self._transport()
        transport.CANDIDATE_HANDLER_TIMEOUT_S = 0.01
        candidate = _Socket()
        release = asyncio.Event()

        @transport.event_handler("on_client_connected")
        async def on_connected(_owner_transport, _websocket):
            await release.wait()

        self.assertFalse(await transport._on_client_connected(candidate))
        self.assertIsNone(transport.candidate_websocket)
        self.assertEqual(candidate.state, _State.CLOSED)
        self.assertEqual(
            transport._tracked_event_tasks("on_client_connected"),
            set(),
        )
        release.set()
        await asyncio.sleep(0)
        await transport.cleanup()

    async def test_actual_transport_bounds_uncertain_close_cleanup(self):
        transport = self._transport()
        transport.SOCKET_CLOSE_TIMEOUT_S = 0.01
        websocket = _UncertainSocket()

        self.assertFalse(await transport.close_socket(websocket))
        self.assertEqual(transport.uncertain_socket_count, 1)
        await transport.cleanup_uncertain_sockets()
        self.assertEqual(transport.uncertain_socket_count, 0)
        await transport.cleanup()

    async def test_actual_delayed_disconnect_cannot_clear_replacement(self):
        transport = self._transport()
        old = _Socket()
        new = _Socket()
        old_disconnect_entered = asyncio.Event()
        release_old_disconnect = asyncio.Event()

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, websocket):
            owner_transport.complete_candidate_handler(websocket, True)

        @transport.event_handler("on_client_disconnected")
        async def on_disconnected(_owner_transport, websocket):
            if websocket is old:
                old_disconnect_entered.set()
                await release_old_disconnect.wait()

        self.assertTrue(await transport._on_client_connected(old))
        self.assertTrue(await transport.admit_client(old))
        self.assertTrue(await transport._on_client_disconnected(old))
        await old_disconnect_entered.wait()
        self.assertTrue(await transport._on_client_connected(new))
        self.assertTrue(await transport.admit_client(new))
        release_old_disconnect.set()
        await asyncio.sleep(0)

        self.assertIs(transport.admitted_websocket, new)
        self.assertIs(transport.output()._websocket, new)
        await transport.cleanup()


if __name__ == "__main__":
    unittest.main()
