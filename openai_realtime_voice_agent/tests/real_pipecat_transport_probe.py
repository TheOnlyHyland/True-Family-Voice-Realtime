"""Fresh-process transport checks against the actual pinned Pipecat package."""

import asyncio
import base64
import enum
import importlib.metadata
import json
import time
import unittest
from pathlib import Path
import sys
import types
from unittest.mock import AsyncMock, Mock


ADDON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ADDON_ROOT))

from app.main import (  # noqa: E402
    Application,
    SafeRealtimeLLMService,
    _CURRENT_REALTIME_SESSION_GENERATION,
)
from app.timers import TimerRegistry  # noqa: E402
from pipecat.adapters.schemas.function_schema import FunctionSchema  # noqa: E402
from pipecat.adapters.schemas.tools_schema import ToolsSchema  # noqa: E402
from pipecat.transports.websocket.server import WebsocketServerParams  # noqa: E402
from pipecat.frames.frames import (  # noqa: E402
    BotStoppedSpeakingFrame,
    CancelFrame,
    LLMFullResponseEndFrame,
    LLMTextFrame,
    OutputAudioRawFrame,
    StartFrame,
    TTSAudioRawFrame,
    TTSTextFrame,
)
from pipecat.processors.frame_processor import FrameDirection  # noqa: E402
from pipecat.services.openai.realtime import events  # noqa: E402

from app.single_owner_websocket import (  # noqa: E402
    SingleOwnerWebsocketServerTransport,
    current_output_audio_context,
)
from app.raw_audio_serializer import RawAudioSerializer  # noqa: E402
from app.phase_emitter import PhaseEmitter, TURN_LIVENESS  # noqa: E402
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


class _FailingAudioSocket(_Socket):
    async def send(self, message):
        if isinstance(message, bytes):
            raise RuntimeError("probe audio write failure")
        await super().send(message)


class _CancellationResistantAudioSocket(_Socket):
    def __init__(self):
        super().__init__()
        self.send_entered = asyncio.Event()
        self.cancellation_resisted = asyncio.Event()
        self.release_send = asyncio.Event()

    async def send(self, message):
        if not isinstance(message, bytes):
            await super().send(message)
            return
        self.send_entered.set()
        try:
            await self.release_send.wait()
        except asyncio.CancelledError:
            self.cancellation_resisted.set()
            await self.release_send.wait()
        if self.state is not _State.OPEN:
            raise RuntimeError("socket was retired before the stale write resumed")
        self.messages.append(message)


class _PersistentlyCancellationResistantAudioSocket(_Socket):
    def __init__(self):
        super().__init__()
        self.send_entered = asyncio.Event()
        self.cancellation_resisted = asyncio.Event()
        self.release_send = asyncio.Event()

    async def send(self, message):
        if not isinstance(message, bytes):
            await super().send(message)
            return
        self.send_entered.set()
        while not self.release_send.is_set():
            try:
                await self.release_send.wait()
            except asyncio.CancelledError:
                self.cancellation_resisted.set()
        if self.state is not _State.OPEN:
            raise RuntimeError("socket was retired before stale PCM resumed")
        self.messages.append(message)


class _CancellationResistantCloseSocket(_Socket):
    def __init__(self):
        super().__init__()
        self.close_entered = asyncio.Event()
        self.cancellation_resisted = asyncio.Event()
        self.release_close = asyncio.Event()
        self.abort_count = 0
        self.transport = types.SimpleNamespace(abort=self._abort)

    def _abort(self):
        self.abort_count += 1
        self.state = _State.CLOSED
        self.release_close.set()

    async def close(self):
        self.close_count += 1
        self.close_entered.set()
        try:
            await self.release_close.wait()
        except asyncio.CancelledError:
            self.cancellation_resisted.set()
            await self.release_close.wait()
        self.state = _State.CLOSED


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

    async def test_actual_silent_terminal_discards_text_transcript_and_pcm(self):
        transport = self._transport(
            serializer=RawAudioSerializer(),
            audio_out_10ms_chunks=1,
        )
        output = transport.output()
        websocket = _Socket()

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, candidate):
            owner_transport.complete_candidate_handler(candidate, True)

        output.create_task = lambda coroutine, *_args, **_kwargs: asyncio.create_task(
            coroutine
        )

        async def cancel_task(task, *_args, **_kwargs):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        output.cancel_task = cancel_task
        output.push_frame = AsyncMock()
        await output.start(StartFrame())
        self.assertTrue(await transport._on_client_connected(websocket))
        self.assertTrue(await transport.admit_client(websocket))

        service = SafeRealtimeLLMService(
            api_key="probe",
            max_context_turns=12,
            authorized_tool_names=("end_conversation",),
        )
        pushed = []

        async def push_frame(frame, *_args, **_kwargs):
            pushed.append(frame)
            if isinstance(frame, TTSAudioRawFrame):
                await output.process_frame(frame, FrameDirection.DOWNSTREAM)

        service.push_frame = push_frame
        service.stop_ttfb_metrics = AsyncMock()
        service.start_llm_usage_metrics = AsyncMock()
        service.stop_processing_metrics = AsyncMock()
        service.set_assistant_output_event_handlers(
            on_audio_frame=lambda frame, response_id, response_generation: (
                transport.register_output_audio_source(
                    frame,
                    (response_id, response_generation),
                    websocket,
                )
            )
        )
        service.set_silent_close_runtime_authorizer(lambda: True)
        service._conversation_window.begin_user_turn(
            {
                "id": "answer-user",
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_audio",
                        "transcript": "unrelated answer",
                    }
                ],
            }
        )
        service._conversation_window.attach_transcript(
            "answer-user",
            "unrelated answer",
        )
        service._conversation_window.activate("answer-user")
        service._confirmed_follow_up_answer_identity = ("answer-user", 1)
        service._active_response_id = "decision-response"
        service._output_response_generation = 1
        service._active_output_response_context = ("decision-response", 1)
        service._begin_decision_output_hold("decision-response", 1)
        call_id = "silent-call"
        call_item = events.ConversationItem(
            id="silent-item",
            type="function_call",
            status="completed",
            call_id=call_id,
            name="end_conversation",
            arguments="{}",
        )
        service._response_tool_call_ids[("decision-response", 1)] = {call_id}
        service._tool_call_details[call_id] = ("end_conversation", {})
        service._running_tool_call_ids.add(call_id)

        await service._handle_evt_audio_delta(
            events.ResponseAudioDelta(
                event_id="audio-delta",
                type="response.output_audio.delta",
                response_id="decision-response",
                item_id="assistant-item",
                output_index=0,
                content_index=0,
                delta=base64.b64encode(b"silent-pcm").decode("ascii"),
            )
        )
        await service._handle_evt_text_delta(
            events.ResponseTextDelta(
                event_id="text-delta",
                type="response.output_text.delta",
                response_id="decision-response",
                item_id="assistant-item",
                output_index=0,
                content_index=0,
                delta="silent text",
            )
        )
        await service._handle_evt_audio_transcript_delta(
            events.ResponseAudioTranscriptDelta(
                event_id="transcript-delta",
                type="response.output_audio_transcript.delta",
                response_id="decision-response",
                item_id="assistant-item",
                output_index=0,
                content_index=0,
                delta="silent transcript",
            )
        )
        self.assertEqual(pushed, [])

        usage = events.Usage(
            total_tokens=0,
            input_tokens=0,
            output_tokens=0,
            input_token_details=events.TokenDetails(),
            output_token_details=events.TokenDetails(),
        )
        from app import ha_sensors

        original_publisher = ha_sensors.PUBLISHER
        ha_sensors.PUBLISHER = types.SimpleNamespace(usage=AsyncMock())
        try:
            await service._handle_evt_response_done(
                events.ResponseDone(
                    event_id="response-done",
                    type="response.done",
                    response=events.Response(
                        id="decision-response",
                        object="realtime.response",
                        status="completed",
                        status_details=None,
                        output=[call_item],
                        output_modalities=["audio"],
                        usage=usage,
                    ),
                )
            )
        finally:
            ha_sensors.PUBLISHER = original_publisher

        self.assertFalse(any(isinstance(frame, TTSAudioRawFrame) for frame in pushed))
        self.assertFalse(any(isinstance(frame, TTSTextFrame) for frame in pushed))
        self.assertFalse(any(isinstance(frame, LLMTextFrame) for frame in pushed))
        self.assertFalse(any(isinstance(message, bytes) for message in websocket.messages))
        service._running_tool_call_ids.clear()
        service.begin_recovery()
        await transport.retire_client(websocket)
        await output.cancel(CancelFrame())
        await transport.cleanup_uncertain_sockets()

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
        self.assertNotIn(
            ("response-old", 5),
            output._true_family_failed_audio_generations,
        )
        self.assertTrue(
            all(frame_id != first.id for frame_id, _private in serializer.serialized_audio)
        )
        self.assertTrue(
            all(not private for _frame_id, private in serializer.serialized_audio)
        )

        blocked_send_entered = asyncio.Event()

        async def blocked_send(message):
            if isinstance(message, bytes):
                blocked_send_entered.set()
                await asyncio.Event().wait()
            old.messages.append(message)

        old.send = blocked_send
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
        await blocked_send_entered.wait()
        self.assertEqual(sender._audio_queue.qsize(), 1)
        self.assertEqual(len(output._true_family_chunk_contexts), 1)
        self.assertEqual(len(output._true_family_active_write_contexts), 1)
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
        self.assertEqual(
            sender._true_family_partial_provenance,
            (("response-old", 5), old),
        )
        self.assertIs(sender._true_family_partial_frame_type, TTSAudioRawFrame)
        self.assertEqual(sender._true_family_partial_num_channels, 1)

        finish_old = asyncio.create_task(
            transport.gracefully_finish_output_audio_generation(
                ("response-old", 5),
                old,
                lambda: transport.admitted_websocket is old,
                timeout_s=0.5,
            )
        )
        await asyncio.sleep(0)
        self.assertFalse(finish_old.done())
        self.assertTrue(await transport.retire_client(old))
        self.assertFalse(await finish_old)
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
        self.assertEqual(len(serializer.serialized_audio), 5)
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

    async def test_actual_queued_and_active_a_drain_before_b_wire_order(self):
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
            audio=b"\x04\x00" * 480,
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

        finish_old = asyncio.create_task(
            transport.gracefully_finish_output_audio_generation(
                ("response-old", 10),
                websocket,
                lambda: transport.admitted_websocket is websocket,
                timeout_s=1.0,
            )
        )
        await asyncio.sleep(0)
        self.assertFalse(finish_old.done())
        websocket.release_send.set()
        self.assertTrue(await finish_old)
        self.assertEqual(websocket.messages, [b"\x04\x00" * 240] * 2)
        self.assertTrue(
            await transport.bind_output_audio_generation(
                ("response-new", 11),
                websocket,
            )
        )

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
            if len(websocket.messages) == 3:
                break
            await asyncio.sleep(0.001)
        self.assertEqual(
            websocket.messages,
            [
                b"\x04\x00" * 240,
                b"\x04\x00" * 240,
                b"\x05\x00" * 240,
            ],
        )
        await output.cancel(CancelFrame())
        await transport.cleanup_uncertain_sockets()

    async def test_actual_partial_a_is_padded_once_before_b(self):
        serializer = RawAudioSerializer()
        transport = self._transport(
            serializer=serializer,
            audio_out_10ms_chunks=1,
        )
        output = transport.output()
        websocket = _Socket()

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, candidate):
            owner_transport.complete_candidate_handler(candidate, True)

        output.create_task = lambda coroutine, *_args, **_kwargs: asyncio.create_task(
            coroutine
        )

        async def cancel_task(task, *_args, **_kwargs):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        output.cancel_task = cancel_task
        output.push_frame = AsyncMock()
        transport.set_output_audio_authorizer(AsyncMock(return_value=True))
        await output.start(StartFrame())
        self.assertTrue(await transport._on_client_connected(websocket))
        self.assertTrue(await transport.admit_client(websocket))
        self.assertTrue(
            await transport.bind_output_audio_generation(
                ("response-a", 20),
                websocket,
            )
        )

        partial = TTSAudioRawFrame(
            audio=b"\x07\x00" * 120,
            sample_rate=24000,
            num_channels=1,
        )
        self.assertTrue(
            transport.register_output_audio_source(
                partial,
                ("response-a", 20),
                websocket,
            )
        )
        await output.process_frame(partial, FrameDirection.DOWNSTREAM)
        self.assertEqual(
            output._media_senders[None]._audio_buffer,
            bytearray(b"\x07\x00" * 120),
        )
        sender = output._media_senders[None]
        self.assertEqual(
            output._true_family_partial_audio[id(sender)].audio,
            b"\x07\x00" * 120,
        )
        # Pipecat may clear its own idle partial buffer. The adapter-owned copy
        # remains authoritative and must still be padded exactly once.
        sender._audio_buffer = bytearray()
        sender._true_family_partial_provenance = None
        sender._true_family_partial_frame_type = None
        sender._true_family_partial_num_channels = None

        self.assertTrue(
            await transport.gracefully_finish_output_audio_generation(
                ("response-a", 20),
                websocket,
                lambda: transport.admitted_websocket is websocket,
                timeout_s=1.0,
            )
        )
        for _ in range(100):
            if len(websocket.messages) == 1:
                break
            await asyncio.sleep(0.001)
        self.assertEqual(
            websocket.messages,
            [b"\x07\x00" * 120 + b"\x00" * 240],
        )

        self.assertTrue(
            await transport.bind_output_audio_generation(
                ("response-b", 21),
                websocket,
            )
        )
        response_b = TTSAudioRawFrame(
            audio=b"\x08\x00" * 240,
            sample_rate=24000,
            num_channels=1,
        )
        self.assertTrue(
            transport.register_output_audio_source(
                response_b,
                ("response-b", 21),
                websocket,
            )
        )
        await output.process_frame(response_b, FrameDirection.DOWNSTREAM)
        for _ in range(100):
            if len(websocket.messages) == 2:
                break
            await asyncio.sleep(0.001)
        self.assertEqual(websocket.messages[-1], b"\x08\x00" * 240)
        await output.cancel(CancelFrame())
        await transport.cleanup_uncertain_sockets()

    async def test_phase_emitter_does_not_relabel_queued_work_to_new_wake(self):
        transport = self._transport(serializer=RawAudioSerializer())
        websocket = _Socket()

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, candidate):
            owner_transport.complete_candidate_handler(candidate, True)

        self.assertTrue(await transport._on_client_connected(websocket))
        self.assertTrue(await transport.admit_client(websocket))
        handler = WebSocketHandler(follow_up_ms=0)
        handler.transport = transport
        handler._websockets = {websocket}
        handler._active_session_nonce = 71
        self.assertTrue(handler.note_device_wake(1))
        emitter = PhaseEmitter(
            handler.broadcast_phase,
            capture_phase_context=handler.capture_phase_authorization_context,
            capture_terminal_idle_context=(
                handler.capture_terminal_idle_phase_authorization_context
            ),
        )

        await emitter._phase_transition_lock.acquire()
        queued = asyncio.create_task(emitter._emit("listening"))
        await asyncio.sleep(0)
        self.assertTrue(handler.note_device_wake(2))
        emitter._phase_transition_lock.release()
        await queued

        self.assertEqual(websocket.messages, [])
        self.assertIsNone(emitter._current)
        await transport.retire_client(websocket)
        await transport.cleanup_uncertain_sockets()

    async def test_actual_service_rejects_historical_speech_pair_after_recovery(self):
        answer_started = Mock(return_value=True)
        answer_confirmed = Mock(return_value=True)
        service = SafeRealtimeLLMService(
            api_key="probe",
            max_context_turns=12,
            request_follow_up_answer_started=answer_started,
            request_follow_up_answer_confirmed=answer_confirmed,
        )
        service.push_interruption_task_frame_and_wait = AsyncMock()
        service.push_frame = AsyncMock()
        service._conversation_window.begin_user_turn(
            {
                "id": "old-user",
                "type": "message",
                "role": "user",
                "content": [],
            }
        )
        service._conversation_window.attach_transcript("old-user", "old answer")
        service._conversation_window.activate("old-user")
        self.assertTrue(
            service._conversation_window.finish_response(
                "completed",
                [
                    {
                        "id": "old-assistant",
                        "type": "message",
                        "role": "assistant",
                        "content": [
                            {"type": "output_text", "text": "old reply"}
                        ],
                    }
                ],
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

        token = _CURRENT_REALTIME_SESSION_GENERATION.set(old_generation)
        try:
            await service._handle_evt_speech_started(
                types.SimpleNamespace(item_id="old-user", audio_start_ms=100)
            )
            await service.handle_evt_input_audio_transcription_completed(
                types.SimpleNamespace(item_id="old-user", transcript="old answer")
            )
        finally:
            _CURRENT_REALTIME_SESSION_GENERATION.reset(token)
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
        await service._handle_evt_speech_started(
            types.SimpleNamespace(item_id="fresh-user", audio_start_ms=201)
        )
        service._conversation_window.begin_user_turn(
            {
                "id": "fresh-user",
                "type": "message",
                "role": "user",
                "content": [],
            }
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

    async def test_recovery_epoch_retires_actual_transport_bind_race(self):
        transport = self._transport(serializer=RawAudioSerializer())
        websocket = _Socket()

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, candidate):
            owner_transport.complete_candidate_handler(candidate, True)

        self.assertTrue(await transport._on_client_connected(websocket))
        self.assertTrue(await transport.admit_client(websocket))
        handler = WebSocketHandler(follow_up_ms=0)
        handler.transport = transport
        handler._websockets = {websocket}
        handler._active_session_nonce = 72
        self.assertTrue(handler.note_device_wake(1))

        await transport._owner_lock.acquire()
        binding = asyncio.create_task(
            handler.bind_assistant_output_response("response-racing", 1)
        )
        await asyncio.sleep(0)
        self.assertFalse(binding.done())
        handler._on_connection_recovery_started()
        transport._owner_lock.release()

        self.assertFalse(await binding)
        self.assertIsNone(handler._assistant_output_grant)
        self.assertIsNone(
            transport.output()._true_family_output_generation
        )
        self.assertFalse(
            await handler._authorize_output_audio(
                ("response-racing", 1),
                websocket,
            )
        )
        await transport.retire_client(websocket)
        await transport.cleanup_uncertain_sockets()

    async def test_actual_slow_tool_phase_preserves_a_until_continuation_drain(self):
        serializer = RawAudioSerializer()
        transport = self._transport(
            serializer=serializer,
            audio_out_10ms_chunks=1,
        )
        output = transport.output()
        websocket = _Socket()
        order = []

        async def ordered_send(message):
            if isinstance(message, bytes):
                order.append(("audio", message))
            else:
                order.append(("phase", json.loads(message)["value"]))
            websocket.messages.append(message)

        websocket.send = ordered_send

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, candidate):
            owner_transport.complete_candidate_handler(candidate, True)

        output.create_task = lambda coroutine, *_args, **_kwargs: asyncio.create_task(
            coroutine
        )

        async def cancel_task(task, *_args, **_kwargs):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        output.cancel_task = cancel_task
        output.push_frame = AsyncMock()
        await output.start(StartFrame())
        self.assertTrue(await transport._on_client_connected(websocket))
        self.assertTrue(await transport.admit_client(websocket))

        handler = WebSocketHandler(follow_up_ms=0)
        handler.transport = transport
        handler._websockets = {websocket}
        handler._active_session_nonce = 73
        self.assertTrue(handler.note_device_wake(1))
        transport.set_output_audio_authorizer(handler._authorize_output_audio)
        self.assertTrue(
            await handler.bind_assistant_output_response("response-a", 1)
        )

        service = SafeRealtimeLLMService(api_key="probe", max_context_turns=12)
        service.stop_ttfb_metrics = AsyncMock()
        service.push_error = AsyncMock()

        async def push_frame(frame, *_args, **_kwargs):
            if isinstance(frame, TTSAudioRawFrame):
                await output.process_frame(frame, FrameDirection.DOWNSTREAM)

        service.push_frame = push_frame
        service.set_assistant_output_event_handlers(
            on_audio_frame=handler.register_assistant_output_frame,
            on_before_tool_continuation=handler.finish_assistant_output_response,
        )
        service._active_output_response_context = ("response-a", 1)
        await service._handle_evt_audio_delta(
            events.ResponseAudioDelta(
                event_id="audio-a",
                type="response.output_audio.delta",
                response_id="response-a",
                item_id="assistant-a",
                output_index=0,
                content_index=0,
                delta=base64.b64encode(b"\x0c\x00" * 120).decode("ascii"),
            )
        )
        sender = output._media_senders[None]
        self.assertEqual(
            output._true_family_partial_audio[id(sender)].audio,
            b"\x0c\x00" * 120,
        )
        self.assertFalse(any(isinstance(message, bytes) for message in websocket.messages))

        emitter = PhaseEmitter(
            handler.broadcast_phase,
            idle_debounce_s=0.001,
            capture_phase_context=handler.capture_phase_authorization_context,
            capture_terminal_idle_context=(
                handler.capture_terminal_idle_phase_authorization_context
            ),
        )
        emitter.push_frame = AsyncMock()
        original_in_flight = TURN_LIVENESS.in_flight
        TURN_LIVENESS.in_flight = 1
        try:
            await emitter.process_frame(
                BotStoppedSpeakingFrame(),
                FrameDirection.DOWNSTREAM,
            )
            await asyncio.sleep(0.02)
            self.assertEqual(order, [("phase", "thinking")])
            self.assertEqual(
                transport.output()._true_family_output_generation,
                ("response-a", 1),
            )
            self.assertIsNotNone(handler._assistant_output_grant)

            TURN_LIVENESS.in_flight = 0
            emitter._cancel_watchdog()
            call_id = "slow-tool"
            service._continuation_result_call_ids.add(call_id)
            service._tool_call_output_contexts[call_id] = ("response-a", 1)
            service._response_finished.set()

            async def send_client_event(event):
                _ = event
                order.append(("response", "create-b"))

            service.send_client_event = send_client_event
            await service._run_tool_continuation(service._session_generation)

            self.assertEqual(order[1][0], "audio")
            self.assertEqual(order[2], ("response", "create-b"))
            self.assertEqual(
                order[1][1],
                b"\x0c\x00" * 120 + b"\x00" * 240,
            )
            self.assertTrue(
                await handler.bind_assistant_output_response("response-b", 2)
            )
            service._active_output_response_context = ("response-b", 2)
            service._current_audio_response = None
            await service._handle_evt_audio_delta(
                events.ResponseAudioDelta(
                    event_id="audio-b",
                    type="response.output_audio.delta",
                    response_id="response-b",
                    item_id="assistant-b",
                    output_index=0,
                    content_index=0,
                    delta=base64.b64encode(b"\x0d\x00" * 240).decode("ascii"),
                )
            )
            for _ in range(100):
                if len([event for event in order if event[0] == "audio"]) == 2:
                    break
                await asyncio.sleep(0.001)
            audio = [event[1] for event in order if event[0] == "audio"]
            self.assertEqual(
                audio,
                [
                    b"\x0c\x00" * 120 + b"\x00" * 240,
                    b"\x0d\x00" * 240,
                ],
            )
        finally:
            TURN_LIVENESS.in_flight = original_in_flight
            emitter._cancel_watchdog()
        await output.cancel(CancelFrame())
        await transport.cleanup_uncertain_sockets()

    async def test_actual_cancellation_resistant_write_retires_socket(self):
        transport = self._transport(
            serializer=RawAudioSerializer(),
            audio_out_10ms_chunks=1,
        )
        transport.OUTPUT_WRITE_SETTLE_TIMEOUT_S = 0.01
        output = transport.output()
        websocket = _CancellationResistantAudioSocket()

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, candidate):
            owner_transport.complete_candidate_handler(candidate, True)

        output.create_task = lambda coroutine, *_args, **_kwargs: asyncio.create_task(
            coroutine
        )

        async def cancel_task(task, *_args, **_kwargs):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        output.cancel_task = cancel_task
        output.push_frame = AsyncMock()
        transport.set_output_audio_authorizer(AsyncMock(return_value=True))
        await output.start(StartFrame())
        self.assertTrue(await transport._on_client_connected(websocket))
        self.assertTrue(await transport.admit_client(websocket))
        context = ("response-resistant", 61)
        self.assertTrue(
            await transport.bind_output_audio_generation(context, websocket)
        )
        frame = TTSAudioRawFrame(
            audio=b"\x0b\x00" * 240,
            sample_rate=24000,
            num_channels=1,
        )
        self.assertTrue(
            transport.register_output_audio_source(frame, context, websocket)
        )
        await output.process_frame(frame, FrameDirection.DOWNSTREAM)
        await websocket.send_entered.wait()

        self.assertFalse(await transport.settle_output_audio_generation(context))
        self.assertTrue(websocket.cancellation_resisted.is_set())
        self.assertIs(transport.admitted_websocket, None)
        self.assertEqual(websocket.state, _State.CLOSED)
        websocket.release_send.set()
        for _ in range(100):
            if not output._true_family_active_write_tasks:
                break
            await asyncio.sleep(0.001)
        self.assertEqual(websocket.messages, [])
        self.assertFalse(output._true_family_active_write_tasks)
        await output.cancel(CancelFrame())
        await transport.cleanup_uncertain_sockets()

    async def test_finish_deadline_settles_resistant_write_before_safe_recovery(self):
        transport = self._transport(
            serializer=RawAudioSerializer(),
            audio_out_10ms_chunks=1,
        )
        transport.OUTPUT_WRITE_SETTLE_TIMEOUT_S = 0.01
        output = transport.output()
        websocket = _PersistentlyCancellationResistantAudioSocket()

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, candidate):
            owner_transport.complete_candidate_handler(candidate, True)

        output.create_task = lambda coroutine, *_args, **_kwargs: asyncio.create_task(
            coroutine
        )

        async def cancel_task(task, *_args, **_kwargs):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        output.cancel_task = cancel_task
        output.push_frame = AsyncMock()
        await output.start(StartFrame())
        sender = output._media_senders[None]
        self.assertFalse(sender._audio_task.done())
        self.assertTrue(await transport._on_client_connected(websocket))
        self.assertTrue(await transport.admit_client(websocket))

        handler = WebSocketHandler(follow_up_ms=0)
        handler.transport = transport
        handler._websockets = {websocket}
        handler._active_session_nonce = 74
        self.assertTrue(handler.note_device_wake(1))
        transport.set_output_audio_authorizer(handler._authorize_output_audio)
        self.assertTrue(
            await handler.bind_assistant_output_response("response-deadline", 1)
        )

        service = SafeRealtimeLLMService(api_key="probe", max_context_turns=12)
        service.stop_ttfb_metrics = AsyncMock()
        service.push_error = AsyncMock()
        response_create = AsyncMock()
        service.send_client_event = response_create
        finish_results = []

        async def finish_before_continuation(response_id, response_generation):
            result = await handler.finish_assistant_output_response(
                response_id,
                response_generation,
            )
            finish_results.append(result)
            return result

        async def push_frame(frame, *_args, **_kwargs):
            if isinstance(frame, TTSAudioRawFrame):
                await output.process_frame(frame, FrameDirection.DOWNSTREAM)

        service.push_frame = push_frame
        service.set_assistant_output_event_handlers(
            on_audio_frame=handler.register_assistant_output_frame,
            on_before_tool_continuation=finish_before_continuation,
        )
        service._active_output_response_context = ("response-deadline", 1)
        await service._handle_evt_audio_delta(
            events.ResponseAudioDelta(
                event_id="audio-deadline",
                type="response.output_audio.delta",
                response_id="response-deadline",
                item_id="assistant-deadline",
                output_index=0,
                content_index=0,
                delta=base64.b64encode(b"\x0e\x00" * 240).decode("ascii"),
            )
        )
        await websocket.send_entered.wait()
        self.assertFalse(sender._audio_task.done())

        call_id = "deadline-tool"
        service._continuation_result_call_ids.add(call_id)
        service._tool_call_output_contexts[call_id] = (
            "response-deadline",
            1,
        )
        service._response_finished.set()
        handler._physical_wake_deadline = time.monotonic() + 0.01

        await asyncio.wait_for(
            service._run_tool_continuation(service._session_generation),
            timeout=0.3,
        )

        self.assertEqual(finish_results, [False])
        response_create.assert_not_awaited()
        service.push_error.assert_awaited_once()
        self.assertTrue(service._recovery_active)
        self.assertTrue(websocket.cancellation_resisted.is_set())
        self.assertEqual(websocket.state, _State.CLOSED)
        self.assertIsNone(transport.admitted_websocket)
        self.assertIsNone(output._websocket)
        self.assertIsNone(output._true_family_output_generation)
        self.assertIsNone(handler._assistant_output_grant)
        self.assertEqual(handler._websockets, set())

        websocket.release_send.set()
        for _ in range(100):
            if not output._true_family_active_write_tasks:
                break
            await asyncio.sleep(0.001)
        self.assertEqual(websocket.messages, [])
        self.assertFalse(output._true_family_chunk_contexts)
        self.assertFalse(output._true_family_active_write_contexts)
        self.assertFalse(output._true_family_active_write_tasks)
        self.assertFalse(sender._audio_task.done())

        replacement = _Socket()
        self.assertTrue(await transport._on_client_connected(replacement))
        self.assertTrue(await transport.admit_client(replacement))
        service.mark_recovery_complete()
        handler._websockets = {replacement}
        handler._active_session_nonce = 75
        self.assertTrue(handler.note_device_wake(2))
        self.assertTrue(
            await handler.bind_assistant_output_response("response-recovered", 2)
        )
        self.assertIs(transport.admitted_websocket, replacement)

        await output.cancel(CancelFrame())
        await transport.cleanup_uncertain_sockets()

    async def test_actual_cancellation_resistant_close_is_bounded_and_aborted(self):
        transport = self._transport(serializer=RawAudioSerializer())
        transport.SOCKET_CLOSE_TIMEOUT_S = 0.001
        websocket = _CancellationResistantCloseSocket()

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, candidate):
            owner_transport.complete_candidate_handler(candidate, True)

        self.assertTrue(await transport._on_client_connected(websocket))
        self.assertTrue(await transport.admit_client(websocket))
        retirement = asyncio.create_task(transport.retire_client(websocket))
        await websocket.close_entered.wait()
        self.assertIsNone(transport.admitted_websocket)
        self.assertIsNone(transport.output()._websocket)

        self.assertTrue(await asyncio.wait_for(retirement, timeout=0.2))
        self.assertTrue(websocket.cancellation_resisted.is_set())
        self.assertEqual(websocket.abort_count, 1)
        self.assertEqual(websocket.state, _State.CLOSED)
        await transport.cleanup_uncertain_sockets()

    async def test_actual_finish_waits_for_source_currently_in_chunker(self):
        transport = self._transport(
            serializer=RawAudioSerializer(),
            audio_out_10ms_chunks=1,
        )
        output = transport.output()
        websocket = _Socket()

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, candidate):
            owner_transport.complete_candidate_handler(candidate, True)

        output.create_task = lambda coroutine, *_args, **_kwargs: asyncio.create_task(
            coroutine
        )

        async def cancel_task(task, *_args, **_kwargs):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        output.cancel_task = cancel_task
        output.push_frame = AsyncMock()
        transport.set_output_audio_authorizer(AsyncMock(return_value=True))
        await output.start(StartFrame())
        sender = output._media_senders[None]
        original_chunker = sender._true_family_handle_audio_frame
        entered = asyncio.Event()
        release = asyncio.Event()

        async def blocked_chunker(frame):
            entered.set()
            await release.wait()
            await original_chunker(frame)

        sender._true_family_handle_audio_frame = blocked_chunker
        self.assertTrue(await transport._on_client_connected(websocket))
        self.assertTrue(await transport.admit_client(websocket))
        context = ("response-a", 30)
        self.assertTrue(await transport.bind_output_audio_generation(context, websocket))
        frame = TTSAudioRawFrame(
            audio=b"\x09\x00" * 240,
            sample_rate=24000,
            num_channels=1,
        )
        self.assertTrue(
            transport.register_output_audio_source(frame, context, websocket)
        )
        processing = asyncio.create_task(
            output.process_frame(frame, FrameDirection.DOWNSTREAM)
        )
        await entered.wait()
        finish = asyncio.create_task(
            transport.gracefully_finish_output_audio_generation(
                context,
                websocket,
                lambda: transport.admitted_websocket is websocket,
                timeout_s=1.0,
            )
        )
        await asyncio.sleep(0)
        self.assertFalse(finish.done())
        release.set()
        await processing
        self.assertTrue(await finish)
        self.assertEqual(websocket.messages, [b"\x09\x00" * 240])
        await output.cancel(CancelFrame())
        await transport.cleanup_uncertain_sockets()

    async def test_actual_finish_no_audio_fast_path(self):
        transport = self._transport(serializer=RawAudioSerializer())
        websocket = _Socket()

        @transport.event_handler("on_client_connected")
        async def on_connected(owner_transport, candidate):
            owner_transport.complete_candidate_handler(candidate, True)

        self.assertTrue(await transport._on_client_connected(websocket))
        self.assertTrue(await transport.admit_client(websocket))
        context = ("response-a", 40)
        self.assertTrue(await transport.bind_output_audio_generation(context, websocket))
        self.assertTrue(
            await transport.gracefully_finish_output_audio_generation(
                context,
                websocket,
                lambda: True,
                timeout_s=0.1,
            )
        )
        self.assertEqual(websocket.messages, [])
        await transport.cleanup()

    async def test_actual_finish_timeout_and_socket_write_failure_discard_a(self):
        for path in ("timeout", "write_failure"):
            with self.subTest(path=path):
                transport = self._transport(
                    serializer=RawAudioSerializer(),
                    audio_out_10ms_chunks=1,
                )
                output = transport.output()
                websocket = _Socket() if path == "timeout" else _FailingAudioSocket()

                @transport.event_handler("on_client_connected")
                async def on_connected(owner_transport, candidate):
                    owner_transport.complete_candidate_handler(candidate, True)

                output.create_task = (
                    lambda coroutine, *_args, **_kwargs: asyncio.create_task(coroutine)
                )

                async def cancel_task(task, *_args, **_kwargs):
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

                output.cancel_task = cancel_task
                output.push_frame = AsyncMock()
                transport.set_output_audio_authorizer(AsyncMock(return_value=True))
                await output.start(StartFrame())
                sender = output._media_senders[None]
                if path == "timeout":
                    await sender._cancel_audio_task()
                self.assertTrue(await transport._on_client_connected(websocket))
                self.assertTrue(await transport.admit_client(websocket))
                context = (f"response-{path}", 50)
                self.assertTrue(
                    await transport.bind_output_audio_generation(context, websocket)
                )
                frame = TTSAudioRawFrame(
                    audio=b"\x0a\x00" * 240,
                    sample_rate=24000,
                    num_channels=1,
                )
                self.assertTrue(
                    transport.register_output_audio_source(
                        frame,
                        context,
                        websocket,
                    )
                )
                await output.process_frame(frame, FrameDirection.DOWNSTREAM)

                self.assertFalse(
                    await transport.gracefully_finish_output_audio_generation(
                        context,
                        websocket,
                        lambda: transport.admitted_websocket is websocket,
                        timeout_s=0.02 if path == "timeout" else 1.0,
                    )
                )
                self.assertIsNone(output._true_family_output_generation)
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
