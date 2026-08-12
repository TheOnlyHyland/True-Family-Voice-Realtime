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
from unittest.mock import AsyncMock, Mock, patch


ADDON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ADDON_ROOT))

from app.main import (  # noqa: E402
    Application,
    REQUEST_FOLLOW_UP_PURPOSE,
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
    FunctionCallInProgressFrame,
    FunctionCallResultFrame,
    FunctionCallsStartedFrame,
    LLMContextFrame,
    LLMFullResponseEndFrame,
    LLMTextFrame,
    OutputAudioRawFrame,
    StartFrame,
    TTSAudioRawFrame,
    TTSTextFrame,
)
from pipecat.processors.aggregators.llm_context import LLMContext  # noqa: E402
from pipecat.processors.aggregators.llm_response_universal import (  # noqa: E402
    LLMContextAggregatorPair,
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

    @staticmethod
    def _message(item_id, role, text=""):
        content_type = "input_audio" if role == "user" else "output_audio"
        return events.ConversationItem(
            id=item_id,
            type="message",
            status="completed",
            role=role,
            content=[
                events.ItemContent(
                    type=content_type,
                    transcript=text,
                )
            ],
        )

    @staticmethod
    def _response(response_id, status, output, *, status_details=None):
        return events.Response(
            id=response_id,
            object="realtime.response",
            status=status,
            status_details=status_details,
            output=output,
            usage=events.Usage(
                total_tokens=0,
                input_tokens=0,
                output_tokens=0,
                input_token_details=events.TokenDetails(),
                output_token_details=events.TokenDetails(),
            ),
        )

    def _wire_actual_aggregator(self, service, context):
        pair = LLMContextAggregatorPair(context)
        assistant = pair.assistant()
        service._context = context
        service.bind_context_aggregator(pair)
        created_tasks = []

        def create_task(coroutine, *_args, **_kwargs):
            task = asyncio.create_task(coroutine)
            created_tasks.append(task)
            return task

        service.create_task = create_task
        assistant.create_task = create_task
        downstream_frames = []

        async def assistant_push(frame, direction=FrameDirection.DOWNSTREAM):
            if isinstance(frame, LLMContextFrame) and direction is FrameDirection.UPSTREAM:
                await service._handle_context(frame.context)
            else:
                downstream_frames.append((frame, direction))

        assistant.push_frame = assistant_push

        async def broadcast(frame_type, **kwargs):
            frame = frame_type(**kwargs)
            self.assertIsInstance(
                frame,
                (
                    FunctionCallsStartedFrame,
                    FunctionCallInProgressFrame,
                    FunctionCallResultFrame,
                ),
            )
            downstream_frames.append(
                (
                    frame,
                    [
                        dict(message)
                        if isinstance(message, dict)
                        else message
                        for message in context.get_messages()
                    ],
                )
            )
            await assistant.process_frame(frame, FrameDirection.DOWNSTREAM)

        service.broadcast_frame = broadcast
        return pair, assistant, created_tasks, downstream_frames

    async def _wait_for_control(self, websocket, control_type):
        for _ in range(1000):
            for message in websocket.messages:
                if not isinstance(message, str):
                    continue
                payload = json.loads(message)
                if payload.get("type") == control_type:
                    return payload
            await asyncio.sleep(0.002)
        self.fail(f"{control_type} control was not emitted")

    async def _confirm_actual_follow_up_answer(
        self,
        handler,
        websocket,
        *,
        transcript,
        tool_call_id,
        item_id,
        sequence,
    ):
        outcome = await handler.reserve_request_follow_up(tool_call_id)
        self.assertEqual(outcome.value, "reserved")
        self.assertTrue(handler.activate_request_follow_up(tool_call_id))
        self.assertTrue(handler.arm_request_follow_up_continuation({tool_call_id}))
        self.assertTrue(
            handler.bind_request_follow_up_response(
                f"{tool_call_id}-question",
                1,
            )
        )
        handler.note_request_follow_up_response_audio(f"{tool_call_id}-question")
        handler.note_request_follow_up_playback_started()
        handler.note_request_follow_up_response_done(
            f"{tool_call_id}-question",
            "completed",
        )
        idle_task = asyncio.create_task(handler._before_reply_idle())
        request_payload = await self._wait_for_control(
            websocket,
            "request_follow_up",
        )
        handler._handle_request_follow_up_ack(
            {
                "type": "request_follow_up_ack",
                "token": request_payload["token"],
                "session_nonce": request_payload["session_nonce"],
                "accepted": True,
            }
        )
        await idle_task
        reservation = handler._request_follow_up_reservation
        if reservation is None:
            self.fail("follow-up reservation disappeared before READY")
        await handler._handle_device_control_message(
            {
                "type": "follow_up_ready",
                "token": reservation.token,
                "session_nonce": reservation.session_nonce,
                "ready_nonce": sequence + 10000,
            },
            websocket,
        )
        commit_payload = await self._wait_for_control(
            websocket,
            "commit_follow_up",
        )
        await handler._handle_device_control_message(
            {
                **commit_payload,
                "type": "commit_follow_up_ack",
                "accepted": True,
            },
            websocket,
        )
        await handler._await_request_follow_up_settlements()
        self.assertEqual(reservation.stage.name, "OPEN")
        handler.note_request_follow_up_turn_boundary()
        self.assertTrue(handler.bind_request_follow_up_answer(item_id, sequence))
        self.assertTrue(
            handler.confirm_request_follow_up_answer(
                item_id,
                sequence,
                transcript,
            )
        )

    def test_response_create_preserves_explicit_tool_disable(self):
        event = events.ResponseCreateEvent(
            response=events.ResponseProperties(
                output_modalities=["audio"],
                tools=[],
                tool_choice="none",
            )
        )

        payload = event.model_dump(exclude_none=True)

        self.assertEqual(payload["response"]["tools"], [])
        self.assertEqual(payload["response"]["tool_choice"], "none")

    async def test_actual_mixed_follow_up_normalizes_through_result_aggregator(self):
        usage_patch = patch("app.ha_sensors.PUBLISHER.usage", new=AsyncMock())
        usage_patch.start()
        self.addCleanup(usage_patch.stop)
        context = LLMContext(messages=[{"role": "user", "content": "Help me choose"}])
        service = SafeRealtimeLLMService(
            api_key="probe",
            max_context_turns=12,
            authorized_tool_names=("request_follow_up", "end_conversation"),
        )
        pair, _assistant, created_tasks, aggregator_frames = (
            self._wire_actual_aggregator(service, context)
        )
        service._conversation_window.begin_user_turn(
            self._message("mixed-user", "user").model_dump(exclude_none=True)
        )
        service._conversation_window.attach_transcript(
            "mixed-user",
            "Help me choose",
        )
        service._conversation_window.activate("mixed-user")
        service._context = context
        service._api_session_ready = True
        service._current_audio_response = None
        service.stop_ttfb_metrics = AsyncMock()
        service.start_llm_usage_metrics = AsyncMock()
        service.stop_processing_metrics = AsyncMock()
        service.retrieve_conversation_item = AsyncMock(
            side_effect=Exception("invalid item id")
        )
        service.push_error = AsyncMock()
        sent_events = []

        async def send_client_event(event):
            sent_events.append(event)

        service.send_client_event = send_client_event
        physical_frames = []

        async def push_frame(frame, direction=FrameDirection.DOWNSTREAM):
            physical_frames.append(frame)
            await pair.assistant().process_frame(frame, direction)

        service.push_frame = push_frame
        service._assistant_output_response_created = AsyncMock(return_value=True)
        service._assistant_output_frame_created = Mock(return_value=True)

        websocket = _Socket()
        handler = WebSocketHandler(follow_up_ms=0)
        handler._websockets = {websocket}
        handler._active_session_nonce = 9101
        handler._clear_device_input = AsyncMock()
        self.assertTrue(handler.note_device_wake(1))
        service.set_request_follow_up_event_handlers(
            on_response_created=handler.bind_request_follow_up_response,
            on_response_audio=handler.note_request_follow_up_response_audio,
            on_response_done=handler.note_request_follow_up_response_done,
            on_response_failed=handler.note_request_follow_up_response_failed,
            on_continuation_arm=handler.arm_request_follow_up_continuation,
            on_continuation_failed=handler.fail_request_follow_up_continuation,
            on_question_output_authorized=(
                handler.request_follow_up_question_output_is_current
            ),
        )
        application = Application()
        application.request_follow_up_supported = True
        application.openai_service = service
        application.websocket_handler = handler
        application._register_conversation_control_tool()

        response_id = "mixed-follow-up-a"
        call_id = "mixed-follow-up-call"
        assistant_item = self._message(
            "mixed-unheard-question",
            "assistant",
            "Which cuisine?",
        )
        function_item = events.ConversationItem(
            id="mixed-follow-up-item",
            type="function_call",
            status="completed",
            call_id=call_id,
            name="request_follow_up",
            arguments=json.dumps({"purpose": REQUEST_FOLLOW_UP_PURPOSE}),
        )
        service._active_response_id = response_id
        service._output_response_generation = 1
        service._active_output_response_context = (response_id, 1)
        self.assertTrue(service._begin_decision_output_hold(response_id, 1))
        for item in (assistant_item, function_item):
            await service._handle_evt_conversation_item_added(
                types.SimpleNamespace(item=item)
            )
        await service._handle_evt_function_call_arguments_done(
            events.ResponseFunctionCallArgumentsDone(
                event_id="mixed-args-done",
                type="response.function_call_arguments.done",
                response_id=response_id,
                item_id=function_item.id,
                output_index=1,
                call_id=call_id,
                arguments=function_item.arguments,
            )
        )
        for _ in range(500):
            if call_id in service._running_tool_call_ids:
                break
            await asyncio.sleep(0.002)
        self.assertIn(
            call_id,
            service._running_tool_call_ids,
            msg=(
                f"scheduled={service._scheduled_tool_call_ids} "
                f"pending={list(service._pending_function_calls)} "
                f"tasks={[repr(task.exception()) if task.done() and not task.cancelled() else repr(task) for task in created_tasks]}"
            ),
        )
        await service._handle_evt_audio_delta(
            events.ResponseAudioDelta(
                event_id="mixed-audio-a",
                type="response.output_audio.delta",
                response_id=response_id,
                item_id=assistant_item.id,
                output_index=0,
                content_index=0,
                delta=base64.b64encode(b"unheard mixed question").decode("ascii"),
            )
        )
        self.assertFalse(
            any(isinstance(frame, TTSAudioRawFrame) for frame in physical_frames)
        )
        service.stop_ttfb_metrics.assert_not_awaited()

        await service._handle_evt_response_done(
            events.ResponseDone(
                event_id="mixed-response-done",
                type="response.done",
                response=self._response(
                    response_id,
                    "completed",
                    [assistant_item, function_item],
                ),
            )
        )
        for _ in range(1000):
            if any(
                getattr(event, "type", None) == "response.create"
                and event.response is not None
                and event.response.tools == []
                and event.response.tool_choice == "none"
                for event in sent_events
            ):
                break
            await asyncio.sleep(0.002)
        no_tools_events = [
            event
            for event in sent_events
            if getattr(event, "type", None) == "response.create"
            and event.response is not None
            and event.response.tools == []
            and event.response.tool_choice == "none"
        ]
        self.assertEqual(len(no_tools_events), 1)
        result_snapshots = [
            snapshot
            for frame, snapshot in aggregator_frames
            if isinstance(frame, FunctionCallResultFrame)
            and frame.tool_call_id == call_id
        ]
        self.assertEqual(len(result_snapshots), 1)
        self.assertEqual(
            [
                message
                for message in result_snapshots[0]
                if message.get("role") == "tool"
                and message.get("tool_call_id") == call_id
            ],
            [
                {
                    "role": "tool",
                    "content": "IN_PROGRESS",
                    "tool_call_id": call_id,
                }
            ],
        )
        self.assertEqual(
            [
                event.item_id
                for event in sent_events
                if getattr(event, "type", None) == "conversation.item.delete"
            ],
            [assistant_item.id],
        )

        tool_output = events.ConversationItem(
            id="mixed-follow-up-output",
            type="function_call_output",
            status="completed",
            call_id=call_id,
            output=json.dumps({"status": "follow_up_reserved"}),
        )
        response_b_item = self._message(
            "mixed-follow-up-question",
            "assistant",
            "Which cuisine would you like?",
        )
        response_b = self._response(
            "mixed-follow-up-b",
            "completed",
            [response_b_item],
        )
        keep_reader_open = asyncio.Event()

        async def response_b_events():
            payloads = [
                events.ConversationItemAdded(
                    event_id="mixed-output-added",
                    type="conversation.item.added",
                    item=tool_output,
                ).model_dump(),
                events.ResponseCreated(
                    event_id="mixed-b-created",
                    type="response.created",
                    response=self._response(
                        "mixed-follow-up-b",
                        "in_progress",
                        [],
                    ),
                ).model_dump(),
                events.ConversationItemAdded(
                    event_id="mixed-b-item-added",
                    type="conversation.item.added",
                    item=response_b_item,
                ).model_dump(),
                events.ResponseAudioTranscriptDelta(
                    event_id="mixed-b-transcript",
                    type="response.output_audio_transcript.delta",
                    response_id="mixed-follow-up-b",
                    item_id=response_b_item.id,
                    output_index=0,
                    content_index=0,
                    delta="Which cuisine would you like?",
                ).model_dump(),
                events.ResponseAudioDelta(
                    event_id="mixed-b-audio",
                    type="response.output_audio.delta",
                    response_id="mixed-follow-up-b",
                    item_id=response_b_item.id,
                    output_index=0,
                    content_index=0,
                    delta=base64.b64encode(b"one physical question").decode("ascii"),
                ).model_dump(),
                events.ResponseDone(
                    event_id="mixed-b-done",
                    type="response.done",
                    response=response_b,
                ).model_dump(),
            ]
            for payload in payloads:
                yield json.dumps(payload)
            await keep_reader_open.wait()

        service._websocket = response_b_events()
        receive_task = asyncio.create_task(service._receive_task_handler())
        for _ in range(1000):
            if service._decision_output_hold is None and handler._request_follow_up_reservation:
                reservation = handler._request_follow_up_reservation
                if reservation.response_completed:
                    break
            await asyncio.sleep(0.002)
        audio = [
            frame.audio
            for frame in physical_frames
            if isinstance(frame, TTSAudioRawFrame)
        ]
        reader_error = (
            receive_task.exception()
            if receive_task.done() and not receive_task.cancelled()
            else None
        )
        self.assertEqual(
            audio,
            [b"one physical question"],
            msg=(
                f"recovery={service._recovery_active} "
                f"hold={service._decision_output_hold!r} "
                f"mode={service._tool_disabled_response_modes!r} "
                f"reservation={handler._request_follow_up_reservation!r} "
                f"reader_error={reader_error!r} "
                f"errors={service.push_error.await_args_list!r}"
            ),
        )
        self.assertNotIn(b"unheard mixed question", audio)
        service.stop_ttfb_metrics.assert_awaited_once()
        bound_reservation = handler._request_follow_up_reservation
        if bound_reservation is None:
            self.fail("bound follow-up reservation disappeared")
        self.assertEqual(bound_reservation.response_generation, 2)
        self.assertEqual(
            bound_reservation.expires_at,
            handler._physical_wake_deadline,
        )
        self.assertGreater(
            bound_reservation.expires_at - time.monotonic(),
            15.0,
        )
        self.assertNotIn(
            assistant_item.id,
            [
                item.get("id")
                for turn in service._conversation_window.turns
                for item in turn.items
            ],
        )
        self.assertNotIn(
            "Which cuisine?",
            str(context.get_messages()),
        )
        replay_items = [
            item.get("id")
            for turn in service._conversation_window.replay_snapshot()
            for item in turn.items
        ]
        self.assertNotIn(assistant_item.id, replay_items)

        handler.note_request_follow_up_playback_started()
        idle_task = asyncio.create_task(handler._before_reply_idle())
        request_payload = None
        for _ in range(1000):
            for message in websocket.messages:
                if not isinstance(message, str):
                    continue
                candidate = json.loads(message)
                if candidate.get("type") == "request_follow_up":
                    request_payload = candidate
                    break
            if request_payload is not None:
                break
            await asyncio.sleep(0.002)
        self.assertIsNotNone(request_payload)
        if request_payload is None:
            self.fail("request_follow_up control was not emitted")
        handler._handle_request_follow_up_ack(
            {
                "type": "request_follow_up_ack",
                "token": request_payload["token"],
                "session_nonce": request_payload["session_nonce"],
                "accepted": True,
            }
        )
        await idle_task
        reservation = handler._request_follow_up_reservation
        self.assertIsNotNone(reservation)
        if reservation is None:
            self.fail("follow-up reservation disappeared before READY")
        ready_nonce = 9102
        await handler._handle_device_control_message(
            {
                "type": "follow_up_ready",
                "token": reservation.token,
                "session_nonce": reservation.session_nonce,
                "ready_nonce": ready_nonce,
            },
            websocket,
        )
        commit_payload = None
        for _ in range(1000):
            for message in websocket.messages:
                if not isinstance(message, str):
                    continue
                candidate = json.loads(message)
                if candidate.get("type") == "commit_follow_up":
                    commit_payload = candidate
                    break
            if commit_payload is not None:
                break
            await asyncio.sleep(0.002)
        self.assertIsNotNone(commit_payload)
        if commit_payload is None:
            self.fail("commit_follow_up control was not emitted")
        await handler._handle_device_control_message(
            {
                **commit_payload,
                "type": "commit_follow_up_ack",
                "accepted": True,
            },
            websocket,
        )
        await handler._await_request_follow_up_settlements()
        self.assertEqual(reservation.stage.name, "OPEN")
        handler.cancel_request_follow_up(send_cancel=False)

        receive_task.cancel()
        await asyncio.gather(receive_task, return_exceptions=True)
        service.begin_recovery()
        for task in created_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*created_tasks, return_exceptions=True)
        await handler.cleanup()

    async def test_actual_mixed_end_normalizes_before_spoken_or_silent_close(self):
        usage_patch = patch("app.ha_sensors.PUBLISHER.usage", new=AsyncMock())
        usage_patch.start()
        self.addCleanup(usage_patch.stop)

        async def run_case(transcript, *, semantic_veto, case_number):
            websocket = _Socket()
            handler = WebSocketHandler(follow_up_ms=0)
            handler._websockets = {websocket}
            handler._active_session_nonce = 9200 + case_number
            handler._clear_device_input = AsyncMock()
            self.assertTrue(handler.note_device_wake(1))
            user_item_id = f"mixed-end-user-{case_number}"
            sequence = 20 + case_number
            await self._confirm_actual_follow_up_answer(
                handler,
                websocket,
                transcript=transcript,
                tool_call_id=f"prior-follow-up-{case_number}",
                item_id=user_item_id,
                sequence=sequence,
            )
            grant = handler._request_follow_up_answer_grant
            self.assertIsNotNone(grant)
            if grant is None:
                self.fail("confirmed follow-up answer grant was not retained")
            self.assertEqual(grant.semantic_close_veto, semantic_veto)

            context = LLMContext(messages=[{"role": "user", "content": transcript}])
            service = SafeRealtimeLLMService(
                api_key="probe",
                max_context_turns=12,
                authorized_tool_names=("request_follow_up", "end_conversation"),
            )
            pair, _assistant, created_tasks, aggregator_frames = (
                self._wire_actual_aggregator(service, context)
            )
            service._conversation_window.begin_user_turn(
                self._message(user_item_id, "user").model_dump(exclude_none=True)
            )
            service._conversation_window.attach_transcript(user_item_id, transcript)
            service._conversation_window.activate(user_item_id)
            service._confirmed_follow_up_answer_identity = (user_item_id, sequence)
            service._api_session_ready = True
            service._current_audio_response = None
            service.stop_ttfb_metrics = AsyncMock()
            service.start_llm_usage_metrics = AsyncMock()
            service.stop_processing_metrics = AsyncMock()
            service.retrieve_conversation_item = AsyncMock(
                side_effect=Exception("invalid item id")
            )
            service.push_error = AsyncMock()
            sent_events = []

            async def send_client_event(event):
                sent_events.append(event)

            service.send_client_event = send_client_event
            physical_frames = []

            async def push_frame(frame, direction=FrameDirection.DOWNSTREAM):
                physical_frames.append(frame)
                await pair.assistant().process_frame(frame, direction)

            service.push_frame = push_frame
            service._assistant_output_response_created = AsyncMock(return_value=True)
            service._assistant_output_frame_created = Mock(return_value=True)
            service.set_spoken_close_response_authorizer(
                handler.silent_close_requires_spoken_response
            )
            close_silently = AsyncMock()
            handler.request_silent_close = close_silently
            application = Application()
            application.request_follow_up_supported = True
            application.openai_service = service
            application.websocket_handler = handler
            application._register_conversation_control_tool()

            response_id = f"mixed-end-a-{case_number}"
            call_id = f"mixed-end-call-{case_number}"
            unheard_item = self._message(
                f"mixed-end-unheard-{case_number}",
                "assistant",
                "Unheard goodbye",
            )
            function_item = events.ConversationItem(
                id=f"mixed-end-function-{case_number}",
                type="function_call",
                status="completed",
                call_id=call_id,
                name="end_conversation",
                arguments="{}",
            )
            service._active_response_id = response_id
            service._output_response_generation = 1
            service._active_output_response_context = (response_id, 1)
            self.assertTrue(service._begin_decision_output_hold(response_id, 1))
            for item in (unheard_item, function_item):
                await service._handle_evt_conversation_item_added(
                    types.SimpleNamespace(item=item)
                )
            await service._handle_evt_function_call_arguments_done(
                events.ResponseFunctionCallArgumentsDone(
                    event_id=f"mixed-end-args-{case_number}",
                    type="response.function_call_arguments.done",
                    response_id=response_id,
                    item_id=function_item.id,
                    output_index=1,
                    call_id=call_id,
                    arguments="{}",
                )
            )
            for _ in range(500):
                if call_id in service._running_tool_call_ids:
                    break
                await asyncio.sleep(0.002)
            self.assertIn(call_id, service._running_tool_call_ids)
            await service._handle_evt_audio_delta(
                events.ResponseAudioDelta(
                    event_id=f"mixed-end-audio-{case_number}",
                    type="response.output_audio.delta",
                    response_id=response_id,
                    item_id=unheard_item.id,
                    output_index=0,
                    content_index=0,
                    delta=base64.b64encode(b"unheard goodbye pcm").decode("ascii"),
                )
            )
            await service._handle_evt_response_done(
                events.ResponseDone(
                    event_id=f"mixed-end-done-{case_number}",
                    type="response.done",
                    response=self._response(
                        response_id,
                        "completed",
                        [unheard_item, function_item],
                    ),
                )
            )

            for _ in range(1000):
                result_seen = any(
                    isinstance(frame, FunctionCallResultFrame)
                    and frame.tool_call_id == call_id
                    for frame, _snapshot in aggregator_frames
                )
                if result_seen:
                    break
                await asyncio.sleep(0.002)
            result_snapshots = [
                snapshot
                for frame, snapshot in aggregator_frames
                if isinstance(frame, FunctionCallResultFrame)
                and frame.tool_call_id == call_id
            ]
            self.assertEqual(len(result_snapshots), 1)
            self.assertEqual(
                [
                    message
                    for message in result_snapshots[0]
                    if message.get("role") == "tool"
                    and message.get("tool_call_id") == call_id
                ],
                [
                    {
                        "role": "tool",
                        "content": "IN_PROGRESS",
                        "tool_call_id": call_id,
                    }
                ],
            )
            self.assertEqual(
                [
                    event.item_id
                    for event in sent_events
                    if getattr(event, "type", None) == "conversation.item.delete"
                ],
                [unheard_item.id],
            )
            tool_output = events.ConversationItem(
                id=f"mixed-end-output-{case_number}",
                type="function_call_output",
                status="completed",
                call_id=call_id,
                output=json.dumps(
                    {
                        "status": (
                            "spoken_response_required"
                            if semantic_veto
                            else "closed_silently"
                        )
                    }
                ),
            )
            await service._handle_evt_conversation_item_added(
                types.SimpleNamespace(item=tool_output)
            )

            if semantic_veto:
                close_silently.assert_not_awaited()
                for _ in range(1000):
                    if any(
                        getattr(event, "type", None) == "response.create"
                        and event.response is not None
                        and event.response.tools == []
                        and event.response.tool_choice == "none"
                        for event in sent_events
                    ):
                        break
                    await asyncio.sleep(0.002)
                response_b_item = self._message(
                    f"mixed-end-ack-{case_number}",
                    "assistant",
                    "Of course.",
                )
                keep_reader_open = asyncio.Event()

                async def response_b_events():
                    payloads = [
                        events.ResponseCreated(
                            event_id=f"mixed-end-b-created-{case_number}",
                            type="response.created",
                            response=self._response(
                                f"mixed-end-b-{case_number}",
                                "in_progress",
                                [],
                            ),
                        ).model_dump(),
                        events.ConversationItemAdded(
                            event_id=f"mixed-end-b-item-{case_number}",
                            type="conversation.item.added",
                            item=response_b_item,
                        ).model_dump(),
                        events.ResponseAudioTranscriptDelta(
                            event_id=f"mixed-end-b-text-{case_number}",
                            type="response.output_audio_transcript.delta",
                            response_id=f"mixed-end-b-{case_number}",
                            item_id=response_b_item.id,
                            output_index=0,
                            content_index=0,
                            delta="Of course.",
                        ).model_dump(),
                        events.ResponseAudioDelta(
                            event_id=f"mixed-end-b-audio-{case_number}",
                            type="response.output_audio.delta",
                            response_id=f"mixed-end-b-{case_number}",
                            item_id=response_b_item.id,
                            output_index=0,
                            content_index=0,
                            delta=base64.b64encode(b"one acknowledgement").decode(
                                "ascii"
                            ),
                        ).model_dump(),
                        events.ResponseDone(
                            event_id=f"mixed-end-b-done-{case_number}",
                            type="response.done",
                            response=self._response(
                                f"mixed-end-b-{case_number}",
                                "completed",
                                [response_b_item],
                            ),
                        ).model_dump(),
                    ]
                    for payload in payloads:
                        yield json.dumps(payload)
                    await keep_reader_open.wait()

                service._websocket = response_b_events()
                receive_task = asyncio.create_task(service._receive_task_handler())
                for _ in range(1000):
                    if service._decision_output_hold is None and not (
                        service._tool_disabled_response_modes
                    ):
                        if any(
                            isinstance(frame, TTSAudioRawFrame)
                            for frame in physical_frames
                        ):
                            break
                    await asyncio.sleep(0.002)
                receive_task.cancel()
                await asyncio.gather(receive_task, return_exceptions=True)
                audio = [
                    frame.audio
                    for frame in physical_frames
                    if isinstance(frame, TTSAudioRawFrame)
                ]
                self.assertEqual(audio, [b"one acknowledgement"])
            else:
                close_silently.assert_awaited_once_with()
                for _ in range(1000):
                    if service._conversation_window.active_turn_id is None:
                        break
                    await asyncio.sleep(0.002)
                self.assertIsNone(service._conversation_window.active_turn_id)
                self.assertFalse(
                    any(
                        getattr(event, "type", None) == "response.create"
                        for event in sent_events
                    )
                )
                self.assertFalse(
                    any(
                        isinstance(frame, TTSAudioRawFrame)
                        for frame in physical_frames
                    )
                )

            self.assertNotIn("Unheard goodbye", str(context.get_messages()))
            replay_items = [
                item.get("id")
                for turn in service._conversation_window.replay_snapshot()
                for item in turn.items
            ]
            self.assertNotIn(unheard_item.id, replay_items)
            self.assertFalse(service._recovery_active)
            handler.invalidate_request_follow_up_turn(send_cancel=False)
            service.begin_recovery()
            for task in created_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*created_tasks, return_exceptions=True)
            await handler.cleanup()

        await run_case(
            "thanks i know what i want now a chinese",
            semantic_veto=True,
            case_number=1,
        )
        await run_case(
            "purple dishwasher",
            semantic_veto=False,
            case_number=2,
        )

    async def test_actual_tool_disabled_response_cannot_dispatch_mutation(self):
        context = LLMContext(messages=[{"role": "user", "content": "Continue"}])
        service = SafeRealtimeLLMService(
            api_key="probe",
            max_context_turns=12,
            authorized_tool_names=("mutate_home",),
        )
        service._context = context
        service._conversation_window.begin_user_turn(
            self._message("mutation-user", "user").model_dump(exclude_none=True)
        )
        service._conversation_window.attach_transcript("mutation-user", "Continue")
        service._conversation_window.activate("mutation-user")
        mutation = AsyncMock()
        service.register_function("mutate_home", mutation)
        service._pending_tool_disabled_response_mode = "follow_up_question"
        service._request_follow_up_response_created = Mock(return_value=True)
        service._assistant_output_response_created = AsyncMock(return_value=True)
        service._assistant_output_frame_created = Mock(return_value=True)
        service._current_audio_response = None
        service.stop_ttfb_metrics = AsyncMock()
        service.push_error = AsyncMock()
        physical_frames = []
        service.push_frame = AsyncMock(
            side_effect=lambda frame, *_args: physical_frames.append(frame)
        )
        sent_events = []

        async def send_client_event(event):
            sent_events.append(event)

        service.send_client_event = send_client_event
        function_item = events.ConversationItem(
            id="forbidden-mutation-item",
            type="function_call",
            status="completed",
            call_id="forbidden-mutation-call",
            name="mutate_home",
            arguments="{}",
        )
        keep_reader_open = asyncio.Event()

        async def server_events():
            payloads = [
                events.ResponseCreated(
                    event_id="mutation-response-created",
                    type="response.created",
                    response=self._response(
                        "mutation-response",
                        "in_progress",
                        [],
                    ),
                ).model_dump(),
                events.ResponseAudioDelta(
                    event_id="mutation-preface-audio",
                    type="response.output_audio.delta",
                    response_id="mutation-response",
                    item_id="mutation-preface",
                    output_index=0,
                    content_index=0,
                    delta=base64.b64encode(b"must stay quarantined").decode("ascii"),
                ).model_dump(),
                events.ConversationItemAdded(
                    event_id="mutation-item-added",
                    type="conversation.item.added",
                    item=function_item,
                ).model_dump(),
                events.ResponseFunctionCallArgumentsDone(
                    event_id="mutation-arguments-done",
                    type="response.function_call_arguments.done",
                    response_id="mutation-response",
                    item_id=function_item.id,
                    output_index=1,
                    call_id=function_item.call_id,
                    arguments="{}",
                ).model_dump(),
            ]
            for payload in payloads:
                yield json.dumps(payload)
            await keep_reader_open.wait()

        service._websocket = server_events()
        receive_task = asyncio.create_task(service._receive_task_handler())
        for _ in range(1000):
            if service._recovery_active:
                break
            await asyncio.sleep(0.002)

        self.assertTrue(service._recovery_active)
        mutation.assert_not_awaited()
        self.assertIn(
            function_item.call_id,
            service._discarded_tool_result_ids,
        )
        self.assertFalse(
            any(isinstance(frame, TTSAudioRawFrame) for frame in physical_frames)
        )
        self.assertTrue(
            any(getattr(event, "type", None) == "response.cancel" for event in sent_events)
        )
        self.assertTrue(
            any(
                "quarantined before dispatch" in call.kwargs["error_msg"]
                for call in service.push_error.await_args_list
            )
        )
        receive_task.cancel()
        await asyncio.gather(receive_task, return_exceptions=True)

    async def test_actual_non_completed_response_discards_held_output(self):
        usage_patch = patch("app.ha_sensors.PUBLISHER.usage", new=AsyncMock())
        usage_patch.start()
        self.addCleanup(usage_patch.stop)
        for case_number, status in enumerate(
            ("cancelled", "failed", "incomplete"),
            start=1,
        ):
            with self.subTest(status=status):
                context = LLMContext(
                    messages=[{"role": "user", "content": "Test failure"}]
                )
                service = SafeRealtimeLLMService(
                    api_key="probe",
                    max_context_turns=12,
                    authorized_tool_names=("request_follow_up", "end_conversation"),
                )
                pair, _assistant, created_tasks, _aggregator_frames = (
                    self._wire_actual_aggregator(service, context)
                )
                user_item_id = f"failed-status-user-{case_number}"
                service._conversation_window.begin_user_turn(
                    self._message(user_item_id, "user").model_dump(exclude_none=True)
                )
                service._conversation_window.attach_transcript(
                    user_item_id,
                    "Test failure",
                )
                service._conversation_window.activate(user_item_id)
                service._assistant_output_response_created = AsyncMock(
                    return_value=True
                )
                service._assistant_output_frame_created = Mock(return_value=True)
                service._current_audio_response = None
                service.stop_ttfb_metrics = AsyncMock()
                service.stop_processing_metrics = AsyncMock()
                service.push_error = AsyncMock()
                physical_frames = []

                async def push_frame(frame, direction=FrameDirection.DOWNSTREAM):
                    physical_frames.append(frame)
                    await pair.assistant().process_frame(frame, direction)

                service.push_frame = push_frame
                response_id = f"failed-status-response-{case_number}"
                assistant_item = self._message(
                    f"failed-status-assistant-{case_number}",
                    "assistant",
                    "Must not escape",
                )

                async def server_events():
                    payloads = [
                        events.ResponseCreated(
                            event_id=f"failed-status-created-{case_number}",
                            type="response.created",
                            response=self._response(
                                response_id,
                                "in_progress",
                                [],
                            ),
                        ).model_dump(),
                        events.ConversationItemAdded(
                            event_id=f"failed-status-item-{case_number}",
                            type="conversation.item.added",
                            item=assistant_item,
                        ).model_dump(),
                        events.ResponseAudioDelta(
                            event_id=f"failed-status-audio-{case_number}",
                            type="response.output_audio.delta",
                            response_id=response_id,
                            item_id=assistant_item.id,
                            output_index=0,
                            content_index=0,
                            delta=base64.b64encode(b"failed response pcm").decode(
                                "ascii"
                            ),
                        ).model_dump(),
                        events.ResponseDone(
                            event_id=f"failed-status-done-{case_number}",
                            type="response.done",
                            response=self._response(
                                response_id,
                                status,
                                [assistant_item],
                                status_details=(
                                    {"error": {"message": "probe failure"}}
                                    if status == "failed"
                                    else {"reason": "probe terminal status"}
                                ),
                            ),
                        ).model_dump(),
                    ]
                    for payload in payloads:
                        yield json.dumps(payload)

                service._websocket = server_events()
                await service._receive_task_handler()

                self.assertTrue(service._recovery_active)
                self.assertIsNone(service._decision_output_hold)
                self.assertFalse(
                    any(
                        isinstance(frame, TTSAudioRawFrame)
                        for frame in physical_frames
                    )
                )
                self.assertTrue(
                    any(
                        status in call.kwargs["error_msg"]
                        for call in service.push_error.await_args_list
                    )
                )
                for task in created_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*created_tasks, return_exceptions=True)

    async def test_actual_empty_terminal_output_discards_audio_before_ttfb(self):
        usage_patch = patch("app.ha_sensors.PUBLISHER.usage", new=AsyncMock())
        usage_patch.start()
        self.addCleanup(usage_patch.stop)
        context = LLMContext(
            messages=[{"role": "user", "content": "Say something"}]
        )
        service = SafeRealtimeLLMService(
            api_key="probe",
            max_context_turns=12,
            authorized_tool_names=("request_follow_up", "end_conversation"),
        )
        pair, _assistant, created_tasks, _aggregator_frames = (
            self._wire_actual_aggregator(service, context)
        )
        service._conversation_window.begin_user_turn(
            self._message("empty-output-user", "user").model_dump(exclude_none=True)
        )
        service._conversation_window.attach_transcript(
            "empty-output-user",
            "Say something",
        )
        service._conversation_window.activate("empty-output-user")
        service._assistant_output_response_created = AsyncMock(return_value=True)
        service._assistant_output_frame_created = Mock(return_value=True)
        service._current_audio_response = None
        service.stop_ttfb_metrics = AsyncMock()
        service.stop_processing_metrics = AsyncMock()
        service.push_error = AsyncMock()
        physical_frames = []

        async def push_frame(frame, direction=FrameDirection.DOWNSTREAM):
            physical_frames.append(frame)
            await pair.assistant().process_frame(frame, direction)

        service.push_frame = push_frame

        async def server_events():
            payloads = [
                events.ResponseCreated(
                    event_id="empty-output-created",
                    type="response.created",
                    response=self._response(
                        "empty-output-response",
                        "in_progress",
                        [],
                    ),
                ).model_dump(),
                events.ResponseAudioDelta(
                    event_id="empty-output-audio",
                    type="response.output_audio.delta",
                    response_id="empty-output-response",
                    item_id="missing-assistant-item",
                    output_index=0,
                    content_index=0,
                    delta=base64.b64encode(b"must never be audible").decode(
                        "ascii"
                    ),
                ).model_dump(),
                events.ResponseDone(
                    event_id="empty-output-done",
                    type="response.done",
                    response=self._response(
                        "empty-output-response",
                        "completed",
                        [],
                    ),
                ).model_dump(),
            ]
            for payload in payloads:
                yield json.dumps(payload)

        service._websocket = server_events()
        await service._receive_task_handler()

        self.assertTrue(service._recovery_active)
        self.assertFalse(
            any(isinstance(frame, TTSAudioRawFrame) for frame in physical_frames)
        )
        service.stop_ttfb_metrics.assert_not_awaited()
        self.assertEqual(
            context.get_messages(),
            [{"role": "user", "content": "Say something"}],
        )
        self.assertEqual(
            service._conversation_window.active_turn_id,
            "empty-output-user",
        )
        self.assertTrue(
            any(
                "structurally replayable" in call.kwargs["error_msg"]
                for call in service.push_error.await_args_list
            )
        )
        for task in created_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*created_tasks, return_exceptions=True)

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
        service._tool_call_response_contexts[call_id] = (
            "decision-response",
            1,
        )
        service._tool_call_generations[call_id] = service._session_generation
        service._tool_call_details[call_id] = ("end_conversation", {})
        service._running_tool_call_ids.add(call_id)
        original_in_flight = TURN_LIVENESS.in_flight
        self.addCleanup(
            setattr,
            TURN_LIVENESS,
            "in_flight",
            original_in_flight,
        )
        TURN_LIVENESS.in_flight = 1
        terminal_authorization = asyncio.create_task(
            service.end_conversation_is_sole_terminal_tool(call_id)
        )

        async def cancel_terminal_authorization():
            if not terminal_authorization.done():
                terminal_authorization.cancel()
            await asyncio.gather(
                terminal_authorization,
                return_exceptions=True,
            )

        self.addAsyncCleanup(cancel_terminal_authorization)
        await asyncio.sleep(0)
        self.assertFalse(terminal_authorization.done())

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
        self.assertTrue(await terminal_authorization)
        TURN_LIVENESS.in_flight = original_in_flight
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

    async def test_actual_held_model_activity_outlives_watchdog_but_stall_fails_closed(self):
        service = SafeRealtimeLLMService(
            api_key="probe",
            max_context_turns=12,
            authorized_tool_names=("request_follow_up", "end_conversation"),
        )
        service._context = LLMContext()
        service._conversation_window.begin_user_turn(
            self._message("watchdog-user", "user").model_dump(exclude_none=True)
        )
        service._conversation_window.attach_transcript("watchdog-user", "Keep going")
        service._conversation_window.activate("watchdog-user")
        service._active_response_id = "watchdog-response"
        service._active_output_response_context = ("watchdog-response", 1)
        self.assertTrue(service._begin_decision_output_hold("watchdog-response", 1))
        service.push_frame = AsyncMock()
        service.push_error = AsyncMock()
        phases = []

        async def send_phase(value):
            phases.append(value)

        emitter = PhaseEmitter(send_phase)
        emitter._current = "thinking"
        # Scale 15 simulated seconds to 15 ms while preserving watchdog ratios.
        emitter.THINKING_TIMEOUT_S = 0.015
        emitter.WATCHDOG_POLL_S = 0.001
        original_activity = TURN_LIVENESS.last_activity
        original_in_flight = TURN_LIVENESS.in_flight
        TURN_LIVENESS.last_activity = time.monotonic()
        TURN_LIVENESS.in_flight = 0
        try:
            emitter._arm_watchdog()
            for _ in range(10):
                await service._handle_evt_text_delta(
                    types.SimpleNamespace(
                        type="response.output_text.delta",
                        response_id="watchdog-response",
                        delta="progress",
                    )
                )
                await asyncio.sleep(0.005)
                self.assertEqual(emitter._current, "thinking")

            self.assertFalse(service._recovery_active)
            self.assertIsNotNone(service._decision_output_hold)
            self.assertEqual(phases, [])
            service.push_frame.assert_not_awaited()

            hold = service._decision_output_hold
            if hold is None:
                self.fail("active held generation disappeared")
            if hold.timeout_task is not None:
                hold.timeout_task.cancel()
                await asyncio.gather(
                    hold.timeout_task,
                    return_exceptions=True,
                )
            service.DECISION_OUTPUT_HOLD_TIMEOUT_S = 0.03
            hold.timeout_task = service._track_terminal_task(
                service._expire_decision_output_hold(hold)
            )
            for _ in range(1000):
                if service._recovery_active and emitter._current == "idle":
                    break
                await asyncio.sleep(0.001)

            self.assertEqual(phases, ["idle"])
            self.assertTrue(service._recovery_active)
            self.assertIsNone(service._decision_output_hold)
            service.push_frame.assert_not_awaited()
            self.assertTrue(
                any(
                    "hold timed out" in call.kwargs["error_msg"]
                    for call in service.push_error.await_args_list
                )
            )
        finally:
            TURN_LIVENESS.last_activity = original_activity
            TURN_LIVENESS.in_flight = original_in_flight
            emitter._cancel_watchdog()
            service.begin_recovery()

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
