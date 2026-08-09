"""Executable ownership tests for the Pipecat 0.0.97 transport adapter."""

import asyncio
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock


ADDON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ADDON_ROOT))


def _stub_module(name, **attributes):
    parts = name.split(".")
    for index in range(1, len(parts) + 1):
        module_name = ".".join(parts[:index])
        module = sys.modules.setdefault(module_name, types.ModuleType(module_name))
        if index > 1:
            parent = sys.modules[".".join(parts[: index - 1])]
            setattr(parent, parts[index - 1], module)
    module = sys.modules[name]
    for key, value in attributes.items():
        if not hasattr(module, key):
            setattr(module, key, value)


class _BaseTransport:
    def __init__(self, *args, **kwargs):
        self._params = kwargs.get("params")
        self._output = None
        self._input = None

    def output(self):
        return self._output

    async def _call_event_handler(self, *_args):
        pass


class _OutputAudioRawFrame:
    _next_id = 0

    def __init__(self, audio=b"", sample_rate=24000, num_channels=1):
        type(self)._next_id += 1
        self.id = type(self)._next_id
        self.audio = audio
        self.sample_rate = sample_rate
        self.num_channels = num_channels


_stub_module(
    "pipecat.frames.frames",
    InputAudioRawFrame=type("InputAudioRawFrame", (), {}),
    OutputAudioRawFrame=_OutputAudioRawFrame,
)
_stub_module(
    "pipecat.transports.websocket.server",
    WebsocketServerTransport=_BaseTransport,
)

from app import single_owner_websocket  # noqa: E402


class _BoundOutputAudioRawFrame(single_owner_websocket.OutputAudioRawFrame):
    _next_id = 0

    def __init__(self, audio=b"", sample_rate=24000, num_channels=1):
        type(self)._next_id += 1
        self.id = type(self)._next_id
        self.audio = audio
        self.sample_rate = sample_rate
        self.num_channels = num_channels


class _Socket:
    def __init__(self):
        self.closed = False

        async def close():
            self.closed = True

        self.close = AsyncMock(side_effect=close)


class _CancellationResistantCloseSocket:
    def __init__(self):
        self.closed = False
        self.close_entered = asyncio.Event()
        self.cancellation_resisted = asyncio.Event()
        self.release_close = asyncio.Event()
        self.abort_calls = 0
        self.transport = types.SimpleNamespace(abort=self._abort)

    def _abort(self):
        self.abort_calls += 1
        self.closed = True
        self.release_close.set()

    async def close(self):
        self.close_entered.set()
        try:
            await self.release_close.wait()
        except asyncio.CancelledError:
            self.cancellation_resisted.set()
            await self.release_close.wait()
        self.closed = True


class SingleOwnerTransportTests(unittest.IsolatedAsyncioTestCase):
    def _transport(self):
        serializer = types.SimpleNamespace(set_audio_admitted=Mock())
        params = types.SimpleNamespace(serializer=serializer)
        transport = single_owner_websocket.SingleOwnerWebsocketServerTransport(
            params=params
        )
        transport._params = params
        output = types.SimpleNamespace(
            _websocket=None,
            _media_senders={},
            _true_family_source_contexts={},
            _true_family_processing_source_contexts={},
            _true_family_chunk_contexts={},
            _true_family_partial_audio={},
            _true_family_active_write_contexts={},
            _true_family_active_write_tasks={},
            _true_family_failed_audio_generations=set(),
            _true_family_audio_state_changed=asyncio.Event(),
            _true_family_output_generation=None,
            _true_family_output_websocket=None,
            _true_family_output_failed_closed=False,
            _true_family_finishing_generation=None,
            _true_family_finished_generation=None,
        )
        output._true_family_audio_state_changed.set()
        output._true_family_owner_transport = transport
        output._true_family_write_audio_frame = AsyncMock(return_value=True)
        transport._output_audio_authorizer = AsyncMock(return_value=True)
        transport._output = output
        transport.output = lambda: output

        async def call_event_handler(name, websocket):
            if name == "on_client_connected":
                transport.complete_candidate_handler(websocket, True)

        transport._call_event_handler = AsyncMock(side_effect=call_event_handler)
        return transport, serializer, output

    @staticmethod
    def _sender(output, *, chunk_size=8):
        sender = types.SimpleNamespace(
            _transport=output,
            _audio_buffer=bytearray(),
            _audio_chunk_size=chunk_size,
            _sample_rate=24000,
            _destination=None,
            _audio_queue=asyncio.Queue(),
        )
        single_owner_websocket._reset_sender_partial_audio(sender)
        single_owner_websocket._patch_sender_audio_queue(sender)
        output._media_senders[None] = sender
        return sender

    async def test_output_frame_context_is_scoped_to_one_physical_write(self):
        transport, _serializer, output = self._transport()
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        observed = []

        async def write_audio_frame(_frame):
            observed.append(
                single_owner_websocket.current_output_audio_context()
            )
            return True

        authorizer = AsyncMock(return_value=True)
        transport.set_output_audio_authorizer(authorizer)
        self.assertTrue(
            await transport.bind_output_audio_generation(("response-a", 9), owner)
        )
        output._true_family_owner_transport = transport
        output._true_family_write_audio_frame = write_audio_frame
        frame = _BoundOutputAudioRawFrame()
        output._true_family_chunk_contexts[frame.id] = (
            ("response-a", 9),
            owner,
        )

        self.assertTrue(
            await single_owner_websocket._single_owner_write_audio_frame(
                output,
                frame,
            )
        )
        self.assertEqual(observed, [("response-a", 9)])
        authorizer.assert_awaited_once_with(
            ("response-a", 9),
            owner,
        )
        self.assertIsNone(
            single_owner_websocket.current_output_audio_context()
        )

    async def test_source_registration_is_generation_and_socket_scoped(self):
        transport, _serializer, output = self._transport()
        owner = _Socket()
        replacement = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        self.assertTrue(
            await transport.bind_output_audio_generation(("response-a", 9), owner)
        )
        frame = _BoundOutputAudioRawFrame()

        self.assertFalse(
            transport.register_output_audio_source(
                frame,
                ("response-a", 8),
                owner,
            )
        )
        self.assertFalse(
            transport.register_output_audio_source(
                frame,
                ("response-a", 9),
                replacement,
            )
        )
        self.assertTrue(
            transport.register_output_audio_source(
                frame,
                ("response-a", 9),
                owner,
            )
        )
        self.assertEqual(
            output._true_family_source_contexts[frame.id],
            (("response-a", 9), owner),
        )

    async def test_graceful_finish_has_a_no_audio_fast_path(self):
        transport, _serializer, output = self._transport()
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        context = ("response-a", 9)
        self.assertTrue(await transport.bind_output_audio_generation(context, owner))

        self.assertTrue(
            await transport.gracefully_finish_output_audio_generation(
                context,
                owner,
                lambda: True,
                timeout_s=0.1,
            )
        )
        self.assertEqual(output._true_family_finished_generation, context)

    async def test_graceful_finish_waits_for_registered_and_processing_sources(self):
        transport, _serializer, output = self._transport()
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        context = ("response-a", 9)
        provenance = (context, owner)
        self.assertTrue(await transport.bind_output_audio_generation(context, owner))
        registered = _BoundOutputAudioRawFrame()
        processing = _BoundOutputAudioRawFrame()
        output._true_family_source_contexts[registered.id] = provenance
        output._true_family_processing_source_contexts[processing.id] = provenance

        finish = asyncio.create_task(
            transport.gracefully_finish_output_audio_generation(
                context,
                owner,
                lambda: True,
                timeout_s=0.2,
            )
        )
        await asyncio.sleep(0)
        self.assertFalse(finish.done())
        output._true_family_source_contexts.pop(registered.id)
        output._true_family_audio_state_changed.set()
        await asyncio.sleep(0)
        self.assertFalse(finish.done())
        output._true_family_processing_source_contexts.pop(processing.id)
        output._true_family_audio_state_changed.set()

        self.assertTrue(await finish)

    async def test_graceful_finish_orders_queued_audio_then_one_padded_partial(self):
        transport, _serializer, output = self._transport()
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        context = ("response-a", 9)
        provenance = (context, owner)
        self.assertTrue(await transport.bind_output_audio_generation(context, owner))
        sender = self._sender(output)
        sender._audio_buffer = bytearray(b"partial")
        sender._true_family_partial_provenance = provenance
        sender._true_family_partial_frame_type = _BoundOutputAudioRawFrame
        sender._true_family_partial_num_channels = 1
        output._true_family_partial_audio[id(sender)] = (
            single_owner_websocket._OwnedPartialAudio(
                sender=sender,
                provenance=provenance,
                audio=b"partial",
                frame_type=_BoundOutputAudioRawFrame,
                num_channels=1,
                sample_rate=24000,
                destination=None,
                chunk_size=8,
            )
        )
        queued = _BoundOutputAudioRawFrame(audio=b"queued-a")
        token = single_owner_websocket._CURRENT_OUTPUT_AUDIO_PROVENANCE.set(
            provenance
        )
        try:
            await sender._audio_queue.put(queued)
        finally:
            single_owner_websocket._CURRENT_OUTPUT_AUDIO_PROVENANCE.reset(token)

        write_entered = asyncio.Event()
        release_first_write = asyncio.Event()
        written = []

        async def write(frame):
            written.append(frame.audio)
            if len(written) == 1:
                write_entered.set()
                await release_first_write.wait()
            return True

        output._true_family_write_audio_frame = write

        async def consume_two():
            for _ in range(2):
                frame = await sender._audio_queue.get()
                try:
                    await single_owner_websocket._single_owner_write_audio_frame(
                        output,
                        frame,
                    )
                finally:
                    sender._audio_queue.task_done()

        consumer = asyncio.create_task(consume_two())
        finish = asyncio.create_task(
            transport.gracefully_finish_output_audio_generation(
                context,
                owner,
                lambda: True,
                timeout_s=0.5,
            )
        )
        await write_entered.wait()
        self.assertFalse(finish.done())
        release_first_write.set()

        self.assertTrue(await finish)
        await consumer
        self.assertEqual(written, [b"queued-a", b"partial\x00"])
        self.assertEqual(sender._audio_buffer, bytearray())

    async def test_graceful_finish_timeout_discards_queued_and_partial_audio(self):
        transport, _serializer, output = self._transport()
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        context = ("response-a", 9)
        provenance = (context, owner)
        self.assertTrue(await transport.bind_output_audio_generation(context, owner))
        sender = self._sender(output)
        sender._audio_buffer = bytearray(b"partial")
        sender._true_family_partial_provenance = provenance
        sender._true_family_partial_frame_type = _BoundOutputAudioRawFrame
        sender._true_family_partial_num_channels = 1
        output._true_family_partial_audio[id(sender)] = (
            single_owner_websocket._OwnedPartialAudio(
                sender=sender,
                provenance=provenance,
                audio=b"partial",
                frame_type=_BoundOutputAudioRawFrame,
                num_channels=1,
                sample_rate=24000,
                destination=None,
                chunk_size=8,
            )
        )
        queued = _BoundOutputAudioRawFrame(audio=b"queued-a")
        token = single_owner_websocket._CURRENT_OUTPUT_AUDIO_PROVENANCE.set(
            provenance
        )
        try:
            await sender._audio_queue.put(queued)
        finally:
            single_owner_websocket._CURRENT_OUTPUT_AUDIO_PROVENANCE.reset(token)

        self.assertFalse(
            await transport.gracefully_finish_output_audio_generation(
                context,
                owner,
                lambda: True,
                timeout_s=0.01,
            )
        )
        self.assertEqual(sender._audio_queue.qsize(), 0)
        self.assertEqual(sender._audio_buffer, bytearray())
        self.assertIsNone(output._true_family_output_generation)

    async def test_graceful_finish_write_failure_retires_generation(self):
        transport, _serializer, output = self._transport()
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        context = ("response-a", 9)
        provenance = (context, owner)
        self.assertTrue(await transport.bind_output_audio_generation(context, owner))
        sender = self._sender(output)
        queued = _BoundOutputAudioRawFrame(audio=b"queued-a")
        token = single_owner_websocket._CURRENT_OUTPUT_AUDIO_PROVENANCE.set(
            provenance
        )
        try:
            await sender._audio_queue.put(queued)
        finally:
            single_owner_websocket._CURRENT_OUTPUT_AUDIO_PROVENANCE.reset(token)
        output._true_family_write_audio_frame = AsyncMock(return_value=False)

        async def consume():
            frame = await sender._audio_queue.get()
            try:
                await single_owner_websocket._single_owner_write_audio_frame(
                    output,
                    frame,
                )
            finally:
                sender._audio_queue.task_done()

        consumer = asyncio.create_task(consume())
        self.assertFalse(
            await transport.gracefully_finish_output_audio_generation(
                context,
                owner,
                lambda: True,
                timeout_s=0.2,
            )
        )
        await consumer
        self.assertIsNone(output._true_family_output_generation)

    async def test_active_write_does_not_hold_owner_lock_and_retirement_cancels_it(self):
        transport, _serializer, output = self._transport()
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        context = ("response-a", 12)
        self.assertTrue(await transport.bind_output_audio_generation(context, owner))
        entered = asyncio.Event()

        async def blocked_write(_frame):
            entered.set()
            await asyncio.Event().wait()
            return True

        output._true_family_write_audio_frame = AsyncMock(side_effect=blocked_write)
        frame = _BoundOutputAudioRawFrame(audio=b"active-a")
        output._true_family_chunk_contexts[frame.id] = (context, owner)
        write = asyncio.create_task(
            single_owner_websocket._single_owner_write_audio_frame(output, frame)
        )
        await entered.wait()

        await asyncio.wait_for(transport._owner_lock.acquire(), timeout=0.1)
        transport._owner_lock.release()
        self.assertTrue(transport.retire_output_audio_generation(context))

        self.assertFalse(await asyncio.wait_for(write, timeout=0.1))
        self.assertFalse(output._true_family_active_write_contexts)
        self.assertFalse(output._true_family_active_write_tasks)

    async def test_first_write_failure_fences_every_later_chunk(self):
        transport, _serializer, output = self._transport()
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        context = ("response-a", 13)
        self.assertTrue(await transport.bind_output_audio_generation(context, owner))
        output._true_family_write_audio_frame = AsyncMock(
            side_effect=(False, True)
        )
        first = _BoundOutputAudioRawFrame(audio=b"first")
        second = _BoundOutputAudioRawFrame(audio=b"second")
        output._true_family_chunk_contexts[first.id] = (context, owner)
        output._true_family_chunk_contexts[second.id] = (context, owner)

        self.assertFalse(
            await single_owner_websocket._single_owner_write_audio_frame(
                output,
                first,
            )
        )
        self.assertFalse(
            await single_owner_websocket._single_owner_write_audio_frame(
                output,
                second,
            )
        )
        self.assertEqual(output._true_family_write_audio_frame.await_count, 1)

    async def test_owned_partial_survives_pipecat_idle_buffer_cleanup(self):
        transport, _serializer, output = self._transport()
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        context = ("response-a", 14)
        provenance = (context, owner)
        self.assertTrue(await transport.bind_output_audio_generation(context, owner))
        sender = self._sender(output)

        async def partial_chunker(frame):
            sender._audio_buffer.extend(frame.audio)

        sender._true_family_handle_audio_frame = partial_chunker
        source = _BoundOutputAudioRawFrame(audio=b"partial")
        output._true_family_source_contexts[source.id] = provenance
        await single_owner_websocket._single_owner_handle_audio_frame(sender, source)
        self.assertEqual(
            output._true_family_partial_audio[id(sender)].audio,
            b"partial",
        )
        single_owner_websocket._reset_sender_partial_audio(sender)

        async def consume():
            frame = await sender._audio_queue.get()
            try:
                await single_owner_websocket._single_owner_write_audio_frame(
                    output,
                    frame,
                )
            finally:
                sender._audio_queue.task_done()

        consumer = asyncio.create_task(consume())
        self.assertTrue(
            await transport.gracefully_finish_output_audio_generation(
                context,
                owner,
                lambda: True,
                timeout_s=0.2,
            )
        )
        await consumer
        written_frame = output._true_family_write_audio_frame.await_args.args[0]
        self.assertEqual(written_frame.audio, b"partial\x00")

    async def test_cancellation_resistant_write_retires_socket_before_return(self):
        transport, _serializer, output = self._transport()
        transport.OUTPUT_WRITE_SETTLE_TIMEOUT_S = 0.01
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        context = ("response-a", 15)
        self.assertTrue(await transport.bind_output_audio_generation(context, owner))
        entered = asyncio.Event()
        resisted = asyncio.Event()
        release = asyncio.Event()

        async def resistant_write(_frame):
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                resisted.set()
                await release.wait()
            return True

        output._true_family_write_audio_frame = resistant_write
        frame = _BoundOutputAudioRawFrame(audio=b"active")
        output._true_family_chunk_contexts[frame.id] = (context, owner)
        write = asyncio.create_task(
            single_owner_websocket._single_owner_write_audio_frame(output, frame)
        )
        await entered.wait()

        with self.assertLogs("app.single_owner_websocket", level="ERROR"):
            self.assertFalse(
                await transport.settle_output_audio_generation(context)
            )
        self.assertTrue(resisted.is_set())
        self.assertTrue(owner.closed)
        self.assertIsNone(transport.admitted_websocket)
        release.set()
        self.assertFalse(await write)

    async def test_failed_finish_settles_cancellation_resistant_active_write(self):
        transport, _serializer, output = self._transport()
        transport.OUTPUT_WRITE_SETTLE_TIMEOUT_S = 0.01
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        context = ("response-deadline", 16)
        self.assertTrue(await transport.bind_output_audio_generation(context, owner))
        entered = asyncio.Event()
        resisted = asyncio.Event()
        release = asyncio.Event()

        async def resistant_write(_frame):
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError:
                resisted.set()
                await release.wait()
            if owner.closed:
                raise RuntimeError("retired socket rejected stale PCM")
            return True

        output._true_family_write_audio_frame = resistant_write
        frame = _BoundOutputAudioRawFrame(audio=b"deadline")
        output._true_family_chunk_contexts[frame.id] = (context, owner)
        write = asyncio.create_task(
            single_owner_websocket._single_owner_write_audio_frame(output, frame)
        )
        await entered.wait()

        with self.assertLogs("app.single_owner_websocket", level="ERROR"):
            self.assertFalse(
                await asyncio.wait_for(
                    transport.gracefully_finish_output_audio_generation(
                        context,
                        owner,
                        lambda: True,
                        timeout_s=0.001,
                    ),
                    timeout=0.2,
                )
            )
        self.assertTrue(resisted.is_set())
        self.assertTrue(owner.closed)
        self.assertIsNone(transport.admitted_websocket)
        self.assertIsNone(output._websocket)

        release.set()
        self.assertFalse(await write)
        self.assertFalse(output._true_family_active_write_contexts)
        self.assertFalse(output._true_family_active_write_tasks)

    async def test_next_generation_waits_for_retired_source_processing(self):
        transport, _serializer, output = self._transport()
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        old_context = ("response-old", 16)
        new_context = ("response-new", 17)
        self.assertTrue(
            await transport.bind_output_audio_generation(old_context, owner)
        )
        sender = self._sender(output)
        entered = asyncio.Event()
        release = asyncio.Event()

        async def blocked_chunker(frame):
            entered.set()
            await release.wait()
            sender._audio_buffer.extend(frame.audio)

        sender._true_family_handle_audio_frame = blocked_chunker
        source = _BoundOutputAudioRawFrame(audio=b"old")
        output._true_family_source_contexts[source.id] = (old_context, owner)
        processing = asyncio.create_task(
            single_owner_websocket._single_owner_handle_audio_frame(sender, source)
        )
        await entered.wait()
        self.assertTrue(transport.retire_output_audio_generation(old_context))

        bind_new = asyncio.create_task(
            transport.bind_output_audio_generation(new_context, owner)
        )
        await asyncio.sleep(0)
        self.assertFalse(bind_new.done())
        release.set()
        await processing

        self.assertTrue(await bind_new)
        self.assertEqual(sender._audio_buffer, bytearray())
        self.assertEqual(
            output._true_family_output_generation,
            new_context,
        )

    async def test_retired_processing_timeout_still_retires_its_socket(self):
        transport, _serializer, output = self._transport()
        transport.OUTPUT_WRITE_SETTLE_TIMEOUT_S = 0.001
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        context = ("response-stalled", 18)
        self.assertTrue(
            await transport.bind_output_audio_generation(context, owner)
        )
        output._true_family_processing_source_contexts[1] = (context, owner)
        self.assertTrue(transport.retire_output_audio_generation(context))

        self.assertFalse(await transport.settle_output_audio_generation())
        self.assertTrue(owner.closed)
        self.assertIsNone(transport.admitted_websocket)

    async def test_cancellation_resistant_close_is_aborted_after_owner_detaches(self):
        transport, _serializer, output = self._transport()
        transport.SOCKET_CLOSE_TIMEOUT_S = 0.001
        owner = _CancellationResistantCloseSocket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)

        retired = asyncio.create_task(transport.retire_client(owner))
        await owner.close_entered.wait()
        self.assertIsNone(transport.admitted_websocket)
        self.assertIsNone(output._websocket)

        self.assertTrue(await asyncio.wait_for(retired, timeout=0.2))
        self.assertTrue(owner.cancellation_resisted.is_set())
        self.assertEqual(owner.abort_calls, 1)
        self.assertTrue(owner.closed)

    async def test_retirement_closes_owner_after_output_settlement_exception(self):
        transport, _serializer, output = self._transport()
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        transport.settle_output_audio_generation = AsyncMock(
            side_effect=RuntimeError("settlement failed")
        )

        with self.assertLogs("app.single_owner_websocket", level="WARNING"):
            self.assertTrue(await transport.retire_client(owner))

        self.assertIsNone(transport.admitted_websocket)
        self.assertIsNone(output._websocket)
        owner.close.assert_awaited_once_with()
        self.assertTrue(owner.closed)

    async def test_unauthenticated_challenger_cannot_displace_owner(self):
        transport, _serializer, output = self._transport()
        owner = _Socket()
        challenger = _Socket()

        self.assertTrue(await transport._on_client_connected(owner))
        self.assertTrue(await transport.admit_client(owner))
        self.assertFalse(await transport._on_client_connected(challenger))

        self.assertIs(transport.admitted_websocket, owner)
        self.assertIs(output._websocket, owner)
        self.assertIsNot(output._websocket, challenger)

    async def test_delayed_old_disconnect_cannot_clear_replacement(self):
        transport, serializer, output = self._transport()
        old = _Socket()
        new = _Socket()
        old_callback_entered = asyncio.Event()
        release_old_callback = asyncio.Event()

        async def event_handler(name, websocket):
            if name == "on_client_connected":
                transport.complete_candidate_handler(websocket, True)
            if name == "on_client_disconnected" and websocket is old:
                old_callback_entered.set()
                await release_old_callback.wait()

        transport._call_event_handler = event_handler
        await transport._on_client_connected(old)
        await transport.admit_client(old)

        old_disconnect = asyncio.create_task(transport._on_client_disconnected(old))
        await old_callback_entered.wait()
        self.assertTrue(await transport._on_client_connected(new))
        self.assertTrue(await transport.admit_client(new))
        release_old_callback.set()
        await old_disconnect

        self.assertIs(transport.admitted_websocket, new)
        self.assertIs(output._websocket, new)
        self.assertTrue(serializer.set_audio_admitted.call_args.args[0])

    async def test_candidate_can_submit_only_exact_hello_receipt(self):
        transport, _serializer, _output = self._transport()
        candidate = _Socket()
        await transport._on_client_connected(candidate)
        hello_ack = (
            '{"type":"hello_ack","nonce":7,"accepted":true,'
            '"audio_out":"pcm","follow_up_ms":0,'
            '"follow_up_open_delay_ms":700,"wake_open_delay_ms":700,'
            '"playback_prebuffer_ms":150}'
        )

        self.assertTrue(transport.message_is_admitted(candidate, hello_ack))
        self.assertFalse(
            transport.message_is_admitted(
                candidate,
                '{"type":"wake","session_nonce":7,"wake_generation":1}',
            )
        )
        self.assertFalse(transport.message_is_admitted(candidate, b"\x00\x00"))
        self.assertFalse(
            transport.message_is_admitted(
                candidate,
                '{"type":"hello_ack","type":"hello_ack","nonce":7,'
                '"accepted":true,"audio_out":"pcm","follow_up_ms":0,'
                '"follow_up_open_delay_ms":700,"wake_open_delay_ms":700,'
                '"playback_prebuffer_ms":150}',
            )
        )

    async def test_candidate_rejected_during_callback_never_enters_receive_loop(self):
        transport, _serializer, _output = self._transport()
        candidate = _Socket()

        async def reject_during_callback(_name, websocket):
            await transport.reject_candidate(websocket)

        transport._call_event_handler = reject_during_callback

        self.assertFalse(await transport._on_client_connected(candidate))
        self.assertIsNone(transport._candidate_websocket)
        candidate.close.assert_awaited_once_with()

    async def test_candidate_handler_timeout_rejects_and_quarantines_uncertain_close(self):
        transport, _serializer, _output = self._transport()
        candidate = _Socket()
        candidate.close = AsyncMock(side_effect=asyncio.TimeoutError())
        transport.CANDIDATE_HANDLER_TIMEOUT_S = 0.01
        transport.SOCKET_CLOSE_TIMEOUT_S = 0.01
        transport._call_event_handler = AsyncMock()

        with self.assertLogs("app.single_owner_websocket", level="WARNING"):
            self.assertFalse(await transport._on_client_connected(candidate))

        self.assertIsNone(transport._candidate_websocket)
        self.assertEqual(transport.uncertain_socket_count, 1)

    async def test_candidate_handler_failure_completion_rejects_without_waiting(self):
        transport, _serializer, _output = self._transport()
        candidate = _Socket()

        async def fail_handler(_name, websocket):
            transport.complete_candidate_handler(websocket, False)

        transport._call_event_handler = fail_handler

        self.assertFalse(await transport._on_client_connected(candidate))
        self.assertTrue(candidate.closed)
        self.assertIsNone(transport._candidate_websocket)

    async def test_retire_reports_unconfirmed_physical_close_as_false(self):
        transport, _serializer, _output = self._transport()
        owner = _Socket()
        await transport._on_client_connected(owner)
        await transport.admit_client(owner)
        owner.close = AsyncMock()
        owner.closed = False

        with self.assertLogs("app.single_owner_websocket", level="WARNING"):
            self.assertFalse(await transport.retire_client(owner))

        self.assertIsNone(transport.admitted_websocket)
        self.assertEqual(transport.uncertain_socket_count, 1)


if __name__ == "__main__":
    unittest.main()
