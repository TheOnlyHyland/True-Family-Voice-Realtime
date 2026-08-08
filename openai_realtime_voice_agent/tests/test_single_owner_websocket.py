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


class _Socket:
    def __init__(self):
        self.closed = False

        async def close():
            self.closed = True

        self.close = AsyncMock(side_effect=close)


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
            _true_family_chunk_contexts={},
            _true_family_output_generation=None,
            _true_family_output_websocket=None,
            _true_family_output_failed_closed=False,
        )
        transport._output = output
        transport.output = lambda: output

        async def call_event_handler(name, websocket):
            if name == "on_client_connected":
                transport.complete_candidate_handler(websocket, True)

        transport._call_event_handler = AsyncMock(side_effect=call_event_handler)
        return transport, serializer, output

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
        frame = _OutputAudioRawFrame()
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
        frame = _OutputAudioRawFrame()

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
