"""Offline tests for serializer admission and control gating."""

import json
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch


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


class _Frame:
    pass


class _InputAudioRawFrame(_Frame):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _OutputAudioRawFrame(_Frame):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class _FrameSerializer:
    pass


class _FrameSerializerType:
    BINARY = "binary"


_stub_module(
    "pipecat.frames.frames",
    Frame=_Frame,
    InputAudioRawFrame=_InputAudioRawFrame,
    OutputAudioRawFrame=_OutputAudioRawFrame,
)
_stub_module(
    "pipecat.serializers.base_serializer",
    FrameSerializer=_FrameSerializer,
    FrameSerializerType=_FrameSerializerType,
)
_stub_module(
    "pipecat.transports.websocket.server",
    WebsocketServerTransport=type("WebsocketServerTransport", (), {}),
)

MODULE_PATH = ADDON_ROOT / "app" / "raw_audio_serializer.py"
SPEC = importlib.util.spec_from_file_location(
    "app.raw_audio_serializer_under_test",
    MODULE_PATH,
)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Could not load raw_audio_serializer for testing")
raw_audio_serializer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = raw_audio_serializer
SPEC.loader.exec_module(raw_audio_serializer)

setattr(raw_audio_serializer, "InputAudioRawFrame", _InputAudioRawFrame)
setattr(raw_audio_serializer, "OutputAudioRawFrame", _OutputAudioRawFrame)


class RawAudioSerializerTests(unittest.IsolatedAsyncioTestCase):
    async def test_binary_and_output_audio_are_closed_until_hello_admission(self):
        serializer = raw_audio_serializer.RawAudioSerializer()
        output = raw_audio_serializer.OutputAudioRawFrame(
            audio=b"\x00\x00",
            sample_rate=24000,
            num_channels=1,
        )

        self.assertIsNone(await serializer.deserialize(b"\x00\x00"))
        self.assertEqual(await serializer.serialize(output), b"")

        serializer.set_audio_admitted(True)
        self.assertIsNotNone(await serializer.deserialize(b"\x00\x00"))
        self.assertEqual(await serializer.serialize(output), b"\x00\x00")

    async def test_binary_audio_requires_the_stage_authorizer(self):
        serializer = raw_audio_serializer.RawAudioSerializer()
        serializer.set_audio_admitted(True)
        serializer.set_binary_audio_authorizer(lambda _source: False)

        self.assertIsNone(await serializer.deserialize(b"\x01\x00"))

    async def test_pre_admission_controls_are_blocked_except_hello_receipt(self):
        serializer = raw_audio_serializer.RawAudioSerializer()
        callback = AsyncMock(return_value=False)
        serializer.set_control_handler(callback)

        await serializer.deserialize(
            json.dumps(
                {
                    "type": "wake",
                    "session_nonce": 7,
                    "wake_generation": 1,
                }
            )
        )
        callback.assert_not_awaited()

        hello_ack = {
            "type": "hello_ack",
            "nonce": 7,
            "accepted": True,
            "audio_out": "pcm",
            "follow_up_ms": 0,
            "follow_up_open_delay_ms": 700,
            "wake_open_delay_ms": 700,
            "playback_prebuffer_ms": 150,
        }
        await serializer.deserialize(json.dumps(hello_ack))
        callback.assert_awaited_once_with(hello_ack, None)

    async def test_admitted_controls_cross_one_central_validator(self):
        serializer = raw_audio_serializer.RawAudioSerializer()
        callback = AsyncMock(return_value=False)
        serializer.set_control_handler(callback)
        serializer.set_audio_admitted(True)
        messages = (
            {
                "type": "request_follow_up_ack",
                "token": 8,
                "session_nonce": 7,
                "accepted": True,
            },
            {
                "type": "follow_up_ready",
                "token": 8,
                "session_nonce": 7,
                "ready_nonce": 9,
            },
            {
                "type": "commit_follow_up_ack",
                "token": 8,
                "session_nonce": 7,
                "ready_nonce": 9,
                "accepted": True,
            },
            {
                "type": "client_revoke",
                "session_nonce": 7,
                "wake_generation": 1,
                "reason": "mute",
            },
        )
        for message in messages:
            self.assertIsNone(await serializer.deserialize(json.dumps(message)))

        self.assertEqual(
            [call.args[0] for call in callback.await_args_list],
            list(messages),
        )

    async def test_duplicate_keys_never_reach_any_security_control_handler(self):
        serializer = raw_audio_serializer.RawAudioSerializer()
        callback = AsyncMock(return_value=False)
        serializer.set_control_handler(callback)
        serializer.set_audio_admitted(True)
        control_types = (
            "hello_ack",
            "request_follow_up_ack",
            "follow_up_ready",
            "commit_follow_up_ack",
            "cancel_request_follow_up_ack",
            "suppress_followup_ack",
            "wake",
            "flush",
            "button_cancel",
            "false_flag",
            "interrupt",
            "client_revoke",
        )

        for control_type in control_types:
            message = (
                f'{{"type":"{control_type}",'
                f'"type":"{control_type}"}}'
            )
            self.assertIsNone(await serializer.deserialize(message))

        self.assertIsNone(
            await serializer.deserialize(
                '{"type":"wake","session_nonce":NaN,"wake_generation":1}'
            )
        )
        callback.assert_not_awaited()

    async def test_output_authorizer_can_revoke_late_assistant_audio(self):
        serializer = raw_audio_serializer.RawAudioSerializer()
        serializer.set_audio_admitted(True)
        authorizer = AsyncMock(return_value=False)
        serializer.set_output_audio_authorizer(authorizer)
        output = raw_audio_serializer.OutputAudioRawFrame(
            audio=b"\x00\x00",
            sample_rate=24000,
            num_channels=1,
        )

        with patch.object(
            raw_audio_serializer,
            "current_output_audio_context",
            return_value=("response-a", 3),
        ):
            self.assertEqual(await serializer.serialize(output), b"")
        authorizer.assert_awaited_once_with(("response-a", 3))


if __name__ == "__main__":
    unittest.main()
