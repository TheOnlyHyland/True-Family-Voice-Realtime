"""Regression tests for Voice PE phase signalling."""

import asyncio
import importlib.util
import sys
import types
import unittest
from pathlib import Path


class _FrameProcessor:
    def __init__(self, *args, **kwargs):
        pass

    async def process_frame(self, frame, direction):
        pass

    async def push_frame(self, frame, direction=None):
        pass


class _Frame:
    pass


frames = sys.modules.get("pipecat.frames.frames")
if frames is None:
    frames = types.ModuleType("pipecat.frames.frames")
    sys.modules["pipecat.frames.frames"] = frames
for name in (
    "Frame",
    "UserStartedSpeakingFrame",
    "UserStoppedSpeakingFrame",
    "BotStartedSpeakingFrame",
    "BotStoppedSpeakingFrame",
):
    if not hasattr(frames, name):
        value = _Frame if name == "Frame" else type(name, (_Frame,), {})
        setattr(frames, name, value)

processor = sys.modules.get("pipecat.processors.frame_processor")
if processor is None:
    processor = types.ModuleType("pipecat.processors.frame_processor")
    sys.modules["pipecat.processors.frame_processor"] = processor
if not hasattr(processor, "FrameProcessor"):
    setattr(processor, "FrameProcessor", _FrameProcessor)
if not hasattr(processor, "FrameDirection"):
    setattr(processor, "FrameDirection", object)

MODULE_PATH = Path(__file__).resolve().parents[1] / "app" / "phase_emitter.py"
SPEC = importlib.util.spec_from_file_location("phase_emitter_under_test", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError("Could not load phase_emitter for testing")
phase_emitter = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = phase_emitter
SPEC.loader.exec_module(phase_emitter)


PhaseEmitter = phase_emitter.PhaseEmitter


class PhaseEmitterTests(unittest.IsolatedAsyncioTestCase):
    async def test_bot_audio_start_precedes_final_idle_boundary(self):
        events = []

        async def send_phase(value):
            events.append(f"phase:{value}")

        async def before_idle():
            events.append("before_idle")

        emitter = PhaseEmitter(
            send_phase,
            idle_debounce_s=0.001,
            before_idle=before_idle,
            on_bot_started=lambda: events.append("bot_started"),
        )

        await emitter.process_frame(
            phase_emitter.BotStartedSpeakingFrame(),
            None,
        )
        await emitter.process_frame(
            phase_emitter.BotStoppedSpeakingFrame(),
            None,
        )
        await asyncio.sleep(0.02)

        self.assertEqual(
            events,
            ["bot_started", "phase:replying", "before_idle", "phase:idle"],
        )

    async def test_force_idle_can_repeat_delivery_after_device_wakes(self):
        phases = []

        async def send_phase(value):
            phases.append(value)

        emitter = PhaseEmitter(send_phase)
        emitter._current = "idle"

        await emitter.force_idle("recovery", force_delivery=True)

        self.assertEqual(phases, ["idle"])

    async def test_stale_reply_context_suppresses_old_idle_emission(self):
        phases = []
        captured = object()
        received = []

        async def before_idle(context):
            received.append(context)
            return False

        emitter = PhaseEmitter(
            phases.append,
            idle_debounce_s=0.001,
            before_idle=before_idle,
            capture_idle_context=lambda: captured,
        )
        emitter._send_phase = lambda value: asyncio.sleep(
            0,
            result=phases.append(value),
        )

        await emitter.process_frame(
            phase_emitter.BotStoppedSpeakingFrame(),
            None,
        )
        await asyncio.sleep(0.02)

        self.assertEqual(received, [captured])
        self.assertEqual(phases, [])

    async def test_force_idle_waits_for_follow_up_settlement(self):
        phases = []
        settlement_started = asyncio.Event()
        release_settlement = asyncio.Event()

        async def send_phase(value):
            phases.append(value)

        async def before_forced_idle():
            settlement_started.set()
            await release_settlement.wait()

        emitter = PhaseEmitter(
            send_phase,
            before_forced_idle=before_forced_idle,
        )
        emitter._current = "replying"

        force_task = asyncio.create_task(emitter.force_idle("recovery"))
        await settlement_started.wait()
        self.assertEqual(phases, [])

        release_settlement.set()
        await force_task
        self.assertEqual(phases, ["idle"])

    async def test_thinking_watchdog_delivers_idle_before_finishing(self):
        idle_delivered = asyncio.Event()
        phases = []

        async def send_phase(value):
            phases.append(value)
            if value == "idle":
                idle_delivered.set()

        emitter = PhaseEmitter(send_phase)
        emitter._current = "thinking"
        emitter.THINKING_TIMEOUT_S = 0.0
        emitter.WATCHDOG_POLL_S = 0.001
        setattr(phase_emitter, "TURN_LIVENESS", phase_emitter.TurnLiveness())

        emitter._arm_watchdog()
        await asyncio.wait_for(idle_delivered.wait(), timeout=0.2)
        await asyncio.sleep(0)

        self.assertEqual(phases, ["idle"])
        self.assertEqual(emitter._current, "idle")
        self.assertIsNone(emitter._watchdog_task)


if __name__ == "__main__":
    unittest.main()
