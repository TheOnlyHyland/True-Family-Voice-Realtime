"""Regression tests for Voice PE phase signalling."""

import asyncio
import sys
import types
import unittest


class _FrameProcessor:
    def __init__(self, *args, **kwargs):
        pass


class _Frame:
    pass


if "pipecat.frames.frames" not in sys.modules:
    frames = types.ModuleType("pipecat.frames.frames")
    setattr(frames, "Frame", _Frame)
    setattr(frames, "UserStartedSpeakingFrame", type("UserStartedSpeakingFrame", (_Frame,), {}))
    setattr(frames, "UserStoppedSpeakingFrame", type("UserStoppedSpeakingFrame", (_Frame,), {}))
    setattr(frames, "BotStartedSpeakingFrame", type("BotStartedSpeakingFrame", (_Frame,), {}))
    setattr(frames, "BotStoppedSpeakingFrame", type("BotStoppedSpeakingFrame", (_Frame,), {}))
    sys.modules["pipecat.frames.frames"] = frames

if "pipecat.processors.frame_processor" not in sys.modules:
    processor = types.ModuleType("pipecat.processors.frame_processor")
    setattr(processor, "FrameProcessor", _FrameProcessor)
    setattr(processor, "FrameDirection", object)
    sys.modules["pipecat.processors.frame_processor"] = processor

from app import phase_emitter


PhaseEmitter = phase_emitter.PhaseEmitter


class PhaseEmitterTests(unittest.IsolatedAsyncioTestCase):
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
        phase_emitter.TURN_LIVENESS = phase_emitter.TurnLiveness()

        emitter._arm_watchdog()
        await asyncio.wait_for(idle_delivered.wait(), timeout=0.2)
        await asyncio.sleep(0)

        self.assertEqual(phases, ["idle"])
        self.assertEqual(emitter._current, "idle")
        self.assertIsNone(emitter._watchdog_task)


if __name__ == "__main__":
    unittest.main()
