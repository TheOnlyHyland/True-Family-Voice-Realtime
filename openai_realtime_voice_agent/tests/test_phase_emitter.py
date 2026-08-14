"""Regression tests for Voice PE phase signalling."""

import asyncio
import importlib.util
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock


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
OutputChainState = phase_emitter.OutputChainState


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

    async def test_rejected_phase_does_not_advance_internal_state(self):
        async def reject_phase(_value, _context):
            return False

        emitter = PhaseEmitter(
            reject_phase,
            capture_phase_context=object,
        )
        emitter._current = "listening"

        await emitter._emit("thinking")

        self.assertEqual(emitter._current, "listening")

    async def test_terminal_idle_recaptures_context_after_before_idle(self):
        epoch = {"value": 1}
        sent_epochs = []

        def capture_terminal_idle_context():
            return types.SimpleNamespace(epoch=epoch["value"])

        async def before_idle():
            epoch["value"] += 1
            return True

        async def send_phase(value, context):
            self.assertEqual(value, "idle")
            sent_epochs.append(context.epoch)
            return context.epoch == epoch["value"]

        emitter = PhaseEmitter(
            send_phase,
            idle_debounce_s=0,
            before_idle=before_idle,
            capture_terminal_idle_context=capture_terminal_idle_context,
            output_chain_state=lambda _context: OutputChainState.FINALIZABLE,
        )

        await emitter._emit_idle_after_debounce(
            object(),
            terminal_idle_context=types.SimpleNamespace(epoch=1),
        )

        self.assertEqual(sent_epochs, [2])
        self.assertEqual(emitter._current, "idle")

    async def test_queued_phase_uses_context_captured_before_transition_lock(self):
        wake_generation = {"value": 1}
        captures = []
        attempts = []

        def capture_context():
            context = types.SimpleNamespace(
                websocket=object(),
                session_nonce=10,
                wake_generation=wake_generation["value"],
            )
            captures.append(context)
            return context

        async def send_phase(value, context):
            attempts.append((value, context.wake_generation))
            return context.wake_generation == wake_generation["value"]

        emitter = PhaseEmitter(
            send_phase,
            capture_phase_context=capture_context,
        )
        await emitter._phase_transition_lock.acquire()
        queued = asyncio.create_task(emitter._emit("listening"))
        await asyncio.sleep(0)
        wake_generation["value"] = 2
        emitter._phase_transition_lock.release()
        await queued

        self.assertEqual([context.wake_generation for context in captures], [1])
        self.assertEqual(attempts, [("listening", 1)])
        self.assertIsNone(emitter._current)

    async def test_missing_scheduled_context_cannot_be_recaptured_later(self):
        send_phase = AsyncMock(return_value=True)
        emitter = PhaseEmitter(
            send_phase,
            capture_phase_context=lambda: None,
        )

        await emitter._emit("listening")

        send_phase.assert_not_awaited()
        self.assertIsNone(emitter._current)

    async def test_slow_tool_thinking_preserves_current_output_owner(self):
        progress_context = object()
        terminal_context = object()
        sent = []

        async def send_phase(value, context, preserve_output=False):
            sent.append((value, context, preserve_output))
            return True

        emitter = PhaseEmitter(
            send_phase,
            idle_debounce_s=0.001,
            capture_phase_context=lambda: progress_context,
            capture_terminal_idle_context=lambda: terminal_context,
        )
        original_in_flight = phase_emitter.TURN_LIVENESS.in_flight
        phase_emitter.TURN_LIVENESS.in_flight = 1
        try:
            await emitter.process_frame(
                phase_emitter.BotStoppedSpeakingFrame(),
                None,
            )
            await asyncio.sleep(0.02)
        finally:
            phase_emitter.TURN_LIVENESS.in_flight = original_in_flight
            emitter._cancel_watchdog()

        self.assertEqual(sent, [("thinking", progress_context, True)])

    async def test_queued_continuation_stays_thinking_at_zero_tool_liveness(self):
        progress_context = object()
        terminal_context = object()
        finalizer_context = object()
        states = [OutputChainState.BUSY, OutputChainState.FINALIZABLE]
        sent = []

        async def send_phase(value, context, preserve_output=False):
            sent.append((value, context, preserve_output))
            return True

        emitter = PhaseEmitter(
            send_phase,
            idle_debounce_s=0.001,
            before_idle=AsyncMock(return_value=True),
            capture_idle_context=lambda: finalizer_context,
            capture_phase_context=lambda: progress_context,
            capture_terminal_idle_context=lambda: terminal_context,
            output_chain_state=lambda context: states.pop(0),
        )
        emitter.WATCHDOG_POLL_S = 0.001

        await emitter.process_frame(
            phase_emitter.BotStoppedSpeakingFrame(),
            None,
        )
        await asyncio.sleep(0.03)

        self.assertEqual(
            sent,
            [
                ("thinking", progress_context, True),
                ("idle", terminal_context, False),
            ],
        )

    async def test_successor_response_revokes_stale_idle_timer(self):
        progress_context = object()
        terminal_context = object()
        finalizer_context = object()
        send_phase = AsyncMock(return_value=True)
        emitter = PhaseEmitter(
            send_phase,
            idle_debounce_s=0.001,
            before_idle=AsyncMock(return_value=True),
            capture_idle_context=lambda: finalizer_context,
            capture_phase_context=lambda: progress_context,
            capture_terminal_idle_context=lambda: terminal_context,
            output_chain_state=lambda context: OutputChainState.REVOKED,
        )

        await emitter.process_frame(
            phase_emitter.BotStoppedSpeakingFrame(),
            None,
        )
        await asyncio.sleep(0.02)

        send_phase.assert_not_awaited()

    async def test_final_partial_bot_start_cannot_cancel_active_finalization(self):
        finalization_started = asyncio.Event()
        release_finalization = asyncio.Event()
        idle_send_started = asyncio.Event()
        release_idle_send = asyncio.Event()
        sent = []
        on_bot_started = Mock()

        async def send_phase(value, context):
            sent.append(value)
            if value == "idle":
                idle_send_started.set()
                await release_idle_send.wait()
            return True

        async def before_idle(_context):
            finalization_started.set()
            await release_finalization.wait()
            return True

        emitter = PhaseEmitter(
            send_phase,
            idle_debounce_s=0.001,
            before_idle=before_idle,
            on_bot_started=on_bot_started,
            capture_idle_context=object,
            capture_phase_context=object,
            capture_terminal_idle_context=object,
            output_chain_state=lambda context: OutputChainState.FINALIZABLE,
        )

        await emitter.process_frame(
            phase_emitter.BotStoppedSpeakingFrame(),
            None,
        )
        await finalization_started.wait()
        idle_task = emitter._idle_task

        await emitter.process_frame(
            phase_emitter.BotStartedSpeakingFrame(),
            None,
        )
        await emitter.process_frame(
            phase_emitter.BotStoppedSpeakingFrame(),
            None,
        )

        self.assertIs(emitter._idle_task, idle_task)
        self.assertFalse(idle_task.cancelled())
        on_bot_started.assert_not_called()
        release_finalization.set()
        await idle_send_started.wait()

        await emitter.process_frame(
            phase_emitter.BotStartedSpeakingFrame(),
            None,
        )
        await emitter.process_frame(
            phase_emitter.BotStoppedSpeakingFrame(),
            None,
        )

        self.assertIs(emitter._idle_task, idle_task)
        self.assertFalse(idle_task.cancelled())
        on_bot_started.assert_not_called()
        release_idle_send.set()
        await idle_task

        self.assertEqual(sent, ["idle"])

        await emitter.process_frame(
            phase_emitter.UserStoppedSpeakingFrame(),
            None,
        )
        await emitter.process_frame(
            phase_emitter.BotStartedSpeakingFrame(),
            None,
        )
        await emitter.process_frame(
            phase_emitter.BotStoppedSpeakingFrame(),
            None,
        )
        self.assertEqual(sent, ["idle"])
        on_bot_started.assert_not_called()
        self.assertTrue(emitter._suppress_thinking)
        self.assertIsNone(emitter._watchdog_task)

        await emitter.process_frame(
            phase_emitter.UserStartedSpeakingFrame(),
            None,
        )
        await emitter.process_frame(
            phase_emitter.BotStartedSpeakingFrame(),
            None,
        )
        self.assertEqual(sent, ["idle", "listening", "replying"])
        on_bot_started.assert_called_once_with()

    async def test_force_idle_suppresses_delayed_bot_feedback_until_real_speech(self):
        sent = []
        on_bot_started = Mock()

        async def send_phase(value, context):
            sent.append(value)
            return True

        emitter = PhaseEmitter(
            send_phase,
            on_bot_started=on_bot_started,
            capture_phase_context=object,
            capture_terminal_idle_context=object,
        )
        emitter._current = "replying"

        await emitter.force_idle("test")
        await emitter.process_frame(
            phase_emitter.BotStartedSpeakingFrame(),
            None,
        )
        await emitter.process_frame(
            phase_emitter.BotStoppedSpeakingFrame(),
            None,
        )

        self.assertEqual(sent, ["idle"])
        on_bot_started.assert_not_called()

    async def test_terminal_idle_uses_distinct_tokenless_context(self):
        progress_context = object()
        terminal_context = object()
        sent = []

        async def send_phase(value, context):
            sent.append((value, context))
            return True

        emitter = PhaseEmitter(
            send_phase,
            idle_debounce_s=0.001,
            before_idle=AsyncMock(return_value=True),
            capture_phase_context=lambda: progress_context,
            capture_terminal_idle_context=lambda: terminal_context,
        )

        await emitter.process_frame(
            phase_emitter.BotStoppedSpeakingFrame(),
            None,
        )
        await asyncio.sleep(0.02)

        self.assertEqual(sent, [("idle", terminal_context)])

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
