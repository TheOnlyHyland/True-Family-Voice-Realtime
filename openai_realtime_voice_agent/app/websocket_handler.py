"""WebSocket handler for managing WebSocket connections and pipelines."""
import asyncio
import json
import logging
import math
import re
import secrets
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional, Callable, Awaitable, Dict

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask
from pipecat.transports.websocket.server import WebsocketServerTransport, WebsocketServerParams
from pipecat.services.openai.realtime.llm import OpenAIRealtimeLLMService

from pipecat.processors.frame_processor import FrameProcessor, FrameDirection
from pipecat.frames.frames import (
    EndFrame,
    ErrorFrame,
    Frame,
    InputAudioRawFrame,
    OutputAudioRawFrame,
    StartFrame,
)
from pipecat.audio.utils import create_stream_resampler
from pipecat.services.openai.realtime import events as openai_rt_events

from app.raw_audio_serializer import RawAudioSerializer
from app.session_manager import SessionManager
from app.audio_recording_service import AudioRecordingService
from app.phase_emitter import PhaseEmitter, TURN_LIVENESS
from app.transcript_logger import TranscriptLogger
from app.media_activity import MediaActivity
from app.request_follow_up_tool import FollowUpReservationOutcome
from app.protocol_json import (
    LEGACY_BACKEND_TO_DEVICE_FIELDS,
    TRUSTED_BACKEND_TO_DEVICE_FIELDS,
    TRUSTED_DEVICE_TO_BACKEND_FIELDS,
    has_exact_fields,
)
from app.single_owner_websocket import (
    SingleOwnerWebsocketServerTransport,
    _close_socket,
    current_message_websocket,
)

logger = logging.getLogger(__name__)

_HIGH_CONFIDENCE_GRATITUDE = re.compile(
    r"\b(?:thanks|thank you|many thanks|cheers)\b"
)
_FIRST_PERSON_COMPLETION = re.compile(
    r"\bi (?:(?:now )?know what i (?:want|need)|"
    r"(?:have|ve) (?:now )?decided|"
    r"(?:have|ve) made (?:up )?my (?:mind|decision)|"
    r"(?:am|m) (?:all )?(?:done|finished)|"
    r"(?:have|ve) (?:got )?what i need|"
    r"(?:do not|don t) need anything (?:else|more))\b"
)
_EXPLICIT_COMPLETION_OR_CANCELLATION = re.compile(
    r"\b(?:no that (?:is|s) all|that (?:is|s) (?:all|everything)|never mind)\b"
)


def _answer_requires_spoken_close(transcript: str) -> bool:
    """Reduce a confirmed answer to one non-reversible semantic veto bit."""
    normalized = " ".join(
        re.sub(r"[^a-z0-9]+", " ", transcript.casefold()).split()
    )
    return bool(
        _HIGH_CONFIDENCE_GRATITUDE.search(normalized)
        or _FIRST_PERSON_COMPLETION.search(normalized)
        or _EXPLICIT_COMPLETION_OR_CANCELLATION.search(normalized)
    )

# The OpenAI Realtime API works in 24 kHz PCM16. The Voice PE firmware plays
# 24 kHz back and streams 16 kHz up. IMPORTANT: pipecat 0.0.97's websocket INPUT
# transport does NOT resample (only the OUTPUT transport does), and OpenAI
# Realtime's pcm16 input rate is hard-locked to 24000 (PCMAudioFormat.rate =
# Literal[24000]) — you cannot tell it the audio is 16 kHz. So the device's
# 16 kHz frames would be read 1.5x too fast / pitched up, garbling the whole
# transcript. The InputResampler below upsamples 16k->24k in the pipeline.
PIPELINE_SAMPLE_RATE = 24000


class _FollowUpStage(str, Enum):
    RESERVED = "reserved"
    PREPARING = "preparing"
    PREPARED = "prepared"
    READY = "ready"
    COMMITTING = "committing"
    OPEN = "open"


@dataclass(frozen=True)
class _ReplyFinalizerContext:
    websocket: Any
    session_nonce: int
    wake_generation: int
    reply_generation: int


@dataclass
class _FollowUpAnswerGrant:
    websocket: Any
    session_nonce: int
    wake_generation: int
    reservation_epoch: int
    token: int
    non_close_tool_generation: int
    user_item_id: Optional[str] = None
    user_item_sequence: Optional[int] = None
    confirmed: bool = False
    semantic_close_veto: bool = False


@dataclass(frozen=True)
class _OpenFollowUpPhaseGrant:
    websocket: Any
    session_nonce: int
    wake_generation: int
    token: int


@dataclass(frozen=True)
class _SilentCloseContext:
    websocket: Any
    session_nonce: int
    wake_generation: int


@dataclass(frozen=True)
class _GracefulCloseContext:
    websocket: Any
    session_nonce: int
    wake_generation: int
    token: int


@dataclass(frozen=True)
class _GracefulCloseAckExpectation:
    context: _GracefulCloseContext
    stage: str
    result: asyncio.Future


@dataclass(frozen=True)
class _AssistantOutputGrant:
    websocket: Any
    session_nonce: int
    wake_generation: int
    response_id: str
    response_generation: int
    authority_epoch: int


@dataclass(frozen=True)
class _PhaseAuthorizationContext:
    websocket: Any
    session_nonce: int
    wake_generation: int
    follow_up_epoch: int
    follow_up_token: Optional[int]
    terminal_idle: bool = False


@dataclass
class _FollowUpReservation:
    websocket: Any
    session_nonce: int
    tool_call_id: str
    non_close_tool_generation: int
    epoch: int
    token: int
    wake_generation: int
    expires_at: float
    stage: _FollowUpStage = _FollowUpStage.RESERVED
    active: bool = False
    continuation_armed: bool = False
    response_id: Optional[str] = None
    response_generation: Optional[int] = None
    question_audio_started: bool = False
    playback_started: bool = False
    response_completed: bool = False
    control_send_started: bool = False
    control_sent: bool = False
    cancel_requested: bool = False
    cancel_send_started: bool = False
    cancel_sent: bool = False
    ack_received: bool = False
    ack_accepted: bool = False
    ack_event: asyncio.Event = field(default_factory=asyncio.Event)
    ready_nonce: Optional[int] = None
    commit_send_started: bool = False
    commit_sent: bool = False
    commit_ack_received: bool = False
    commit_ack_accepted: bool = False
    commit_ack_event: asyncio.Event = field(default_factory=asyncio.Event)
    cancel_ack_received: bool = False
    cancel_ack_accepted: bool = False
    cancel_ack_cleared: bool = False
    cancel_ack_confirms_revocation: bool = False
    cancel_ack_event: asyncio.Event = field(default_factory=asyncio.Event)
    cancel_task: Optional[asyncio.Task] = None
    socket_retired: bool = False
    revocation_confirmed: bool = False


@dataclass
class _HelloTransaction:
    websocket: Any
    client_id: str
    nonce: int
    values: dict
    on_admitted: Callable[[str], Awaitable[None]]


class SessionActivityTracker(FrameProcessor):
    """Processor that tracks session activity by monitoring audio frames."""
    
    def __init__(self, activity_callback, **kwargs):
        super().__init__(**kwargs)
        self.activity_callback = activity_callback
    
    async def process_frame(self, frame: Frame, direction: FrameDirection):
        if isinstance(frame, StartFrame):
            logger.debug("🎬 SessionActivityTracker: Received StartFrame")
            await super().process_frame(frame, direction)
            await self.push_frame(frame, direction)
            return
        elif isinstance(frame, EndFrame):
            logger.debug("🏁 SessionActivityTracker: Received EndFrame")
            await self.push_frame(frame, direction)
            return
        
        # Track activity on any audio frame
        if isinstance(frame, (InputAudioRawFrame, OutputAudioRawFrame)):
            if self.activity_callback:
                self.activity_callback()
            logger.debug(f"🎵 SessionActivityTracker: Processing {type(frame).__name__} ({len(frame.audio)} bytes)")
        
        # Pass frame through to next processor
        await self.push_frame(frame, direction)


class InputResampler(FrameProcessor):
    """Upsample incoming device mic audio to the OpenAI Realtime input rate.

    The Voice PE streams 16 kHz PCM16. pipecat 0.0.97's websocket input transport
    forwards those frames unchanged, and OpenAI Realtime reads pcm16 input at a
    fixed 24 kHz — so without this the audio is interpreted ~1.5x too fast,
    badly degrading transcription (e.g. first word dropped, words mangled). This
    sits right after transport.input() and resamples each InputAudioRawFrame to
    out_rate. Uses a streaming resampler so there are no per-chunk edge artifacts.
    """

    def __init__(self, out_rate: int = PIPELINE_SAMPLE_RATE, **kwargs):
        super().__init__(**kwargs)
        self._out_rate = out_rate
        self._resampler = create_stream_resampler()
        self._logged = False

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if isinstance(frame, InputAudioRawFrame) and frame.sample_rate != self._out_rate:
            if not frame.audio:
                return  # nothing to resample / forward; don't emit empty audio
            try:
                resampled = await self._resampler.resample(
                    frame.audio, frame.sample_rate, self._out_rate
                )
            except Exception as e:
                logger.warning(f"⚠️ input resample {frame.sample_rate}->{self._out_rate} failed: {e!r}")
                return  # drop rather than forward wrong-rate audio
            # The streaming resampler buffers internally and can return empty
            # bytes while priming or on a tiny chunk. OpenAI rejects an
            # input_audio_buffer.append with empty audio ("got empty bytes"), so
            # drop those frames — the samples stay buffered and come out next call.
            if not resampled:
                return
            if not self._logged:
                logger.info(
                    f"🎙️ Resampling device input {frame.sample_rate}Hz -> {self._out_rate}Hz for OpenAI"
                )
                self._logged = True
            frame = InputAudioRawFrame(
                audio=resampled,
                sample_rate=self._out_rate,
                num_channels=frame.num_channels,
            )
        await self.push_frame(frame, direction)


class ConnectionRecovery(FrameProcessor):
    """Auto-reconnect the OpenAI Realtime session when its WebSocket dies.

    pipecat 0.0.97's OpenAIRealtimeLLMService has NO reconnect logic: when the
    OpenAI WS drops (1011 keepalive ping timeout, 1001 going away on the 60-min
    cap, 1006, or any send/receive failure) it treats the send error as fatal and
    floods ErrorFrame — ~15/s, one per forwarded mic frame — forever. The single
    persistent session is then dead until the add-on restarts, so the device gets
    no answer to any further turn (observed live: a 1011 flood after which the
    next question got silence).

    This processor watches the ErrorFrames as they travel upstream to the task
    source, and on the first connection-death signature it:
      1. emits `idle` to the device so it unsticks (LED + mic reset), and
      2. calls service.reset_conversation() — the one PUBLIC method that does
         _disconnect() + _connect() + re-sends the session config (instructions,
         tools, turn detection) — to bring the session back IN PLACE. No pipeline
         rebuild: the running pipeline keeps the same service object, which is
         exactly the one reset_conversation reconnects.
    A guard + cooldown collapse the error flood into a single reconnect attempt,
    retrying at most every RECONNECT_COOLDOWN_S while the link stays down.
    """

    # Substrings that mark a dead/closed OpenAI websocket (vs an app-level error
    # like a tool failure, which we must NOT reconnect on). These appear on the
    # SEND-side flood ("Error sending client event: …"), so they're paired with
    # the "client event" check below to avoid reacting to a device disconnect.
    _DEATH_MARKERS = (
        "keepalive ping timeout",
        "going away",
        "no close frame",
        "ConnectionClosed",
        "connection is closed",
        "sent 1011",
        "sent 1001",
        "1006",
    )
    # Substrings that UNAMBIGUOUSLY mean OUR OpenAI session is gone and must be
    # reconnected, regardless of how the error surfaced. The 60-minute cap can
    # arrive as a proactive OpenAI *error event* (code='session_expired', "Your
    # session hit the maximum duration of 60 minutes.") with NO "client event"
    # send-flood and NO close-code marker — so the paired check above misses it
    # and the session stays dead until the add-on restarts. These markers force a
    # reconnect on their own. They can only come from OpenAI (not a device close),
    # so no "client event" guard is needed.
    _SESSION_DEAD_MARKERS = (
        "session_expired",
        "maximum duration",
        "context compaction failed",
    )
    RECONNECT_COOLDOWN_S = 5.0
    RECONNECT_BACKOFF_INITIAL_S = 1.0
    RECONNECT_BACKOFF_MAX_S = 15.0
    TOOL_DRAIN_TIMEOUT_S = 180.0
    IDLE_UNSTICK_COOLDOWN_S = 2.0
    # Proactive refresh: reconnect BEFORE OpenAI's 60-min session cap, but only
    # while the house is genuinely quiet, so the cap practically never lands
    # mid-conversation (where it costs the user a turn).
    REFRESH_AGE_S = 55 * 60   # refresh once the session is this old
    REFRESH_QUIET_S = 60.0    # ... and no mic audio flowed for this long
    REFRESH_CHECK_S = 60.0    # poll cadence of the background check

    def __init__(
        self,
        openai_service,
        emit_idle=None,
        phase_emitter=None,
        on_recovery_started=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._service = openai_service
        self._emit_idle = emit_idle  # async callable(value:str), e.g. broadcast_phase
        # Preferred idle route: PhaseEmitter.force_idle() keeps the emitter's
        # phase state consistent AND suppresses the racing `thinking` from VAD
        # stop events still in flight (observed: a raw broadcast idle was
        # overridden 400 ms later and the device sat in `thinking` with an
        # open mic for 44 s). emit_idle stays as fallback wiring.
        self._phase_emitter = phase_emitter
        self._on_recovery_started = on_recovery_started
        self._reconnecting = False
        self._last_attempt = 0.0
        self._last_idle_unstick = 0.0
        # Diagnostics: when the current OpenAI session connected, so we can log its
        # age at a drop (the 60-min cap shows up as ~3600 s) and the reconnect
        # duration (the brief gap the user hears).
        self._connected_at = time.monotonic()
        # Proactive-refresh state. This processor sits right behind
        # transport.input(), so every mic frame passes through it — the cheapest
        # possible "is anyone interacting?" signal (the device only streams the
        # mic during an active turn or the follow-up window).
        self._last_input_audio = time.monotonic()
        self._refresh_task = None
        self._recovery_task = None
        self._recovery_delayed = False
        self._recovery_complete_callback = None

    def set_recovery_complete_callback(self, callback) -> None:
        self._recovery_complete_callback = callback

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        if self._refresh_task is None:
            self._refresh_task = asyncio.create_task(self._proactive_refresh_loop())
        if isinstance(frame, InputAudioRawFrame):
            # Only kept for the proactive-refresh "is anyone interacting?" check.
            # (Stale-audio clearing is now done at the cut-off source — the device
            # sends {"type":"flush"} when a follow-up window times out — not
            # reactively on mic-resume, which disturbed the VAD and caused garbage.)
            self._last_input_audio = time.monotonic()
            if self._reconnecting:
                logger.debug("🔇 dropping device audio during OpenAI recovery")
                return
        if isinstance(frame, ErrorFrame) and not self._reconnecting:
            msg = str(getattr(frame, "error", "") or "")
            # Two reconnect triggers:
            #  (a) the OpenAI send-side flood ("Error sending client event: …" +
            #      a close-code marker) — OUR WS died mid-send. We require the
            #      "client event" signature so a normal DEVICE-side disconnect
            #      (also 1011/ConnectionClosed, but the device went away) does NOT
            #      trigger an OpenAI reconnect.
            #  (b) an unambiguous OpenAI session-dead error event (session_expired
            #      / "maximum duration") — this is the 60-min cap surfacing as a
            #      proactive error event with NO send-flood, so (a) misses it.
            #      It can only come from OpenAI, so it needs no "client event" guard.
            send_flood = "client event" in msg and any(m in msg for m in self._DEATH_MARKERS)
            session_dead = any(m in msg for m in self._SESSION_DEAD_MARKERS)
            # (c) the OpenAI READ side died or ended (network drop / silent
            #     server close). pipecat produces no ErrorFrame for these at
            #     all — SafeRealtimeLLMService wraps the receive loop and
            #     reports them with this message. Without it the session sat
            #     deaf for hours until the next utterance hit the dead socket.
            reader_dead = "realtime receive loop" in msg
            if send_flood or session_dead or reader_dead:
                self._notify_recovery_started()
                now = time.monotonic()
                delay = max(
                    0.0,
                    self.RECONNECT_COOLDOWN_S - (now - self._last_attempt),
                )
                self._reconnecting = True
                if delay:
                    self._recovery_delayed = True
                    self._recovery_task = asyncio.create_task(
                        self._recover_after(delay, msg)
                    )
                else:
                    self._recovery_delayed = False
                    self._last_attempt = now
                    self._recovery_task = asyncio.create_task(self._recover(msg))
            else:
                # Non-connection-death error that ENDS a turn without a reply:
                # most importantly an OpenAI rate-limit ("Rate limit reached …"),
                # but also any other transient response.create failure. No bot
                # speech was produced, so PhaseEmitter never fires
                # BotStopped→idle; the device is left stuck in `thinking`
                # (LED keeps blinking) with no device-side watchdog to recover.
                # Emit one `idle` to unstick it so the user can just try again.
                # Guarded by a short cooldown so a rare flood collapses to one.
                now = time.monotonic()
                if now - self._last_idle_unstick >= self.IDLE_UNSTICK_COOLDOWN_S:
                    self._last_idle_unstick = now
                    asyncio.create_task(self._unstick_idle(msg))
        await self.push_frame(frame, direction)

    async def _recover_after(self, delay: float, reason: str) -> None:
        await asyncio.sleep(delay)
        self._recovery_delayed = False
        await self._recover(reason)

    async def force_reconnect(
        self,
        reason: str,
        *,
        bypass_cooldown: bool = False,
    ) -> None:
        """Positive-liveness reconnect: for wedged (half-open) sockets that
        produce NO ErrorFrames at all — audio streams out, nothing comes back
        (observed live 2026-07-16: wake + speech after an idle gap → zero
        server events, no error, request lost)."""
        now = time.monotonic()
        if self._reconnecting:
            if not (
                bypass_cooldown
                and self._recovery_delayed
                and self._recovery_task is not None
            ):
                return
            self._recovery_task.cancel()
            await asyncio.gather(self._recovery_task, return_exceptions=True)
            self._reconnecting = False
            self._recovery_delayed = False
        if (
            not bypass_cooldown
            and now - self._last_attempt < self.RECONNECT_COOLDOWN_S
        ):
            return
        self._reconnecting = True
        self._notify_recovery_started()
        self._last_attempt = now
        self._recovery_delayed = False
        self._recovery_task = asyncio.create_task(self._recover(reason))
        await self._recovery_task

    async def reject_wake_while_recovering(self) -> bool:
        """Close a device wake immediately while OpenAI is unavailable."""
        if not self._reconnecting:
            return False
        logger.warning("🔌 device woke during OpenAI recovery — returning it to idle")
        await self._go_idle("wake during OpenAI recovery", force_delivery=True)
        return True

    async def _recover(self, reason: str):
        self._notify_recovery_started()
        t0 = time.monotonic()
        age_s = t0 - self._connected_at
        self._reconnecting = True
        try:
            begin_recovery = getattr(self._service, "begin_recovery", None)
            if begin_recovery is not None:
                begin_recovery()
            logger.warning(
                f"🔌 OpenAI Realtime connection lost after {age_s:.0f}s "
                f"({reason[:90]}) — reconnecting…"
            )
            # Unstick the device first, regardless of how the reconnect goes.
            try:
                await asyncio.wait_for(
                    self._go_idle(f"reconnect: {reason[:60]}"),
                    timeout=2.0,
                )
            except TimeoutError:
                logger.warning("⚠️ timed out emitting idle during recovery")
            except Exception as e:
                logger.warning(f"⚠️ could not emit idle during recovery: {e!r}")
            reset = getattr(self._service, "reset_conversation", None)
            if reset is None:
                logger.error("❌ service has no reset_conversation(); cannot reconnect in place")
                return
            if begin_recovery is not None:
                await asyncio.sleep(0)
            tool_deadline = time.monotonic() + self.TOOL_DRAIN_TIMEOUT_S
            while TURN_LIVENESS.in_flight > 0:
                if time.monotonic() >= tool_deadline:
                    logger.warning(
                        "⚠️ old tools exceeded recovery drain timeout; "
                        "discarding their future conversation results"
                    )
                    discard_running = getattr(
                        self._service,
                        "discard_running_tool_results",
                        None,
                    )
                    if discard_running is not None:
                        discard_result = discard_running()
                        if discard_result is not None:
                            await discard_result
                    break
                logger.info(
                    "🔇 waiting for %s old tool(s) before replacing OpenAI session",
                    TURN_LIVENESS.in_flight,
                )
                await asyncio.sleep(0.1)
            wait_for_scheduled_calls = getattr(
                self._service,
                "wait_for_scheduled_tool_calls",
                None,
            )
            if wait_for_scheduled_calls is not None:
                await wait_for_scheduled_calls()
            wait_for_pending_results = getattr(
                self._service,
                "wait_for_pending_tool_results",
                None,
            )
            if wait_for_pending_results is not None:
                await wait_for_pending_results()
            attempt = 0
            backoff_s = self.RECONNECT_BACKOFF_INITIAL_S
            while True:
                attempt += 1
                self._last_attempt = time.monotonic()
                try:
                    await reset()
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.error(
                        f"❌ OpenAI reconnect attempt {attempt} failed: {e!r}; "
                        f"retrying in {backoff_s:.1f}s"
                    )
                    await asyncio.sleep(backoff_s)
                    backoff_s = min(
                        backoff_s * 2,
                        self.RECONNECT_BACKOFF_MAX_S,
                    )
                    continue

                mark_complete = getattr(self._service, "mark_recovery_complete", None)
                if mark_complete is not None:
                    mark_complete()
                if self._recovery_complete_callback is not None:
                    self._recovery_complete_callback()
                self._connected_at = time.monotonic()
                logger.info(
                    f"✅ OpenAI Realtime session ready after {attempt} attempt(s) in "
                    f"{self._connected_at - t0:.1f}s (gap the user may have heard)"
                )
                return
        finally:
            self._reconnecting = False
            if self._recovery_task is asyncio.current_task():
                self._recovery_task = None

    def _notify_recovery_started(self) -> None:
        if self._on_recovery_started is None:
            return
        try:
            self._on_recovery_started()
        except Exception as error:
            logger.warning("⚠️ recovery-start callback failed: %r", error)

    async def _proactive_refresh_loop(self):
        """Refresh the OpenAI session BEFORE the 60-min cap, during real idle.

        The cap reconnect is recoverable (~3 s), but when it lands
        mid-conversation that turn hiccups. Refreshing proactively while
        nothing is happening means users practically never meet the cap.
        "Quiet" is double-checked: no assistant response in flight AND no mic
        audio for REFRESH_QUIET_S — so it can never fire during a turn, a
        reply, or an open follow-up window.
        """
        while True:
            try:
                await asyncio.sleep(self.REFRESH_CHECK_S)
                if self._reconnecting:
                    continue
                now = time.monotonic()
                age = now - self._connected_at
                quiet = now - self._last_input_audio
                busy = (
                    getattr(self._service, "_current_assistant_response", None) is not None
                    or TURN_LIVENESS.in_flight > 0
                )
                if (age >= self.REFRESH_AGE_S and quiet >= self.REFRESH_QUIET_S
                        and not busy and now - self._last_attempt >= self.RECONNECT_COOLDOWN_S):
                    self._reconnecting = True
                    self._last_attempt = now
                    logger.info(
                        f"🔄 proactive session refresh (session {age/60:.0f} min old, "
                        f"quiet for {quiet:.0f}s) — staying ahead of the 60-min cap"
                    )
                    await self._recover("proactive refresh before the 60-min session cap")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"⚠️ proactive refresh loop error: {e!r}")

    async def _go_idle(self, reason: str, force_delivery: bool = False) -> None:
        """Put the device in idle for a dead turn — via PhaseEmitter when wired."""
        if self._phase_emitter is not None:
            await self._phase_emitter.force_idle(
                reason,
                force_delivery=force_delivery,
            )
        elif self._emit_idle is not None:
            await self._emit_idle("idle")

    async def cleanup(self) -> None:
        """Stop recovery-owned tasks before the pipeline service shuts down."""
        current_task = asyncio.current_task()
        tasks = [
            task
            for task in (self._recovery_task, self._refresh_task)
            if task is not None and task is not current_task and not task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._recovery_task = None
        self._refresh_task = None
        self._reconnecting = False
        await super().cleanup()

    async def _unstick_idle(self, reason: str):
        """Emit `idle` to the device after a turn-ending error (e.g. rate limit).

        The session is still alive (no reconnect needed) — we just nudge the
        device out of its stuck `thinking` blink so the user can retry.
        """
        try:
            logger.warning(f"⚠️ turn ended on error, emitting idle to unstick device ({reason[:90]})")
            await self._go_idle(f"turn ended on error: {reason[:60]}")
        except Exception as e:
            logger.warning(f"⚠️ could not emit idle after turn-ending error: {e!r}")


class WebSocketHandler:
    """Handles WebSocket transport initialization, pipeline building, and event management."""

    GRACEFUL_CLOSE_ACK_TIMEOUT_S = 3.0
    REQUEST_FOLLOW_UP_EXPIRY_S = 15.0
    REQUEST_FOLLOW_UP_SEND_TIMEOUT_S = 1.0
    REQUEST_FOLLOW_UP_ACK_TIMEOUT_S = 2.0
    FIRMWARE_AUDIO_RING_BYTES = 2 * 1024 * 1024
    FIRMWARE_OUTPUT_BYTES_PER_SECOND = 24000 * 2
    FIRMWARE_PLAYBACK_PREBUFFER_MAX_S = 2.0
    FIRMWARE_SPEAKER_DRAIN_TIMEOUT_S = 3.0
    FIRMWARE_MIC_SEND_BARRIER_TIMEOUT_S = 0.05
    FIRMWARE_FOLLOW_UP_CHIME_WAIT_TIMEOUT_S = 2.0
    FIRMWARE_FOLLOW_UP_READY_CALLBACK_TIMEOUT_S = 8.0
    FIRMWARE_FOLLOW_UP_COMMIT_TIMEOUT_S = 5.0
    FOLLOW_UP_PROTOCOL_MARGIN_S = 2.0
    REQUEST_FOLLOW_UP_COMMIT_ACK_TIMEOUT_S = (
        FIRMWARE_FOLLOW_UP_COMMIT_TIMEOUT_S + 1.0
    )
    REQUEST_FOLLOW_UP_ACCEPTED_TTL_S = 12.0
    PHYSICAL_WAKE_CEILING_S = 120.0
    FOLLOW_UP_MEDIA_CHECK_TIMEOUT_S = 1.0
    HELLO_SEND_TIMEOUT_S = 1.0
    HELLO_ACK_TIMEOUT_S = 3.0
    SOCKET_CLOSE_TIMEOUT_S = 1.0
    ASSISTANT_CANCEL_TIMEOUT_S = 2.0
    PROTOCOL_HISTORY_LIMIT = 256
    MAX_UNCERTAIN_SOCKETS = 4
    MAX_FOLLOW_UP_TASKS = 32
    MAX_FOLLOW_UP_CANCELLATIONS = 8
    MAX_WEDGE_TASKS = 32
    
    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        session_manager: Optional[SessionManager] = None,
        audio_recording_service: Optional[AudioRecordingService] = None,
        follow_up_ms: int = 0,
        follow_up_open_delay_ms: int = 700,
        wake_open_delay_ms: int = 700,
        playback_prebuffer_ms: int = 0,
        media_activity_check: Optional[Callable[[], Awaitable[MediaActivity]]] = None,
    ):
        """
        Initialize WebSocket handler.

        Args:
            host: Host address to bind to
            port: Port to listen on
            session_manager: Session manager instance
            audio_recording_service: Audio recording service instance
            follow_up_ms: Legacy automatic-window duration sent in `hello`.
                Version 0.22.5 requires 0; explicit windows use the separate
                PREPARE/READY/COMMIT transaction.
            follow_up_open_delay_ms: How long (ms) the device waits after a reply
                finishes before opening that follow-up mic (bridges the speaker
                hardware tail). Sent in the `hello` handshake.
            wake_open_delay_ms: How long (ms) the device waits after the wake
                chime before opening the mic, so the chime's hardware tail can't
                leak into the fresh mic as a ghost turn. Sent in `hello`.
        """
        self.host = host
        self.port = port
        self.session_manager = session_manager
        self.audio_recording_service = audio_recording_service
        self.follow_up_ms = max(0, int(follow_up_ms))
        self.follow_up_open_delay_ms = max(0, int(follow_up_open_delay_ms))
        self.wake_open_delay_ms = max(0, int(wake_open_delay_ms))
        self.playback_prebuffer_ms = max(0, int(playback_prebuffer_ms))
        self._media_activity_check = media_activity_check

        self.transport: Optional[WebsocketServerTransport] = None
        self.pipeline: Optional[Pipeline] = None
        self.runner: Optional[PipelineRunner] = None
        self.current_task: Optional[PipelineTask] = None
        self._connection_recovery: Optional[ConnectionRecovery] = None
        # The serializer instance the transport reads through. Kept so
        # build_pipeline can wire its device-interrupt callback to the OpenAI
        # service.
        self._serializer: Optional[RawAudioSerializer] = None
        # Connected device websockets, used to push va_client control/phase
        # messages as TEXT frames (the audio path uses the binary serializer).
        self._websockets: set = set()
        self._uncertain_retired_sockets: set = set()
        self._active_session_nonce: Optional[int] = None
        self._socket_transition_lock = asyncio.Lock()
        self._wedge_tasks: set = set()
        # Graceful close is a single-device acknowledged control transaction.
        # Old firmware and out-of-turn requests fail closed instead of silently
        # claiming that the next follow-up will be suppressed.
        self._graceful_close_lock = asyncio.Lock()
        self._graceful_close_next_token = 1
        self._graceful_close_pending_token: Optional[int] = None
        self._graceful_close_pending_context: Optional[
            _GracefulCloseContext
        ] = None
        self._graceful_close_owner_context: Optional[
            _GracefulCloseContext
        ] = None
        self._graceful_close_ack_expectation: Optional[
            _GracefulCloseAckExpectation
        ] = None
        self._graceful_close_requested_generation: Optional[int] = None
        self._graceful_close_committed_token: Optional[int] = None
        self._graceful_close_committed_context: Optional[
            _GracefulCloseContext
        ] = None
        self._user_turn_non_close_generation: Optional[int] = None
        self._device_wake_generation = 0
        self._device_audio_generation: Optional[int] = None
        self._wake_session_socket: Any = None
        self._wake_session_nonce: Optional[int] = None
        self._physical_wake_deadline = 0.0
        self._request_follow_up_budget_spent = True
        self._request_follow_up_budget_tool_call_id: Optional[str] = None
        self._request_follow_up_answer_grant: Optional[_FollowUpAnswerGrant] = None
        self._open_follow_up_phase_grant: Optional[_OpenFollowUpPhaseGrant] = None
        self._silent_close_context: Optional[_SilentCloseContext] = None
        self._request_follow_up_epoch = 0
        self._reply_generation = 0
        self._assistant_output_grant: Optional[_AssistantOutputGrant] = None
        self._assistant_output_authority_epoch = 0
        self._connection_recovery_active = False
        self._recovery_output_settlement_task: Optional[asyncio.Task] = None
        self._cancel_assistant_output_callback = None
        self._device_input_clear_generation = 0
        self._issued_request_follow_up_tokens: set[int] = set()
        self._seen_ready_nonces: set[int] = set()
        self._request_follow_up_reservation: Optional[_FollowUpReservation] = None
        self._bound_follow_up_question_context: Optional[
            tuple[str, int, int, int]
        ] = None
        self._request_follow_up_cancellations: Dict[tuple[int, int], _FollowUpReservation] = {}
        self._request_follow_up_expiry_task: Optional[asyncio.Task] = None
        self._request_follow_up_tasks: set[asyncio.Task] = set()
        self._request_follow_up_settlement_tasks: set[asyncio.Task] = set()
        self._follow_up_fail_closed = False
        self._request_follow_up_control_lock = asyncio.Lock()
        self._issued_hello_nonces: set[int] = set()
        self._hello_transaction: Optional[_HelloTransaction] = None
        self._hello_timeout_task: Optional[asyncio.Task] = None
        self._on_client_disconnected_callback: Optional[Callable[[str], None]] = None
        self._clear_device_input = None
        self._input_clear_fail_closed = False
        self._input_clear_settled = asyncio.Event()
        self._input_clear_settled.set()
        self._input_clear_recovery_ready = False
        # Speaker context v1 (fork): set by main.py when speaker names are
        # configured; wired to the serializer + OpenAI service in build_pipeline.
        self.speaker_probe = None
    def _track_wedge_task(self, coroutine) -> Optional[asyncio.Task]:
        if len(self._wedge_tasks) >= self.MAX_WEDGE_TASKS:
            close = getattr(coroutine, "close", None)
            if close is not None:
                close()
            logger.error("Voice lifecycle background-task limit reached")
            return None
        task = asyncio.create_task(coroutine)
        self._wedge_tasks.add(task)
        task.add_done_callback(self._wedge_tasks.discard)
        return task

    def _on_connection_recovery_started(self) -> None:
        self._connection_recovery_active = True
        self._device_audio_generation = None
        self._open_follow_up_phase_grant = None
        graceful_context = self._graceful_close_owner_context
        if graceful_context is not None:
            self._clear_graceful_close_context(graceful_context)
        retired_output = self._retire_assistant_output_grant()
        if retired_output is not None:
            existing = self._recovery_output_settlement_task
            if existing is not None and not existing.done():
                self._enter_follow_up_fail_closed(
                    "overlapping recovery output settlement"
                )
            else:
                async def settle_recovery_output() -> None:
                    try:
                        await self._settle_retired_assistant_output(retired_output)
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        logger.warning(
                            "Recovery output settlement failed: %r",
                            error,
                        )
                        await self._retire_bound_socket(
                            retired_output.websocket,
                            retired_output.session_nonce,
                        )

                task = asyncio.create_task(settle_recovery_output())
                self._recovery_output_settlement_task = task

                def clear_recovery_settlement(completed: asyncio.Task) -> None:
                    if self._recovery_output_settlement_task is completed:
                        self._recovery_output_settlement_task = None
                    if not completed.cancelled():
                        completed.exception()

                task.add_done_callback(clear_recovery_settlement)
        self.invalidate_request_follow_up_turn()
    
    def create_transport(self) -> WebsocketServerTransport:
        """
        Create and initialize WebSocket transport.
        
        Returns:
            WebsocketServerTransport instance
        """
        logger.info("Initializing WebSocket transport...")
        
        # Use RawAudioSerializer for binary PCM audio. It tags incoming frames
        # with the device mic rate (16 kHz for Voice PE); the transport
        # resamples in/out to the 24 kHz pipeline rate below.
        serializer = RawAudioSerializer()
        self._serializer = serializer
        set_control_handler = getattr(serializer, "set_control_handler", None)
        if set_control_handler is not None:
            set_control_handler(self._handle_device_control_message)
        serializer.set_binary_audio_authorizer(self._binary_audio_is_admitted)
        self._set_serializer_audio_admitted(False)

        # Create WebsocketServerTransport with WebsocketServerParams
        # The transport will start its own server automatically
        transport = SingleOwnerWebsocketServerTransport(
            host=self.host,
            port=self.port,
            params=WebsocketServerParams(
                serializer=serializer,
                audio_in_enabled=True,
                audio_out_enabled=True,
                audio_in_sample_rate=PIPELINE_SAMPLE_RATE,
                audio_out_sample_rate=PIPELINE_SAMPLE_RATE,
            )
        )
        transport.set_output_audio_authorizer(self._authorize_output_audio)
        self.transport = transport
        
        logger.info(f"✅ WebSocket transport created - will listen on ws://{self.host}:{self.port}/")
        return self.transport

    def _binary_audio_is_admitted(self, websocket: Any) -> bool:
        if self._follow_up_fail_closed or self._input_clear_fail_closed:
            return False
        if websocket is None or tuple(self._websockets) != (websocket,):
            return False
        if self._active_session_nonce is None:
            return False
        if (
            self._device_wake_generation == 0
            or self._device_audio_generation != self._device_wake_generation
            or not self._physical_wake_is_current()
            or self._silent_close_is_current()
        ):
            return False
        reservation = self._request_follow_up_reservation
        if reservation is None:
            return True
        return (
            reservation.websocket is websocket
            and reservation.session_nonce == self._active_session_nonce
            and reservation.wake_generation == self._device_wake_generation
            and reservation.stage is _FollowUpStage.OPEN
            and reservation.commit_ack_received
            and reservation.commit_ack_accepted
            and self._request_follow_up_context_is_valid(reservation)
        )

    async def _authorize_output_audio(
        self,
        context: Optional[tuple[str, int]] = None,
        websocket: Any = None,
    ) -> bool:
        if self._follow_up_fail_closed or self._input_clear_fail_closed:
            return False
        if not self._physical_wake_is_current() or self._silent_close_is_current():
            return False
        bound_question = self._bound_follow_up_question_context
        if (
            bound_question is not None
            and isinstance(context, tuple)
            and len(context) == 2
            and context == bound_question[:2]
            and not self.request_follow_up_question_output_is_current(
                context[0],
                context[1],
            )
        ):
            return False
        reservation = self._request_follow_up_reservation
        if reservation is not None and reservation.stage is not _FollowUpStage.RESERVED:
            logger.warning("Late assistant audio invalidated a prepared follow-up")
            self.cancel_request_follow_up()
            return False
        grant = self._assistant_output_grant
        return (
            grant is not None
            and context == (grant.response_id, grant.response_generation)
            and websocket is grant.websocket
            and self._assistant_output_grant_is_current(grant)
        )

    def _assistant_output_grant_is_current(
        self,
        grant: _AssistantOutputGrant,
    ) -> bool:
        return (
            self._assistant_output_grant is grant
            and grant.authority_epoch == self._assistant_output_authority_epoch
            and not self._connection_recovery_active
            and tuple(self._websockets) == (grant.websocket,)
            and self._active_session_nonce == grant.session_nonce
            and self._device_wake_generation == grant.wake_generation
            and self._physical_wake_is_current()
            and not self._silent_close_is_current()
        )

    async def finish_assistant_output_response(
        self,
        response_id: str,
        response_generation: int,
    ) -> bool:
        """Drain one response and settle failure before releasing its grant."""
        context = (response_id, response_generation)
        async with self._socket_transition_lock:
            grant = self._assistant_output_grant
            if (
                grant is None
                or context != (grant.response_id, grant.response_generation)
                or not self._assistant_output_grant_is_current(grant)
            ):
                return False
            deadline = self._physical_wake_deadline

        finish_generation = getattr(
            self.transport,
            "gracefully_finish_output_audio_generation",
            None,
        )
        if finish_generation is None:
            return False
        finished = await finish_generation(
            context,
            grant.websocket,
            lambda: self._assistant_output_grant_is_current(grant),
            timeout_s=max(0.0, deadline - time.monotonic()),
        )
        if finished is not True:
            await self._settle_retired_assistant_output(grant)

        async with self._socket_transition_lock:
            current = self._assistant_output_grant_is_current(grant)
            if finished is True and current:
                return True
            if self._assistant_output_grant is grant:
                self._assistant_output_grant = None
                retire_generation = getattr(
                    self.transport,
                    "retire_output_audio_generation",
                    None,
                )
                if retire_generation is not None:
                    retire_generation(context)
            return False

    async def bind_assistant_output_response(
        self,
        response_id: str,
        response_generation: int,
    ) -> bool:
        """Grant PCM only to the physical wake that owns this response."""
        async with self._socket_transition_lock:
            sockets = tuple(self._websockets)
            session_nonce = self._active_session_nonce
            wake_generation = self._device_wake_generation
            authority_epoch = self._assistant_output_authority_epoch
            if (
                len(sockets) != 1
                or type(session_nonce) is not int
                or session_nonce <= 0
                or type(wake_generation) is not int
                or wake_generation <= 0
                or self._connection_recovery_active
                or not self._physical_wake_is_current()
                or self._silent_close_is_current()
            ):
                self._assistant_output_grant = None
                retire_generation = getattr(
                    self.transport,
                    "retire_output_audio_generation",
                    None,
                )
                if retire_generation is not None:
                    retire_generation()
                return False
            grant = _AssistantOutputGrant(
                websocket=sockets[0],
                session_nonce=session_nonce,
                wake_generation=wake_generation,
                response_id=response_id,
                response_generation=response_generation,
                authority_epoch=authority_epoch,
            )
            bind_generation = getattr(
                self.transport,
                "bind_output_audio_generation",
                None,
            )
            if bind_generation is not None and not await bind_generation(
                (response_id, response_generation),
                sockets[0],
            ):
                self._assistant_output_grant = None
                return False
            if (
                tuple(self._websockets) != sockets
                or self._active_session_nonce != session_nonce
                or self._device_wake_generation != wake_generation
                or self._assistant_output_authority_epoch != authority_epoch
                or self._connection_recovery_active
                or not self._physical_wake_is_current()
                or self._silent_close_is_current()
            ):
                self._assistant_output_grant = None
                retire_generation = getattr(
                    self.transport,
                    "retire_output_audio_generation",
                    None,
                )
                if retire_generation is not None:
                    retire_generation((response_id, response_generation))
                return False
            self._assistant_output_grant = grant
            return True

    def register_assistant_output_frame(
        self,
        frame: Any,
        response_id: str,
        response_generation: int,
    ) -> bool:
        """Bind one source PCM frame to its exact socket, wake, and response."""
        grant = self._assistant_output_grant
        context = (response_id, response_generation)
        if (
            grant is None
            or context != (grant.response_id, grant.response_generation)
            or not self._assistant_output_grant_is_current(grant)
        ):
            return False
        register_source = getattr(
            self.transport,
            "register_output_audio_source",
            None,
        )
        if register_source is None:
            return False
        return register_source(frame, context, grant.websocket) is True

    def _physical_wake_is_current(self) -> bool:
        return (
            self._device_wake_generation > 0
            and self._physical_wake_deadline > time.monotonic()
        )

    def _silent_close_is_current(self) -> bool:
        context = self._silent_close_context
        return (
            context is not None
            and tuple(self._websockets) == (context.websocket,)
            and self._active_session_nonce == context.session_nonce
            and self._device_wake_generation == context.wake_generation
        )

    def _retire_assistant_output_grant(self) -> Optional[_AssistantOutputGrant]:
        grant = self._assistant_output_grant
        self._assistant_output_grant = None
        self._assistant_output_authority_epoch += 1
        retire_generation = getattr(
            self.transport,
            "retire_output_audio_generation",
            None,
        )
        if retire_generation is not None:
            context = (
                (grant.response_id, grant.response_generation)
                if grant is not None
                else None
            )
            retire_generation(context)
        return grant

    def revoke_assistant_output(self) -> None:
        """Synchronously revoke queued and in-flight PCM during recovery."""
        self._retire_assistant_output_grant()

    async def _settle_retired_assistant_output(
        self,
        grant: Optional[_AssistantOutputGrant],
    ) -> bool:
        settle_generation = getattr(
            self.transport,
            "settle_output_audio_generation",
            None,
        )
        if settle_generation is None:
            return True
        context = (
            (grant.response_id, grant.response_generation)
            if grant is not None
            else None
        )
        owner_marker = object()
        transport_owner = getattr(
            self.transport,
            "admitted_websocket",
            owner_marker,
        )
        transport_owner_lost = (
            grant is not None
            and transport_owner is not owner_marker
            and transport_owner is not grant.websocket
        )
        settlement_failed = False
        settlement_cancelled = None
        if transport_owner_lost:
            settled = False
        else:
            try:
                settled = await settle_generation(context)
            except asyncio.CancelledError as error:
                settled = False
                settlement_cancelled = error
            except Exception as error:
                logger.warning(
                    "Assistant output settlement failed closed: %s",
                    type(error).__name__,
                )
                settled = False
                settlement_failed = True
        if grant is None:
            if settlement_cancelled is not None:
                raise settlement_cancelled
            return settled is not False
        if settled is not False and not transport_owner_lost:
            return True
        owner_was_current = (
            tuple(self._websockets) == (grant.websocket,)
            and self._active_session_nonce == grant.session_nonce
        )
        self._detach_unsettled_output_owner(grant)
        if owner_was_current and (settlement_failed or settlement_cancelled is not None):
            closed = await self._retire_transport_client(grant.websocket)
            self._mark_socket_retired(grant.websocket)
            if closed:
                self._notify_socket_retired(grant.websocket)
            else:
                self._remember_uncertain_socket(grant.websocket)
        if settlement_cancelled is not None:
            raise settlement_cancelled
        return False

    def _detach_unsettled_output_owner(self, grant: _AssistantOutputGrant) -> None:
        if (
            tuple(self._websockets) == (grant.websocket,)
            and self._active_session_nonce == grant.session_nonce
        ):
            self._set_serializer_audio_admitted(False)
            self._websockets.discard(grant.websocket)
            self._active_session_nonce = None
            self._device_audio_generation = None
            self._user_turn_non_close_generation = None
            self._request_follow_up_budget_spent = True
            self._request_follow_up_budget_tool_call_id = None
            self._request_follow_up_answer_grant = None
            self._silent_close_context = None
            self._physical_wake_deadline = 0.0
            self._mark_socket_retired(grant.websocket)

    async def _cancel_retired_assistant_output(
        self,
        grant: Optional[_AssistantOutputGrant],
    ) -> None:
        if grant is None or self._cancel_assistant_output_callback is None:
            return
        try:
            await asyncio.wait_for(
                self._cancel_assistant_output_callback(
                    grant.response_id,
                    grant.response_generation,
                ),
                timeout=self.ASSISTANT_CANCEL_TIMEOUT_S,
            )
        except Exception as error:
            logger.warning("Assistant response cancellation failed: %r", error)
    
    def build_pipeline(
        self,
        transport: WebsocketServerTransport,
        openai_service: OpenAIRealtimeLLMService,
        client_id: str,
        activity_callback: Optional[Callable[[], None]] = None
    ) -> tuple[Pipeline, PipelineRunner, PipelineTask]:
        """
        Build pipeline for a WebSocket transport connection.
        
        Args:
            transport: The WebSocket transport instance
            openai_service: The OpenAI service instance
            client_id: Unique identifier for the client device
            activity_callback: Optional callback for session activity tracking
            
        Returns:
            Tuple of (Pipeline, PipelineRunner, PipelineTask)
        """
        logger.info(f"🔗 Building pipeline for client: {client_id}")
        
        if openai_service is None:
            raise RuntimeError("OpenAI service must be created before building pipeline")
        
        logger.info(f"🔗 Building pipeline with WebSocket transport and OpenAI service: {type(openai_service).__name__}")
        
        # Create activity trackers
        input_activity_tracker = SessionActivityTracker(
            activity_callback=activity_callback or (lambda: None)
        )
        output_activity_tracker = SessionActivityTracker(
            activity_callback=activity_callback or (lambda: None)
        )
        
        # Create context aggregator with cached context if available
        context_aggregator = None
        context_initializer = None
        if self.session_manager:
            context_aggregator = self.session_manager.create_context_aggregator(client_id)
            context_initializer = self.session_manager.create_context_initializer(client_id, context_aggregator)
            bind_aggregator = getattr(
                openai_service,
                "bind_context_aggregator",
                None,
            )
            if bind_aggregator is not None:
                bind_aggregator(context_aggregator)
        
        # Build pipeline components. InputResampler runs FIRST (right after the
        # transport) so every later stage — VAD, context aggregator, OpenAI
        # service — sees correctly-rated 24 kHz audio instead of the device's
        # raw 16 kHz (which OpenAI would otherwise read 1.5x too fast).
        # Built early so ConnectionRecovery can route its unstick/reconnect
        # idle through PhaseEmitter.force_idle() (consistent phase state +
        # racing-`thinking` suppression); it is APPENDED near the end of the
        # pipeline below, before transport.output().
        phase_emitter = PhaseEmitter(
            send_phase=self.broadcast_phase,
            before_idle=self._before_reply_idle,
            before_forced_idle=self._cancel_request_follow_up_before_forced_idle,
            on_bot_started=self.note_assistant_playback_started,
            capture_idle_context=self.capture_reply_finalizer_context,
            capture_phase_context=self.capture_phase_authorization_context,
            capture_terminal_idle_context=(
                self.capture_terminal_idle_phase_authorization_context
            ),
        )

        set_follow_up_handlers = getattr(
            openai_service,
            "set_request_follow_up_event_handlers",
            None,
        )
        if set_follow_up_handlers is not None:
            set_follow_up_handlers(
                on_response_created=self.bind_request_follow_up_response,
                on_response_audio=self.note_request_follow_up_response_audio,
                on_response_done=self.note_request_follow_up_response_done,
                on_response_failed=self.note_request_follow_up_response_failed,
                on_continuation_arm=self.arm_request_follow_up_continuation,
                on_continuation_failed=self.fail_request_follow_up_continuation,
                on_question_output_authorized=(
                    self.request_follow_up_question_output_is_current
                ),
            )
        set_output_handlers = getattr(
            openai_service,
            "set_assistant_output_event_handlers",
            None,
        )
        if set_output_handlers is not None:
            set_output_handlers(
                on_response_created=self.bind_assistant_output_response,
                on_audio_frame=self.register_assistant_output_frame,
                on_before_tool_continuation=self.finish_assistant_output_response,
                on_output_revoked=self.revoke_assistant_output,
            )
        set_spoken_close_authorizer = getattr(
            openai_service,
            "set_spoken_close_response_authorizer",
            None,
        )
        if set_spoken_close_authorizer is not None:
            set_spoken_close_authorizer(
                self.silent_close_requires_spoken_response
            )
        self._cancel_assistant_output_callback = getattr(
            openai_service,
            "cancel_assistant_output_response",
            None,
        )

        connection_recovery = ConnectionRecovery(
            openai_service=openai_service,
            emit_idle=self.broadcast_phase,
            phase_emitter=phase_emitter,
            on_recovery_started=self._on_connection_recovery_started,
        )
        self._connection_recovery = connection_recovery

        pipeline_components = [
            transport.input(),
            # Watch for OpenAI connection-death ErrorFrames (they travel upstream
            # to the task source, so place this upstream of the service) and
            # reconnect in place. Without it a 1011/1001 drop bricks the session.
            connection_recovery,
            InputResampler(out_rate=PIPELINE_SAMPLE_RATE),
            input_activity_tracker,
        ]
        
        # Add input audio recorder to capture ONLY InputAudioRawFrame
        input_recorder = self.audio_recording_service.get_input_recorder() if self.audio_recording_service else None
        if input_recorder:
            pipeline_components.append(input_recorder)
        
        # The user aggregator consumes private transcription frames upstream.
        # Only assistant reply text is logged, by the downstream tap.
        if context_aggregator:
            context_components = [
                context_aggregator.user(),
                openai_service,
                TranscriptLogger(capture="assistant"),
                context_aggregator.assistant(),
            ]
            pipeline_components.extend(context_components)
        else:
            pipeline_components.extend([
                TranscriptLogger(capture="user"),
                openai_service,
                TranscriptLogger(capture="assistant"),
            ])

        pipeline_components.append(output_activity_tracker)

        # Emit va_client phase messages (listening/thinking/replying/idle) to
        # the device, derived from Pipecat speaking frames as they pass
        # downstream. Placed before transport.output() so it sees both the
        # user (UserStarted/Stopped) and bot (BotStarted/Stopped) frames.
        # (Constructed above, before ConnectionRecovery.)
        pipeline_components.append(phase_emitter)

        # Add output audio recorder to capture ONLY OutputAudioRawFrame
        output_recorder = self.audio_recording_service.get_output_recorder() if self.audio_recording_service else None
        if output_recorder:
            pipeline_components.append(output_recorder)

        pipeline_components.append(transport.output())
        
        # Add context initializer if we have cached messages
        if context_initializer:
            pipeline_components.append(context_initializer)
        
        pipeline = Pipeline(pipeline_components)
        logger.info("✅ Pipeline created for WebSocket connection")
        
        # Audio recording is handled by AudioFrameRecorder processors in the pipeline
        if self.audio_recording_service:
            logger.info("🎙️ Audio recording enabled - will record input and output audio")
        
        # Create the runner and task, but leave execution to Application.run so
        # there is exactly one authoritative owner of the pipeline lifecycle.
        # Disable idle timeout - server should always stay ready for connections.
        runner = PipelineRunner()
        task = PipelineTask(pipeline, idle_timeout_secs=None, cancel_on_idle_timeout=False)

        logger.info("✅ Pipeline initialized successfully")

        # Wire the device "stop" interrupt. The serializer calls this when it
        # sees {"type":"interrupt"} from the device.
        #
        # The DEVICE stops playback AUTHORITATIVELY: on "stop" its firmware
        # flushes the PSRAM queue and drops all further incoming TTS
        # (suppress_incoming_audio_) until the next turn boundary. So the backend
        # does NOT need to clear its own output here — the user already hears
        # silence. The backend's only job is to stop OpenAI generating MORE
        # tokens: a plain response.cancel. It is sent unconditionally so a
        # response.create that is in flight cannot escape cancellation; the
        # benign response_cancel_not_active race is filtered by the service.
        #
        # We deliberately do NOT queue an InterruptionTaskFrame anymore. It made
        # pipecat run _handle_interruption → _truncate_current_audio_response(),
        # which tells OpenAI to truncate the assistant audio at the *playback*
        # position. But OpenAI bursts the reply faster than real-time, so that
        # position overshoots the audio that actually exists and OpenAI rejects
        # the truncate with invalid_request_error ("Audio content of N ms is
        # already shorter than M ms"). That error left the realtime session in a
        # broken state where the user's VERY NEXT turn got NO response — the
        # recurring "say stop, then immediately ask again → silence" bug. Since
        # the device already silenced playback, dropping the truncate costs us
        # nothing and keeps the next turn alive. (The backend still drains its
        # already-buffered output to the device, which the device discards —
        # minor wasted bandwidth, tracked as roadmap #3; no extra tokens because
        # response.cancel stops further generation.)
        # FOLLOW-UP-WINDOW STOP (the "stop heard as a question" bug). During the
        # post-reply follow-up window the device mic is OPEN and streaming, so by
        # the time the device's local wake-word detects "stop" and sends us the
        # interrupt, the stop word's audio is ALREADY in OpenAI's input buffer.
        # Left alone, semantic VAD commits it as a user turn and the response
        # gate can make the model literally ANSWER the word "stop"
        # ("Ik hou me stil…"). The device's local detection must therefore be
        # authoritative on the cloud side too, in two layers:
        #   1) input_audio_buffer.clear discards the not-yet-committed stop-word
        #      audio (the device closed its own mic gate in the same instant),
        #      so in the common case no turn is created at all;
        #   2) if semantic VAD committed BEFORE our clear landed (tight race),
        #      a response can be created moments later anyway — so any assistant
        #      conversation item that appears within INTERRUPT_KILL_WINDOW_S of
        #      a device interrupt is cancelled on arrival (handler below). A
        #      legitimate next turn cannot fall inside that window: after a stop
        #      the mic is closed, and a fresh wake-word turn needs the chime +
        #      speech + VAD end-of-turn (> 2 s) before a response is created.
        _interrupt_kill_until = {"t": 0.0}
        INTERRUPT_KILL_WINDOW_S = 1.5
        # A device "stop" must cancel the NEXT assistant response too, not only
        # the one currently playing. After a stop, the only responses OpenAI can
        # still produce before the user speaks again are unwanted:
        #   - the cancelled reply's already-generated tail;
        #   - a slow tool's answer (web search ~2-4 s) the user stopped mid-run,
        #     created on the tool result OUTSIDE the 1.5 s time-window;
        #   - most common: OpenAI's STT hearing the user's spoken "stop" as a
        #     turn and the model REPLYING to it ("Okay, I'll stop"), which lands
        #     ~1.8 s later — just outside 1.5 s (observed 2026-06-14 22:51: the
        #     device flashed red but a fresh "I'll be quiet" reply played, so the
        #     user had to say stop twice).
        # The time-window alone misses the >1.5 s cases. This flag, armed on
        # EVERY device interrupt, makes _kill_racing_response cancel that one
        # next response regardless of timing. It is consumed when used and
        # cleared at the next genuine turn boundary (real speech via
        # on_real_speech, and {"type":"wake"}) — and a legitimate next turn needs
        # the user to actually speak — so it can never cancel a real turn.
        _kill_next_response = {"v": False}
        _dangling_response_pending = {"v": False}
        _dangling_response_generation = {"v": 0}
        DANGLING_RESPONSE_TIMEOUT_S = 12.0

        def _next_device_input_clear_generation() -> int:
            self._device_input_clear_generation = (
                -1
                if self._device_input_clear_generation in (0, -0x7FFFFFFF)
                else self._device_input_clear_generation - 1
            )
            return self._device_input_clear_generation

        async def _clear_device_input(
            reason: str,
            generation: Optional[int] = None,
        ) -> None:
            clear_input = getattr(
                openai_service,
                "clear_input_audio_buffer_authoritatively",
                None,
            )
            if clear_input is None:
                raise RuntimeError("OpenAI service has no authoritative input clear")
            if generation is None:
                generation = _next_device_input_clear_generation()
            await clear_input(generation)
            logger.info("Device input clear settled at OpenAI (%s)", reason)

        self._clear_device_input = _clear_device_input

        async def _on_device_interrupt():
            self.invalidate_request_follow_up_turn(send_cancel=False)
            _dangling_response_pending["v"] = False
            _dangling_response_generation["v"] += 1
            _interrupt_kill_until["t"] = time.monotonic() + INTERRUPT_KILL_WINDOW_S
            # Arm the next-response kill on EVERY stop (see the flag comment):
            # the 1.5 s time-window alone misses responses that land later —
            # OpenAI replying to the spoken "stop", or a slow tool's answer.
            _kill_next_response["v"] = True
            # Suppression must become active before either network await below;
            # function arguments are dispatched on a separate Pipecat task.
            suppress_tools = getattr(
                openai_service,
                "suppress_tools_at_interrupt",
                None,
            )
            interrupt_generation = 0
            if suppress_tools is not None:
                interrupt_generation = await suppress_tools()
            clear_error = None
            try:
                await self._clear_device_input_or_retire(
                    "device interrupt",
                    interrupt_generation,
                )
                logger.info(
                    "🛑 device interrupt → input_audio_buffer.clear acknowledged "
                    "(drop in-flight user audio)"
                )
            except Exception as e:
                clear_error = e
                logger.warning(
                    "🛑 device interrupt input clear failed closed: %r",
                    e,
                )
            try:
                cancel_event = openai_rt_events.ResponseCancelEvent()
                note_cancel = getattr(
                    openai_service,
                    "note_interrupt_cancel_event",
                    None,
                )
                if note_cancel is not None:
                    note_cancel(cancel_event.event_id, interrupt_generation)
                await openai_service.send_client_event(cancel_event)
                logger.info("🛑 device interrupt → response.cancel sent")
            except Exception as e:
                logger.info(f"🛑 device interrupt → response.cancel no-op ({e!r})")
                fail_cancel = getattr(
                    openai_service,
                    "fail_interrupt_cancel",
                    None,
                )
                if fail_cancel is not None:
                    await fail_cancel(interrupt_generation, e)
            if clear_error is not None:
                raise clear_error

        @openai_service.event_handler("on_conversation_item_created")
        async def _kill_racing_response(service, item_id, item):
            # Function-call-only responses have no assistant role. They must be
            # killed too or a stopped response can still mutate the home.
            is_assistant = getattr(item, "role", None) == "assistant"
            is_function_call = getattr(item, "type", None) == "function_call"
            if not is_assistant and not is_function_call:
                return
            within_window = time.monotonic() < _interrupt_kill_until["t"]
            kill_armed = (
                _kill_next_response["v"]
                or _dangling_response_pending["v"]
            )
            if not within_window and not kill_armed:
                return
            mark_interrupted = getattr(
                openai_service,
                "mark_interrupted_response",
                None,
            )
            if mark_interrupted is not None:
                await mark_interrupted()
            _dangling_response_pending["v"] = False
            _dangling_response_generation["v"] += 1
            # Consume the flag: this assistant item is the unwanted response the
            # user's stop pre-empted — a stop-acknowledgement ("Okay, I'll stop"),
            # a stopped tool's answer, or the cancelled reply's tail.
            if not _dangling_response_pending["v"]:
                _kill_next_response["v"] = False
            try:
                cancel_event = openai_rt_events.ResponseCancelEvent()
                note_cancel = getattr(
                    openai_service,
                    "note_interrupt_cancel_event",
                    None,
                )
                if note_cancel is not None:
                    note_cancel(cancel_event.event_id)
                await openai_service.send_client_event(cancel_event)
                logger.info(
                    "🛑 response raced in right after a device interrupt → "
                    "response.cancel (post-stop)"
                )
            except Exception as e:
                logger.info(f"🛑 post-interrupt racing-response cancel no-op ({e!r})")
            if is_function_call:
                suppress_call = getattr(
                    openai_service,
                    "suppress_function_call_after_interrupt",
                    None,
                )
                call_id = getattr(item, "call_id", None)
                if suppress_call is not None and call_id:
                    await suppress_call(call_id)

        async def _on_device_session_start():
            # va_client sends {"type":"start"} once per WebSocket CONNECTION
            # (on connect) — NOT per wake. A reconnect mid-utterance (wifi
            # blip, backend restart with session reuse) can leave half an
            # utterance in OpenAI's input buffer; start every (re)connection
            # with a clean one. The per-WAKE/follow-up stale-buffer case is
            # covered by the device's {"type":"flush"} on follow-up timeout.
            try:
                await self._clear_device_input_or_retire("device reconnect")
                logger.info(
                    "🎬 device (re)connected → input_audio_buffer.clear "
                    "acknowledged (clean start)"
                )
            except Exception as e:
                logger.warning("🎬 connect-time input clear failed closed: %r", e)
                raise

        async def _on_device_mic_flush():
            self.invalidate_request_follow_up_turn(send_cancel=False)
            # The device sends {"type":"flush"} when a follow-up window times out
            # mid-stream. Drop any uncommitted partial utterance NOW, at the
            # cut-off, so a later wake can't "complete" it into a stale answer.
            # This replaced the reactive clear-on-mic-resume, which fired on
            # every wake and disturbed the server VAD → spurious garbage commits.
            # Also a turn boundary for the dangling-VAD guard: the follow-up
            # closed without speech, so any later server-VAD stop is dangling.
            phase_emitter.note_wake()
            try:
                await self._clear_device_input_or_retire("follow-up timeout")
                logger.info(
                    "🧽 follow-up cut-off → input_audio_buffer.clear acknowledged "
                    "(drop partial utterance)"
                )
            except Exception as e:
                logger.warning("🧽 mic-flush input clear failed closed: %r", e)
                raise

        WEDGE_TIMEOUT_S = 12.0

        async def _wedge_check(wake_mono: float):
            # If the server VAD shows no life this long after a wake, the
            # OpenAI socket is presumed half-open (dead) → reconnect in place.
            # False positive = a silent wake (user said nothing): the reconnect
            # is 3s during idle, harmless. Cooldown lives in force_reconnect.
            await asyncio.sleep(WEDGE_TIMEOUT_S)
            if getattr(phase_emitter, "last_vad_mono", 0.0) < wake_mono:
                logger.warning(
                    "🧟 no server VAD activity %.0fs after wake — presuming a "
                    "half-open OpenAI socket, reconnecting", WEDGE_TIMEOUT_S)
                await connection_recovery.force_reconnect("wedge: silent after wake")

        async def _on_device_wake():
            self.cancel_graceful_close_request()
            if await connection_recovery.reject_wake_while_recovering():
                self._device_audio_generation = None
                self.invalidate_request_follow_up_turn()
                return
            self._track_wedge_task(_wedge_check(time.monotonic()))
            # va_client sends {"type":"wake"} on every wake (start_session). Mark
            # the turn boundary for the dangling-VAD guard (A): until the user
            # actually speaks, a server-VAD end-of-turn is a stale pre-wake
            # segment closing late → suppress its thinking + cancel its garbage
            # response (handled in PhaseEmitter via the kill-window callbacks).
            phase_emitter.note_wake()
            # Preserve a dangling-response kill until its stale response is
            # consumed; ordinary post-stop kills end at this turn boundary.
            if not _dangling_response_pending["v"]:
                _kill_next_response["v"] = False

        # Wire the dangling-VAD guard's kill-window into the PhaseEmitter. It
        # reuses the SAME _interrupt_kill_until + _kill_racing_response machinery
        # as the device stop: on a dangling stop, arm it so the auto-created
        # garbage response is cancelled; on a real UserStartedSpeaking, clear it
        # so a genuine new turn's response is never cancelled.
        def _clear_kill_window():
            # Managed turns have an explicit response.create boundary below;
            # retain dangling protection until that boundary so a delayed stale
            # function call cannot slip through after speech begins.
            if not _dangling_response_pending["v"]:
                _dangling_response_generation["v"] += 1
                _dangling_response_pending["v"] = False
                _interrupt_kill_until["t"] = 0.0
                _kill_next_response["v"] = False
            self.note_request_follow_up_turn_boundary()
            self.cancel_graceful_close_request()

        def _arm_dangling_response_kill():
            _interrupt_kill_until["t"] = (
                time.monotonic() + INTERRUPT_KILL_WINDOW_S
            )
            _kill_next_response["v"] = True
            _dangling_response_pending["v"] = True
            _dangling_response_generation["v"] += 1
            generation = _dangling_response_generation["v"]

            async def expire_dangling_response_kill():
                await asyncio.sleep(DANGLING_RESPONSE_TIMEOUT_S)
                if generation != _dangling_response_generation["v"]:
                    return
                _dangling_response_pending["v"] = False
                _kill_next_response["v"] = False
                _interrupt_kill_until["t"] = 0.0

            self._track_wedge_task(expire_dangling_response_kill())
            self._track_wedge_task(
                connection_recovery.force_reconnect(
                    "dangling server VAD boundary",
                    bypass_cooldown=True,
                )
            )

        def _clear_dangling_response_kill():
            _dangling_response_generation["v"] += 1
            _dangling_response_pending["v"] = False
            _kill_next_response["v"] = False
            _interrupt_kill_until["t"] = 0.0

        set_recovery_callback = getattr(
            connection_recovery,
            "set_recovery_complete_callback",
            None,
        )
        if set_recovery_callback is not None:
            def _clear_recovery_guards():
                _clear_dangling_response_kill()
                self._connection_recovery_active = False
                self._input_clear_recovery_ready = True
                if self._input_clear_settled.is_set():
                    self._input_clear_fail_closed = False

            set_recovery_callback(_clear_recovery_guards)

        phase_emitter.set_kill_window_handlers(
            on_dangling=_arm_dangling_response_kill,
            on_real_speech=_clear_kill_window,
        )

        if self._serializer is not None:
            self._serializer.set_interrupt_handler(_on_device_interrupt)
            self._serializer.set_session_start_handler(_on_device_session_start)
            self._serializer.set_mic_flush_handler(_on_device_mic_flush)
            self._serializer.set_wake_handler(_on_device_wake)

            # Speaker context v1 (fork): per-wake voice-type verdict → injected
            # as a system conversation item. Out-of-band w.r.t. the audio path;
            # it lands ~2.5 s after the wake, so the FIRST reply of a turn may
            # not have it yet — follow-ups and later turns do. Gating of
            # speaker-restricted tools does NOT depend on this injection (see
            # SafeRealtimeLLMService.register_function in main.py).
            speaker_probe = self.speaker_probe
            if speaker_probe is not None and speaker_probe.enabled:
                from .speaker_context import verdict_text

                async def _on_speaker_verdict(label, name, f0):
                    try:
                        await openai_service.send_client_event(
                            openai_rt_events.ConversationItemCreateEvent(
                                item=openai_rt_events.ConversationItem(
                                    type="message",
                                    role="system",
                                    content=[openai_rt_events.ItemContent(
                                        type="input_text",
                                        text=verdict_text(speaker_probe, label, name, f0),
                                    )],
                                )
                            )
                        )
                    except Exception as e:
                        logger.warning(f"⚠️ speaker verdict injection failed: {e!r}")

                speaker_probe.on_verdict = _on_speaker_verdict
                self._serializer.set_speaker_probe(speaker_probe)

            # Button-cancel shortly after a wake = user flagging a false
            # trigger: label the latest probe capture like mark_false_wake.
            async def _on_button_cancel():
                try:
                    import os
                    d = "/share/voice-probes"
                    files = sorted(f for f in os.listdir(d)
                                   if f.startswith("probe_") and f.endswith(".wav"))
                    if files:
                        latest = files[-1]
                        os.rename(os.path.join(d, latest),
                                  os.path.join(d, latest.replace("probe_", "falsewake_", 1)))
                        logger.info(f"🏷️ button-flagged false wake: {latest}")
                        from .ha_sensors import PUBLISHER
                        await PUBLISHER.false_wake()
                except Exception as e:
                    logger.warning(f"⚠️ button false-wake flag failed: {e!r}")
            self._serializer.set_button_cancel_handler(_on_button_cancel)

            async def _on_first_audio():
                sockets = tuple(self._websockets)
                if (
                    len(sockets) == 1
                    and sockets[0] is self._wake_session_socket
                    and self._active_session_nonce == self._wake_session_nonce
                    and self._device_wake_generation > 0
                ):
                    await self._send_json(
                        sockets[0],
                        {
                            "type": "ack",
                            "session_nonce": self._active_session_nonce,
                            "wake_generation": self._device_wake_generation,
                        },
                    )
            self._serializer.set_first_audio_handler(_on_first_audio)

        return pipeline, runner, task
    
    def extract_client_id(self, websocket) -> str:
        """
        Extract client ID from websocket connection.
        
        Args:
            websocket: WebSocket connection object
            
        Returns:
            Client ID string
        """
        client_ip = None
        if hasattr(websocket, 'client') and websocket.client:
            client_ip = websocket.client.host
        elif hasattr(websocket, 'remote_address'):
            client_ip = str(websocket.remote_address[0]) if websocket.remote_address else None
        
        if not client_ip:
            client_ip = f"unknown_{uuid.uuid4().hex[:8]}"
            logger.warning("⚠️ Could not extract client IP, using generated ID")

        return client_ip

    async def _send_json(self, websocket, obj: dict) -> None:
        """Send a JSON object to one device as a TEXT websocket frame.

        Compact separators keep controls below the firmware's bounded parser
        limit. The final firmware parses fields structurally and then enforces
        the exact trusted or legacy shape.
        """
        try:
            await websocket.send(json.dumps(obj, separators=(",", ":")))
        except Exception as e:
            logger.warning(f"⚠️ Could not send {obj.get('type')} to device: {e!r}")

    def _hello_values(self) -> dict:
        return {
            "audio_out": "pcm",
            "follow_up_ms": self.follow_up_ms,
            "follow_up_open_delay_ms": self.follow_up_open_delay_ms,
            "wake_open_delay_ms": self.wake_open_delay_ms,
            "playback_prebuffer_ms": self.playback_prebuffer_ms,
        }

    def _request_follow_up_ready_timeout_s(self) -> float:
        """Bound PREPARE-to-READY by the final firmware's physical worst case."""
        ring_drain_s = (
            self.FIRMWARE_AUDIO_RING_BYTES
            / self.FIRMWARE_OUTPUT_BYTES_PER_SECOND
        )
        negotiated_callback_s = (
            self.FIRMWARE_FOLLOW_UP_CHIME_WAIT_TIMEOUT_S
            + self.follow_up_open_delay_ms / 1000.0
            + 1.0
        )
        callback_s = max(
            self.FIRMWARE_FOLLOW_UP_READY_CALLBACK_TIMEOUT_S,
            negotiated_callback_s,
        )
        return float(
            math.ceil(
                ring_drain_s
                + self.FIRMWARE_PLAYBACK_PREBUFFER_MAX_S
                + self.FIRMWARE_SPEAKER_DRAIN_TIMEOUT_S
                + self.FIRMWARE_MIC_SEND_BARRIER_TIMEOUT_S
                + callback_s
                + self.FOLLOW_UP_PROTOCOL_MARGIN_S
            )
        )

    @staticmethod
    def _new_protocol_id(
        issued: set[int],
        *,
        forbidden: frozenset[int] = frozenset(),
    ) -> int:
        """Return an unpredictable nonzero 31-bit value never issued this run."""
        if len(issued) >= WebSocketHandler.PROTOCOL_HISTORY_LIMIT:
            raise RuntimeError("Protocol identifier history is full")
        for _ in range(128):
            candidate = secrets.randbits(31)
            if (
                candidate != 0
                and candidate not in issued
                and candidate not in forbidden
            ):
                issued.add(candidate)
                return candidate
        raise RuntimeError("Could not allocate a unique protocol identifier")

    def _set_serializer_audio_admitted(self, admitted: bool) -> None:
        setter = getattr(self._serializer, "set_audio_admitted", None)
        if setter is not None:
            setter(admitted)

    async def _start_hello(
        self,
        websocket,
        client_id: str,
        on_admitted: Callable[[str], Awaitable[None]],
    ) -> bool:
        """Send a nonce hello, then return so Pipecat can receive its ACK."""
        self._set_serializer_audio_admitted(False)
        nonce = self._new_protocol_id(
            self._issued_hello_nonces,
            forbidden=frozenset(self._issued_request_follow_up_tokens),
        )
        values = self._hello_values()
        transaction = _HelloTransaction(
            websocket=websocket,
            client_id=client_id,
            nonce=nonce,
            values=values,
            on_admitted=on_admitted,
        )
        self._hello_transaction = transaction
        hello = {"type": "hello", "nonce": nonce, **values}
        try:
            await asyncio.wait_for(
                websocket.send(json.dumps(hello, separators=(",", ":"))),
                timeout=self.HELLO_SEND_TIMEOUT_S,
            )
        except Exception as error:
            logger.warning("⚠️ Voice PE hello failed; rejecting socket: %r", error)
            if self._hello_transaction is transaction:
                self._clear_hello_transaction()
            await self._reject_transport_candidate(websocket)
            return False

        if self._hello_transaction is not transaction:
            return False
        timeout_task = asyncio.create_task(self._expire_hello(transaction))
        self._hello_timeout_task = timeout_task
        return True

    async def _expire_hello(self, transaction: _HelloTransaction) -> None:
        try:
            await asyncio.sleep(self.HELLO_ACK_TIMEOUT_S)
        except asyncio.CancelledError:
            return
        async with self._socket_transition_lock:
            if self._hello_transaction is not transaction:
                return
            logger.warning(
                "Voice PE did not acknowledge hello; rejecting socket",
            )
            self._clear_hello_transaction()
            await self._reject_transport_candidate(transaction.websocket)

    async def _handle_hello_ack(self, data: dict, websocket: Any) -> None:
        session_start: Optional[Callable[[], Awaitable[None]]] = None
        admitted_session_nonce = None
        async with self._socket_transition_lock:
            transaction = self._hello_transaction
            if transaction is None:
                logger.warning("Ignoring hello ACK with no pending transaction")
                return
            if websocket is not transaction.websocket:
                logger.warning("Ignoring hello ACK from a non-candidate socket")
                return
            if (
                not has_exact_fields(
                    data,
                    TRUSTED_DEVICE_TO_BACKEND_FIELDS["hello_ack"],
                )
                or type(data.get("nonce")) is not int
                or data.get("nonce") != transaction.nonce
                or type(data.get("accepted")) is not bool
            ):
                logger.warning("Ignoring stale or malformed hello ACK")
                return

            values_match = all(
                data.get(key) == value and type(data.get(key)) is type(value)
                for key, value in transaction.values.items()
            )
            if data.get("accepted") is not True or not values_match:
                logger.warning(
                    "Voice PE rejected or misapplied hello; rejecting socket",
                )
                self._clear_hello_transaction()
                await self._reject_transport_candidate(transaction.websocket)
                return
            if self._input_clear_fail_closed:
                logger.warning(
                    "Voice PE admission blocked until OpenAI input-clear recovery"
                )
                self._clear_hello_transaction()
                await self._reject_transport_candidate(transaction.websocket)
                return

            admit_client = getattr(self.transport, "admit_client", None)
            if admit_client is not None and not await admit_client(transaction.websocket):
                logger.warning("Voice PE hello owner promotion failed")
                self._clear_hello_transaction()
                await self._reject_transport_candidate(transaction.websocket)
                return
            self._websockets = {transaction.websocket}
            self._active_session_nonce = transaction.nonce
            self._device_wake_generation = 0
            self._device_audio_generation = None
            self._wake_session_socket = None
            self._wake_session_nonce = None
            self._physical_wake_deadline = 0.0
            self._request_follow_up_answer_grant = None
            self._open_follow_up_phase_grant = None
            self._silent_close_context = None
            self._request_follow_up_budget_spent = True
            self._request_follow_up_budget_tool_call_id = None
            self._issued_request_follow_up_tokens.clear()
            self._seen_ready_nonces.clear()
            self._follow_up_fail_closed = False
            # Keep every input and output path closed until OpenAI confirms the
            # reconnect clear below. The physical socket alone is not admission.
            self._input_clear_fail_closed = True
            self._set_serializer_audio_admitted(False)
            try:
                await transaction.on_admitted(transaction.client_id)
            except Exception:
                self._clear_hello_transaction()
                self._set_serializer_audio_admitted(False)
                self._websockets.clear()
                self._active_session_nonce = None
                self._physical_wake_deadline = 0.0
                self._request_follow_up_answer_grant = None
                self._open_follow_up_phase_grant = None
                self._silent_close_context = None
                await self._retire_transport_client(transaction.websocket)
                raise
            self._clear_hello_transaction()
            session_start = getattr(self._serializer, "_on_session_start", None)
            if session_start is None:
                async def _fallback_session_start() -> None:
                    await self._clear_device_input_or_retire("device reconnect")

                session_start = _fallback_session_start
            admitted_session_nonce = transaction.nonce

        try:
            if session_start is None:
                raise RuntimeError("authoritative reconnect input clear is unavailable")
            await session_start()
        except Exception as error:
            logger.warning(
                "Voice PE reconnect clear failed; socket remains retired: %r",
                error,
            )
            if (
                websocket in self._websockets
                and self._active_session_nonce == admitted_session_nonce
            ):
                self._input_clear_fail_closed = True
                self._set_serializer_audio_admitted(False)
                await self._retire_bound_socket(
                    websocket,
                    admitted_session_nonce,
                )
            return
        async with self._socket_transition_lock:
            self._input_clear_fail_closed = False
            if (
                websocket in self._websockets
                and self._active_session_nonce == admitted_session_nonce
            ):
                self._set_serializer_audio_admitted(True)
        logger.info("Voice PE hello acknowledged and socket admitted")

    async def _reject_transport_candidate(self, websocket: Any) -> bool:
        reject = getattr(self.transport, "reject_candidate", None)
        if reject is not None:
            closed = bool(await reject(websocket))
            if not closed:
                self._remember_uncertain_socket(websocket)
            return closed
        return await self._close_websocket(websocket)

    async def _retire_transport_client(self, websocket: Any) -> bool:
        retire = getattr(self.transport, "retire_client", None)
        if retire is not None:
            return bool(await retire(websocket))
        return await self._close_websocket(websocket)

    def _clear_hello_transaction(self) -> None:
        self._hello_transaction = None
        timeout_task = self._hello_timeout_task
        self._hello_timeout_task = None
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        if (
            timeout_task is not None
            and timeout_task is not current_task
            and not timeout_task.done()
        ):
            timeout_task.cancel()

    async def _close_websocket(self, websocket) -> bool:
        try:
            closed = await _close_socket(
                websocket,
                timeout_s=self.SOCKET_CLOSE_TIMEOUT_S,
            )
        except asyncio.CancelledError:
            self._remember_uncertain_socket(websocket)
            raise
        if closed:
            self._uncertain_retired_sockets.discard(websocket)
            return True
        self._remember_uncertain_socket(websocket)
        return False

    def _remember_uncertain_socket(self, websocket: Any) -> None:
        if websocket in self._uncertain_retired_sockets:
            return
        if len(self._uncertain_retired_sockets) >= self.MAX_UNCERTAIN_SOCKETS:
            logger.error("Voice PE uncertain-socket quarantine is full")
            return
        self._uncertain_retired_sockets.add(websocket)

    def _mark_socket_retired(self, websocket) -> None:
        reservation = self._request_follow_up_reservation
        if reservation is not None and reservation.websocket is websocket:
            reservation.cancel_requested = True
            reservation.socket_retired = True
            reservation.ack_event.set()
            reservation.commit_ack_event.set()
            reservation.cancel_ack_event.set()
            self._request_follow_up_epoch += 1
            self._request_follow_up_reservation = None
            self._cancel_request_follow_up_expiry_task()
        if websocket is self._wake_session_socket:
            self._device_audio_generation = None
            self._physical_wake_deadline = 0.0
            self._request_follow_up_answer_grant = None
            self._open_follow_up_phase_grant = None
            self._silent_close_context = None
        retired_cancellation_keys = []
        for key, pending in self._request_follow_up_cancellations.items():
            if pending.websocket is websocket:
                pending.socket_retired = True
                pending.ack_event.set()
                pending.commit_ack_event.set()
                pending.cancel_ack_event.set()
                retired_cancellation_keys.append(key)
        for key in retired_cancellation_keys:
            self._request_follow_up_cancellations.pop(key, None)

    def _notify_socket_retired(self, websocket) -> None:
        if self._on_client_disconnected_callback is None:
            return
        try:
            self._on_client_disconnected_callback(self.extract_client_id(websocket))
        except Exception as error:
            logger.warning("⚠️ Voice PE disconnect callback failed: %r", error)

    async def _retire_bound_socket(
        self,
        websocket,
        session_nonce: int,
        *,
        expected_wake_generation: Optional[int] = None,
    ) -> bool:
        """Unadmit and close the exact socket whose control state is ambiguous."""
        async with self._socket_transition_lock:
            if expected_wake_generation is not None and (
                tuple(self._websockets) != (websocket,)
                or self._active_session_nonce != session_nonce
                or self._device_wake_generation != expected_wake_generation
            ):
                return False
            retired_output = None
            if (
                websocket in self._websockets
                and self._active_session_nonce == session_nonce
            ):
                retired_output = self._retire_assistant_output_grant()
                self._set_serializer_audio_admitted(False)
                self._websockets.discard(websocket)
                self._active_session_nonce = None
                self._device_audio_generation = None
                self._user_turn_non_close_generation = None
                self._request_follow_up_budget_spent = True
                self._request_follow_up_budget_tool_call_id = None
                self._request_follow_up_answer_grant = None
                self._open_follow_up_phase_grant = None
                self._silent_close_context = None
                graceful_context = self._graceful_close_owner_context
                if (
                    graceful_context is not None
                    and graceful_context.websocket is websocket
                ):
                    self._clear_graceful_close_context(graceful_context)
                self._physical_wake_deadline = 0.0
            await self._settle_retired_assistant_output(retired_output)
            await self._cancel_retired_assistant_output(retired_output)
            closed = await self._retire_transport_client(websocket)
            self._mark_socket_retired(websocket)
            if closed:
                self._notify_socket_retired(websocket)
            else:
                self._remember_uncertain_socket(websocket)
            return closed

    async def _clear_device_input_or_retire(
        self,
        reason: str,
        generation: Optional[int] = None,
    ) -> None:
        """Settle one clear while every device path is closed, or retire it."""
        async with self._socket_transition_lock:
            sockets = tuple(self._websockets)
            session_nonce = self._active_session_nonce
            if len(sockets) != 1 or session_nonce is None:
                raise RuntimeError("No admitted Voice PE owns the input clear")
            websocket = sockets[0]
            clear_device_input = self._clear_device_input
            self._input_clear_fail_closed = True
            self._input_clear_settled.clear()
            self._input_clear_recovery_ready = False
        try:
            if clear_device_input is None:
                raise RuntimeError("authoritative input clear is unavailable")
            await clear_device_input(reason, generation)
        except (Exception, asyncio.CancelledError):
            await self._retire_bound_socket(websocket, session_nonce)
            self._input_clear_settled.set()
            if self._input_clear_recovery_ready:
                self._input_clear_fail_closed = False
            raise
        async with self._socket_transition_lock:
            if (
                websocket in self._websockets
                and self._active_session_nonce == session_nonce
            ):
                self._input_clear_fail_closed = False
                self._set_serializer_audio_admitted(True)
            elif not self._websockets:
                self._input_clear_fail_closed = False
            self._input_clear_recovery_ready = False
        self._input_clear_settled.set()

    async def _handle_device_control_message(
        self,
        data: dict,
        websocket: Any = None,
    ) -> bool:
        """Validate one physical-socket control before local side effects."""
        if websocket is None:
            websocket = current_message_websocket()
        message_type = data.get("type") if isinstance(data, dict) else None
        if message_type == "hello_ack":
            if websocket is None:
                logger.warning("Ignoring hello ACK without physical socket ownership")
                return False
            await self._handle_hello_ack(data, websocket)
            return False
        if not isinstance(message_type, str):
            logger.warning("Ignoring malformed Voice PE control")
            return False

        if (
            websocket is None
            or tuple(self._websockets) != (websocket,)
            or self._active_session_nonce is None
        ):
            logger.warning("Ignoring control from a non-admitted Voice PE socket")
            return False

        expected_fields = TRUSTED_DEVICE_TO_BACKEND_FIELDS.get(message_type)
        if expected_fields is None or not has_exact_fields(data, expected_fields):
            logger.warning("Ignoring unknown or malformed Voice PE control")
            return False

        if message_type == "request_follow_up_ack":
            self._handle_request_follow_up_ack(data)
            return False
        if message_type == "follow_up_ready":
            self._handle_follow_up_ready(data)
            return False
        if message_type == "commit_follow_up_ack":
            self._handle_commit_follow_up_ack(data)
            return False
        if message_type == "cancel_request_follow_up_ack":
            self._handle_cancel_request_follow_up_ack(data)
            return False
        if message_type == "suppress_followup_ack":
            self._handle_graceful_close_ack(data, websocket)
            return False
        session_nonce = data.get("session_nonce")
        wake_generation = data.get("wake_generation")
        reason = data.get("reason")
        if (
            type(session_nonce) is not int
            or session_nonce != self._active_session_nonce
            or type(wake_generation) is not int
            or wake_generation < 0
            or wake_generation > 0x7FFFFFFF
            or (
                "reason" in expected_fields
                and (
                    not isinstance(reason, str)
                    or not reason
                    or len(reason) > 64
                )
            )
        ):
            logger.warning("Ignoring malformed or stale Voice PE control")
            return False

        if (
            message_type
            in {
                "wake",
                "client_revoke",
                "interrupt",
                "flush",
                "button_cancel",
                "false_flag",
            }
            and self._input_clear_fail_closed
        ):
            await self._input_clear_settled.wait()

        if message_type == "wake":
            async with self._socket_transition_lock:
                if self._input_clear_fail_closed:
                    logger.warning(
                        "Rejecting Voice PE wake until input-clear recovery completes"
                    )
                    return False
                wake_generation_is_next = (
                    self._device_wake_generation == 0
                    or wake_generation
                    == (
                        1
                        if self._device_wake_generation == 0x7FFFFFFF
                        else self._device_wake_generation + 1
                    )
                )
                if (
                    tuple(self._websockets) != (websocket,)
                    or session_nonce != self._active_session_nonce
                    or wake_generation == 0
                    or not wake_generation_is_next
                ):
                    logger.warning("Ignoring replayed or out-of-order Voice PE wake")
                    return False
                retired_output = self._retire_assistant_output_grant()
                await self._settle_retired_assistant_output(retired_output)
                if not self.note_device_wake(wake_generation):
                    logger.warning("Ignoring replayed or out-of-order Voice PE wake")
                    return False
                return True

        if wake_generation != self._device_wake_generation:
            logger.warning("Ignoring stale Voice PE wake-generation control")
            return False

        if message_type == "client_revoke":
            retired_output = None
            async with self._socket_transition_lock:
                if (
                    tuple(self._websockets) != (websocket,)
                    or session_nonce != self._active_session_nonce
                    or wake_generation != self._device_wake_generation
                ):
                    return False
                self._device_audio_generation = None
                self._open_follow_up_phase_grant = None
                retired_output = self._retire_assistant_output_grant()
                self.invalidate_request_follow_up_turn(send_cancel=False)
                self.cancel_graceful_close_request()
                await self._settle_retired_assistant_output(retired_output)
            await self._cancel_retired_assistant_output(retired_output)
            try:
                await self._clear_device_input_or_retire(str(reason))
            except Exception as error:
                logger.warning(
                    "Device input clear failed closed; socket retired: %r",
                    error,
                )
            return False
        if message_type in {
            "interrupt",
            "flush",
            "button_cancel",
            "false_flag",
        }:
            async with self._socket_transition_lock:
                if (
                    tuple(self._websockets) != (websocket,)
                    or session_nonce != self._active_session_nonce
                    or wake_generation != self._device_wake_generation
                ):
                    return False
                self._device_audio_generation = None
                self._open_follow_up_phase_grant = None
                retired_output = self._retire_assistant_output_grant()
                self.invalidate_request_follow_up_turn(send_cancel=False)
                await self._settle_retired_assistant_output(retired_output)
                return True
        return False

    async def broadcast_json(self, obj: dict) -> None:
        """Send a JSON object to every connected device as a TEXT frame."""
        for ws in list(self._websockets):
            await self._send_json(ws, obj)

    async def _broadcast_json_strict(self, obj: dict) -> None:
        """Send a control frame or fail when no device accepts the write."""
        websockets = list(self._websockets)
        if not websockets:
            raise RuntimeError("No Voice PE is connected")

        message = json.dumps(obj, separators=(",", ":"))
        delivered = 0
        for websocket in websockets:
            try:
                await asyncio.wait_for(websocket.send(message), timeout=1.0)
                delivered += 1
            except Exception as error:
                logger.warning(
                    "⚠️ Could not deliver strict %s control: %r",
                    obj.get("type"),
                    error,
                )
        if delivered == 0:
            raise RuntimeError("No Voice PE accepted the control frame")

    async def arm_graceful_close(
        self,
        expected_non_close_generation: Optional[int] = None,
    ) -> bool:
        """Prepare then commit one context-bound, drain-safe graceful close."""
        sockets = tuple(self._websockets)
        session_nonce = self._active_session_nonce
        wake_generation = self._device_wake_generation
        if len(sockets) != 1 or session_nonce is None:
            raise RuntimeError("No Voice PE is connected")
        if not self._physical_wake_is_current():
            return False
        await self._cancel_request_follow_up_and_wait()
        async with self._graceful_close_lock:
            if self._graceful_close_owner_context is not None:
                raise RuntimeError("A conflicting graceful close is active")
            if (
                tuple(self._websockets) != sockets
                or self._active_session_nonce != session_nonce
                or self._device_wake_generation != wake_generation
                or not self._physical_wake_is_current()
            ):
                return False
            token = self._graceful_close_next_token
            self._graceful_close_next_token = (token % 0x7FFFFFFF) + 1
            context = _GracefulCloseContext(
                websocket=sockets[0],
                session_nonce=session_nonce,
                wake_generation=wake_generation,
                token=token,
            )
            if not self._graceful_close_context_is_current(context):
                return False
            self._graceful_close_pending_token = token
            self._graceful_close_pending_context = context
            # PREPARE may be accepted even when its ACK is lost. Retain this
            # exact owner until a context-bound CANCEL succeeds or its socket dies.
            self._graceful_close_owner_context = context
            try:
                await self._send_graceful_close_stage(
                    "prepare_suppress_followup",
                    "prepared",
                    context,
                )
                if not self._graceful_close_context_is_current(context):
                    if self._graceful_close_owner_context is context:
                        await self._cancel_graceful_close_context(context)
                    return False
                if (
                    expected_non_close_generation is not None
                    and expected_non_close_generation
                    != TURN_LIVENESS.non_close_tool_generation
                ):
                    await self._cancel_graceful_close_context(context)
                    return False
                # Track before transmission: if firmware commits but its ACK is
                # lost, a later non-close tool still knows which token to cancel.
                self._graceful_close_committed_token = token
                self._graceful_close_committed_context = context
                await self._send_graceful_close_stage(
                    "commit_suppress_followup",
                    "committed",
                    context,
                )
                if not self._graceful_close_context_is_current(context):
                    if self._graceful_close_owner_context is context:
                        await self._cancel_graceful_close_context(context)
                    return False
                if (
                    expected_non_close_generation is not None
                    and expected_non_close_generation
                    != TURN_LIVENESS.non_close_tool_generation
                ):
                    await self._cancel_graceful_close_context(context)
                    return False
                return True
            except BaseException:
                if (
                    self._graceful_close_owner_context is context
                    and not self._graceful_close_context_is_current(context)
                ):
                    await asyncio.shield(
                        self._cancel_graceful_close_context(context)
                    )
                raise
            finally:
                if self._graceful_close_pending_context is context:
                    self._graceful_close_pending_context = None
                    self._graceful_close_pending_token = None

    async def request_graceful_close(self) -> None:
        """Record a close request; arm it only at the final bot-stop boundary."""
        await self._cancel_request_follow_up_and_wait()
        self._graceful_close_requested_generation = (
            TURN_LIVENESS.non_close_tool_generation
        )

    async def _arm_requested_graceful_close(self) -> None:
        requested_generation = self._graceful_close_requested_generation
        self._graceful_close_requested_generation = None
        if requested_generation is None:
            return
        if requested_generation != TURN_LIVENESS.non_close_tool_generation:
            logger.info("Graceful close cancelled because another tool ran")
            return
        await self.arm_graceful_close(requested_generation)

    def cancel_graceful_close_request(self) -> None:
        """Drop a deferred close at a fresh user-turn boundary."""
        self._graceful_close_requested_generation = None

    def silent_close_decision_is_current(self) -> bool:
        """Return whether this response may make a terminal close decision."""
        grant = self._request_follow_up_answer_grant
        return (
            grant is not None
            and grant.confirmed
            and self._follow_up_answer_grant_is_current(grant)
            and self._request_follow_up_reservation is None
            and not self._request_follow_up_budget_spent
            and self._silent_close_context is None
            and all(
                value is None
                for value in (
                    self._graceful_close_requested_generation,
                    self._graceful_close_pending_token,
                    self._graceful_close_committed_token,
                    self._graceful_close_owner_context,
                )
            )
        )

    def silent_close_requires_spoken_response(self) -> bool:
        """Require speech when the exact answer semantically completed the turn."""
        grant = self._request_follow_up_answer_grant
        return bool(
            self.silent_close_decision_is_current()
            and grant is not None
            and grant.semantic_close_veto
        )

    def silent_close_is_allowed(self) -> bool:
        """Allow silent close only when the confirmed answer has no semantic veto."""
        return (
            self.silent_close_decision_is_current()
            and not self.silent_close_requires_spoken_response()
        )

    async def request_silent_close(self) -> None:
        """Commit a graceful close, fence output, and send current-wake idle."""
        async with self._socket_transition_lock:
            if not self.silent_close_is_allowed():
                raise RuntimeError(
                    "Silent close requires a confirmed follow-up answer"
                )
            grant = self._request_follow_up_answer_grant
            if grant is None:
                raise RuntimeError("Silent close lost its follow-up answer owner")
            context = _SilentCloseContext(
                websocket=grant.websocket,
                session_nonce=grant.session_nonce,
                wake_generation=grant.wake_generation,
            )
            expected_generation = grant.non_close_tool_generation
            self._silent_close_context = context
            self._request_follow_up_answer_grant = None
            self._request_follow_up_budget_spent = True
            self._request_follow_up_budget_tool_call_id = None
            self._user_turn_non_close_generation = None
            retired_output = self._retire_assistant_output_grant()
        await self._settle_retired_assistant_output(retired_output)

        try:
            if not await self.arm_graceful_close(expected_generation):
                raise RuntimeError("Silent close lost tool-generation ownership")
            async with self._socket_transition_lock:
                if not self._silent_close_is_current():
                    raise RuntimeError("Silent close lost its physical wake owner")
                self._device_audio_generation = None
                sent = await self._send_silent_close_terminal_idle_locked(context)
                if sent:
                    self._open_follow_up_phase_grant = None
            if not sent:
                raise RuntimeError("Silent close could not deliver final idle")
        except BaseException:
            await self._retire_bound_socket(
                context.websocket,
                context.session_nonce,
                expected_wake_generation=context.wake_generation,
            )
            raise

    async def _check_nearby_media_activity(self) -> MediaActivity:
        """Run the internal media guard with an independent fail-closed deadline."""
        if self._media_activity_check is None:
            return MediaActivity.CLEAR
        try:
            status = await asyncio.wait_for(
                self._media_activity_check(),
                timeout=self.FOLLOW_UP_MEDIA_CHECK_TIMEOUT_S,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("⚠️ Nearby media state is uncertain; follow-up stays closed")
            return MediaActivity.UNCERTAIN
        if not isinstance(status, MediaActivity):
            logger.warning("⚠️ Nearby media guard returned malformed state")
            return MediaActivity.UNCERTAIN
        return status

    def note_device_wake(self, wake_generation: Optional[int] = None) -> bool:
        """Start one physical wake and its absolute follow-up time ceiling."""
        sockets = tuple(self._websockets)
        if len(sockets) != 1 or self._active_session_nonce is None:
            self._user_turn_non_close_generation = None
            self._request_follow_up_budget_spent = True
            self._request_follow_up_budget_tool_call_id = None
            self._request_follow_up_answer_grant = None
            self._open_follow_up_phase_grant = None
            return False

        if wake_generation is None:
            wake_generation = (
                1
                if self._device_wake_generation in (0, 0x7FFFFFFF)
                else self._device_wake_generation + 1
            )
        if type(wake_generation) is not int or not 0 < wake_generation <= 0x7FFFFFFF:
            return False
        if self._device_wake_generation != 0:
            expected = (
                1
                if self._device_wake_generation == 0x7FFFFFFF
                else self._device_wake_generation + 1
            )
            if wake_generation != expected:
                return False

        # A valid newer firmware wake is authoritative evidence that every old
        # local follow-up grant was already revoked before this message was sent.
        self._settle_graceful_close_for_new_wake()
        self.invalidate_request_follow_up_turn(send_cancel=False)
        self._retire_assistant_output_grant()
        self._bound_follow_up_question_context = None
        self._open_follow_up_phase_grant = None
        self._device_wake_generation = wake_generation
        self._device_audio_generation = wake_generation
        self._wake_session_socket = sockets[0]
        self._wake_session_nonce = self._active_session_nonce
        self._physical_wake_deadline = (
            time.monotonic() + self.PHYSICAL_WAKE_CEILING_S
        )
        self._silent_close_context = None
        self._request_follow_up_budget_spent = False
        self._request_follow_up_budget_tool_call_id = None
        self._user_turn_non_close_generation = (
            TURN_LIVENESS.non_close_tool_generation
        )
        return True

    def _wake_context_matches(
        self,
        wake_generation: int,
        websocket,
        session_nonce: int,
        non_close_tool_generation: int,
    ) -> bool:
        return (
            wake_generation == self._device_wake_generation
            and websocket is self._wake_session_socket
            and session_nonce == self._wake_session_nonce
            and tuple(self._websockets) == (websocket,)
            and self._active_session_nonce == session_nonce
            and self._physical_wake_is_current()
            and not self._silent_close_is_current()
            and self._user_turn_non_close_generation
            == non_close_tool_generation
            and TURN_LIVENESS.non_close_tool_generation
            == non_close_tool_generation
        )

    async def reserve_request_follow_up(
        self,
        tool_call_id: str,
    ) -> FollowUpReservationOutcome:
        """Spend current-turn authority and conditionally prepare one control."""
        if not isinstance(tool_call_id, str) or not tool_call_id:
            raise RuntimeError("Requested follow-up requires its tool call ID")
        if self._follow_up_fail_closed:
            raise RuntimeError("Requested follow-up protocol is fail-closed")
        generation = TURN_LIVENESS.non_close_tool_generation
        sockets = tuple(self._websockets)
        current = self._request_follow_up_reservation

        if current is not None:
            if (
                current.tool_call_id == tool_call_id
                and self._request_follow_up_context_is_valid(current)
            ):
                return FollowUpReservationOutcome.ALREADY_RESERVED
            await self._cancel_request_follow_up_and_wait()
            return FollowUpReservationOutcome.REQUIRES_WAKE

        if self.follow_up_ms != 0:
            raise RuntimeError("Requested follow-up is disabled in automatic mode")
        if len(sockets) != 1:
            raise RuntimeError("Requested follow-up requires exactly one Voice PE")
        if self._active_session_nonce is None:
            raise RuntimeError("Requested follow-up requires an admitted Voice PE session")
        if self._user_turn_non_close_generation is None:
            raise RuntimeError("Requested follow-up requires a current user turn")
        if (
            self._device_wake_generation == 0
            or self._wake_session_socket is not sockets[0]
            or self._wake_session_nonce != self._active_session_nonce
        ):
            raise RuntimeError("Requested follow-up requires a genuine device wake")
        if generation != self._user_turn_non_close_generation:
            raise RuntimeError(
                "Another tool already ran in the current user turn"
            )
        if any(
            value is not None
            for value in (
                self._graceful_close_requested_generation,
                self._graceful_close_pending_token,
                self._graceful_close_committed_token,
                self._graceful_close_owner_context,
            )
        ):
            raise RuntimeError("A conflicting graceful close is active")
        if self._request_follow_up_budget_spent:
            logger.info(
                "Requested follow-up requires a fresh wake because answer authority "
                "was not rearmed"
            )
            return FollowUpReservationOutcome.REQUIRES_WAKE

        wake_generation = self._device_wake_generation
        websocket = sockets[0]
        session_nonce = self._active_session_nonce
        self._request_follow_up_budget_spent = True
        self._request_follow_up_budget_tool_call_id = tool_call_id
        self._request_follow_up_answer_grant = None
        if not self._physical_wake_is_current():
            return FollowUpReservationOutcome.REQUIRES_WAKE

        media_status = await self._check_nearby_media_activity()
        if not self._wake_context_matches(
            wake_generation,
            websocket,
            session_nonce,
            generation,
        ):
            raise RuntimeError("Requested follow-up context changed during media check")
        if media_status is not MediaActivity.CLEAR:
            logger.info(
                "Requested follow-up requires a fresh wake because nearby media is %s",
                media_status.value,
            )
            return FollowUpReservationOutcome.REQUIRES_WAKE

        self._request_follow_up_epoch += 1
        token = self._new_protocol_id(
            self._issued_request_follow_up_tokens,
            forbidden=frozenset(self._issued_hello_nonces),
        )
        reservation = _FollowUpReservation(
            websocket=websocket,
            session_nonce=session_nonce,
            tool_call_id=tool_call_id,
            non_close_tool_generation=generation,
            epoch=self._request_follow_up_epoch,
            token=token,
            wake_generation=wake_generation,
            expires_at=self._bounded_follow_up_expiry(
                self.REQUEST_FOLLOW_UP_EXPIRY_S
            ),
        )
        self._request_follow_up_reservation = reservation
        self._arm_request_follow_up_expiry(reservation)
        return FollowUpReservationOutcome.RESERVED

    def activate_request_follow_up(self, tool_call_id: str) -> bool:
        """Activate a prepared reservation after its tool result was queued."""
        reservation = self._request_follow_up_reservation
        if reservation is None:
            return False
        if reservation.tool_call_id != tool_call_id:
            return False
        if not self._request_follow_up_context_is_valid(reservation):
            self.cancel_request_follow_up()
            return False
        reservation.active = True
        return True

    async def _expire_request_follow_up(
        self,
        reservation: _FollowUpReservation,
    ) -> None:
        try:
            await asyncio.sleep(
                max(0.0, reservation.expires_at - time.monotonic())
            )
        except asyncio.CancelledError:
            return
        if (
            self._request_follow_up_reservation is reservation
            and time.monotonic() >= reservation.expires_at
        ):
            task = self.cancel_request_follow_up()
            if task is not None:
                await asyncio.shield(task)

    def cancel_request_follow_up(
        self,
        *,
        send_cancel: bool = True,
    ) -> Optional[asyncio.Task]:
        """Invalidate one transaction and revoke its token when it was sent."""
        reservation = self._request_follow_up_reservation
        if reservation is None:
            return None
        return self._cancel_request_follow_up_reservation(
            reservation,
            send_cancel=send_cancel,
        )

    def _cancel_request_follow_up_reservation(
        self,
        reservation: _FollowUpReservation,
        *,
        send_cancel: bool = True,
    ) -> Optional[asyncio.Task]:
        reservation.cancel_requested = True
        if not send_cancel:
            reservation.revocation_confirmed = True
        reservation.ack_event.set()
        reservation.commit_ack_event.set()
        if self._request_follow_up_reservation is reservation:
            self._request_follow_up_epoch += 1
            self._request_follow_up_reservation = None
            self._cancel_request_follow_up_expiry_task()
        if (
            send_cancel
            and reservation.control_send_started
            and not reservation.socket_retired
            and not reservation.revocation_confirmed
        ):
            if reservation.cancel_task is None:
                key = (reservation.session_nonce, reservation.token)
                if (
                    key not in self._request_follow_up_cancellations
                    and len(self._request_follow_up_cancellations)
                    >= self.MAX_FOLLOW_UP_CANCELLATIONS
                ):
                    reservation.socket_retired = True
                    logger.error("Follow-up cancellation quarantine is full")
                    return self._track_request_follow_up_task(
                        self._retire_bound_socket(
                            reservation.websocket,
                            reservation.session_nonce,
                        ),
                        settlement=True,
                    )
                self._request_follow_up_cancellations[key] = reservation
                reservation.cancel_task = self._track_request_follow_up_task(
                    self._cancel_request_follow_up_or_close(reservation),
                    settlement=True,
                )
            return reservation.cancel_task
        return None

    async def _cancel_request_follow_up_and_wait(self) -> None:
        task = self.cancel_request_follow_up()
        if task is not None:
            await asyncio.shield(task)
        await self._await_request_follow_up_settlements()

    async def _cancel_request_follow_up_before_forced_idle(self) -> None:
        task = self.invalidate_request_follow_up_turn()
        if task is not None:
            await asyncio.shield(task)
        await self._await_request_follow_up_settlements()

    def _retire_consumed_request_follow_up(self) -> bool:
        """Turn current OPEN speech into an unconfirmed answer grant."""
        reservation = self._request_follow_up_reservation
        if reservation is None or reservation.stage is not _FollowUpStage.OPEN:
            return False
        valid = self._request_follow_up_context_is_valid(reservation)
        self._request_follow_up_epoch += 1
        self._request_follow_up_reservation = None
        self._bound_follow_up_question_context = None
        self._cancel_request_follow_up_expiry_task()
        self._request_follow_up_budget_spent = True
        self._request_follow_up_budget_tool_call_id = None
        self._request_follow_up_answer_grant = (
            _FollowUpAnswerGrant(
                websocket=reservation.websocket,
                session_nonce=reservation.session_nonce,
                wake_generation=reservation.wake_generation,
                reservation_epoch=reservation.epoch,
                token=reservation.token,
                non_close_tool_generation=reservation.non_close_tool_generation,
            )
            if valid
            else None
        )
        self._open_follow_up_phase_grant = (
            _OpenFollowUpPhaseGrant(
                websocket=reservation.websocket,
                session_nonce=reservation.session_nonce,
                wake_generation=reservation.wake_generation,
                token=reservation.token,
            )
            if valid
            else None
        )
        return valid

    def _bounded_follow_up_expiry(self, ttl_s: float) -> float:
        return min(
            time.monotonic() + max(0.0, ttl_s),
            self._physical_wake_deadline,
        )

    def _arm_request_follow_up_expiry(
        self,
        reservation: _FollowUpReservation,
    ) -> None:
        self._cancel_request_follow_up_expiry_task()
        expiry_task = asyncio.create_task(
            self._expire_request_follow_up(reservation)
        )
        self._request_follow_up_expiry_task = expiry_task
        expiry_task.add_done_callback(self._request_follow_up_task_done)

    def _cancel_request_follow_up_expiry_task(self) -> None:
        expiry_task = self._request_follow_up_expiry_task
        self._request_follow_up_expiry_task = None
        try:
            current_task = asyncio.current_task()
        except RuntimeError:
            current_task = None
        if (
            expiry_task is not None
            and expiry_task is not current_task
            and not expiry_task.done()
        ):
            expiry_task.cancel()

    def _request_follow_up_task_done(self, task: asyncio.Task) -> None:
        self._request_follow_up_tasks.discard(task)
        self._request_follow_up_settlement_tasks.discard(task)
        if not task.cancelled():
            error = task.exception()
            if error is not None:
                logger.warning("⚠️ follow-up expiry task failed: %r", error)

    def _track_request_follow_up_task(
        self,
        coroutine,
        *,
        settlement: bool = False,
    ) -> asyncio.Task:
        if len(self._request_follow_up_tasks) >= self.MAX_FOLLOW_UP_TASKS:
            close = getattr(coroutine, "close", None)
            if close is not None:
                close()
            self._enter_follow_up_fail_closed("background-task limit reached")
            raise RuntimeError("Follow-up task limit reached")
        task = asyncio.create_task(coroutine)
        self._request_follow_up_tasks.add(task)
        if settlement:
            self._request_follow_up_settlement_tasks.add(task)
        task.add_done_callback(self._request_follow_up_task_done)
        return task

    def _enter_follow_up_fail_closed(self, reason: str) -> None:
        """Block all audio and retire the owner after a protocol resource fault."""
        if self._follow_up_fail_closed:
            return
        self._follow_up_fail_closed = True
        logger.error("Voice PE follow-up protocol failed closed: %s", reason)
        sockets = tuple(self._websockets)
        if len(sockets) != 1 or self._active_session_nonce is None:
            return
        self._track_wedge_task(
            self._retire_bound_socket(sockets[0], self._active_session_nonce)
        )

    async def _await_request_follow_up_settlements(self) -> None:
        while self._request_follow_up_settlement_tasks:
            tasks = tuple(self._request_follow_up_settlement_tasks)
            await asyncio.gather(*tasks, return_exceptions=True)
            for task in tasks:
                if task.done():
                    self._request_follow_up_settlement_tasks.discard(task)

    def note_request_follow_up_turn_boundary(self, *, force: bool = False) -> None:
        """Capture real OPEN speech without rearming before its transcript."""
        reservation = self._request_follow_up_reservation
        if reservation is not None and reservation.stage is _FollowUpStage.OPEN:
            self._retire_consumed_request_follow_up()
            force = True
        if not force and self._user_turn_non_close_generation is not None:
            return
        if reservation is not None:
            self.cancel_request_follow_up()
        self._user_turn_non_close_generation = TURN_LIVENESS.non_close_tool_generation

    def _follow_up_answer_grant_is_current(
        self,
        grant: _FollowUpAnswerGrant,
    ) -> bool:
        return (
            self._physical_wake_is_current()
            and tuple(self._websockets) == (grant.websocket,)
            and self._active_session_nonce == grant.session_nonce
            and self._wake_session_socket is grant.websocket
            and self._wake_session_nonce == grant.session_nonce
            and self._device_wake_generation == grant.wake_generation
            and self._request_follow_up_epoch == grant.reservation_epoch + 1
            and self._user_turn_non_close_generation
            == grant.non_close_tool_generation
            and TURN_LIVENESS.non_close_tool_generation
            == grant.non_close_tool_generation
            and not self._silent_close_is_current()
        )

    def _open_follow_up_phase_grant_is_current(
        self,
        grant: _OpenFollowUpPhaseGrant,
    ) -> bool:
        """Keep physical OPEN progression separate from spent tool authority."""
        return (
            self._physical_wake_is_current()
            and tuple(self._websockets) == (grant.websocket,)
            and self._active_session_nonce == grant.session_nonce
            and self._wake_session_socket is grant.websocket
            and self._wake_session_nonce == grant.session_nonce
            and self._device_wake_generation == grant.wake_generation
            and not self._silent_close_is_current()
        )

    def bind_request_follow_up_answer(
        self,
        user_item_id: str,
        user_item_sequence: int,
    ) -> bool:
        """Bind an OPEN answer grant to its exact fresh Realtime speech item."""
        if (
            not isinstance(user_item_id, str)
            or not user_item_id
            or type(user_item_sequence) is not int
            or user_item_sequence <= 0
        ):
            return False
        reservation = self._request_follow_up_reservation
        if reservation is not None and reservation.stage is _FollowUpStage.OPEN:
            # OpenAI's speech-start event can outrun the queued PhaseEmitter frame.
            # Consume the exact OPEN transaction here so identity binding does not
            # depend on downstream processor scheduling.
            self.note_request_follow_up_turn_boundary()
        grant = self._request_follow_up_answer_grant
        if (
            grant is None
            or grant.confirmed
            or grant.user_item_id is not None
            or self._request_follow_up_reservation is not None
            or not self._follow_up_answer_grant_is_current(grant)
        ):
            if reservation is not None and reservation.stage is _FollowUpStage.OPEN:
                logger.info("Follow-up answer identity failed closed after OPEN speech")
            return False
        grant.user_item_id = user_item_id
        grant.user_item_sequence = user_item_sequence
        return True

    def confirm_request_follow_up_answer(
        self,
        user_item_id: str,
        user_item_sequence: int,
        transcript: str,
    ) -> bool:
        """Rearm one next-round decision only for a genuine current answer."""
        grant = self._request_follow_up_answer_grant
        if (
            grant is None
            or grant.confirmed
            or grant.user_item_id != user_item_id
            or grant.user_item_sequence != user_item_sequence
            or not isinstance(transcript, str)
            or not transcript.strip()
            or self._request_follow_up_reservation is not None
            or not self._follow_up_answer_grant_is_current(grant)
            or any(
                value is not None
                for value in (
                    self._graceful_close_requested_generation,
                    self._graceful_close_pending_token,
                    self._graceful_close_committed_token,
                    self._graceful_close_owner_context,
                )
            )
        ):
            return False
        grant.semantic_close_veto = _answer_requires_spoken_close(transcript)
        grant.confirmed = True
        self._request_follow_up_budget_spent = False
        self._request_follow_up_budget_tool_call_id = None
        return True

    def invalidate_request_follow_up_turn(
        self,
        *,
        send_cancel: bool = True,
    ) -> Optional[asyncio.Task]:
        """End the current user turn and invalidate any prepared follow-up."""
        task = self.cancel_request_follow_up(send_cancel=send_cancel)
        self._user_turn_non_close_generation = None
        self._request_follow_up_budget_spent = True
        self._request_follow_up_budget_tool_call_id = None
        self._request_follow_up_answer_grant = None
        return task

    def _request_follow_up_context_is_valid(
        self,
        reservation: _FollowUpReservation,
    ) -> bool:
        sockets = tuple(self._websockets)
        generation = TURN_LIVENESS.non_close_tool_generation
        return (
            self.follow_up_ms == 0
            and time.monotonic() < reservation.expires_at
            and self._physical_wake_is_current()
            and not self._silent_close_is_current()
            and reservation.epoch == self._request_follow_up_epoch
            and reservation.wake_generation == self._device_wake_generation
            and reservation.websocket is self._wake_session_socket
            and reservation.session_nonce == self._wake_session_nonce
            and self._request_follow_up_budget_spent
            and self._request_follow_up_budget_tool_call_id
            == reservation.tool_call_id
            and (
                self._user_turn_non_close_generation is None
                or reservation.non_close_tool_generation
                == self._user_turn_non_close_generation
            )
            and reservation.non_close_tool_generation == generation
            and len(sockets) == 1
            and sockets[0] is reservation.websocket
            and self._active_session_nonce == reservation.session_nonce
            and all(
                value is None
                for value in (
                    self._graceful_close_requested_generation,
                    self._graceful_close_pending_token,
                    self._graceful_close_committed_token,
                    self._graceful_close_owner_context,
                )
            )
        )

    def arm_request_follow_up_continuation(
        self,
        tool_call_ids: set[str],
    ) -> bool:
        """Arm only the managed response.create carrying this tool's result."""
        reservation = self._request_follow_up_reservation
        if reservation is None:
            return False
        if not self._request_follow_up_context_is_valid(reservation):
            self.cancel_request_follow_up()
            return False
        if not reservation.active or tool_call_ids != {reservation.tool_call_id}:
            self.cancel_request_follow_up()
            return False
        reservation.continuation_armed = True
        return True

    def fail_request_follow_up_continuation(
        self,
        tool_call_ids: set[str],
    ) -> None:
        reservation = self._request_follow_up_reservation
        if reservation is None:
            return
        if (
            reservation.continuation_armed
            or reservation.tool_call_id in tool_call_ids
        ):
            self.cancel_request_follow_up()

    def bind_request_follow_up_response(
        self,
        response_id: Optional[str],
        response_generation: Optional[int],
    ) -> bool:
        """Consume one response.created armed at the managed send boundary."""
        reservation = self._request_follow_up_reservation
        if reservation is None or not reservation.active:
            return False
        if not self._request_follow_up_context_is_valid(reservation):
            self.cancel_request_follow_up()
            return False
        if not reservation.continuation_armed:
            logger.info(
                "Requested follow-up cancelled by an unowned competing response"
            )
            self.cancel_request_follow_up()
            return False
        reservation.continuation_armed = False
        if (
            not isinstance(response_id, str)
            or not response_id
            or type(response_generation) is not int
            or response_generation <= 0
        ):
            self.cancel_request_follow_up()
            return False
        if reservation.response_id is None:
            reservation.response_id = response_id
            reservation.response_generation = response_generation
            reservation.expires_at = self._bounded_follow_up_expiry(
                self.PHYSICAL_WAKE_CEILING_S
            )
            self._bound_follow_up_question_context = (
                response_id,
                response_generation,
                reservation.epoch,
                reservation.token,
            )
            self._arm_request_follow_up_expiry(reservation)
            return True
        if (
            reservation.response_id != response_id
            or reservation.response_generation != response_generation
        ):
            logger.info("Requested follow-up cancelled by a conflicting response")
            self.cancel_request_follow_up()
            return False
        return True

    def request_follow_up_question_output_is_current(
        self,
        response_id: str,
        response_generation: int,
    ) -> bool:
        """Authorize held question output only for the exact live reservation."""
        reservation = self._request_follow_up_reservation
        return bool(
            reservation is not None
            and self._bound_follow_up_question_context
            == (
                response_id,
                response_generation,
                reservation.epoch,
                reservation.token,
            )
            and reservation.active
            and not reservation.cancel_requested
            and reservation.stage is _FollowUpStage.RESERVED
            and reservation.response_id == response_id
            and reservation.response_generation == response_generation
            and reservation.question_audio_started
            and self._request_follow_up_context_is_valid(reservation)
        )

    def note_request_follow_up_response_audio(
        self,
        response_id: Optional[str],
    ) -> None:
        """Qualify only audio emitted by the exact bound OpenAI response."""
        reservation = self._request_follow_up_reservation
        if reservation is None or not reservation.active:
            return
        if not self._request_follow_up_context_is_valid(reservation):
            self.cancel_request_follow_up()
            return
        if not response_id or reservation.response_id != response_id:
            if reservation.response_id is not None:
                logger.info("Requested follow-up cancelled by wrong-response audio")
                self.cancel_request_follow_up()
            return
        reservation.question_audio_started = True

    def note_request_follow_up_playback_started(self) -> None:
        """Bind physical playback start to the qualified continuation response."""
        reservation = self._request_follow_up_reservation
        if reservation is None or not reservation.active:
            return
        if not self._request_follow_up_context_is_valid(reservation):
            self.cancel_request_follow_up()
            return
        if reservation.response_id is None or not reservation.question_audio_started:
            return
        reservation.playback_started = True

    def note_assistant_playback_started(self) -> None:
        """Advance the reply boundary before binding any managed question."""
        self._reply_generation = (
            1 if self._reply_generation >= 0x7FFFFFFF else self._reply_generation + 1
        )
        answer_grant = self._request_follow_up_answer_grant
        preserve_answer_decision = (
            answer_grant is not None
            and answer_grant.confirmed
            and self._follow_up_answer_grant_is_current(answer_grant)
        )
        if (
            self._request_follow_up_reservation is None
            and not preserve_answer_decision
        ):
            self._request_follow_up_answer_grant = None
            self._request_follow_up_budget_spent = True
            self._request_follow_up_budget_tool_call_id = None
        self.note_request_follow_up_playback_started()

    def capture_reply_finalizer_context(
        self,
    ) -> Optional[_ReplyFinalizerContext]:
        sockets = tuple(self._websockets)
        if (
            len(sockets) != 1
            or self._active_session_nonce is None
            or self._device_wake_generation == 0
        ):
            return None
        return _ReplyFinalizerContext(
            websocket=sockets[0],
            session_nonce=self._active_session_nonce,
            wake_generation=self._device_wake_generation,
            reply_generation=self._reply_generation,
        )

    def _reply_finalizer_is_current(
        self,
        context: Optional[_ReplyFinalizerContext],
    ) -> bool:
        return (
            context is not None
            and tuple(self._websockets) == (context.websocket,)
            and self._active_session_nonce == context.session_nonce
            and self._device_wake_generation == context.wake_generation
            and self._reply_generation == context.reply_generation
        )

    def note_request_follow_up_response_done(
        self,
        response_id: Optional[str],
        status: Optional[str],
    ) -> None:
        reservation = self._request_follow_up_reservation
        if reservation is None or not reservation.active:
            return
        if reservation.response_id != response_id:
            # The tool-call response can finish after activation but before its
            # continuation is created. Only the bound continuation is terminal.
            return
        if status != "completed" or not reservation.question_audio_started:
            self.cancel_request_follow_up()
            return
        reservation.response_completed = True

    def note_request_follow_up_response_failed(
        self,
        response_id: Optional[str] = None,
    ) -> None:
        reservation = self._request_follow_up_reservation
        if reservation is None:
            return
        if response_id is None or reservation.response_id in (None, response_id):
            self.cancel_request_follow_up()

    def _handle_request_follow_up_ack(self, data: dict) -> None:
        reservation = self._request_follow_up_reservation
        token = data.get("token")
        session_nonce = data.get("session_nonce")
        if (
            reservation is None
            or set(data) != {"type", "token", "session_nonce", "accepted"}
            or type(token) is not int
            or token != reservation.token
            or type(session_nonce) is not int
            or session_nonce != reservation.session_nonce
            or type(data.get("accepted")) is not bool
            or not reservation.control_send_started
            or reservation.stage is not _FollowUpStage.PREPARING
            or tuple(self._websockets) != (reservation.websocket,)
            or self._active_session_nonce != reservation.session_nonce
        ):
            logger.warning("⚠️ Ignoring stale requested-follow-up ACK")
            return
        if reservation.ack_received:
            logger.warning("⚠️ Ignoring duplicate requested-follow-up ACK")
            return
        reservation.ack_received = True
        reservation.ack_accepted = data.get("accepted") is True
        if reservation.ack_accepted:
            reservation.stage = _FollowUpStage.PREPARED
            reservation.expires_at = self._bounded_follow_up_expiry(
                self._request_follow_up_ready_timeout_s()
            )
            self._arm_request_follow_up_expiry(reservation)
        reservation.ack_event.set()

    def _handle_follow_up_ready(self, data: dict) -> None:
        reservation = self._request_follow_up_reservation
        token = data.get("token")
        session_nonce = data.get("session_nonce")
        ready_nonce = data.get("ready_nonce")
        if (
            reservation is None
            or set(data) != {"type", "token", "session_nonce", "ready_nonce"}
            or type(token) is not int
            or token != reservation.token
            or type(session_nonce) is not int
            or session_nonce != reservation.session_nonce
            or type(ready_nonce) is not int
            or not 0 < ready_nonce <= 0x7FFFFFFF
            or ready_nonce in {reservation.token, reservation.session_nonce}
            or ready_nonce in self._seen_ready_nonces
            or len(self._seen_ready_nonces) >= self.PROTOCOL_HISTORY_LIMIT
            or reservation.stage is not _FollowUpStage.PREPARED
            or not reservation.ack_accepted
            or not self._request_follow_up_context_is_valid(reservation)
        ):
            logger.warning("Ignoring stale or malformed follow-up READY receipt")
            return
        self._seen_ready_nonces.add(ready_nonce)
        reservation.ready_nonce = ready_nonce
        reservation.stage = _FollowUpStage.READY
        reservation.expires_at = (
            self._bounded_follow_up_expiry(
                self.REQUEST_FOLLOW_UP_COMMIT_ACK_TIMEOUT_S + 2.0
            )
        )
        self._arm_request_follow_up_expiry(reservation)
        self._track_request_follow_up_task(
            self._commit_ready_follow_up(reservation),
            settlement=True,
        )

    def _handle_commit_follow_up_ack(self, data: dict) -> None:
        reservation = self._request_follow_up_reservation
        token = data.get("token")
        session_nonce = data.get("session_nonce")
        ready_nonce = data.get("ready_nonce")
        if (
            reservation is None
            or set(data)
            != {"type", "token", "session_nonce", "ready_nonce", "accepted"}
            or type(token) is not int
            or token != reservation.token
            or type(session_nonce) is not int
            or session_nonce != reservation.session_nonce
            or type(ready_nonce) is not int
            or ready_nonce != reservation.ready_nonce
            or type(data.get("accepted")) is not bool
            or reservation.stage is not _FollowUpStage.COMMITTING
            or not reservation.commit_send_started
            or tuple(self._websockets) != (reservation.websocket,)
            or self._active_session_nonce != reservation.session_nonce
        ):
            logger.warning("Ignoring stale or malformed follow-up COMMIT ACK")
            return
        if reservation.commit_ack_received:
            logger.warning("Ignoring duplicate follow-up COMMIT ACK")
            return
        reservation.commit_ack_received = True
        reservation.commit_ack_accepted = data.get("accepted") is True
        if reservation.commit_ack_accepted:
            # The firmware emits this receipt while its mic is still closed and
            # opens only after a final local check. Marking OPEN here ensures the
            # first subsequently received PCM frame is not lost to task scheduling.
            reservation.stage = _FollowUpStage.OPEN
            self._device_audio_generation = reservation.wake_generation
        reservation.commit_ack_event.set()

    def _handle_cancel_request_follow_up_ack(self, data: dict) -> None:
        token = data.get("token")
        session_nonce = data.get("session_nonce")
        if (
            set(data)
            != {"type", "token", "session_nonce", "accepted", "cleared"}
            or type(token) is not int
            or type(session_nonce) is not int
        ):
            logger.warning("⚠️ Ignoring malformed requested-follow-up cancel ACK")
            return
        key = (session_nonce, token)
        reservation = self._request_follow_up_cancellations.get(key)
        if (
            reservation is None
            or not reservation.cancel_send_started
            or tuple(self._websockets) != (reservation.websocket,)
            or self._active_session_nonce != reservation.session_nonce
        ):
            logger.warning("⚠️ Ignoring stale requested-follow-up cancel ACK")
            return
        if reservation.cancel_ack_received:
            logger.warning("⚠️ Ignoring duplicate requested-follow-up cancel ACK")
            return
        accepted = data.get("accepted")
        cleared = data.get("cleared")
        if type(accepted) is not bool or type(cleared) is not bool:
            logger.warning("⚠️ Ignoring malformed requested-follow-up cancel ACK")
            return
        reservation.cancel_ack_received = True
        reservation.cancel_ack_accepted = accepted
        reservation.cancel_ack_cleared = cleared
        # Active and already-retired same-session tokens both settle as
        # accepted+cleared. Any other pair is ambiguous and closes the socket.
        reservation.cancel_ack_confirms_revocation = accepted and cleared
        reservation.cancel_ack_event.set()

    async def _cancel_request_follow_up_or_close(
        self,
        reservation: _FollowUpReservation,
    ) -> bool:
        key = (reservation.session_nonce, reservation.token)
        reservation.cancel_send_started = True
        payload = json.dumps(
            {
                "type": "cancel_request_follow_up",
                "token": reservation.token,
                "session_nonce": reservation.session_nonce,
            },
            separators=(",", ":"),
        )
        try:
            async with self._request_follow_up_control_lock:
                if reservation.socket_retired:
                    self._request_follow_up_cancellations.pop(key, None)
                    return True
                await asyncio.wait_for(
                    reservation.websocket.send(payload),
                    timeout=self.REQUEST_FOLLOW_UP_SEND_TIMEOUT_S,
                )
                reservation.cancel_sent = True
        except Exception as error:
            logger.warning("⚠️ Could not revoke requested follow-up: %r", error)
        else:
            try:
                await asyncio.wait_for(
                    reservation.cancel_ack_event.wait(),
                    timeout=self.REQUEST_FOLLOW_UP_ACK_TIMEOUT_S,
                )
            except TimeoutError:
                logger.warning(
                    "Voice PE did not confirm requested-follow-up cancellation",
                )
            if reservation.socket_retired:
                self._request_follow_up_cancellations.pop(key, None)
                return True
            if (
                reservation.cancel_ack_received
                and reservation.cancel_ack_confirms_revocation
            ):
                self._request_follow_up_cancellations.pop(key, None)
                return True

        logger.warning(
            "⚠️ Requested-follow-up ownership is ambiguous; retiring Voice PE socket"
        )
        await self._retire_bound_socket(
            reservation.websocket,
            reservation.session_nonce,
        )
        self._request_follow_up_cancellations.pop(key, None)
        return False

    async def _send_requested_follow_up(self) -> None:
        """Send the PREPARE while firmware is still in the replying phase."""
        reservation = self._request_follow_up_reservation
        if (
            reservation is None
            or reservation.stage is not _FollowUpStage.RESERVED
            or not reservation.active
            or reservation.response_id is None
            or not reservation.question_audio_started
            or not reservation.playback_started
            or not reservation.response_completed
        ):
            if reservation is not None and reservation.active:
                self.cancel_request_follow_up()
            return

        if not self._request_follow_up_context_is_valid(reservation):
            self.cancel_request_follow_up()
            return

        try:
            async with self._request_follow_up_control_lock:
                if (
                    self._request_follow_up_reservation is not reservation
                    or reservation.cancel_requested
                ):
                    return
                payload = json.dumps(
                    {
                        "type": "request_follow_up",
                        "token": reservation.token,
                        "session_nonce": reservation.session_nonce,
                    },
                    separators=(",", ":"),
                )
                reservation.stage = _FollowUpStage.PREPARING
                reservation.control_send_started = True
                await asyncio.wait_for(
                    reservation.websocket.send(payload),
                    timeout=self.REQUEST_FOLLOW_UP_SEND_TIMEOUT_S,
                )
                reservation.control_sent = True
        except Exception as error:
            logger.warning("⚠️ Could not deliver requested follow-up: %r", error)
            task = self._cancel_request_follow_up_reservation(reservation)
            if task is not None:
                await asyncio.shield(task)
            return

        if (
            self._request_follow_up_reservation is not reservation
            or reservation.cancel_requested
        ):
            task = self._cancel_request_follow_up_reservation(reservation)
            if task is not None:
                await asyncio.shield(task)
            return

        try:
            await asyncio.wait_for(
                reservation.ack_event.wait(),
                timeout=self.REQUEST_FOLLOW_UP_ACK_TIMEOUT_S,
            )
        except TimeoutError:
            logger.warning("Voice PE did not acknowledge follow-up PREPARE")
            task = self._cancel_request_follow_up_reservation(reservation)
            if task is not None:
                await asyncio.shield(task)
            return

        if (
            self._request_follow_up_reservation is not reservation
            or reservation.cancel_requested
            or not reservation.ack_accepted
            or not self._request_follow_up_context_is_valid(reservation)
        ):
            task = self._cancel_request_follow_up_reservation(
                reservation,
                send_cancel=(
                    reservation.ack_accepted or not reservation.ack_received
                ),
            )
            if task is not None:
                await asyncio.shield(task)
            return

        logger.info("Voice PE acknowledged follow-up PREPARE; awaiting READY")

    async def _commit_ready_follow_up(
        self,
        reservation: _FollowUpReservation,
    ) -> None:
        """Guarantee READY/COMMIT ownership is retired on every task exit."""
        try:
            await self._commit_ready_follow_up_transaction(reservation)
        finally:
            if (
                self._request_follow_up_reservation is reservation
                and reservation.stage
                in {_FollowUpStage.READY, _FollowUpStage.COMMITTING}
            ):
                task = self._cancel_request_follow_up_reservation(reservation)
                if task is not None:
                    await asyncio.shield(task)

    async def _commit_ready_follow_up_transaction(
        self,
        reservation: _FollowUpReservation,
    ) -> None:
        """Run the final media fence and commit one exact READY transaction."""
        media_status = await self._check_nearby_media_activity()
        try:
            async with self._request_follow_up_control_lock:
                if (
                    self._request_follow_up_reservation is not reservation
                    or reservation.cancel_requested
                    or reservation.stage is not _FollowUpStage.READY
                    or reservation.ready_nonce is None
                    or not self._request_follow_up_context_is_valid(reservation)
                ):
                    return
                if media_status is not MediaActivity.CLEAR:
                    logger.info(
                        "Follow-up READY cancelled because nearby media is %s",
                        media_status.value,
                    )
                    self._cancel_request_follow_up_reservation(reservation)
                    return

                payload = json.dumps(
                    {
                        "type": "commit_follow_up",
                        "token": reservation.token,
                        "session_nonce": reservation.session_nonce,
                        "ready_nonce": reservation.ready_nonce,
                    },
                    separators=(",", ":"),
                )
                reservation.stage = _FollowUpStage.COMMITTING
                reservation.commit_send_started = True
                await asyncio.wait_for(
                    reservation.websocket.send(payload),
                    timeout=self.REQUEST_FOLLOW_UP_SEND_TIMEOUT_S,
                )
                reservation.commit_sent = True
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning("Could not deliver follow-up COMMIT: %r", error)
            task = self._cancel_request_follow_up_reservation(reservation)
            if task is not None:
                await asyncio.shield(task)
            return

        if (
            self._request_follow_up_reservation is not reservation
            or reservation.cancel_requested
        ):
            task = self._cancel_request_follow_up_reservation(reservation)
            if task is not None:
                await asyncio.shield(task)
            return

        try:
            await asyncio.wait_for(
                reservation.commit_ack_event.wait(),
                timeout=self.REQUEST_FOLLOW_UP_COMMIT_ACK_TIMEOUT_S,
            )
        except TimeoutError:
            logger.warning("Voice PE did not acknowledge follow-up COMMIT")
            task = self._cancel_request_follow_up_reservation(reservation)
            if task is not None:
                await asyncio.shield(task)
            return

        if (
            self._request_follow_up_reservation is not reservation
            or reservation.cancel_requested
            or not reservation.commit_ack_received
            or not reservation.commit_ack_accepted
            or not self._request_follow_up_context_is_valid(reservation)
        ):
            task = self._cancel_request_follow_up_reservation(
                reservation,
                send_cancel=(
                    reservation.commit_ack_accepted
                    or not reservation.commit_ack_received
                ),
            )
            if task is not None:
                await asyncio.shield(task)
            return

        reservation.stage = _FollowUpStage.OPEN
        reservation.expires_at = (
            self._bounded_follow_up_expiry(
                self.REQUEST_FOLLOW_UP_ACCEPTED_TTL_S
            )
        )
        self._arm_request_follow_up_expiry(reservation)
        logger.info("Voice PE acknowledged follow-up COMMIT; explicit window is open")

    async def _before_reply_idle(
        self,
        context: Optional[_ReplyFinalizerContext] = None,
    ) -> bool:
        """Apply a deferred conversation control at the final reply boundary."""
        if context is None:
            context = self.capture_reply_finalizer_context()
        if not self._reply_finalizer_is_current(context):
            return False
        try:
            await self._arm_requested_graceful_close()
            if not self._reply_finalizer_is_current(context):
                return False
            await self._send_requested_follow_up()
        except asyncio.CancelledError:
            if self._reply_finalizer_is_current(context):
                self.cancel_request_follow_up()
            raise
        finally:
            if self._reply_finalizer_is_current(context):
                await self._await_request_follow_up_settlements()
            if self._reply_finalizer_is_current(context):
                self._user_turn_non_close_generation = None
                self._request_follow_up_budget_spent = True
                self._request_follow_up_answer_grant = None
        return self._reply_finalizer_is_current(context)

    async def cancel_deferred_conversation_controls(self) -> None:
        """Cancel controls that must not survive another tool starting."""
        task = self.invalidate_request_follow_up_turn()
        if task is not None:
            await asyncio.shield(task)
        await self._await_request_follow_up_settlements()
        await self.cancel_graceful_close()

    async def cancel_graceful_close(self) -> None:
        """Invalidate pending/committed close before another tool executes."""
        await self._cancel_request_follow_up_and_wait()
        self._graceful_close_requested_generation = None
        async with self._graceful_close_lock:
            context = self._graceful_close_owner_context
            if context is not None:
                await self._cancel_graceful_close_context(context)

    def _graceful_close_context_is_current(
        self,
        context: _GracefulCloseContext,
    ) -> bool:
        return (
            tuple(self._websockets) == (context.websocket,)
            and self._active_session_nonce == context.session_nonce
            and self._device_wake_generation == context.wake_generation
            and self._physical_wake_is_current()
        )

    async def _send_graceful_close_control(
        self,
        context: _GracefulCloseContext,
        message_type: str,
        *,
        require_current_context: bool,
    ) -> bool:
        if (
            require_current_context
            and not self._graceful_close_context_is_current(context)
        ):
            raise RuntimeError("Graceful close lost its physical wake owner")
        if (
            context.websocket not in self._websockets
            or self._active_session_nonce != context.session_nonce
        ):
            if require_current_context:
                raise RuntimeError("Graceful close lost its physical socket owner")
            return False
        payload = {
            "type": message_type,
            "token": context.token,
            "session_nonce": context.session_nonce,
            "wake_generation": context.wake_generation,
        }
        try:
            await asyncio.wait_for(
                context.websocket.send(json.dumps(payload, separators=(",", ":"))),
                timeout=1.0,
            )
        except Exception as error:
            logger.warning(
                "⚠️ Could not deliver strict %s control: %r",
                message_type,
                error,
            )
            raise RuntimeError("No Voice PE accepted the control frame") from error
        if (
            require_current_context
            and not self._graceful_close_context_is_current(context)
        ):
            raise RuntimeError("Graceful close changed owner during control send")
        return True

    def _clear_graceful_close_context(
        self,
        context: _GracefulCloseContext,
    ) -> None:
        expectation = self._graceful_close_ack_expectation
        if expectation is not None and expectation.context is context:
            self._graceful_close_ack_expectation = None
            if not expectation.result.done():
                expectation.result.set_result(None)
        if self._graceful_close_pending_context is context:
            self._graceful_close_pending_context = None
            self._graceful_close_pending_token = None
        if self._graceful_close_committed_context is context:
            self._graceful_close_committed_context = None
            self._graceful_close_committed_token = None
        if self._graceful_close_owner_context is context:
            self._graceful_close_owner_context = None

    def _settle_graceful_close_for_new_wake(self) -> None:
        """Burn every old graceful owner before installing a newer wake."""
        expectation = self._graceful_close_ack_expectation
        self._graceful_close_ack_expectation = None
        if expectation is not None and not expectation.result.done():
            expectation.result.set_result(None)
        self._graceful_close_pending_context = None
        self._graceful_close_pending_token = None
        self._graceful_close_committed_context = None
        self._graceful_close_committed_token = None
        self._graceful_close_owner_context = None
        self._graceful_close_requested_generation = None

    async def _cancel_graceful_close_context(
        self,
        context: _GracefulCloseContext,
    ) -> None:
        # Fail closed: callers must not execute a competing home action while
        # firmware may still be armed to suppress that action's follow-up.
        await self._send_graceful_close_control(
            context,
            "cancel_suppress_followup",
            require_current_context=False,
        )
        self._clear_graceful_close_context(context)

    async def _send_graceful_close_stage(
        self,
        message_type: str,
        expected_stage: str,
        context: _GracefulCloseContext,
    ) -> None:
        expectation = _GracefulCloseAckExpectation(
            context=context,
            stage=expected_stage,
            result=asyncio.get_running_loop().create_future(),
        )
        self._graceful_close_ack_expectation = expectation
        logger.info(
            "Graceful close %s sent; waiting %.1fs for Voice PE ACK",
            expected_stage,
            self.GRACEFUL_CLOSE_ACK_TIMEOUT_S,
        )
        try:
            await self._send_graceful_close_control(
                context,
                message_type,
                require_current_context=True,
            )
            accepted = await asyncio.wait_for(
                expectation.result,
                timeout=self.GRACEFUL_CLOSE_ACK_TIMEOUT_S,
            )
        except TimeoutError as error:
            raise RuntimeError(
                f"Voice PE did not acknowledge graceful close {expected_stage}"
            ) from error
        finally:
            if self._graceful_close_ack_expectation is expectation:
                self._graceful_close_ack_expectation = None
        if accepted is None:
            raise RuntimeError("Graceful close physical owner was retired")
        if accepted is not True:
            self._clear_graceful_close_context(context)
            raise RuntimeError(
                f"Voice PE rejected graceful close {expected_stage} outside an active turn"
            )
        logger.info(
            "Graceful close %s acknowledged",
            expected_stage,
        )

    def _handle_graceful_close_ack(
        self,
        data: dict,
        websocket: Any = None,
    ) -> None:
        """Settle only the exact stage and immutable physical owner awaiting it."""
        expectation = self._graceful_close_ack_expectation
        context = expectation.context if expectation is not None else None
        token = data.get("token")
        session_nonce = data.get("session_nonce")
        wake_generation = data.get("wake_generation")
        stage = data.get("stage")
        accepted = data.get("accepted")
        if (
            expectation is None
            or context is None
            or not has_exact_fields(
                data,
                TRUSTED_DEVICE_TO_BACKEND_FIELDS["suppress_followup_ack"],
            )
            or data.get("type") != "suppress_followup_ack"
            or type(token) is not int
            or token != context.token
            or type(session_nonce) is not int
            or session_nonce != context.session_nonce
            or type(wake_generation) is not int
            or wake_generation != context.wake_generation
            or stage != expectation.stage
            or type(accepted) is not bool
            or websocket is not context.websocket
            or self._graceful_close_pending_context is not context
            or self._graceful_close_owner_context is not context
            or (
                accepted is True
                and not self._graceful_close_context_is_current(context)
            )
            or expectation.result.done()
        ):
            logger.warning("⚠️ Ignoring stale graceful-close ACK")
            return
        expectation.result.set_result(accepted)

    async def broadcast_bytes(self, data: bytes) -> None:
        """Send raw binary (24 kHz mono PCM16 audio) to every connected device.

        The device treats every BINARY frame as reply audio, so this pushes
        sound to the speaker outside any OpenAI response for timers and
        authenticated announcements."""
        if (
            self._follow_up_fail_closed
            or self._input_clear_fail_closed
            or len(self._websockets) != 1
            or self._active_session_nonce is None
        ):
            return
        for ws in list(self._websockets):
            try:
                await ws.send(data)
            except Exception as e:
                logger.warning(f"⚠️ broadcast_bytes failed: {e!r}")

    @staticmethod
    def _build_phase_control(
        value: str,
        *,
        session_nonce: Optional[int] = None,
        wake_generation: Optional[int] = None,
        follow_up_token: Optional[int] = None,
    ) -> dict[str, Any]:
        """Build an exact trusted phase or the preserved legacy staging shape."""
        if value not in {"listening", "thinking", "replying", "idle"}:
            raise ValueError("invalid phase")
        if session_nonce is None and wake_generation is None:
            payload = {"type": "phase", "value": value}
            if not has_exact_fields(
                payload,
                LEGACY_BACKEND_TO_DEVICE_FIELDS["phase"],
            ):
                raise RuntimeError("invalid legacy phase shape")
            return payload
        if (
            type(session_nonce) is not int
            or session_nonce <= 0
            or type(wake_generation) is not int
            or wake_generation <= 0
        ):
            raise ValueError("trusted phase requires positive credentials")
        payload = {
            "type": "phase",
            "value": value,
            "session_nonce": session_nonce,
            "wake_generation": wake_generation,
        }
        field_contract = TRUSTED_BACKEND_TO_DEVICE_FIELDS["phase"]
        if follow_up_token is not None:
            if value == "idle":
                raise ValueError("terminal idle phase cannot carry a follow-up token")
            if type(follow_up_token) is not int or follow_up_token <= 0:
                raise ValueError("follow-up phase requires a positive token")
            payload["token"] = follow_up_token
            field_contract = TRUSTED_BACKEND_TO_DEVICE_FIELDS[
                "follow_up_progress_phase"
            ]
        if not has_exact_fields(
            payload,
            field_contract,
        ):
            raise RuntimeError("invalid trusted phase shape")
        return payload

    def capture_phase_authorization_context(
        self,
    ) -> Optional[_PhaseAuthorizationContext]:
        """Capture one phase's exact wake and optional OPEN-answer authority."""
        websockets = tuple(self._websockets)
        session_nonce = self._active_session_nonce
        wake_generation = self._device_wake_generation
        if (
            len(websockets) != 1
            or type(session_nonce) is not int
            or session_nonce <= 0
            or type(wake_generation) is not int
            or wake_generation <= 0
            or not self._physical_wake_is_current()
            or self._silent_close_is_current()
        ):
            return None
        grant = self._open_follow_up_phase_grant
        follow_up_token = (
            grant.token
            if grant is not None
            and self._open_follow_up_phase_grant_is_current(grant)
            else None
        )
        return _PhaseAuthorizationContext(
            websocket=websockets[0],
            session_nonce=session_nonce,
            wake_generation=wake_generation,
            follow_up_epoch=self._request_follow_up_epoch,
            follow_up_token=follow_up_token,
            terminal_idle=False,
        )

    def capture_terminal_idle_phase_authorization_context(
        self,
    ) -> Optional[_PhaseAuthorizationContext]:
        """Capture the same wake authority with an explicitly tokenless idle."""
        context = self.capture_phase_authorization_context()
        if context is None:
            return None
        return _PhaseAuthorizationContext(
            websocket=context.websocket,
            session_nonce=context.session_nonce,
            wake_generation=context.wake_generation,
            follow_up_epoch=context.follow_up_epoch,
            follow_up_token=None,
            terminal_idle=True,
        )

    async def _send_phase_for_context(
        self,
        websocket: Any,
        value: str,
        session_nonce: int,
        wake_generation: int,
        follow_up_token: Optional[int] = None,
        follow_up_epoch: Optional[int] = None,
        terminal_idle: bool = False,
    ) -> bool:
        """Send a phase only while its exact physical wake context remains current."""
        async with self._socket_transition_lock:
            return await self._send_phase_for_context_locked(
                websocket,
                value,
                session_nonce,
                wake_generation,
                follow_up_token,
                follow_up_epoch,
                terminal_idle,
            )

    async def _send_phase_for_context_locked(
        self,
        websocket: Any,
        value: str,
        session_nonce: int,
        wake_generation: int,
        follow_up_token: Optional[int] = None,
        follow_up_epoch: Optional[int] = None,
        terminal_idle: bool = False,
    ) -> bool:
        context = _PhaseAuthorizationContext(
            websocket=websocket,
            session_nonce=session_nonce,
            wake_generation=wake_generation,
            follow_up_epoch=(
                self._request_follow_up_epoch
                if follow_up_epoch is None
                else follow_up_epoch
            ),
            follow_up_token=follow_up_token,
            terminal_idle=terminal_idle,
        )
        if not self._phase_authorization_context_is_current(context):
            return False
        payload = self._build_phase_control(
            value,
            session_nonce=session_nonce,
            wake_generation=wake_generation,
            follow_up_token=follow_up_token,
        )
        remaining = self._physical_wake_deadline - time.monotonic()
        if remaining <= 0:
            return False
        try:
            await asyncio.wait_for(
                websocket.send(json.dumps(payload, separators=(",", ":"))),
                timeout=min(1.0, remaining),
            )
        except Exception as error:
            logger.warning("Could not deliver generation-bound phase: %r", error)
            return False
        return True

    async def _send_silent_close_terminal_idle_locked(
        self,
        context: _SilentCloseContext,
    ) -> bool:
        """Send the one tokenless idle permitted after silent close commits."""
        if (
            not self._silent_close_is_current()
            or not self._physical_wake_is_current()
            or tuple(self._websockets) != (context.websocket,)
            or self._active_session_nonce != context.session_nonce
            or self._device_wake_generation != context.wake_generation
            or self._request_follow_up_answer_grant is not None
        ):
            return False
        payload = self._build_phase_control(
            "idle",
            session_nonce=context.session_nonce,
            wake_generation=context.wake_generation,
        )
        remaining = self._physical_wake_deadline - time.monotonic()
        if remaining <= 0:
            return False
        try:
            await asyncio.wait_for(
                context.websocket.send(json.dumps(payload, separators=(",", ":"))),
                timeout=min(1.0, remaining),
            )
        except Exception as error:
            logger.warning("Could not deliver silent-close terminal idle: %r", error)
            return False
        return True

    def _phase_authorization_context_is_current(
        self,
        context: _PhaseAuthorizationContext,
    ) -> bool:
        if (
            context.websocket not in self._websockets
            or context.session_nonce != self._active_session_nonce
            or context.wake_generation != self._device_wake_generation
            or context.follow_up_epoch != self._request_follow_up_epoch
            or not self._physical_wake_is_current()
            or self._silent_close_is_current()
        ):
            return False
        grant = self._open_follow_up_phase_grant
        current_follow_up_token = (
            grant.token
            if grant is not None
            and self._open_follow_up_phase_grant_is_current(grant)
            else None
        )
        if context.terminal_idle:
            return context.follow_up_token is None
        return context.follow_up_token == current_follow_up_token

    async def broadcast_phase(
        self,
        value: str,
        context: Optional[_PhaseAuthorizationContext] = None,
        preserve_output: bool = False,
    ) -> bool:
        """Send one generation-bound va_client phase to the admitted device."""
        async with self._socket_transition_lock:
            if context is None:
                context = (
                    self.capture_terminal_idle_phase_authorization_context()
                    if value == "idle"
                    else self.capture_phase_authorization_context()
                )
            if (
                context is None
                or not self._phase_authorization_context_is_current(context)
                or context.terminal_idle != (value == "idle")
            ):
                return False
            if preserve_output and (
                value != "thinking"
                or TURN_LIVENESS.in_flight <= 0
                or self._assistant_output_grant is None
                or not self._assistant_output_grant_is_current(
                    self._assistant_output_grant
                )
            ):
                return False
            if value in {"thinking", "idle"} and not preserve_output:
                retired_output = self._retire_assistant_output_grant()
                await self._settle_retired_assistant_output(retired_output)
                if not self._phase_authorization_context_is_current(context):
                    return False
            if value in {"thinking", "replying", "idle"}:
                self._device_audio_generation = None
            logger.info(f"➡️ broadcast phase '{value}' to {len(self._websockets)} device(s)")
            sent = await self._send_phase_for_context_locked(
                context.websocket,
                value,
                context.session_nonce,
                context.wake_generation,
                context.follow_up_token,
                context.follow_up_epoch,
                context.terminal_idle,
            )
            if sent:
                grant = self._open_follow_up_phase_grant
                if (
                    grant is not None
                    and self._open_follow_up_phase_grant_is_current(grant)
                    and (
                        (
                            value == "replying"
                            and context.follow_up_token == grant.token
                        )
                        or (value == "idle" and context.terminal_idle)
                    )
                ):
                    self._open_follow_up_phase_grant = None
            return sent
    
    def setup_event_handlers(
        self,
        transport: WebsocketServerTransport,
        on_client_connected_callback: Callable[[str], Awaitable[None]],
        on_client_disconnected_callback: Optional[Callable[[str], None]] = None,
        openai_service_getter: Optional[Callable[[str], Optional[OpenAIRealtimeLLMService]]] = None
    ):
        """
        Setup WebSocket event handlers.
        
        Args:
            transport: The WebSocket transport instance
            on_client_connected_callback: Async callback function(client_id) called when client connects
            on_client_disconnected_callback: Optional callback function(client_id) called when client disconnects
            openai_service_getter: Optional function(client_id) -> OpenAIRealtimeLLMService to get service for interrupt
        """
        self._on_client_disconnected_callback = on_client_disconnected_callback
        self.transport = transport

        @transport.event_handler("on_client_connected")
        async def on_client_connected(_transport: WebsocketServerTransport, websocket):
            """Handle new WebSocket client connection."""
            accepted = False
            try:
                client_id = self.extract_client_id(websocket)
                logger.info(f"🔗 New WebSocket connection from IP: {client_id}")
                # The project-owned transport invokes this only for its sole raw
                # candidate. An admitted owner is never displaced by a challenger.
                async with self._socket_transition_lock:
                    transport_has_owner_contract = hasattr(
                        _transport,
                        "admitted_websocket",
                    )
                    transport_owner = getattr(_transport, "admitted_websocket", None)
                    if transport_owner is not None:
                        logger.warning("Rejected Voice PE candidate while owner remains active")
                        await self._reject_transport_candidate(websocket)
                        return
                    if not transport_has_owner_contract and self._websockets:
                        logger.warning("Rejected Voice PE candidate without owner-safe transport")
                        await self._close_websocket(websocket)
                        return
                    # The transport may admit a replacement before an old app-level
                    # disconnect callback resumes. Its identity-owned output is
                    # already detached, so clear only stale application metadata.
                    if self._websockets:
                        graceful_context = self._graceful_close_owner_context
                        if graceful_context is not None:
                            self._clear_graceful_close_context(graceful_context)
                        retired_output = self._retire_assistant_output_grant()
                        await self._settle_retired_assistant_output(retired_output)
                        await self._cancel_retired_assistant_output(retired_output)
                        self.invalidate_request_follow_up_turn(send_cancel=False)
                        self._websockets.clear()
                        self._active_session_nonce = None
                        self._physical_wake_deadline = 0.0
                        self._request_follow_up_answer_grant = None
                        self._open_follow_up_phase_grant = None
                        self._silent_close_context = None
                        self._set_serializer_audio_admitted(False)
                    pending = self._hello_transaction
                    if pending is not None and pending.websocket is not websocket:
                        logger.warning("Rejected Voice PE candidate while hello is pending")
                        await self._reject_transport_candidate(websocket)
                        return
                    accepted = await self._start_hello(
                        websocket,
                        client_id,
                        on_client_connected_callback,
                    )
            finally:
                complete = getattr(_transport, "complete_candidate_handler", None)
                if complete is not None:
                    complete(websocket, accepted)

        @transport.event_handler("on_client_disconnected")
        async def on_client_disconnected(transport: WebsocketServerTransport, websocket, *args, **kwargs):
            """Handle client disconnection."""
            async with self._socket_transition_lock:
                was_admitted = websocket in self._websockets
                was_pending = (
                    self._hello_transaction is not None
                    and self._hello_transaction.websocket is websocket
                )
                if was_admitted or was_pending:
                    self.invalidate_request_follow_up_turn(send_cancel=False)
                    self._mark_socket_retired(websocket)
                graceful_context = self._graceful_close_owner_context
                if (
                    graceful_context is not None
                    and graceful_context.websocket is websocket
                ):
                    self._clear_graceful_close_context(graceful_context)
                if was_admitted:
                    retired_output = self._retire_assistant_output_grant()
                    await self._settle_retired_assistant_output(retired_output)
                    await self._cancel_retired_assistant_output(retired_output)
                if was_pending:
                    self._clear_hello_transaction()
                self._websockets.discard(websocket)
                self._uncertain_retired_sockets.discard(websocket)
                if was_admitted:
                    self._active_session_nonce = None
                    self._device_audio_generation = None
                    self._physical_wake_deadline = 0.0
                    self._request_follow_up_answer_grant = None
                    self._silent_close_context = None
                    self._set_serializer_audio_admitted(False)
                client_id = self.extract_client_id(websocket)
                if client_id:
                    logger.info(f"🔌 Client {client_id} disconnected")
                    if was_admitted and on_client_disconnected_callback:
                        on_client_disconnected_callback(client_id)
    
    async def cleanup(self):
        """Cleanup WebSocket handler resources."""
        self._set_serializer_audio_admitted(False)
        self._device_audio_generation = None
        self._open_follow_up_phase_grant = None
        graceful_context = self._graceful_close_owner_context
        if graceful_context is not None:
            self._clear_graceful_close_context(graceful_context)
        retired_output = self._retire_assistant_output_grant()
        hello_timeout_task = self._hello_timeout_task
        follow_up_expiry_task = self._request_follow_up_expiry_task
        self._clear_hello_transaction()
        try:
            self.invalidate_request_follow_up_turn()
        except Exception as error:
            self._enter_follow_up_fail_closed("cleanup could not schedule revocation")
            logger.warning("Follow-up cleanup entered fail-closed retirement: %r", error)
        await self._settle_retired_assistant_output(retired_output)
        await self._cancel_retired_assistant_output(retired_output)
        await asyncio.sleep(0)
        follow_up_tasks = list(self._request_follow_up_tasks)
        if (
            follow_up_expiry_task is not None
            and follow_up_expiry_task not in follow_up_tasks
        ):
            follow_up_tasks.append(follow_up_expiry_task)
        if follow_up_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*follow_up_tasks, return_exceptions=True),
                    timeout=(
                        self.REQUEST_FOLLOW_UP_SEND_TIMEOUT_S
                        + self.REQUEST_FOLLOW_UP_ACK_TIMEOUT_S
                        + self.SOCKET_CLOSE_TIMEOUT_S
                        + 0.5
                    ),
                )
            except TimeoutError:
                for task in follow_up_tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*follow_up_tasks, return_exceptions=True)
        self._request_follow_up_tasks.clear()
        self._request_follow_up_settlement_tasks.clear()
        self._request_follow_up_cancellations.clear()
        self._physical_wake_deadline = 0.0
        self._request_follow_up_answer_grant = None
        self._open_follow_up_phase_grant = None
        self._silent_close_context = None
        if hello_timeout_task is not None and not hello_timeout_task.done():
            await asyncio.gather(hello_timeout_task, return_exceptions=True)
        wedge_tasks = list(self._wedge_tasks)
        for task in wedge_tasks:
            task.cancel()
        if wedge_tasks:
            await asyncio.gather(*wedge_tasks, return_exceptions=True)
        self._wedge_tasks.clear()

        if self._connection_recovery is not None:
            await self._connection_recovery.cleanup()

        if self.runner:
            try:
                await self.runner.cancel()
            except Exception as e:
                logger.warning(f"⚠️ Error cancelling runner: {e}")

        uncertain = tuple(self._uncertain_retired_sockets)
        for websocket in uncertain:
            await self._close_websocket(websocket)
        remaining_uncertain = len(self._uncertain_retired_sockets)
        self._uncertain_retired_sockets.clear()
        if remaining_uncertain:
            logger.warning(
                "Voice PE cleanup released %d socket(s) with unconfirmed closure",
                remaining_uncertain,
            )
        
        if self.transport:
            try:
                cleanup_uncertain = getattr(
                    self.transport,
                    "cleanup_uncertain_sockets",
                    None,
                )
                if cleanup_uncertain is not None:
                    await cleanup_uncertain()
                if hasattr(self.transport, 'stop'):
                    await self.transport.stop()
            except Exception as e:
                logger.warning(f"⚠️ Error stopping transport: {e}")
