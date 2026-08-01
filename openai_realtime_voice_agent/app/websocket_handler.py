"""WebSocket handler for managing WebSocket connections and pipelines."""
import asyncio
import json
import logging
import time
import uuid
from typing import Optional, Callable, Awaitable, Dict

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

logger = logging.getLogger(__name__)

# The OpenAI Realtime API works in 24 kHz PCM16. The Voice PE firmware plays
# 24 kHz back and streams 16 kHz up. IMPORTANT: pipecat 0.0.97's websocket INPUT
# transport does NOT resample (only the OUTPUT transport does), and OpenAI
# Realtime's pcm16 input rate is hard-locked to 24000 (PCMAudioFormat.rate =
# Literal[24000]) — you cannot tell it the audio is 16 kHz. So the device's
# 16 kHz frames would be read 1.5x too fast / pitched up, garbling the whole
# transcript. The InputResampler below upsamples 16k->24k in the pipeline.
PIPELINE_SAMPLE_RATE = 24000


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

    def __init__(self, openai_service, emit_idle=None, phase_emitter=None, **kwargs):
        super().__init__(**kwargs)
        self._service = openai_service
        self._emit_idle = emit_idle  # async callable(value:str), e.g. broadcast_phase
        # Preferred idle route: PhaseEmitter.force_idle() keeps the emitter's
        # phase state consistent AND suppresses the racing `thinking` from VAD
        # stop events still in flight (observed: a raw broadcast idle was
        # overridden 400 ms later and the device sat in `thinking` with an
        # open mic for 44 s). emit_idle stays as fallback wiring.
        self._phase_emitter = phase_emitter
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
    ):
        """
        Initialize WebSocket handler.

        Args:
            host: Host address to bind to
            port: Port to listen on
            session_manager: Session manager instance
            audio_recording_service: Audio recording service instance
            follow_up_ms: How long (ms) the device should keep the mic open
                after a reply so the user can answer without a wake word. Sent to
                the device in the `hello` handshake. 0 = turn-based (no window).
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
        self._wedge_tasks: set = set()
        # Graceful close is a single-device acknowledged control transaction.
        # Old firmware and out-of-turn requests fail closed instead of silently
        # claiming that the next follow-up will be suppressed.
        self._graceful_close_lock = asyncio.Lock()
        self._graceful_close_ack = asyncio.Event()
        self._graceful_close_next_token = 1
        self._graceful_close_pending_token: Optional[int] = None
        self._graceful_close_ack_stage: Optional[str] = None
        self._graceful_close_accepted = False
        self._graceful_close_requested_generation: Optional[int] = None
        self._graceful_close_committed_token: Optional[int] = None
        # Speaker context v1 (fork): set by main.py when speaker names are
        # configured; wired to the serializer + OpenAI service in build_pipeline.
        self.speaker_probe = None
        # Voice enrollment recorder (fork): set by main.py; the serializer feeds
        # it every inbound mic frame while an enrollment session is active.
        self.enrollment_recorder = None
        self.enrollment_conductor = None
    
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

        # Create WebsocketServerTransport with WebsocketServerParams
        # The transport will start its own server automatically
        self.transport = WebsocketServerTransport(
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
        
        logger.info(f"✅ WebSocket transport created - will listen on ws://{self.host}:{self.port}/")
        return self.transport
    
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
            before_idle=self._arm_requested_graceful_close,
        )

        connection_recovery = ConnectionRecovery(
            openai_service=openai_service,
            emit_idle=self.broadcast_phase,
            phase_emitter=phase_emitter,
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
        # Left alone, the server VAD commits it as a user turn and — with
        # create_response=true — the model literally ANSWERS the word "stop"
        # ("Ik hou me stil…"). The device's local detection must therefore be
        # authoritative on the cloud side too, in two layers:
        #   1) input_audio_buffer.clear discards the not-yet-committed stop-word
        #      audio (the device closed its own mic gate in the same instant),
        #      so in the common case no turn is created at all;
        #   2) if the server VAD committed BEFORE our clear landed (tight race),
        #      OpenAI creates a response moments later anyway — so any assistant
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

        async def _on_device_interrupt():
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
            try:
                clear_event = openai_rt_events.InputAudioBufferClearEvent()
                note_clear = getattr(
                    openai_service,
                    "note_interrupt_input_clear",
                    None,
                )
                if note_clear is not None:
                    note_clear(interrupt_generation)
                await openai_service.send_client_event(clear_event)
                logger.info("🛑 device interrupt → input_audio_buffer.clear sent (drop in-flight user audio)")
            except Exception as e:
                logger.info(f"🛑 device interrupt → input_audio_buffer.clear no-op ({e!r})")
                fail_clear = getattr(
                    openai_service,
                    "fail_interrupt_input_clear",
                    None,
                )
                if fail_clear is not None:
                    await fail_clear(interrupt_generation, e)
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
                note_clear = getattr(
                    openai_service,
                    "note_unscoped_input_clear",
                    None,
                )
                if note_clear is not None:
                    note_clear()
                await openai_service.send_client_event(openai_rt_events.InputAudioBufferClearEvent())
                logger.info("🎬 device (re)connected → input_audio_buffer.clear (clean start)")
            except Exception as e:
                logger.debug(f"🎬 connect-time input clear no-op ({e!r})")
                cancel_clear = getattr(
                    openai_service,
                    "cancel_unscoped_input_clear",
                    None,
                )
                if cancel_clear is not None:
                    cancel_clear()

        async def _on_device_mic_flush():
            # The device sends {"type":"flush"} when a follow-up window times out
            # mid-stream. Drop any uncommitted partial utterance NOW, at the
            # cut-off, so a later wake can't "complete" it into a stale answer.
            # This replaced the reactive clear-on-mic-resume, which fired on
            # every wake and disturbed the server VAD → spurious garbage commits.
            # Also a turn boundary for the dangling-VAD guard: the follow-up
            # closed without speech, so any later server-VAD stop is dangling.
            phase_emitter.note_wake()
            try:
                note_clear = getattr(
                    openai_service,
                    "note_unscoped_input_clear",
                    None,
                )
                if note_clear is not None:
                    note_clear()
                await openai_service.send_client_event(openai_rt_events.InputAudioBufferClearEvent())
                logger.info("🧽 follow-up cut-off → input_audio_buffer.clear (drop partial utterance)")
            except Exception as e:
                logger.debug(f"🧽 mic-flush input clear no-op ({e!r})")
                cancel_clear = getattr(
                    openai_service,
                    "cancel_unscoped_input_clear",
                    None,
                )
                if cancel_clear is not None:
                    cancel_clear()

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
                return
            wedge_task = asyncio.create_task(_wedge_check(time.monotonic()))
            self._wedge_tasks.add(wedge_task)
            wedge_task.add_done_callback(self._wedge_tasks.discard)
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

            expiry_task = asyncio.create_task(expire_dangling_response_kill())
            self._wedge_tasks.add(expiry_task)
            expiry_task.add_done_callback(self._wedge_tasks.discard)
            reconnect_task = asyncio.create_task(
                connection_recovery.force_reconnect(
                    "dangling server VAD boundary",
                    bypass_cooldown=True,
                )
            )
            self._wedge_tasks.add(reconnect_task)
            reconnect_task.add_done_callback(self._wedge_tasks.discard)

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
            set_recovery_callback(_clear_dangling_response_kill)

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
            if self.speaker_probe is not None and self.speaker_probe.enabled:
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
                                        text=verdict_text(self.speaker_probe, label, name, f0),
                                    )],
                                )
                            )
                        )
                    except Exception as e:
                        logger.warning(f"⚠️ speaker verdict injection failed: {e!r}")

                self.speaker_probe.on_verdict = _on_speaker_verdict
                self._serializer.set_speaker_probe(self.speaker_probe)

            if self.enrollment_recorder is not None:
                self._serializer.set_enrollment_recorder(self.enrollment_recorder)
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
                await self.broadcast_json({"type": "ack"})
            self._serializer.set_first_audio_handler(_on_first_audio)

            if self.enrollment_conductor is not None:
                async def _on_device_enroll_stopped():
                    await self.enrollment_conductor.stop()
                self._serializer.set_enroll_stopped_handler(_on_device_enroll_stopped)

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

        IMPORTANT: use COMPACT separators (no space after ':' or ','). The Voice
        PE va_client does a literal substring match on `"value":"<phase>"`
        (va_client.cpp handle_text_), so the default json.dumps output
        `"value": "listening"` (with a space) would NOT match and the device
        would silently ignore every phase. Compact output `"value":"listening"`
        matches. This is what made listening/thinking/replying never reach the
        device (LED stuck idle, no-speech watchdog never cancelled).
        """
        try:
            await websocket.send(json.dumps(obj, separators=(",", ":")))
        except Exception as e:
            logger.warning(f"⚠️ Could not send {obj.get('type')} to device: {e!r}")

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
    ) -> None:
        """Prepare then commit one token-bound, drain-safe graceful close."""
        async with self._graceful_close_lock:
            token = self._graceful_close_next_token
            self._graceful_close_next_token = (token % 0x7FFFFFFF) + 1
            self._graceful_close_pending_token = token
            try:
                await self._send_graceful_close_stage(
                    "prepare_suppress_followup",
                    "prepared",
                    token,
                )
                if (
                    expected_non_close_generation is not None
                    and expected_non_close_generation
                    != TURN_LIVENESS.non_close_tool_generation
                ):
                    await self._cancel_graceful_close_token(token)
                    return
                # Track before transmission: if firmware commits but its ACK is
                # lost, a later non-close tool still knows which token to cancel.
                self._graceful_close_committed_token = token
                await self._send_graceful_close_stage(
                    "commit_suppress_followup",
                    "committed",
                    token,
                )
                if (
                    expected_non_close_generation is not None
                    and expected_non_close_generation
                    != TURN_LIVENESS.non_close_tool_generation
                ):
                    await self._cancel_graceful_close_token(token)
            finally:
                self._graceful_close_pending_token = None

    async def request_graceful_close(self) -> None:
        """Record a close request; arm it only at the final bot-stop boundary."""
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

    async def cancel_graceful_close(self) -> None:
        """Invalidate pending/committed close before another tool executes."""
        self._graceful_close_requested_generation = None
        async with self._graceful_close_lock:
            tokens = {
                token
                for token in (
                    self._graceful_close_pending_token,
                    self._graceful_close_committed_token,
                )
                if token is not None
            }
            for token in tokens:
                await self._cancel_graceful_close_token(token)

    async def _cancel_graceful_close_token(self, token: int) -> None:
        # Fail closed: callers must not execute a competing home action while
        # firmware may still be armed to suppress that action's follow-up.
        await self._broadcast_json_strict(
            {"type": "cancel_suppress_followup", "token": token}
        )
        if self._graceful_close_committed_token == token:
            self._graceful_close_committed_token = None

    async def _send_graceful_close_stage(
        self,
        message_type: str,
        expected_stage: str,
        token: int,
    ) -> None:
        self._graceful_close_accepted = False
        self._graceful_close_ack_stage = None
        self._graceful_close_ack.clear()
        logger.info(
            "Graceful close %s sent (token=%s); waiting %.1fs for Voice PE ACK",
            expected_stage,
            token,
            self.GRACEFUL_CLOSE_ACK_TIMEOUT_S,
        )
        await self._broadcast_json_strict({"type": message_type, "token": token})
        try:
            await asyncio.wait_for(
                self._graceful_close_ack.wait(),
                timeout=self.GRACEFUL_CLOSE_ACK_TIMEOUT_S,
            )
        except TimeoutError as error:
            raise RuntimeError(
                f"Voice PE did not acknowledge graceful close {expected_stage}"
            ) from error
        if not self._graceful_close_accepted:
            raise RuntimeError(
                f"Voice PE rejected graceful close {expected_stage} outside an active turn"
            )
        if self._graceful_close_ack_stage != expected_stage:
            raise RuntimeError(
                f"Voice PE returned the wrong graceful close stage: "
                f"{self._graceful_close_ack_stage}"
            )
        logger.info(
            "Graceful close %s acknowledged (token=%s)",
            expected_stage,
            token,
        )

    def _handle_graceful_close_ack(self, data: dict) -> None:
        """Accept only the ACK for the current token; ignore stale devices."""
        logger.info(
            "Graceful close ACK received: stage=%s token=%s accepted=%s pending=%s",
            data.get("stage"),
            data.get("token"),
            data.get("accepted"),
            self._graceful_close_pending_token,
        )
        if data.get("token") != self._graceful_close_pending_token:
            logger.warning("⚠️ Ignoring stale graceful-close ACK")
            return
        self._graceful_close_ack_stage = data.get("stage")
        self._graceful_close_accepted = data.get("accepted") is True
        self._graceful_close_ack.set()

    async def broadcast_bytes(self, data: bytes) -> None:
        """Send raw binary (24 kHz mono PCM16 audio) to every connected device.

        The device treats every BINARY frame as reply audio, so this pushes
        sound to the speaker outside any OpenAI response — used by the
        enrollment conductor's guidance prompts."""
        for ws in list(self._websockets):
            try:
                await ws.send(data)
            except Exception as e:
                logger.warning(f"⚠️ broadcast_bytes failed: {e!r}")

    async def broadcast_phase(self, value: str) -> None:
        """Send a va_client phase message to every connected device."""
        # TEMP instrumentation: log the broadcast + how many device sockets we
        # think are connected (was debug).
        logger.info(f"➡️ broadcast phase '{value}' to {len(self._websockets)} device(s)")
        await self.broadcast_json({"type": "phase", "value": value})
    
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
        @transport.event_handler("on_client_connected")
        async def on_client_connected(transport: WebsocketServerTransport, websocket):
            """Handle new WebSocket client connection."""
            client_id = self.extract_client_id(websocket)
            logger.info(f"🔗 New WebSocket connection from IP: {client_id}")
            # Track the raw connection so we can push phase/control TEXT frames.
            self._websockets.add(websocket)
            # Handshake ack expected by the va_client protocol (server -> device
            # "hello"). The Voice PE firmware tolerates its absence, but sending
            # it keeps both sides in lockstep with the documented protocol.
            # follow_up_ms tells the device how long to keep the mic open after a
            # reply (post-reply follow-up window); 0/absent = turn-based. Sent on
            # every connect so an add-on config change takes effect on reconnect.
            await self._send_json(
                websocket,
                {
                    "type": "hello",
                    "audio_out": "pcm",
                    "follow_up_ms": self.follow_up_ms,
                    "follow_up_open_delay_ms": self.follow_up_open_delay_ms,
                    "wake_open_delay_ms": self.wake_open_delay_ms,
                    "playback_prebuffer_ms": self.playback_prebuffer_ms,
                },
            )
            await on_client_connected_callback(client_id)

        @transport.event_handler("on_client_disconnected")
        async def on_client_disconnected(transport: WebsocketServerTransport, websocket, *args, **kwargs):
            """Handle client disconnection."""
            self._websockets.discard(websocket)
            client_id = self.extract_client_id(websocket)
            if client_id:
                logger.info(f"🔌 Client {client_id} disconnected")
                if on_client_disconnected_callback:
                    on_client_disconnected_callback(client_id)
        
        # Handle text messages from client (e.g., interrupt messages)
        @transport.event_handler("on_client_message")
        async def on_client_message(transport: WebsocketServerTransport, websocket, message):
            """Handle text messages from WebSocket client."""
            try:
                client_id = self.extract_client_id(websocket)
                
                # Try to parse as JSON
                if isinstance(message, bytes):
                    message = message.decode('utf-8')
                
                try:
                    data = json.loads(message)
                    message_type = data.get("type")
                    
                    if message_type == "interrupt":
                        logger.info(f"🛑 Interrupt received from client {client_id}")
                        
                        # Get OpenAI service for this client
                        openai_service = None
                        if openai_service_getter:
                            openai_service = openai_service_getter(client_id)
                        
                        if openai_service:
                            # Send interrupt event to OpenAI Realtime API
                            # The interrupt event tells OpenAI to stop speaking and listen for user input
                            try:
                                # Try to send interrupt event directly to the service
                                # OpenAI Realtime API expects: {"type": "response.interrupt"}
                                if hasattr(openai_service, 'send_interrupt'):
                                    await openai_service.send_interrupt()
                                    logger.info(f"✅ Interrupt sent to OpenAI service for client {client_id}")
                                elif hasattr(openai_service, 'push_event'):
                                    # Send interrupt event via push_event
                                    await openai_service.push_event({"type": "response.interrupt"})
                                    logger.info(f"✅ Interrupt event sent to OpenAI service for client {client_id}")
                                elif hasattr(openai_service, '_send_event'):
                                    # Try private method if available
                                    await openai_service._send_event({"type": "response.interrupt"})
                                    logger.info(f"✅ Interrupt sent via _send_event to OpenAI service for client {client_id}")
                                else:
                                    # Fallback: log warning
                                    logger.warning(f"⚠️ Could not find method to send interrupt to OpenAI service. Available methods: {[m for m in dir(openai_service) if not m.startswith('__')]}")
                            except Exception as e:
                                logger.error(f"❌ Error sending interrupt to OpenAI service: {e}", exc_info=True)
                        else:
                            logger.warning(f"⚠️ No OpenAI service found for client {client_id}, cannot send interrupt")
                    elif message_type == "start":
                        # va_client sends {"type":"start"} on connect. The
                        # pipeline already streams continuously with server VAD,
                        # so there's nothing to start here — just acknowledge.
                        logger.debug(f"▶️ start from client {client_id}")
                    elif message_type == "ping":
                        # Keepalive. Reply with pong on the same connection.
                        await self._send_json(websocket, {"type": "pong"})
                    elif message_type == "suppress_followup_ack":
                        self._handle_graceful_close_ack(data)
                    else:
                        logger.debug(f"📨 Received message from client {client_id}: {message_type}")
                        
                except json.JSONDecodeError:
                    logger.debug(f"📨 Received non-JSON message from client {client_id}: {message[:100]}")
                    
            except Exception as e:
                logger.error(f"❌ Error handling client message: {e}", exc_info=True)
    
    async def cleanup(self):
        """Cleanup WebSocket handler resources."""
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
        
        if self.transport:
            try:
                if hasattr(self.transport, 'stop'):
                    await self.transport.stop()
            except Exception as e:
                logger.warning(f"⚠️ Error stopping transport: {e}")
