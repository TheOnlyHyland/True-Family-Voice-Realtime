"""Identity-safe single-owner adapter for Pipecat 0.0.97 WebSocket transport."""

import asyncio
import contextvars
import inspect
import logging
import time
from dataclasses import dataclass
from types import MethodType
from typing import Any, Optional

from pipecat.frames.frames import InputAudioRawFrame, OutputAudioRawFrame
from pipecat.transports.websocket.server import WebsocketServerTransport

from .protocol_json import (
    MAX_CONTROL_MESSAGE_BYTES,
    TRUSTED_DEVICE_TO_BACKEND_FIELDS,
    decode_protocol_object,
    has_exact_fields,
)


logger = logging.getLogger(__name__)

_HELLO_ACK_FIELDS = TRUSTED_DEVICE_TO_BACKEND_FIELDS["hello_ack"]

_CURRENT_MESSAGE_WEBSOCKET: contextvars.ContextVar[Optional[Any]] = (
    contextvars.ContextVar("true_family_voice_websocket", default=None)
)
_CURRENT_OUTPUT_AUDIO_CONTEXT: contextvars.ContextVar[Optional[tuple[str, int]]] = (
    contextvars.ContextVar("true_family_voice_output_audio", default=None)
)
_CURRENT_OUTPUT_AUDIO_PROVENANCE: contextvars.ContextVar[
    Optional[tuple[tuple[str, int], Any]]
] = contextvars.ContextVar("true_family_voice_output_audio_provenance", default=None)


@dataclass(frozen=True)
class _OwnedPartialAudio:
    """Adapter-owned copy of partial PCM that Pipecat may clear while idle."""

    sender: Any
    provenance: tuple[tuple[str, int], Any]
    audio: bytes
    frame_type: type
    num_channels: int
    sample_rate: int
    destination: Any
    chunk_size: int


def current_message_websocket() -> Optional[Any]:
    """Return the physical socket whose frame is being deserialized."""
    return _CURRENT_MESSAGE_WEBSOCKET.get()


def current_output_audio_context() -> Optional[tuple[str, int]]:
    """Return the response identity whose PCM frame is being serialized."""
    return _CURRENT_OUTPUT_AUDIO_CONTEXT.get()


def _socket_reports_closed(websocket: Any) -> bool:
    """Return true only when the WebSocket exposes a physically closed state."""
    if getattr(websocket, "closed", None) is True:
        return True
    state = getattr(websocket, "state", None)
    return getattr(state, "name", None) == "CLOSED" or state == 3


def _consume_close_task_result(task: asyncio.Task) -> None:
    if task.cancelled():
        return
    try:
        task.exception()
    except Exception:
        pass


def _abort_socket_transport(websocket: Any) -> bool:
    """Best-effort synchronous abort for supported WebSocket implementations."""
    aborted = False
    owners = (
        websocket,
        getattr(websocket, "connection", None),
        getattr(websocket, "protocol", None),
    )
    seen = set()
    for owner in owners:
        transport = getattr(owner, "transport", None)
        if transport is None or id(transport) in seen:
            continue
        seen.add(id(transport))
        abort = getattr(transport, "abort", None)
        if not callable(abort):
            continue
        try:
            result = abort()
            if inspect.isawaitable(result):
                close = getattr(result, "close", None)
                if close is not None:
                    close()
                continue
            aborted = True
        except Exception as error:
            logger.warning(
                "Voice PE socket transport abort failed (%s)",
                error.__class__.__name__,
            )
    return aborted


async def _close_socket(websocket: Any, *, timeout_s: float = 1.0) -> bool:
    """Close one socket and report only an observed physical closure."""
    close = getattr(websocket, "close", None)
    if close is None:
        logger.warning("Voice PE socket has no close operation")
        return False

    waited_closed = False

    async def close_and_wait() -> None:
        nonlocal waited_closed
        result = close()
        if inspect.isawaitable(result):
            await result
        wait_closed = getattr(websocket, "wait_closed", None)
        if callable(wait_closed):
            result = wait_closed()
            if inspect.isawaitable(result):
                await result
            waited_closed = True

    close_task = asyncio.create_task(close_and_wait())
    try:
        done, pending = await asyncio.wait({close_task}, timeout=timeout_s)
    except asyncio.CancelledError:
        close_task.cancel()
        _abort_socket_transport(websocket)
        close_task.add_done_callback(_consume_close_task_result)
        raise
    if pending:
        close_task.cancel()
        aborted = _abort_socket_transport(websocket)
        done, pending = await asyncio.wait({close_task}, timeout=0.05)
        if pending:
            close_task.add_done_callback(_consume_close_task_result)
        logger.warning(
            "Voice PE socket close timed out%s",
            " and transport was aborted" if aborted else "",
        )
        return _socket_reports_closed(websocket)
    try:
        close_task.result()
    except Exception as error:
        logger.warning(
            "Voice PE socket closure could not be confirmed (%s)",
            error.__class__.__name__,
        )
        return False
    closed = waited_closed or _socket_reports_closed(websocket)
    if not closed:
        logger.warning("Voice PE socket close returned without a closed state")
    return closed


async def _single_owner_client_handler(input_transport: Any, websocket: Any) -> None:
    """Pipecat 0.0.97 client loop with identity-safe finalization.

    Upstream stores the newest socket in ``self._websocket`` and an older loop's
    finalizer later closes that shared value. This implementation closes and
    clears only its own socket and lets the parent adapter decide admission.
    """
    owner_transport = input_transport._true_family_owner_transport
    try:
        accepted = await input_transport._callbacks.on_client_connected(websocket)
    except asyncio.CancelledError:
        await owner_transport.close_socket(websocket)
        raise
    except BaseException as error:
        logger.warning(
            "Voice PE candidate handler failed (%s)",
            error.__class__.__name__,
        )
        await owner_transport.close_socket(websocket)
        return
    if accepted is not True:
        await owner_transport.close_socket(websocket)
        return

    input_transport._websocket = websocket
    if input_transport._params.session_timeout:
        monitor = input_transport._monitor_task
        if monitor is not None and not monitor.done():
            monitor.cancel()
        input_transport._monitor_task = input_transport.create_task(
            input_transport._monitor_websocket(
                websocket,
                input_transport._params.session_timeout,
            )
        )

    try:
        async for message in websocket:
            if not input_transport._params.serializer:
                continue
            if not owner_transport.message_is_admitted(websocket, message):
                continue

            source_token = _CURRENT_MESSAGE_WEBSOCKET.set(websocket)
            try:
                frame = await input_transport._params.serializer.deserialize(message)
            finally:
                _CURRENT_MESSAGE_WEBSOCKET.reset(source_token)

            if not frame:
                continue
            if isinstance(frame, InputAudioRawFrame):
                await input_transport.push_audio_frame(frame)
            else:
                await input_transport.push_frame(frame)
    except Exception as error:
        logger.info(
            "Voice PE receive loop ended (%s)",
            error.__class__.__name__,
        )
    finally:
        await input_transport._callbacks.on_client_disconnected(websocket)
        await owner_transport.close_socket(websocket)
        if input_transport._websocket is websocket:
            input_transport._websocket = None


def _reset_sender_partial_audio(sender: Any) -> None:
    sender._audio_buffer = bytearray()
    sender._true_family_partial_provenance = None
    sender._true_family_partial_frame_type = None
    sender._true_family_partial_num_channels = None


def _notify_output_audio_state(output_transport: Any) -> None:
    event = getattr(output_transport, "_true_family_audio_state_changed", None)
    if event is not None:
        event.set()


def _mark_output_audio_generation_failed(
    output_transport: Any,
    context: tuple[str, int],
) -> None:
    output_transport._true_family_failed_audio_generations.add(context)
    _notify_output_audio_state(output_transport)


def _output_audio_provenance_is_current(
    output_transport: Any,
    context: tuple[str, int],
    expected_websocket: Any,
) -> bool:
    owner_transport = output_transport._true_family_owner_transport
    return (
        not output_transport._true_family_output_failed_closed
        and context not in output_transport._true_family_failed_audio_generations
        and context == output_transport._true_family_output_generation
        and expected_websocket is output_transport._true_family_output_websocket
        and expected_websocket is owner_transport._admitted_websocket
    )


def _drain_sender_audio_queue(sender: Any) -> None:
    """Drop queued PCM while preserving non-audio lifecycle frames."""
    queue = getattr(sender, "_audio_queue", None)
    if queue is None:
        return
    retained = []
    while True:
        try:
            frame = queue.get_nowait()
        except asyncio.QueueEmpty:
            break
        queue.task_done()
        if not isinstance(frame, OutputAudioRawFrame):
            retained.append(frame)
    for frame in retained:
        queue.put_nowait(frame)


def _clear_output_audio_state(output_transport: Any) -> None:
    """Remove every queued or partial byte belonging to a retired generation."""
    output_transport._true_family_source_contexts.clear()
    output_transport._true_family_chunk_contexts.clear()
    output_transport._true_family_partial_audio.clear()
    for task in getattr(
        output_transport,
        "_true_family_active_write_tasks",
        {},
    ).values():
        if not task.done():
            task.cancel()
    for sender in getattr(output_transport, "_media_senders", {}).values():
        _reset_sender_partial_audio(sender)
        _drain_sender_audio_queue(sender)
    _notify_output_audio_state(output_transport)


def _fail_output_audio_closed(output_transport: Any) -> None:
    """Retire all PCM after an internal provenance registry invariant fails."""
    context = output_transport._true_family_output_generation
    if context is not None:
        _mark_output_audio_generation_failed(output_transport, context)
    output_transport._true_family_output_failed_closed = True
    output_transport._true_family_output_generation = None
    output_transport._true_family_output_websocket = None
    output_transport._true_family_finishing_generation = None
    output_transport._true_family_finished_generation = None
    _clear_output_audio_state(output_transport)


async def _single_owner_audio_queue_put(queue: Any, frame: Any) -> None:
    """Bind chunks reconstructed by Pipecat's real chunker outside the frame."""
    original_put = queue._true_family_put
    if not isinstance(frame, OutputAudioRawFrame):
        await original_put(frame)
        return

    provenance = _CURRENT_OUTPUT_AUDIO_PROVENANCE.get()
    if provenance is None:
        return
    context, expected_websocket = provenance
    sender = queue._true_family_sender
    output_transport = sender._transport
    if not _output_audio_provenance_is_current(
        output_transport,
        context,
        expected_websocket,
    ):
        return

    frame_id = getattr(frame, "id", None)
    contexts = output_transport._true_family_chunk_contexts
    if (
        frame_id is None
        or frame_id in contexts
        or len(contexts)
        >= output_transport._true_family_owner_transport.MAX_PENDING_AUDIO_CHUNKS
    ):
        logger.error("Assistant audio provenance registry failed closed")
        _fail_output_audio_closed(output_transport)
        return

    contexts[frame_id] = provenance
    _notify_output_audio_state(output_transport)
    try:
        await original_put(frame)
    except BaseException:
        if contexts.get(frame_id) is provenance:
            contexts.pop(frame_id, None)
            _notify_output_audio_state(output_transport)
        raise


def _patch_sender_audio_queue(sender: Any) -> None:
    """Patch every queue Pipecat creates, including interruption replacements."""
    queue = sender._audio_queue
    if getattr(queue, "_true_family_owner_patched", False):
        return
    queue._true_family_put = queue.put
    queue._true_family_sender = sender
    queue.put = MethodType(_single_owner_audio_queue_put, queue)
    queue._true_family_owner_patched = True


def _single_owner_create_audio_task(sender: Any) -> None:
    """Preserve provenance capture when Pipecat replaces its audio queue."""
    sender._true_family_create_audio_task()
    if sender._audio_task is not None:
        _patch_sender_audio_queue(sender)


async def _single_owner_handle_audio_frame(sender: Any, frame: Any) -> None:
    """Authorize a source frame, then call Pipecat's unmodified chunker."""
    output_transport = sender._transport
    frame_id = getattr(frame, "id", None)
    provenance = output_transport._true_family_source_contexts.pop(
        frame_id,
        None,
    )
    _notify_output_audio_state(output_transport)
    if provenance is None:
        output_transport._true_family_partial_audio.pop(id(sender), None)
        _reset_sender_partial_audio(sender)
        return
    context, expected_websocket = provenance
    if not _output_audio_provenance_is_current(
        output_transport,
        context,
        expected_websocket,
    ):
        output_transport._true_family_partial_audio.pop(id(sender), None)
        _reset_sender_partial_audio(sender)
        return

    owned_partial = output_transport._true_family_partial_audio.get(id(sender))
    if owned_partial is not None:
        if owned_partial.provenance != provenance:
            logger.error("Assistant partial-audio ownership changed")
            _fail_output_audio_closed(output_transport)
            _reset_sender_partial_audio(sender)
            return
        if sender._audio_buffer:
            if bytes(sender._audio_buffer) != owned_partial.audio:
                logger.error("Assistant partial-audio mirror diverged")
                _fail_output_audio_closed(output_transport)
                _reset_sender_partial_audio(sender)
                return
        else:
            sender._audio_buffer = bytearray(owned_partial.audio)
            sender._true_family_partial_provenance = provenance
            sender._true_family_partial_frame_type = owned_partial.frame_type
            sender._true_family_partial_num_channels = owned_partial.num_channels

    processing = output_transport._true_family_processing_source_contexts
    if (
        frame_id is None
        or frame_id in processing
        or len(processing)
        >= output_transport._true_family_owner_transport.MAX_PENDING_AUDIO_CHUNKS
        or sender._audio_buffer
        and sender._true_family_partial_provenance != provenance
    ):
        logger.error("Assistant audio source processing failed closed")
        _fail_output_audio_closed(output_transport)
        _reset_sender_partial_audio(sender)
        return
    processing[frame_id] = provenance
    _notify_output_audio_state(output_transport)

    provenance_token = _CURRENT_OUTPUT_AUDIO_PROVENANCE.set(provenance)
    try:
        await sender._true_family_handle_audio_frame(frame)
    except BaseException:
        _mark_output_audio_generation_failed(output_transport, context)
        output_transport._true_family_partial_audio.pop(id(sender), None)
        _reset_sender_partial_audio(sender)
        raise
    finally:
        _CURRENT_OUTPUT_AUDIO_PROVENANCE.reset(provenance_token)
        if processing.get(frame_id) is provenance:
            processing.pop(frame_id, None)
        _notify_output_audio_state(output_transport)

    # A generation can be retired while Pipecat awaits its resampler or queue.
    # Never leave bytes from that retired call in Pipecat's partial buffer.
    if not _output_audio_provenance_is_current(
        output_transport,
        context,
        expected_websocket,
    ):
        output_transport._true_family_partial_audio.pop(id(sender), None)
        _reset_sender_partial_audio(sender)
    elif sender._audio_buffer:
        sender._true_family_partial_provenance = provenance
        sender._true_family_partial_frame_type = type(frame)
        sender._true_family_partial_num_channels = getattr(
            frame,
            "num_channels",
            None,
        )
        output_transport._true_family_partial_audio[id(sender)] = _OwnedPartialAudio(
            sender=sender,
            provenance=provenance,
            audio=bytes(sender._audio_buffer),
            frame_type=type(frame),
            num_channels=getattr(frame, "num_channels", 0),
            sample_rate=sender._sample_rate,
            destination=sender._destination,
            chunk_size=sender._audio_chunk_size,
        )
    else:
        output_transport._true_family_partial_audio.pop(id(sender), None)
        _reset_sender_partial_audio(sender)
    _notify_output_audio_state(output_transport)


async def _single_owner_set_transport_ready(output_transport: Any, frame: Any) -> None:
    """Patch each pinned Pipecat media sender before it receives PCM."""
    await output_transport._true_family_set_transport_ready(frame)
    for sender in output_transport._media_senders.values():
        required = (
            "_audio_buffer",
            "_audio_queue",
            "_audio_task",
            "_create_audio_task",
            "handle_audio_frame",
        )
        missing = [name for name in required if not hasattr(sender, name)]
        if missing:
            raise RuntimeError(
                "Unsupported Pipecat media sender contract: " + ", ".join(missing)
            )
        if not getattr(sender, "_true_family_owner_patched", False):
            sender._true_family_handle_audio_frame = sender.handle_audio_frame
            sender._true_family_create_audio_task = sender._create_audio_task
            sender.handle_audio_frame = MethodType(
                _single_owner_handle_audio_frame,
                sender,
            )
            sender._create_audio_task = MethodType(
                _single_owner_create_audio_task,
                sender,
            )
            sender._true_family_owner_patched = True
        _reset_sender_partial_audio(sender)
        output_transport._true_family_partial_audio.pop(id(sender), None)
        _patch_sender_audio_queue(sender)


def _finish_active_audio_write(
    output_transport: Any,
    frame_id: Any,
    provenance: tuple[tuple[str, int], Any],
    task: asyncio.Task,
) -> None:
    if output_transport._true_family_active_write_tasks.get(frame_id) is task:
        output_transport._true_family_active_write_tasks.pop(frame_id, None)
    if output_transport._true_family_active_write_contexts.get(frame_id) == provenance:
        output_transport._true_family_active_write_contexts.pop(frame_id, None)
    if not task.cancelled():
        try:
            task.exception()
        except Exception:
            pass
    _notify_output_audio_state(output_transport)


async def _run_output_audio_write(
    output_transport: Any,
    frame: Any,
    context: tuple[str, int],
) -> bool:
    """Publish failure before task completion can release the active registry."""
    try:
        written = await output_transport._true_family_write_audio_frame(frame)
    except BaseException:
        _mark_output_audio_generation_failed(output_transport, context)
        raise
    if written is not True:
        _mark_output_audio_generation_failed(output_transport, context)
    return written is True


async def _single_owner_write_audio_frame(output_transport: Any, frame: Any) -> bool:
    """Authorize one reconstructed chunk against its exact physical owner."""
    frame_id = getattr(frame, "id", None)
    provenance = output_transport._true_family_chunk_contexts.pop(
        frame_id,
        None,
    )
    _notify_output_audio_state(output_transport)
    if provenance is None:
        return False
    context, expected_websocket = provenance
    owner_transport = output_transport._true_family_owner_transport
    active_writes = output_transport._true_family_active_write_contexts
    if frame_id is None or frame_id in active_writes:
        logger.error("Assistant audio write registry failed closed")
        _fail_output_audio_closed(output_transport)
        return False
    active_writes[frame_id] = provenance
    _notify_output_audio_state(output_transport)
    write_started = False

    try:
        async with owner_transport._owner_lock:
            if (
                output_transport._true_family_output_failed_closed
                or context in output_transport._true_family_failed_audio_generations
                or context != output_transport._true_family_output_generation
                or expected_websocket
                is not output_transport._true_family_output_websocket
                or expected_websocket is not owner_transport._admitted_websocket
                or expected_websocket is not output_transport._websocket
            ):
                _mark_output_audio_generation_failed(output_transport, context)
                return False
            authorizer = owner_transport._output_audio_authorizer
            if authorizer is None:
                _mark_output_audio_generation_failed(output_transport, context)
                return False
            try:
                if not await authorizer(context, expected_websocket):
                    _mark_output_audio_generation_failed(output_transport, context)
                    return False
            except asyncio.CancelledError:
                _mark_output_audio_generation_failed(output_transport, context)
                raise
            except Exception as error:
                logger.warning(
                    "Assistant audio authorization failed (%s)",
                    error.__class__.__name__,
                )
                _mark_output_audio_generation_failed(output_transport, context)
                return False

        if not _output_audio_provenance_is_current(
            output_transport,
            context,
            expected_websocket,
        ):
            _mark_output_audio_generation_failed(output_transport, context)
            return False

        context_token = _CURRENT_OUTPUT_AUDIO_CONTEXT.set(context)
        provenance_token = _CURRENT_OUTPUT_AUDIO_PROVENANCE.set(provenance)
        try:
            write_task = asyncio.create_task(
                _run_output_audio_write(
                    output_transport,
                    frame,
                    context,
                )
            )
        finally:
            _CURRENT_OUTPUT_AUDIO_PROVENANCE.reset(provenance_token)
            _CURRENT_OUTPUT_AUDIO_CONTEXT.reset(context_token)
        output_transport._true_family_active_write_tasks[frame_id] = write_task
        write_started = True
        write_task.add_done_callback(
            lambda task: _finish_active_audio_write(
                output_transport,
                frame_id,
                provenance,
                task,
            )
        )
        _notify_output_audio_state(output_transport)
        try:
            done, _pending = await asyncio.wait(
                {write_task},
                timeout=owner_transport.OUTPUT_WRITE_TIMEOUT_S,
            )
            if not done:
                _mark_output_audio_generation_failed(output_transport, context)
                write_task.cancel()
                return False
            written = write_task.result()
            if written is not True:
                _mark_output_audio_generation_failed(output_transport, context)
                return False
            if not _output_audio_provenance_is_current(
                output_transport,
                context,
                expected_websocket,
            ):
                _mark_output_audio_generation_failed(output_transport, context)
                return False
            return True
        except asyncio.CancelledError:
            current_task = asyncio.current_task()
            if current_task is not None and current_task.cancelling():
                if not write_task.done():
                    write_task.cancel()
                _mark_output_audio_generation_failed(output_transport, context)
                raise
            _mark_output_audio_generation_failed(output_transport, context)
            return False
        except Exception as error:
            logger.warning(
                "Assistant audio write failed (%s)",
                error.__class__.__name__,
            )
            _mark_output_audio_generation_failed(output_transport, context)
            return False
    finally:
        if not write_started and active_writes.get(frame_id) is provenance:
            active_writes.pop(frame_id, None)
        _notify_output_audio_state(output_transport)


async def _single_owner_write_frame(output_transport: Any, frame: Any) -> None:
    """Surface physical audio serialization and socket failures to the owner gate."""
    provenance = _CURRENT_OUTPUT_AUDIO_PROVENANCE.get()
    if provenance is None:
        await output_transport._true_family_write_frame(frame)
        return
    context, expected_websocket = provenance
    if (
        _CURRENT_OUTPUT_AUDIO_CONTEXT.get() != context
        or not _output_audio_provenance_is_current(
            output_transport,
            context,
            expected_websocket,
        )
        or expected_websocket is not output_transport._websocket
    ):
        raise RuntimeError("Assistant audio output ownership changed")
    serializer = output_transport._params.serializer
    if serializer is None:
        raise RuntimeError("Assistant audio output has no serializer or WebSocket")
    payload = await serializer.serialize(frame)
    if not payload:
        raise RuntimeError("Assistant audio serialization produced no payload")
    if (
        not _output_audio_provenance_is_current(
            output_transport,
            context,
            expected_websocket,
        )
        or expected_websocket is not output_transport._websocket
    ):
        raise RuntimeError("Assistant audio output ownership changed")
    await expected_websocket.send(payload)


class SingleOwnerWebsocketServerTransport(WebsocketServerTransport):
    """Pipecat server transport with project-owned admission and ownership.

    A raw connection is only a candidate. It receives no pipeline output and
    may submit only one exact-shape ``hello_ack`` until ``admit_client`` binds
    it as the sole owner. A candidate cannot replace an admitted owner.
    """

    CANDIDATE_HANDLER_TIMEOUT_S = 2.0
    SOCKET_CLOSE_TIMEOUT_S = 1.0
    OUTPUT_WRITE_TIMEOUT_S = 2.0
    OUTPUT_WRITE_SETTLE_TIMEOUT_S = 0.25
    OUTPUT_FINISH_POLL_S = 0.05
    MAX_UNCERTAIN_SOCKETS = 16
    MAX_PENDING_AUDIO_CHUNKS = 4096

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._candidate_websocket: Optional[Any] = None
        self._admitted_websocket: Optional[Any] = None
        self._candidate_handler_completion: Optional[asyncio.Future[bool]] = None
        self._uncertain_sockets: set[Any] = set()
        self._owner_lock = asyncio.Lock()
        self._output_audio_authorizer = None

    @property
    def admitted_websocket(self) -> Optional[Any]:
        return self._admitted_websocket

    @property
    def candidate_websocket(self) -> Optional[Any]:
        return self._candidate_websocket

    @property
    def uncertain_socket_count(self) -> int:
        return len(self._uncertain_sockets)

    def input(self):
        input_transport = super().input()
        if not getattr(input_transport, "_true_family_owner_patched", False):
            required = (
                "_callbacks",
                "_params",
                "_websocket",
                "_monitor_task",
                "_monitor_websocket",
                "create_task",
                "push_audio_frame",
                "push_frame",
            )
            missing = [name for name in required if not hasattr(input_transport, name)]
            if missing:
                raise RuntimeError(
                    "Unsupported Pipecat WebSocket input contract: "
                    + ", ".join(missing)
                )
            input_transport._true_family_owner_transport = self
            input_transport._client_handler = MethodType(
                _single_owner_client_handler,
                input_transport,
            )
            input_transport._true_family_owner_patched = True
        return input_transport

    def output(self):
        output_transport = super().output()
        if not getattr(output_transport, "_true_family_output_patched", False):
            write_audio_frame = getattr(output_transport, "write_audio_frame", None)
            write_frame = getattr(output_transport, "_write_frame", None)
            if write_audio_frame is None or write_frame is None:
                raise RuntimeError(
                    "Unsupported Pipecat WebSocket output contract: "
                    "write_audio_frame/_write_frame"
                )
            output_transport._true_family_write_audio_frame = write_audio_frame
            output_transport._true_family_write_frame = write_frame
            output_transport._true_family_owner_transport = self
            output_transport._true_family_source_contexts = {}
            output_transport._true_family_processing_source_contexts = {}
            output_transport._true_family_chunk_contexts = {}
            output_transport._true_family_partial_audio = {}
            output_transport._true_family_active_write_contexts = {}
            output_transport._true_family_active_write_tasks = {}
            output_transport._true_family_failed_audio_generations = set()
            output_transport._true_family_audio_state_changed = asyncio.Event()
            output_transport._true_family_audio_state_changed.set()
            output_transport._true_family_output_generation = None
            output_transport._true_family_output_websocket = None
            output_transport._true_family_output_failed_closed = False
            output_transport._true_family_finishing_generation = None
            output_transport._true_family_finished_generation = None
            output_transport.write_audio_frame = MethodType(
                _single_owner_write_audio_frame,
                output_transport,
            )
            output_transport._write_frame = MethodType(
                _single_owner_write_frame,
                output_transport,
            )
            set_transport_ready = getattr(
                output_transport,
                "set_transport_ready",
                None,
            )
            if set_transport_ready is None:
                raise RuntimeError(
                    "Unsupported Pipecat WebSocket output contract: set_transport_ready"
                )
            output_transport._true_family_set_transport_ready = set_transport_ready
            output_transport.set_transport_ready = MethodType(
                _single_owner_set_transport_ready,
                output_transport,
            )
            output_transport._true_family_output_patched = True
        return output_transport

    def set_output_audio_authorizer(self, authorizer: Any) -> None:
        """Install the final application-owned socket/wake/response gate."""
        self._output_audio_authorizer = authorizer

    def register_output_audio_source(
        self,
        frame: Any,
        context: tuple[str, int],
        websocket: Any,
    ) -> bool:
        """Bind one pre-chunk Pipecat frame to the active transport generation."""
        output_transport = self.output()
        frame_id = getattr(frame, "id", None)
        sources = output_transport._true_family_source_contexts
        registry_failed = (
            frame_id is not None
            and (
                frame_id in sources
                or len(sources) >= self.MAX_PENDING_AUDIO_CHUNKS
            )
        )
        if registry_failed:
            logger.error("Assistant audio source registry failed closed")
            _fail_output_audio_closed(output_transport)
            return False
        if (
            frame_id is None
            or output_transport._true_family_output_failed_closed
            or context in output_transport._true_family_failed_audio_generations
            or context != output_transport._true_family_output_generation
            or websocket is not output_transport._true_family_output_websocket
            or websocket is not self._admitted_websocket
        ):
            return False
        if context in {
            output_transport._true_family_finishing_generation,
            output_transport._true_family_finished_generation,
        }:
            _mark_output_audio_generation_failed(output_transport, context)
            return False
        sources[frame_id] = (context, websocket)
        _notify_output_audio_state(output_transport)
        return True

    async def bind_output_audio_generation(
        self,
        context: tuple[str, int],
        websocket: Any,
    ) -> bool:
        """Begin one exact output generation and discard older queued PCM."""
        if (
            not isinstance(context, tuple)
            or len(context) != 2
            or not isinstance(context[0], str)
            or not context[0]
            or type(context[1]) is not int
            or context[1] <= 0
        ):
            self.retire_output_audio_generation()
            return False
        output_transport = self.output()
        if (
            output_transport._true_family_output_generation is not None
            or output_transport._true_family_active_write_tasks
            or output_transport._true_family_processing_source_contexts
        ) and not await self.settle_output_audio_generation():
            return False
        async with self._owner_lock:
            if (
                websocket is not self._admitted_websocket
                or output_transport._true_family_output_generation is not None
                or output_transport._true_family_active_write_tasks
                or output_transport._true_family_processing_source_contexts
            ):
                self.retire_output_audio_generation()
                return False
            _clear_output_audio_state(output_transport)
            output_transport._true_family_failed_audio_generations.clear()
            output_transport._true_family_output_generation = context
            output_transport._true_family_output_websocket = websocket
            output_transport._true_family_output_failed_closed = False
            output_transport._true_family_finishing_generation = None
            output_transport._true_family_finished_generation = None
            _notify_output_audio_state(output_transport)
            return True

    @staticmethod
    def _generation_has_pending_state(
        output_transport: Any,
        context: tuple[str, int],
        websocket: Any,
        *registry_names: str,
    ) -> bool:
        provenance = (context, websocket)
        return any(
            provenance in getattr(output_transport, name, {}).values()
            for name in registry_names
        )

    def _finish_ownership_is_current(
        self,
        output_transport: Any,
        context: tuple[str, int],
        websocket: Any,
        ownership_is_current: Any,
    ) -> bool:
        if (
            output_transport._true_family_output_failed_closed
            or output_transport._true_family_output_generation != context
            or output_transport._true_family_output_websocket is not websocket
            or self._admitted_websocket is not websocket
            or output_transport._websocket is not websocket
        ):
            return False
        try:
            return ownership_is_current() is True
        except Exception as error:
            logger.warning(
                "Assistant audio finish ownership check failed (%s)",
                error.__class__.__name__,
            )
            return False

    async def _wait_for_output_audio_state(
        self,
        output_transport: Any,
        context: tuple[str, int],
        websocket: Any,
        ownership_is_current: Any,
        deadline: float,
        *registry_names: str,
    ) -> bool:
        while self._generation_has_pending_state(
            output_transport,
            context,
            websocket,
            *registry_names,
        ):
            if (
                context in output_transport._true_family_failed_audio_generations
                or not self._finish_ownership_is_current(
                    output_transport,
                    context,
                    websocket,
                    ownership_is_current,
                )
            ):
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            changed = output_transport._true_family_audio_state_changed
            changed.clear()
            if not self._generation_has_pending_state(
                output_transport,
                context,
                websocket,
                *registry_names,
            ):
                continue
            try:
                await asyncio.wait_for(
                    changed.wait(),
                    timeout=min(self.OUTPUT_FINISH_POLL_S, remaining),
                )
            except TimeoutError:
                pass
        return (
            context not in output_transport._true_family_failed_audio_generations
            and self._finish_ownership_is_current(
                output_transport,
                context,
                websocket,
                ownership_is_current,
            )
        )

    async def _flush_final_partial_audio(
        self,
        output_transport: Any,
        context: tuple[str, int],
        websocket: Any,
        ownership_is_current: Any,
    ) -> bool:
        provenance = (context, websocket)
        padded_chunk_sent = False
        partials = output_transport._true_family_partial_audio
        for sender_id, partial in tuple(partials.items()):
            sender = partial.sender
            if (
                padded_chunk_sent
                or partial.provenance != provenance
                or not 0 < len(partial.audio) < partial.chunk_size
                or partial.frame_type is None
                or type(partial.num_channels) is not int
                or partial.num_channels <= 0
                or sender not in output_transport._media_senders.values()
                or sender._audio_buffer
                and bytes(sender._audio_buffer) != partial.audio
                or not self._finish_ownership_is_current(
                    output_transport,
                    context,
                    websocket,
                    ownership_is_current,
                )
            ):
                return False

            audio = partial.audio.ljust(
                partial.chunk_size,
                b"\x00",
            )
            _reset_sender_partial_audio(sender)
            partials.pop(sender_id, None)
            try:
                frame = partial.frame_type(
                    audio=audio,
                    sample_rate=partial.sample_rate,
                    num_channels=partial.num_channels,
                )
                frame.transport_destination = partial.destination
                provenance_token = _CURRENT_OUTPUT_AUDIO_PROVENANCE.set(provenance)
                try:
                    await sender._audio_queue.put(frame)
                finally:
                    _CURRENT_OUTPUT_AUDIO_PROVENANCE.reset(provenance_token)
            except BaseException:
                _mark_output_audio_generation_failed(output_transport, context)
                raise
            padded_chunk_sent = True
        return True

    async def gracefully_finish_output_audio_generation(
        self,
        context: tuple[str, int],
        websocket: Any,
        ownership_is_current: Any,
        *,
        timeout_s: float,
    ) -> bool:
        """Drain one response, settling all physical work before any failure."""
        output_transport = self.output()
        if (
            not callable(ownership_is_current)
            or not isinstance(timeout_s, (int, float))
            or timeout_s <= 0
            or not self._finish_ownership_is_current(
                output_transport,
                context,
                websocket,
                ownership_is_current,
            )
            or output_transport._true_family_finishing_generation not in (None, context)
        ):
            await self.settle_output_audio_generation(context)
            return False

        deadline = time.monotonic() + timeout_s
        output_transport._true_family_finishing_generation = context
        output_transport._true_family_finished_generation = None
        _notify_output_audio_state(output_transport)
        succeeded = False
        try:
            sources_drained = await self._wait_for_output_audio_state(
                output_transport,
                context,
                websocket,
                ownership_is_current,
                deadline,
                "_true_family_source_contexts",
                "_true_family_processing_source_contexts",
            )
            if not sources_drained:
                return False
            if not await self._flush_final_partial_audio(
                output_transport,
                context,
                websocket,
                ownership_is_current,
            ):
                return False
            chunks_drained = await self._wait_for_output_audio_state(
                output_transport,
                context,
                websocket,
                ownership_is_current,
                deadline,
                "_true_family_chunk_contexts",
                "_true_family_active_write_contexts",
            )
            if not chunks_drained:
                return False
            output_transport._true_family_finished_generation = context
            succeeded = True
            return True
        finally:
            if output_transport._true_family_finishing_generation == context:
                output_transport._true_family_finishing_generation = None
            if not succeeded:
                await self.settle_output_audio_generation(context)
            _notify_output_audio_state(output_transport)

    def retire_output_audio_generation(
        self,
        context: Optional[tuple[str, int]] = None,
    ) -> bool:
        """Fail closed for one generation without clearing a newer owner."""
        output_transport = self._output
        if output_transport is None:
            return False
        active = getattr(
            output_transport,
            "_true_family_output_generation",
            None,
        )
        if context is not None and active != context:
            return False
        output_transport._true_family_output_generation = None
        output_transport._true_family_output_websocket = None
        output_transport._true_family_output_failed_closed = False
        if getattr(
            output_transport,
            "_true_family_finishing_generation",
            None,
        ) == active:
            output_transport._true_family_finishing_generation = None
        if getattr(
            output_transport,
            "_true_family_finished_generation",
            None,
        ) == active:
            output_transport._true_family_finished_generation = None
        _clear_output_audio_state(output_transport)
        return active is not None

    async def settle_output_audio_generation(
        self,
        context: Optional[tuple[str, int]] = None,
    ) -> bool:
        """Wait until an in-flight physical write can no longer cross a boundary."""
        output_transport = self._output
        if output_transport is None:
            return True
        active_context = output_transport._true_family_output_generation
        target_context = context if context is not None else active_context
        websocket = (
            output_transport._true_family_output_websocket
            if target_context == active_context
            else None
        )
        matching_tasks = []
        for frame_id, task in tuple(
            output_transport._true_family_active_write_tasks.items()
        ):
            provenance = output_transport._true_family_active_write_contexts.get(
                frame_id
            )
            if provenance is None:
                continue
            write_context, write_websocket = provenance
            if target_context is None or write_context == target_context:
                matching_tasks.append(task)
                if websocket is None:
                    websocket = write_websocket
        if websocket is None:
            for processing_context, processing_websocket in (
                output_transport._true_family_processing_source_contexts.values()
            ):
                if target_context is None or processing_context == target_context:
                    websocket = processing_websocket
                    break
        if target_context is None or target_context == active_context:
            self.retire_output_audio_generation(target_context)
        for task in matching_tasks:
            if not task.done():
                task.cancel()
        pending = set(matching_tasks)
        deadline = time.monotonic() + self.OUTPUT_WRITE_SETTLE_TIMEOUT_S
        try:
            if pending:
                _done, pending = await asyncio.wait(
                    pending,
                    timeout=max(0.0, deadline - time.monotonic()),
                )
            while True:
                processing_pending = any(
                    target_context is None or provenance[0] == target_context
                    for provenance in output_transport._true_family_processing_source_contexts.values()
                )
                if not processing_pending:
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                changed = output_transport._true_family_audio_state_changed
                changed.clear()
                if not any(
                    target_context is None or provenance[0] == target_context
                    for provenance in output_transport._true_family_processing_source_contexts.values()
                ):
                    continue
                try:
                    await asyncio.wait_for(
                        changed.wait(),
                        timeout=min(self.OUTPUT_FINISH_POLL_S, remaining),
                    )
                except TimeoutError:
                    pass
        except asyncio.CancelledError:
            if websocket is not None:
                retirement = asyncio.create_task(
                    self._retire_wedged_output_socket(websocket)
                )
                try:
                    await asyncio.shield(retirement)
                except asyncio.CancelledError:
                    await asyncio.gather(retirement, return_exceptions=True)
            raise
        processing_pending = any(
            target_context is None or provenance[0] == target_context
            for provenance in output_transport._true_family_processing_source_contexts.values()
        )
        if pending or processing_pending:
            logger.error(
                "Assistant audio work resisted cancellation; retiring its socket"
            )
            if websocket is not None:
                await self._retire_wedged_output_socket(websocket)
            return False
        await asyncio.sleep(0)
        async with self._owner_lock:
            if target_context is None:
                return not bool(
                    output_transport._true_family_active_write_contexts
                )
            return not any(
                provenance[0] == target_context
                for provenance in output_transport._true_family_active_write_contexts.values()
            )

    async def _retire_wedged_output_socket(self, websocket: Any) -> bool:
        """Detach a socket before a cancellation-resistant write can resume."""
        async with self._owner_lock:
            if self._admitted_websocket is websocket:
                self.retire_output_audio_generation()
                self._admitted_websocket = None
                if self._output is not None and self._output._websocket is websocket:
                    self._output._websocket = None
                serializer = getattr(self._params, "serializer", None)
                setter = getattr(serializer, "set_audio_admitted", None)
                if setter is not None:
                    setter(False)
        return await self.close_socket(websocket)

    async def _on_client_connected(self, websocket: Any) -> bool:
        reject = False
        completion: Optional[asyncio.Future[bool]] = None
        async with self._owner_lock:
            if self._admitted_websocket is not None:
                logger.warning(
                    "Rejected an additional Voice PE connection while an owner is admitted"
                )
                reject = True
            elif (
                self._candidate_websocket is not None
                and self._candidate_websocket is not websocket
            ):
                logger.warning(
                    "Rejected an additional Voice PE connection during hello admission"
                )
                reject = True
            else:
                self._candidate_websocket = websocket
                completion = asyncio.get_running_loop().create_future()
                self._candidate_handler_completion = completion

        if reject:
            await self.close_socket(websocket)
            return False
        if completion is None:
            await self.close_socket(websocket)
            return False

        event_name = "on_client_connected"
        event_tasks_before = self._tracked_event_tasks(event_name)
        scheduled_tasks: set[asyncio.Task[Any]] = set()
        deadline = asyncio.get_running_loop().time() + self.CANDIDATE_HANDLER_TIMEOUT_S
        try:
            await asyncio.wait_for(
                self._call_event_handler(event_name, websocket),
                timeout=max(0.001, deadline - asyncio.get_running_loop().time()),
            )
            scheduled_tasks = (
                self._tracked_event_tasks(event_name) - event_tasks_before
            )
            accepted = await asyncio.wait_for(
                asyncio.shield(completion),
                timeout=max(0.001, deadline - asyncio.get_running_loop().time()),
            )
            if scheduled_tasks:
                _done, pending = await asyncio.wait(
                    scheduled_tasks,
                    timeout=max(0.001, deadline - asyncio.get_running_loop().time()),
                )
                if pending:
                    raise asyncio.TimeoutError
        except asyncio.CancelledError:
            scheduled_tasks.update(
                self._tracked_event_tasks(event_name) - event_tasks_before
            )
            await self._cancel_event_tasks(scheduled_tasks)
            await self.reject_candidate(websocket)
            raise
        except Exception as error:
            scheduled_tasks.update(
                self._tracked_event_tasks(event_name) - event_tasks_before
            )
            await self._cancel_event_tasks(scheduled_tasks)
            logger.warning(
                "Voice PE candidate admission handler failed or timed out (%s)",
                error.__class__.__name__,
            )
            await self.reject_candidate(websocket)
            return False
        finally:
            async with self._owner_lock:
                if self._candidate_handler_completion is completion:
                    self._candidate_handler_completion = None
                if not completion.done():
                    completion.cancel()

        if accepted is not True:
            await self.reject_candidate(websocket)
            return False
        async with self._owner_lock:
            return (
                self._candidate_websocket is websocket
                and self._admitted_websocket is None
            )

    def complete_candidate_handler(self, websocket: Any, accepted: bool) -> bool:
        """Complete the scheduled Pipecat connection handler for one candidate."""
        completion = self._candidate_handler_completion
        if (
            self._candidate_websocket is not websocket
            or completion is None
            or completion.done()
        ):
            return False
        completion.set_result(accepted is True)
        return True

    def _tracked_event_tasks(self, event_name: str) -> set[asyncio.Task[Any]]:
        """Return Pipecat's currently tracked tasks for one pinned event."""
        return {
            task
            for tracked_name, task in getattr(self, "_event_tasks", set())
            if tracked_name == event_name
        }

    @staticmethod
    async def _cancel_event_tasks(tasks: set[asyncio.Task[Any]]) -> None:
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def admit_client(self, websocket: Any) -> bool:
        """Atomically promote the exact hello candidate to pipeline owner."""
        async with self._owner_lock:
            if (
                self._candidate_websocket is not websocket
                or self._admitted_websocket is not None
            ):
                return False
            output_transport = self.output()
            current_output = getattr(output_transport, "_websocket", None)
            if current_output not in (None, websocket):
                return False
            self.retire_output_audio_generation()
            self._candidate_websocket = None
            completion = self._candidate_handler_completion
            self._candidate_handler_completion = None
            if completion is not None and not completion.done():
                completion.set_result(True)
            self._admitted_websocket = websocket
            output_transport._websocket = websocket
            serializer = getattr(self._params, "serializer", None)
            setter = getattr(serializer, "set_audio_admitted", None)
            if setter is not None:
                setter(True)
            return True

    async def reject_candidate(self, websocket: Any) -> bool:
        """Forget and close one failed, never-admitted hello candidate."""
        async with self._owner_lock:
            if self._candidate_websocket is websocket:
                self._candidate_websocket = None
                completion = self._candidate_handler_completion
                self._candidate_handler_completion = None
                if completion is not None and not completion.done():
                    completion.set_result(False)
        return await self.close_socket(websocket)

    async def retire_client(self, websocket: Any) -> bool:
        """Detach output from one exact owner before closing it."""
        settle_output = False
        retired = False
        async with self._owner_lock:
            if self._candidate_websocket is websocket:
                self._candidate_websocket = None
                retired = True
                completion = self._candidate_handler_completion
                self._candidate_handler_completion = None
                if completion is not None and not completion.done():
                    completion.set_result(False)
            if self._admitted_websocket is websocket:
                self.retire_output_audio_generation()
                self._admitted_websocket = None
                settle_output = True
                retired = True
                if self._output is not None and self._output._websocket is websocket:
                    self._output._websocket = None
                serializer = getattr(self._params, "serializer", None)
                setter = getattr(serializer, "set_audio_admitted", None)
                if setter is not None:
                    setter(False)
        if settle_output:
            await self.settle_output_audio_generation()
        closed = await self.close_socket(websocket)
        if not retired:
            logger.warning("Voice PE retire requested for a non-owner socket")
        return closed

    async def _on_client_disconnected(self, websocket: Any) -> bool:
        notify = False
        settle_output = False
        async with self._owner_lock:
            if self._candidate_websocket is websocket:
                self._candidate_websocket = None
                completion = self._candidate_handler_completion
                self._candidate_handler_completion = None
                if completion is not None and not completion.done():
                    completion.set_result(False)
                notify = True
            if self._admitted_websocket is websocket:
                self.retire_output_audio_generation()
                self._admitted_websocket = None
                settle_output = True
                notify = True
                if self._output is not None and self._output._websocket is websocket:
                    self._output._websocket = None
                serializer = getattr(self._params, "serializer", None)
                setter = getattr(serializer, "set_audio_admitted", None)
                if setter is not None:
                    setter(False)
        if settle_output:
            await self.settle_output_audio_generation()
        if notify:
            await self._call_event_handler("on_client_disconnected", websocket)
        return notify

    async def close_socket(self, websocket: Any) -> bool:
        """Close and quarantine one socket whose physical close is uncertain."""
        if _socket_reports_closed(websocket):
            self._uncertain_sockets.discard(websocket)
            return True
        try:
            closed = await _close_socket(
                websocket,
                timeout_s=self.SOCKET_CLOSE_TIMEOUT_S,
            )
        except asyncio.CancelledError:
            self._remember_uncertain_socket(websocket)
            raise
        if closed:
            self._uncertain_sockets.discard(websocket)
        else:
            self._remember_uncertain_socket(websocket)
        return closed

    def _remember_uncertain_socket(self, websocket: Any) -> None:
        if websocket in self._uncertain_sockets:
            return
        if len(self._uncertain_sockets) >= self.MAX_UNCERTAIN_SOCKETS:
            logger.error("Voice PE transport uncertain-socket quarantine is full")
            return
        self._uncertain_sockets.add(websocket)

    async def cleanup_uncertain_sockets(self) -> None:
        """Retry bounded uncertain closes, then release all shutdown references."""
        await self.settle_output_audio_generation()
        uncertain = tuple(self._uncertain_sockets)
        for websocket in uncertain:
            await self.close_socket(websocket)
        remaining = len(self._uncertain_sockets)
        self._uncertain_sockets.clear()
        if remaining:
            logger.warning(
                "Voice PE shutdown released %d socket(s) with unconfirmed closure",
                remaining,
            )

    def message_is_admitted(self, websocket: Any, message: Any) -> bool:
        """Apply the first inbound gate before the shared serializer runs."""
        if websocket is self._admitted_websocket:
            return True
        if websocket is not self._candidate_websocket or not isinstance(message, str):
            return False
        if len(message.encode("utf-8")) > MAX_CONTROL_MESSAGE_BYTES:
            return False
        try:
            payload = decode_protocol_object(message)
        except (TypeError, ValueError):
            return False
        return (
            has_exact_fields(payload, _HELLO_ACK_FIELDS)
            and payload.get("type") == "hello_ack"
        )
