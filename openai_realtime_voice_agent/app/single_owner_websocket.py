"""Identity-safe single-owner adapter for Pipecat 0.0.97 WebSocket transport."""

import asyncio
import contextvars
import inspect
import logging
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

    try:
        await asyncio.wait_for(close_and_wait(), timeout=timeout_s)
    except asyncio.CancelledError:
        raise
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


def _output_audio_provenance_is_current(
    output_transport: Any,
    context: tuple[str, int],
    expected_websocket: Any,
) -> bool:
    owner_transport = output_transport._true_family_owner_transport
    return (
        not output_transport._true_family_output_failed_closed
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
    for sender in getattr(output_transport, "_media_senders", {}).values():
        _reset_sender_partial_audio(sender)
        _drain_sender_audio_queue(sender)


def _fail_output_audio_closed(output_transport: Any) -> None:
    """Retire all PCM after an internal provenance registry invariant fails."""
    output_transport._true_family_output_failed_closed = True
    output_transport._true_family_output_generation = None
    output_transport._true_family_output_websocket = None
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
    try:
        await original_put(frame)
    except BaseException:
        if contexts.get(frame_id) is provenance:
            contexts.pop(frame_id, None)
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
    provenance = output_transport._true_family_source_contexts.pop(
        getattr(frame, "id", None),
        None,
    )
    if provenance is None:
        _reset_sender_partial_audio(sender)
        return
    context, expected_websocket = provenance
    if not _output_audio_provenance_is_current(
        output_transport,
        context,
        expected_websocket,
    ):
        _reset_sender_partial_audio(sender)
        return

    provenance_token = _CURRENT_OUTPUT_AUDIO_PROVENANCE.set(provenance)
    try:
        await sender._true_family_handle_audio_frame(frame)
    except BaseException:
        _reset_sender_partial_audio(sender)
        raise
    finally:
        _CURRENT_OUTPUT_AUDIO_PROVENANCE.reset(provenance_token)

    # A generation can be retired while Pipecat awaits its resampler or queue.
    # Never leave bytes from that retired call in Pipecat's partial buffer.
    if not _output_audio_provenance_is_current(
        output_transport,
        context,
        expected_websocket,
    ):
        _reset_sender_partial_audio(sender)


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
        _patch_sender_audio_queue(sender)


async def _single_owner_write_audio_frame(output_transport: Any, frame: Any) -> bool:
    """Authorize one reconstructed chunk against its exact physical owner."""
    provenance = output_transport._true_family_chunk_contexts.pop(
        getattr(frame, "id", None),
        None,
    )
    if provenance is None:
        return False
    context, expected_websocket = provenance
    owner_transport = output_transport._true_family_owner_transport

    async with owner_transport._owner_lock:
        if (
            output_transport._true_family_output_failed_closed
            or context != output_transport._true_family_output_generation
            or expected_websocket
            is not output_transport._true_family_output_websocket
            or expected_websocket is not owner_transport._admitted_websocket
            or expected_websocket is not output_transport._websocket
        ):
            return False
        authorizer = owner_transport._output_audio_authorizer
        if authorizer is None:
            return False
        try:
            if not await authorizer(context, expected_websocket):
                return False
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning(
                "Assistant audio authorization failed (%s)",
                error.__class__.__name__,
            )
            return False

        context_token = _CURRENT_OUTPUT_AUDIO_CONTEXT.set(context)
        try:
            return await asyncio.wait_for(
                output_transport._true_family_write_audio_frame(frame),
                timeout=owner_transport.OUTPUT_WRITE_TIMEOUT_S,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            logger.warning(
                "Assistant audio write failed (%s)",
                error.__class__.__name__,
            )
            return False
        finally:
            _CURRENT_OUTPUT_AUDIO_CONTEXT.reset(context_token)


class SingleOwnerWebsocketServerTransport(WebsocketServerTransport):
    """Pipecat server transport with project-owned admission and ownership.

    A raw connection is only a candidate. It receives no pipeline output and
    may submit only one exact-shape ``hello_ack`` until ``admit_client`` binds
    it as the sole owner. A candidate cannot replace an admitted owner.
    """

    CANDIDATE_HANDLER_TIMEOUT_S = 2.0
    SOCKET_CLOSE_TIMEOUT_S = 1.0
    OUTPUT_WRITE_TIMEOUT_S = 2.0
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
            if write_audio_frame is None:
                raise RuntimeError(
                    "Unsupported Pipecat WebSocket output contract: write_audio_frame"
                )
            output_transport._true_family_write_audio_frame = write_audio_frame
            output_transport._true_family_owner_transport = self
            output_transport._true_family_source_contexts = {}
            output_transport._true_family_chunk_contexts = {}
            output_transport._true_family_output_generation = None
            output_transport._true_family_output_websocket = None
            output_transport._true_family_output_failed_closed = False
            output_transport.write_audio_frame = MethodType(
                _single_owner_write_audio_frame,
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
            or context != output_transport._true_family_output_generation
            or websocket is not output_transport._true_family_output_websocket
            or websocket is not self._admitted_websocket
        ):
            return False
        sources[frame_id] = (context, websocket)
        return True

    async def bind_output_audio_generation(
        self,
        context: tuple[str, int],
        websocket: Any,
    ) -> bool:
        """Begin one exact output generation and discard older queued PCM."""
        async with self._owner_lock:
            if (
                not isinstance(context, tuple)
                or len(context) != 2
                or not isinstance(context[0], str)
                or not context[0]
                or type(context[1]) is not int
                or context[1] <= 0
                or websocket is not self._admitted_websocket
            ):
                self.retire_output_audio_generation()
                return False
            output_transport = self.output()
            _clear_output_audio_state(output_transport)
            output_transport._true_family_output_generation = context
            output_transport._true_family_output_websocket = websocket
            output_transport._true_family_output_failed_closed = False
            return True

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
        _clear_output_audio_state(output_transport)
        return active is not None

    async def settle_output_audio_generation(
        self,
        context: Optional[tuple[str, int]] = None,
    ) -> bool:
        """Wait until an in-flight physical write can no longer cross a boundary."""
        self.retire_output_audio_generation(context)
        async with self._owner_lock:
            return self.retire_output_audio_generation(context)

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
                retired = True
                if self._output is not None and self._output._websocket is websocket:
                    self._output._websocket = None
                serializer = getattr(self._params, "serializer", None)
                setter = getattr(serializer, "set_audio_admitted", None)
                if setter is not None:
                    setter(False)
        closed = await self.close_socket(websocket)
        if not retired:
            logger.warning("Voice PE retire requested for a non-owner socket")
        return closed

    async def _on_client_disconnected(self, websocket: Any) -> bool:
        notify = False
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
                notify = True
                if self._output is not None and self._output._websocket is websocket:
                    self._output._websocket = None
                serializer = getattr(self._params, "serializer", None)
                setter = getattr(serializer, "set_audio_admitted", None)
                if setter is not None:
                    setter(False)
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
        self.retire_output_audio_generation()
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
