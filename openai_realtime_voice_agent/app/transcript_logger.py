"""Track assistant transcript completion without logging conversation content.

WHY: with gpt-realtime the model hears the user's audio natively and bursts the
whole spoken reply as audio. This processor observes its text frames only to emit
bounded completion metadata; the conversation content never enters production
logs.

How the text reaches us (verified against pipecat 0.0.97's *real*
`pipecat.services.openai.realtime.llm.OpenAIRealtimeLLMService` — NOT the older
`openai_realtime_beta` module, which pushes different frames):

  - Assistant reply, AUDIO modality (what we use): the service handles
    `response.output_audio_transcript.delta` and pushes a **`TTSTextFrame`** per
    chunk (NOT `LLMTextFrame` — that's only for the text modality). The whole
    response is bracketed by `LLMFullResponseStartFrame` /
    `LLMFullResponseEndFrame`. We accumulate the chunks and log one line on the
    End frame. (We also match `LLMTextFrame` so a text-modality run still logs.)
    These flow DOWNSTREAM out of the service, so the "assistant" tap sits AFTER
    it in the pipeline.

User transcripts are intentionally not logged. They are retained only in the
bounded in-memory conversation window for reconnect replay.
"""
import logging

from pipecat.frames.frames import (
    Frame,
    TTSTextFrame,
    LLMTextFrame,
    LLMFullResponseEndFrame,
)
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection

logger = logging.getLogger(__name__)


class TranscriptLogger(FrameProcessor):
    """Forward-only processor that logs assistant transcript lines."""

    def __init__(self, capture: str = "both", **kwargs):
        super().__init__(**kwargs)
        self._capture = capture
        self._assistant_chars = 0

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if self._capture in ("assistant", "both"):
            # Accumulate the reply text chunks (audio modality -> TTSTextFrame;
            # text modality -> LLMTextFrame), then log once per response on the
            # End bracket so it's one readable line instead of one per chunk.
            if isinstance(frame, (TTSTextFrame, LLMTextFrame)):
                if frame.text:
                    self._assistant_chars += len(frame.text)
            elif isinstance(frame, LLMFullResponseEndFrame):
                character_count = self._assistant_chars
                self._assistant_chars = 0
                if character_count:
                    logger.info(
                        "🤖 assistant response completed (%d characters)",
                        character_count,
                    )

        await self.push_frame(frame, direction)
