"""False-wake labeling tool for locally retained speaker probes."""

import logging
import os
from typing import Awaitable, Callable, Dict, TYPE_CHECKING

if TYPE_CHECKING:
    from pipecat.services.llm_service import FunctionCallParams


logger = logging.getLogger(__name__)


def get_false_alarm_tool_definition() -> Dict:
    return {
        "type": "function",
        "name": "mark_false_wake",
        "description": (
            "Mark the most recent wake as a FALSE trigger. Use when the user says "
            "the device woke by mistake, such as 'that was a false alarm', "
            "'nobody called you', or 'you weren't being spoken to'. Confirm in "
            "one short sentence."
        ),
        "parameters": {"type": "object", "properties": {}},
    }


def create_false_alarm_tool_handler() -> Callable[["FunctionCallParams"], Awaitable[None]]:
    async def false_alarm_handler(params: "FunctionCallParams") -> None:
        try:
            probes_dir = "/share/voice-probes"
            files = sorted(
                filename
                for filename in os.listdir(probes_dir)
                if filename.startswith("probe_") and filename.endswith(".wav")
            )
            if not files:
                await params.result_callback(
                    {"status": "no recent wake capture found"}
                )
                return
            latest = files[-1]
            marked = latest.replace("probe_", "falsewake_", 1)
            os.rename(
                os.path.join(probes_dir, latest),
                os.path.join(probes_dir, marked),
            )
            logger.info("🏷️ marked latest wake as false")
            try:
                from .ha_sensors import PUBLISHER

                await PUBLISHER.false_wake()
            except Exception:
                pass
            await params.result_callback(
                {
                    "status": "marked",
                    "note": "Logged as a false trigger for retraining. Confirm briefly.",
                }
            )
        except Exception:
            logger.exception("❌ mark_false_wake failed")
            await params.result_callback(
                {"error": "could not mark it; say so briefly"}
            )

    return false_alarm_handler
