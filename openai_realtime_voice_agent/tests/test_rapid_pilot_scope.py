"""Rapid-pilot feature-scope invariants."""

import unittest
from pathlib import Path


ADDON_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = ADDON_ROOT.parent


class RapidPilotScopeTests(unittest.TestCase):
    def test_backend_enrollment_implementation_and_controls_are_absent(self):
        self.assertFalse((ADDON_ROOT / "app" / "enrollment.py").exists())
        sources = "\n".join(
            path.read_text(encoding="utf-8")
            for path in (
                ADDON_ROOT / "app" / "raw_audio_serializer.py",
                ADDON_ROOT / "app" / "websocket_handler.py",
                ADDON_ROOT / "app" / "protocol_json.py",
                ADDON_ROOT / "config.yaml",
                ADDON_ROOT / "root" / "run.sh",
            )
        )
        for removed in (
            "EnrollmentRecorder",
            "EnrollmentConductor",
            "enrollment_phrase",
            "enrollment_tts_voice",
            "wake_sound_entity",
            "enroll_stopped",
            '"enroll":',
        ):
            self.assertNotIn(removed, sources)

    def test_current_docs_state_that_enrollment_is_not_available(self):
        current_docs = "\n".join(
            path.read_text(encoding="utf-8").lower()
            for path in (
                BACKEND_ROOT / "README.md",
                BACKEND_ROOT / "docs" / "configuration.md",
                BACKEND_ROOT / "docs" / "features.md",
                BACKEND_ROOT / "docs" / "faq.md",
                ADDON_ROOT / "README.md",
                ADDON_ROOT / "DOCS.md",
            )
        )
        self.assertIn("enrollment is not", current_docs)
        self.assertNotIn("enrollment coach", current_docs)
        self.assertNotIn("start enrollment", current_docs)

    def test_former_enrollment_tool_name_remains_reserved(self):
        main_source = (ADDON_ROOT / "app" / "main.py").read_text(encoding="utf-8")
        self.assertIn(
            'RESERVED_MCP_TOOL_NAMES = frozenset({"voice_enrollment"})',
            main_source,
        )


if __name__ == "__main__":
    unittest.main()
