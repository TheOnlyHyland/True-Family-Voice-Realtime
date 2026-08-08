"""Standalone backend contract tests for the settled rapid-pilot firmware."""

import hashlib
import json
import os
import re
import tomllib
import unittest
from pathlib import Path
import sys


ADDON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ADDON_ROOT))

from app.protocol_json import (  # noqa: E402
    LEGACY_BACKEND_TO_DEVICE_FIELDS,
    LEGACY_DEVICE_TO_BACKEND_FIELDS,
    MAX_CONTROL_MESSAGE_BYTES,
    TRUSTED_BACKEND_TO_DEVICE_FIELDS,
    TRUSTED_DEVICE_TO_BACKEND_FIELDS,
)

FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "rapid_pilot_protocol.json"
CONTRACT = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))

DEFAULT_FIRMWARE_ROOT = Path(__file__).resolve().parents[3] / "firmware"
FIRMWARE_ROOT = Path(
    os.environ.get("TRUE_FAMILY_VOICE_FIRMWARE_ROOT", DEFAULT_FIRMWARE_ROOT)
)
COMPONENT_ROOT = FIRMWARE_ROOT / "esphome" / "components" / "va_client"
_FIRMWARE_ARTIFACT_ROOT = os.environ.get(
    "TRUE_FAMILY_VOICE_FIRMWARE_ARTIFACT_ROOT",
    "",
)
FIRMWARE_ARTIFACT_ROOT = (
    Path(_FIRMWARE_ARTIFACT_ROOT) if _FIRMWARE_ARTIFACT_ROOT else None
)
REQUIRE_EXTERNAL_FIRMWARE = (
    os.environ.get("TRUE_FAMILY_VOICE_REQUIRE_FIRMWARE_VALIDATION", "") == "1"
)


class FirmwareProtocolContractTests(unittest.TestCase):
    def test_release_metadata_and_ci_are_bound_to_the_rapid_pilot(self):
        backend_root = ADDON_ROOT.parent
        repository = json.loads(
            (backend_root / "repository.json").read_text(encoding="utf-8")
        )
        pyproject = tomllib.loads(
            (ADDON_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        )
        lock = tomllib.loads(
            (ADDON_ROOT / "poetry.lock").read_text(encoding="utf-8")
        )
        config = (ADDON_ROOT / "config.yaml").read_text(encoding="utf-8")
        workflow = (
            backend_root / ".github" / "workflows" / "build-addon.yml"
        ).read_text(encoding="utf-8")
        dockerfile = (ADDON_ROOT / "Dockerfile").read_text(encoding="utf-8")
        lock_digest = hashlib.sha256(
            (ADDON_ROOT / "poetry.lock").read_bytes()
        ).hexdigest()

        self.assertEqual(repository["addons"][0]["version"], "0.21.0")
        self.assertEqual(pyproject["tool"]["poetry"]["version"], "0.21.0")
        locked_versions = {
            package["name"]: package["version"]
            for package in lock["package"]
        }
        self.assertEqual(locked_versions["pipecat-ai"], "0.0.97")
        self.assertEqual(locked_versions["numpy"], "2.2.6")
        self.assertEqual(locked_versions["loguru"], "0.7.3")
        self.assertEqual(
            lock_digest,
            "13193c62fc95a0c05c7b6e89efe7db060b4f00438db46c83dc43a23eb1d9af15",
        )
        self.assertIn('version: "0.21.0"', config)
        self.assertIn('follow_up_listen_seconds: 0', config)
        self.assertIn("needs: test", workflow)
        self.assertIn('"poetry==$POETRY_VERSION"', workflow)
        self.assertIn('"poetry-core==$POETRY_CORE_VERSION"', workflow)
        self.assertIn("python -m unittest discover -s tests -v", workflow)
        self.assertIn("poetry sync --only main --no-root", workflow)
        self.assertIn("Checkout exact installed firmware source commit", workflow)
        self.assertIn("Download and verify immutable firmware package", workflow)
        self.assertIn("TRUE_FAMILY_VOICE_REQUIRE_FIRMWARE_VALIDATION:", workflow)
        self.assertIn("ref: ${{ env.FIRMWARE_SOURCE_COMMIT }}", workflow)
        self.assertIn("FIRMWARE_VERSION: 0.19.0", workflow)
        self.assertIn(
            "FIRMWARE_SOURCE_COMMIT: bcb3bf4cbf181397b51aa7cc5bca5cfecefc7b3a",
            workflow,
        )
        self.assertIn("pilot_firmware_source_only", workflow)
        self.assertIn("github.event_name == 'release'", workflow)
        self.assertIn(f"POETRY_LOCK_SHA256: {lock_digest}", workflow)
        self.assertIn(f"ARG POETRY_LOCK_SHA256={lock_digest}", dockerfile)
        self.assertIn(
            "poetry sync --only main --no-root --no-interaction --no-ansi",
            dockerfile,
        )
        self.assertEqual(
            dockerfile.count("COPY --from=builder /opt/venv /opt/venv"),
            1,
        )
        self.assertNotIn("pip3 install /tmp/app_build", dockerfile)

    def test_vendored_contract_is_bounded_and_explicitly_lan_only(self):
        self.assertEqual(CONTRACT["minimum_firmware_version"], "0.19.0")
        self.assertEqual(
            CONTRACT["validated_firmware_release"],
            {
                "version": "0.19.0",
                "manifest_sha256": (
                    "b9b12d87346148d5260a53d6303eb8c44ffb3cd24d6eb5c1a0017baccdc3a9d3"
                ),
                "factory_sha256": (
                    "7f0ffaeaecb861ceb342ad571501b14c6017161bbb6d90f489002ae4271f6b14"
                ),
                "ota_sha256": (
                    "68ab4263b407244d5cce05d7a81888604bd90dccfb38e93c8a63f4a55a070ad8"
                ),
                "elf_sha256": (
                    "d1f77ac2f71a6491bd750f44efa5e6bacdd977edc945b6ef20d241995e843775"
                ),
                "sha256sums_sha256": (
                    "fb4f71aebb6556ca6b6f659832943c698400f62cb9ee44bc1a10b2f5894050ce"
                ),
            },
        )
        self.assertEqual(CONTRACT["max_control_message_bytes"], 2048)
        self.assertEqual(CONTRACT["max_protocol_id"], 0x7FFFFFFF)
        self.assertEqual(
            CONTRACT["transport_security"],
            "unauthenticated_plaintext_trusted_lan_only",
        )

    def test_vendored_contract_requires_prepare_ready_media_commit_order(self):
        self.assertEqual(
            CONTRACT["follow_up_sequence"],
            [
                "request_follow_up",
                "request_follow_up_ack",
                "follow_up_ready",
                "final_media_check",
                "commit_follow_up",
                "commit_follow_up_ack",
                "microphone_open",
            ],
        )

    def test_vendored_control_shapes_match_backend_source(self):
        source = (ADDON_ROOT / "app" / "websocket_handler.py").read_text(
            encoding="utf-8"
        )
        serializer = (ADDON_ROOT / "app" / "raw_audio_serializer.py").read_text(
            encoding="utf-8"
        )
        transport = (
            ADDON_ROOT / "app" / "single_owner_websocket.py"
        ).read_text(encoding="utf-8")

        self.assertEqual(
            CONTRACT["trusted_backend_to_device_fields"],
            {key: list(value) for key, value in TRUSTED_BACKEND_TO_DEVICE_FIELDS.items()},
        )
        self.assertEqual(
            CONTRACT["legacy_backend_to_device_fields"],
            {key: list(value) for key, value in LEGACY_BACKEND_TO_DEVICE_FIELDS.items()},
        )
        self.assertEqual(
            CONTRACT["trusted_device_to_backend_fields"],
            {key: list(value) for key, value in TRUSTED_DEVICE_TO_BACKEND_FIELDS.items()},
        )
        self.assertEqual(
            CONTRACT["legacy_device_to_backend_fields"],
            {key: list(value) for key, value in LEGACY_DEVICE_TO_BACKEND_FIELDS.items()},
        )
        self.assertIn("MAX_CONTROL_MESSAGE_BYTES", serializer)
        self.assertEqual(MAX_CONTROL_MESSAGE_BYTES, 2048)
        self.assertIn("decode_protocol_object", transport)
        self.assertIn("_FollowUpStage.READY", source)
        self.assertIn("_FollowUpStage.COMMITTING", source)
        self.assertIn("_FollowUpStage.OPEN", source)
        self.assertNotIn("nonce=%s", source)
        self.assertNotIn("token=%s", source)

    def test_vendored_timing_bounds_match_backend_derivation(self):
        timing = CONTRACT["timing_ms"]
        physical_minimum_ms = (
            timing["audio_ring_bytes"]
            / (
                CONTRACT["audio"]["sample_rate_hz"]
                * CONTRACT["audio"]["sample_width_bytes"]
            )
            * 1000
            + timing["playback_prebuffer_max"]
            + timing["speaker_stop_timeout"]
            + timing["mic_send_barrier_timeout"]
            + timing["follow_up_chime_wait_timeout"]
            + timing["follow_up_open_delay_max"]
        )
        self.assertEqual(timing["backend_ready_timeout"], 59000)
        self.assertEqual(timing["backend_commit_ack_timeout"], 6000)
        self.assertGreater(timing["backend_ready_timeout"], physical_minimum_ms)
        source = (ADDON_ROOT / "app" / "websocket_handler.py").read_text(
            encoding="utf-8"
        )
        for term in (
            "FIRMWARE_AUDIO_RING_BYTES = 2 * 1024 * 1024",
            "FIRMWARE_OUTPUT_BYTES_PER_SECOND = 24000 * 2",
            "FIRMWARE_PLAYBACK_PREBUFFER_MAX_S = 2.0",
            "FIRMWARE_SPEAKER_DRAIN_TIMEOUT_S = 3.0",
            "FIRMWARE_MIC_SEND_BARRIER_TIMEOUT_S = 0.05",
            "FIRMWARE_FOLLOW_UP_CHIME_WAIT_TIMEOUT_S = 2.0",
            "FIRMWARE_FOLLOW_UP_READY_CALLBACK_TIMEOUT_S = 8.0",
            "FIRMWARE_FOLLOW_UP_COMMIT_TIMEOUT_S = 5.0",
            "math.ceil(",
        ):
            self.assertIn(term, source)

    def test_external_final_firmware_artifact_matches_vendored_release(self):
        release = CONTRACT["validated_firmware_release"]
        artifact_root = FIRMWARE_ARTIFACT_ROOT
        if artifact_root is None or not artifact_root.is_dir():
            if REQUIRE_EXTERNAL_FIRMWARE:
                self.fail("required final firmware artifact is absent")
            self.skipTest("optional final firmware artifact is not present")
        expected = {
            "manifest.json": release["manifest_sha256"],
            "true-family-voice-esp32s3.factory.bin": release["factory_sha256"],
            "true-family-voice-esp32s3.ota.bin": release["ota_sha256"],
            "true-family-voice-esp32s3.elf": release["elf_sha256"],
        }
        for filename, digest in expected.items():
            self.assertEqual(
                hashlib.sha256((artifact_root / filename).read_bytes()).hexdigest(),
                digest,
            )

        checksums = {
            filename: digest
            for line in (artifact_root / "SHA256SUMS").read_text(
                encoding="utf-8"
            ).splitlines()
            for digest, filename in (line.split(maxsplit=1),)
        }
        self.assertEqual(checksums, expected)
        self.assertEqual(
            hashlib.sha256((artifact_root / "SHA256SUMS").read_bytes()).hexdigest(),
            release["sha256sums_sha256"],
        )
        manifest = json.loads(
            (artifact_root / "manifest.json").read_text(encoding="utf-8")
        )
        self.assertEqual(manifest["version"], release["version"])
        self.assertEqual(
            manifest["builds"][0]["parts"][0]["sha256"],
            release["factory_sha256"],
        )
        self.assertEqual(
            manifest["builds"][0]["ota"]["sha256"],
            release["ota_sha256"],
        )

    def test_external_firmware_source_matches_vendored_contract(self):
        if not (COMPONENT_ROOT / "va_client.cpp").is_file():
            if REQUIRE_EXTERNAL_FIRMWARE:
                self.fail("required exact firmware source checkout is absent")
            self.skipTest("optional sibling firmware checkout is not present")
        self.assertEqual(
            (FIRMWARE_ROOT / "VERSION").read_text(encoding="utf-8").strip(),
            CONTRACT["validated_firmware_release"]["version"],
        )
        cpp = (COMPONENT_ROOT / "va_client.cpp").read_text(encoding="utf-8")
        lifecycle = (COMPONENT_ROOT / "follow_up_lifecycle.h").read_text(
            encoding="utf-8"
        )
        safety = (COMPONENT_ROOT / "follow_up_safety.h").read_text(
            encoding="utf-8"
        )
        header = (COMPONENT_ROOT / "va_client.h").read_text(encoding="utf-8")
        firmware_sources = cpp + lifecycle + safety + header

        for term in (
            'if (type == "hello")',
            'if (type == "request_follow_up")',
            'if (type == "cancel_request_follow_up")',
            'if (type == "commit_follow_up")',
            '"follow_up_ready"',
            '"commit_follow_up_ack"',
            '"client_revoke"',
            'message.has_exact({"type", "session_nonce", "wake_generation"})',
            '{"type", "value", "session_nonce", "wake_generation"}',
            "trusted_phase_matches",
            "kRequestFollowUpReadyTimeoutMs = 8000",
            "kRequestFollowUpCommitTimeoutMs = 5000",
            "kMicSendBarrierTimeoutMs = 50",
            "kFollowupOpenDelayMaxMs = 5000",
            "kSpeakerStopTimeoutMs = 3000",
            "kPlaybackPrebufferMaxMs = 2000",
            "kAudioBufBytes = 2 * 1024 * 1024",
        ):
            self.assertIn(term, firmware_sources)
        for term in (
            "FollowUpStage::PREPARED",
            "FollowUpStage::READY",
            "FollowUpStage::OPEN",
            "commit_is_safe",
            "open_follow_up_after_commit",
        ):
            self.assertIn(term, lifecycle)
        self.assertIn("kWsTextMessageMaxBytes = 2048", safety)
        self.assertIn("kProtocolTokenMax = 0x7FFFFFFF", safety)

        inbound_shapes = {
            tuple(re.findall(r'"([a-z_]+)"', fields))
            for fields in re.findall(
                r"message\.has_exact\(\s*\{([^}]*)\}\s*\)",
                cpp,
                flags=re.DOTALL,
            )
        }
        for fields in CONTRACT["trusted_backend_to_device_fields"].values():
            self.assertIn(tuple(fields), inbound_shapes)
        for fields in CONTRACT["legacy_backend_to_device_fields"].values():
            self.assertIn(tuple(fields), inbound_shapes)

        sender_ranges = {
            "button_cancel": (
                "void VaClient::send_button_cancel",
                "void VaClient::send_false_flag",
            ),
            "false_flag": (
                "void VaClient::send_false_flag",
                "bool VaClient::send_mic_flush_",
            ),
            "flush": (
                "bool VaClient::send_mic_flush_",
                "bool VaClient::send_wake_",
            ),
            "wake": (
                "bool VaClient::send_wake_",
                "bool VaClient::send_hello_ack_",
            ),
            "hello_ack": (
                "bool VaClient::send_hello_ack_",
                "bool VaClient::send_request_follow_up_ack_",
            ),
            "request_follow_up_ack": (
                "bool VaClient::send_request_follow_up_ack_",
                "bool VaClient::send_cancel_request_follow_up_ack_",
            ),
            "cancel_request_follow_up_ack": (
                "bool VaClient::send_cancel_request_follow_up_ack_",
                "bool VaClient::send_follow_up_ready_",
            ),
            "follow_up_ready": (
                "bool VaClient::send_follow_up_ready_",
                "bool VaClient::send_follow_up_commit_ack_",
            ),
            "commit_follow_up_ack": (
                "bool VaClient::send_follow_up_commit_ack_",
                "bool VaClient::send_client_revoke_",
            ),
            "client_revoke": (
                "bool VaClient::send_client_revoke_",
                "bool VaClient::send_interrupt_control_",
            ),
            "interrupt": (
                "bool VaClient::send_interrupt_control_",
                "void VaClient::clear_request_follow_up_",
            ),
            "suppress_followup_ack": (
                "void VaClient::send_graceful_close_ack_",
                "void VaClient::fire_phase_led_",
            ),
        }
        for message_type, (start, end) in sender_ranges.items():
            body = cpp[cpp.index(start) : cpp.index(end, cpp.index(start))]
            fields = []
            for field in re.findall(r'\\"([a-z_]+)\\"\s*:', body):
                if field not in fields:
                    fields.append(field)
            self.assertEqual(
                fields,
                CONTRACT["trusted_device_to_backend_fields"][message_type],
            )

        start_session = cpp[
            cpp.index("bool VaClient::start_session") : cpp.index(
                "void VaClient::open_followup_window_",
                cpp.index("bool VaClient::start_session"),
            )
        ]
        wake_steps = (
            "pending_protocol_wake_generation",
            "send_wake_",
            "record_wake_transmitted",
            "open_transmitted_wake",
        )
        positions = [start_session.index(step) for step in wake_steps]
        self.assertEqual(positions, sorted(positions))
        self.assertIn('send_client_revoke_("wake_commit_race"', start_session)


if __name__ == "__main__":
    unittest.main()
