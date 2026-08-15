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
FINAL_FIRMWARE_RELEASE = {
    "status": "finalized",
    "version": "0.20.2",
    "repository": "TheOnlyHyland/True-Family-Voice-Firmware",
    "source_commit": "f1ec219732e1015c63314b8ae7f395e4b10209eb",
    "manifest_sha256": (
        "71793abf3f1a77c32e82a4ecca8c5549cf24ae7b4346599580cc919059ac4b21"
    ),
    "factory_sha256": (
        "d64b1619257801cc5887a96e8a6f51e39719609a822d5f31506ec5780b9db9ab"
    ),
    "ota_sha256": (
        "5e33514f1d036eb263989c8e64930c0dfdb76c2fc5e9c7062a8eaa9d10940b48"
    ),
    "elf_sha256": (
        "97ba5a12444e4ec7e7c9348f9729d025f2d698dca039f1276cecfaa493c0d1e2"
    ),
    "sha256sums_sha256": (
        "7b518719f1e8a30190240bd027b975deb7bad6f6d92b9fdd422ef87318a55bc8"
    ),
}

DEFAULT_FIRMWARE_ROOT = Path(__file__).resolve().parents[3] / "firmware"
_FIRMWARE_ROOT = os.environ.get("TRUE_FAMILY_VOICE_FIRMWARE_ROOT", "")
FIRMWARE_ROOT = Path(_FIRMWARE_ROOT) if _FIRMWARE_ROOT else DEFAULT_FIRMWARE_ROOT
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
REQUIRE_EXTERNAL_FIRMWARE_SOURCE = bool(_FIRMWARE_ROOT) or REQUIRE_EXTERNAL_FIRMWARE


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

        self.assertEqual(repository["addons"][0]["version"], "0.22.9")
        self.assertEqual(pyproject["tool"]["poetry"]["version"], "0.22.9")
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
        self.assertIn('version: "0.22.9"', config)
        self.assertIn('follow_up_listen_seconds: 0', config)
        self.assertIn("needs: test", workflow)
        self.assertIn('"poetry==$POETRY_VERSION"', workflow)
        self.assertIn('"poetry-core==$POETRY_CORE_VERSION"', workflow)
        self.assertIn("python -m unittest discover -s tests -v", workflow)
        self.assertIn("poetry sync --only main --no-root", workflow)
        self.assertIn("Checkout exact release firmware source commit", workflow)
        self.assertIn("Verify exact release firmware source checkout", workflow)
        self.assertNotIn("Checkout exact regression firmware source commit", workflow)
        self.assertIn(
            "Download and verify immutable release firmware package",
            workflow,
        )
        self.assertEqual(
            workflow.count("TRUE_FAMILY_VOICE_REQUIRE_FIRMWARE_VALIDATION:"),
            1,
        )
        self.assertIn(
            "TRUE_FAMILY_VOICE_REQUIRE_FIRMWARE_VALIDATION: ${{ "
            "(github.event_name == 'release' || (github.event_name == "
            "'workflow_dispatch' && inputs.publish)) && '1' || '0' }}",
            workflow,
        )
        release_env = {
            "FIRMWARE_RELEASE_BINDING": "finalized",
            "FIRMWARE_RELEASE_VERSION": FINAL_FIRMWARE_RELEASE["version"],
            "FIRMWARE_RELEASE_REPOSITORY": FINAL_FIRMWARE_RELEASE["repository"],
            "FIRMWARE_RELEASE_SOURCE_COMMIT": FINAL_FIRMWARE_RELEASE[
                "source_commit"
            ],
            "FIRMWARE_RELEASE_MANIFEST_SHA256": FINAL_FIRMWARE_RELEASE[
                "manifest_sha256"
            ],
            "FIRMWARE_RELEASE_FACTORY_SHA256": FINAL_FIRMWARE_RELEASE[
                "factory_sha256"
            ],
            "FIRMWARE_RELEASE_OTA_SHA256": FINAL_FIRMWARE_RELEASE["ota_sha256"],
            "FIRMWARE_RELEASE_ELF_SHA256": FINAL_FIRMWARE_RELEASE["elf_sha256"],
            "FIRMWARE_RELEASE_SHA256SUMS_SHA256": FINAL_FIRMWARE_RELEASE[
                "sha256sums_sha256"
            ],
        }
        for field, value in release_env.items():
            self.assertIn(f"{field}: {value}", workflow)
        self.assertIn("ref: ${{ env.FIRMWARE_RELEASE_SOURCE_COMMIT }}", workflow)
        self.assertIn('[[ "$DIGEST" =~ ^[0-9a-f]{64}$ ]]', workflow)
        self.assertNotIn("REGRESSION_FIRMWARE_", workflow)
        source_checkout = workflow.split(
            "      - name: Checkout exact release firmware source commit\n",
            1,
        )[1].split("\n      - name:", 1)[0]
        self.assertNotRegex(source_checkout, re.compile(r"^\s*if:", re.MULTILINE))
        self.assertIn(
            "repository: ${{ env.FIRMWARE_RELEASE_REPOSITORY }}",
            source_checkout,
        )
        self.assertIn("ref: ${{ env.FIRMWARE_RELEASE_SOURCE_COMMIT }}", source_checkout)
        self.assertIn(
            'test "$(git -C firmware-candidate rev-parse HEAD)" =',
            workflow,
        )
        source_verify = workflow.split(
            "      - name: Verify exact release firmware source checkout\n",
            1,
        )[1].split("\n      - name:", 1)[0]
        self.assertNotRegex(source_verify, re.compile(r"^\s*if:", re.MULTILINE))
        self.assertIn(
            'test "$(tr -d \'\\r\\n\' < firmware-candidate/VERSION)" =',
            source_verify,
        )
        package_step = workflow.split(
            "      - name: Download and verify immutable release firmware package\n",
            1,
        )[1].split("\n      - name:", 1)[0]
        self.assertIn("github.event_name == 'release'", package_step)
        self.assertIn("inputs.publish", package_step)
        self.assertIn(
            "Require finalized exact firmware release binding",
            workflow,
        )
        normalized_workflow = re.sub(r"\\\n\s*", "", workflow)
        for field, value in release_env.items():
            self.assertIn(f'test "${field}" = "{value}"', normalized_workflow)
        self.assertIn("pilot_firmware_source_only", workflow)
        self.assertGreaterEqual(
            workflow.count('test "$PILOT_FIRMWARE_SOURCE_ONLY" != "true"'),
            2,
        )
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
        self.assertEqual(CONTRACT["backend_version"], "0.22.9")
        self.assertEqual(
            CONTRACT["firmware_release_binding"],
            FINAL_FIRMWARE_RELEASE,
        )
        self.assertNotIn("regression_firmware_version", CONTRACT)
        self.assertNotIn("regression_firmware_release", CONTRACT)
        self.assertNotIn("pending_firmware_contract_version", CONTRACT)
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
        self.assertEqual(
            CONTRACT["tool_continuation_audio_sequence"],
            [
                "response_a_done",
                "source_and_chunker_work_drained",
                "final_partial_pcm_padded_at_most_once",
                "queued_and_active_writes_drained",
                "follow_up_finalized",
                "response_b_created",
            ],
        )
        self.assertEqual(
            CONTRACT["tool_continuation_failure_sequence"],
            [
                "finish_deadline_expired",
                "response_a_generation_settled",
                "stale_socket_detached_and_closed_or_aborted",
                "response_a_grant_released",
                "recovery_without_response_b",
            ],
        )
        self.assertEqual(
            CONTRACT["phase_semantics"],
            {
                "initial_physical_values": [
                    "listening",
                    "thinking",
                    "replying",
                ],
                "follow_up_progress_values": [
                    "listening",
                    "thinking",
                    "replying",
                ],
                "terminal_value": "idle",
                "terminal_token_forbidden": True,
                "physical_wake_ceiling_ms": 120000,
            },
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
            CONTRACT["trusted_backend_to_device_fields"]["phase"],
            ["type", "value", "session_nonce", "wake_generation"],
        )
        self.assertEqual(
            CONTRACT["trusted_backend_to_device_fields"][
                "follow_up_progress_phase"
            ],
            ["type", "value", "token", "session_nonce", "wake_generation"],
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

    def test_external_release_firmware_artifact_matches_vendored_release(self):
        release = CONTRACT["firmware_release_binding"]
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
            if REQUIRE_EXTERNAL_FIRMWARE_SOURCE:
                self.fail("required exact firmware source checkout is absent")
            self.skipTest("optional sibling firmware checkout is not present")
        actual_version = (FIRMWARE_ROOT / "VERSION").read_text(
            encoding="utf-8"
        ).strip()
        expected_version = CONTRACT["firmware_release_binding"]["version"]
        if actual_version != expected_version:
            if not REQUIRE_EXTERNAL_FIRMWARE_SOURCE:
                self.skipTest(
                    "optional sibling firmware checkout does not match the release binding"
                )
            else:
                self.assertEqual(actual_version, expected_version)
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
            tuple(sorted(re.findall(r'"([a-z_]+)"', fields)))
            for fields in re.findall(
                r"message\.has_exact\(\s*\{([^}]*)\}\s*\)",
                cpp,
                flags=re.DOTALL,
            )
        }
        for fields in CONTRACT["trusted_backend_to_device_fields"].values():
            self.assertIn(tuple(sorted(fields)), inbound_shapes)
        for fields in CONTRACT["legacy_backend_to_device_fields"].values():
            self.assertIn(tuple(sorted(fields)), inbound_shapes)

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
