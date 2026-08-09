"""Release, image-layout, privacy, and translation invariants."""

import hashlib
import re
import unittest
from pathlib import Path


ADDON_ROOT = Path(__file__).resolve().parents[1]
BACKEND_ROOT = ADDON_ROOT.parent
LOCK_SHA256 = "13193c62fc95a0c05c7b6e89efe7db060b4f00438db46c83dc43a23eb1d9af15"
MODEL_SHA256 = "d51abcf31717ef28162f26acb9d44dd4127c3d44c9b8624f699f3425daca8e77"
AARCH64_BASE = "sha256:4ecdb87bbf2e24c220140aedefdc521f7e3c499bc116c886f432261dc247d2f8"
AMD64_BASE = "sha256:84023298f975360b4350fe76118a1a5b117ff8f7ca5249d6398381659051aba3"
DEBIAN_SNAPSHOT = "20260801T000000Z"
MODEL_URL = (
    "https://github.com/k2-fsa/sherpa-onnx/releases/download/"
    "speaker-recongition-models/nemo_en_titanet_large.onnx"
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class ReleaseHardeningTests(unittest.TestCase):
    def test_production_image_installs_the_app_under_safe_path(self):
        dockerfile = _read(ADDON_ROOT / "Dockerfile")
        run_script = _read(ADDON_ROOT / "root" / "run.sh")

        self.assertIn("poetry build --format wheel", dockerfile)
        self.assertIn("pip install --no-deps /tmp/app_wheel/*.whl", dockerfile)
        self.assertIn("PYTHONSAFEPATH=1 /opt/venv/bin/python -P", dockerfile)
        self.assertIn("WORKDIR /opt/true-family-voice", dockerfile)
        self.assertNotIn("COPY app/ /app/", dockerfile)
        self.assertIn("exec python -m app.main", run_script)
        self.assertIn("TRUE_FAMILY_VOICE_STARTUP_SMOKE", run_script)
        self.assertIn("run_production_startup_smoke", _read(ADDON_ROOT / "app" / "main.py"))

    def test_image_inputs_are_immutable_and_runtime_verifiable(self):
        dockerfile = _read(ADDON_ROOT / "Dockerfile")
        build = _read(ADDON_ROOT / "build.yaml")
        workflow = _read(BACKEND_ROOT / ".github" / "workflows" / "build-addon.yml")

        self.assertEqual(
            hashlib.sha256((ADDON_ROOT / "poetry.lock").read_bytes()).hexdigest(),
            LOCK_SHA256,
        )
        for value in (LOCK_SHA256, MODEL_SHA256, AARCH64_BASE, AMD64_BASE):
            self.assertIn(value, workflow + dockerfile + build)
        self.assertIn("ADDON_VERSION: 0.22.0", workflow)
        self.assertIn("FIRMWARE_RELEASE_BINDING: finalized", workflow)
        self.assertNotIn("FIRMWARE_RELEASE_BINDING: pending", workflow)
        self.assertIn("BUILD_REVISION=${{ github.sha }}", workflow)
        self.assertIn("org.opencontainers.image.revision", dockerfile)
        self.assertIn("io.true-family.voice.poetry-lock-sha256", dockerfile)
        self.assertNotIn("base-debian:bookworm", dockerfile + build + workflow)
        self.assertIn("/usr/share/true-family-voice/poetry.lock", dockerfile)
        self.assertIn("sha256sum /opt/voiceprint/embedder.onnx", dockerfile)
        self.assertIn(DEBIAN_SNAPSHOT, dockerfile + workflow)
        self.assertIn(MODEL_URL, dockerfile + workflow)
        self.assertIn('requires = ["poetry-core==2.0.1"]', _read(
            ADDON_ROOT / "pyproject.toml"
        ))
        for package in (
            "python3=3.11.2-1+b1",
            "python3-pip=23.0.1+dfsg-1",
            "python3-venv=3.11.2-1+b1",
            "python3-dev=3.11.2-1+b1",
            "build-essential=12.9",
            "curl=7.88.1-10+deb12u15",
            "ffmpeg=7:5.1.9-0+deb12u1",
            "libsndfile1=1.2.0-1+deb12u1",
        ):
            self.assertIn(package, dockerfile)
        self.assertNotIn("deb.debian.org", dockerfile)
        self.assertNotIn("security.debian.org", dockerfile)

    def test_every_github_action_is_commit_pinned(self):
        workflow = _read(BACKEND_ROOT / ".github" / "workflows" / "build-addon.yml")
        action_refs = re.findall(r"^\s*uses:\s*[^\s@]+@([^\s]+)$", workflow, re.MULTILINE)

        self.assertGreaterEqual(len(action_refs), 10)
        self.assertTrue(all(re.fullmatch(r"[0-9a-f]{40}", ref) for ref in action_refs))
        self.assertNotRegex(workflow, r"uses:\s*[^\s]+@v\d")

    def test_ci_smokes_the_image_and_never_overwrites_release_tags(self):
        workflow = _read(BACKEND_ROOT / ".github" / "workflows" / "build-addon.yml")

        for term in (
            "Build exact production image once",
            "load: true",
            "Set up QEMU",
            "Smoke the content-addressed image",
            "EXPECTED_IMAGE_ID",
            "TRUE_FAMILY_VOICE_STARTUP_SMOKE=1",
            "'[\"/run.sh\"]'",
            "test ! -e /app",
            "Seal the exact smoked image artifact",
            "Upload exact smoked image artifact",
            "Download exact smoked image artifacts",
            "environment: backend-production",
            'description: "Exact 40-character candidate commit to publish"',
            'description: "Historical pilot switch; rejected when publishing 0.22.0"',
            'test "$GITHUB_SHA" = "$SOURCE_COMMIT"',
            "Refuse immutable version or source-tag overwrite",
            "docker manifest inspect",
            "github.event_name == 'workflow_dispatch' && inputs.publish",
            "Verify release artifact was published first",
            'test "$VERSION_MANIFEST" = "$SOURCE_MANIFEST"',
            "Require finalized exact firmware release binding",
            'test "$FIRMWARE_RELEASE_BINDING" = "finalized"',
            'test "$FIRMWARE_RELEASE_VERSION" = "0.20.0"',
            "FIRMWARE_RELEASE_SOURCE_COMMIT: "
            "36abf4ba861e2ca30968882311ed3b2562b47367",
            "FIRMWARE_RELEASE_MANIFEST_SHA256: "
            "09fa1bb26d032fccc496834171ebc314abbf5e08da2d68d8801210db0b006e9f",
        ):
            self.assertIn(term, workflow)
        self.assertGreaterEqual(
            workflow.count('test "$PILOT_FIRMWARE_SOURCE_ONLY" != "true"'),
            2,
        )
        self.assertNotIn(":latest", workflow)
        self.assertNotIn('test "$GITHUB_REF" = "refs/heads/main"', workflow)
        publish_job = workflow.split("\n  publish:\n", 1)[1].split(
            "\n  release-verify:\n", 1
        )[0]
        self.assertNotIn("docker/build-push-action", publish_job)
        release_job = workflow.split("\n  release-verify:\n", 1)[1]
        self.assertNotIn("docker/build-push-action", release_job)
        self.assertIn("docker image load --input", publish_job)
        self.assertIn('push: false', workflow)

        rootfs_smoke = ADDON_ROOT / "tests" / "production_rootfs_smoke.sh"
        self.assertTrue(rootfs_smoke.stat().st_mode & 0o111)
        self.assertIn(
            "PYTHONSAFEPATH=1 \"$VENV/bin/python\" -m app.main --startup-smoke",
            _read(rootfs_smoke),
        )
        self.assertIn(
            'TRUE_FAMILY_VOICE_STARTUP_SMOKE=1 bash "$ROOTFS/run.sh"',
            _read(rootfs_smoke),
        )

    def test_release_install_consumes_the_verified_architecture_image(self):
        config = _read(ADDON_ROOT / "config.yaml")
        release = _read(BACKEND_ROOT / "RELEASE.md")

        self.assertIn(
            "image: ghcr.io/theonlyhyland/true-family-voice-realtime/"
            "openai-realtime-voice-agent-{arch}",
            config,
        )
        self.assertIn("Upgrade firmware first", release)
        self.assertIn("Rollback the backend first", release)
        self.assertIn("publish=true", release)
        self.assertIn("do not merge the version bump to `main` yet", release.lower())
        self.assertIn("only after the images exist", release.lower())
        self.assertIn("only then publish the github release", release.lower())
        self.assertIn("pilot_firmware_source_only=false", release)
        self.assertIn("Release-event", release)
        self.assertIn(
            "verification enforces the same public binding",
            release,
        )

    def test_english_and_dutch_cover_every_schema_option(self):
        config = _read(ADDON_ROOT / "config.yaml")
        schema = config.split("\nschema:\n", 1)[1]
        schema_keys = set(re.findall(r"^  ([a-z0-9_]+):", schema, re.MULTILINE))

        for language in ("en", "nl"):
            translation = _read(ADDON_ROOT / "translations" / f"{language}.yaml")
            translation_keys = set(
                re.findall(r"^  ([a-z0-9_]+):\s*$", translation, re.MULTILINE)
            )
            self.assertEqual(translation_keys, schema_keys)
        self.assertIn("  announce_token: password", config)

    def test_current_docs_use_safe_rollout_and_no_enrollment_surface(self):
        docs = "\n".join(
            _read(path).lower()
            for path in (
                BACKEND_ROOT / "README.md",
                BACKEND_ROOT / "RELEASE.md",
                BACKEND_ROOT / "docs" / "getting-started.md",
                ADDON_ROOT / "DOCS.md",
                ADDON_ROOT / "CHANGELOG.md",
            )
        )
        configuration = _read(BACKEND_ROOT / "docs" / "configuration.md")

        self.assertIn("firmware first", docs)
        self.assertIn("backend first", docs)
        self.assertIn("firmware 0.20.0", docs)
        self.assertIn("firmware 0.19.0", docs)
        self.assertIn("binding is finalized", docs)
        self.assertNotIn("firmware binding is pending", docs)
        self.assertIn(
            "backend microphone enrollment is deliberately absent from the 0.22",
            configuration.lower(),
        )
        self.assertNotIn("enrollment_phrase", configuration)
        self.assertNotIn("enrollment_tts_voice", configuration)
        self.assertNotIn(
            "home assistant builds it locally",
            _read(BACKEND_ROOT / "docs" / "getting-started.md").lower(),
        )
        self.assertIn("enable_voice_memory", configuration)
        self.assertIn("empty", configuration.lower())

        adoption_docs = "\n".join(
            _read(path)
            for path in (
                BACKEND_ROOT / "README.md",
                BACKEND_ROOT / "docs" / "getting-started.md",
                BACKEND_ROOT / "docs" / "configuration.md",
                BACKEND_ROOT / "docs" / "faq.md",
                ADDON_ROOT / "README.md",
                ADDON_ROOT / "DOCS.md",
            )
        )
        self.assertIn("api_encryption_key", adoption_docs)
        self.assertNotIn('api_key: "the-API-encryption-key', adoption_docs)
        self.assertNotIn("Firmware/blob/main/esphome-builder", adoption_docs)
        self.assertIn("do not auto-discover", adoption_docs)

        support = _read(BACKEND_ROOT / ".github" / "ISSUE_TEMPLATE" / "config.yml")
        self.assertIn("TheOnlyHyland/True-Family-Voice-Realtime", support)
        self.assertNotIn("TristanBrotherton", support)

    def test_enrollment_implementation_is_absent_and_memory_defaults_off(self):
        main_source = _read(ADDON_ROOT / "app" / "main.py")
        config = _read(ADDON_ROOT / "config.yaml")

        self.assertFalse((ADDON_ROOT / "app" / "enrollment.py").exists())
        self.assertNotIn("get_enrollment_tool_definition", main_source)
        self.assertNotIn("create_enrollment_tool_handler", main_source)
        self.assertIn(
            'RESERVED_MCP_TOOL_NAMES = frozenset({"voice_enrollment"})',
            main_source,
        )
        self.assertNotIn("enrollment_phrase", config)
        self.assertNotIn("enrollment_tts_voice", config)
        self.assertIn("enable_voice_memory: false", config)
        self.assertIn("enable_voice_memory: bool", config)

    def test_sensitive_tool_content_is_absent_from_logging_calls(self):
        openclaw = _read(ADDON_ROOT / "app" / "openclaw_tool.py")
        web_search = _read(ADDON_ROOT / "app" / "web_search_tool.py")
        logging_config = _read(ADDON_ROOT / "app" / "logging_config.py")

        self.assertNotIn("e!r", openclaw)
        self.assertNotIn("web_search answer:", web_search)
        self.assertNotIn("exc_info=True", web_search)
        self.assertIn("_redact_openclaw_url", logging_config)
        self.assertIn("OPENCLAW_URL", logging_config)

    def test_audio_provenance_does_not_depend_on_private_chunk_attributes(self):
        transport = _read(ADDON_ROOT / "app" / "single_owner_websocket.py")
        main = _read(ADDON_ROOT / "app" / "main.py")

        self.assertNotIn("_true_family_output_context", main)
        self.assertIn("_single_owner_handle_audio_frame", transport)
        self.assertIn("_true_family_handle_audio_frame", transport)
        self.assertIn("_single_owner_audio_queue_put", transport)
        self.assertIn("gracefully_finish_output_audio_generation", transport)
        self.assertIn("_single_owner_write_frame", transport)
        self.assertIn("finish_assistant_output_response", _read(
            ADDON_ROOT / "app" / "websocket_handler.py"
        ))
        self.assertNotIn("sender._resampler.resample", transport)
        self.assertIn("register_output_audio_source", transport)
        self.assertIn("_true_family_chunk_contexts", transport)
        self.assertIn("expected_websocket is not owner_transport._admitted_websocket", transport)
        self.assertIn("on_audio_frame=self.register_assistant_output_frame", _read(
            ADDON_ROOT / "app" / "websocket_handler.py"
        ))


if __name__ == "__main__":
    unittest.main()
