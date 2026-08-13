"""Release, image-layout, privacy, and translation invariants."""

import copy
import hashlib
import json
import re
import runpy
import subprocess
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
RELEASE_INTEGRITY = runpy.run_path(
    str(BACKEND_ROOT / ".github" / "scripts" / "release_integrity.py")
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class ReleaseHardeningTests(unittest.TestCase):
    def test_registry_absence_classification_is_explicit_and_secret_safe(self):
        reference = "ghcr.io/example/backend-aarch64:0.22.5"
        explicit = RELEASE_INTEGRITY["explicit_manifest_not_found"]
        require_absent = RELEASE_INTEGRITY["require_manifest_absent"]
        integrity_error = RELEASE_INTEGRITY["ReleaseIntegrityError"]

        for error_text in (
            f"no such manifest: {reference}\n",
            "manifest unknown\n",
            "manifest unknown: manifest unknown\n",
            json.dumps(
                {
                    "errors": [
                        {
                            "code": "MANIFEST_UNKNOWN",
                            "message": "manifest unknown",
                        }
                    ]
                }
            ),
        ):
            with self.subTest(error_text=error_text):
                self.assertTrue(explicit(error_text, reference))
                require_absent(
                    reference,
                    runner=lambda *args, **kwargs: subprocess.CompletedProcess(
                        args=args,
                        returncode=1,
                        stderr=error_text,
                    ),
                )

        for error_text in (
            "unauthorized: bearer private-registry-token",
            "toomanyrequests: rate limit exceeded",
            "dial tcp: network is unreachable",
            "",
            f"no such manifest: {reference}\nunauthorized: private-registry-token",
            json.dumps(
                {
                    "errors": [
                        {"code": "MANIFEST_UNKNOWN"},
                        {"code": "UNAUTHORIZED"},
                    ]
                }
            ),
        ):
            with self.subTest(error_text=error_text):
                self.assertFalse(explicit(error_text, reference))
                with self.assertRaises(integrity_error) as raised:
                    require_absent(
                        reference,
                        runner=lambda *args, **kwargs: subprocess.CompletedProcess(
                            args=args,
                            returncode=1,
                            stderr=error_text,
                        ),
                    )
                self.assertNotIn("private-registry-token", str(raised.exception))

        with self.assertRaisesRegex(integrity_error, "Refusing to overwrite"):
            require_absent(
                reference,
                runner=lambda *args, **kwargs: subprocess.CompletedProcess(
                    args=args,
                    returncode=0,
                    stderr="",
                ),
            )

    def test_release_evidence_schema_and_identity_are_exact(self):
        build_evidence = RELEASE_INTEGRITY["build_evidence"]
        validate_evidence = RELEASE_INTEGRITY["validate_evidence"]
        integrity_error = RELEASE_INTEGRITY["ReleaseIntegrityError"]
        identity = {
            "version": "0.22.5",
            "source_commit": "a" * 40,
            "registry": "ghcr.io",
            "image_name": (
                "theonlyhyland/true-family-voice-realtime/"
                "openai-realtime-voice-agent"
            ),
        }
        evidence = build_evidence(
            **identity,
            digests={
                "aarch64": "sha256:" + "1" * 64,
                "amd64": "sha256:" + "2" * 64,
            },
        )

        validate_evidence(evidence, **identity)
        self.assertEqual(
            set(evidence["images"]),
            {"aarch64", "amd64"},
        )
        self.assertEqual(
            evidence["images"]["aarch64"]["version_ref"],
            "ghcr.io/theonlyhyland/true-family-voice-realtime/"
            "openai-realtime-voice-agent-aarch64:0.22.5",
        )
        self.assertEqual(
            evidence["images"]["amd64"]["source_ref"],
            "ghcr.io/theonlyhyland/true-family-voice-realtime/"
            f"openai-realtime-voice-agent-amd64:sha-{'a' * 40}",
        )

        invalid_documents = []
        extra_field = copy.deepcopy(evidence)
        extra_field["unexpected"] = True
        invalid_documents.append(extra_field)
        bad_digest = copy.deepcopy(evidence)
        bad_digest["images"]["aarch64"]["manifest_digest"] = "sha256:short"
        invalid_documents.append(bad_digest)
        moved_ref = copy.deepcopy(evidence)
        moved_ref["images"]["amd64"]["source_ref"] += "-moved"
        invalid_documents.append(moved_ref)
        missing_arch = copy.deepcopy(evidence)
        del missing_arch["images"]["amd64"]
        invalid_documents.append(missing_arch)
        for document in invalid_documents:
            with self.subTest(document=document):
                with self.assertRaises(integrity_error):
                    validate_evidence(document, **identity)

    def test_release_evidence_requires_one_exact_successful_dispatch(self):
        select_artifact = RELEASE_INTEGRITY["select_evidence_artifact"]
        validate_run = RELEASE_INTEGRITY["validate_publication_run"]
        integrity_error = RELEASE_INTEGRITY["ReleaseIntegrityError"]
        source_commit = "b" * 40
        artifact_name = (
            "true-family-voice-backend-0.22.5-release-evidence-"
            f"{source_commit}"
        )
        artifact = {
            "expired": False,
            "id": 101,
            "name": artifact_name,
            "workflow_run": {
                "head_sha": source_commit,
                "id": 202,
            },
        }
        pages = [
            {
                "artifacts": [
                    {**artifact, "expired": True, "id": 100},
                    artifact,
                ]
            }
        ]

        self.assertEqual(
            select_artifact(
                pages,
                name=artifact_name,
                source_commit=source_commit,
            ),
            (101, 202),
        )
        duplicate = copy.deepcopy(pages)
        duplicate[0]["artifacts"].append({**artifact, "id": 102})
        for invalid_pages in (
            [],
            [{"artifacts": []}],
            duplicate,
            [
                {
                    "artifacts": [
                        {
                            **artifact,
                            "workflow_run": {
                                **artifact["workflow_run"],
                                "head_sha": "c" * 40,
                            },
                        }
                    ]
                }
            ],
        ):
            with self.subTest(invalid_pages=invalid_pages):
                with self.assertRaises(integrity_error):
                    select_artifact(
                        invalid_pages,
                        name=artifact_name,
                        source_commit=source_commit,
                    )

        run = {
            "conclusion": "success",
            "event": "workflow_dispatch",
            "head_sha": source_commit,
            "id": 202,
            "path": ".github/workflows/build-addon.yml",
            "repository": {
                "full_name": "TheOnlyHyland/True-Family-Voice-Realtime"
            },
            "status": "completed",
        }
        run_identity = {
            "run_id": 202,
            "repository": "TheOnlyHyland/True-Family-Voice-Realtime",
            "source_commit": source_commit,
            "workflow_path": ".github/workflows/build-addon.yml",
        }
        validate_run(run, **run_identity)
        for field, bad_value in (
            ("id", 203),
            ("event", "push"),
            ("status", "in_progress"),
            ("conclusion", "failure"),
            ("head_sha", "c" * 40),
            ("path", ".github/workflows/other.yml"),
        ):
            invalid_run = copy.deepcopy(run)
            invalid_run[field] = bad_value
            with self.subTest(field=field):
                with self.assertRaises(integrity_error):
                    validate_run(invalid_run, **run_identity)
        wrong_repository = copy.deepcopy(run)
        wrong_repository["repository"]["full_name"] = "example/other"
        with self.assertRaises(integrity_error):
            validate_run(wrong_repository, **run_identity)

    def test_current_release_changelog_covers_candidate_hardening(self):
        changelog = _read(ADDON_ROOT / "CHANGELOG.md")
        current = changelog.split("## 0.22.6\n", 1)[1].split(
            "\n## 0.22.5\n",
            1,
        )[0]

        for term in (
            "set_living_room_tv_power",
            "switch.living_room_tv_smart_switch",
            "read-back",
            "firmware `0.20.2`",
            "f1ec219732e1015c63314b8ae7f395e4b10209eb",
        ):
            self.assertIn(term, current)

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
        self.assertIn("ADDON_VERSION: 0.22.6", workflow)
        self.assertIn("ARG BUILD_VERSION=0.22.6", dockerfile)
        self.assertIn("FIRMWARE_RELEASE_BINDING: finalized", workflow)
        self.assertNotIn("FIRMWARE_RELEASE_BINDING: pending", workflow)
        self.assertNotIn("REGRESSION_FIRMWARE_", workflow)
        self.assertNotIn("pending_firmware_contract_version", workflow)
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
            'description: "Required when publish=true, for example v0.22.6"',
            'description: "Historical pilot switch; rejected when publishing 0.22.6"',
            'test "$GITHUB_SHA" = "$SOURCE_COMMIT"',
            "Refuse immutable version or source-tag overwrite",
            "docker manifest inspect",
            "github.event_name == 'workflow_dispatch' && inputs.publish",
            "Verify release artifact was published first",
            'test "$VERSION_MANIFEST" = "$SOURCE_MANIFEST"',
            "Require finalized exact firmware release binding",
            "Checkout exact release firmware source commit",
            "Verify exact release firmware source checkout",
            "Download and verify immutable release firmware package",
            'RELEASE_URL="https://github.com/$FIRMWARE_RELEASE_REPOSITORY/releases/download/$FIRMWARE_RELEASE_VERSION"',
            "manifest.json",
            "true-family-voice-esp32s3.factory.bin",
            "true-family-voice-esp32s3.ota.bin",
            "true-family-voice-esp32s3.elf",
            "SHA256SUMS",
            'test "$FIRMWARE_RELEASE_BINDING" = "finalized"',
            'test "$FIRMWARE_RELEASE_VERSION" = "0.20.2"',
            "FIRMWARE_RELEASE_SOURCE_COMMIT: "
            "f1ec219732e1015c63314b8ae7f395e4b10209eb",
            "FIRMWARE_RELEASE_MANIFEST_SHA256: "
            "71793abf3f1a77c32e82a4ecca8c5549cf24ae7b4346599580cc919059ac4b21",
            "FIRMWARE_RELEASE_FACTORY_SHA256: "
            "d64b1619257801cc5887a96e8a6f51e39719609a822d5f31506ec5780b9db9ab",
            "FIRMWARE_RELEASE_OTA_SHA256: "
            "5e33514f1d036eb263989c8e64930c0dfdb76c2fc5e9c7062a8eaa9d10940b48",
            "FIRMWARE_RELEASE_ELF_SHA256: "
            "97ba5a12444e4ec7e7c9348f9729d025f2d698dca039f1276cecfaa493c0d1e2",
            "FIRMWARE_RELEASE_SHA256SUMS_SHA256: "
            "7b518719f1e8a30190240bd027b975deb7bad6f6d92b9fdd422ef87318a55bc8",
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

    def test_publication_evidence_gate_is_cross_run_and_digest_bound(self):
        workflow = _read(BACKEND_ROOT / ".github" / "workflows" / "build-addon.yml")
        publish_job = workflow.split("\n  publish:\n", 1)[1].split(
            "\n  release-verify:\n",
            1,
        )[0]
        release_job = workflow.split("\n  release-verify:\n", 1)[1]

        self.assertIn(
            "group: addon-${{ (github.event_name == 'release' && "
            "github.event.release.tag_name) || (github.event_name == "
            "'workflow_dispatch' && inputs.publish && inputs.release_tag) || "
            "github.ref }}",
            workflow,
        )
        self.assertIn("cancel-in-progress: false", workflow)
        self.assertEqual(
            workflow.count('- ".github/scripts/release_integrity.py"'),
            2,
        )
        self.assertIn(
            "RELEASE_EVIDENCE_ARTIFACT: "
            "true-family-voice-backend-0.22.6-release-evidence-${{ github.sha }}",
            workflow,
        )
        self.assertIn(
            "RELEASE_EVIDENCE_FILE: "
            "true-family-voice-backend-0.22.6-release-evidence.json",
            workflow,
        )
        self.assertIn("require-manifest-absent", publish_job)
        self.assertNotIn(
            'docker manifest inspect "$IMAGE:$TAG" >/dev/null 2>&1',
            publish_job,
        )
        for term in (
            "Build machine-readable backend release evidence",
            "write-evidence",
            "Validate backend release evidence before upload",
            "validate-evidence",
            "Upload immutable backend release evidence",
            "retention-days: 90",
            "aarch64_digest=",
            "amd64_digest=",
        ):
            self.assertIn(term, publish_job)

        for term in (
            "actions: read",
            "Locate exact successful publication evidence",
            "gh api --paginate",
            "select-artifact",
            "validate-publication-run",
            '--source-commit "$GITHUB_SHA"',
            '--workflow-path ".github/workflows/build-addon.yml"',
            "Download exact publication evidence by artifact and run ID",
            "artifact-ids: ${{ steps.locate-evidence.outputs.artifact_id }}",
            "run-id: ${{ steps.locate-evidence.outputs.run_id }}",
            "Validate exact publication evidence",
            "RECORDED_DIGEST=",
            'test "$VERSION_DIGEST" = "$RECORDED_DIGEST"',
            'test "$SOURCE_DIGEST" = "$RECORDED_DIGEST"',
        ):
            self.assertIn(term, release_job)
        self.assertNotIn(
            'test "$VERSION_DIGEST" = "$SOURCE_DIGEST"',
            release_job,
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
        self.assertIn("pilot_firmware_source_only=false", release)
        self.assertIn("Release-event", release)
        self.assertIn("retained for 90 days", release)
        self.assertIn("two moved tags must not validate a release", release)
        self.assertIn("repository metadata advertising 0.22.6", release)
        self.assertIn("advance to a new backend version", release)
        self.assertIn("only an explicit manifest-not-found", release)
        self.assertIn("ambiguous Docker failures abort publication", release)
        self.assertIn(
            "verification enforces the same public binding",
            release,
        )
        candidate_ci = release.index("1. Commit the exact release candidate")
        protected_publish = release.index("2. Manually dispatch")
        create_release = release.index("5. Create tag `v0.22.6`")
        release_verification = release.index("6. Wait for **Verify release")
        merge_main = release.index("7. Only after release verification passes")
        self.assertLess(candidate_ci, protected_publish)
        self.assertLess(protected_publish, create_release)
        self.assertLess(create_release, release_verification)
        self.assertLess(release_verification, merge_main)

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
        self.assertIn("firmware 0.20.2", docs)
        self.assertIn("backend 0.22.6", docs)
        self.assertIn("backend 0.22.5", docs)
        self.assertIn("0.20.6", docs)
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
