#!/usr/bin/env python3
"""Fail-closed registry and release-evidence checks for backend publication."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable


EVIDENCE_SCHEMA = "true-family-voice-backend-release-evidence/v1"
ARCHITECTURES = ("aarch64", "amd64")
SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
SOURCE_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")


class ReleaseIntegrityError(RuntimeError):
    """A release identity could not be proven exactly."""


def explicit_manifest_not_found(error_text: str, reference: str) -> bool:
    """Accept only an unambiguous registry manifest-not-found response."""
    normalized = error_text.replace("\r\n", "\n").strip()
    if normalized in {
        f"no such manifest: {reference}",
        "manifest unknown",
        "manifest unknown: manifest unknown",
    }:
        return True
    try:
        payload = json.loads(normalized)
    except (json.JSONDecodeError, TypeError):
        return False
    if not isinstance(payload, dict) or set(payload) != {"errors"}:
        return False
    errors = payload["errors"]
    return bool(
        isinstance(errors, list)
        and errors
        and all(
            isinstance(error, dict)
            and error.get("code") == "MANIFEST_UNKNOWN"
            for error in errors
        )
    )


def require_manifest_absent(
    reference: str,
    *,
    runner: Callable[..., subprocess.CompletedProcess[Any]] | None = None,
) -> None:
    """Prove one registry reference is absent without exposing Docker errors."""
    run = runner or subprocess.run
    docker = os.environ.get("RELEASE_INTEGRITY_DOCKER", "docker")
    try:
        result = run(
            [docker, "manifest", "inspect", reference],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
        )
    except OSError as error:
        raise ReleaseIntegrityError(
            f"Registry lookup could not run for {reference}; refusing publication"
        ) from error
    if result.returncode == 0:
        raise ReleaseIntegrityError(
            f"Refusing to overwrite existing immutable tag {reference}"
        )
    if explicit_manifest_not_found(result.stderr or "", reference):
        return
    raise ReleaseIntegrityError(
        f"Registry lookup did not explicitly prove {reference} absent; "
        "refusing publication"
    )


def select_evidence_artifact(
    pages: Any,
    *,
    name: str,
    source_commit: str,
) -> tuple[int, int]:
    """Select exactly one non-expired artifact for the release commit."""
    if not isinstance(pages, list) or not pages:
        raise ReleaseIntegrityError("Release evidence artifact pages are invalid")
    artifacts = []
    for page in pages:
        if not isinstance(page, dict) or not isinstance(page.get("artifacts"), list):
            raise ReleaseIntegrityError("Release evidence artifact page is invalid")
        artifacts.extend(page["artifacts"])
    candidates = [
        artifact
        for artifact in artifacts
        if isinstance(artifact, dict)
        and artifact.get("name") == name
        and artifact.get("expired") is False
    ]
    if len(candidates) != 1:
        raise ReleaseIntegrityError(
            "Expected exactly one non-expired backend release evidence artifact"
        )
    artifact = candidates[0]
    artifact_id = artifact.get("id")
    workflow_run = artifact.get("workflow_run")
    if (
        type(artifact_id) is not int
        or artifact_id <= 0
        or not isinstance(workflow_run, dict)
        or type(workflow_run.get("id")) is not int
        or workflow_run["id"] <= 0
        or workflow_run.get("head_sha") != source_commit
        or not SOURCE_COMMIT_RE.fullmatch(source_commit)
    ):
        raise ReleaseIntegrityError(
            "Release evidence artifact identity does not match the release commit"
        )
    return artifact_id, workflow_run["id"]


def validate_publication_run(
    run: Any,
    *,
    run_id: int,
    repository: str,
    source_commit: str,
    workflow_path: str,
) -> None:
    """Require a successful dispatch of this workflow at the release commit."""
    if not isinstance(run, dict) or not isinstance(run.get("repository"), dict):
        raise ReleaseIntegrityError("Publication workflow run record is invalid")
    if (
        run.get("id") != run_id
        or run["repository"].get("full_name") != repository
        or run.get("event") != "workflow_dispatch"
        or run.get("status") != "completed"
        or run.get("conclusion") != "success"
        or run.get("head_sha") != source_commit
        or run.get("path") != workflow_path
        or not SOURCE_COMMIT_RE.fullmatch(source_commit)
    ):
        raise ReleaseIntegrityError(
            "Release evidence did not come from the exact successful publication run"
        )


def _image_references(
    registry: str,
    image_name: str,
    architecture: str,
    version: str,
    source_commit: str,
) -> tuple[str, str]:
    image = f"{registry}/{image_name}-{architecture}"
    return f"{image}:{version}", f"{image}:sha-{source_commit}"


def build_evidence(
    *,
    version: str,
    source_commit: str,
    registry: str,
    image_name: str,
    digests: dict[str, str],
) -> dict[str, Any]:
    """Build one exact two-architecture release-evidence document."""
    images = {}
    for architecture in ARCHITECTURES:
        version_ref, source_ref = _image_references(
            registry,
            image_name,
            architecture,
            version,
            source_commit,
        )
        images[architecture] = {
            "manifest_digest": digests.get(architecture),
            "source_ref": source_ref,
            "version_ref": version_ref,
        }
    evidence = {
        "backend_version": version,
        "images": images,
        "schema": EVIDENCE_SCHEMA,
        "source_commit": source_commit,
    }
    validate_evidence(
        evidence,
        version=version,
        source_commit=source_commit,
        registry=registry,
        image_name=image_name,
    )
    return evidence


def validate_evidence(
    evidence: Any,
    *,
    version: str,
    source_commit: str,
    registry: str,
    image_name: str,
) -> None:
    """Require exact schema, identities, references, and manifest digests."""
    if not isinstance(evidence, dict) or set(evidence) != {
        "backend_version",
        "images",
        "schema",
        "source_commit",
    }:
        raise ReleaseIntegrityError("Release evidence has an invalid top-level schema")
    if evidence["schema"] != EVIDENCE_SCHEMA:
        raise ReleaseIntegrityError("Release evidence schema version is not supported")
    if evidence["backend_version"] != version:
        raise ReleaseIntegrityError("Release evidence backend version does not match")
    if (
        evidence["source_commit"] != source_commit
        or not SOURCE_COMMIT_RE.fullmatch(source_commit)
    ):
        raise ReleaseIntegrityError("Release evidence source commit does not match")
    images = evidence["images"]
    if not isinstance(images, dict) or set(images) != set(ARCHITECTURES):
        raise ReleaseIntegrityError("Release evidence architectures are not exact")
    for architecture in ARCHITECTURES:
        image = images[architecture]
        if not isinstance(image, dict) or set(image) != {
            "manifest_digest",
            "source_ref",
            "version_ref",
        }:
            raise ReleaseIntegrityError(
                f"Release evidence {architecture} image schema is not exact"
            )
        version_ref, source_ref = _image_references(
            registry,
            image_name,
            architecture,
            version,
            source_commit,
        )
        if image["version_ref"] != version_ref:
            raise ReleaseIntegrityError(
                f"Release evidence {architecture} version reference does not match"
            )
        if image["source_ref"] != source_ref:
            raise ReleaseIntegrityError(
                f"Release evidence {architecture} source reference does not match"
            )
        digest = image["manifest_digest"]
        if not isinstance(digest, str) or not SHA256_RE.fullmatch(digest):
            raise ReleaseIntegrityError(
                f"Release evidence {architecture} digest is not an exact SHA-256"
            )


def _add_evidence_identity_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--version", required=True)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--registry", required=True)
    parser.add_argument("--image-name", required=True)


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)

    absent = commands.add_parser("require-manifest-absent")
    absent.add_argument("reference")

    select = commands.add_parser("select-artifact")
    select.add_argument("--pages", type=Path, required=True)
    select.add_argument("--name", required=True)
    select.add_argument("--source-commit", required=True)

    publication_run = commands.add_parser("validate-publication-run")
    publication_run.add_argument("--file", type=Path, required=True)
    publication_run.add_argument("--run-id", type=int, required=True)
    publication_run.add_argument("--repository", required=True)
    publication_run.add_argument("--source-commit", required=True)
    publication_run.add_argument("--workflow-path", required=True)

    write = commands.add_parser("write-evidence")
    write.add_argument("--output", type=Path, required=True)
    _add_evidence_identity_arguments(write)
    for architecture in ARCHITECTURES:
        write.add_argument(f"--{architecture}-digest", required=True)

    validate = commands.add_parser("validate-evidence")
    validate.add_argument("--file", type=Path, required=True)
    _add_evidence_identity_arguments(validate)
    return parser.parse_args()


def main() -> int:
    args = _parse_arguments()
    try:
        if args.command == "require-manifest-absent":
            require_manifest_absent(args.reference)
            return 0
        if args.command == "select-artifact":
            pages = json.loads(args.pages.read_text(encoding="utf-8"))
            artifact_id, run_id = select_evidence_artifact(
                pages,
                name=args.name,
                source_commit=args.source_commit,
            )
            print(
                json.dumps(
                    {"artifact_id": artifact_id, "run_id": run_id},
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "validate-publication-run":
            run = json.loads(args.file.read_text(encoding="utf-8"))
            validate_publication_run(
                run,
                run_id=args.run_id,
                repository=args.repository,
                source_commit=args.source_commit,
                workflow_path=args.workflow_path,
            )
            return 0
        identity = {
            "version": args.version,
            "source_commit": args.source_commit,
            "registry": args.registry,
            "image_name": args.image_name,
        }
        if args.command == "write-evidence":
            evidence = build_evidence(
                **identity,
                digests={
                    architecture: getattr(args, f"{architecture}_digest")
                    for architecture in ARCHITECTURES
                },
            )
            args.output.write_text(
                json.dumps(evidence, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            return 0
        evidence = json.loads(args.file.read_text(encoding="utf-8"))
        validate_evidence(evidence, **identity)
        return 0
    except (OSError, json.JSONDecodeError, ReleaseIntegrityError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
