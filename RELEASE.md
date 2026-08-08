# Release Safety

## Compatibility Order

Version 0.21.1 is a coordinated protocol release and requires Voice PE firmware
0.19.0 or newer.

> **Upgrade firmware first.** Keep the existing backend running, update and
> verify the device firmware, then update the backend to 0.21.1. Starting the
> 0.21.1 backend against older firmware is unsupported and must not be used as a
> rollout shortcut.

> **Rollback the backend first.** Keep the newer firmware running, roll the
> backend back to the previously compatible version and verify reconnection,
> then roll back firmware if still necessary. Rolling firmware back while the
> 0.21.1 backend is running reverses the safe compatibility order.

## Published Artifact

`config.yaml` names the architecture-specific GHCR image, so Home Assistant
release installs consume the exact version tag instead of rebuilding from a
mutable local dependency source. The Docker build is bound to:

- The SHA-256-pinned Home Assistant base image for each architecture.
- Debian package indexes frozen at snapshot `20260801T000000Z`, with every
  directly installed build and runtime package version pinned explicitly.
- Poetry `2.0.1` and Poetry Core `2.0.1` as the pinned build toolchain.
- `poetry.lock` SHA-256
  `13193c62fc95a0c05c7b6e89efe7db060b4f00438db46c83dc43a23eb1d9af15`.
- Pipecat `0.0.97`, NumPy `2.2.6`, and Loguru `0.7.3` from that lock.
- TitaNet model SHA-256
  `d51abcf31717ef28162f26acb9d44dd4127c3d44c9b8624f699f3425daca8e77`.
  The exact model release URL is recorded in the image label, and its bytes are
  hash-checked before crossing from the builder into the runtime image.
- An installed application wheel, proven importable with `PYTHONSAFEPATH=1`.

CI builds each architecture image once. It runs the arm64 image through QEMU and
the amd64 image natively, addressing each image by its Docker image ID rather
than a mutable tag. The gate runs the image's exact `CMD ["/run.sh"]` with
`TRUE_FAMILY_VOICE_STARTUP_SMOKE=1` and checks the in-image lock, model digest,
installed module path, build labels, architecture, and exact runtime versions.
Only after those checks does CI save and hash the exact image. A protected
publication job loads that saved image, verifies the image ID and archive hash,
and pushes it without rebuilding.

The backend's read-only protocol fixture is aligned to the final firmware
`0.19.0` artifact: manifest SHA-256
`b9b12d87346148d5260a53d6303eb8c44ffb3cd24d6eb5c1a0017baccdc3a9d3`, factory
SHA-256 `7f0ffaeaecb861ceb342ad571501b14c6017161bbb6d90f489002ae4271f6b14`,
and OTA SHA-256
`68ab4263b407244d5cce05d7a81888604bd90dccfb38e93c8a63f4a55a070ad8`.
The corresponding ELF SHA-256 is
`d1f77ac2f71a6491bd750f44efa5e6bacdd977edc945b6ef20d241995e843775`,
and the exact `SHA256SUMS` file SHA-256 is
`fb4f71aebb6556ca6b6f659832943c698400f62cb9ee44bc1a10b2f5894050ce`.
The firmware artifact is not copied into or modified by this repository.

Normal backend CI checks out the exact public firmware `0.19.0` tag and
downloads the release's actual `manifest.json`, binaries, ELF, and
`SHA256SUMS`. It validates source-level protocol shapes, every package member,
the package manifest version, and the exact published `SHA256SUMS` bytes.
Missing source, missing release assets, a source/version/protocol mismatch, or
any artifact hash mismatch fails CI. A standalone local test run may use
the committed fixture when those external inputs are absent; CI sets
`TRUE_FAMILY_VOICE_REQUIRE_FIRMWARE_VALIDATION=1`, so release validation cannot
skip either source or artifact checks.

The household RAPID-PILOT image gate is narrower and cannot authorize a public
GitHub release. An explicit manual dispatch with
`pilot_firmware_source_only=true` checks out installed firmware source commit
`bcb3bf4cbf181397b51aa7cc5bca5cfecefc7b3a`, runs the complete backend and
source-level protocol suite, and still requires both architecture image smokes.
Only the protected GHCR image publication may use this path. Release-event
verification always requires the public firmware tag and exact release assets.

## Accepted Rapid-Pilot Privileges

Version 0.21.1 deliberately inherits the pilot's `host_network: true`,
`homeassistant_api: true`, and read-write `/share` mount. These remain accepted
deployment risks, not reduced-sandbox claims: a backend compromise can reach the
host network, use the add-on's Home Assistant API credential, and alter the
voice probe, print, and memory files under `/share`.

Model authority is narrower than those process privileges. An MCP call is
authorized only when its exact case-sensitive name is explicitly listed in
`mcp_tool_allowlist` and was exposed in the current Realtime session. An empty
allow-list exposes no MCP tools. Deployment must therefore populate the full
intended whole-home list explicitly rather than relying on inherited process
access.

If a development host has no Docker engine, the executable
`openai_realtime_voice_agent/tests/production_rootfs_smoke.sh` builds the wheel,
installs the exact lock into a temporary production-shaped rootfs, and starts
the installed module under `PYTHONSAFEPATH=1`. This is the faithful local
fallback; the CI actual-image smoke remains mandatory before publication.

An unpublished local add-on copy may remove the `image` key and use
`build.yaml`; that path uses the same pinned Dockerfile inputs but is a developer
build, not the release artifact consumed by normal installations.

## Publication Order

1. Commit the exact release candidate on a reviewed release branch and let its
   normal CI pass. Do not merge the version bump to `main` yet.
2. Manually dispatch **Build and Publish Home Assistant Addon** from that exact
   candidate commit with `publish=true`, `release_tag=v0.21.1`, and
   `source_commit=<the same 40-character commit>`.
3. Approve its protected `backend-production` environment gate only after both
   architecture build-and-smoke jobs pass.
4. Record both architecture image digests printed by the workflow.
5. Verify both version tags and both `sha-<commit>` tags resolve to those
   digests.
6. Only after the images exist, merge the identical candidate source to `main`.
   This is the step that exposes the repository update to Home Assistant users.
7. Tag the reviewed candidate commit `v0.21.1`; only then publish the GitHub release.
   The release event is verification-only and must not build or push an image.

The publication workflow refuses to overwrite either an existing `0.21.1` tag
or its source-commit tag. A partially published failed run must be investigated;
do not delete or replace a successful architecture tag without treating that as
a new release version.
