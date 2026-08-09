# Release Safety

## Compatibility Order

The exact Voice PE firmware binding for backend 0.22.4 is finalized:

- Firmware version: `0.20.0`
- Repository: `TheOnlyHyland/True-Family-Voice-Firmware`
- Source commit: `36abf4ba861e2ca30968882311ed3b2562b47367`
- Manifest SHA-256:
  `09fa1bb26d032fccc496834171ebc314abbf5e08da2d68d8801210db0b006e9f`
- Factory image SHA-256:
  `8a35ceb28bb939707869edfa9fc32b4fedda3c14bb5a8b7b5dc80f5340a9ad65`
- OTA image SHA-256:
  `a5ed1f17def9c008ac86992293f808b9b0ee5ff9e32c0a8d76e014219c735d38`
- ELF SHA-256:
  `9ca821815f68e1b25207c6a9d93081a72cf76c51b38f1a5ad0178e77136b7b88`
- `SHA256SUMS` SHA-256:
  `280816f68552f2eaa6785891e98876db2729d311c860a4982464c2ca846b71b3`

The workflow records `FIRMWARE_RELEASE_BINDING: finalized`. Publication and
release jobs still fail closed unless every value above is exact, the public
firmware package matches every hash, and the checked-out source has the exact
commit and version.

> **Upgrade firmware first.** Keep the existing backend running, update and
> verify exact firmware 0.20.0, then update to backend 0.22.4 only after its
> protected images and GitHub release exist. Starting backend 0.22.4 before the
> firmware update succeeds is unsupported. A source checkout is not a release
> artifact.

> **Rollback the backend first.** Keep the newer firmware running, roll the
> backend back to released 0.21.1, where explicit follow-up fails closed, or to
> the documented legacy 0.20.6 path when required, and verify reconnection before
> considering any firmware rollback. Rolling
> firmware back while a newer backend is running reverses the safe compatibility
> order.

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

Every PR, manual, publication, and release test job checks out exact firmware
0.20.0 source commit `36abf4ba861e2ca30968882311ed3b2562b47367` and validates the full source-level
protocol contract, including the tokenized `follow_up_progress_phase` shape.
Prepared package assets are not assumed public during PR or non-publishing manual
CI, so those jobs do not download or require them. Publication and release jobs
additionally download the immutable public package and require every finalized
hash above. Package evidence is never allowed to replace exact source validation.

The historical `pilot_firmware_source_only` workflow input is retained for
auditability but cannot authorize a 0.22.4 image or public GitHub release. Every
0.22.4 publication requires `pilot_firmware_source_only=false`, a finalized public
firmware source commit, and every exact release-artifact hash. Release-event
verification enforces the same public binding.

## Follow-Up Transaction Authenticity

The finalized firmware 0.20.0 binding must implement the fixture's exact
`follow_up_progress_phase` shape: `type`, `value`, `token`, `session_nonce`, and
`wake_generation`. Initial physical-wake phases retain the existing trusted
four-field shape, and the historical two-field phase remains legacy-only.
The backend emits tokenized `listening`, `thinking`, and `replying` progression
only for the current answer to an OPEN follow-up transaction. Terminal `idle` is
always tokenless and a token-bearing terminal phase is rejected. Phase authority
also expires at the 120-second physical-wake ceiling, during silent close, or
after its captured wake or local transaction epoch changes. Mandatory release
CI validates these protocol shapes against the exact finalized firmware source;
they cannot be waived by the historical source-only pilot input.

Answer acceptance also requires the exact fresh OpenAI speech-start item ID and
its monotonic local sequence before that same item's nonblank transcript can
confirm the follow-up. Delayed historical transcripts, duplicate completion,
recovery, socket replacement, and a new physical wake cannot confer authority on
a later round.

## Silent Terminal Decision Gate

For the response to one freshly confirmed follow-up answer, backend 0.22.4 holds
text, audio transcript, and PCM for at most 500 ms or 48,000 PCM bytes. Normal
speech is released after that bound; a separate 512-event cap prevents
zero-length delta floods from bypassing the memory limit. Held output is
discarded only after
`response.done` proves one completed response with exactly one authorized
`end_conversation` call, no mixed or pending tool work, and a still-current
device-owned close grant. Tool execution then waits on that immutable terminal
ledger before sending the silent close result.

Every failed precondition releases the held output through the normal response
path. Recovery discards the hold and retires the physical output generation
immediately. This gate does not depend on prompting or on receiving tool-call
events before audio deltas.

## Response-Generation Audio Barrier

Backend 0.22.4 binds each assistant PCM source frame, Pipecat chunker operation,
queued chunk, adapter-owned partial buffer, and active WebSocket write to the exact admitted
socket and `(response_id, response_generation)`. A tool continuation waits for
response A to finish before it arms a follow-up or creates response B. The drain
may pad and send at most one final partial PCM chunk; it never truncates the last
words merely because `response.done` arrived before Pipecat completed playback.

The wait does not hold the socket/session transition lock. Stop, mute,
disconnect, socket replacement, recovery, wake expiry, timeout, serializer
failure, and physical WebSocket write failure retire response A and discard its
queued or partial audio. No audio from an old generation may cross into a new
physical owner. A new generation also waits for old in-flight source processing;
if processing or a WebSocket write resists bounded cancellation, the transport
retires that socket before allowing another owner or generation. A failed or
expired graceful finish settles its generation before the handler releases the
response grant; a cancellation-resistant physical write therefore forces
owner detachment and socket close or abort before failure returns, and no tool
continuation may create response B on that path.

## Accepted Rapid-Pilot Privileges

Version 0.22.4 deliberately inherits the pilot's `host_network: true`,
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

The firmware binding is finalized, but publication remains protected and
content-addressed. Execute this sequence only from the reviewed candidate after
its normal CI passes; never treat a source checkout as an installable release.

1. Commit the exact release candidate on a reviewed release branch and let its
   normal CI pass. Do not merge the version bump to `main` yet.
2. Manually dispatch **Build and Publish Home Assistant Addon** from that exact
   candidate commit with `publish=true`, `release_tag=v0.22.4`, and
   `source_commit=<the same 40-character commit>`. Keep
   `pilot_firmware_source_only=false`.
3. Approve its protected `backend-production` environment gate only after both
   architecture build-and-smoke jobs pass.
4. Record both architecture image digests printed by the workflow.
5. Verify both version tags and both `sha-<commit>` tags resolve to those
   digests.
6. Only after the images exist, merge the identical candidate source to `main`.
   This is the step that exposes the repository update to Home Assistant users.
7. Tag the reviewed candidate commit `v0.22.4`; only then publish the GitHub release.
   The release event is verification-only and must not build or push an image.

The publication workflow refuses to overwrite either an existing `0.22.4` tag
or its source-commit tag. A partially published failed run must be investigated;
do not delete or replace a successful architecture tag without treating that as
a new release version.
