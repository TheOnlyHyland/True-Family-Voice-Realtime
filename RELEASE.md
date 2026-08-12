# Release Safety

## Compatibility Order

The exact Voice PE firmware binding for backend 0.22.5 is finalized:

- Firmware version: `0.20.1`
- Repository: `TheOnlyHyland/True-Family-Voice-Firmware`
- Source commit: `13ad7efe2df75f846f8fb48a939934db75efb5fd`
- Manifest SHA-256:
  `00c4439827f37d4b2d92ff52bfe1631263b209b6a014d1de38bef4c0f418930c`
- Factory image SHA-256:
  `2dc5bac2c0185438339ef614c8572edc5387bf047b2929fd6ab6b05053bb857b`
- OTA image SHA-256:
  `7fa2e386486d7b55b37f135da1bd7fd97fce13a50d2b105f559fd5f9559557af`
- ELF SHA-256:
  `1037c5de9abcb38708deb765072b3e395a67d9884d3236d5ad83056a9f681cfc`
- `SHA256SUMS` SHA-256:
  `05dfdcbf50e50d5ea141ce6680890f264f369b072b5d0aec74919cb6c87b41e2`

The workflow records `FIRMWARE_RELEASE_BINDING: finalized`. Publication and
release jobs still fail closed unless every value above is exact, the public
firmware package matches every hash, and the checked-out source has the exact
commit and version.

> **Upgrade firmware first.** Keep the existing backend running, update and
> verify exact firmware 0.20.1, then update to backend 0.22.5 only after its
> protected images and GitHub release exist. Starting backend 0.22.5 before the
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
and pushes it without rebuilding. After both architecture version and source tags
are published, that same successful protected workflow records one validated
machine-readable evidence artifact for backend 0.22.5. The artifact binds the
candidate commit and exact aarch64/amd64 registry references to their SHA-256
manifest digests and is retained for 90 days.

Publication and release-event verification share one version/tag concurrency
key even when they run from different Git refs. Normal push and pull-request CI
remain keyed by their own refs. Release verification locates exactly one
non-expired evidence artifact, proves its workflow run was a successful
`workflow_dispatch` of this workflow at the release commit, and downloads it by
its artifact and run IDs. Both current version and `sha-<commit>` tags must still
resolve to each recorded architecture digest. Comparing the tags only to each
other is insufficient because two moved tags must not validate a release.

Every PR, manual, publication, and release test job checks out exact firmware
0.20.1 source commit `13ad7efe2df75f846f8fb48a939934db75efb5fd` and validates the full source-level
protocol contract, including the tokenized `follow_up_progress_phase` shape.
Prepared package assets are not assumed public during PR or non-publishing manual
CI, so those jobs do not download or require them. Publication and release jobs
additionally download the immutable public package and require every finalized
hash above. Package evidence is never allowed to replace exact source validation.

The historical `pilot_firmware_source_only` workflow input is retained for
auditability but cannot authorize a 0.22.5 image or public GitHub release. Every
0.22.5 publication requires `pilot_firmware_source_only=false`, a finalized public
firmware source commit, and every exact release-artifact hash. Release-event
verification enforces the same public binding.

## Follow-Up Transaction Authenticity

The finalized firmware 0.20.1 binding must implement the fixture's exact
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

The generated follow-up question is a no-tools continuation: the Realtime
request carries an empty tool list and `tool_choice: none`. Its held text,
transcript, and PCM are released only while the exact response ID, response
generation, reservation epoch, token, socket, session nonce, wake generation,
and physical deadline still match. That authority is checked again around every
await and every frame. A server-emitted function call in this mode is quarantined
before dispatch and forces recovery.

## Silent Terminal Decision Gate

Backend 0.22.5 holds each managed response's text, audio transcript, and PCM until
`response.done` establishes its exact terminal structure. The hold fails closed
after 60 seconds, 3 MiB, or 4,096 events; reaching a bound discards the output and
enters recovery rather than releasing a partial response. Ordinary valid speech
is released only after its server output and local conversation projection agree.

An otherwise exact `request_follow_up` or `end_conversation` call accompanied by
one unheard assistant message is normalized before dispatch: the assistant item
must be deleted from OpenAI, confirmed deleted, removed from local history, and
reprojected with the exact in-progress tool placeholder. Additional, ambiguous,
malformed, pending, or unconfirmed output fails closed. A silent close is
authorized only when the resulting immutable terminal ledger contains exactly
one authorized `end_conversation` call, no pending tool work, and a still-current
device-owned close grant.

High-confidence gratitude, completion, decision, and cancellation answers veto
the silent path. They receive one brief natural no-tools spoken acknowledgement
instead. That continuation also carries `tools: []` and `tool_choice: none`; any
function call is quarantined before dispatch. Unrelated answers remain eligible
for exact sole-tool silent close.

Recovery synchronously revokes physical output and held-output release authority,
settles terminal and continuation waiters, rolls back unreleasable local output,
and cancels normalization work. A stale release callback or recovery-raced
transport bind cannot restore the old output grant.

## Response-Generation Audio Barrier

Backend 0.22.5 binds each assistant PCM source frame, Pipecat chunker operation,
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

## Context-Bound Graceful Close

Graceful close uses one immutable `(websocket, session_nonce, wake_generation,
token)` for PREPARE, COMMIT, ACK, and CANCEL. ACKs must match the exact physical
socket, context, stage, and token. An exact negative ACK clears only its own
transaction before failure is surfaced; stale ACKs cannot settle a replacement.

A valid newer firmware wake is authoritative evidence that the old close owner
was revoked. The backend burns the old pending, committed, expected-ACK, owner,
and deferred state locally, settles its waiter, and sends no old-wake CANCEL into
the new turn. Successful silent close sends tokenless terminal `idle` only to its
still-current local socket/session/wake. If close fails, the socket is retired
only while that exact triple still matches; a superseding wake remains admitted
and untouched.

## Accepted Rapid-Pilot Privileges

Version 0.22.5 deliberately inherits the pilot's `host_network: true`,
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

1. Commit the exact release candidate on a reviewed candidate branch and let its
   normal CI pass. Do not merge the version bump to `main` yet.
2. Manually dispatch **Build and Publish Home Assistant Addon** from that exact
   candidate commit with `publish=true`, `release_tag=v0.22.5`, and
   `source_commit=<the same 40-character commit>`. Keep
   `pilot_firmware_source_only=false`.
3. Approve its protected `backend-production` environment gate only after both
   architecture build-and-smoke jobs pass. Require the publish job to finish
   successfully with its unique backend 0.22.5 evidence artifact.
4. Record both architecture image digests and verify the version and
   `sha-<commit>` tags resolve to the evidence values.
5. Create tag `v0.22.5` at that exact candidate commit and publish the GitHub
   release. The release event is verification-only and must not build or push an
   image.
6. Wait for **Verify release uses pre-published immutable images** to pass. It
   must prove the exact successful publication run and its recorded digests.
7. Only after release verification passes, merge the identical candidate tree to
   `main`. This is the step that exposes repository metadata advertising 0.22.5
   to Home Assistant users; merging earlier is prohibited.

The publication workflow refuses to overwrite either an existing `0.22.5` tag
or its source-commit tag. A partially published failed run must be investigated;
do not delete or replace a successful architecture tag without treating that as
a new release version. A missing evidence artifact or failed release verification
does not permit retrying over existing tags; advance to a new backend version.
The registry preflight treats only an explicit manifest-not-found response as
absence. Authentication, network, rate-limit, empty, mixed, or otherwise
ambiguous Docker failures abort publication without printing captured registry
diagnostics that could contain credentials.
