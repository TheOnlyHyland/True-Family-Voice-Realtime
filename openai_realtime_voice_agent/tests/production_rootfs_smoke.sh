#!/usr/bin/env bash
set -euo pipefail

ADDON_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
EXPECTED_LOCK_SHA256="13193c62fc95a0c05c7b6e89efe7db060b4f00438db46c83dc43a23eb1d9af15"
PYTHON_BIN=${PYTHON_BIN:-python3}
POETRY_BIN=${POETRY_BIN:-poetry}

test "$(sha256sum "$ADDON_ROOT/poetry.lock")" = \
    "$EXPECTED_LOCK_SHA256  $ADDON_ROOT/poetry.lock"
test "$($POETRY_BIN --version)" = "Poetry (version 2.0.1)"

TEMP_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/true-family-voice-rootfs.XXXXXX")
trap 'rm -rf "$TEMP_ROOT"' EXIT
ROOTFS="$TEMP_ROOT/rootfs"
VENV="$ROOTFS/opt/venv"
WHEEL_DIR="$TEMP_ROOT/wheel"

mkdir -p "$ROOTFS/opt/true-family-voice" \
    "$ROOTFS/usr/share/true-family-voice" \
    "$WHEEL_DIR"
"$PYTHON_BIN" -m venv "$VENV"

(
    cd "$ADDON_ROOT"
    VIRTUAL_ENV="$VENV" PATH="$VENV/bin:$PATH" \
        "$POETRY_BIN" sync --only main --no-root --no-interaction --no-ansi
    "$POETRY_BIN" build --format wheel --output "$WHEEL_DIR" \
        --no-interaction --no-ansi
)

"$VENV/bin/pip" install --no-deps "$WHEEL_DIR"/*.whl
cp "$ADDON_ROOT/poetry.lock" \
    "$ROOTFS/usr/share/true-family-voice/poetry.lock"
cp "$ADDON_ROOT/root/run.sh" "$ROOTFS/run.sh"
chmod a+x "$ROOTFS/run.sh"
test ! -e "$ROOTFS/app"

(
    cd "$ROOTFS/opt/true-family-voice"
    PYTHONSAFEPATH=1 "$VENV/bin/python" -P -c \
        'import importlib.util; spec = importlib.util.find_spec("app.main"); assert spec is not None and "/site-packages/app/main.py" in spec.origin'
    PYTHONSAFEPATH=1 "$VENV/bin/python" -m app.main --startup-smoke
    PATH="$VENV/bin:$PATH" PYTHONSAFEPATH=1 \
        TRUE_FAMILY_VOICE_STARTUP_SMOKE=1 bash "$ROOTFS/run.sh"
)

printf '%s\n' "Production rootfs startup smoke passed"
