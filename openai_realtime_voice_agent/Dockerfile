# Home Assistant add-on image.
#
# Uses the Debian (glibc) base image instead of Alpine (musl). Heavy Python
# dependencies pulled in transitively by pipecat (llvmlite, numba, ...) ship
# prebuilt manylinux wheels for glibc, so they install without compiling
# against a specific LLVM. The previous Alpine build compiled llvmlite from
# source and broke once it resolved to llvmlite 0.47 (which requires LLVM 20)
# while the image only provided LLVM 15.
#
# Override the base per-arch via build.yaml (build_from) / --build-arg BUILD_FROM.
ARG BUILD_FROM=ghcr.io/home-assistant/aarch64-base-debian@sha256:4ecdb87bbf2e24c220140aedefdc521f7e3c499bc116c886f432261dc247d2f8
ARG POETRY_LOCK_SHA256=13193c62fc95a0c05c7b6e89efe7db060b4f00438db46c83dc43a23eb1d9af15
ARG TITANET_MODEL_SHA256=d51abcf31717ef28162f26acb9d44dd4127c3d44c9b8624f699f3425daca8e77
ARG TITANET_MODEL_URL=https://github.com/k2-fsa/sherpa-onnx/releases/download/speaker-recongition-models/nemo_en_titanet_large.onnx
ARG DEBIAN_SNAPSHOT=20260801T000000Z
ARG POETRY_VERSION=2.0.1
ARG POETRY_CORE_VERSION=2.0.1
ARG BUILD_VERSION=0.22.5
ARG BUILD_REVISION=unpublished
FROM ${BUILD_FROM} AS builder

ARG DEBIAN_SNAPSHOT
ARG POETRY_VERSION
ARG POETRY_CORE_VERSION
ARG POETRY_LOCK_SHA256
ARG TITANET_MODEL_SHA256
ARG TITANET_MODEL_URL

ENV PIP_BREAK_SYSTEM_PACKAGES=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONSAFEPATH=1 \
    PIP_NO_CACHE_DIR=1

RUN rm -f /etc/apt/sources.list.d/* \
    && printf '%s\n' \
        "deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT} bookworm main" \
        "deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT} bookworm-updates main" \
        "deb [check-valid-until=no] https://snapshot.debian.org/archive/debian-security/${DEBIAN_SNAPSHOT} bookworm-security main" \
        > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends \
        python3=3.11.2-1+b1 \
        python3-pip=23.0.1+dfsg-1 \
        python3-venv=3.11.2-1+b1 \
        python3-dev=3.11.2-1+b1 \
        build-essential=12.9 \
        curl=7.88.1-10+deb12u15 \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install \
        "poetry==${POETRY_VERSION}" \
        "poetry-core==${POETRY_CORE_VERSION}" \
    && python3 -m venv /opt/venv

COPY pyproject.toml poetry.lock /tmp/app_build/
RUN test "$(sha256sum /tmp/app_build/poetry.lock)" = \
        "${POETRY_LOCK_SHA256}  /tmp/app_build/poetry.lock" \
    && cd /tmp/app_build \
    && VIRTUAL_ENV=/opt/venv PATH="/opt/venv/bin:${PATH}" \
        poetry sync --only main --no-root --no-interaction --no-ansi \
    && /opt/venv/bin/python -c \
        'from importlib.metadata import version; assert version("pipecat-ai") == "0.0.97"; assert version("numpy") == "2.2.6"; assert version("loguru") == "0.7.3"'

# Install the application as a wheel. PYTHONSAFEPATH deliberately removes the
# working directory from sys.path, so a loose /app tree cannot satisfy
# `python -m app.main` in production.
COPY app/ /tmp/app_build/app/
RUN cd /tmp/app_build \
    && poetry build --format wheel --output /tmp/app_wheel --no-interaction --no-ansi \
    && /opt/venv/bin/pip install --no-deps /tmp/app_wheel/*.whl \
    && PYTHONSAFEPATH=1 /opt/venv/bin/python -P -c \
        'import importlib.util; assert importlib.util.find_spec("app.main") is not None'

# TitaNet-L is baked into the image so startup needs no network. The release
# asset URL is content-verified before it crosses into the runtime stage.
RUN mkdir -p /opt/voiceprint \
    && curl --proto '=https' --tlsv1.2 --fail --location --retry 3 \
        --output /opt/voiceprint/embedder.onnx "${TITANET_MODEL_URL}" \
    && test "$(sha256sum /opt/voiceprint/embedder.onnx)" = \
        "${TITANET_MODEL_SHA256}  /opt/voiceprint/embedder.onnx"

FROM ${BUILD_FROM}

ARG DEBIAN_SNAPSHOT
ARG BUILD_REVISION
ARG BUILD_VERSION
ARG POETRY_VERSION
ARG POETRY_CORE_VERSION
ARG POETRY_LOCK_SHA256
ARG TITANET_MODEL_SHA256
ARG TITANET_MODEL_URL

ENV PATH="/opt/venv/bin:${PATH}" \
    PYTHONUNBUFFERED=1 \
    PYTHONSAFEPATH=1

LABEL org.opencontainers.image.source="https://github.com/TheOnlyHyland/True-Family-Voice-Realtime" \
      org.opencontainers.image.version="${BUILD_VERSION}" \
      org.opencontainers.image.revision="${BUILD_REVISION}" \
      io.true-family.voice.debian-snapshot="${DEBIAN_SNAPSHOT}" \
      io.true-family.voice.poetry-version="${POETRY_VERSION}" \
      io.true-family.voice.poetry-core-version="${POETRY_CORE_VERSION}" \
      io.true-family.voice.poetry-lock-sha256="${POETRY_LOCK_SHA256}" \
      io.true-family.voice.titanet-source="${TITANET_MODEL_URL}" \
      io.true-family.voice.titanet-sha256="${TITANET_MODEL_SHA256}"

# ffmpeg + libsndfile1 cover Pipecat's runtime audio paths.
RUN rm -f /etc/apt/sources.list.d/* \
    && printf '%s\n' \
        "deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT} bookworm main" \
        "deb [check-valid-until=no] https://snapshot.debian.org/archive/debian/${DEBIAN_SNAPSHOT} bookworm-updates main" \
        "deb [check-valid-until=no] https://snapshot.debian.org/archive/debian-security/${DEBIAN_SNAPSHOT} bookworm-security main" \
        > /etc/apt/sources.list \
    && apt-get -o Acquire::Check-Valid-Until=false update \
    && apt-get install -y --no-install-recommends \
        python3=3.11.2-1+b1 \
        ffmpeg=7:5.1.9-0+deb12u1 \
        libsndfile1=1.2.0-1+deb12u1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /opt/voiceprint/embedder.onnx /opt/voiceprint/embedder.onnx
COPY --from=builder /tmp/app_build/poetry.lock /usr/share/true-family-voice/poetry.lock

# s6-overlay / bashio run script
# TitaNet-L: validated on this household's far-field XMOS audio (100% self,
# zero cross-matches at 3s) where WeSpeaker/VoxCeleb-domain models failed.
COPY root/ /
RUN chmod a+x /run.sh

WORKDIR /opt/true-family-voice
CMD ["/run.sh"]
