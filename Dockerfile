# Minimal worker image for Pyjobkit (issue #60).
#
# Build:
#   docker build -t pyjobkit/worker:dev .
#
# Run (Postgres example):
#   docker run --rm \
#     -e PYJOBKIT_DSN=postgresql+asyncpg://user:pass@host/db \
#     pyjobkit/worker:dev
#
# Configuration is read from PYJOBKIT_* environment variables (see
# pyjobkit/config.py for the full list) or a TOML file mounted at
# /etc/pyjobkit.toml.

FROM python:3.13-slim AS build

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY pyjobkit ./pyjobkit
RUN python -m pip install --upgrade pip build \
    && python -m build --wheel --outdir /wheels

FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN useradd --create-home --shell /bin/bash --uid 1000 pyjobkit
USER pyjobkit
ENV PATH="/home/pyjobkit/.local/bin:$PATH"

COPY --from=build /wheels /tmp/wheels
RUN python -m pip install --user --no-cache-dir \
    /tmp/wheels/pyjobkit-*.whl asyncpg aiosqlite \
    && rm -rf /tmp/wheels

WORKDIR /home/pyjobkit
ENTRYPOINT ["pyjobkit"]
CMD []
