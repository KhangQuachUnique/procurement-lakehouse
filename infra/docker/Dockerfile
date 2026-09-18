FROM ghcr.io/astral-sh/uv:0.12.15@sha256:62f8c047d0a0e9ece6b53fc63df902585a67a47a7f318ddec4a37db586edc8e3 AS uv
FROM python:3.12-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea AS builder
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --extra ops --no-install-project --no-editable
COPY src ./src
RUN uv sync --locked --extra ops --no-editable

FROM python:3.12-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea AS runtime
RUN groupadd --gid 10001 procurement \
    && useradd --uid 10001 --gid procurement --create-home procurement \
    && mkdir -p /var/lib/procurement \
    && chown procurement:procurement /var/lib/procurement
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    APP_ENV=production \
    DLT_PIPELINES_DIR=/var/lib/procurement/pipelines \
    INGESTION_LOCK_DIR=/var/lib/procurement/locks \
    RUNTIME__DLTHUB_TELEMETRY=false
USER procurement
CMD ["python", "-m", "procurement.jobs.ingest", "--help"]
