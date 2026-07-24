FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

COPY ./pyproject.toml ./uv.lock /app/

WORKDIR /app

ENV UV_LINK_MODE=copy
RUN uv sync --frozen --no-dev

COPY harvester ./harvester
ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT ["python", "-m", "harvester"]
