FROM python:3.12-slim AS build
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
# --no-compile: bytecode compiled here is thrown away by the layer copy below
# and would only bloat the image.
RUN pip install --no-cache-dir --no-compile --target=/deps .

FROM python:3.12-slim
LABEL org.opencontainers.image.title="reapi-allowlist" \
      org.opencontainers.image.description="Derives the ADSB.lol re-api feeder allowlist from readsb and mlat-server, and writes it to a CiliumGatewayClassConfig" \
      org.opencontainers.image.licenses="BSD-3-Clause"
RUN useradd --uid 1000 --create-home app
COPY --from=build /deps /deps
ENV PYTHONPATH=/deps PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
USER 1000
ENTRYPOINT ["python", "-m", "reapi_allowlist"]
