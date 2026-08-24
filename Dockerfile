FROM python:3.12-slim AS build
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir --target=/deps .

FROM python:3.12-slim
RUN useradd --uid 1000 --create-home app
COPY --from=build /deps /deps
ENV PYTHONPATH=/deps PYTHONUNBUFFERED=1
USER 1000
ENTRYPOINT ["python", "-m", "reapi_allowlist"]
