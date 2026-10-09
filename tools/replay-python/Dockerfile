# syntax=docker/dockerfile:1
FROM python:3.11-slim

RUN groupadd -g 10001 streamforge && \
    useradd -u 10001 -g streamforge -s /bin/sh streamforge

WORKDIR /app

COPY pyproject.toml ./
COPY tools/replay-python/ tools/replay-python/

RUN pip install --no-cache-dir .

USER 10001:10001

ENTRYPOINT ["streamforge-replay"]
