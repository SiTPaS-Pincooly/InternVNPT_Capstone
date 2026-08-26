# The official apache/superset image has no ClickHouse driver installed by
# default, so Superset can't connect to it out of the box. This adds the one
# package needed (clickhouse-connect provides the SQLAlchemy dialect that
# Superset's "Add Database" screen looks for) on top of the stock image.
#
# Why the activate + `uv pip` dance rather than a plain `pip install`:
#
#   Superset switched to `uv` for package management after the 4.1 branch.
#   The image ships a virtualenv at /app/.venv, but uv-created venvs do NOT
#   include a `pip` executable - so `/app/.venv/bin/pip` does not exist and
#   fails the build with exit code 127. A bare `pip install` as root is
#   worse: it may succeed while installing into the SYSTEM interpreter,
#   which Superset does not use, leaving the driver invisible at runtime.
#
#   Sourcing the venv's activate script and using `uv pip` is the pattern
#   Apache documents for extending these images.
#
#   On images built from apache/superset:4.1.x or older, replace the RUN
#   below with:  RUN pip install --no-cache-dir clickhouse-connect
#
# After changing this file the image MUST be rebuilt explicitly - a plain
# `docker compose up` reuses the cached image and the change is silently
# ignored:
#
#   docker compose build --no-cache superset
#   docker compose up -d --force-recreate superset
#   docker exec ids-superset python -c "import clickhouse_connect; print(clickhouse_connect.__version__)"

FROM apache/superset:latest

USER root
RUN . /app/.venv/bin/activate && \
    uv pip install --no-cache clickhouse-connect
USER superset
