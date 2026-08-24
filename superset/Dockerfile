# The official apache/superset image has no ClickHouse driver installed by
# default, so Superset can't connect to it out of the box. This adds the
# one package needed (clickhouse-connect provides the SQLAlchemy dialect
# Superset's "Add Database" screen looks for) on top of the stock image.
FROM apache/superset:latest

USER root
RUN pip install --no-cache-dir clickhouse-connect
USER superset
