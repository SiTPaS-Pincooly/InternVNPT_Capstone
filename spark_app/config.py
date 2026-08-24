"""
config.py

Central place for every connection setting the Spark app needs: Kafka,
ClickHouse, and streaming job settings (checkpoint location, starting
offsets). Every value has a sensible local-Docker default but can be
overridden with an environment variable — so the same code works
unchanged whether you're running against `docker-compose.yml` on your
laptop or, later, a differently-configured environment, without editing
this file.

Nothing here is a secret (no passwords are hardcoded beyond the
ClickHouse dev-default), which is fine for a local Docker setup — if you
ever point this at a real remote deployment, override
CLICKHOUSE_PASSWORD via environment variable rather than editing the
default below.
"""

import os

# --------------------------------------------------------------------------
# Kafka
# --------------------------------------------------------------------------
# Everything runs locally via Docker — no TLS, no auth, just host:port.
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "network-traffic")

# Where Spark starts reading from when the stream first launches.
# "latest"   -> only see records produced after the job starts (good for a
#               live demo: start streaming_job.py first, then generate_traffic.py)
# "earliest" -> replay everything still retained on the topic (good for
#               reprocessing / debugging against already-generated data)
KAFKA_STARTING_OFFSETS = os.environ.get("KAFKA_STARTING_OFFSETS", "latest")

# --------------------------------------------------------------------------
# ClickHouse
# --------------------------------------------------------------------------
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
# ClickHouse exposes two ports by default: 8123 (HTTP, used by the JDBC
# driver Spark writes through) and 9000 (native TCP protocol, used by the
# clickhouse-driver Python client / most CLI tools). Both are configured
# here since clickhouse_writer.py may end up using either.
CLICKHOUSE_HTTP_PORT = int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123"))
CLICKHOUSE_NATIVE_PORT = int(os.environ.get("CLICKHOUSE_NATIVE_PORT", "9000"))
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DATABASE", "ids")
CLICKHOUSE_TABLE = os.environ.get("CLICKHOUSE_TABLE", "detections")

# JDBC URL Spark's foreachBatch writer connects through.
CLICKHOUSE_JDBC_URL = (
    f"jdbc:clickhouse://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/{CLICKHOUSE_DATABASE}"
)

# --------------------------------------------------------------------------
# Rules
# --------------------------------------------------------------------------
RULES_PATH = os.environ.get("RULES_PATH", "./rules/rules.json")

# --------------------------------------------------------------------------
# Spark Structured Streaming job settings
# --------------------------------------------------------------------------
SPARK_APP_NAME = os.environ.get("SPARK_APP_NAME", "streaming-ids")

# Structured Streaming needs a checkpoint directory to track progress
# (which Kafka offsets have been processed) so it can resume correctly
# after a restart instead of reprocessing or skipping records.
CHECKPOINT_LOCATION = os.environ.get("CHECKPOINT_LOCATION", "./_checkpoints/streaming_job")

# How often a micro-batch is triggered. "0 seconds" (or omitting a
# trigger entirely) processes as fast as possible; setting this gives you
# a predictable batch cadence, which is easier to reason about when you're
# checking against the <2s detection latency target.
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "1 second")