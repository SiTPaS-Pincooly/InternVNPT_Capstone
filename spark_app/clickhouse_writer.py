"""
clickhouse_writer.py

foreachBatch sink for streaming_job.py. Writes two tables per micro-batch:

1. `detections` - full-feature rows, but ONLY where the row is worth
   keeping for accuracy scoring later: either it's a real attack
   (label != "normal") or the rules engine flagged it (is_detection).
   Correctly-ignored normal traffic is NOT stored here - that's the
   deliberate lightweight design (see #2 below for why that's still
   enough for full metrics).

2. `traffic_counts` - one tiny aggregate row per (batch, label): just a
   count. This exists because `detections` alone can compute precision
   and recall (every attack row and every flagged row is in there), but
   NOT false-positive rate, since correctly-ignored normal traffic isn't
   stored anywhere - there's no denominator for "false positives out of
   how many normal packets total." `traffic_counts` supplies that
   denominator at near-zero storage cost (counts, not raw rows), and
   evaluate_accuracy.py (the report script) reads both tables.

Uses clickhouse-connect (HTTP, port 8123 - same client Superset's
container uses) rather than Spark's JDBC writer. This sidesteps needing
the ClickHouse JDBC driver jar on Spark's classpath (a genuinely fiddly
setup step); each micro-batch is small enough at the 1 GB/min demo cap
that collecting it to the driver via toPandas() is not a bottleneck.

Requires: pip install clickhouse-connect --break-system-packages
(needs to be added to requirements.txt alongside confluent-kafka/orjson
if it isn't already there).

Latency tracking
-----------------
`detections` also carries `processed_at` and `latency_seconds`, so
evaluate_accuracy.py can report a real, measured number against the <2s
target instead of just asserting it was met. These are stamped once per
micro-batch (a single `time.time()` call covering every row in the
batch), not per row - Spark's own `current_timestamp()` only has
whole-second granularity in most builds, and a single batch-level
timestamp is the standard, accurate-enough approximation for
micro-batch systems at a ~1s trigger cadence. `latency_seconds` is
computed against each row's own `timestamp` (when generate_traffic.py
emitted it), so it reflects true producer-to-storage latency, not just
Spark's internal batch time.
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from typing import Optional

import clickhouse_connect
from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import config
from schema import NSL_KDD_SCHEMA, STATUS_VALID

DETECTIONS_TABLE = config.CLICKHOUSE_TABLE  # "detections" by default
COUNTS_TABLE = "traffic_counts"

# Columns streaming_job.parse_messages() attaches. Referenced by name rather
# than imported from streaming_job to keep the dependency one-directional
# (streaming_job -> clickhouse_writer, never back).
STATUS_COLUMN = "_status"
COUNT_LABEL_COLUMN = "_count_label"

_SPARK_TO_CLICKHOUSE_TYPE = {
    "double": "Float64",
    "integer": "Int32",
    "string": "String",
}

_client = None  # lazily created, reused across batches


def get_client():
    """One shared clickhouse-connect client per process, created on first use."""
    global _client
    if _client is None:
        _client = clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_HTTP_PORT,
            username=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DATABASE,
        )
    return _client


# --------------------------------------------------------------------------
# Table creation
# --------------------------------------------------------------------------

def _detections_ddl() -> str:
    """Build the `detections` table DDL from NSL_KDD_SCHEMA directly, so it
    can never drift out of sync with schema.py - add a feature there and
    it shows up here automatically."""
    feature_cols = [
        f"    `{f.name}` {_SPARK_TO_CLICKHOUSE_TYPE[f.dataType.typeName()]}"
        for f in NSL_KDD_SCHEMA.fields
    ]
    extra_cols = [
        "    `matched_rule_ids` Array(String)",
        "    `matched_attack_types` Array(String)",
        "    `max_severity` Nullable(String)",
        "    `is_detection` Bool",
        "    `batch_id` Int64",
        "    `processed_at` DateTime64(3)",
        "    `latency_seconds` Float64",
    ]
    all_cols = ",\n".join(feature_cols + extra_cols)
    return f"""
    CREATE TABLE IF NOT EXISTS {DETECTIONS_TABLE} (
{all_cols}
    )
    ENGINE = MergeTree
    ORDER BY (label, timestamp)
    """.strip()


def _counts_ddl() -> str:
    return f"""
    CREATE TABLE IF NOT EXISTS {COUNTS_TABLE} (
        `batch_id` Int64,
        `batch_time` DateTime64(3),
        `label` String,
        `record_count` UInt64
    )
    ENGINE = MergeTree
    ORDER BY (label, batch_time)
    """.strip()


def ensure_tables(client=None) -> None:
    """Idempotent - safe to call every time streaming_job.py starts."""
    client = client or get_client()
    client.command(f"CREATE DATABASE IF NOT EXISTS {config.CLICKHOUSE_DATABASE}")
    client.command(_detections_ddl())
    client.command(_counts_ddl())


# --------------------------------------------------------------------------
# Per-batch write logic (the foreachBatch function)
# --------------------------------------------------------------------------

def _prepare_detections(batch_df: DataFrame, batch_id: int) -> DataFrame:
    """Keep only rows worth storing: real attacks or flagged rows.

    Malformed records are excluded here rather than upstream. streaming_job
    carries them through rule evaluation (harmlessly - rules_engine coalesces
    every condition to False, so an all-null row can never be flagged) so
    that one groupBy can count them alongside real traffic. This is where
    they get dropped, on `_status`.
    """
    feature_names = [f.name for f in NSL_KDD_SCHEMA.fields]
    keep_cols = feature_names + [
        "matched_rule_ids",
        "matched_attack_types",
        "max_severity",
        "is_detection",
    ]

    worth_storing = (F.col("label") != "normal") | (F.col("is_detection"))
    if STATUS_COLUMN in batch_df.columns:
        worth_storing = (F.col(STATUS_COLUMN) == F.lit(STATUS_VALID)) & worth_storing

    return (
        batch_df.filter(worth_storing)
        .select(*keep_cols)
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
    )


def _prepare_counts(batch_df: DataFrame, batch_id: int) -> DataFrame:
    """One row per label present in this batch: just a count.

    Groups on `_count_label` when streaming_job supplied it, which is the
    record's real `label` for valid rows and its malformed pseudo-label
    otherwise. That makes this single aggregation the counter for BOTH real
    traffic volume and the malformed rate - no second pass, no second table.

    Falls back to plain `label` so the offline self-test and any caller that
    hasn't been through parse_messages() still works.

    NOTE for evaluate_accuracy.py: rows whose label matches `__%__` are
    malformed counters, NOT traffic. Exclude them from every denominator or
    the false-positive rate is computed against the wrong total. Use
    schema.REAL_TRAFFIC_ONLY_SQL.
    """
    group_col = COUNT_LABEL_COLUMN if COUNT_LABEL_COLUMN in batch_df.columns else "label"
    return (
        batch_df.groupBy(F.col(group_col).alias("label"))
        .count()
        .withColumnRenamed("count", "record_count")
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
        .withColumn("batch_time", F.current_timestamp())
    )


def _to_detections_pdf(detections_df: DataFrame):
    """Collect the detections DataFrame to pandas and stamp processed_at /
    latency_seconds. Split out from write_batch so this can be sanity
    checked without a live ClickHouse connection (see self-test below).
    """
    det_pdf = detections_df.toPandas()
    if det_pdf.empty:
        return det_pdf

    now = time.time()
    det_pdf["processed_at"] = datetime.now(timezone.utc)
    det_pdf["latency_seconds"] = now - det_pdf["timestamp"].astype(float)
    return det_pdf


def write_batch(batch_df: DataFrame, batch_id: int) -> dict[str, int]:
    """The function passed to `.foreachBatch(write_batch)` in streaming_job.py.

    Both writes are skipped (not just no-op'd loudly) when a batch has
    nothing relevant - normal streaming micro-batches at low attack
    ratios will often have zero rows worth writing to `detections`, and
    that's expected, not an error.

    Returns the per-label counts already collected for `traffic_counts`,
    including the malformed pseudo-labels, so streaming_job.py can log the
    malformed rate without triggering a second Spark action for it.
    """
    # MEMORY_AND_DISK, not the default MEMORY_ONLY: an oversized batch then
    # spills to disk instead of pushing the driver into OutOfMemoryError.
    # Slower in that case, but a slow batch is recoverable and a dead JVM
    # is not. config.MAX_OFFSETS_PER_TRIGGER is the primary guard; this is
    # the backstop.
    batch_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        client = get_client()

        detections = _prepare_detections(batch_df, batch_id)
        det_pdf = _to_detections_pdf(detections)
        if not det_pdf.empty:
            client.insert_df(DETECTIONS_TABLE, det_pdf)

        counts = _prepare_counts(batch_df, batch_id)
        counts_pdf = counts.toPandas()
        if not counts_pdf.empty:
            client.insert_df(COUNTS_TABLE, counts_pdf)
            return dict(
                zip(counts_pdf["label"], counts_pdf["record_count"].astype(int))
            )
        return {}
    finally:
        batch_df.unpersist()


# --------------------------------------------------------------------------
# Standalone sanity check.
#
#   python spark_app/clickhouse_writer.py
#       Offline. Exercises the DataFrame prep logic - which rows get kept,
#       how counts aggregate - and prints the DDL. Needs no database.
#
#   python spark_app/clickhouse_writer.py --live
#       Everything above, PLUS the real round-trip against a running
#       ClickHouse: ensure_tables(), insert_df() into both tables, read the
#       rows back, then delete them again. This is the one code path that
#       cannot be verified without a server.
#
# The --live rows are tagged with batch_id = -1 (a value no real batch ever
# uses) so they can be deleted afterwards without touching real data.
# --------------------------------------------------------------------------

TEST_BATCH_ID = -1


def _live_round_trip(kept_pdf, counts_pdf) -> int:
    """Insert the self-test fixture into a running ClickHouse, read it back,
    then remove it. Returns 0 on success, non-zero on failure."""
    print("\n" + "=" * 70)
    print("LIVE ROUND-TRIP")
    print("=" * 70)
    print(f"host={config.CLICKHOUSE_HOST}:{config.CLICKHOUSE_HTTP_PORT} "
          f"db={config.CLICKHOUSE_DATABASE} user={config.CLICKHOUSE_USER}")

    try:
        client = get_client()
        client.command("SELECT 1")
    except Exception as exc:
        print(f"\nFAIL  cannot connect: {exc}")
        print("      Is the stack up?   docker compose ps")
        print("      Code 194 means the password in config.py does not match")
        print("      CLICKHOUSE_PASSWORD in docker-compose.yml.")
        return 2

    print("  ok  connected")

    try:
        ensure_tables(client)
        tables = {r[0] for r in client.query(
            f"SHOW TABLES FROM {config.CLICKHOUSE_DATABASE}").result_rows}
        print(f"  ok  ensure_tables() -> {sorted(tables)}")
    except Exception as exc:
        print(f"\nFAIL  ensure_tables(): {exc}")
        return 3

    # Re-tag the fixture so the cleanup below can find exactly these rows.
    kept_pdf = kept_pdf.copy()
    counts_pdf = counts_pdf.copy()
    kept_pdf["batch_id"] = TEST_BATCH_ID
    counts_pdf["batch_id"] = TEST_BATCH_ID

    try:
        client.insert_df(DETECTIONS_TABLE, kept_pdf)
        print(f"  ok  insert_df({DETECTIONS_TABLE}) -> {len(kept_pdf)} rows")
    except Exception as exc:
        print(f"\nFAIL  insert_df({DETECTIONS_TABLE}): {exc}")
        print("      Common causes: a NaN in a non-nullable Float64 column,")
        print("      the timezone-aware processed_at vs DateTime64(3), or a")
        print("      Python list not mapping onto Array(String).")
        return 4

    try:
        client.insert_df(COUNTS_TABLE, counts_pdf)
        print(f"  ok  insert_df({COUNTS_TABLE}) -> {len(counts_pdf)} rows")
    except Exception as exc:
        print(f"\nFAIL  insert_df({COUNTS_TABLE}): {exc}")
        return 5

    # Read back and check the values survived the trip intact - an insert
    # that succeeds but mangles arrays or nulls is worse than one that fails.
    det_back = client.query(
        f"SELECT label, matched_rule_ids, max_severity, is_detection "
        f"FROM {DETECTIONS_TABLE} WHERE batch_id = {TEST_BATCH_ID} ORDER BY label"
    ).result_rows
    cnt_back = client.query(
        f"SELECT label, record_count FROM {COUNTS_TABLE} "
        f"WHERE batch_id = {TEST_BATCH_ID} ORDER BY label"
    ).result_rows

    print(f"\n  read back from {DETECTIONS_TABLE}:")
    for r in det_back:
        print(f"    {r}")
    print(f"  read back from {COUNTS_TABLE}:")
    for r in cnt_back:
        print(f"    {r}")

    checks = [
        (f"{DETECTIONS_TABLE} row count", len(det_back) == len(kept_pdf)),
        (f"{COUNTS_TABLE} row count", len(cnt_back) == len(counts_pdf)),
        ("Array(String) survived", any(isinstance(r[1], (list, tuple)) and r[1]
                                       for r in det_back)),
        ("Bool survived", all(isinstance(r[3], (bool, int)) for r in det_back)),
    ]

    print()
    failed = 0
    for name, ok in checks:
        print(f"  {'ok  ' if ok else 'FAIL'}  {name}")
        failed += 0 if ok else 1

    # Clean up so the test fixture never pollutes real metrics.
    try:
        for table in (DETECTIONS_TABLE, COUNTS_TABLE):
            client.command(
                f"ALTER TABLE {table} DELETE WHERE batch_id = {TEST_BATCH_ID}")
        print(f"\n  ok  cleanup: test rows (batch_id={TEST_BATCH_ID}) deleted")
        print("      (ClickHouse mutations are async; they may take a moment "
              "to disappear from SELECTs)")
    except Exception as exc:
        print(f"\n  warn  cleanup failed: {exc}")
        print(f"        Remove manually: ALTER TABLE {DETECTIONS_TABLE} "
              f"DELETE WHERE batch_id = {TEST_BATCH_ID}")

    print("\n" + ("LIVE ROUND-TRIP PASSED" if not failed
                  else f"{failed} CHECK(S) FAILED"))
    return 1 if failed else 0


if __name__ == "__main__":
    import argparse

    from pyspark.sql import SparkSession

    _parser = argparse.ArgumentParser(
        description="Self-test for clickhouse_writer.py.")
    _parser.add_argument(
        "--live", action="store_true",
        help="also insert into a running ClickHouse, read back, and clean up")
    _args = _parser.parse_args()

    spark = SparkSession.builder.appName("clickhouse_writer_selftest").master("local[1]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("--- detections DDL ---")
    print(_detections_ddl())
    print("\n--- traffic_counts DDL ---")
    print(_counts_ddl())

    feature_names = [f.name for f in NSL_KDD_SCHEMA.fields]
    # Backdate "timestamp" by ~0.4s so latency_seconds comes out as a
    # small positive number, like a real record would show.
    fake_emit_time = time.time() - 0.4

    def _base_row():
        return {
            f: (fake_emit_time if f == "timestamp" else
                0.0 if NSL_KDD_SCHEMA[f].dataType.typeName() == "double" else
                (0 if NSL_KDD_SCHEMA[f].dataType.typeName() == "integer" else "x"))
            for f in feature_names
        }

    rows = [
        {**_base_row(), "label": "ddos", "matched_rule_ids": ["ddos_flood"],
         "matched_attack_types": ["ddos"], "max_severity": "high", "is_detection": True},
        {**_base_row(), "label": "normal", "matched_rule_ids": [], "matched_attack_types": [],
         "max_severity": None, "is_detection": False},
        {**_base_row(), "label": "normal", "matched_rule_ids": ["port_scan_sweep"],
         "matched_attack_types": ["port_scan"], "max_severity": "medium",
         "is_detection": True},  # false positive: labeled normal, flagged anyway
    ]
    df = spark.createDataFrame(rows)

    kept = _prepare_detections(df, batch_id=1)
    print(f"\n--- rows kept for detections table: {kept.count()} of {df.count()} total ---")
    kept.select("label", "matched_rule_ids", "is_detection", "batch_id").show(truncate=False)

    kept_pdf = _to_detections_pdf(kept)
    print("--- pandas frame with processed_at / latency_seconds stamped ---")
    print(kept_pdf[["label", "timestamp", "processed_at", "latency_seconds"]].to_string(index=False))

    counts = _prepare_counts(df, batch_id=1)
    print("--- traffic_counts rows (denominator for FP rate later) ---")
    counts.select("label", "record_count", "batch_id").show(truncate=False)
    counts_pdf = counts.toPandas()

    spark.stop()

    if _args.live:
        sys.exit(_live_round_trip(kept_pdf, counts_pdf))

    print("\nOffline checks done. To test against a running ClickHouse:")
    print("    python spark_app/clickhouse_writer.py --live")