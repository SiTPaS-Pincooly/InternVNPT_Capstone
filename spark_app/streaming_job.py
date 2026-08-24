"""
Main Spark Structured Streaming entrypoint for the Streaming IDS.

Pipeline:
    Kafka -> JSON parsing -> validity classification
         -> rule evaluation -> ClickHouse foreachBatch sink

The job intentionally keeps orchestration here and delegates detection
logic to rules_engine.py and storage to clickhouse_writer.py.

Malformed-record handling
-------------------------
An earlier version of this file assumed `from_json` returns a NULL struct
for an unparseable message, and filtered on `_parsed.isNotNull()`. It does
not: in PERMISSIVE mode (the default) Spark returns a NON-NULL struct whose
fields are all null. `isNotNull()` on that is true, so the filter passed
everything through and the malformed counter read zero forever.

Records are now classified by what actually arrived (see schema.py's
REQUIRED_FIELDS), into three buckets:

    valid                     -> every required field present
    __malformed_incomplete__  -> parsed as JSON, but a required field is
                                 missing or mistyped        (built wrong)
    __malformed_unparseable__ -> nothing parsed at all       (sent wrong)

Both malformed buckets are counted into `traffic_counts` under those
reserved pseudo-labels, so the malformed rate is chartable in Superset next
to real traffic. Neither is ever written to `detections`.

Cost of the counter
-------------------
It costs one extra *column*, not one extra pass. The classification is a
projection fused into the same stage as the JSON parse, and the two buckets
are counted by the `traffic_counts` groupBy that clickhouse_writer.py
already performs - no separate aggregation.

Net effect is fewer Spark actions per micro-batch, not more:

    before: malformed agg + isEmpty + counts + detections  = 4 actions
    after:  counts + detections                            = 2 actions

Malformed rows are carried through rule evaluation rather than filtered out
first, which avoids an extra filter+branch. That is safe precisely because
rules_engine wraps every condition in coalesce(..., False): an all-null row
evaluates to is_detection=false, never null. The writer then drops them on
`_status`.
"""

from __future__ import annotations

import logging
from functools import reduce
from operator import or_

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

import clickhouse_writer
import config
from rules_engine import evaluate, load_rules
from schema import (
    ALL_FIELDS,
    MALFORMED_INCOMPLETE,
    MALFORMED_UNPARSEABLE,
    NSL_KDD_SCHEMA,
    REQUIRED_FIELDS,
    STATUS_VALID,
)


LOGGER = logging.getLogger("streaming_ids")

#: Column names this module adds alongside the NSL-KDD fields.
STATUS_COLUMN = "_status"
COUNT_LABEL_COLUMN = "_count_label"


def build_spark() -> SparkSession:
    """Create the Spark session used by the streaming job."""
    spark = (
        SparkSession.builder
        .appName(config.SPARK_APP_NAME)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_kafka_stream(spark: SparkSession) -> DataFrame:
    """Create the raw Kafka streaming DataFrame."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", config.KAFKA_STARTING_OFFSETS)
        .load()
    )


def _status_column() -> Column:
    """Classify a parsed record as valid / incomplete / unparseable.

    Order matters: unparseable is checked first, because a record that
    parsed to nothing at all also trivially has required fields missing,
    and would otherwise be miscounted as merely incomplete.
    """
    # How many of the 43 fields survived parsing. Zero means from_json got
    # nothing usable out of the message at all.
    fields_present = reduce(
        lambda a, b: a + b,
        [F.col(f"_parsed.{name}").isNotNull().cast("int") for name in ALL_FIELDS],
    )

    nothing_parsed = F.col("_parsed").isNull() | (fields_present == F.lit(0))

    required_missing = reduce(
        or_,
        [F.col(f"_parsed.{name}").isNull() for name in REQUIRED_FIELDS],
    )

    return (
        F.when(nothing_parsed, F.lit(MALFORMED_UNPARSEABLE))
        .when(required_missing, F.lit(MALFORMED_INCOMPLETE))
        .otherwise(F.lit(STATUS_VALID))
    )


def parse_messages(kafka_df: DataFrame) -> DataFrame:
    """Parse Kafka values into NSL-KDD records and classify their validity.

    Returns the 43 schema columns flattened out, plus:
      `_status`      - valid / __malformed_incomplete__ / __malformed_unparseable__
      `_count_label` - what this row counts as in `traffic_counts`: its real
                       `label` when valid, otherwise its malformed bucket.
                       Lets a single groupBy cover real traffic and
                       malformed records together.
    """
    parsed = kafka_df.select(
        F.from_json(F.col("value").cast("string"), NSL_KDD_SCHEMA).alias("_parsed")
    ).withColumn(STATUS_COLUMN, _status_column())

    return parsed.select(
        F.col("_parsed.*"),
        F.col(STATUS_COLUMN),
        F.when(F.col(STATUS_COLUMN) == F.lit(STATUS_VALID), F.col("_parsed.label"))
        .otherwise(F.col(STATUS_COLUMN))
        .alias(COUNT_LABEL_COLUMN),
    )


def make_batch_writer(rules):
    """Build the foreachBatch callback.

    A closure keeps the validated rules loaded once at startup rather than
    re-reading rules.json for every micro-batch, and carries the cumulative
    malformed totals for the life of the process.
    """
    totals = {MALFORMED_UNPARSEABLE: 0, MALFORMED_INCOMPLETE: 0}

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        # Rules are evaluated over the whole batch, malformed rows included.
        # They cannot produce a detection: every condition is coalesced to
        # False in rules_engine, so an all-null row yields is_detection=false.
        # write_batch() then excludes them from `detections` on `_status`.
        evaluated_df = evaluate(batch_df, rules)

        # write_batch returns the per-label counts it already collected for
        # `traffic_counts`, so logging costs no additional Spark action.
        counts = clickhouse_writer.write_batch(evaluated_df, batch_id)

        unparseable = counts.get(MALFORMED_UNPARSEABLE, 0)
        incomplete = counts.get(MALFORMED_INCOMPLETE, 0)
        totals[MALFORMED_UNPARSEABLE] += unparseable
        totals[MALFORMED_INCOMPLETE] += incomplete

        valid = sum(
            n for label, n in counts.items()
            if label not in (MALFORMED_UNPARSEABLE, MALFORMED_INCOMPLETE)
        )

        if unparseable or incomplete:
            total_seen = valid + unparseable + incomplete
            LOGGER.warning(
                "[batch %d] malformed: %d unparseable (transmission), "
                "%d incomplete (generation/schema) of %d records (%.3f%%); "
                "cumulative %d unparseable / %d incomplete",
                batch_id, unparseable, incomplete, total_seen,
                100.0 * (unparseable + incomplete) / max(total_seen, 1),
                totals[MALFORMED_UNPARSEABLE], totals[MALFORMED_INCOMPLETE],
            )
        elif valid:
            LOGGER.info("[batch %d] %d records, none malformed", batch_id, valid)

    return process_batch


def main() -> None:
    """Start and block on the Structured Streaming query."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    spark = build_spark()

    try:
        LOGGER.info("Loading rules from %s", config.RULES_PATH)
        rules = load_rules(config.RULES_PATH)
        LOGGER.info("Loaded %d detection rule(s)", len(rules))

        LOGGER.info(
            "Connecting to ClickHouse at %s:%d/%s",
            config.CLICKHOUSE_HOST,
            config.CLICKHOUSE_HTTP_PORT,
            config.CLICKHOUSE_DATABASE,
        )
        clickhouse_writer.ensure_tables()
        LOGGER.info("ClickHouse tables are ready")

        kafka_df = build_kafka_stream(spark)
        parsed_df = parse_messages(kafka_df)

        process_batch = make_batch_writer(rules)

        query = (
            parsed_df.writeStream
            .foreachBatch(process_batch)
            .option("checkpointLocation", config.CHECKPOINT_LOCATION)
            .trigger(processingTime=config.TRIGGER_INTERVAL)
            .start()
        )

        LOGGER.info(
            "Streaming IDS started: topic=%s, offsets=%s, trigger=%s",
            config.KAFKA_TOPIC,
            config.KAFKA_STARTING_OFFSETS,
            config.TRIGGER_INTERVAL,
        )

        query.awaitTermination()

    except KeyboardInterrupt:
        LOGGER.info("Stopping Streaming IDS")
    finally:
        spark.stop()
        LOGGER.info("Spark session stopped")


if __name__ == "__main__":
    main()
