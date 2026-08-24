"""
schema.py

Defines the Spark StructType that mirrors the exact JSON shape produced by
producer/generate_traffic.py: the 41 NSL-KDD features, plus `timestamp`
and `label`.

This schema is used with `from_json(col("value"), NSL_KDD_SCHEMA)` in
streaming_job.py to turn each raw Kafka message (a JSON string) into typed,
queryable Spark columns.

Type mapping notes
-------------------
- Rate features (serror_rate, same_srv_rate, all dst_host_*_rate, ...) are
  DoubleType: they're numpy floats clipped to [0, 1] in the generator.
- `duration` is DoubleType, not IntegerType: the generator produces it via
  `.round(2)` (e.g. 12.34), so typing it as an int would make from_json
  silently return null on every row instead of raising an error.
- Byte/count fields (src_bytes, dst_bytes, count, dst_host_count, ...) are
  IntegerType: the generator casts these with `.astype(int)` or
  `rng.integers(...)`.
- `protocol_type`, `service`, `flag`, `label` are StringType (categorical).
- `timestamp` is DoubleType: it comes from Python's `time.time()`, a Unix
  epoch float in seconds. Cast to TimestampType in streaming_job.py with
  `to_timestamp(col("timestamp"))` if you need it as a real timestamp
  column for windowing.
- Every field is nullable=True. A malformed or partial JSON message should
  produce nulls in from_json rather than crash the whole stream — better
  to catch/filter bad rows downstream than lose the pipeline over one
  message.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

NSL_KDD_SCHEMA = StructType([
    # --- basic connection features ---
    StructField("duration", DoubleType(), True),
    StructField("protocol_type", StringType(), True),
    StructField("service", StringType(), True),
    StructField("flag", StringType(), True),
    StructField("src_bytes", IntegerType(), True),
    StructField("dst_bytes", IntegerType(), True),
    StructField("land", IntegerType(), True),
    StructField("wrong_fragment", IntegerType(), True),
    StructField("urgent", IntegerType(), True),

    # --- content features ---
    StructField("hot", IntegerType(), True),
    StructField("num_failed_logins", IntegerType(), True),
    StructField("logged_in", IntegerType(), True),
    StructField("num_compromised", IntegerType(), True),
    StructField("root_shell", IntegerType(), True),
    StructField("su_attempted", IntegerType(), True),
    StructField("num_root", IntegerType(), True),
    StructField("num_file_creations", IntegerType(), True),
    StructField("num_shells", IntegerType(), True),
    StructField("num_access_files", IntegerType(), True),
    StructField("num_outbound_cmds", IntegerType(), True),
    StructField("is_host_login", IntegerType(), True),
    StructField("is_guest_login", IntegerType(), True),

    # --- traffic features (2-second time window) ---
    StructField("count", IntegerType(), True),
    StructField("srv_count", IntegerType(), True),
    StructField("serror_rate", DoubleType(), True),
    StructField("srv_serror_rate", DoubleType(), True),
    StructField("rerror_rate", DoubleType(), True),
    StructField("srv_rerror_rate", DoubleType(), True),
    StructField("same_srv_rate", DoubleType(), True),
    StructField("diff_srv_rate", DoubleType(), True),
    StructField("srv_diff_host_rate", DoubleType(), True),

    # --- host-based traffic features ---
    StructField("dst_host_count", IntegerType(), True),
    StructField("dst_host_srv_count", IntegerType(), True),
    StructField("dst_host_same_srv_rate", DoubleType(), True),
    StructField("dst_host_diff_srv_rate", DoubleType(), True),
    StructField("dst_host_same_src_port_rate", DoubleType(), True),
    StructField("dst_host_srv_diff_host_rate", DoubleType(), True),
    StructField("dst_host_serror_rate", DoubleType(), True),
    StructField("dst_host_srv_serror_rate", DoubleType(), True),
    StructField("dst_host_rerror_rate", DoubleType(), True),
    StructField("dst_host_srv_rerror_rate", DoubleType(), True),

    # --- added beyond the raw NSL-KDD 41 features ---
    StructField("timestamp", DoubleType(), True),   # Unix epoch seconds
    StructField("label", StringType(), True),         # normal / attack type
])


# --------------------------------------------------------------------------
# Record validity contract
# --------------------------------------------------------------------------
# These live here, next to the schema, because they describe the record's
# shape contract - which fields a well-formed message must carry, and what
# we call a message that doesn't. streaming_job.py classifies with them,
# clickhouse_writer.py stores the counts, and evaluate_accuracy.py has to
# exclude them from its denominators.

#: Fields generate_traffic.py emits on EVERY record, whatever the class.
#: If one of these is null after from_json, the message is not usable.
#:
#: Why these five: `timestamp` and `label` are stamped unconditionally in
#: generate_batch(); `protocol_type`, `service` and `flag` are the three
#: categorical features, and are the only fields where a type error shows
#: up as a null rather than as a plausible-looking zero. A numeric feature
#: that arrives corrupt nulls out too, but a rule reading it would then
#: simply not match - it can't cause a false detection.
REQUIRED_FIELDS = ("timestamp", "protocol_type", "service", "flag", "label")

ALL_FIELDS = tuple(f.name for f in NSL_KDD_SCHEMA.fields)

#: Value of the `_status` column for a usable record.
STATUS_VALID = "valid"

#: Reserved pseudo-labels written into `traffic_counts` so the malformed
#: rate is chartable in Superset alongside real traffic volume. The double
#: underscores mark them as not-a-real-traffic-class: anything computing a
#: rate over traffic MUST exclude labels matching `__%__` or the
#: denominators are wrong.
#:
#: The split is a diagnostic, not bookkeeping - the two buckets fail in
#: different places, so which one moves tells you where to look:
#:
#:   unparseable -> the bytes on the wire were not valid JSON at all.
#:                  Nothing survived. That is a TRANSMISSION or
#:                  serialization fault: truncated Kafka message, encoding
#:                  mismatch, a producer that crashed mid-write, a
#:                  non-IDS producer writing to the same topic.
#:
#:   incomplete  -> the bytes parsed as JSON, but a required field is
#:                  missing or arrived as the wrong type. The message got
#:                  here intact; it was wrong when it was BUILT. That is a
#:                  GENERATION fault or schema drift - generate_traffic.py
#:                  and schema.py have diverged.
#:
#: A transport problem and a schema problem look identical in a single
#: counter, and have completely different fixes. Hence two.
MALFORMED_UNPARSEABLE = "__malformed_unparseable__"
MALFORMED_INCOMPLETE = "__malformed_incomplete__"

MALFORMED_LABELS = (MALFORMED_UNPARSEABLE, MALFORMED_INCOMPLETE)

#: SQL predicate for excluding the pseudo-labels from a metrics query.
#: evaluate_accuracy.py should append this to every aggregate it runs
#: against `traffic_counts`.
REAL_TRAFFIC_ONLY_SQL = "label NOT LIKE '\\_\\_%\\_\\_'"