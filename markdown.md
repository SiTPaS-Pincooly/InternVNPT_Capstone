# Streaming IDS — Capstone Project Status

*Last updated: 2026-08-25 — first successful end-to-end run*

## Overview
A rule-based real-time Intrusion Detection System (IDS) built on Apache Kafka and Apache Spark Structured Streaming. Simulated network traffic (NSL-KDD schema) is streamed through Kafka, evaluated against a JSON rule file in Spark, and flagged detections are written to a storage layer for dashboarding — plus a lightweight accuracy-scoring layer for the capstone report.

**Target detection latency:** under 2 seconds, end to end — **measured at ~3.3 s and shown to be unreachable in this architecture**; see "Why the <2s latency target is unreachable" for the analysis and comparison with how production IDS solve it.

## Architecture
```
generate_traffic.py  →  Kafka topic  →  Spark Structured Streaming  →  ClickHouse  →  Superset
 (NSL-KDD JSON,          (network-       (schema.py, rules_engine.py,   (storage)     (dashboards)
  95% normal /            traffic)        streaming_job.py,
  5% attack)                              clickhouse_writer.py)
                                                    │
                                                    └──→ evaluate_accuracy.py (report script) → report/accuracy.md

rules/validate_rules.py — offline side-channel: generator → rules_engine directly,
                          no Kafka/ClickHouse. Dev-time rule tuning only.
```

## Key decisions made so far

| Area | Decision | Why |
|---|---|---|
| Storage | ClickHouse (over StarRocks) | Append-only event log fits `MergeTree` engine; standard choice for security/log analytics; single-binary deploy is far simpler than StarRocks' FE/BE setup for a solo capstone |
| Dashboard | Superset (replacing Streamlit) | Native ClickHouse connector, no custom dashboard code needed |
| Superset image | Lightweight wrapper (`FROM apache/superset:latest` + `pip install clickhouse-connect`) | Resolves Open Problem #1 (see below) — avoids touching the separate, unrelated full-source Superset build that already exists locally |
| Detection approach | Rule-based (JSON rule file), with Random Forest ML as a possible stretch goal | Rules are explainable and fast to build; already have a labeled dataset if ML gets added later |
| Attack types covered | DDoS, port scanning, brute force, malware phoning home (C2), data exfiltration, malicious file download | Six categories, each mapped to a distinct NSL-KDD feature signature |
| Rule evaluation | Rules match on **features only, never on `label`** | A real IDS doesn't see ground truth; `label` is reserved exclusively for scoring accuracy afterward, not for detection itself. Now enforced in code: `label` is a validation-rejected rule field |
| Detection/orchestration split | `rules_engine.py` holds detection logic only; `streaming_job.py` holds Kafka/Spark/ClickHouse plumbing only | Keeps the rules testable with no broker and no database running — which is exactly what `validate_rules.py` exploits |
| Malformed records | Classified into **two** buckets (`unparseable` vs `incomplete`), counted in the existing `traffic_counts` groupBy | A transport fault and a schema-drift fault look identical in a single counter but have completely different fixes. Splitting them makes the counter diagnostic instead of merely informational — see below |
| Malformed counting cost | Folded into the `traffic_counts` aggregation rather than given its own action or table | The counter costs one extra *column*, not one extra pass. Net effect was **fewer** Spark actions per micro-batch (4 → 2), not more |
| ClickHouse writer client | `clickhouse-connect` (HTTP, port 8123) instead of Spark's JDBC writer | Avoids needing the ClickHouse JDBC driver jar on Spark's classpath (fiddly to set up); each micro-batch is small enough at the demo throughput cap to collect via `toPandas()` without being a bottleneck |
| `detections` table contents | Store a row only if it's a real attack (`label != "normal"`) **or** flagged by the rules engine (`is_detection`) — not all traffic | Lighter than storing every row; still enough to compute precision and recall in full, since every attack and every flag ends up in the table either way |
| `traffic_counts` table | One small aggregate row per `(batch, label)` — just a count, not raw rows | `detections` alone can't compute false-positive rate, since correctly-ignored normal traffic is never stored there — no denominator. This table supplies that denominator at near-zero storage cost |

## Repo structure
```
streaming-ids/
├── producer/
│   └── generate_traffic.py         ✅ built, tested (benchmarked; label dtype bug fixed 2026-08-24)
├── spark_app/
│   ├── config.py                    ✅ built, tested (defaults verified, env var override confirmed)
│   ├── schema.py                     ✅ built, tested against real generator output
│   ├── rules_engine.py               ✅ built, tested against a real local Spark session
│   │                                    (7 validation cases + 13 evaluation assertions, all passing)
│   ├── clickhouse_writer.py          ✅ built, DDL + row-filtering logic tested locally — insert_df()
│   │                                    round-trip against a live ClickHouse NOT yet verified
│   └── streaming_job.py              ✅ built — Kafka → JSON parse → validity classification → rules
│                                        → ClickHouse. Malformed handling rewritten and tested 2026-08-24.
├── rules/
│   ├── rules.json                    ✅ built — six attack signatures, validated (see results below)
│   └── validate_rules.py             ✅ built, fixed and run 2026-08-24 — offline rule scorer
├── evaluation/
│   └── evaluate_accuracy.py          ✅ built and RUN against live ClickHouse — 1.61M records scored
├── tools/
│   ├── check_env.py                  ✅ host prerequisite checker (derives versions from PySpark)
│   └── kafka_probe.py                ✅ host-side Kafka consumer probe
├── report/
│   └── accuracy.md                   ✅ generated by evaluate_accuracy.py --markdown
├── clickhouse-init/
│   └── 01-create-database.sql        ✅ built — creates the `ids` database on first boot only
├── superset/
│   └── Dockerfile                     ✅ built (lightweight `FROM apache/superset:latest` + clickhouse-connect)
├── docker-compose.yml                 ✅ built and RUNNING — kafka+clickhouse+superset all healthy
├── .gitignore                         ✅ built, verified with git check-ignore (certs/ now deleted)
└── requirements.txt                   ✅ built — clickhouse-connect present, complete
```

**Infra decision:** dropped the two Azure VMs (`Capstone-Test`, `Kafka-Broker`) — everything now runs on the laptop (i5-13420H, 16GB RAM). Kafka, ClickHouse, and Superset run as local Docker containers via `docker-compose.yml`; `generate_traffic.py` connects to `localhost:9092` directly, no SSH/TLS/networking config needed. The `.pem` SSH keys are no longer part of the project (but are still sitting in `certs/` — delete them, especially before pushing to a public repo). Worth watching RAM headroom once Kafka + ClickHouse + Superset + Spark are all running locally at once — may need to stop containers not actively in use.

## `generate_traffic.py` — done

- Emits all 41 NSL-KDD features + `timestamp` + `label` as JSON.
- Each traffic class (normal + 6 attack types) has its own numpy-vectorized feature distribution, built from how that traffic actually behaves (e.g. DDoS → near-zero duration, high `serror_rate`; data exfiltration → large outbound `src_bytes`) rather than uniform random values.
- Scales horizontally via `multiprocessing`: one Kafka producer per worker process, each paced toward an even share of `--target-rate`.
- `--dry-run` mode benchmarks generation throughput with no Kafka broker needed.
- Measured record size: **~1,120 bytes/record** (serialized JSON).
- **Bug fixed 2026-08-24:** `generate_batch()` set `rec["label"] = label_arr[i]`, which is a `numpy.str_`, not a Python `str`. Every feature was already `.item()`-converted, so `label` was the single remaining numpy-typed value in each record. It serialized to JSON fine (numpy.str_ subclasses str), so the producer path never noticed — but it made `spark.createDataFrame(records)` fail with a `PickleException` on the JVM side, which is what broke `validate_rules.py`. Now `str(label_arr[i])`.

### Benchmark results

| Environment | Config | Measured throughput |
|---|---|---|
| Dev sandbox (1 vCPU, uncontended) | `--workers 1` | ~34,600 records/sec |
| Laptop — i5-13420H (8c/12t, hybrid P+E cores), 16GB RAM, NVMe, Windows | `--workers 8` | **~169,000 records/sec** (generation only, no Kafka) |

### Throughput target reassessed
- Original stretch target: 1,000,000 records/sec — requires ~55 CPU cores, 10GbE networking, and a multi-broker Kafka cluster; not achievable on personal hardware.
- Revised target: 200,000–300,000 records/sec — found to be borderline/short on the laptop above once Kafka overhead is added (realistic estimate: ~120,000–145,000 records/sec end-to-end).
- **Decision:** cap demo throughput intentionally at **1 GB/minute (~14,880 records/sec)** using `--workers 1 --target-rate 14880` (matches the script's built-in default), to avoid stressing the laptop during development; full-throttle numbers are documented above as the measured ceiling, not the live demo rate.

## `schema.py` — done

- Defines `NSL_KDD_SCHEMA`, a Spark `StructType` mirroring `generate_traffic.py`'s JSON exactly: 41 NSL-KDD features + `timestamp` + `label` (43 fields total).
- Verified with a real Spark session: generated actual records with `generate_batch`, serialized to JSON, parsed with `from_json(col("value"), NSL_KDD_SCHEMA)` — all fields parsed correctly with zero unexpected nulls.
- Key type decision: `duration` is `DoubleType` (not `IntegerType`), since the generator emits it as a rounded float — typing it as int would have made `from_json` silently null out every row.
- Field-for-field cross-check against `generate_traffic.py`'s `FEATURE_NAMES` confirmed: names and order match exactly.
- **Added 2026-08-24 — the record validity contract.** `REQUIRED_FIELDS`, `ALL_FIELDS`, `STATUS_VALID`, `MALFORMED_UNPARSEABLE`, `MALFORMED_INCOMPLETE`, `MALFORMED_LABELS`, and `REAL_TRAFFIC_ONLY_SQL` now live here, next to the schema, because they describe the record's *shape contract*. `streaming_job.py` classifies with them, `clickhouse_writer.py` stores the counts, and `evaluate_accuracy.py` must exclude them from its denominators — one definition, three consumers.

## Confirmed: generator field count
Directly verified `generate_traffic.py` produces the correct 41 NSL-KDD features (checked `FEATURE_NAMES` length and a live generated record's keys) — no fields missing, no unexpected extras.

## `config.py` — done

- Central settings for Kafka (`localhost:9092`, topic `network-traffic`, starting offsets), ClickHouse (host, HTTP port 8123, native port 9000, database `ids`, table `detections`, pre-built JDBC URL), `RULES_PATH`, `CHECKPOINT_LOCATION`, and `TRIGGER_INTERVAL` (default 1 second).
- Every value reads from an environment variable with a local-Docker default, so it doesn't need editing once `docker-compose.yml` exists — confirmed override behavior works (tested `KAFKA_TOPIC` override).
- Included `CHECKPOINT_LOCATION` proactively: Structured Streaming needs this to track processed Kafka offsets across restarts, easy to forget until the job crashes once.
- Note: `CLICKHOUSE_JDBC_URL` is defined but currently unused — `clickhouse_writer.py` connects via `clickhouse-connect` (HTTP) instead of Spark JDBC (see decision above). Harmless to leave in `config.py` in case a JDBC path is added later.

## `rules_engine.py` — done, tested

> **History note:** an earlier version of this document marked `rules_engine.py` as built and tested, but the file was never actually saved to disk — `streaming_job.py` and `validate_rules.py` were both importing a module that did not exist. Written and tested for real on 2026-08-24.

**Separation of concerns.** This module knows how to decide whether a record is an attack, and *nothing* about Kafka, Spark sessions, triggers, checkpoints, or ClickHouse. `streaming_job.py` owns all of that and calls in. That boundary is what makes `validate_rules.py` possible: the rules can be scored offline with no broker and no database running.

**Public surface:**
- `load_rules(path)` → validated `list[Rule]`, raises `RuleLoadError`
- `evaluate(df, rules)` → appends `matched_rule_ids`, `matched_attack_types`, `max_severity`, `is_detection`
- `parse_rules(raw)` / `compile_rules(rules)` exposed for testing

**Design points:**
- Validation is strict and fails loudly at startup rather than silently producing a rule that never matches: unknown field names (checked against `NSL_KDD_SCHEMA`), unknown operators, duplicate `rule_id`s, invalid severities, empty condition lists, and type-mismatched values (e.g. `"150"` against a numeric field) all raise `RuleLoadError` immediately.
- **`label` is a validation-rejected field.** Not a convention any more — a rule that tries to read ground truth is refused at load time with an explicit error.
- Supported operators: `>`, `>=`, `<`, `<=`, `==`, `!=`, `in`, `not_in`. Conditions within a rule are ANDed; rules are ORed; multiple rules can share an `attack_type` to express "either signature counts" (and `matched_attack_types` is deduplicated accordingly).
- **Nulls never match.** A malformed record that slipped through parsing with null features produces *null* comparisons in Spark, not `False`. Every condition is wrapped in `coalesce(..., False)` so one null field can't poison the AND chain.
- **No Python UDF.** Everything compiles to native Spark Column expressions (`when`, `array`, `filter`, `array_max`), so it stays inside Catalyst. A UDF would cross the JVM/Python boundary once per row — real overhead against the <2s latency target.
- `evaluate()` appends, never replaces: `label` and every other input column survive untouched for the writer to score against later.

**Test results** (real local Spark 3.5.1 session, `python rules_engine.py`):

| Group | Result |
|---|---|
| Validation rejects malformed rules | 7/7 pass — unknown field, `label` access, bad operator, bad severity, duplicate `rule_id`, empty conditions, type-mismatched value |
| Evaluation correctness | 13/13 pass |

Notable individual assertions: a DDoS-shaped row matching two DDoS rules got `max_severity = critical` (highest of the two) with `matched_attack_types` deduplicated to `["ddos"]`; a normal row matched nothing and got `max_severity = null`; a row with null features neither matched nor crashed; and the executed Catalyst plan was asserted to contain **no `BatchEvalPython` node**, confirming the no-UDF claim rather than just asserting it.

## `rules/validate_rules.py` — what it's for

**The short version:** it's a dev-time scorer that tells you whether `rules.json` actually catches attacks, without needing Docker, Kafka, or ClickHouse running.

**Why it exists.** The rules in `rules.json` are hand-tuned thresholds — `count >= 150`, `serror_rate >= 0.7`, and so on. Nothing about the pipeline tells you whether those numbers are *right*. Set a threshold slightly too high and that attack type is silently never detected; set it too low and normal traffic gets flagged constantly. Either way the pipeline runs perfectly and reports nonsense. This script closes that loop in about thirty seconds:

1. Generate N labeled records with `generate_traffic.py` (real generator, real distributions)
2. Run them straight through `rules_engine.evaluate()` — no Kafka, no Spark streaming, no database
3. Report, per attack type, what fraction was caught (**recall**), plus the **false-positive rate** on normal traffic

Because it imports `rules_engine` directly, it only works *because* detection and orchestration are separate modules. It is deliberately not the capstone accuracy report — that's `evaluate_accuracy.py`, which scores the real stream after it has been through Kafka, Spark and ClickHouse. This is the fast feedback loop you run **after every edit to `rules.json`**, before anything touches the pipeline.

Usage: `python rules/validate_rules.py --n 20000 --seed 42`

**Two bugs fixed 2026-08-24** (it had never successfully run):
- `REPO_ROOT` was `Path(__file__).resolve().parent`, but the file lives in `rules/` — so it looked for `rules/producer/` and `rules/spark_app/` and failed on import. Now `.parent.parent`.
- The `numpy.str_` label bug in `generate_traffic.py` (see above) crashed `spark.createDataFrame` with a `PickleException`.

### Rule validation results — first real run (n=20,000, seed=42, 95% normal)

| Attack type | Total | Caught | Recall |
|---|---:|---:|---:|
| brute_force | 167 | 167 | **100.0%** |
| ddos | 167 | 167 | **100.0%** |
| malware_c2 | 167 | 167 | **100.0%** |
| port_scan | 167 | 167 | **100.0%** |
| data_exfiltration | 166 | 161 | 97.0% |
| malicious_download | 166 | 156 | 94.0% |

**Normal traffic:** 19,000 rows, 130 false positives → **0.684% false-positive rate**

**Attribution consistency:** 0 detections where `matched_attack_types` failed to include the row's true label — i.e. every caught attack was attributed to the *correct* attack type, not just flagged as generically suspicious.

False positives by rule:

| Rule | FPs | Rate on normal traffic |
|---|---:|---:|
| `malicious_download_inbound` | 107 | 0.563% |
| `data_exfiltration_outbound` | 17 | 0.089% |
| `malware_c2_beacon` | 6 | 0.032% |

The two byte-volume rules account for ~95% of all false positives, and are also the only two rules with imperfect recall — both are pure size-threshold rules (`dst_bytes >= 65000` / `src_bytes >= 50000`), so they sit on the overlapping tail of the normal traffic distribution. That's the expected shape of the tradeoff and is worth writing up in the report as a precision/recall tuning discussion rather than "fixing" to 100%.

## `clickhouse_writer.py` — done (DDL/logic tested; live insert unverified)

- Two tables, written per micro-batch via `.foreachBatch(write_batch)`:
  - **`detections`** — full 43 NSL-KDD feature columns (DDL generated directly from `NSL_KDD_SCHEMA`, so it can't drift out of sync with `schema.py`) plus `matched_rule_ids`/`matched_attack_types` (`Array(String)`), `max_severity`, `is_detection`, `batch_id`, `processed_at`, `latency_seconds`. Only rows where `label != "normal"` **or** `is_detection` are kept.
  - **`traffic_counts`** — one row per `(batch, label)`: just a count. Supplies the denominator `detections` can't, so false-positive rate is computable later without storing every normal row. **As of 2026-08-24 it also carries the two malformed pseudo-labels**, so the malformed rate rides the same aggregation and the same table. `_prepare_counts()` groups on `_count_label` when `streaming_job` supplied it and falls back to plain `label` otherwise, so the offline self-test still works unchanged.
  - `_prepare_detections()` now also requires `_status == 'valid'`, so malformed rows can be carried through rule evaluation for counting without ever reaching `detections`.
  - `write_batch()` returns the per-label counts it collected, so the caller can log without a second Spark action.
- Uses `clickhouse-connect` (HTTP client, port 8123) rather than Spark's JDBC writer, to avoid needing the ClickHouse JDBC driver jar on Spark's classpath.
- `ensure_tables()` issues idempotent `CREATE TABLE IF NOT EXISTS` for both tables — safe to call on every `streaming_job.py` startup.
- `batch_df.persist()` / `unpersist()` wraps the write so the batch isn't recomputed twice (once per table write).
- Latency tracking: `processed_at` and `latency_seconds` are stamped once per micro-batch and computed against each row's own producer `timestamp`, so `evaluate_accuracy.py` can report a measured number against the <2s target rather than asserting it.
- Self-tested locally: a 3-row fixture (1 real attack, 1 correctly-ignored normal row, 1 false-positive) confirmed `detections` keeps exactly the 2 rows it should and drops the correctly-ignored one, while `traffic_counts` still reflects all 3. **Not yet tested against a live ClickHouse** — first thing to verify once `docker compose up -d` runs on the laptop.

## `streaming_job.py` — built, malformed handling rewritten

- Main Spark Structured Streaming entrypoint wiring Kafka → JSON parsing → validity classification → rule evaluation → ClickHouse. Orchestration only — all detection logic lives in `rules_engine.py`.
- Loads and validates `rules.json` once at startup, then reuses the validated rule set for every micro-batch (held in a closure, not re-read per batch).
- Calls `clickhouse_writer.ensure_tables()` before starting the streaming query so the ClickHouse database/tables exist before any batch is written.
- Reads the Kafka topic and starting-offset configuration directly from `config.py` (`network-traffic`, default `latest`).
- Parses Kafka message values with `NSL_KDD_SCHEMA` using Spark's native `from_json()` expression.
- Uses `foreachBatch()` with `clickhouse_writer.write_batch()`, checkpointing, and the configured 1-second processing trigger.
- Import chain verified (`rules_engine`, `clickhouse_writer`, `config`, `schema` all resolve). The complete Kafka/Spark/ClickHouse flow is **not yet verified against the live Docker stack**.

### Bug found and fixed 2026-08-24: the malformed counter always read zero

The original `parse_messages()` assumed `from_json` returns a **NULL struct** for an unparseable message, and filtered on `_parsed.isNotNull()`.

It doesn't. In PERMISSIVE mode — the default — Spark returns a **non-null struct whose fields are all null**. `isNotNull()` on that is `true`. Verified against Spark 3.5.1:

| Kafka message | old `_is_valid` | outcome |
|---|---|---|
| `not JSON at all` | `True` | passed straight through |
| valid JSON, most fields absent | `True` | passed straight through |
| valid JSON, wrong type on `count` | `True` | passed straight through |
| empty message | `False` | correctly dropped |

So `malformed_count` was permanently 0, nothing was ever filtered, and every garbage message reached the rule engine as a row of nulls. Only the `coalesce(..., False)` in `rules_engine.py` kept `is_detection` from arriving as SQL `NULL` at a non-nullable ClickHouse `Bool` column — load-bearing entirely by accident.

**Worth writing up in the report.** "The malformed-record counter that silently always read zero" is a concrete argument for why the end-to-end Docker run matters: every component passed its own unit test, and the bug still sat there.

## Malformed-record classification and counter

Records are now classified by *what actually arrived* — see `REQUIRED_FIELDS` in `schema.py` — into three buckets:

| `_status` | Meaning | Where the record broke |
|---|---|---|
| `valid` | every required field present | — |
| `__malformed_unparseable__` | nothing parsed at all; the bytes weren't JSON | **Transmission** — truncated Kafka message, encoding mismatch, producer crashed mid-write, or a foreign producer on the topic |
| `__malformed_incomplete__` | parsed as JSON, but a required field is missing or mistyped | **Generation** — the message arrived intact and was wrong when it was *built*: a `generate_traffic.py` bug, or drift between it and `schema.py` |

That split is the answer to "where did the record break?" — a transport fault and a schema fault are indistinguishable in a single counter and have completely different fixes. Which bucket moves tells you which file to open.

`REQUIRED_FIELDS` is `timestamp`, `protocol_type`, `service`, `flag`, `label`. `timestamp` and `label` are stamped unconditionally in `generate_batch()`; the other three are the categorical features, the only ones where a type error surfaces as a null rather than a plausible-looking zero. A corrupt *numeric* feature also nulls out, but a rule reading it then simply fails to match — it can't manufacture a false detection.

**Where the counts go:** both buckets are written to `traffic_counts` under those reserved pseudo-labels, so malformed rate is chartable in Superset directly against real traffic volume — no new table, no new DDL.

> ⚠️ **`evaluate_accuracy.py` must exclude them.** Any label matching `__%__` is a counter, not traffic. Included in a denominator, it silently corrupts the false-positive rate. Use `schema.REAL_TRAFFIC_ONLY_SQL`.

### Why it cost nothing

The counter adds one **column**, not one pass. Classification is a projection fused into the same stage as the JSON parse, and both buckets are counted by the `traffic_counts` groupBy `clickhouse_writer.py` already ran. Malformed rows are carried *through* rule evaluation rather than filtered out first — which avoids an extra filter+branch, and is safe precisely because of the `coalesce`: an all-null row evaluates to `is_detection=false`, never null. The writer then drops them on `_status`.

Spark actions per micro-batch went **down**:

| | Actions |
|---|---|
| Before | malformed agg + `isEmpty` + counts + detections = **4** |
| After | counts + detections = **2** |

`write_batch()` now returns the per-label counts it already collected, so `streaming_job.py` logs the malformed rate without triggering a further action.

### Is `coalesce` slowing the rule engine? — measured, no

The concern was that null-guarding every condition costs throughput. Benchmarked on 4,000,000 rows, all 6 rules / 17 conditions, 7 iterations, median:

| Variant | Median | Throughput |
|---|---:|---:|
| Without `coalesce` | 394.9 ms | 10.1M rows/sec |
| With `coalesce` | 319.5 ms | 12.5M rows/sec |

The guarded version measured *faster*, which is measurement noise — the honest reading is **no detectable cost**. `coalesce` compiles into the same fused projection as the comparison itself; the null check is a branch Catalyst already emits. It stays, as defence in depth: `streaming_job.py` is now the primary filter, and `rules_engine.py` stays null-safe on its own so it can't be broken by a future caller that skips classification (`validate_rules.py` is already exactly such a caller).

### Verified end to end

400 generated records plus 6 hand-corrupted messages (3 transmission-style, 3 generation-style) pushed through the real `parse_messages()` → `evaluate()` → `_prepare_counts()`/`_prepare_detections()` path:

```
__malformed_incomplete__        3      normal              380
__malformed_unparseable__       3      ddos/port_scan/...   20
```

All 7 assertions passed: both buckets counted correctly and separately; 400 valid records counted; no malformed row was ever flagged as a detection; **no malformed row produced `is_detection = NULL`**; malformed rows excluded from `detections`; counts total equals batch size. Catalyst plan confirmed free of `BatchEvalPython`. `rules_engine.py`'s 20 self-tests and `validate_rules.py`'s scores were re-run afterwards — no regression.

## `docker-compose.yml` — built, not yet verified running

- Services: `kafka` (apache/kafka, single-node KRaft mode, no Zookeeper needed), `clickhouse` (exposes 8123 HTTP + 9000 native, auto-creates the `ids` database via `clickhouse-init/01-create-database.sql` on first boot), `superset` (custom `superset/Dockerfile` extending `apache/superset` with `clickhouse-connect` added).
- Ports match `config.py`'s defaults exactly (9092, 8123, 9000, 8088).
- YAML syntax validated; **not run end-to-end** — no Docker available in the environment these files were built in. Needs verification on the laptop, especially Superset's first-boot bootstrap chain (`db upgrade` → `create-admin` → `init` → `run`), which is the most version-sensitive part.

## Live pipeline results — first successful end-to-end run (2026-08-25)

Kafka → Spark Structured Streaming → ClickHouse, scored by `evaluate_accuracy.py` against what the pipeline actually stored. **1,610,000 records across 33 micro-batches.** Full export: `report/accuracy.md`.

### Confusion matrix

|  | Predicted attack | Predicted normal |
|---|---:|---:|
| **Actually attack** | TP **79,299** | FN **1,201** |
| **Actually normal** | FP **9,043** | TN **1,520,457** |

Traffic mix came out at exactly 95.00% normal / 5.00% attack, matching the generator's configured ratio — a useful independent check that nothing was lost or duplicated in transit.

### Overall

| Metric | Value |
|---|---:|
| Precision | **89.8%** |
| Recall | **98.5%** |
| F1 | **93.9%** |
| Accuracy | **99.36%** |
| False-positive rate | **0.591%** |

### Per attack type

| Attack type | Seen | Caught | Missed | Recall | Precision | F1 |
|---|---:|---:|---:|---:|---:|---:|
| `brute_force` | 13,524 | 13,524 | 0 | 100.0% | 100.0% | 100.0% |
| `ddos` | 13,524 | 13,524 | 0 | 100.0% | 100.0% | 100.0% |
| `port_scan` | 13,524 | 13,524 | 0 | 100.0% | 100.0% | 100.0% |
| `malware_c2` | 13,524 | 13,524 | 0 | 100.0% | 95.9% | 97.9% |
| `data_exfiltration` | 13,202 | 12,702 | 500 | 96.2% | 93.4% | 94.8% |
| `malicious_download` | 13,202 | 12,501 | 701 | 94.7% | **62.2%** | 75.1% |

### The offline scorer predicted the live result

`validate_rules.py` scores `rules.json` straight from the generator, with no Kafka and no database. Its numbers held up against the full pipeline:

| Metric | Offline (20k records) | Live pipeline (1.61M) |
|---|---:|---:|
| False-positive rate | 0.684% | 0.591% |
| `malicious_download` recall | 94.0% | 94.7% |
| `data_exfiltration` recall | 97.0% | 96.2% |
| `malicious_download` precision | 64.1% | 62.2% |

That agreement is worth stating in the report as a result in its own right: **the pipeline does not distort the data.** Serialization, Kafka transport, `from_json` parsing, native-expression rule evaluation and the ClickHouse round-trip together introduced no measurable change in detection outcomes. It also retroactively validates the offline scorer as a fast proxy for rule tuning.

### Storage economics, confirmed

89,543 rows landed in `detections` — **5.56% of ingest**, against the 5.6% predicted from the design. The other 94.4% of traffic was counted in `traffic_counts` rather than stored, and every metric above was still computable.

### ⚠️ Latency figures from this run are NOT valid

| Statistic | Reported |
|---|---:|
| mean | 2,445 s (0.68 h) |
| p50 | 2,758 s (0.77 h) |
| p99 | 12,192 s (3.39 h) |
| under 2 s target | **0 of 89,543 (0.00%)** |

**This does not mean the latency target was missed — it means it was not measured.** This run *replayed a backlog*: the records were produced from about 21:55 onward and processed at about 01:15. `latency_seconds` is computed as `processed_at - record.timestamp`, so on a replay it measures **how old each record was**, not how long the pipeline took. The p99 of 3.39 hours matches the age of the oldest records on the topic exactly.

The number is therefore correct arithmetic on the wrong population. To measure the real thing, the producer and the streaming job must run **concurrently** so records are consumed as they are produced — Stage 5 in `TESTING.md` as originally written. Until then the `<2 s` target remains unverified.

*Report angle:* worth including as-is with the explanation, rather than quietly omitted. A metric that is arithmetically valid and semantically meaningless is a good illustration of why a measurement needs its population defined, and the fix (measure on live traffic, not replay) is the interesting part.

## Why the <2s latency target is unreachable — and what other systems do instead

The `<2s` figure was set before anything was measurable. Having measured it, this section records what actually limits the pipeline, whether it is our code or the architecture, and how systems that *do* achieve sub-second detection are built differently.

### 0. Architectural positioning — where the target came from

**The `<2s` target was inherited from the intuition of inline intrusion detection — a sensor deciding on a packet in flight — and applied to a streaming analytics pipeline whose role is storage, correlation and reporting.**

Measurement showed the two operate on different timescales by design. Critically, **no established architecture targets 2 seconds at all**:

| Layer | Example | Typical detection latency |
|---|---|---|
| Inline sensor | Snort 3 / Suricata, on the wire | **350 µs – low ms** |
| Streaming security analytics | Confluent's own ksqlDB IDS reference design | **60-second windows** |
| **This project's target** | — | **2 s — belonging to neither** |

Two seconds is roughly **5,700× slower** than a production sensor and **30× faster** than a streaming IDS design published by the company that builds Kafka. It did not come from either tradition. It came from an intuition about what "real-time" ought to feel like, and it fell into a gap between two architectures.

That reframes the finding. The mismatch is in the **requirement**, not the build:

- **Detection** — deciding whether a single record is malicious — belongs at the sensor, inline, in microseconds. It is stateless, per-record work.
- **Correlation, storage, scoring and dashboards** — everything this pipeline does — belong downstream, where seconds to minutes are not merely acceptable but expected, because the work needs temporal context and cross-source joins that a sensor cannot perform.

This pipeline sits, correctly, in the second layer. It sustains **6,700 records/sec at 98.5% recall and a 0.59% false-positive rate across 1.6 M records**. What it is not, and was never structured to be, is a sensor. The remainder of this section quantifies exactly where the boundary falls and why it cannot be moved by tuning.

### 1. What we measured

Batch duration is almost independent of batch size:

| Batch size | Duration | Per record |
|---:|---:|---:|
| 0 records | 19 ms | — |
| 4,500 | 1,800 ms | 400 µs |
| 10,700 | 2,000 ms | 187 µs |
| 15,000 | 2,225 ms | 148 µs |

3.3× the work costs 1.24× the time. Fitting the non-empty points:

```
batch_ms = 1,618 fixed  +  40.5 µs/record
```

**73% of a 15,000-record batch is fixed overhead, not work.** The model predicts 6,742 rec/s at a 15,000 cap; measured was 6,743 rec/s.

Because latency ≈ 1.5 × batch duration (average half a batch waiting, plus one batch processing), there is a **latency floor of ~2.5 s** that no batch size can go below.

| Sustained rate | Cap required | Batch duration | Mean detection latency |
|---:|---:|---:|---:|
| 2,500/s | ~4,500 | 1.80 s | ~2.7 s |
| 8,000/s | ~19,500 | 2.41 s | ~3.6 s |
| 15,000/s | ~62,000 | 4.13 s | ~6.2 s |

Where the time goes, from Spark's own SQL instrumentation (~10,700-record batch, 1 partition):

| Job | Operation | Time | Share |
|---|---|---:|---:|
| A | Kafka scan → parse → rules → cache | 1,029 ms | 51% |
| B | cached scan → detections `toPandas()` | 410 ms | 20% |
| C | cached scan → groupBy → counts | 83 ms | 4% |
| — | two ClickHouse HTTP inserts (outside Spark) | ~480 ms | 24% |

### 2. This is not our code — it is the documented floor of micro-batch streaming

Databricks published a study of exactly this overhead. Their **baseline was 700–900 ms per micro-batch** at 100K–1M events/sec on tuned cluster hardware. Their diagnosis names our situation precisely: the offset log write at the start of each micro-batch and the commit log write at the end *"can take up a majority of the processing time **especially for stateless single stage pipelines**."*

That is exactly what this pipeline is — stateless, single stage. Their measurement of that component alone: **337 ms → 31 ms** when made asynchronous.

Our 1,618 ms on a laptop, with the checkpoint on Windows NTFS through the `winutils` Hadoop shim and two synchronous HTTP inserts per batch, is roughly **2× the floor Databricks reports on a cluster**. That ratio is proportionate to the environment, not anomalous. Nothing in our code is pathological.

### 3. The one optimisation that targets it does not apply to us

Apache Spark 3.4+ added **async progress tracking** (`asyncProgressTrackingEnabled`) in open source, precisely to remove that offset/commit-log cost.

It cannot be used here. It supports **only no-op, console, memory, and Kafka sinks — not `foreachBatch` or any custom sink.** Writing to ClickHouse requires `foreachBatch`. So the single largest removable overhead component is architecturally incompatible with having ClickHouse as the sink at all. (It also weakens exactly-once guarantees, which for a security audit trail would be its own problem.)

**This is the core structural finding.** The overhead is known, documented, and has an official fix — and the fix is unavailable to any pipeline that writes to a custom sink.

### 4. Is it a technology problem? Partly.

| Engine | Model | Latency |
|---|---|---|
| **Apache Flink** | true per-event streaming | lowest of the three; no micro-batch floor |
| **Kafka Streams** | per-event, library | low, above Flink |
| **Spark Structured Streaming** | micro-batch | highest **by design** |

Spark processes data in *finite time-based chunks*; the per-batch cost is paid whether the batch holds one record or a million. Flink processes each event as it arrives, so there is no equivalent fixed toll. Spark offers a continuous-processing mode for lower latency, but it has long been experimental and does not support this kind of sink.

Moving to Flink would remove the floor — at the cost of a full rewrite in a framework with weaker Python support than the PySpark code here.

### 5. Is it a logic problem? Yes — and this is the more important answer.

**Real intrusion detection systems do not detect inside the data pipeline.**

Snort 3 evaluates rules **inline on the sensor, at packet speed, on the local device**, producing verdicts in **under a millisecond** — its ML classifier is reported at **350 microseconds** on a 4.7 GHz processor. The downstream layer receives the sensor's *event stream* and performs investigation, enrichment and correlation — work that needs temporal context and cross-source joins. **It does not perform the detection.**

The division of labour is:

```
sensor  ->  detection      (microseconds, inline, at the edge)
pipeline ->  correlation,   (seconds to minutes, downstream)
             storage,
             dashboards
```

Our architecture puts detection in the second box and then measures it against the first box's SLA.

The corroborating evidence is striking: **Confluent's own reference design for an IDS built on ksqlDB uses 60-second tumbling windows** and makes no sub-second latency claim. The company that builds Kafka, designing an IDS on their own streaming stack, targets a *minute*. That is the latency class this architecture belongs to.

So the `<2s` target was not merely optimistic — it was applied to the wrong component.

### 6. The bypass we had not considered: detect at the edge

`rules_engine.py` evaluates pure feature thresholds — no joins, no windows, no state, no cross-record context. **Every rule is decidable from a single record in isolation.** That is precisely the property that allows detection to move to where the record is created.

`generate_traffic.py` already builds records with vectorised numpy at ~169,000 rec/s. The same six rules could be applied there as boolean array operations, adding microseconds per record. Kafka would then carry each record *with its detection verdict already attached*, and Spark would do what micro-batch is good at: storage, aggregation, and dashboards, where 2–3 seconds is entirely acceptable.

Detection latency would drop from ~3.3 s to **microseconds**, because Spark would no longer be in the detection path at all.

This is not a workaround — it is what Snort and Suricata actually do. The cost is conceptual: **Spark stops being the detector**, which changes the project's premise. For a capstone specifically about Spark Structured Streaming, that is a real tension, and it may be better presented as "the architecture we would build next" than retrofitted now.

### 7. Options, ranked by honesty-per-effort

| Option | Latency achieved | Effort | Verdict |
|---|---|---|---|
| Re-state the SLA against measurement | 3.3 s at 6,700 rec/s | none | **Recommended.** The number is real and defensible |
| Move rules to the producer (edge detection) | microseconds | ~half a day | Architecturally correct; changes the premise |
| Rewrite the ClickHouse sink (batch counts, write from executors, drop persist) | est. 2.1–2.6 s | ~1 day | ~30% chance of reaching sub-2s; measure first |
| Port to Flink | sub-second | full rewrite | Correct engine, wrong time |
| Keep tuning Spark configs | no change | — | Exhausted. Three attempts, two negative, one marginal |

### 8. Conclusion for the report

The pipeline is not badly built and the hardware is not the limit — CPU utilisation during batches was 21–68%, so the cores were mostly idle. The constraint is that **micro-batch streaming charges a fixed per-batch toll of ~1.6 s in this environment**, the documented fix for the largest component of that toll is incompatible with a custom sink, and the target itself belongs to a different architectural layer than the one being measured.

Measured, the system is a correct near-real-time security analytics pipeline: **~6,700 records/sec sustained, ~3.3 s mean detection latency, 98.5% recall at a 0.59% false-positive rate.** Those are the numbers to report, against the sustainable rate rather than against a target set before measurement was possible.

**Sources:** [Databricks — Latency goes subsecond in Apache Spark Structured Streaming](https://www.databricks.com/blog/latency-goes-subsecond-apache-spark-structured-streaming) · [Async progress tracking in Spark 3.4](https://www.waitingforcode.com/apache-spark-structured-streaming/what-new-apache-spark-3.4.0-async-progress-tracking-structured-streaming/read) · [Comparing Spark Structured Streaming, Flink and Kafka Streams](https://www.onehouse.ai/blog/apache-spark-structured-streaming-vs-apache-flink-vs-apache-kafka-streams-comparing-stream-processing-engines) · [Stack Overflow Blog — SnortML and the evolving architecture of intrusion detection](https://stackoverflow.blog/2026/05/11/when-the-sensor-starts-thinking-snortml-agentic-ai-and-the-evolving-architecture-of-intrusion-detection/) · [Confluent — Build an intrusion detection system using ksqlDB](https://www.confluent.io/blog/build-a-intrusion-detection-using-ksqldb/)

## Live-run findings

Things discovered by actually running the stack, rather than by testing components in isolation. This section is the evidence that the isolation testing was necessary but not sufficient — worth citing in the report.

### 1. ClickHouse disables network access for a passwordless `default` user — hit 2026-08-24

`docker-compose.yml` set no ClickHouse credentials, and `config.py` defaulted `CLICKHOUSE_PASSWORD` to `""`. Since **ClickHouse 25.1**, the official image disables *network* access for `default` unless one of `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` or `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT` is set, and writes a randomly generated password into `/etc/clickhouse-server/users.d/default-password.xml`.

The failure is asymmetric, which is what made it read as a DDL bug:

| Connection | Result |
|---|---|
| `docker exec ids-clickhouse clickhouse-client` (local) | works |
| `clickhouse_writer.py` via HTTP on 8123 | `Code: 194 ... Authentication failed` |
| Superset / any GUI | same |

So `SHOW DATABASES` succeeded and the service reported `healthy`, while nothing could actually write. **Fixed** by setting `CLICKHOUSE_PASSWORD: ids_local_dev` in the compose file and matching the default in `config.py` (still env-overridable). Requires `docker compose up -d --force-recreate clickhouse`, or `down -v` if the generated password file persists.

*Report angle:* a healthcheck that only proves the process is up, not that it is usable, is a weak healthcheck. `wget --spider http://localhost:8123/ping` passes regardless of whether any client can authenticate.

### 2. PySpark crashes on Python 3.12+ on Windows — hit 2026-08-24

`clickhouse_writer.py`'s self-test died at `.count()` with `Python worker exited unexpectedly (crashed)`. This is a known PySpark bug whose conditions this project matches exactly: **Windows + Spark 3.5 or later + local mode + Python 3.12 or newer**, triggering specifically inside `createDataFrame()`. On Python 3.13 there is a second, independent breakage — PySpark calls `socketserver.UnixStreamServer`, removed in 3.13.

**Fix:** run everything inside a Python 3.11 virtual environment (`py -3.11 -m venv .venv`).

**Blast radius.** Every entry point that builds a DataFrame from Python objects is affected: `clickhouse_writer.py`'s self-test, `rules_engine.py`'s self-test, and `validate_rules.py`. The streaming job proper is *probably* safe — it reads from Kafka and evaluates rules entirely through native Spark expressions with no Python UDF (a design decision originally made for latency, which turns out to also dodge this bug) — but `toPandas()` in the writer still crosses the Python boundary, so 3.11 is the only safe answer.

*Report angle:* the no-UDF rule engine was justified on latency grounds. It incidentally removed the pipeline's dependence on Python workers in the hot path. Worth noting as a case where the performance-motivated design bought unexpected robustness.

### 3. Spark on Windows needs Hadoop's winutils.exe — hit 2026-08-24

`Hadoop home directory C:\hadoop-3.4.3\bin does not exist`. Two separate problems in one: the folder didn't exist, and 3.4.3 is the wrong line anyway — **PySpark 3.5.x bundles Hadoop 3.3.4**, so winutils must be a **3.3.x** build.

Not Hadoop the cluster — no HDFS, no YARN, ever. But Spark's filesystem layer calls Hadoop's native Windows code, and Structured Streaming checkpointing fails without `winutils.exe` + `hadoop.dll`. Two files on disk, no services.

Also worth recording: `HADOOP_HOME` must point at the folder **containing** `bin`, not at `bin` itself — Spark appends `\bin` on its own, which is what produced the doubled path in the error.

*Report angle:* this is the third consecutive failure caused by running Spark natively rather than in a container. Individually they're trivia; together they're the actual argument for containerizing the processing engine too, and worth presenting that way rather than as three unrelated setup hiccups.

### 4. Scala 2.12/2.13 mismatch on the Kafka connector — hit 2026-08-24

`java.lang.NoSuchMethodError: 'scala.collection.mutable.WrappedArray scala.Predef$.wrapRefArray(java.lang.Object[])'` at the first `.load()` on the Kafka source. That method returns `WrappedArray` in Scala **2.12** and `ArraySeq` in **2.13** — a textbook cross-Scala jar mismatch.

Root cause: `requirements.txt` left `pyspark` unpinned, so `pip install` fetched **4.x** (Scala 2.13), while the `--packages` coordinate used was `spark-sql-kafka-0-10_2.12:3.5.1` (Scala 2.12).

**Fixed** by pinning `pyspark==3.5.1` — the version every test in this project was verified against — and using `_2.12:3.5.1`.

**Knock-on effect worth recording:** the winutils version follows from the PySpark version too. PySpark 3.5.x bundles Hadoop 3.3.4 → 3.3.x winutils; Spark 4.x bundles Hadoop 3.4.x → 3.4.x winutils. An earlier attempt used `C:\hadoop-3.4.3`, which was in fact *correct* for the PySpark 4.x that was installed at the time; switching to 3.3.6 is what makes it correct for the now-pinned 3.5.1. One unpinned dependency silently determined three other version choices.

*Report angle:* the strongest single lesson from commissioning. An unpinned `pyspark` line propagated into the Scala build, the Kafka connector coordinate, and the Hadoop native binaries — three failures at three different layers, none of which named the real cause. `tools/check_env.py` now derives all three from the installed package rather than from documentation.

### 5. Unbounded micro-batch → driver OutOfMemoryError — hit 2026-08-24

`java.lang.OutOfMemoryError: Java heap space` in the stream execution thread, at Stage 4/5.

**Root cause — a genuine design gap, not a setup problem.** `build_kafka_stream()` set no `maxOffsetsPerTrigger`, and Structured Streaming consumes *every available offset* in a single micro-batch by default. On an idle topic that is invisible; against a backlog it is fatal. The whole backlog becomes one batch, gets `persist()`ed, and is then collected to the driver by `clickhouse_writer`'s `toPandas()`. A backlog is trivially easy to create — leave the producer running while the job is stopped, or restart against a stale checkpoint.

Compounded by Spark's **1 GB default driver heap**. In local mode the driver *is* the executor: it parses, evaluates rules, caches the batch and collects it.

**Fixed** three ways:

1. `config.MAX_OFFSETS_PER_TRIGGER` (default **50,000**, ~3.4× the 14,880 rec/s demo cap so steady state is never throttled), applied in `build_kafka_stream()`
2. `write_batch()` now persists with `MEMORY_AND_DISK` instead of the default `MEMORY_ONLY`, so an oversized batch spills rather than killing the JVM
3. `--driver-memory 4g` documented in every `spark-submit` invocation

*Report angle:* this is the first failure that was the **pipeline's own design**, not the environment — and it directly threatens the `<2s` latency target, independently of memory. An unbounded batch blows the 1-second trigger no matter how fast the rules evaluate, so bounding it is a latency control as much as a memory one. Good material for a "backpressure and bounded batches" section.

### 6. Catalyst constraint propagation hung every micro-batch — hit 2026-08-25

**The first genuine bug in the detection code, and the hardest to see.** The streaming query started cleanly, reported `status='Processing new data'`, stayed `isActive`, and never completed a single batch. Kafka was innocent: a host-side probe consumed **1,372,349 messages in 10 seconds** from the same broker, on the same address, moments earlier.

A driver thread dump was what settled it. The stream execution thread was `RUNNABLE` — burning CPU, not blocked — here:

```
Project.getAllValidConstraints
  -> ExpressionSet.++ -> HashSet.addEntry
    -> CaseWhen.equals -> Or.equals          (structural equality, recursive)
LogicalRDD$.rewriteStatsAndConstraints
ForeachBatchSink.addBatch
```

**Cause.** `compile_rules()` produced one projection containing ~42 `CASE WHEN` and ~119 `coalesce` expressions, because each rule's predicate is inlined once per output column. The worst offender was `max_severity`: the chained `when(rank==4,…).otherwise(when(rank==3,…))` form inlines `max_rank` **once per severity level**, and `max_rank` itself contains all six rule predicates — four levels meant four more full copies.

`ForeachBatchSink` calls `LogicalRDD.fromDataset` on **every micro-batch**, which computes plan constraints. `ExpressionSet` uses structural equality, so `CaseWhen.equals` recurses the whole subtree on every pairwise comparison. Reproduced directly: calling `.constraints()` on this plan throws `OutOfMemoryError`; with propagation disabled it returns in **6 ms**.

**Fixed two ways:**

1. `spark.sql.constraintPropagation.enabled=false` in `build_spark()` — the real fix. Constraint propagation only powers filter inference and nullability pruning, neither of which applies here (no joins, no filters to push), so it costs nothing.
2. `max_severity` now uses `element_at(array(NULL, low, medium, high, critical), max_rank + 1)`, referencing `max_rank` exactly once. Measured on the real six-rule set, same settings: **42 → 20 `CASE WHEN`, 119 → 68 `coalesce`**, plan string 15,230 → 11,922 chars.

All 20 `rules_engine` assertions and the 7 end-to-end fixture checks still pass, and the plan remains free of `BatchEvalPython`.

*Report angle:* the single best finding of the whole project. Every unit test passed because they call `evaluate()` on a batch DataFrame directly — `.count()` and `.collect()` never trigger constraint computation. Only `ForeachBatchSink` does, and only per-batch, so the bug was structurally invisible to component testing and appeared solely under streaming. It also demonstrates that "no Python UDF, all native expressions" is not automatically fast: expression *duplication* has a superlinear cost in the optimiser that has nothing to do with row throughput.

### 7. Added `tools/check_env.py` and `tools/kafka_probe.py`

`kafka_probe.py` tests a **host-side consumer** — the one path nothing else covered. The topic contents had been checked with `docker exec` (inside the container) and the producer writes from the host, but producing and consuming fail differently, and Spark is a host-side consumer. It prints the broker address *as advertised to the host*, the watermark offsets, and then actually consumes; its verdict line says whether a fault is in Kafka or in Spark. It is what eliminated Kafka as a suspect in finding #6.

`check_env.py`: rather than document the host requirements in prose and hope, there is now a stdlib-only checker that verifies all of them and **derives the version-locked ones from the installed PySpark** — the bundled Hadoop version for winutils, and the Scala/Spark coordinate for the Kafka package. `--spark` additionally runs a real `createDataFrame()`, which is the exact call the Python 3.12 bug breaks. Self-verified: run against a deliberately broken environment it correctly flagged Java 21, a missing `confluent_kafka`, and an unreachable Docker daemon.

### 8. PySpark ships no Kafka connector — now confirmed

`spark_app/streaming_job.py` cannot run as a plain script: PySpark bundles zero Kafka jars, so `.format("kafka")` fails with *"Failed to find data source: kafka"*. It must be launched via `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<pyspark-version>`. Nothing in the repo documented this; `TESTING.md` now does. `pyspark` is also unpinned in `requirements.txt`, which is a hazard given the coordinate must match the installed version exactly.

## Open problems

1. ~~**Superset conflict.**~~ **Resolved.** Went with the lightweight wrapper Dockerfile (`FROM apache/superset:latest` + `pip install clickhouse-connect`) rather than touching the separate, unrelated full-source Superset build that already exists locally. `docker-compose.yml`'s `superset` service already points at `build: ./superset`, so this drops in with no compose changes needed.
2. ~~**`rules_engine.py` missing from disk.**~~ **Resolved 2026-08-24** — written and tested against a real Spark session.
3. ~~**`clickhouse-init/01-create-database.sql` does not exist.**~~ **Resolved 2026-08-24** — written. Creates the database only; the tables stay owned by `clickhouse_writer.ensure_tables()` so there is one source of truth for DDL. Note it runs on **first boot only** (empty data volume), so `docker compose down -v` is what re-triggers it.
4. ~~**Malformed-record filter never fired.**~~ **Resolved 2026-08-24** — `from_json` returns an all-null struct, not a null struct. Replaced with required-field classification, split into transmission vs generation buckets. See above.
5. ~~**Nothing in the pipeline has been run against real Docker containers yet.**~~ **Resolved 2026-08-25** — full end-to-end run completed, 1,610,000 records. Results above.

5a. ~~**The `<2s` latency target is still unverified.**~~ **Measured and analysed 2026-08-25.** ~3.3 s mean at 6,700 rec/s. Shown to be architecturally unreachable: micro-batch charges a ~1.6 s fixed per-batch toll, and Spark's own fix for the largest component (async progress tracking) does not support custom sinks. See the dedicated section.
6. ~~**Stale credentials in `certs/`.**~~ **Resolved 2026-08-24** — `certs/` deleted, and `.gitignore` now blocks `*.pem`/`*.key`/`*.p12`/`*.pfx`/`*.crt`/`certs/`. No `git init` had happened, so the keys are absent from history entirely.
7. **`traffic_counts` now mixes traffic with counters.** The `__%__` pseudo-labels are the agreed tradeoff for not adding a table, but they are a foot-gun for anything that aggregates the table naively — including a Superset chart built by dragging `label` onto an axis. `evaluate_accuracy.py` must filter them; charts should too.

## Next steps
1. ~~Resolve the Superset driver/duplication problem.~~ Done.
2. ~~Write `rules_engine.py`.~~ Done and tested.
3. ~~Get `validate_rules.py` running and score `rules.json`.~~ Done — results above.
4. ~~Delete `certs/*.pem`; write `.gitignore`.~~ Done 2026-08-24 — `certs/` removed, `.gitignore` written and verified with `git check-ignore` against a replica tree. The repo has not been `git init`'d yet, so the keys never entered any commit history.
5. ~~Create `clickhouse-init/01-create-database.sql`.~~ Done.
6. ~~`evaluate_accuracy.py`.~~ Done — see below.
7. **Run `docker compose up -d` on the laptop** and confirm all services come up healthy — including a real `insert_df()` round-trip for `clickhouse_writer.py`. Watch Superset's first-boot bootstrap chain, the most version-sensitive part.
8. ~~Run the complete pipeline end-to-end and score it.~~ Done — 1.61M records, results above.
8a. ~~Re-measure latency on live traffic.~~ Done — ~3.3 s mean, ~6,700 rec/s sustained. Target shown unreachable; SLA to be re-stated against measurement.
8b. **Connect Superset to ClickHouse** and build the dashboards (remember `label NOT LIKE '\_\_%\_\_'`).
8c. **Run the Stage 8 failure tests** in `TESTING.md` — malformed injection, checkpoint resume, throughput ceiling.
9. Write the technical README (what it is, prerequisites, how to run). `markdown.md` stays as the progress log — deliberately kept separate so it can travel with the repo as working context across machines.

## `evaluate_accuracy.py` — built, math verified

The capstone accuracy report. Distinct from `validate_rules.py`: that scores `rules.json` offline straight from the generator; this scores the **live pipeline**, where everything measured has survived serialization, Kafka, Spark parsing, rule evaluation and a ClickHouse insert. If the two disagree, the gap *is* the pipeline — worth investigating rather than averaging away.

**What it reports**

- Overall confusion matrix (TP / FN / FP / TN), precision, recall, F1, accuracy
- **False-positive rate** — the number that needs `traffic_counts`, since the TN count exists nowhere else
- Per-attack-type recall, precision and F1. Per-type precision comes from `arrayJoin(matched_attack_types)`, so a row flagged as the *wrong* attack type counts against that type — the honest reading, and the one that exposes a noisy rule
- Observed detection latency vs the 2s target: mean/p50/p95/p99/max and the share under target
- Malformed rate, split into the transmission and generation buckets
- **Integrity cross-checks** between the two tables — every attack record in `traffic_counts` must have a matching row in `detections`. Catches dropped rows or a report run mid-stream, which otherwise produce plausible-looking but wrong metrics

**Structure.** The SQL layer and the arithmetic are separate: `compute_report()` is pure, takes plain row tuples, and has no database or pandas dependency. That is what makes the math testable without a live ClickHouse.

**Flags:** `--json` and `--markdown` write report-ready output (the markdown emits tables that paste straight into the writeup); `--show-sql` prints the queries without connecting; `--self-test` runs the arithmetic against a fixture.

**Verification status.** `--self-test` feeds `compute_report()` a fixture captured from a real 3-batch pipeline run where the answers are known independently (TP=2206, FN=26, FP=250, 42,408 normal records seen) — **18/18 assertions pass**, covering every derived metric including the noisiest rule's 64.1% precision. Empty-database and zero-division paths render without crashing. **The SQL itself is unverified** — no ClickHouse has run these queries yet. `arrayJoin` and the `LIKE '\_\_%\_\_'` escaping are the two things to watch on first live run.

**Exit codes:** `0` clean · `2` cannot reach ClickHouse · `3` tables empty (job hasn't written yet) · `4` an integrity check failed.
