# Streaming IDS — Capstone Project Status

*Last updated: 2026-08-24*

## Overview
A rule-based real-time Intrusion Detection System (IDS) built on Apache Kafka and Apache Spark Structured Streaming. Simulated network traffic (NSL-KDD schema) is streamed through Kafka, evaluated against a JSON rule file in Spark, and flagged detections are written to a storage layer for dashboarding — plus a lightweight accuracy-scoring layer for the capstone report.

**Target detection latency:** under 2 seconds, end to end.

## Architecture
```
generate_traffic.py  →  Kafka topic  →  Spark Structured Streaming  →  ClickHouse  →  Superset
 (NSL-KDD JSON,          (network-       (schema.py, rules_engine.py,   (storage)     (dashboards)
  95% normal /            traffic)        streaming_job.py,
  5% attack)                              clickhouse_writer.py)
                                                    │
                                                    └──→ evaluate_accuracy.py (report script, planned)

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
│   └── evaluate_accuracy.py          ✅ built — metric math verified against known-truth fixture
│                                        (18/18); SQL layer unverified until live ClickHouse
├── clickhouse-init/
│   └── 01-create-database.sql        ✅ built — creates the `ids` database on first boot only
├── superset/
│   └── Dockerfile                     ✅ built (lightweight `FROM apache/superset:latest` + clickhouse-connect)
├── docker-compose.yml                 ✅ built, kafka+clickhouse+superset untested end-to-end
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

## Open problems

1. ~~**Superset conflict.**~~ **Resolved.** Went with the lightweight wrapper Dockerfile (`FROM apache/superset:latest` + `pip install clickhouse-connect`) rather than touching the separate, unrelated full-source Superset build that already exists locally. `docker-compose.yml`'s `superset` service already points at `build: ./superset`, so this drops in with no compose changes needed.
2. ~~**`rules_engine.py` missing from disk.**~~ **Resolved 2026-08-24** — written and tested against a real Spark session.
3. ~~**`clickhouse-init/01-create-database.sql` does not exist.**~~ **Resolved 2026-08-24** — written. Creates the database only; the tables stay owned by `clickhouse_writer.ensure_tables()` so there is one source of truth for DDL. Note it runs on **first boot only** (empty data volume), so `docker compose down -v` is what re-triggers it.
4. ~~**Malformed-record filter never fired.**~~ **Resolved 2026-08-24** — `from_json` returns an all-null struct, not a null struct. Replaced with required-field classification, split into transmission vs generation buckets. See above.
5. **Nothing in the pipeline has been run against real Docker containers yet.** Every component has been tested in isolation (real Spark for the rule engine, schema, and malformed classification; mocks/dry-run elsewhere) — never against a live Kafka/ClickHouse stack.
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
8. **Run the complete pipeline end-to-end** and verify Kafka → Spark → ClickHouse, then run `evaluate_accuracy.py` against the result. This is the first moment the `<2s` latency claim becomes a measured number rather than a design target.
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
