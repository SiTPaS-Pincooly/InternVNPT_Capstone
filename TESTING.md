# First Live Run — Test Plan

Every component passes its own tests. None of them have met each other. This is the order to introduce them in, and what breaks at each step.

Stage order is **dependency order** — each stage assumes the ones before it passed. When something breaks, fix it before moving on. A failure at Stage 5 that's really a Stage 2 problem is the expensive kind.

*Verified against Spark 3.5.1 / Scala 2.12. Nothing here has run against the Docker stack — that's the point of the exercise.*

---

## ⛔ Four things will stop you before anything else

Two were hit for real on the first attempt; the others are verified against a real Spark 3.5.1 install. Deal with all four before Stage 1 or you'll debug the wrong layer.

### 0. PySpark crashes on Python 3.12+ on Windows

**Hit for real on 2026-08-24 at Stage 2c.** This is a known PySpark bug, and the conditions match this project exactly:

| Condition | This project |
|---|---|
| Windows | yes |
| Spark 3.5 or later | yes |
| local mode (not cluster) | yes |
| Python 3.12 or newer | ← **check this** |

It crashes specifically inside `createDataFrame()`, with:

```
org.apache.spark.SparkException: Python worker exited unexpectedly (crashed)
```

On Python 3.13 there's a second, separate breakage: PySpark uses `socketserver.UnixStreamServer`, which 3.13 removed.

**Check:**

```powershell
python --version
python -c "import pyspark; print(pyspark.__version__)"
```

**Fix — use a Python 3.11 virtual environment:**

```powershell
py -3.11 -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Everything from Stage 0 onward runs inside that venv. Note `.venv/` is already covered by `.gitignore`.

> **What this affects.** Every entry point that builds a DataFrame from Python objects: `clickhouse_writer.py`'s self-test, `rules_engine.py`'s self-test, and `rules/validate_rules.py`. The streaming job proper reads from Kafka and evaluates rules through native Spark expressions with **no Python UDF**, so it may survive on 3.12 — but `toPandas()` in the writer still crosses the Python boundary. Don't rely on it; use 3.11.
>
> While pinning, pin `pyspark` in `requirements.txt` too. It's currently unpinned, and Stage 4's `--packages` coordinate has to match the installed version exactly.

### 1. Spark cannot read Kafka without an extra package


PySpark ships **zero** Kafka jars — the `jars/` directory contains none. Running `python spark_app/streaming_job.py` fails with:

```
Failed to find data source: kafka. Please deploy the application as per
the deployment section of Structured Streaming + Kafka Integration Guide.
```

Nothing in the repo documents this. The job must be launched through `spark-submit` with a `--packages` coordinate matching your exact PySpark version — see Stage 4.

### 2. ClickHouse disables network access for a passwordless `default` user

**Confirmed hit on 2026-08-24 at Stage 2b.** Since ClickHouse 25.1, the official image disables *network* access for `default` unless one of `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` / `CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT` is set, and writes a random password into `/etc/clickhouse-server/users.d/default-password.xml`.

The symptom is asymmetric, which is what makes it confusing:

| Connection | Result |
|---|---|
| `docker exec ids-clickhouse clickhouse-client` | works — it's a *local* connection |
| `clickhouse_writer.py` on port 8123 | `Code: 194 ... Authentication failed` |
| Superset, any GUI client | same failure |

So the database looks perfectly healthy while nothing can write to it.

**Fixed** by setting `CLICKHOUSE_PASSWORD: ids_local_dev` in the `clickhouse` service and matching it in `config.py`. Both are already committed. After pulling those changes:

```bash
docker compose up -d --force-recreate clickhouse
# if it still fails, the container kept the generated password file:
docker compose down -v && docker compose up -d
```

> `down -v` deletes the data volume, which also re-runs `01-create-database.sql`. At this stage that's harmless — there's no data worth keeping yet.

### 3. Port 8088 may already be taken

There is a separate full-source Superset build on this machine. If any part of it is running, `docker compose up` fails to bind 8088 and the `superset` service dies while Kafka and ClickHouse come up fine — which reads like a Superset bug rather than a port clash.

```powershell
netstat -ano | findstr :8088
```

---

## Stage 0 — Host environment setup

Everything in this stage installs on the laptop, not in Docker. **Spark and the producer run natively** — only Kafka, ClickHouse and Superset are containerized. That's a deliberate choice (fast iteration, and `config.py` can use `localhost` throughout), but the cost is that Spark inherits your Python, Java and OS. Every failure in this section is that cost.

### Run the checker first

```powershell
python tools/check_env.py --spark
```

It verifies every requirement below, derives the correct Hadoop and Kafka versions from your installed PySpark, and explains each failure inline. Re-run it after each fix. **Get a clean run before Stage 1** — nothing downstream is worth debugging until it passes.

### The four version constraints

These are locked to each other. Pin PySpark and the other three follow from it.

| # | Requirement | Value | Why |
|---|---|---|---|
| 1 | **Python** | **3.11.x** | 3.12+ crashes `createDataFrame()` on Windows in local mode; 3.13 also removed `socketserver.UnixStreamServer`, which PySpark calls |
| 2 | **Java JDK** | **8, 11 or 17** | Spark 3.5 supports only these. Java 21 gives module-access errors that look nothing like a version problem |
| 3 | **PySpark** | **pinned to `3.5.1`** in `requirements.txt` | Determines #4 *and* the Stage 4 Kafka coordinate *and* the Scala build. Leaving it unpinned installs 4.x, which is Scala 2.13 — and then a 2.12 Kafka jar fails with `NoSuchMethodError: scala.Predef$.wrapRefArray` |
| 4 | **Hadoop winutils** | **3.3.x** (for PySpark 3.5.x) | PySpark 3.5.x bundles **Hadoop 3.3.4**. Spark **4.x bundles Hadoop 3.4.x** and would need a 3.4.x build instead — so this follows from #3, don't guess it |

> **Don't copy version numbers out of documentation — derive them.** `python tools/check_env.py` reads the installed PySpark and prints the winutils line and the exact `--packages` coordinate it needs. Every version-related failure in this project so far came from a number that was right for *some* PySpark and wrong for the installed one.

### Setup, in order

```powershell
# 1. Python 3.11 venv — everything else goes inside it
py -3.11 -m venv .venv
.venv\Scripts\activate

# 2. Project packages
pip install -r requirements.txt

# 3. Confirm what Hadoop version your PySpark wants
python tools/check_env.py
```

**4. Hadoop Windows binaries.** Download a **3.3.x** build (3.3.5 or 3.3.6 both work with Spark's 3.3.4 libs) from the community [cdarlint/winutils](https://github.com/cdarlint/winutils) repo — that's third-party binaries, which is the standard practice here but is a trust decision worth making knowingly. You need `bin\winutils.exe` and `bin\hadoop.dll`.

You only need two files — `winutils.exe` and `hadoop.dll`. `git clone` on a GitHub *tree* URL won't work (git only clones repo roots, not subfolders); download the files directly instead.

```powershell
# extract/download so that C:\hadoop-3.3.6\bin\winutils.exe exists

# 1. test in the CURRENT session first, before making anything permanent
$env:HADOOP_HOME = "C:\hadoop-3.3.6"
$env:PATH += ";C:\hadoop-3.3.6\bin"
Test-Path "$env:HADOOP_HOME\bin\winutils.exe"   # must be True
Test-Path "$env:HADOOP_HOME\bin\hadoop.dll"     # must be True

# 2. once Spark runs in that window, persist it
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop-3.3.6", "User")
$p = [Environment]::GetEnvironmentVariable("Path", "User")
$clean = ($p -split ';' | Where-Object { $_ -and $_ -notmatch 'hadoop' }) -join ';'
[Environment]::SetEnvironmentVariable("Path", "$clean;C:\hadoop-3.3.6\bin", "User")

# 3. open a NEW terminal — neither method affects an already-open window
```

> **Don't use `setx PATH "%PATH%;..."`.** `%PATH%` expands to your User *and* System PATH combined, so it writes the whole thing into User PATH — duplicating every system entry — and `setx` truncates at 1024 characters, which can silently mangle it. The `SetEnvironmentVariable` form above edits only the User scope, and the `-notmatch 'hadoop'` filter strips any stale entry from a previous attempt at the same time.
>
> To switch Hadoop versions later, re-run step 2 with the new path — there is nothing to change inside Spark. It reads `HADOOP_HOME`, falling back to the `hadoop.home.dir` system property (overridable per job with `--conf spark.driver.extraJavaOptions=-Dhadoop.home.dir=...`, though the environment variable is cleaner).

> ⚠️ **`HADOOP_HOME` points at the folder CONTAINING `bin`, not at `bin` itself.** Setting it to `C:\hadoop-3.3.6\bin` produces `Hadoop home directory ...\bin does not exist`, because Spark appends `\bin` itself.

**5. Java.** Set `JAVA_HOME` to the JDK **root** (the folder containing `bin`), not to `bin`. Avoid paths with spaces where you can — they're a known source of Windows launcher bugs.

**6. Docker Desktop** running, and ports **9092, 8123, 9000, 8088** free.

### Checklist

- [ ] `python tools/check_env.py --spark` reports no blocking problems
- [ ] Python 3.11 inside `.venv`
- [ ] Java 8/11/17, `JAVA_HOME` at the JDK root
- [ ] `HADOOP_HOME` set to a 3.3.x folder containing `bin\winutils.exe` and `bin\hadoop.dll`
- [ ] `pyspark` pinned in `requirements.txt`
- [ ] Docker Desktop running

> **Is Hadoop actually required?** Not Hadoop the cluster — you never run HDFS or YARN. But Spark's filesystem layer calls into Hadoop's native Windows code, and Structured Streaming's checkpointing (`_checkpoints/`) fails without `winutils.exe`. Two files, no services.

> **Run every command from the repo root.** `config.py` defaults `RULES_PATH` to `./rules/rules.json` and `CHECKPOINT_LOCATION` to `./_checkpoints/streaming_job` — both relative. From any other directory the job dies at startup on a missing rules file.

---

## Stage 1 — Bring the stack up

*First real test of `docker-compose.yml`.*

```bash
docker compose up -d
docker compose ps
docker compose logs -f superset   # first boot takes several minutes
```

**Pass**

- [ ] `kafka` and `clickhouse` both report `healthy` — both have healthchecks defined; give them 30–60 seconds.
- [ ] Superset reaches `Running on http://0.0.0.0:8088`. Its bootstrap chain is `db upgrade` → `create-admin` → `init` → `run`, and it's the most version-sensitive thing in the compose file because it pulls `apache/superset:latest`. If `db upgrade` fails, pin a known tag instead of `latest` in `superset/Dockerfile`.

**Fail**

- **A container restarts in a loop** — run `docker compose logs <service>` before changing anything. For Kafka, a KRaft cluster-id mismatch after a partial earlier run is fixed by `docker compose down -v`.

> **Watch RAM.** Kafka + ClickHouse + Superset + a Spark driver on 16 GB is tight. If the laptop starts swapping, stop Superset while testing Stages 2–6 — nothing before Stage 7 needs it.

---

## Stage 2 — ClickHouse: database, DDL, insert round-trip

> ⚠️ **The biggest untested gap.** This is the one component that has never touched a real server. Test it **in isolation**, before Kafka and Spark are anywhere near the picture — otherwise a DDL error surfaces as a mysterious streaming failure.

### a) Did the init SQL actually run?

```bash
docker exec ids-clickhouse clickhouse-client --query "SHOW DATABASES"
```

**Pass**

- [ ] `ids` appears in the list. Confirms `01-create-database.sql` executed.

> It runs on **first boot only**. If ClickHouse was ever started before that file existed, the volume is already initialised and the script was skipped. `docker compose down -v` forces a re-init.

**Fail**

- **Syntax error mentioning `COMMENT`** — older ClickHouse builds reject `COMMENT` on `CREATE DATABASE`. Harmless; delete that clause.

> **Note:** this step passing does **not** mean network clients can connect. `docker exec ... clickhouse-client` is a local connection and succeeds even when network access is disabled for `default` — see blocker #2 above. Step (b) is the first real test of network auth.

### b) DDL creation

```powershell
python -c "import sys; sys.path.insert(0,'spark_app'); import clickhouse_writer as w; w.ensure_tables(); c=w.get_client(); print(c.query('SHOW TABLES FROM ids').result_rows)"
```

**Pass**

- [ ] `detections` and `traffic_counts` both listed. The DDL is generated from `NSL_KDD_SCHEMA`, so this also proves the schema translates cleanly to ClickHouse types.

**Fail**

- **`Code: 194 ... Authentication failed: password is incorrect, or there is no user with such name`** — blocker #2. Not a DDL problem at all; the connection never authenticated. Fix the compose password and recreate the container.
- **Error on the `count` column** — it's an aggregate function name. The generated DDL backticks it, but if a quoting bug slips through, this is where it shows.
- **Unknown data type `Bool`** — needs ClickHouse ≥ 21.12. `latest` is far past that, so this only bites on a pinned old image.

### c) The insert round-trip — the call that has never executed

Two modes, no editing required:

```powershell
python spark_app\clickhouse_writer.py           # offline: prep logic + DDL only
python spark_app\clickhouse_writer.py --live    # the real round-trip
```

`--live` does the whole thing end to end against your running ClickHouse: connects, runs `ensure_tables()`, `insert_df()` into **both** tables, reads the rows back, checks the values survived, then deletes them again.

The fixture rows are tagged `batch_id = -1` — a value no real batch ever uses — so cleanup removes exactly them and nothing else. Nothing you insert here can pollute later metrics.

**Pass**

- [ ] `LIVE ROUND-TRIP PASSED`, with `ok` on connect, `ensure_tables()`, both inserts, and all four read-back checks.

**Fail** — the exit code tells you which layer:

| Exit | Meaning |
|---|---|
| `2` | Cannot connect — stack down, or the `config.py` password doesn't match `docker-compose.yml` |
| `3` | `ensure_tables()` — the generated DDL was rejected |
| `4` | `insert_df()` into `detections` — the pandas → ClickHouse type mapping |
| `5` | `insert_df()` into `traffic_counts` |

Exit `4` is the interesting one, and the script prints the three candidates: a `NaN` in a non-nullable `Float64`, the timezone-aware `processed_at` against `DateTime64(3)`, or a Python list failing to map onto `Array(String)`.

> Cleanup uses `ALTER TABLE ... DELETE`, which is an asynchronous mutation in ClickHouse — the rows may linger in a `SELECT` for a few seconds after the script reports success. That's normal, not a failed cleanup.

---

## Stage 3 — Kafka: producer and topic

*Low risk.*

```bash
# a) generator alone, no broker involved
python producer/generate_traffic.py --dry-run

# b) produce a small burst for real
python producer/generate_traffic.py --workers 1 --target-rate 1000

# c) confirm the bytes are on the topic
docker exec ids-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic network-traffic \
  --from-beginning --max-messages 1
```

**Pass**

- [ ] One JSON record prints, with all 43 fields. The topic auto-creates on first produce (`KAFKA_AUTO_CREATE_TOPICS_ENABLE` is on) — no manual create step.
- [ ] The `label` field is a plain string. Confirms the `numpy.str_` fix survived into the serialized output.

---

## Stage 4 — Spark reads Kafka

> ⚠️ **Blocked without `--packages`.**

Substitute your actual PySpark version for `3.5.1` in both places. The Scala suffix follows the Spark major version: **3.5.x → `_2.12`**, **4.x → `_2.13`**.

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --driver-memory 4g \
  spark_app/streaming_job.py
```

> **`--driver-memory 4g` matters.** Spark's default driver heap is **1 GB**, and in local mode the driver *is* the executor — it parses, evaluates rules, caches the batch, and collects it via `toPandas()`. 1 GB is not enough headroom. 4 GB is comfortable on a 16 GB laptop alongside the three containers.

**Pass**

- [ ] Log shows these three lines, in order:
  ```
  Loaded 6 detection rule(s)
  ClickHouse tables are ready
  Streaming IDS started
  ```
  That means rules, database and Kafka source all resolved.

**Fail**

- **First run hangs on "resolving dependencies"** — it's downloading jars from Maven Central. Needs internet, takes a minute or two, cached afterwards.
- **`NoSuchMethodError: 'scala.collection.mutable.WrappedArray scala.Predef$.wrapRefArray(java.lang.Object[])'`** — **hit for real on 2026-08-24.** A Scala 2.12 jar running on a Scala 2.13 runtime (that method returns `WrappedArray` in 2.12 and `ArraySeq` in 2.13). Caused by using a `_2.12:3.5.1` coordinate against an installed PySpark 4.x. Fix by making the coordinate match the installed PySpark — or, as this project does, pin PySpark to 3.5.1 and use `_2.12:3.5.1`. A quick tell: PySpark 3.5.x ships `py4j-0.10.9.7`; if the traceback shows a newer py4j, you're on 4.x.
- **`NoClassDefFoundError` / other `NoSuchMethodError`** — same family. The package and PySpark must be identical, including the patch number.
- **Job starts but every batch is empty** — expected, and **hit for real on 2026-08-24**. `KAFKA_STARTING_OFFSETS` defaults to `latest`, so the job only sees records produced *after* it started. 855,000 records already on the topic from an earlier producer run were correctly ignored. Empty batches log nothing at all, so this looks identical to a broken job.

  Confirm the topic actually has data before suspecting Spark:

  ```powershell
  docker exec ids-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic network-traffic
  ```

  To replay what's already there:

  ```powershell
  Remove-Item -Recurse -Force _checkpoints
  $env:KAFKA_STARTING_OFFSETS = "earliest"
  ```

  > **Deleting the checkpoint is mandatory.** `startingOffsets` is only consulted on a *fresh* start — with a checkpoint present the committed position wins and `earliest` is silently ignored. This is the usual reason "I set earliest and nothing changed."
- **`java.lang.OutOfMemoryError: Java heap space` in the stream execution thread** — **hit for real on 2026-08-24.** Two causes, both now addressed:
  1. **An unbounded micro-batch.** Structured Streaming consumes *all* available offsets in one batch unless told otherwise. Against a backlog — producer left running while the job was down, or a stale checkpoint replaying — the whole backlog becomes a single batch, gets cached, and is collected to the driver by `toPandas()`. `config.MAX_OFFSETS_PER_TRIGGER` (default **50,000**) now caps it, and `write_batch` persists with `MEMORY_AND_DISK` so an oversized batch spills instead of dying.
  2. **The 1 GB default driver heap.** Use `--driver-memory 4g`.

  After an OOM the checkpoint may hold a half-committed offset. If it crashes again immediately on restart, clear it: `Remove-Item -Recurse -Force _checkpoints` — you lose stream position, not data.

---

## Stage 5 — End to end

*The actual milestone.*

> **Order matters.** Streaming job first, producer second. With `latest` offsets, starting the producer first means those records are simply never seen.

```bash
# terminal A — leave running from Stage 4
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 --driver-memory 4g spark_app/streaming_job.py

# terminal B — the documented demo cap, 1 GB/min
python producer/generate_traffic.py --workers 1 --target-rate 14880

# terminal C — watch rows arrive
docker exec ids-clickhouse clickhouse-client --query \
  "SELECT label, count() FROM ids.detections GROUP BY label ORDER BY label"
```

**Pass**

- [ ] Roughly **830 rows per second** of traffic land in `detections` — about 5.6% of ingest, which is the storage design working as intended.
- [ ] `traffic_counts` gains ~7 rows per batch, one per label present. If malformed buckets appear here and you didn't inject anything, something upstream is genuinely corrupting messages — worth chasing.

**Fail**

- **`detections` fills but `traffic_counts` stays empty (or vice versa)** — both writes happen in the same `write_batch` call, so one succeeding alone points at a type error in the other's insert, not at the pipeline.

---

## Stage 6 — The accuracy report

> ⚠️ **The SQL has never been executed.**

```bash
# let it run a minute, stop the producer, then:
python evaluation/evaluate_accuracy.py
python evaluation/evaluate_accuracy.py --markdown report/accuracy.md
```

| Exit code | Meaning |
|---|---|
| `0` | Clean — all integrity checks passed |
| `2` | Cannot reach ClickHouse |
| `3` | Tables empty — the job hasn't written yet |
| `4` | An integrity check failed |

**Pass**

- [ ] Numbers land near the offline scores — roughly 98–99% recall, ~0.6% false-positive rate. A large gap from `validate_rules.py`'s numbers means the pipeline changed the data. That's a finding, not noise.
- [ ] Latency section shows a real sub-2s figure. **This is the first time the `<2s` target becomes measured rather than asserted.** Capture this output for the report.

**Fail**

- **Exit 4, integrity check on record counts** — usually means the report ran while the stream was still writing. Stop the producer, wait for the last batch, re-run.
- **Syntax error on `arrayJoin` or the `LIKE` escaping** — the two constructs that couldn't be verified without a server. `--show-sql` prints every query so you can paste it into `clickhouse-client` and isolate the failing one.

---

## Stage 7 — Superset

Open `http://localhost:8088`, log in with `admin` / `admin`, then add a database connection:

```
clickhousedb://default:ids_local_dev@ids-clickhouse:8123/ids
```

**Fail**

- **Using `localhost` in that URI** — the single most common mistake here. Superset runs *inside* a container, where `localhost` is Superset itself. It must reach ClickHouse by its compose service name, `ids-clickhouse`. Host tools use `localhost:8123`; Superset does not.
- **ClickHouse isn't in the driver dropdown** — the wrapper Dockerfile's `pip install clickhouse-connect` didn't take. Rebuild explicitly: `docker compose build --no-cache superset`.

**Pass**

- [ ] Test Connection succeeds and both tables are listed.

> When charting `traffic_counts`, filter `label NOT LIKE '\_\_%\_\_'` — otherwise the malformed counters plot as if they were a traffic class.

---

## Stage 8 — Deliberate failure tests

*Optional — but this is report material.*

Everything above proves it works. These prove you know **how it fails**, which is the more interesting half of a capstone writeup.

```bash
# inject a garbage message while the job runs
docker exec -i ids-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic network-traffic

# then type:  not json at all     -> __malformed_unparseable__
# then type:  {"duration": 1.0}   -> __malformed_incomplete__
```

- [ ] **Both malformed buckets increment correctly.** Query `traffic_counts` for the `__%__` labels. This is the counter that silently read zero before the rewrite — worth demonstrating that it now doesn't.
- [ ] **Checkpoint resume.** Kill the job mid-stream, restart it, confirm it resumes from committed offsets rather than reprocessing or skipping. Deleting `_checkpoints/` should make it start fresh — that contrast is the demonstration.
- [ ] **Throughput ceiling.** Raise `--target-rate` until Spark's batch duration exceeds the 1-second trigger. The rate where that happens is the real end-to-end ceiling, and a far more defensible number than the generation-only 169,000 rec/sec.
- [ ] **ClickHouse unavailable.** `docker compose stop clickhouse` mid-run and watch what the job does. Nothing retries right now, so this shows exactly how the pipeline degrades — worth knowing before an examiner asks.

---

## If it all passes

Capture three things straight into the report while the stack is still up:

1. The `evaluate_accuracy.py` output — the latency section especially
2. A Superset screenshot
3. The `traffic_counts` malformed rows from Stage 8

Then update `markdown.md` — the entries that still say *"not verified against the live Docker stack"* become the results section.
