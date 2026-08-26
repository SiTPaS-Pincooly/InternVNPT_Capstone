# Superset Dashboard — build guide

Seven charts, each answering a question the others don't: **what happened, how well, what is happening now, where is it weak, is it keeping up.**

Every query runs against the two ClickHouse tables the pipeline produces. Paste each into **SQL Lab**, run it, then *Save as dataset* and build the chart from it.

> **None of this SQL has been executed against a live ClickHouse.** It is written around the specific traps in this schema (catalogued at the bottom), but run each query in SQL Lab first — it reports errors immediately, and that is far easier to fix than a half-built chart.

---

## 0. Prerequisites

```powershell
docker compose start superset
docker compose logs -f superset      # wait for: Running on http://0.0.0.0:8088
```

Open <http://localhost:8088>, log in `admin` / `admin`, then **Settings → Database Connections → + Database → ClickHouse Connect**:

| Field | Value |
|---|---|
| HOST | `ids-clickhouse` |
| PORT | `8123` |
| DATABASE | `ids` |
| USERNAME | `default` |
| PASSWORD | `ids_local_dev` |
| SSL | **off** |

> `ids-clickhouse`, **not** `localhost`. Superset runs inside a container, where `localhost` is Superset itself. The host name comes from `container_name` in `docker-compose.yml`; the password from `CLICKHOUSE_PASSWORD` on the same service.

---

# The dashboard

## A. Volume row — four Big Numbers

One dataset, four charts. Raw counts, before any rate is derived from them.

```sql
SELECT
    (SELECT sumIf(record_count, label != 'normal')
     FROM ids.traffic_counts WHERE NOT startsWith(label, '__'))  AS attack_records,
    (SELECT sum(record_count)
     FROM ids.traffic_counts WHERE NOT startsWith(label, '__'))  AS total_records,
    countIf(label != 'normal' AND is_detection)                  AS true_positives,
    countIf(label != 'normal' AND NOT is_detection)              AS missed,
    countIf(label =  'normal' AND is_detection)                  AS false_positives
FROM ids.detections
```

| Chart | Metric | Subheader |
|---|---|---|
| Attack traffic detected | `attack_records` | `5% of 510,000 records` |
| True positives | `true_positives` | `caught` |
| Missed traffic | `missed` | `missed` |
| False positives | `false_positives` | `false alarms` |

> **Make the count the hero, not the percentage.** The attack *share* is fixed by `--normal-ratio` in the generator — it reads 5% on every run and carries no information. The count is what moves as the pipeline runs.
>
> Colour `missed` and `false_positives` with your status/warning colour. Both already carry labels, so colour is additive rather than the sole signal, and it directs the eye to what needs attention instead of leaving four equally-weighted black numbers.

---

## B. Accuracy row — three Big Numbers

The query that **needs both tables**. `detections` yields precision and recall, but the false-positive *rate* requires the count of normal traffic that was correctly ignored — and correctly-ignored traffic is never stored. `traffic_counts` supplies that denominator.

```sql
SELECT
    tp / (tp + fn)     AS recall,
    tp / (tp + fp)     AS `precision`,
    fp / normal_total  AS false_positive_rate
FROM
(
    SELECT
        countIf(label != 'normal' AND is_detection)     AS tp,
        countIf(label != 'normal' AND NOT is_detection) AS fn,
        countIf(label =  'normal' AND is_detection)     AS fp
    FROM ids.detections
) AS d
CROSS JOIN
(
    SELECT sum(record_count) AS normal_total
    FROM ids.traffic_counts
    WHERE label = 'normal'
) AS t
```

| Metric | D3 format | Renders |
|---|---|---|
| `recall` | `.1%` | `98.5%` |
| `precision` | `.1%` | `89.7%` |
| `false_positive_rate` | `.3%` | `0.595%` |

**Recall first.** Missing an attack is the costly failure in a security system, so it leads the row.

> **Return raw ratios here, not percentages.** Big Number charts have a D3 format control that multiplies by 100 itself — a `* 100` in the SQL renders `8,970%`.
>
> The false-positive rate needs three decimals. At `.1%` it rounds to `0.6%` and the difference between 0.59% and 0.64% disappears, which is exactly the range worth watching when tuning a rule.
>
> `precision` is backticked because it is a reserved word in several SQL dialects.

---

## C. Alert table — the centrepiece

One row per **alert**, not per record. Real IDS consoles deduplicate; showing thousands of near-identical `brute_force` lines is not how anyone reads this data.

```sql
SELECT
    formatDateTime(toStartOfMinute(toDateTime(timestamp)),
                   '%Y-%m-%d %H:%i',
                   'Asia/Ho_Chi_Minh')             AS window_start,
    toStartOfMinute(toDateTime(timestamp))         AS window_start_ts,
    multiIf(
        label =  'normal' AND is_detection,                      'FALSE POSITIVE',
        label != 'normal' AND NOT is_detection,                  'MISSED',
        label != 'normal' AND has(matched_attack_types, label),  'CORRECT',
        label != 'normal' AND is_detection,                      'WRONG TYPE',
                                                                 'unclassified'
    )                                              AS verdict,
    label                                          AS actual_label,
    arrayStringConcat(matched_attack_types, ', ')  AS detected_as,
    arrayStringConcat(matched_rule_ids, ', ')      AS rules_fired,
    max_severity,

    count()                                        AS event_count,
    uniqExact(service)                             AS services_touched,
    min(`count`)                                   AS conn_min,
    max(`count`)                                   AS conn_max,
    round(avg(latency_seconds), 2)                 AS avg_latency_s,

    multiIf(
        label =  'normal' AND is_detection,     0,
        label != 'normal' AND NOT is_detection, 1,
                                                2
    )                                              AS problem_rank
FROM ids.detections
GROUP BY window_start, window_start_ts, verdict, actual_label, detected_as,
         rules_fired, max_severity, problem_rank
```

- **Chart type:** Table, **Query mode: Raw records** (the SQL already aggregates — do not let Superset aggregate again)
- **Columns:** `window_start`, `verdict`, `actual_label`, `detected_as`, `event_count`, `max_severity`, `rules_fired`, `conn_min`, `conn_max`, `services_touched`
- **Ordering:** `window_start_ts` desc → `problem_rank` asc → `event_count` desc
- **Row limit:** 100

**Reading a row:** *"During the 10:24 minute, 1,008 records were genuinely DDoS, `ddos_flood` fired, the verdict was CORRECT, they touched 15 services, and connection counts ran 200–510."*

| Verdict | Meaning |
|---|---|
| `CORRECT` | attack caught, attributed to the right type |
| `WRONG TYPE` | attack caught, but labelled as a different attack |
| `MISSED` | real attack, no rule fired |
| `FALSE POSITIVE` | normal traffic flagged |

> `WRONG TYPE` exists because one row can be flagged by a rule belonging to another attack class — a single normal record has been seen tripping both `malware_c2_beacon` and `malicious_download_inbound`. Without that state those count as correct, which is precisely what hides the `malicious_download` precision problem.
>
> **Make problems findable with a dashboard filter on `verdict`** (Filter box → Value → `verdict`). One click shows only false positives. Superset's conditional formatting works on **numeric columns only**, so `verdict` cannot be coloured — but `problem_rank` can, if you prefer colour to filtering.
>
> **Why `problem_rank` is in the sort.** `generate_batch()` stamps `time.time()` once per 5,000-record chunk, so thousands of records share a timestamp. With that many ties the engine falls back to storage order, and `detections` is `ORDER BY (label, timestamp)` — `brute_force` sorts first alphabetically and fills the row limit with one attack type. `problem_rank` floats false positives and misses to the top of each group instead.

---

## D. Recall by attack type — a table, not a bar chart

```sql
SELECT
    label                                           AS attack_type,
    count()                                         AS seen,
    countIf(is_detection)                           AS caught,
    countIf(NOT is_detection)                       AS missed,
    round(countIf(is_detection) / count() * 100, 1) AS recall_pct
FROM ids.detections
WHERE label != 'normal'
GROUP BY attack_type
ORDER BY missed DESC
```

- **Chart type:** Table, **Query mode: Raw records**
- **Columns:** `attack_type`, `seen`, `caught`, `missed`, `recall_pct`
- **Customize → Cell bars: on**

> **Why not a bar chart.** Recall runs 95–100%. Bars must be baselined at zero, and from zero six bars spanning 95–100% are visually identical — the chart would be correct and communicate nothing. Truncating the axis to 90–100% would make differences visible but is a worse fault: it exaggerates small gaps and is one of the most-cited chart deceptions.
>
> A table sidesteps it. Percentages are *read* rather than compared by length, and `missed` sorted descending puts the two weak rules on top with real cell bars while the four perfect ones show `0`. "191 attacks walked past" also lands harder than "95.7% recall".
>
> If you want a chart too, plot **`missed`** as bars — genuine zero baseline, four categories at zero and two standing up. Never plot `recall_pct` as bars.
>
> **Percentages in tables are multiplied in SQL**, unlike the Big Numbers in row B — Table charts in raw-records mode have no reliable per-column format control.

---

## E. Precision by attack type — where the noise is

`arrayJoin` fans each row out to every attack type it was *claimed* as, so a wrong attribution counts against that type. This is the chart that exposes `malicious_download` at ~62%.

```sql
SELECT
    attack_type,
    count()                                                AS times_claimed,
    countIf(label = attack_type)                           AS correct,
    round(countIf(label = attack_type) / count() * 100, 1) AS precision_pct
FROM (
    SELECT label, arrayJoin(matched_attack_types) AS attack_type
    FROM ids.detections
)
GROUP BY attack_type
ORDER BY precision_pct ASC
```

**Chart type:** Bar Chart, horizontal, `attack_type` as dimension, `precision_pct` as metric.

> **Bars work here** — unlike recall, precision spans 62%–100%, a genuinely wide range that a zero-baselined bar renders honestly and legibly.

---

## F. Detection latency over time

The evidence behind the performance section. Add a reference line at **2.0** for the original target: the gap is the finding, and showing it is more honest than omitting it.

```sql
SELECT
    toStartOfMinute(processed_at)             AS minute,
    round(quantile(0.50)(latency_seconds), 2) AS p50_s,
    round(quantile(0.95)(latency_seconds), 2) AS p95_s,
    round(max(latency_seconds), 2)            AS max_s
FROM ids.detections
WHERE processed_at >= now() - INTERVAL 15 MINUTE
GROUP BY minute
ORDER BY minute
```

**Chart type:** **Line Chart** (the ECharts one), X-axis `minute`, **Time Grain: Minute**.
**Metrics:** `AVG(p50_s)`, `AVG(p95_s)`, `MAX(max_s)`.

> **Use `AVG`/`MAX`, never `SUM`, on percentile columns.** Summing a p50 adds percentiles together, which means nothing. `SUM` appears to work here only because the SQL emits one row per minute and the grain is also Minute, so each bucket holds exactly one row — switch the grain to Hour and you would silently get 60 p50s added up.

> **Widen the interval while building the chart.** `now() - INTERVAL 15 MINUTE` returns **zero rows** unless the pipeline ran in the last quarter hour — Superset then shows "No results were returned for this query", which looks like a broken chart rather than an empty window. Use `INTERVAL 1 DAY` to develop against historical data, and narrow it back before the live demo.
>
> **Filter the window in SQL, not in the chart.** With no filter the chart spans every run ever made, and a line chart draws straight through the hours of idle time between them — those long featureless slopes are interpolation, not data. Superset's filter presets bottom out at "Last day", far too coarse. `now() - INTERVAL 15 MINUTE` is evaluated per query, so on a live dashboard the window also slides forward.
>
> **Run the producer for ~5 minutes, not 60 seconds.** At Minute grain a one-minute run yields one or two points, with partial minutes at each end producing meaningless spikes.

---

# Optional — drill-down and extras

Neither belongs on the main dashboard. Put the raw table on a second tab if you want a click-through path.

<details>
<summary><b>Raw record table</b> — one row per stored record, for investigating a single alert</summary>

```sql
SELECT
    formatDateTime(toDateTime(timestamp),
                   '%Y-%m-%d %H:%i:%S',
                   'Asia/Ho_Chi_Minh')             AS event_time,
    formatDateTime(toStartOfMinute(toDateTime(timestamp)),
                   '%Y-%m-%d %H:%i',
                   'Asia/Ho_Chi_Minh')             AS window_start,
    toDateTime(timestamp)                          AS event_ts,
    label                                          AS actual_label,
    arrayStringConcat(matched_attack_types, ', ')  AS detected_as,
    arrayStringConcat(matched_rule_ids, ', ')      AS rules_fired,
    max_severity,
    multiIf(
        label =  'normal' AND is_detection,                      'FALSE POSITIVE',
        label != 'normal' AND NOT is_detection,                  'MISSED',
        label != 'normal' AND has(matched_attack_types, label),  'CORRECT',
        label != 'normal' AND is_detection,                      'WRONG TYPE',
                                                                 'unclassified'
    )                                              AS verdict,
    protocol_type,
    service,
    src_bytes,
    dst_bytes,
    `count`                                        AS connection_count,
    round(latency_seconds, 3)                      AS latency_s,
    multiIf(
        label =  'normal' AND is_detection,     0,
        label != 'normal' AND NOT is_detection, 1,
                                                2
    )                                              AS problem_rank
FROM ids.detections
```

Ordering: `event_ts` desc → `problem_rank` asc. Row limit 500.

`window_start` is included deliberately: **cross-filters match on column name**, so sharing it with chart C lets a click on an alert narrow this table to the records inside that minute.

</details>

<details>
<summary><b>Throughput over time</b> — only if the report needs a picture of it</summary>

```sql
SELECT
    batch_time,
    sum(record_count) AS records
FROM ids.traffic_counts
WHERE NOT startsWith(label, '__')
  AND batch_time >= now() - INTERVAL 15 MINUTE
GROUP BY batch_time
ORDER BY batch_time
```

**Line Chart**, Time Grain **Minute**. The sustained rate is already stated as a number in the report, so this chart is largely redundant — take latency (chart F) if you only have room for one.

If you need finer than Minute, Superset's Time Grain list is limited for ClickHouse. Bypass it by making the x-axis a string — a grain is only applied to *temporal* columns:

```sql
formatDateTime(batch_time, '%H:%i:%S', 'Asia/Ho_Chi_Minh') AS t
```

then use a **Bar Chart** with `t` as the dimension. `HH:MM:SS` sorts lexicographically in time order.

</details>

---

# Deliberately excluded

Recording these matters as much as the charts themselves — each was considered and cut for a reason worth stating in the report.

| Chart | Why it was cut |
|---|---|
| **Accuracy %** | Misleading under class imbalance. Traffic is 95% normal, so a detector that flags **nothing** scores 95.0%. Ours reaches 99.4% — only 4.36 points above the do-nothing baseline. Recall and false-positive rate cannot be gamed that way; accuracy can. If you keep it, retitle it **"Accuracy (baseline 95.0%)"** so it disarms the obvious question. |
| **F1** | Fully determined by precision and recall, both already on screen. Adds a tile, adds no information. |
| **Malformed records** | Permanently reads `0` on a healthy pipeline — dead real estate. The counter is genuinely valuable (it silently read zero for the *wrong* reason before it was fixed), but its value is demonstrated by the **Stage 8 failure test**: inject bad messages, screenshot it counting 2 unparseable and 1 incomplete. One image of it moving proves far more than a flat zero ever will. |
| **Attack-type breakdown pie/bar** | `generate_batch()` splits the attack share evenly across the six types, so the bars are near-identical by construction. The chart shows your *configuration*, not a finding. |
| **Two-slice attack-vs-normal pie** | A two-slice pie is a stat tile wearing a costume — the ratio is the whole message and a circle adds nothing. Replaced by the Big Number in row A. |

If a chart is cut, the underlying query is still worth keeping in this file. The malformed query in particular is what you run during the failure test:

```sql
SELECT
    multiIf(label = '__malformed_unparseable__', 'Unparseable (transmission)',
            label = '__malformed_incomplete__',  'Incomplete (generation/schema)',
            label)            AS failure_kind,
    sum(record_count)         AS records
FROM ids.traffic_counts
WHERE startsWith(label, '__')
GROUP BY failure_kind
```

---

# Running it live

Every query is a plain aggregate over tables Spark is actively writing to, and ClickHouse makes inserts visible to `SELECT` immediately — so all charts update while the pipeline runs. Four things must be set for that to actually happen.

**1. Turn on auto-refresh.** Dashboard → **⋯ → Edit properties → Refresh frequency → 30 seconds**, then **save the dashboard**. Unsaved, the setting does not persist.

**2. Disable result caching.** Each dataset → **Edit → Settings → Cache timeout → `0`**. A cached chart shows data from ten minutes ago while the pipeline runs beside it — the most confusing possible failure, because nothing looks broken.

**3. Truncate both tables before the run.**

```powershell
docker exec ids-clickhouse clickhouse-client --query "TRUNCATE TABLE ids.detections"
docker exec ids-clickhouse clickhouse-client --query "TRUNCATE TABLE ids.traffic_counts"
```

The KPI tiles are cumulative over everything in `detections`. Without a truncate you are showing several runs blended, and the numbers barely move because new data is a small fraction of the total.

**4. Use `now() - INTERVAL n MINUTE` in the time-series queries.** Evaluated per query, so the window slides forward on each refresh instead of staying pinned to when you built the chart.

### Two things to watch

**Don't add a `rand()` tie-breaker to any live table.** It is recomputed on every query, so tied rows reorder every 30 seconds — the table looks busy in a way that is not real movement. Genuinely new rows still arrive at the top without it.

**Auto-refresh competes with the pipeline for the same machine.** Charts re-querying every 30 seconds, while Spark and Kafka already saturate the laptop (see the performance analysis in `markdown.md`), is measurable contention and shows up in batch durations. Use **30 or 60 seconds, not 10**. The queries are cheap; the aggregate effect on a busy host is not.

---

# Layout

```
[ Attack traffic ][ True positives ][ Missed ][ False alarms ]     <- A, 4 tiles
[ Recall ][ Precision ][ False-positive rate ]                     <- B, 3 tiles
[ C — ALERT TABLE, full width                              ]
[ D — recall/miss table    ][ E — precision by type bar    ]
[ F — detection latency, with a 2s reference line          ]
```

Row A is *what happened*, row B is *how well*, C is *what is happening now*, D and E are *where it is weak*, F is *is it keeping up*.

---

# Traps specific to this schema

**1. `__%__` pseudo-labels are counters, not traffic.** `traffic_counts` carries `__malformed_unparseable__` and `__malformed_incomplete__` alongside real labels. Any query computing a rate must exclude them or the denominator is wrong. Use `NOT startsWith(label, '__')`, which avoids LIKE-escaping problems entirely.

**2. `count` is both a column name and a function.** `detections` has a `count` feature column (NSL-KDD connection count). Backtick it — `` `count` `` — or ClickHouse parses it as the aggregate.

**3. Arrays don't render in Superset tables.** `matched_rule_ids` and `matched_attack_types` are `Array(String)`. Wrap them in `arrayStringConcat(col, ', ')`.

**4. `timestamp` is a Float64 epoch, not a DateTime.** Use `toDateTime(timestamp)`. `processed_at` and `batch_time` are already `DateTime64(3)`.

**4a. Times are stored in UTC, and Superset re-converts temporal columns.** `time.time()` is timezone-free and the ClickHouse container runs UTC, so raw values render 7 hours behind local. Attaching a timezone in SQL is *not* enough: Superset flags the result as temporal (the clock icon in the Columns panel) and normalises it back to UTC, discarding the offset. Hand it a **formatted string** instead. `YYYY-MM-DD HH:MM` sorts lexicographically in chronological order, so ordering is unaffected. Keep a real `DateTime` alongside it for filters and cross-filtering.

**4b. `formatDateTime` uses MySQL specifiers, not strftime.** `%M` is the **full month name**; minutes are **`%i`**. `'%Y-%m-%d %H:%M'` silently produces `2026-08-25 10:August`.

**4c. After changing a dataset's SQL, three steps — not one.** Save/overwrite the dataset; **Sync columns from source** on the dataset (Superset caches column names *and types*, so a column stays flagged temporal until you resync); then **⋯ → Force refresh** on the chart, since results are cached separately. Skipping any of these makes a correct fix look like it did nothing.

**5. `detections` is not all traffic.** It holds only real attacks and flagged rows — about 5.6% of ingest. Never compute a share of total traffic from it; use `traffic_counts` for denominators.

**6. Percentage formatting differs by chart type.** **Big Number** charts have a D3 format control, so return the raw ratio (`0.897`) and set `.1%` — a `* 100` in SQL there renders `8,970%`. **Table** charts in raw-records mode have no reliable per-column format control, so multiply by 100 in SQL and name the column `*_pct`.

**7. `ORDER BY` and `LIMIT` do not belong in a virtual dataset.** Superset wraps the SQL as a subquery and adds its own. An inner `ORDER BY` may be discarded; an inner `LIMIT` **is** applied and will silently select an arbitrary subset. Keep virtual datasets as plain projections, expose computed sort keys as columns, and set ordering and row limit in the chart controls.

**8. Cross-filters match on column name.** For a click in one chart to filter another, both must expose a column with the same name — which is why the raw table carries `window_start` even though it also has `event_time`.

**9. Truncating tables mid-demo empties the dashboard.** The charts read live tables, so a `TRUNCATE` before a fresh run blanks everything until the pipeline writes again.
