"""
evaluate_accuracy.py

The capstone accuracy report. Reads what the pipeline actually stored in
ClickHouse and scores it: per-attack-type precision/recall/F1, overall
false-positive rate, observed detection latency against the <2s target, and
the malformed-record rate.

This is NOT rules/validate_rules.py. That one scores rules.json offline,
straight from the generator, with no Kafka and no database - a fast dev-time
loop for tuning thresholds. This one scores the LIVE PIPELINE end to end:
everything here survived serialization, Kafka, Spark parsing, rule
evaluation and a ClickHouse insert. If the two disagree, the difference is
the pipeline, and that is worth investigating rather than averaging away.

    python evaluation/evaluate_accuracy.py
    python evaluation/evaluate_accuracy.py --markdown report/accuracy.md
    python evaluation/evaluate_accuracy.py --self-test

Where the numbers come from
---------------------------
`detections` holds every real attack and every flagged row, so true
positives, false negatives and false positives are all countable there
directly.

It cannot supply the false-positive RATE. Correctly-ignored normal traffic
is never stored - that is the deliberate design that keeps the table at ~5%
of ingest - so there is no denominator in `detections` for "false positives
out of how many normal records". `traffic_counts` supplies exactly that, at
the cost of one small row per (batch, label).

    recall, precision, F1   <- detections alone
    false-positive rate     <- needs traffic_counts
    malformed rate          <- traffic_counts only

Reserved pseudo-labels
----------------------
`traffic_counts` also carries `__malformed_unparseable__` and
`__malformed_incomplete__`. Those are counters, not traffic. Every query
below that computes a rate filters them out with
schema.REAL_TRAFFIC_ONLY_SQL; leaving them in silently inflates the
denominator and understates the false-positive rate.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Sequence

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "spark_app"))

import config
# NOTE: importing schema pulls in pyspark, which is heavier than a report
# script strictly needs. It is deliberate: REAL_TRAFFIC_ONLY_SQL and the
# malformed label names must have exactly one definition, and that
# definition belongs next to the schema they describe. pyspark is already a
# project dependency, so this costs import time, not an install.
from schema import (
    MALFORMED_INCOMPLETE,
    MALFORMED_UNPARSEABLE,
    REAL_TRAFFIC_ONLY_SQL,
)

LATENCY_TARGET_SECONDS = 2.0


# ---------------------------------------------------------------------------
# Data carriers
# ---------------------------------------------------------------------------

@dataclass
class LabelStats:
    """Per-attack-type scoring. `predicted`/`correct` come from the
    matched_attack_types attribution, not from is_detection, so a row the
    rules flagged as the WRONG attack type counts against that type's
    precision - which is the honest reading."""
    label: str
    support: int = 0          # records of this class the pipeline saw
    stored: int = 0           # rows landed in `detections`
    caught: int = 0           # is_detection = true
    predicted: int = 0        # rows attributed to this type by any rule
    correct: int = 0          # ...of which the true label agrees

    @property
    def missed(self) -> int:
        return self.stored - self.caught

    @property
    def recall(self) -> Optional[float]:
        return self.caught / self.stored if self.stored else None

    @property
    def precision(self) -> Optional[float]:
        return self.correct / self.predicted if self.predicted else None

    @property
    def f1(self) -> Optional[float]:
        p, r = self.precision, self.recall
        if not p or not r:
            return None
        return 2 * p * r / (p + r)


@dataclass
class Report:
    batches: int = 0
    first_batch_time: str = ""
    last_batch_time: str = ""

    total_records: int = 0        # real traffic only, malformed excluded
    normal_total: int = 0
    attack_total: int = 0

    tp: int = 0
    fn: int = 0
    fp: int = 0

    labels: list[LabelStats] = field(default_factory=list)

    latency: dict = field(default_factory=dict)
    malformed: dict = field(default_factory=dict)
    integrity: list[tuple[str, bool, str]] = field(default_factory=list)

    # -- derived ----------------------------------------------------------
    @property
    def tn(self) -> int:
        return self.normal_total - self.fp

    @property
    def precision(self) -> Optional[float]:
        d = self.tp + self.fp
        return self.tp / d if d else None

    @property
    def recall(self) -> Optional[float]:
        d = self.tp + self.fn
        return self.tp / d if d else None

    @property
    def f1(self) -> Optional[float]:
        p, r = self.precision, self.recall
        if not p or not r:
            return None
        return 2 * p * r / (p + r)

    @property
    def false_positive_rate(self) -> Optional[float]:
        """FP / all normal traffic seen. The number `detections` cannot
        produce on its own."""
        return self.fp / self.normal_total if self.normal_total else None

    @property
    def accuracy(self) -> Optional[float]:
        total = self.attack_total + self.normal_total
        return (self.tp + self.tn) / total if total else None

    @property
    def malformed_total(self) -> int:
        return sum(self.malformed.values())

    @property
    def malformed_rate(self) -> Optional[float]:
        seen = self.total_records + self.malformed_total
        return self.malformed_total / seen if seen else None


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

def _q(database: str, table: str) -> str:
    return f"`{database}`.`{table}`"


def build_queries(database: str) -> dict[str, str]:
    """Every query the report runs, in one place so they can be read (and
    pasted into clickhouse-client) without digging through call sites."""
    det = _q(database, "detections")
    cnt = _q(database, "traffic_counts")
    return {
        # Per-label outcome among stored rows. Every attack row is stored
        # (label != 'normal' always qualifies), so caught/missed here are
        # complete, not a sample.
        "per_label": f"""
            SELECT label,
                   count()                  AS stored,
                   countIf(is_detection)    AS caught
            FROM {det}
            GROUP BY label
            ORDER BY label
        """,
        # Attribution: which attack type did the rules claim, and was it
        # right. arrayJoin fans a multi-match row out to one row per claimed
        # type, which is what per-type precision should count.
        "attribution": f"""
            SELECT attack_type,
                   count()                        AS predicted,
                   countIf(label = attack_type)   AS correct
            FROM (
                SELECT label, arrayJoin(matched_attack_types) AS attack_type
                FROM {det}
            )
            GROUP BY attack_type
            ORDER BY attack_type
        """,
        # Real traffic volume - the denominators. Pseudo-labels excluded.
        "traffic": f"""
            SELECT label, sum(record_count) AS total
            FROM {cnt}
            WHERE {REAL_TRAFFIC_ONLY_SQL}
            GROUP BY label
            ORDER BY label
        """,
        # Pipeline health. The complement of the filter above.
        "malformed": f"""
            SELECT label, sum(record_count) AS total
            FROM {cnt}
            WHERE NOT ({REAL_TRAFFIC_ONLY_SQL})
            GROUP BY label
            ORDER BY label
        """,
        # Observed producer-to-storage latency. NOTE this covers stored rows
        # only - attacks and flagged traffic - not every record. That is the
        # right population for a DETECTION latency claim, but say so.
        "latency": f"""
            SELECT count()                              AS n,
                   avg(latency_seconds)                 AS mean,
                   quantile(0.50)(latency_seconds)      AS p50,
                   quantile(0.95)(latency_seconds)      AS p95,
                   quantile(0.99)(latency_seconds)      AS p99,
                   max(latency_seconds)                 AS max,
                   countIf(latency_seconds < {LATENCY_TARGET_SECONDS}) AS under_target
            FROM {det}
        """,
        "window": f"""
            SELECT count(DISTINCT batch_id)  AS batches,
                   min(batch_time)           AS first_seen,
                   max(batch_time)           AS last_seen
            FROM {cnt}
        """,
    }


# ---------------------------------------------------------------------------
# Computation - pure, no I/O, unit-testable
# ---------------------------------------------------------------------------

def compute_report(
    per_label: Sequence[tuple],
    attribution: Sequence[tuple],
    traffic: Sequence[tuple],
    malformed: Sequence[tuple],
    latency: Optional[tuple] = None,
    window: Optional[tuple] = None,
) -> Report:
    """Turn raw query rows into a scored Report.

    Kept free of any database or pandas dependency so the arithmetic can be
    checked against known-truth fixtures - see --self-test.
    """
    rep = Report()

    if window:
        rep.batches = int(window[0] or 0)
        rep.first_batch_time = str(window[1] or "")
        rep.last_batch_time = str(window[2] or "")

    traffic_by_label = {str(l): int(n) for l, n in traffic}
    rep.normal_total = traffic_by_label.get("normal", 0)
    rep.total_records = sum(traffic_by_label.values())
    rep.attack_total = rep.total_records - rep.normal_total

    attr = {str(a): (int(p), int(c)) for a, p, c in attribution}
    stats: dict[str, LabelStats] = {}

    for label, stored, caught in per_label:
        label = str(label)
        stored, caught = int(stored), int(caught)
        if label == "normal":
            # Every stored normal row is by definition a false positive:
            # normal traffic only reaches `detections` when a rule fired.
            rep.fp = stored
            continue
        s = stats.setdefault(label, LabelStats(label))
        s.stored, s.caught = stored, caught
        s.support = traffic_by_label.get(label, stored)
        rep.tp += caught
        rep.fn += stored - caught

    # An attack type may be claimed by the rules without ever appearing as a
    # true label (pure misattribution), so seed from attribution too.
    for atype, (predicted, correct) in attr.items():
        s = stats.setdefault(atype, LabelStats(atype))
        s.predicted, s.correct = predicted, correct
        if not s.support:
            s.support = traffic_by_label.get(atype, 0)

    rep.labels = sorted(stats.values(), key=lambda s: s.label)
    rep.malformed = {str(l): int(n) for l, n in malformed}

    if latency and latency[0]:
        n = int(latency[0])
        rep.latency = {
            "n": n,
            "mean": float(latency[1] or 0.0),
            "p50": float(latency[2] or 0.0),
            "p95": float(latency[3] or 0.0),
            "p99": float(latency[4] or 0.0),
            "max": float(latency[5] or 0.0),
            "under_target": int(latency[6] or 0),
            "under_target_pct": (int(latency[6] or 0) / n) if n else None,
        }

    rep.integrity = _integrity_checks(rep, traffic_by_label, stats)
    return rep


def _integrity_checks(rep, traffic_by_label, stats) -> list[tuple[str, bool, str]]:
    """Cross-checks between the two tables. These catch dropped rows, a
    half-written batch, or a report run while the stream is still going -
    all of which produce plausible-looking but wrong metrics."""
    checks: list[tuple[str, bool, str]] = []

    for s in stats.values():
        expected = traffic_by_label.get(s.label)
        if expected is None:
            continue
        ok = expected == s.stored
        checks.append((
            f"every {s.label} record reached `detections`",
            ok,
            f"traffic_counts {expected:,} vs detections {s.stored:,}"
            + ("" if ok else "  <- rows missing; was the report run mid-stream?"),
        ))

    ok = rep.fp <= rep.normal_total
    checks.append((
        "false positives do not exceed normal traffic seen",
        ok,
        f"{rep.fp:,} of {rep.normal_total:,}",
    ))

    if rep.latency:
        ok = rep.latency["max"] >= 0
        checks.append((
            "latency values are non-negative",
            ok,
            f"max {rep.latency['max']:.3f}s"
            + ("" if ok else "  <- clock skew between producer and Spark host"),
        ))

    return checks


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _pct(x: Optional[float], places: int = 1) -> str:
    return "n/a" if x is None else f"{x * 100:.{places}f}%"


def render_text(rep: Report) -> str:
    L: list[str] = []
    w = 78
    L.append("=" * w)
    L.append("STREAMING IDS - ACCURACY REPORT".center(w))
    L.append("=" * w)

    L.append("")
    L.append(f"Batches scored     {rep.batches:,}")
    if rep.first_batch_time:
        L.append(f"Window             {rep.first_batch_time}  ->  {rep.last_batch_time}")
    L.append(f"Records seen       {rep.total_records:,}"
             f"   ({rep.normal_total:,} normal, {rep.attack_total:,} attack)")

    L.append("")
    L.append("-" * w)
    L.append("OVERALL")
    L.append("-" * w)
    L.append(f"  true positives    {rep.tp:>10,}    attacks correctly flagged")
    L.append(f"  false negatives   {rep.fn:>10,}    attacks missed")
    L.append(f"  false positives   {rep.fp:>10,}    normal traffic flagged")
    L.append(f"  true negatives    {rep.tn:>10,}    normal traffic correctly ignored")
    L.append("")
    L.append(f"  precision         {_pct(rep.precision):>10}")
    L.append(f"  recall            {_pct(rep.recall):>10}")
    L.append(f"  F1                {_pct(rep.f1):>10}")
    L.append(f"  accuracy          {_pct(rep.accuracy, 2):>10}")
    L.append(f"  false-pos rate    {_pct(rep.false_positive_rate, 3):>10}"
             "    <- requires traffic_counts")

    L.append("")
    L.append("-" * w)
    L.append("BY ATTACK TYPE")
    L.append("-" * w)
    L.append(f"  {'type':<22}{'seen':>8}{'caught':>8}{'missed':>8}"
             f"{'recall':>9}{'prec':>9}{'F1':>8}")
    for s in rep.labels:
        L.append(
            f"  {s.label:<22}{s.stored:>8,}{s.caught:>8,}{s.missed:>8,}"
            f"{_pct(s.recall):>9}{_pct(s.precision):>9}{_pct(s.f1):>8}"
        )

    if rep.latency:
        lat = rep.latency
        L.append("")
        L.append("-" * w)
        L.append(f"DETECTION LATENCY  (target < {LATENCY_TARGET_SECONDS:.0f}s)")
        L.append("-" * w)
        L.append(f"  mean {lat['mean']:.3f}s   p50 {lat['p50']:.3f}s   "
                 f"p95 {lat['p95']:.3f}s   p99 {lat['p99']:.3f}s   max {lat['max']:.3f}s")
        L.append(f"  under target      {lat['under_target']:,} of {lat['n']:,}"
                 f"  ({_pct(lat['under_target_pct'], 2)})")
        L.append("  measured over stored rows (attacks + flagged), producer timestamp")
        L.append("  to ClickHouse write - not every record on the topic.")

    L.append("")
    L.append("-" * w)
    L.append("PIPELINE HEALTH")
    L.append("-" * w)
    if rep.malformed_total == 0:
        L.append("  no malformed records")
    else:
        u = rep.malformed.get(MALFORMED_UNPARSEABLE, 0)
        i = rep.malformed.get(MALFORMED_INCOMPLETE, 0)
        L.append(f"  unparseable       {u:>10,}    transmission / serialization fault")
        L.append(f"  incomplete        {i:>10,}    generation fault or schema drift")
        L.append(f"  malformed rate    {_pct(rep.malformed_rate, 4):>10}")

    L.append("")
    L.append("-" * w)
    L.append("INTEGRITY")
    L.append("-" * w)
    for name, ok, detail in rep.integrity:
        L.append(f"  [{'OK' if ok else '!!'}] {name:<48} {detail}")

    L.append("")
    L.append("=" * w)
    return "\n".join(L)


def render_markdown(rep: Report) -> str:
    """Report-ready tables. Paste straight into the capstone writeup."""
    L: list[str] = []
    L.append("## Detection accuracy\n")
    L.append(f"Scored over {rep.batches:,} micro-batches — "
             f"{rep.total_records:,} records "
             f"({rep.normal_total:,} normal, {rep.attack_total:,} attack).\n")

    L.append("| Metric | Value |")
    L.append("|---|---:|")
    L.append(f"| Precision | {_pct(rep.precision)} |")
    L.append(f"| Recall | {_pct(rep.recall)} |")
    L.append(f"| F1 | {_pct(rep.f1)} |")
    L.append(f"| Accuracy | {_pct(rep.accuracy, 2)} |")
    L.append(f"| False-positive rate | {_pct(rep.false_positive_rate, 3)} |")
    L.append("")

    L.append("| Attack type | Seen | Caught | Missed | Recall | Precision | F1 |")
    L.append("|---|---:|---:|---:|---:|---:|---:|")
    for s in rep.labels:
        L.append(f"| `{s.label}` | {s.stored:,} | {s.caught:,} | {s.missed:,} | "
                 f"{_pct(s.recall)} | {_pct(s.precision)} | {_pct(s.f1)} |")
    L.append("")

    if rep.latency:
        lat = rep.latency
        L.append("## Detection latency\n")
        L.append(f"| Statistic | Seconds |")
        L.append("|---|---:|")
        for k in ("mean", "p50", "p95", "p99", "max"):
            L.append(f"| {k} | {lat[k]:.3f} |")
        L.append("")
        L.append(f"{lat['under_target']:,} of {lat['n']:,} "
                 f"({_pct(lat['under_target_pct'], 2)}) under the "
                 f"{LATENCY_TARGET_SECONDS:.0f}s target.\n")

    if rep.malformed_total:
        L.append("## Pipeline health\n")
        L.append("| Bucket | Count | Indicates |")
        L.append("|---|---:|---|")
        L.append(f"| Unparseable | {rep.malformed.get(MALFORMED_UNPARSEABLE, 0):,} "
                 "| Transmission or serialization fault |")
        L.append(f"| Incomplete | {rep.malformed.get(MALFORMED_INCOMPLETE, 0):,} "
                 "| Generation fault or schema drift |")
        L.append(f"\nMalformed rate: {_pct(rep.malformed_rate, 4)}.\n")

    return "\n".join(L)


def report_to_dict(rep: Report) -> dict:
    return {
        "window": {
            "batches": rep.batches,
            "first_batch_time": rep.first_batch_time,
            "last_batch_time": rep.last_batch_time,
        },
        "volume": {
            "total_records": rep.total_records,
            "normal": rep.normal_total,
            "attack": rep.attack_total,
        },
        "confusion": {"tp": rep.tp, "fn": rep.fn, "fp": rep.fp, "tn": rep.tn},
        "overall": {
            "precision": rep.precision,
            "recall": rep.recall,
            "f1": rep.f1,
            "accuracy": rep.accuracy,
            "false_positive_rate": rep.false_positive_rate,
        },
        "by_attack_type": [
            {
                "label": s.label, "seen": s.stored, "caught": s.caught,
                "missed": s.missed, "recall": s.recall,
                "precision": s.precision, "f1": s.f1,
            }
            for s in rep.labels
        ],
        "latency": rep.latency,
        "malformed": {
            "counts": rep.malformed,
            "total": rep.malformed_total,
            "rate": rep.malformed_rate,
        },
        "integrity": [
            {"check": n, "passed": ok, "detail": d} for n, ok, d in rep.integrity
        ],
    }


# ---------------------------------------------------------------------------
# ClickHouse access
# ---------------------------------------------------------------------------

def fetch_report(client, database: str) -> Report:
    q = build_queries(database)

    def rows(key):
        return client.query(q[key]).result_rows

    def one(key):
        r = rows(key)
        return r[0] if r else None

    return compute_report(
        per_label=rows("per_label"),
        attribution=rows("attribution"),
        traffic=rows("traffic"),
        malformed=rows("malformed"),
        latency=one("latency"),
        window=one("window"),
    )


# ---------------------------------------------------------------------------
# Self-test: the arithmetic, against a fixture whose answers are known
# ---------------------------------------------------------------------------

#: Captured from a real 3-batch run of the pipeline at the 14,880 rec/s demo
#: cap (seed 11), taken at the point the DataFrames would be handed to
#: insert_df(). Known-truth: tp=2206, fn=26, fp=250, normal seen=42,408.
_FIXTURE = {
    "per_label": [
        ("brute_force", 372, 372), ("data_exfiltration", 372, 361),
        ("ddos", 372, 372), ("malicious_download", 372, 357),
        ("malware_c2", 372, 372), ("normal", 250, 250),
        ("port_scan", 372, 372),
    ],
    "attribution": [
        ("brute_force", 372, 372), ("data_exfiltration", 398, 361),
        ("ddos", 372, 372), ("malicious_download", 557, 357),
        ("malware_c2", 385, 372), ("port_scan", 372, 372),
    ],
    "traffic": [
        ("brute_force", 372), ("data_exfiltration", 372), ("ddos", 372),
        ("malicious_download", 372), ("malware_c2", 372), ("normal", 42408),
        ("port_scan", 372),
    ],
    "malformed": [(MALFORMED_INCOMPLETE, 1), (MALFORMED_UNPARSEABLE, 2)],
    "latency": (2482, 0.412, 0.395, 0.688, 0.912, 1.204, 2482),
    "window": (3, "2026-08-24 08:49:34.717", "2026-08-24 08:49:47.224"),
}


def self_test() -> int:
    rep = compute_report(**_FIXTURE)

    def close(a, b, tol=5e-4):
        return a is not None and abs(a - b) < tol

    checks = [
        ("true positives = 2206", rep.tp == 2206),
        ("false negatives = 26", rep.fn == 26),
        ("false positives = 250", rep.fp == 250),
        ("true negatives = 42158", rep.tn == 42158),
        ("normal traffic seen = 42408", rep.normal_total == 42408),
        ("attack traffic seen = 2232", rep.attack_total == 2232),
        ("total records = 44640", rep.total_records == 44640),
        ("precision = 89.8%", close(rep.precision, 2206 / 2456)),
        ("recall = 98.8%", close(rep.recall, 2206 / 2232)),
        ("F1 = 94.1%", close(rep.f1, 0.941085)),
        ("false-positive rate = 0.590%", close(rep.false_positive_rate, 250 / 42408)),
        ("accuracy = 99.38%", close(rep.accuracy, 44364 / 44640)),
        ("malformed total = 3", rep.malformed_total == 3),
        ("malformed rate over all messages seen",
         close(rep.malformed_rate, 3 / 44643)),
        ("ddos recall = 100%", close(rep.labels[2].recall, 1.0)),
        ("malicious_download recall = 95.97%", close(rep.labels[3].recall, 357 / 372)),
        ("malicious_download precision = 64.1% (noisiest rule)",
         close(rep.labels[3].precision, 357 / 557)),
        ("every integrity check passes", all(ok for _, ok, _ in rep.integrity)),
    ]

    print(render_text(rep))
    print("\nSELF-TEST")
    failed = 0
    for name, ok in checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
        failed += 0 if ok else 1
    print(f"\n{'ALL PASSED' if not failed else f'{failed} FAILED'}")
    return 1 if failed else 0


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="Score the Streaming IDS against what it stored in ClickHouse.",
    )
    p.add_argument("--host", default=config.CLICKHOUSE_HOST)
    p.add_argument("--port", type=int, default=config.CLICKHOUSE_HTTP_PORT)
    p.add_argument("--user", default=config.CLICKHOUSE_USER)
    p.add_argument("--password", default=config.CLICKHOUSE_PASSWORD)
    p.add_argument("--database", default=config.CLICKHOUSE_DATABASE)
    p.add_argument("--json", metavar="PATH", help="also write the report as JSON")
    p.add_argument("--markdown", metavar="PATH",
                   help="also write report-ready markdown tables")
    p.add_argument("--show-sql", action="store_true",
                   help="print the queries and exit without connecting")
    p.add_argument("--self-test", action="store_true",
                   help="check the metric arithmetic against a known fixture")
    args = p.parse_args(argv)

    if args.self_test:
        return self_test()

    if args.show_sql:
        for name, sql in build_queries(args.database).items():
            print(f"-- {name}\n{sql.strip()}\n")
        return 0

    try:
        import clickhouse_connect
    except ImportError:
        print("clickhouse-connect is not installed. "
              "pip install clickhouse-connect", file=sys.stderr)
        return 2

    try:
        client = clickhouse_connect.get_client(
            host=args.host, port=args.port, username=args.user,
            password=args.password, database=args.database,
        )
    except Exception as exc:
        print(f"Could not reach ClickHouse at {args.host}:{args.port} - {exc}\n"
              "Is the stack up?  docker compose up -d", file=sys.stderr)
        return 2

    rep = fetch_report(client, args.database)

    if rep.total_records == 0:
        print("`traffic_counts` is empty - the streaming job has not written "
              "anything yet.\nStart streaming_job.py, then generate_traffic.py, "
              "then re-run this.", file=sys.stderr)
        return 3

    print(render_text(rep))

    if args.json:
        Path(args.json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json).write_text(
            json.dumps(report_to_dict(rep), indent=2), encoding="utf-8")
        print(f"\nJSON written to {args.json}")

    if args.markdown:
        Path(args.markdown).parent.mkdir(parents=True, exist_ok=True)
        Path(args.markdown).write_text(render_markdown(rep), encoding="utf-8")
        print(f"Markdown written to {args.markdown}")

    return 0 if all(ok for _, ok, _ in rep.integrity) else 4


if __name__ == "__main__":
    sys.exit(main())
