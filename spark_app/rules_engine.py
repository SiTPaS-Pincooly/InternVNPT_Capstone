"""
rules_engine.py

The detection layer of the Streaming IDS, kept deliberately separate from
streaming_job.py: this module knows *how to decide whether a connection
record is an attack*, and nothing about Kafka, Spark sessions, triggers,
checkpoints, or ClickHouse. streaming_job.py owns all of that plumbing and
calls into here.

That split is what makes the rules testable offline - rules/validate_rules.py
imports this module directly, feeds it generated traffic, and scores recall
and false-positive rate without a broker or a database anywhere in sight.

Two public functions:

    load_rules(path) -> list[Rule]
        Read and strictly validate rules.json. Fails loudly at startup
        (RuleLoadError) rather than silently producing a rule that can
        never match.

    evaluate(df, rules) -> DataFrame
        Add `matched_rule_ids`, `matched_attack_types`, `max_severity` and
        `is_detection` to a DataFrame of parsed NSL-KDD records.

Design notes
------------
- **No Python UDFs.** Every rule compiles down to native Spark Column
  expressions (`when`, `array`, `filter`, `array_max`). A UDF would cross
  the JVM/Python boundary once per row, which is real overhead against the
  <2s end-to-end latency target. Native expressions stay inside Catalyst
  and get optimised with the rest of the plan.

- **Rules never read `label`.** A real IDS does not get to see ground
  truth. `label` is validated as an *illegal* rule field for exactly that
  reason, and is reserved for accuracy scoring after the fact
  (evaluate_accuracy.py). Without this guard it would be trivially easy to
  write a rule that scores 100% and means nothing.

- **Nulls never match.** A malformed record that slipped through parsing
  with null features produces null comparisons in Spark, not False. Each
  condition is wrapped in coalesce(..., False) so a null feature counts as
  "did not match" instead of poisoning the AND chain.

- **Conditions within a rule are ANDed; rules are ORed.** Several rules may
  share an `attack_type` to express "either signature counts as this
  attack" - matched_attack_types is deduplicated for that reason.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from schema import NSL_KDD_SCHEMA


# --------------------------------------------------------------------------
# Vocabulary the validator enforces
# --------------------------------------------------------------------------

#: Comparison operators a rule condition may use.
SUPPORTED_OPERATORS = {">", ">=", "<", "<=", "==", "!=", "in", "not_in"}

#: Operators whose `value` must be a non-empty list.
_LIST_OPERATORS = {"in", "not_in"}

#: Operators that only make sense against a numeric field.
_ORDERING_OPERATORS = {">", ">=", "<", "<="}

#: Severity levels, ordered least to most severe. The index+1 is the rank
#: used to compute `max_severity` with array_max.
SEVERITY_ORDER = ["low", "medium", "high", "critical"]
_SEVERITY_RANK = {name: i + 1 for i, name in enumerate(SEVERITY_ORDER)}

#: Fields a rule is allowed to test. `label` is excluded on purpose - see
#: the module docstring.
_SCHEMA_TYPES = {f.name: f.dataType.typeName() for f in NSL_KDD_SCHEMA.fields}
FORBIDDEN_FIELDS = {"label"}
ALLOWED_FIELDS = set(_SCHEMA_TYPES) - FORBIDDEN_FIELDS

_NUMERIC_TYPES = {"double", "integer", "long", "float", "short", "byte"}

#: Columns evaluate() appends. Exposed so clickhouse_writer.py and the
#: accuracy report can refer to them by name instead of hardcoding strings.
OUTPUT_COLUMNS = (
    "matched_rule_ids",
    "matched_attack_types",
    "max_severity",
    "is_detection",
)

_REQUIRED_RULE_KEYS = {"rule_id", "attack_type", "severity", "conditions"}
_REQUIRED_CONDITION_KEYS = {"field", "op", "value"}


class RuleLoadError(Exception):
    """Raised when rules.json is malformed, or describes a rule that could
    never match. Deliberately fatal: a typo'd field name in a rule file is
    a silent 0%-recall bug if it is allowed through."""


@dataclass(frozen=True)
class Condition:
    field: str
    op: str
    value: Any


@dataclass(frozen=True)
class Rule:
    rule_id: str
    attack_type: str
    severity: str
    conditions: tuple[Condition, ...]
    description: str = ""

    @property
    def severity_rank(self) -> int:
        return _SEVERITY_RANK[self.severity]


# --------------------------------------------------------------------------
# Loading and validation
# --------------------------------------------------------------------------

def _validate_condition(raw: Any, rule_id: str, index: int) -> Condition:
    where = f"rule '{rule_id}', condition #{index}"

    if not isinstance(raw, dict):
        raise RuleLoadError(f"{where}: expected an object, got {type(raw).__name__}")

    missing = _REQUIRED_CONDITION_KEYS - raw.keys()
    if missing:
        raise RuleLoadError(f"{where}: missing key(s) {sorted(missing)}")

    unknown = raw.keys() - _REQUIRED_CONDITION_KEYS
    if unknown:
        raise RuleLoadError(f"{where}: unexpected key(s) {sorted(unknown)}")

    field, op, value = raw["field"], raw["op"], raw["value"]

    if field in FORBIDDEN_FIELDS:
        raise RuleLoadError(
            f"{where}: field '{field}' is not available to the detection logic. "
            "A real IDS cannot see ground truth; 'label' is reserved for "
            "accuracy scoring only."
        )

    if field not in ALLOWED_FIELDS:
        raise RuleLoadError(
            f"{where}: unknown field '{field}'. It is not in NSL_KDD_SCHEMA "
            "(check schema.py for the exact spelling)."
        )

    if op not in SUPPORTED_OPERATORS:
        raise RuleLoadError(
            f"{where}: unsupported operator '{op}'. "
            f"Supported: {sorted(SUPPORTED_OPERATORS)}"
        )

    field_type = _SCHEMA_TYPES[field]

    if op in _LIST_OPERATORS:
        if not isinstance(value, (list, tuple)) or len(value) == 0:
            raise RuleLoadError(
                f"{where}: operator '{op}' requires a non-empty list value, "
                f"got {value!r}"
            )
        if any(isinstance(v, (list, dict)) for v in value):
            raise RuleLoadError(f"{where}: list values must be scalars, got {value!r}")
        return Condition(field=field, op=op, value=tuple(value))

    if isinstance(value, (list, dict)):
        raise RuleLoadError(
            f"{where}: operator '{op}' requires a scalar value, got {value!r}"
        )

    # bool is a subclass of int in Python; allow it only where it makes sense.
    if op in _ORDERING_OPERATORS:
        if field_type not in _NUMERIC_TYPES:
            raise RuleLoadError(
                f"{where}: operator '{op}' needs a numeric field, but "
                f"'{field}' is {field_type}."
            )
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise RuleLoadError(
                f"{where}: operator '{op}' needs a numeric value, got {value!r}"
            )
    else:  # == / !=
        if field_type in _NUMERIC_TYPES and not isinstance(value, (int, float)):
            raise RuleLoadError(
                f"{where}: '{field}' is numeric ({field_type}) but the value "
                f"is {value!r}. A string here would never match."
            )
        if field_type == "string" and not isinstance(value, str):
            raise RuleLoadError(
                f"{where}: '{field}' is a string field but the value is "
                f"{value!r}. This would never match."
            )

    return Condition(field=field, op=op, value=value)


def _validate_rule(raw: Any, index: int) -> Rule:
    if not isinstance(raw, dict):
        raise RuleLoadError(
            f"rule #{index}: expected an object, got {type(raw).__name__}"
        )

    missing = _REQUIRED_RULE_KEYS - raw.keys()
    if missing:
        raise RuleLoadError(f"rule #{index}: missing key(s) {sorted(missing)}")

    rule_id = raw["rule_id"]
    if not isinstance(rule_id, str) or not rule_id.strip():
        raise RuleLoadError(f"rule #{index}: 'rule_id' must be a non-empty string")

    attack_type = raw["attack_type"]
    if not isinstance(attack_type, str) or not attack_type.strip():
        raise RuleLoadError(f"rule '{rule_id}': 'attack_type' must be a non-empty string")

    severity = raw["severity"]
    if severity not in _SEVERITY_RANK:
        raise RuleLoadError(
            f"rule '{rule_id}': invalid severity {severity!r}. "
            f"Valid: {SEVERITY_ORDER}"
        )

    raw_conditions = raw["conditions"]
    if not isinstance(raw_conditions, list) or len(raw_conditions) == 0:
        raise RuleLoadError(
            f"rule '{rule_id}': 'conditions' must be a non-empty list. "
            "A rule with no conditions would match every record."
        )

    conditions = tuple(
        _validate_condition(c, rule_id, i)
        for i, c in enumerate(raw_conditions)
    )

    description = raw.get("description", "")
    if not isinstance(description, str):
        raise RuleLoadError(f"rule '{rule_id}': 'description' must be a string")

    return Rule(
        rule_id=rule_id,
        attack_type=attack_type,
        severity=severity,
        conditions=conditions,
        description=description,
    )


def parse_rules(raw_rules: Any) -> list[Rule]:
    """Validate an already-deserialised rule list. Split out from
    load_rules() so tests can exercise validation without touching disk."""
    if not isinstance(raw_rules, list):
        raise RuleLoadError(
            f"rules file must contain a JSON array of rule objects, "
            f"got {type(raw_rules).__name__}"
        )
    if not raw_rules:
        raise RuleLoadError("rules file contains no rules - nothing would ever be detected")

    rules = [_validate_rule(raw, i) for i, raw in enumerate(raw_rules)]

    seen: dict[str, int] = {}
    for i, rule in enumerate(rules):
        if rule.rule_id in seen:
            raise RuleLoadError(
                f"duplicate rule_id '{rule.rule_id}' (rules #{seen[rule.rule_id]} "
                f"and #{i}). rule_ids must be unique - they are what identifies "
                "a detection downstream."
            )
        seen[rule.rule_id] = i

    return rules


def load_rules(path: str | Path) -> list[Rule]:
    """Read and strictly validate a rules JSON file.

    Raises RuleLoadError on anything wrong: missing file, bad JSON, unknown
    field or operator, duplicate rule_id, invalid severity, or a rule that
    reads `label`.
    """
    rules_path = Path(path)
    try:
        raw_text = rules_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise RuleLoadError(f"rules file not found: {rules_path}") from exc
    except OSError as exc:
        raise RuleLoadError(f"could not read rules file {rules_path}: {exc}") from exc

    try:
        raw_rules = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise RuleLoadError(f"invalid JSON in {rules_path}: {exc}") from exc

    return parse_rules(raw_rules)


# --------------------------------------------------------------------------
# Compiling rules to native Spark Column expressions
# --------------------------------------------------------------------------

def _condition_to_column(condition: Condition) -> Column:
    """Compile one condition into a never-null boolean Column."""
    col = F.col(condition.field)
    op, value = condition.op, condition.value

    if op == ">":
        expr = col > F.lit(value)
    elif op == ">=":
        expr = col >= F.lit(value)
    elif op == "<":
        expr = col < F.lit(value)
    elif op == "<=":
        expr = col <= F.lit(value)
    elif op == "==":
        expr = col == F.lit(value)
    elif op == "!=":
        expr = col != F.lit(value)
    elif op == "in":
        expr = col.isin(list(value))
    elif op == "not_in":
        expr = ~col.isin(list(value))
    else:  # unreachable - the validator rejects anything else
        raise RuleLoadError(f"unsupported operator '{op}'")

    # A null feature yields a null comparison in Spark. Treat that as
    # "no match" so one bad field can't poison the whole AND chain.
    return F.coalesce(expr, F.lit(False))


def _rule_to_column(rule: Rule) -> Column:
    """Compile a rule into a single boolean Column (its conditions ANDed)."""
    expr = _condition_to_column(rule.conditions[0])
    for condition in rule.conditions[1:]:
        expr = expr & _condition_to_column(condition)
    return expr


def compile_rules(rules: Sequence[Rule]) -> dict[str, Column]:
    """Build the four output Columns for a rule set.

    Returned as a plain dict so callers can inspect or reuse individual
    expressions; evaluate() just applies them all with withColumns().
    """
    empty_str_array = F.array().cast("array<string>")

    if not rules:
        # Degenerate but valid: nothing is ever detected. load_rules()
        # rejects an empty file, but compile_rules() may be called directly.
        return {
            "matched_rule_ids": empty_str_array,
            "matched_attack_types": empty_str_array,
            "max_severity": F.lit(None).cast("string"),
            "is_detection": F.lit(False),
        }

    matches = [(rule, _rule_to_column(rule)) for rule in rules]

    # Build an array with the rule_id where matched and null where not, then
    # drop the nulls. array_compact() would be tidier but is Spark 3.4+;
    # filter() works from 3.1 and keeps this portable.
    rule_id_array = F.filter(
        F.array(*[
            F.when(matched, F.lit(rule.rule_id)).otherwise(F.lit(None).cast("string"))
            for rule, matched in matches
        ]),
        lambda x: x.isNotNull(),
    )

    attack_type_array = F.array_distinct(
        F.filter(
            F.array(*[
                F.when(matched, F.lit(rule.attack_type)).otherwise(F.lit(None).cast("string"))
                for rule, matched in matches
            ]),
            lambda x: x.isNotNull(),
        )
    )

    # Highest severity rank among matched rules; 0 when nothing matched.
    max_rank = F.array_max(
        F.array(*[
            F.when(matched, F.lit(rule.severity_rank)).otherwise(F.lit(0))
            for rule, matched in matches
        ])
    )

    # Map rank -> name with a single lookup rather than a chain of when().
    #
    # The obvious `when(rank==4,"critical").otherwise(when(rank==3,...))` form
    # inlines `max_rank` once PER LEVEL - and max_rank contains every rule's
    # full predicate, so four levels means four more copies of all six rules.
    # Catalyst does not CSE these; it walks the duplicated tree, and
    # constraint propagation then compares the copies pairwise. That is what
    # made the streaming query hang in Project.getAllValidConstraints.
    #
    # element_at is 1-based, so index = rank + 1: index 1 holds NULL for
    # rank 0 (nothing matched), then low/medium/high/critical. max_rank now
    # appears exactly once.
    severity_expr = F.element_at(
        F.array(F.lit(None).cast("string"), *[F.lit(s) for s in SEVERITY_ORDER]),
        max_rank + F.lit(1),
    )

    any_match = matches[0][1]
    for _, matched in matches[1:]:
        any_match = any_match | matched

    return {
        "matched_rule_ids": rule_id_array,
        "matched_attack_types": attack_type_array,
        "max_severity": severity_expr,
        "is_detection": any_match,
    }


def evaluate(df: DataFrame, rules: Sequence[Rule]) -> DataFrame:
    """Add the four detection columns to `df`.

    Appends, never replaces: every input column (including `label`, which
    the writer needs for scoring) is preserved untouched.

    Parameters
    ----------
    df :
        Parsed NSL-KDD records - the struct expanded out of from_json in
        streaming_job.py, matching NSL_KDD_SCHEMA.
    rules :
        Validated rules from load_rules().

    Returns
    -------
    DataFrame with `matched_rule_ids` (array<string>),
    `matched_attack_types` (array<string>), `max_severity` (string, null
    when nothing matched) and `is_detection` (boolean) appended.
    """
    missing = ALLOWED_FIELDS - set(df.columns)
    referenced = {c.field for rule in rules for c in rule.conditions}
    absent = referenced & missing
    if absent:
        raise RuleLoadError(
            f"input DataFrame is missing field(s) referenced by rules: "
            f"{sorted(absent)}. Was it parsed with NSL_KDD_SCHEMA?"
        )

    columns = compile_rules(rules)

    # withColumns() (Spark 3.3+) applies all four in one projection.
    if hasattr(df, "withColumns"):
        return df.withColumns(columns)

    for name, expr in columns.items():
        df = df.withColumn(name, expr)
    return df


# --------------------------------------------------------------------------
# Self-test: exercises loading, validation failures, and evaluation against
# a real local Spark session with a hand-built fixture. No Kafka, no
# ClickHouse, no rules.json required.
#
#     python rules_engine.py
# --------------------------------------------------------------------------

if __name__ == "__main__":
    from pyspark.sql import SparkSession

    print("=" * 70)
    print("rules_engine.py self-test")
    print("=" * 70)

    # ---- 1. validation must reject bad rules -----------------------------
    bad_cases = [
        ("unknown field", [{"rule_id": "r", "attack_type": "a", "severity": "high",
                            "conditions": [{"field": "srcbytes", "op": ">", "value": 1}]}]),
        ("reads label", [{"rule_id": "r", "attack_type": "a", "severity": "high",
                          "conditions": [{"field": "label", "op": "==", "value": "ddos"}]}]),
        ("bad operator", [{"rule_id": "r", "attack_type": "a", "severity": "high",
                           "conditions": [{"field": "count", "op": "=>", "value": 1}]}]),
        ("bad severity", [{"rule_id": "r", "attack_type": "a", "severity": "urgent",
                           "conditions": [{"field": "count", "op": ">", "value": 1}]}]),
        ("duplicate rule_id", [
            {"rule_id": "dup", "attack_type": "a", "severity": "high",
             "conditions": [{"field": "count", "op": ">", "value": 1}]},
            {"rule_id": "dup", "attack_type": "b", "severity": "low",
             "conditions": [{"field": "count", "op": ">", "value": 2}]},
        ]),
        ("empty conditions", [{"rule_id": "r", "attack_type": "a", "severity": "high",
                               "conditions": []}]),
        ("string value on numeric field", [
            {"rule_id": "r", "attack_type": "a", "severity": "high",
             "conditions": [{"field": "count", "op": ">", "value": "150"}]}]),
    ]

    print("\n[1] validation rejects malformed rules")
    for name, raw in bad_cases:
        try:
            parse_rules(raw)
        except RuleLoadError as exc:
            print(f"  PASS  {name:<32} -> {str(exc)[:70]}")
        else:
            print(f"  FAIL  {name:<32} -> accepted, but should have been rejected")

    # ---- 2. evaluation against a real Spark session ----------------------
    spark = (
        SparkSession.builder
        .appName("rules_engine_selftest")
        .master("local[1]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    sample_rules = parse_rules([
        {"rule_id": "ddos_flood", "attack_type": "ddos", "severity": "critical",
         "conditions": [
             {"field": "count", "op": ">=", "value": 150},
             {"field": "serror_rate", "op": ">=", "value": 0.7},
             {"field": "dst_bytes", "op": "<=", "value": 20},
         ]},
        {"rule_id": "port_scan_sweep", "attack_type": "port_scan", "severity": "medium",
         "conditions": [
             {"field": "diff_srv_rate", "op": ">=", "value": 0.5},
             {"field": "same_srv_rate", "op": "<=", "value": 0.15},
             {"field": "rerror_rate", "op": ">=", "value": 0.4},
         ]},
        # Second signature for the same attack_type, to prove rules OR
        # together and attack types deduplicate.
        {"rule_id": "ddos_flood_alt", "attack_type": "ddos", "severity": "high",
         "conditions": [
             {"field": "count", "op": ">=", "value": 200},
             {"field": "protocol_type", "op": "in", "value": ["tcp", "udp"]},
         ]},
    ])
    print(f"\n[2] loaded {len(sample_rules)} sample rules")

    def row(**overrides):
        base = {}
        for f in NSL_KDD_SCHEMA.fields:
            t = f.dataType.typeName()
            base[f.name] = 0.0 if t == "double" else (0 if t == "integer" else "none")
        base["label"] = "normal"
        base.update(overrides)
        return base

    fixture = [
        # DDoS-shaped: matches both ddos rules -> critical, deduped attack type
        row(label="ddos", count=300, serror_rate=0.95, dst_bytes=0,
            protocol_type="tcp", src_bytes=40),
        # Port-scan-shaped: matches the medium rule only
        row(label="port_scan", diff_srv_rate=0.8, same_srv_rate=0.05,
            rerror_rate=0.9, count=40, protocol_type="tcp"),
        # Normal: should match nothing
        row(label="normal", count=3, serror_rate=0.0, dst_bytes=2400,
            same_srv_rate=0.95, diff_srv_rate=0.02, protocol_type="tcp"),
        # Null-feature row: must not match, must not crash
        row(label="normal", count=None, serror_rate=None, dst_bytes=None,
            protocol_type=None),
    ]

    df = spark.createDataFrame(fixture, schema=NSL_KDD_SCHEMA)
    result = evaluate(df, sample_rules)

    print("\n[3] evaluate() output")
    result.select(
        "label", "count", "is_detection",
        "matched_rule_ids", "matched_attack_types", "max_severity",
    ).show(truncate=False)

    rows = result.collect()
    checks = [
        ("ddos row flagged", rows[0]["is_detection"] is True),
        ("ddos matched both ddos rules",
         set(rows[0]["matched_rule_ids"]) == {"ddos_flood", "ddos_flood_alt"}),
        ("ddos attack types deduplicated", rows[0]["matched_attack_types"] == ["ddos"]),
        ("ddos severity is the highest matched (critical)",
         rows[0]["max_severity"] == "critical"),
        ("port_scan row flagged", rows[1]["is_detection"] is True),
        ("port_scan matched only its rule",
         rows[1]["matched_rule_ids"] == ["port_scan_sweep"]),
        ("port_scan severity medium", rows[1]["max_severity"] == "medium"),
        ("normal row not flagged", rows[2]["is_detection"] is False),
        ("normal row has no rule ids", rows[2]["matched_rule_ids"] == []),
        ("normal row severity is null", rows[2]["max_severity"] is None),
        ("null-feature row not flagged", rows[3]["is_detection"] is False),
        ("input columns preserved",
         all(f.name in result.columns for f in NSL_KDD_SCHEMA.fields)),
        ("no python UDF in plan",
         "BatchEvalPython" not in result._jdf.queryExecution().executedPlan().toString()),
    ]

    print("[4] assertions")
    failed = 0
    for name, ok in checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}")
        failed += 0 if ok else 1

    print(f"\n{'ALL CHECKS PASSED' if failed == 0 else f'{failed} CHECK(S) FAILED'}")
    spark.stop()
