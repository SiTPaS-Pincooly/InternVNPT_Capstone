"""
validate_rules.py

Offline sanity check for rules/rules.json: generates real synthetic
traffic with producer/generate_traffic.py, runs it through
spark_app/rules_engine.py, and reports per-attack-type recall plus the
overall false-positive rate on normal traffic.

This is NOT the capstone accuracy report (that's evaluate_accuracy.py,
which scores the live ClickHouse-stored stream) - this is a fast,
Kafka-free dev-time check to catch a badly-tuned threshold before it
ever reaches the pipeline. Re-run this after any change to rules.json.

Usage: python validate_rules.py [--n 20000] [--seed 42]
"""

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "producer"))
sys.path.insert(0, str(REPO_ROOT / "spark_app"))

import numpy as np
from pyspark.sql import SparkSession

from generate_traffic import generate_batch
from schema import NSL_KDD_SCHEMA
import rules_engine


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=20000, help="rows to generate")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rules", default=str(REPO_ROOT / "rules" / "rules.json"))
    args = parser.parse_args()

    spark = SparkSession.builder.appName("rules_validation").master("local[1]").getOrCreate()

    rng = np.random.default_rng(seed=args.seed)
    records = generate_batch(rng, batch_size=args.n, normal_ratio=0.95)
    df = spark.createDataFrame(records, schema=NSL_KDD_SCHEMA)

    rules = rules_engine.load_rules(args.rules)
    result = rules_engine.evaluate(df, rules)
    pdf = result.select("label", "is_detection", "matched_attack_types").toPandas()

    print(f"Total rows: {len(pdf)}\n")
    print(f"{'attack_type':<22}{'total':>8}{'caught':>8}{'recall':>10}")
    for label in sorted(pdf["label"].unique()):
        if label == "normal":
            continue
        subset = pdf[pdf["label"] == label]
        caught = subset["is_detection"].sum()
        total = len(subset)
        print(f"{label:<22}{total:>8}{caught:>8}{caught/total:>10.1%}")

    normal = pdf[pdf["label"] == "normal"]
    fp = normal["is_detection"].sum()
    print(f"\nNormal traffic: {len(normal)} rows, {fp} false positives "
          f"({fp/len(normal):.3%} false positive rate)")

    mismatches = sum(
        1 for _, row in pdf[pdf["is_detection"] & (pdf["label"] != "normal")].iterrows()
        if row["label"] not in row["matched_attack_types"]
    )
    print(f"Detections where matched_attack_types didn't include the true label: {mismatches}")

    spark.stop()


if __name__ == "__main__":
    main()