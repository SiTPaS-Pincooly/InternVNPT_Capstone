"""
generate_traffic.py

High-throughput synthetic network traffic generator that emits JSON records
matching the NSL-KDD feature schema (41 features + label) to a Kafka topic.

Design goals
------------
1. Records must look like *plausible* network sessions, not uniform random
   noise. Each traffic class (normal + 6 attack types) has its own feature
   distribution, based on how that traffic actually behaves:

     - normal              : diverse services, low error rates, SF flag
     - ddos                : flood of short/half-open connections, high
                              error rates, near-zero payload
     - port_scan           : many distinct services/ports probed briefly,
                              low same_srv_rate, high diff_srv_rate
     - brute_force          : repeated auth attempts on one service,
                              elevated num_failed_logins
     - malware_c2           : small periodic beacon-like connections,
                              short duration, low bytes, single service
     - data_exfiltration    : large outbound byte volume, longer duration
     - malicious_download    : large inbound byte volume, elevated
                              num_file_creations after transfer

2. Throughput must scale toward ~1,000,000 records/sec. Pure single-process
   Python cannot reach that on its own, so this script scales horizontally:
   feature generation is fully vectorized with numpy (no per-row Python
   loops), and multiple worker *processes* each run their own Kafka
   producer in parallel. See the "Reaching 1M records/sec" note at the
   bottom of this file for how to tune workers/batch size for your machine.

Usage
-----
    python generate_traffic.py --brokers localhost:9092 --topic network-traffic \
        --target-rate 1000000 --workers 8

    # Test the generator without a Kafka broker (measures raw gen throughput):
    python generate_traffic.py --dry-run --target-rate 1000000 --workers 8
"""

import argparse
import json
import multiprocessing as mp
import time
from typing import Callable

import numpy as np

try:
    import orjson

    def dumps(obj) -> bytes:
        return orjson.dumps(obj)
except ImportError:  # orjson is optional but much faster than json
    def dumps(obj) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode("utf-8")

try:
    from confluent_kafka import Producer
    HAVE_KAFKA = True
except ImportError:
    HAVE_KAFKA = False


# --------------------------------------------------------------------------
# NSL-KDD categorical value pools
# --------------------------------------------------------------------------

PROTOCOLS = np.array(["tcp", "udp", "icmp"])
PROTOCOL_WEIGHTS_NORMAL = np.array([0.82, 0.15, 0.03])

SERVICES = np.array([
    "http", "ftp", "ftp_data", "smtp", "ssh", "telnet", "dns", "pop_3",
    "imap4", "https", "ntp_u", "private", "other", "domain_u", "finger",
])
FLAGS = np.array(["SF", "S0", "REJ", "RSTO", "RSTR", "S1", "SH", "OTH"])

# 41 NSL-KDD feature names, in the canonical order, plus the label we add.
FEATURE_NAMES = [
    "duration", "protocol_type", "service", "flag", "src_bytes", "dst_bytes",
    "land", "wrong_fragment", "urgent", "hot", "num_failed_logins",
    "logged_in", "num_compromised", "root_shell", "su_attempted",
    "num_root", "num_file_creations", "num_shells", "num_access_files",
    "num_outbound_cmds", "is_host_login", "is_guest_login", "count",
    "srv_count", "serror_rate", "srv_serror_rate", "rerror_rate",
    "srv_rerror_rate", "same_srv_rate", "diff_srv_rate",
    "srv_diff_host_rate", "dst_host_count", "dst_host_srv_count",
    "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
    "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
    "dst_host_serror_rate", "dst_host_srv_serror_rate",
    "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
]
# Adds "timestamp" (needed for streaming/windowing, not present in the
# static NSL-KDD dataset) and "label" beyond the 41 raw features.


def _rate(rng: np.random.Generator, n: int, low: float, high: float) -> np.ndarray:
    """A [0,1]-bounded rate feature, clipped."""
    return np.clip(rng.uniform(low, high, n), 0.0, 1.0)


# --------------------------------------------------------------------------
# Per-class feature generators
# Each takes (rng, n) and returns a dict[str, np.ndarray] of length n
# covering every column in FEATURE_NAMES.
# --------------------------------------------------------------------------

def gen_normal(rng: np.random.Generator, n: int) -> dict:   
    return {
        "duration": rng.exponential(120, n).round(2),
        "protocol_type": rng.choice(PROTOCOLS, n, p=PROTOCOL_WEIGHTS_NORMAL),
        "service": rng.choice(SERVICES, n),
        "flag": rng.choice(FLAGS, n, p=[0.85, 0.03, 0.02, 0.02, 0.02, 0.02, 0.02, 0.02]),
        "src_bytes": rng.lognormal(6.5, 1.5, n).astype(int),
        "dst_bytes": rng.lognormal(7.0, 1.8, n).astype(int),
        "land": rng.choice([0, 1], n, p=[0.999, 0.001]),
        "wrong_fragment": rng.choice([0, 1, 2], n, p=[0.97, 0.02, 0.01]),
        "urgent": rng.choice([0, 1], n, p=[0.995, 0.005]),
        "hot": rng.poisson(0.2, n),
        "num_failed_logins": rng.choice([0, 1], n, p=[0.97, 0.03]),
        "logged_in": rng.choice([0, 1], n, p=[0.35, 0.65]),
        "num_compromised": np.zeros(n, dtype=int),
        "root_shell": np.zeros(n, dtype=int),
        "su_attempted": np.zeros(n, dtype=int),
        "num_root": np.zeros(n, dtype=int),
        "num_file_creations": rng.poisson(0.1, n),
        "num_shells": np.zeros(n, dtype=int),
        "num_access_files": rng.poisson(0.05, n),
        "num_outbound_cmds": np.zeros(n, dtype=int),
        "is_host_login": np.zeros(n, dtype=int),
        "is_guest_login": rng.choice([0, 1], n, p=[0.98, 0.02]),
        "count": rng.integers(1, 40, n),
        "srv_count": rng.integers(1, 40, n),
        "serror_rate": _rate(rng, n, 0.0, 0.05),
        "srv_serror_rate": _rate(rng, n, 0.0, 0.05),
        "rerror_rate": _rate(rng, n, 0.0, 0.05),
        "srv_rerror_rate": _rate(rng, n, 0.0, 0.05),
        "same_srv_rate": _rate(rng, n, 0.7, 1.0),
        "diff_srv_rate": _rate(rng, n, 0.0, 0.1),
        "srv_diff_host_rate": _rate(rng, n, 0.0, 0.15),
        "dst_host_count": rng.integers(1, 255, n),
        "dst_host_srv_count": rng.integers(1, 255, n),
        "dst_host_same_srv_rate": _rate(rng, n, 0.7, 1.0),
        "dst_host_diff_srv_rate": _rate(rng, n, 0.0, 0.1),
        "dst_host_same_src_port_rate": _rate(rng, n, 0.0, 0.3),
        "dst_host_srv_diff_host_rate": _rate(rng, n, 0.0, 0.1),
        "dst_host_serror_rate": _rate(rng, n, 0.0, 0.05),
        "dst_host_srv_serror_rate": _rate(rng, n, 0.0, 0.05),
        "dst_host_rerror_rate": _rate(rng, n, 0.0, 0.05),
        "dst_host_srv_rerror_rate": _rate(rng, n, 0.0, 0.05),
    }


def gen_ddos(rng: np.random.Generator, n: int) -> dict:
    d = gen_normal(rng, n)
    d.update({
        "duration": np.zeros(n),
        "protocol_type": rng.choice(PROTOCOLS, n, p=[0.6, 0.1, 0.3]),
        "flag": rng.choice(["S0", "REJ", "RSTO"], n, p=[0.7, 0.2, 0.1]),
        "src_bytes": rng.integers(0, 50, n),
        "dst_bytes": np.zeros(n, dtype=int),
        "wrong_fragment": rng.choice([0, 1, 3], n, p=[0.8, 0.15, 0.05]),
        "count": rng.integers(200, 511, n),
        "srv_count": rng.integers(200, 511, n),
        "serror_rate": _rate(rng, n, 0.85, 1.0),
        "srv_serror_rate": _rate(rng, n, 0.85, 1.0),
        "same_srv_rate": _rate(rng, n, 0.9, 1.0),
        "dst_host_count": rng.integers(200, 255, n),
        "dst_host_srv_count": rng.integers(200, 255, n),
        "dst_host_serror_rate": _rate(rng, n, 0.85, 1.0),
        "dst_host_srv_serror_rate": _rate(rng, n, 0.85, 1.0),
    })
    return d


def gen_port_scan(rng: np.random.Generator, n: int) -> dict:
    d = gen_normal(rng, n)
    d.update({
        "duration": rng.exponential(0.5, n).round(3),
        "flag": rng.choice(["S0", "REJ"], n, p=[0.6, 0.4]),
        "src_bytes": rng.integers(0, 20, n),
        "dst_bytes": np.zeros(n, dtype=int),
        "count": rng.integers(50, 200, n),
        "srv_count": rng.integers(1, 10, n),
        "same_srv_rate": _rate(rng, n, 0.0, 0.1),
        "diff_srv_rate": _rate(rng, n, 0.6, 1.0),
        "srv_diff_host_rate": _rate(rng, n, 0.5, 1.0),
        "rerror_rate": _rate(rng, n, 0.5, 0.9),
        "srv_rerror_rate": _rate(rng, n, 0.5, 0.9),
        "dst_host_diff_srv_rate": _rate(rng, n, 0.5, 1.0),
        "dst_host_same_srv_rate": _rate(rng, n, 0.0, 0.1),
    })
    return d


def gen_brute_force(rng: np.random.Generator, n: int) -> dict:
    d = gen_normal(rng, n)
    d.update({
        "service": rng.choice(["ssh", "telnet", "ftp"], n),
        "duration": rng.exponential(3, n).round(2),
        "flag": rng.choice(["SF", "REJ"], n, p=[0.5, 0.5]),
        "src_bytes": rng.integers(10, 300, n),
        "dst_bytes": rng.integers(0, 100, n),
        "num_failed_logins": rng.integers(2, 8, n),
        "logged_in": rng.choice([0, 1], n, p=[0.85, 0.15]),
        "hot": rng.poisson(1.5, n),
        "count": rng.integers(20, 120, n),
        "srv_count": rng.integers(20, 120, n),
        "same_srv_rate": _rate(rng, n, 0.85, 1.0),
        "dst_host_same_src_port_rate": _rate(rng, n, 0.5, 1.0),
    })
    return d


def gen_malware_c2(rng: np.random.Generator, n: int) -> dict:
    d = gen_normal(rng, n)
    d.update({
        "service": rng.choice(["http", "https", "private", "other"], n),
        "duration": rng.exponential(2, n).round(2),
        "flag": np.full(n, "SF"),
        "src_bytes": rng.integers(40, 200, n),
        "dst_bytes": rng.integers(40, 200, n),
        "count": rng.integers(1, 5, n),
        "srv_count": rng.integers(1, 5, n),
        "same_srv_rate": _rate(rng, n, 0.9, 1.0),
        "dst_host_count": rng.integers(1, 10, n),
        "dst_host_srv_count": rng.integers(1, 10, n),
        "dst_host_same_src_port_rate": _rate(rng, n, 0.7, 1.0),
    })
    return d


def gen_data_exfiltration(rng: np.random.Generator, n: int) -> dict:
    d = gen_normal(rng, n)
    d.update({
        "service": rng.choice(["ftp_data", "http", "https"], n),
        "duration": rng.exponential(300, n).round(2),
        "flag": np.full(n, "SF"),
        "src_bytes": rng.lognormal(13, 1.2, n).astype(int),   # large outbound
        "dst_bytes": rng.integers(0, 500, n),
        "logged_in": np.ones(n, dtype=int),
        "num_compromised": rng.poisson(0.5, n),
        "num_outbound_cmds": rng.poisson(0.3, n),
        "num_file_creations": rng.poisson(1.0, n),
        "count": rng.integers(1, 15, n),
        "srv_count": rng.integers(1, 15, n),
    })
    return d


def gen_malicious_download(rng: np.random.Generator, n: int) -> dict:
    d = gen_normal(rng, n)
    d.update({
        "service": rng.choice(["http", "ftp", "ftp_data"], n),
        "duration": rng.exponential(60, n).round(2),
        "flag": np.full(n, "SF"),
        "src_bytes": rng.integers(0, 500, n),
        "dst_bytes": rng.lognormal(13, 1.2, n).astype(int),   # large inbound
        "num_file_creations": rng.poisson(2.0, n),
        "num_access_files": rng.poisson(1.0, n),
        "hot": rng.poisson(1.0, n),
        "count": rng.integers(1, 20, n),
        "srv_count": rng.integers(1, 20, n),
    })
    return d


# Class name -> (generator fn, mixture weight within the attack share)
ATTACK_PROFILES: dict[str, Callable[[np.random.Generator, int], dict]] = {
    "ddos": gen_ddos,
    "port_scan": gen_port_scan,
    "brute_force": gen_brute_force,
    "malware_c2": gen_malware_c2,
    "data_exfiltration": gen_data_exfiltration,
    "malicious_download": gen_malicious_download,
}


def generate_batch(rng: np.random.Generator, batch_size: int, normal_ratio: float) -> list[dict]:
    """Vectorized generation of one batch, mixed across classes, then
    converted to a list of per-record dicts ready for JSON serialization."""
    n_normal = int(round(batch_size * normal_ratio))
    n_attack_total = batch_size - n_normal
    attack_names = list(ATTACK_PROFILES.keys())
    # split remaining rows evenly across the 6 attack types
    counts = np.full(len(attack_names), n_attack_total // len(attack_names))
    counts[: n_attack_total % len(attack_names)] += 1

    columns: dict[str, list[np.ndarray]] = {name: [] for name in FEATURE_NAMES}
    labels: list[np.ndarray] = []

    if n_normal > 0:
        d = gen_normal(rng, n_normal)
        for k in FEATURE_NAMES:
            columns[k].append(d[k])
        labels.append(np.full(n_normal, "normal"))

    for name, cnt in zip(attack_names, counts):
        if cnt <= 0:
            continue
        d = ATTACK_PROFILES[name](rng, int(cnt))
        for k in FEATURE_NAMES:
            columns[k].append(d[k])
        labels.append(np.full(int(cnt), name))

    merged = {k: np.concatenate(v) for k, v in columns.items()}
    label_arr = np.concatenate(labels)

    # shuffle so classes aren't grouped in a suspiciously contiguous block
    order = rng.permutation(batch_size)
    for k in merged:
        merged[k] = merged[k][order]
    label_arr = label_arr[order]

    now = time.time()
    records = []
    for i in range(batch_size):
        rec = {k: (merged[k][i].item() if hasattr(merged[k][i], "item") else merged[k][i]) for k in FEATURE_NAMES}
        rec["timestamp"] = now
        rec["label"] = str(label_arr[i])
        records.append(rec)
    return records


# --------------------------------------------------------------------------
# Worker process: generate + produce, paced toward this worker's share of
# the target rate.
# --------------------------------------------------------------------------

def worker(worker_id: int, args: argparse.Namespace) -> None:
    rng = np.random.default_rng(seed=worker_id + int(time.time()))
    per_worker_target = args.target_rate / args.workers

    producer = None
    if not args.dry_run:
        if not HAVE_KAFKA:
            raise RuntimeError(
                "confluent-kafka is not installed. Install it with "
                "`pip install confluent-kafka --break-system-packages`, "
                "or pass --dry-run to benchmark generation only."
            )
        producer = Producer({
            "bootstrap.servers": args.brokers,
            "acks": "1",
            "compression.type": "lz4",
            "linger.ms": 5,
            "batch.num.messages": 10000,
            "queue.buffering.max.messages": 500000,
            "queue.buffering.max.kbytes": 1048576,
        })

        # Fail fast if the broker is unreachable. Without this the producer
        # happily buffers every message locally and reports success, because
        # produce() is asynchronous - the first sign of trouble would be an
        # empty Kafka topic and a Spark job that never sees a record.
        try:
            producer.list_topics(timeout=10)
        except Exception as exc:
            raise RuntimeError(
                f"Cannot reach Kafka at {args.brokers}: {exc}\n"
                "Is the stack up?   docker compose ps"
            ) from exc

    sent = 0
    window_start = time.time()
    window_count = 0
    start = window_start
    deadline = start + args.duration_sec if args.duration_sec > 0 else None

    # produce() is fire-and-forget. Without a delivery callback a broker-side
    # rejection is completely silent, and the worker reports every record as
    # "sent" when none of them landed. These counters make generated vs
    # actually-acknowledged visible.
    stats = {"delivered": 0, "failed": 0}

    def on_delivery(err, _msg):
        if err is None:
            stats["delivered"] += 1
        else:
            stats["failed"] += 1
            if stats["failed"] <= 5:      # don't flood on a total outage
                print(f"[worker {worker_id}] DELIVERY FAILED: {err}")

    try:
        while deadline is None or time.time() < deadline:
            batch_start = time.time()
            records = generate_batch(rng, args.batch_size, args.normal_ratio)

            if producer is not None:
                for rec in records:
                    producer.produce(args.topic, value=dumps(rec),
                                     callback=on_delivery)
                producer.poll(0)
            sent += len(records)
            window_count += len(records)

            # pace this worker toward its share of the target rate
            elapsed = time.time() - batch_start
            target_elapsed = args.batch_size / per_worker_target
            if elapsed < target_elapsed:
                time.sleep(target_elapsed - elapsed)

            now = time.time()
            if now - window_start >= 5:
                rate = window_count / (now - window_start)
                print(f"[worker {worker_id}] {rate:,.0f} records/sec "
                      f"(generated: {sent:,}  delivered: {stats['delivered']:,})")
                window_start = now
                window_count = 0

    except KeyboardInterrupt:
        print(f"[worker {worker_id}] interrupted - flushing buffered messages")

    finally:
        # MUST run even on Ctrl+C. linger.ms=5 and batch.num.messages=10000
        # mean up to ~10k records per worker are sitting in the client buffer
        # at any moment; exiting without flushing silently discards them.
        if producer is not None:
            remaining = producer.flush(30)
            if remaining:
                print(f"[worker {worker_id}] WARNING: {remaining:,} message(s) "
                      "still unsent after a 30s flush - they were dropped.")

            print(f"[worker {worker_id}] done. generated: {sent:,}  "
                  f"delivered: {stats['delivered']:,}  failed: {stats['failed']:,}")
            if stats["delivered"] < sent:
                print(f"[worker {worker_id}] WARNING: "
                      f"{sent - stats['delivered']:,} record(s) never reached "
                      "Kafka. Spark will not see them.")
        else:
            print(f"[worker {worker_id}] done. generated: {sent:,} (dry run)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--brokers", default="localhost:9092")
    parser.add_argument("--topic", default="network-traffic")
    parser.add_argument("--target-rate", type=float, default=15_000, # Set to ~1GB/minutes
                         help="total records/sec across all workers")
    parser.add_argument("--workers", type=int, default=mp.cpu_count())
    parser.add_argument("--batch-size", type=int, default=5000)
    parser.add_argument("--normal-ratio", type=float, default=0.95)
    parser.add_argument("--duration-sec", type=float, default=0,
                         help="0 = run until interrupted")
    parser.add_argument("--dry-run", action="store_true",
                         help="skip Kafka, just measure generation throughput")
    args = parser.parse_args()

    print(f"Starting {args.workers} worker process(es), "
          f"target total rate {args.target_rate:,.0f} records/sec, "
          f"dry_run={args.dry_run}")

    procs = [mp.Process(target=worker, args=(i, args)) for i in range(args.workers)]
    for p in procs:
        p.start()
    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        for p in procs:
            p.terminate()


if __name__ == "__main__":
    main()