"""
kafka_probe.py

Isolates WHERE a "Spark sees no records" problem actually lives.

Three things have been confirmed separately in this project, and none of
them prove the case that matters:

  * the topic has data          (checked with docker exec, i.e. from INSIDE
                                 the container - says nothing about the host)
  * a console consumer works    (also inside the container)
  * the Python producer works   (host -> broker, but PRODUCING, not consuming)

Nothing has yet tested a HOST-SIDE CONSUMER, which is exactly what Spark is.
This does that, and prints the broker metadata the host actually receives -
including the advertised host:port the client is told to reconnect to, which
is the usual culprit when producing works and consuming does not.

    python tools/kafka_probe.py
    python tools/kafka_probe.py --brokers 127.0.0.1:9092 --seconds 15

Reads its defaults from spark_app/config.py, so it probes exactly what
streaming_job.py would.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "spark_app"))

try:
    import config
    _DEFAULT_BROKERS = config.KAFKA_BOOTSTRAP_SERVERS
    _DEFAULT_TOPIC = config.KAFKA_TOPIC
except Exception:                                   # config imports pyspark
    _DEFAULT_BROKERS, _DEFAULT_TOPIC = "localhost:9092", "network-traffic"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--brokers", default=_DEFAULT_BROKERS)
    ap.add_argument("--topic", default=_DEFAULT_TOPIC)
    ap.add_argument("--seconds", type=float, default=10.0,
                    help="how long to poll for messages")
    args = ap.parse_args()

    try:
        from confluent_kafka import Consumer, KafkaException, TopicPartition
        from confluent_kafka.admin import AdminClient
    except ImportError:
        print("confluent-kafka is not installed in this interpreter.")
        return 2

    print("=" * 70)
    print("KAFKA PROBE - host-side consumer".center(70))
    print("=" * 70)
    print(f"bootstrap : {args.brokers}")
    print(f"topic     : {args.topic}\n")

    # ---- 1. metadata -----------------------------------------------------
    # This is the step Spark hangs on when it hangs. If it times out here,
    # the problem is reaching the broker at all - not offsets, not Spark.
    print("-- 1. cluster metadata " + "-" * 46)
    try:
        t0 = time.time()
        md = AdminClient({"bootstrap.servers": args.brokers}).list_topics(timeout=10)
        print(f"  ok   metadata received in {time.time() - t0:.2f}s")
    except Exception as exc:
        print(f"  FAIL {exc}")
        print("\n  The host cannot reach the broker at all.")
        print("  If the Python PRODUCER works against this same address, the")
        print("  difference is almost always name resolution (localhost ->")
        print("  ::1 on Windows) or a firewall rule scoped to one executable.")
        return 3

    print("\n  brokers as advertised TO THIS HOST:")
    for b in md.brokers.values():
        print(f"    id={b.id}  {b.host}:{b.port}")
    print("  ^ a client connects to bootstrap once, then reconnects to the")
    print("    address above. If that host is unreachable from here (e.g. an")
    print("    internal Docker name, or 'localhost' resolving to IPv6), every")
    print("    consumer hangs while the producer may still appear to work.")

    if args.topic not in md.topics:
        print(f"\n  FAIL topic {args.topic!r} does not exist.")
        print(f"       topics present: {sorted(md.topics)[:10]}")
        return 4

    t = md.topics[args.topic]
    if t.error is not None:
        print(f"\n  FAIL topic error: {t.error}")
        return 4
    print(f"\n  topic {args.topic!r}: {len(t.partitions)} partition(s)")
    for p in t.partitions.values():
        print(f"    partition {p.id}  leader={p.leader}  replicas={p.replicas}")

    # ---- 2. watermarks ---------------------------------------------------
    print("\n-- 2. offsets visible from this host " + "-" * 32)
    consumer = Consumer({
        "bootstrap.servers": args.brokers,
        "group.id": f"kafka-probe-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    total = 0
    try:
        for p in t.partitions.values():
            tp = TopicPartition(args.topic, p.id)
            try:
                lo, hi = consumer.get_watermark_offsets(tp, timeout=10)
                print(f"    partition {p.id}: low={lo:,}  high={hi:,}  "
                      f"available={hi - lo:,}")
                total += hi - lo
            except KafkaException as exc:
                print(f"    partition {p.id}: FAIL {exc}")
                return 5

        if total == 0:
            print("\n  Topic is empty from this host's point of view.")
            return 6

        # ---- 3. actually consume ----------------------------------------
        print(f"\n-- 3. consuming from the beginning for {args.seconds:g}s "
              + "-" * 20)
        consumer.assign([TopicPartition(args.topic, p.id, 0)
                         for p in t.partitions.values()])

        got, errors, first = 0, 0, None
        deadline = time.time() + args.seconds
        while time.time() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            if msg.error():
                errors += 1
                if errors <= 3:
                    print(f"    error: {msg.error()}")
                continue
            got += 1
            if first is None:
                first = msg.value()

        print(f"\n    messages consumed : {got:,}")
        print(f"    errors            : {errors}")
        if first:
            preview = first.decode("utf-8", "replace")[:110]
            print(f"    first payload     : {preview}...")

        print()
        if got:
            print("  RESULT: this host CAN consume. Kafka and the network are")
            print("          fine, and the problem is inside Spark - its")
            print("          checkpoint, its offsets, or its foreachBatch.")
            return 0

        print("  RESULT: metadata works but NO messages arrive at this host,")
        print("          even though the offsets above say data exists.")
        print("          That points at the advertised address in step 1")
        print("          being unreachable from here.")
        return 7

    finally:
        consumer.close()


if __name__ == "__main__":
    sys.exit(main())
