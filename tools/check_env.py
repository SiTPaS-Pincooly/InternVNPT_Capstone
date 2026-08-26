"""
check_env.py

One-shot environment check for the Streaming IDS. Verifies every host-side
prerequisite before you spend time debugging a pipeline problem that is
really a setup problem.

    python tools/check_env.py
    python tools/check_env.py --spark    # also runs a real Spark job (slower)

Stdlib only, on purpose: it has to be runnable BEFORE `pip install -r
requirements.txt`, to tell you whether the interpreter you are about to
install into is the right one.

Why each of these matters is printed inline when a check fails - the point
is to be self-explanatory at 2am, not to send you back to the docs.
"""

from __future__ import annotations

import argparse
import os
import platform
import re
import socket
import subprocess
import sys
from pathlib import Path

IS_WINDOWS = platform.system() == "Windows"

PASS, FAIL, WARN = "PASS", "FAIL", "WARN"
_results: list[tuple[str, str, str]] = []


def record(status: str, name: str, detail: str = "") -> None:
    _results.append((status, name, detail))


def _run(cmd: list[str]) -> tuple[int, str]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return p.returncode, (p.stdout or "") + (p.stderr or "")
    except FileNotFoundError:
        return 127, ""
    except subprocess.TimeoutExpired:
        return 124, ""


# ---------------------------------------------------------------------------
# 1. Python
# ---------------------------------------------------------------------------

def check_python() -> None:
    major, minor = sys.version_info[:2]
    where = sys.executable
    in_venv = sys.prefix != getattr(sys, "base_prefix", sys.prefix)

    if (major, minor) == (3, 11):
        record(PASS, f"Python {major}.{minor}", where)
    elif (major, minor) >= (3, 12) and IS_WINDOWS:
        record(FAIL, f"Python {major}.{minor} on Windows",
               "PySpark crashes inside createDataFrame() on Windows with "
               "Python 3.12+ in local mode. Python 3.13 additionally removed "
               "socketserver.UnixStreamServer, which PySpark calls.\n"
               "        Fix:  py -3.11 -m venv .venv  &&  .venv\\Scripts\\activate")
    elif (major, minor) < (3, 9):
        record(FAIL, f"Python {major}.{minor}", "Too old for this project - use 3.11.")
    else:
        record(WARN, f"Python {major}.{minor}",
               "Project is verified on 3.11; other versions may work off Windows.")

    record(PASS if in_venv else WARN, "Virtual environment",
           where if in_venv else
           "Not running inside a venv. Not fatal, but a venv is how you keep "
           "the 3.11 interpreter pinned for this project.")


# ---------------------------------------------------------------------------
# 2. Java
# ---------------------------------------------------------------------------

def check_java() -> None:
    rc, out = _run(["java", "-version"])
    if rc == 127:
        record(FAIL, "Java on PATH",
               "`java` not found. Spark needs a JDK 8, 11 or 17.")
        return

    m = re.search(r'version "(\d+)(?:\.(\d+))?', out)
    if not m:
        record(WARN, "Java version", f"Could not parse:\n        {out.splitlines()[0] if out else ''}")
        return

    major = int(m.group(1))
    if major == 1:                      # "1.8.0_xxx" style
        major = int(m.group(2) or 8)

    if major in (8, 11, 17):
        record(PASS, f"Java {major}", out.splitlines()[0].strip())
    elif major >= 21:
        record(FAIL, f"Java {major}",
               "Spark 3.5 supports Java 8, 11 and 17 only. Java 21+ produces "
               "module-access errors that look nothing like a version problem.\n"
               "        Fix: install a 17 JDK and point JAVA_HOME at it.")
    else:
        record(WARN, f"Java {major}", "Unexpected version; 8, 11 or 17 expected.")

    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        record(WARN, "JAVA_HOME", "Not set. Usually fine if java is on PATH, "
                                  "but Spark launchers sometimes need it.")
    elif not Path(java_home).is_dir():
        record(FAIL, "JAVA_HOME", f"Set to {java_home!r}, which does not exist.")
    elif not (Path(java_home) / "bin").is_dir():
        record(FAIL, "JAVA_HOME",
               f"{java_home!r} has no bin/ subdirectory. It must point at the "
               "JDK ROOT, not at its bin folder.")
    else:
        detail = java_home
        if " " in java_home:
            detail += "   (contains spaces - a known source of Windows launcher bugs)"
        record(PASS, "JAVA_HOME", detail)


# ---------------------------------------------------------------------------
# 3. PySpark, and the two versions derived from it
# ---------------------------------------------------------------------------

def check_pyspark() -> str | None:
    try:
        import pyspark  # noqa
    except ImportError:
        record(FAIL, "pyspark installed", "pip install -r requirements.txt")
        return None

    version = pyspark.__version__
    record(PASS, f"pyspark {version}", Path(pyspark.__file__).parent.as_posix())

    jars = Path(pyspark.__file__).parent / "jars"
    hadoop_version = None
    if jars.is_dir():
        for j in jars.iterdir():
            m = re.match(r"hadoop-client-api-([\d.]+)\.jar$", j.name)
            if m:
                hadoop_version = m.group(1)
                break

    if hadoop_version:
        record(PASS, "Bundled Hadoop version", f"{hadoop_version}   "
               f"(winutils.exe must be this line, i.e. {hadoop_version.rsplit('.', 1)[0]}.x)")
    else:
        record(WARN, "Bundled Hadoop version", "Could not determine from jars/.")

    # Kafka connector is NOT bundled - derive the coordinate the job needs.
    has_kafka_jar = any("kafka" in j.name for j in jars.iterdir()) if jars.is_dir() else False
    scala = "2.13" if version.startswith("4.") else "2.12"
    coord = f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{version}"
    record(PASS if has_kafka_jar else WARN, "Kafka connector",
           f"Not bundled (expected). Launch with:\n"
           f"        spark-submit --packages {coord} spark_app/streaming_job.py"
           if not has_kafka_jar else "bundled")

    return hadoop_version


# ---------------------------------------------------------------------------
# 4. Hadoop Windows binaries
# ---------------------------------------------------------------------------

def check_hadoop(expected_version: str | None) -> None:
    if not IS_WINDOWS:
        record(PASS, "winutils.exe", "Not required off Windows.")
        return

    hadoop_home = os.environ.get("HADOOP_HOME")
    if not hadoop_home:
        record(FAIL, "HADOOP_HOME",
               "Not set. Spark's filesystem layer needs winutils.exe on "
               "Windows; Structured Streaming checkpointing fails without it.")
        return

    home = Path(hadoop_home)
    if not home.is_dir():
        record(FAIL, "HADOOP_HOME", f"Set to {hadoop_home!r}, which does not exist.")
        return

    bin_dir = home / "bin"
    if not bin_dir.is_dir():
        record(FAIL, "HADOOP_HOME layout",
               f"{hadoop_home!r} has no bin/ subdirectory.\n"
               "        HADOOP_HOME must be the folder CONTAINING bin, not bin itself.\n"
               "        Correct:  C:\\hadoop-3.3.6        (with C:\\hadoop-3.3.6\\bin\\winutils.exe)")
        return

    record(PASS, "HADOOP_HOME", hadoop_home)

    for fname, why in (
        ("winutils.exe", "file permission checks"),
        ("hadoop.dll", "native IO; missing it causes UnsatisfiedLinkError at runtime"),
    ):
        f = bin_dir / fname
        record(PASS if f.is_file() else FAIL, f"  {fname}",
               str(f) if f.is_file() else f"Missing from {bin_dir} - needed for {why}.")

    if expected_version:
        want_line = expected_version.rsplit(".", 1)[0]      # e.g. "3.3"
        found = re.search(r"(\d+\.\d+)\.\d+", home.name)
        if found and found.group(1) != want_line:
            record(FAIL, "  Hadoop version match",
                   f"HADOOP_HOME looks like Hadoop {found.group(0)}, but this "
                   f"PySpark bundles Hadoop {expected_version}. Use a {want_line}.x build.")
        elif found:
            record(PASS, "  Hadoop version match", f"{found.group(0)} matches {want_line}.x")

    path_entries = [p.strip().rstrip("\\/").lower()
                    for p in os.environ.get("PATH", "").split(os.pathsep)]
    record(PASS if str(bin_dir).rstrip("\\/").lower() in path_entries else WARN,
           "  %HADOOP_HOME%\\bin on PATH",
           "yes" if str(bin_dir).rstrip("\\/").lower() in path_entries
           else "Not on PATH. Usually still works, but add it to be safe.")


# ---------------------------------------------------------------------------
# 5. Project Python packages
# ---------------------------------------------------------------------------

def check_packages() -> None:
    for mod, why in (
        ("numpy", "generate_traffic.py"),
        ("pandas", "clickhouse_writer.py toPandas()"),
        ("pyarrow", "Spark <-> pandas conversion"),
        ("clickhouse_connect", "clickhouse_writer.py, evaluate_accuracy.py"),
        ("confluent_kafka", "generate_traffic.py producer"),
        ("orjson", "generate_traffic.py serialization (optional, falls back to json)"),
    ):
        try:
            __import__(mod)
            record(PASS, f"  {mod}", "")
        except ImportError:
            optional = "optional" in why
            record(WARN if optional else FAIL, f"  {mod}",
                   f"Missing - needed by {why}")


# ---------------------------------------------------------------------------
# 6. Docker and ports
# ---------------------------------------------------------------------------

def check_docker() -> None:
    rc, out = _run(["docker", "info"])
    if rc == 127:
        record(FAIL, "Docker CLI", "`docker` not found on PATH.")
        return
    if rc != 0:
        record(FAIL, "Docker daemon",
               "CLI found but the daemon is not responding. Start Docker Desktop.")
        return
    record(PASS, "Docker daemon", "running")

    rc, out = _run(["docker", "compose", "ps", "--format", "{{.Name}}\t{{.State}}"])
    if rc == 0 and out.strip():
        for line in out.strip().splitlines():
            if "\t" in line:
                name, state = line.split("\t", 1)
                record(PASS if "running" in state.lower() else WARN,
                       f"  {name}", state)
    else:
        record(WARN, "  compose services",
               "None running yet - `docker compose up -d` when you get to Stage 1.")


def check_ports() -> None:
    for port, who in ((9092, "Kafka"), (8123, "ClickHouse HTTP"),
                      (9000, "ClickHouse native"), (8088, "Superset")):
        s = socket.socket()
        s.settimeout(0.6)
        listening = s.connect_ex(("127.0.0.1", port)) == 0
        s.close()
        record(PASS, f"  {port} ({who})",
               "in use - expected once the stack is up; a conflict if it isn't"
               if listening else "free")


# ---------------------------------------------------------------------------
# 7. Optional: prove Spark actually works
# ---------------------------------------------------------------------------

def check_spark_runs() -> None:
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        record(FAIL, "Spark smoke test", "pyspark not installed")
        return

    try:
        spark = (SparkSession.builder
                 .appName("env_check").master("local[1]")
                 .config("spark.python.worker.faulthandler.enabled", "true")
                 .config("spark.ui.enabled", "false")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        # createDataFrame from Python objects is exactly what breaks on
        # Windows + Python 3.12+, so test that specific path.
        n = spark.createDataFrame([(1, "a"), (2, "b")], "x int, y string").count()
        spark.stop()
        record(PASS if n == 2 else FAIL, "Spark smoke test",
               f"createDataFrame + count returned {n}")
    except Exception as exc:
        first = str(exc).strip().splitlines()[0] if str(exc).strip() else repr(exc)
        record(FAIL, "Spark smoke test", f"{first}\n"
               "        If this says 'Python worker exited unexpectedly', it is "
               "the Python 3.12+ / Windows bug - use Python 3.11.")


# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Check Streaming IDS prerequisites.")
    ap.add_argument("--spark", action="store_true",
                    help="also start a real Spark session (slower, but conclusive)")
    args = ap.parse_args()

    print("=" * 74)
    print("STREAMING IDS - ENVIRONMENT CHECK".center(74))
    print("=" * 74)
    print(f"{platform.system()} {platform.release()}   {platform.machine()}\n")

    print("-- interpreter " + "-" * 59)
    check_python()
    print("-- java " + "-" * 66)
    check_java()
    print("-- spark " + "-" * 65)
    hadoop_version = check_pyspark()
    check_hadoop(hadoop_version)
    print("-- project packages " + "-" * 54)
    check_packages()
    print("-- docker " + "-" * 64)
    check_docker()
    print("-- ports " + "-" * 65)
    check_ports()
    if args.spark:
        print("-- spark smoke test " + "-" * 54)
        check_spark_runs()

    print()
    for status, name, detail in _results:
        mark = {PASS: "  ok ", FAIL: " FAIL", WARN: " warn"}[status]
        print(f"[{mark}] {name}")
        if detail:
            for line in detail.splitlines():
                print(f"        {line}")

    fails = sum(1 for s, _, _ in _results if s == FAIL)
    warns = sum(1 for s, _, _ in _results if s == WARN)
    print("\n" + "=" * 74)
    if fails:
        print(f"{fails} blocking problem(s), {warns} warning(s). "
              "Fix the FAILs before running the pipeline.")
    else:
        print(f"No blocking problems. {warns} warning(s).")
    print("=" * 74)
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
