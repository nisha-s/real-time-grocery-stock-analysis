#!/usr/bin/env python3
"""
benchmark.py - scalability and latency sweep for the streaming pipeline.

Measures the throughput and per-batch latency of the PySpark Structured Streaming
windowed aggregation across three dimensions:

  - batch size      (maxFilesPerTrigger)
  - parallelism     (Spark master local[N]; a local stand-in for executor cores)
  - window duration (sliding-window length)

Each run uses Trigger.AvailableNow, so Spark deterministically drains a fixed
corpus of detection JSON files in batches of the chosen size and then stops. The
numbers reported come from Spark's own query progress metrics (numInputRows and
triggerExecution duration per micro-batch), not from wall-clock guesses.

Every configuration runs in its own process (a fresh JVM) so that the core count
is actually applied. Results go to a CSV and are rendered as charts.

Usage (from the repository root):
  python -m src.benchmark --sweep         # full sweep + charts (what you want)
  python -m src.benchmark --run-one --cores 4 --batch 20 --window "10 seconds" \
      --corpus <dir> --out <csv> --sweep-tag batch     # one config (internal)
"""

import os
import sys
import csv
import json
import time
import argparse
import subprocess
import statistics
from datetime import datetime, timezone, timedelta

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
RESULTS_DIR = os.path.join(SCRIPT_DIR, "benchmark_results")
CORPUS_DIR = os.path.join(RESULTS_DIR, "_corpus")
CKPT_ROOT = os.path.join(RESULTS_DIR, "_ckpt")
RESULTS_CSV = os.path.join(RESULTS_DIR, "benchmark_results.csv")

CORPUS_SIZE = 300  # number of detection records in the fixed corpus

PRODUCTS = [
    (1, "Bausch & Lomb Renu Solution"),
    (14, "CLOROX 2 LIQUID 44 OZ"),
    (25, "Nissin Ramen Noodles - Chicken"),
    (90, "Chef Boyardee Beef Ravioli"),
    (120, "Yoplait Yogurt Strawberry Banana"),
]

CSV_FIELDS = [
    "sweep_tag", "cores", "batch_size", "window",
    "total_rows", "num_batches", "proc_time_s",
    "throughput_rows_s", "avg_latency_ms", "p95_latency_ms", "wall_s",
]


# --------------------------------------------------------------------------- #
# Corpus generation
# --------------------------------------------------------------------------- #
def generate_corpus(corpus_dir, n):
    """Create n detection JSON files with event times spread one second apart,
    so the time windows are actually populated."""
    os.makedirs(corpus_dir, exist_ok=True)
    for f in os.listdir(corpus_dir):
        if f.endswith(".json"):
            os.remove(os.path.join(corpus_dir, f))

    base = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=n)
    for i in range(n):
        cid, name = PRODUCTS[i % len(PRODUCTS)]
        ts = (base + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%S")
        rec = {
            "timestamp": ts,
            "filename": f"bench_{i}.png",
            "class_id": cid,
            "product_name": name,
            "product_upc": "000000000000",
            "confidence": 0.95,
        }
        with open(os.path.join(corpus_dir, f"bench_{i:05d}.json"), "w") as fh:
            json.dump(rec, fh)
    print(f"[corpus] wrote {n} files to {corpus_dir}")


# --------------------------------------------------------------------------- #
# One benchmark run (own process / fresh JVM)
# --------------------------------------------------------------------------- #
def run_one(cores, batch, window_dur, corpus, out_csv, sweep_tag):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, count, avg, window, to_timestamp
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, DoubleType,
    )

    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("filename", StringType(), True),
        StructField("class_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_upc", StringType(), True),
        StructField("confidence", DoubleType(), True),
    ])

    spark = (
        SparkSession.builder
        .appName(f"bench-c{cores}-b{batch}")
        .master(f"local[{cores}]")
        .config("spark.sql.shuffle.partitions", str(max(2, cores * 2)))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.readStream
        .format("json")
        .schema(schema)
        .option("maxFilesPerTrigger", batch)
        .option("multiLine", "true")
        .load(corpus)
    )
    parsed = (
        raw.withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
        .filter((col("confidence") > 0.1) & col("event_time").isNotNull())
    )
    agg = (
        parsed.withWatermark("event_time", "30 seconds")
        .groupBy(window(col("event_time"), window_dur), col("product_name"))
        .agg(count("*").alias("detections"), avg("confidence").alias("avg_conf"))
    )

    ckpt = os.path.join(CKPT_ROOT, f"c{cores}_b{batch}_w{window_dur.replace(' ', '')}")
    # fresh checkpoint each run so maxFilesPerTrigger/window changes take effect
    import shutil
    shutil.rmtree(ckpt, ignore_errors=True)

    t0 = time.time()
    query = (
        agg.writeStream
        .outputMode("update")
        .format("noop")
        .option("checkpointLocation", ckpt)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    wall = time.time() - t0

    # Collect metrics from Spark's own per-batch progress reports
    total_rows = 0
    batch_durations = []
    for p in query.recentProgress:
        rows = p.get("numInputRows", 0) or 0
        if rows > 0:
            total_rows += rows
            dur = p.get("durationMs", {}).get("triggerExecution")
            if dur is not None:
                batch_durations.append(float(dur))

    spark.stop()

    num_batches = len(batch_durations)
    proc_time_s = sum(batch_durations) / 1000.0 if batch_durations else 0.0
    throughput = (total_rows / proc_time_s) if proc_time_s > 0 else 0.0
    avg_latency = statistics.mean(batch_durations) if batch_durations else 0.0
    if batch_durations:
        s = sorted(batch_durations)
        p95 = s[min(len(s) - 1, int(round(0.95 * (len(s) - 1))))]
    else:
        p95 = 0.0

    row = {
        "sweep_tag": sweep_tag,
        "cores": cores,
        "batch_size": batch,
        "window": window_dur,
        "total_rows": total_rows,
        "num_batches": num_batches,
        "proc_time_s": round(proc_time_s, 3),
        "throughput_rows_s": round(throughput, 1),
        "avg_latency_ms": round(avg_latency, 1),
        "p95_latency_ms": round(p95, 1),
        "wall_s": round(wall, 2),
    }

    write_header = not os.path.exists(out_csv)
    with open(out_csv, "a", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        if write_header:
            w.writeheader()
        w.writerow(row)

    print(f"[run] cores={cores} batch={batch} window='{window_dur}' -> "
          f"{total_rows} rows, {num_batches} batches, "
          f"throughput={throughput:.0f} rows/s, avg_latency={avg_latency:.0f} ms")


# --------------------------------------------------------------------------- #
# Full sweep (spawns one subprocess per config)
# --------------------------------------------------------------------------- #
def sweep():
    os.makedirs(RESULTS_DIR, exist_ok=True)
    os.makedirs(CKPT_ROOT, exist_ok=True)
    if os.path.exists(RESULTS_CSV):
        os.remove(RESULTS_CSV)

    generate_corpus(CORPUS_DIR, CORPUS_SIZE)

    # Baseline, then vary one dimension at a time.
    base_cores, base_batch, base_window = 4, 20, "10 seconds"
    configs = []
    for b in [5, 10, 20, 50]:
        configs.append(("batch", base_cores, b, base_window))
    for c in [1, 2, 4]:
        configs.append(("cores", c, base_batch, base_window))
    for w in ["5 seconds", "10 seconds", "30 seconds"]:
        configs.append(("window", base_cores, base_batch, w))

    print(f"\n[sweep] {len(configs)} configurations to run\n")
    for i, (tag, cores, batch, win) in enumerate(configs, 1):
        print(f"[sweep] ({i}/{len(configs)}) tag={tag} cores={cores} batch={batch} window='{win}'")
        cmd = [
            sys.executable, "-m", "src.benchmark", "--run-one",
            "--cores", str(cores), "--batch", str(batch), "--window", win,
            "--corpus", CORPUS_DIR, "--out", RESULTS_CSV, "--sweep-tag", tag,
        ]
        subprocess.run(cmd, cwd=REPO_ROOT, check=True)

    print(f"\n[sweep] done. results -> {RESULTS_CSV}")
    make_plots()


# --------------------------------------------------------------------------- #
# Charts
# --------------------------------------------------------------------------- #
def make_plots():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    rows = []
    with open(RESULTS_CSV) as fh:
        for r in csv.DictReader(fh):
            rows.append(r)

    def subset(tag):
        rs = [r for r in rows if r["sweep_tag"] == tag]
        return rs

    # Batch-size sweep: throughput & latency vs batch size
    b = sorted(subset("batch"), key=lambda r: int(r["batch_size"]))
    if b:
        x = [int(r["batch_size"]) for r in b]
        fig, ax1 = plt.subplots(figsize=(8, 4.5))
        ax1.plot(x, [float(r["throughput_rows_s"]) for r in b], "o-", color="#2563eb", label="Throughput")
        ax1.set_xlabel("Batch size (maxFilesPerTrigger)")
        ax1.set_ylabel("Throughput (rows/s)", color="#2563eb")
        ax2 = ax1.twinx()
        ax2.plot(x, [float(r["avg_latency_ms"]) for r in b], "s--", color="#dc2626", label="Avg latency")
        ax2.set_ylabel("Avg batch latency (ms)", color="#dc2626")
        ax1.set_title("Throughput and latency vs batch size")
        ax1.grid(True, linestyle="--", alpha=0.4)
        fig.tight_layout()
        fig.savefig(os.path.join(RESULTS_DIR, "throughput_latency_vs_batch.png"), dpi=150)
        plt.close(fig)

    # Cores sweep: throughput vs parallelism
    c = sorted(subset("cores"), key=lambda r: int(r["cores"]))
    if c:
        x = [int(r["cores"]) for r in c]
        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.plot(x, [float(r["throughput_rows_s"]) for r in c], "o-", color="#2563eb")
        ax.set_xlabel("Parallelism (local[N] cores)")
        ax.set_ylabel("Throughput (rows/s)")
        ax.set_title("Throughput vs parallelism")
        ax.set_xticks(x)
        ax.grid(True, linestyle="--", alpha=0.4)
        fig.tight_layout()
        fig.savefig(os.path.join(RESULTS_DIR, "throughput_vs_cores.png"), dpi=150)
        plt.close(fig)

    # Window sweep: latency vs window duration
    w = subset("window")
    order = {"5 seconds": 5, "10 seconds": 10, "30 seconds": 30}
    w = sorted(w, key=lambda r: order.get(r["window"], 0))
    if w:
        x = [order.get(r["window"], 0) for r in w]
        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.plot(x, [float(r["avg_latency_ms"]) for r in w], "s-", color="#dc2626")
        ax.set_xlabel("Window duration (seconds)")
        ax.set_ylabel("Avg batch latency (ms)")
        ax.set_title("Latency vs window duration")
        ax.set_xticks(x)
        ax.grid(True, linestyle="--", alpha=0.4)
        fig.tight_layout()
        fig.savefig(os.path.join(RESULTS_DIR, "latency_vs_window.png"), dpi=150)
        plt.close(fig)

    print(f"[plots] charts written to {RESULTS_DIR}")


def main():
    ap = argparse.ArgumentParser(description="Streaming scalability/latency benchmark")
    ap.add_argument("--sweep", action="store_true", help="run the full sweep and make charts")
    ap.add_argument("--plots-only", action="store_true", help="rebuild charts from an existing CSV")
    ap.add_argument("--run-one", action="store_true", help="run a single configuration (internal)")
    ap.add_argument("--cores", type=int, default=4)
    ap.add_argument("--batch", type=int, default=20)
    ap.add_argument("--window", type=str, default="10 seconds")
    ap.add_argument("--corpus", type=str, default=CORPUS_DIR)
    ap.add_argument("--out", type=str, default=RESULTS_CSV)
    ap.add_argument("--sweep-tag", type=str, default="single")
    args = ap.parse_args()

    if args.run_one:
        run_one(args.cores, args.batch, args.window, args.corpus, args.out, args.sweep_tag)
    elif args.plots_only:
        make_plots()
    else:
        sweep()


if __name__ == "__main__":
    main()
