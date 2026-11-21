#!/usr/bin/env python3
"""
pyspark_simple_processor.py

A simplified, cross-platform PySpark *batch* processor for the GroZi-120
detection pipeline.

Unlike `pyspark_stream_processor.py` (which uses Spark Structured Streaming and
requires Hadoop/winutils on Windows), this version polls the JSON output
directory on an interval and processes any *new* detection files as a batch
using plain `spark.read`. It avoids streaming checkpoints entirely, so it runs
reliably on Windows, macOS, and Linux with no extra setup beyond a Java runtime.

It writes metrics in the exact same log format as the streaming processor:

    TS | Batch N | Detections: X | AvgConf: f | AvgLatencyMs: f | UniqueProducts: Y

so the resulting log can be visualized with `visualize_metrics.py` unchanged.

Usage:
    # Terminal 1: generate detections
    python -m src.realtime_grozi_pipeline

    # Terminal 2: process them
    python -m src.pyspark_simple_processor

Press Ctrl+C to stop.
"""

import os
import sys
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# Resolve paths against the project root (one level up from this src/ directory)
# so the processor works regardless of the current working directory.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)

JSON_INPUT_DIR = os.path.join(REPO_ROOT, "stream_output_json")
METRICS_LOG_DIR = os.path.join(SCRIPT_DIR, "spark_metrics_logs_simple")
POLL_INTERVAL = 2.0          # seconds between polls for new files
LATENCY_ALERT_MS = 2000      # warn if average end-to-end latency exceeds this

detection_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("filename", StringType(), True),
    StructField("class_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("product_upc", StringType(), True),
    StructField("confidence", DoubleType(), True),
])


def initialize_spark():
    """Initialize a local Spark session (streaming/Hadoop-free)."""
    print("[Spark] Initializing Spark session (batch mode)...")
    spark = (
        SparkSession.builder
        .appName("GroZi-120 Simple Batch Processor")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"[Spark] Ready (version {spark.version})")
    return spark


def list_json_files():
    """Return the set of JSON file paths currently in the input directory."""
    if not os.path.isdir(JSON_INPUT_DIR):
        return set()
    return {
        os.path.join(JSON_INPUT_DIR, f)
        for f in os.listdir(JSON_INPUT_DIR)
        if f.lower().endswith(".json")
    }


def process_batch(spark, file_paths, batch_id, log_file):
    """Read the given JSON files as one batch, compute and log metrics."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    df = (
        spark.read
        .schema(detection_schema)
        .option("multiLine", "true")
        .json(list(file_paths))
    )

    # Filter out low-confidence detections, mirroring the streaming processor.
    df = df.filter(col("confidence") > 0.1)

    total_detections = df.count()
    if total_detections == 0:
        print(f"[Batch {batch_id}] No valid detections at {timestamp}")
        return

    avg_confidence = df.agg(avg("confidence")).collect()[0][0] or 0.0
    unique_products = df.select("product_name").distinct().count()

    # End-to-end latency: wall-clock gap between the detection timestamp embedded
    # in each JSON record and the time this batch is processed. Guarded against
    # negative values. Computed in Python to keep this processor dependency-light.
    now_epoch = time.time()
    latencies_ms = []
    for row in df.select("timestamp").where(col("timestamp").isNotNull()).collect():
        try:
            event_epoch = datetime.strptime(row["timestamp"], "%Y-%m-%dT%H:%M:%S").timestamp()
            latencies_ms.append(max(0.0, (now_epoch - event_epoch) * 1000.0))
        except (ValueError, TypeError):
            continue
    avg_latency_ms = sum(latencies_ms) / len(latencies_ms) if latencies_ms else None

    print(f"\n{'=' * 70}")
    print(f"[Batch {batch_id}] Processed at {timestamp}")
    print(f"  Total Detections: {total_detections}")
    print(f"  Average Confidence: {avg_confidence:.4f}")
    print(f"  Avg E2E Latency: {avg_latency_ms:.1f} ms" if avg_latency_ms is not None else "  Avg E2E Latency: N/A")
    print(f"  Unique Products: {unique_products}")
    print(f"{'=' * 70}\n")

    if avg_latency_ms is not None and avg_latency_ms > LATENCY_ALERT_MS:
        print(f"[WARN] Avg E2E Latency {avg_latency_ms:.1f} ms exceeds {LATENCY_ALERT_MS} ms.")

    # Log line in the exact format expected by visualize_metrics.py
    with open(log_file, "a") as f:
        f.write(
            f"{timestamp} | Batch {batch_id} | "
            f"Detections: {total_detections} | "
            f"AvgConf: {avg_confidence:.4f} | "
            f"AvgLatencyMs: {avg_latency_ms if avg_latency_ms is not None else 'NA'} | "
            f"UniqueProducts: {unique_products}\n"
        )

    # Top products in this batch
    product_counts = (
        df.groupBy("product_name", "class_id")
        .agg(count("*").alias("count"), avg("confidence").alias("avg_conf"))
        .orderBy(col("count").desc())
    )
    print(f"[Batch {batch_id}] Top products in this batch:")
    product_counts.show(5, truncate=False)


def main():
    print("\n" + "=" * 70)
    print("PySpark Simple Batch Processor - GroZi Detection")
    print("=" * 70 + "\n")

    os.makedirs(JSON_INPUT_DIR, exist_ok=True)
    os.makedirs(METRICS_LOG_DIR, exist_ok=True)
    log_file = os.path.join(METRICS_LOG_DIR, "batch_metrics.log")
    with open(log_file, "w") as f:
        f.write(f"Stream started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 70 + "\n")

    spark = initialize_spark()
    print(f"[Setup] Watching for JSON files in: {JSON_INPUT_DIR}")
    print(f"[Setup] Metrics log: {log_file}")
    print("Press Ctrl+C to stop.\n")

    seen = set()
    batch_id = 0
    try:
        while True:
            current = list_json_files()
            new_files = current - seen
            if new_files:
                process_batch(spark, new_files, batch_id, log_file)
                seen |= new_files
                batch_id += 1
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        print("\n[Main] Shutting down gracefully...")
    finally:
        spark.stop()
        print("[Main] Spark session closed")
        print(f"[Main] Metrics saved to: {log_file}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[Fatal Error] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
