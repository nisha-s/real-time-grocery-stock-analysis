# PySpark Processing for the GroZi-120 Detection Pipeline

This module implements the **PySpark ingestion / processing** stage of the
pipeline. It continuously reads the JSON detection outputs produced by the YOLO
inference pipeline and processes them in batches, computing basic metrics and
end-to-end latency.

## Overview

- **Real-time data ingestion** from JSON detection files
- **Micro-batch / batch processing**
- **Basic metrics** (detection counts, average confidence, unique products)
- **End-to-end latency** per batch (detection timestamp to processing time)
- **Checkpointing / fault tolerance** (streaming processor resumes after a crash)
- **Performance monitoring** through a metrics log and visualizations

## Files

1. **`pyspark_stream_processor.py`** - Full PySpark **Structured Streaming** application.
   Uses streaming checkpoints; on Windows this requires a Hadoop `winutils.exe` setup.
2. **`pyspark_simple_processor.py`** - Simplified **batch** processor.
   Polls the output directory and processes new files with `spark.read` (no
   streaming checkpoints), so it runs with no extra setup on Windows, macOS and Linux.
3. **`visualize_metrics.py`** - Renders charts from the metrics log.
4. **Output directories** (all git-ignored, created at run time):
   - `spark_checkpoint/` - streaming checkpoint data (stream processor)
   - `spark_metrics_logs/` - metrics log from the stream processor
   - `spark_metrics_logs_simple/` - metrics log from the simple processor
   - `spark_metrics_plots/` - generated visualization PNGs

**Recommended for a quick start / for Windows:** use `pyspark_simple_processor.py`
to avoid Hadoop/`winutils` configuration entirely.

## Prerequisites

- A Java runtime (JDK 8, 11, or 17) on your `PATH` (required by PySpark).
- Project dependencies installed: `pip install -r requirements.txt`.

All commands below are run **from the repository root**.

## Usage

### Quick start (recommended, all platforms)

```bash
# Terminal 1: run the detection pipeline (writes JSON to stream_output_json/)
python -m src.realtime_grozi_pipeline

# Terminal 2: process the detections (batch mode, no Hadoop needed)
python -m src.pyspark_simple_processor
```

### Structured Streaming variant

While the detection pipeline is running, in a second terminal:

```bash
python -m src.pyspark_stream_processor
```

This will:
- Initialize a Spark session
- Read JSON files from `stream_output_json/` as a stream
- Process detections in micro-batches (default trigger: 2 seconds)
- Log metrics to the console and to `src/spark_metrics_logs/batch_metrics.log`

### Visualize the metrics

After collecting some data:

```bash
# For the streaming processor
python -m src.visualize_metrics --log src/spark_metrics_logs/batch_metrics.log --save src/spark_metrics_plots

# For the simple processor
python -m src.visualize_metrics --log src/spark_metrics_logs_simple/batch_metrics.log --save src/spark_metrics_plots
```

This generates PNGs for detections per batch, average confidence, average
end-to-end latency (with an optional threshold line and p95 annotation), and
unique products per batch.

## Configuration

### Stream processor (`pyspark_stream_processor.py`)

```python
WINDOW_DURATION = "10 seconds"    # window size for aggregations
SLIDE_DURATION  = "5 seconds"     # sliding interval
TRIGGER_INTERVAL = "2 seconds"    # micro-batch trigger interval
```

- **TRIGGER_INTERVAL** - how often Spark checks for new data and processes a micro-batch.
- **maxFilesPerTrigger** - maximum JSON files processed per batch (default: 10).

### Simple processor (`pyspark_simple_processor.py`)

```python
POLL_INTERVAL = 2.0        # seconds between polls for new files
LATENCY_ALERT_MS = 2000    # warn if average latency exceeds this
```

## Metrics log format

Both processors write the same line format, so either log can be fed to
`visualize_metrics.py`:

```
2025-11-10 21:41:33 | Batch 0 | Detections: 10 | AvgConf: 0.9123 | AvgLatencyMs: 842.0 | UniqueProducts: 4
```

> **Note on latency:** latency is the wall-clock gap between the timestamp
> written into each detection record and the time the batch is processed. It is
> only meaningful when the processor runs **alongside** a live detection
> pipeline. If you point a processor at JSON files that were generated hours or
> days earlier, the reported latency will reflect that age, not pipeline speed.

## Stateful windowed aggregation

By default the streaming processor logs per-micro-batch metrics. Passing
`--windowed` instead runs a stateful sliding-window aggregation: detections are
grouped into event-time windows (`WINDOW_DURATION`, sliding every
`SLIDE_DURATION`) per product and counted, with a watermark so Spark can bound
and clean up the streaming state.

```bash
python -m src.pyspark_stream_processor --windowed
```

Output is a running table of `window | product_name | detections | avg_confidence`,
updated each trigger as new detections arrive.

## Benchmark (scalability and latency)

`benchmark.py` measures throughput and per-batch latency of the windowed
aggregation across batch size, parallelism (`local[N]` cores), and window
duration. It drains a fixed corpus with `Trigger.AvailableNow` and reads Spark's
own per-batch progress metrics, running each configuration in its own process so
the core count is applied cleanly.

```bash
python -m src.benchmark --sweep
```

Results are written to `src/benchmark_results/benchmark_results.csv` and rendered
as charts in the same folder. In testing, batch size was the dominant factor:
larger batches raised throughput but increased latency, while extra cores and
longer windows had little effect at this (small) data volume, where the job is
overhead-bound rather than compute-bound.

## Fault tolerance via checkpointing (streaming processor only)

> Checkpointing is a feature of **`pyspark_stream_processor.py`** only. The
> simple batch processor tracks seen files in memory and does **not** survive a
> restart.

As the streaming processor commits each micro-batch, Spark records its progress
in `src/spark_checkpoint/` (`offsets/`, `commits/`, and `sources/0/` - which
lists exactly which JSON files have already been consumed). If the processor is
killed and restarted against the same checkpoint directory, it **resumes from
where it stopped**: it does not reprocess old files and does not lose new ones.

### How to test it

With Docker (the checkpoint lives on the bind-mounted disk, so it survives the
container being destroyed):

```bash
# 1. Start the detection pipeline (or generate some JSON files) so data exists.
#    Then start the streaming processor:
docker compose run --rm --no-deps processor python -m src.pyspark_stream_processor

# 2. Let it process a few batches (watch for "[Batch 0]", "[Batch 1]" ...).
# 3. Kill it:  Ctrl+C  (or, from another terminal:  docker stop <container>).
#    This simulates a crash.
# 4. Add more detections (run the pipeline again, or drop new JSON files in
#    stream_output_json/).
# 5. Restart the SAME command:
docker compose run --rm --no-deps processor python -m src.pyspark_stream_processor
```

### What proves it worked

- The restarted run continues the **batch numbering** (e.g. it starts at
  `[Batch 2]`, not back at `[Batch 0]`).
- The restarted run processes **only the newly added detections**, not the ones
  already handled before the crash.

This was verified end-to-end: run 1 processed 12 files (Batch 0 = 10, Batch 1 =
2); after a simulated crash and 6 new files, run 2 resumed at **Batch 2** and
processed exactly **6** detections - the 12 earlier files were not reprocessed.

### Reset the checkpoint

To start completely fresh (reprocess everything):

```bash
rm -rf src/spark_checkpoint/
```

## Troubleshooting

### "Java not found" / Spark fails to start

Install a JDK (8, 11, or 17) and ensure `java -version` works:

```bash
# Debian/Ubuntu
sudo apt-get install openjdk-17-jdk
# macOS (Homebrew)
brew install openjdk@17
```

### "No JSON files found"

Ensure `python -m src.realtime_grozi_pipeline` is running and writing to
`stream_output_json/`.

### Windows: Hadoop `NativeIO` / `winutils.exe` error

The simplest fix is to use the batch processor:

```bash
python -m src.pyspark_simple_processor
```

If you want the full Structured Streaming processor on Windows, install
`winutils.exe`:

1. Download `winutils.exe` (and `hadoop.dll`) matching your Hadoop version from
   https://github.com/cdarlint/winutils
2. Place them in `C:\hadoop\bin\`
3. Set the environment variable `HADOOP_HOME=C:\hadoop`
4. Restart your terminal/IDE

## Possible future enhancements

- Windowed / time-based aggregations (hourly / daily summaries)
- Kafka integration for production streaming
- Alert triggers for anomalies (e.g. sustained high latency, empty shelves)
- Integration with a streaming dashboard (e.g. Grafana)

## References

- [PySpark Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Triggers / micro-batch processing](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#triggers)
- [Checkpointing & recovery](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#recovering-from-failures-with-checkpointing)
