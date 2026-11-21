# Running with Docker

Docker lets you run the full pipeline (YOLO detection + PySpark processing)
without installing Python 3.11, Java, or the ML stack on your machine. Your
`inSitu/` dataset and the fine-tuned model stay on disk and are mounted into the
containers.

## Prerequisites

- Docker Desktop installed and running.
- The GroZi-120 dataset present under `inSitu/` (e.g. `inSitu/1/video/`, ...).
  Without it, the feed simulator has no images to stream.
- The fine-tuned model at `runs/classify/grozi_cls_v8n_aug10/weights/best.pt`
  (included in this repository).

## Build the image

From the repository root:

```bash
docker compose build
```

This builds one image (`grocery-stock-analysis`) with Python 3.11, a Java
runtime, OpenCV libraries, and all Python dependencies. The first build
downloads PyTorch and PySpark, so it takes a few minutes.

## Run the full pipeline

```bash
docker compose up
```

This starts two containers that share the project directory:

- **pipeline** — simulates the camera feeds, runs YOLO classification, and
  writes one JSON detection per frame to `stream_output_json/`.
- **processor** — the PySpark batch processor, which reads those JSON files and
  logs per-batch metrics to `src/spark_metrics_logs_simple/batch_metrics.log`.

The `pipeline` container exits once it has streamed all frames; the `processor`
keeps polling. Press `Ctrl+C` to stop, then clean up with:

```bash
docker compose down
```

## Visualize the metrics

After a run, generate the charts (writes PNGs to `src/spark_metrics_plots/`):

```bash
docker compose run --rm processor \
  python -m src.visualize_metrics \
    --log src/spark_metrics_logs_simple/batch_metrics.log \
    --save src/spark_metrics_plots
```

Because the project directory is bind-mounted, all outputs (JSON detections,
metrics logs, and plots) appear on your local disk.

## Run commands interactively

To open a shell inside the image and run steps manually:

```bash
docker compose run --rm pipeline bash
# then, inside the container:
python -m src.realtime_grozi_pipeline
python -m src.pyspark_simple_processor
```

## Structured Streaming variant

To use the Structured Streaming processor instead of the batch one:

```bash
docker compose run --rm processor python -m src.pyspark_stream_processor
```

## Notes

- **CPU only:** the image installs CPU-only PyTorch. Inference on CPU is fine
  for this dataset but slower than GPU.
- **Retraining** (`prepare_grozi_yolo.py`, `train_yolo_grozi.py`) also works in
  the container but is not needed to run the pipeline — the fine-tuned model is
  already included.
