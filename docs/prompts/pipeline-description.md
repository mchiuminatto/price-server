# Prce Pipeline

## Overview

The goal of this pipeline is to pre-process price to check quality, reduce its size by converting it to parquet format and patching existing price dataset.

Can read price from multiple storage types:

- AWS S3
- Local file system (default)

## Architecture considerations.

1. Each step of this process must be decoupled with queues
2. Each step must be a workload the is fired when at least one message is in its input queue.
3. Operation must be vectorized as much as possible.
4. This process will be implemented with python and all its ecosyste.

It has 6 steps:

### Step 1: Work queue serializer

Goal: List all the files to process in the input storage and put them in a queue: "normalizer queue" so all the next step workers can take from that queue and process the files.

### Step 2: Price normalizer

The goal is to reduce the file size by saving it as a parquet file, merging bid and ask in one file (in case of OHLC) and normalizing the columns names.

In the case of tick data:

time, open_bid, high_bid_, low_bid, close_bid, volume_bis, open_ask, high_ask_, low_ask, close_ask, volume_ask

In the case of tick data

time, ask, bid, volume.

One or more workers can process this step taking the input from the "normalizer_queue"

When each worker finishes, will enrich the received payload que the process result and put it in a message in the queue for the next step named: "quality_checker_queue"

### Step 3: Price normalizer

Each worker will take a message from the queue: "quality_checker_queue"

Based on a set of validation rules will apply validations to each time series, basically detecting price gaps. Will enrich the received payload with the quality report and put it in the next processing queue named: "updater_queue"

# Step 4: Price patcher

Each worker for this step will pull a message from the queue: "updater_queue"

Will verify if an existing price exists and will append or insert the new price into the existing one or will create output the new one.

Once finished, will enrich the received payload and put it in the queue for the next step: "file_mover_queue"

# Step 5: File mover

Each file mover worker will pull a message from the queue: "file_mover_queue"

Each message will move the previous step file to the target storage and path within the storage.

# Recommended Stack & Architecture

## Message Queue

### Redis Streams (via `redis-py`)

- Lightweight, fast, supports consumer groups natively — perfect for the queue-per-step pattern described
- Each named queue (`normalizer_queue`, `quality_checker_queue`, etc.) maps to a Redis Stream
- **Alternatives:**
  - **RabbitMQ** — if you need more advanced routing
  - **AWS SQS** — if you want fully managed and are already on AWS

---

## Workers

### Python `asyncio` + `concurrent.futures.ProcessPoolExecutor`

- Each step is an async worker that polls its input queue
- CPU-bound steps (normalization, quality checks) offloaded to a process pool to avoid the GIL
- Workers are stateless and horizontally scalable (k8s HPA already in the repo)

---

## Data Processing (vectorized)

### Pandas (already in the stack) + PyArrow

- **Pandas** for all transformations and validations
- **PyArrow** for reading/writing Parquet files efficiently
- For very large datasets: consider **Polars** as a drop-in replacement (faster, lower memory)

---

## Storage Abstraction

### `fsspec`

- Single unified API over both local filesystem and AWS S3
- `fsspec.open("s3://bucket/path")` vs `fsspec.open("/local/path")` — same interface
- Pairs naturally with PyArrow's Parquet I/O

---

## Pipeline Orchestration (optional)

None initially — the queue-driven design is self-orchestrating. If observability or retries become a need, consider **Prefect** or **Temporal** later.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Storage (fsspec)                         │
│                   Local FS  │  AWS S3                           │
└──────────────┬──────────────┴─────────────────────────────────-─┘
               │ list files
               ▼
┌──────────────────────┐
│  Step 1              │
│  Work Queue          │──► normalizer_queue (Redis Stream)
│  Serializer          │
└──────────────────────┘
               │
               ▼ (N workers)
┌──────────────────────┐
│  Step 2              │◄── normalizer_queue
│  Price Normalizer    │    • CSV → Parquet
│  (pandas + pyarrow)  │    • Merge bid/ask
│                      │──► quality_checker_queue
└──────────────────────┘
               │
               ▼ (N workers)
┌──────────────────────┐
│  Step 3              │◄── quality_checker_queue
│  Quality Checker     │    • Gap detection
│  (pandas)            │    • Enrich payload w/ QC report
│                      │──► updater_queue
└──────────────────────┘
               │
               ▼ (N workers)
┌──────────────────────┐
│  Step 4              │◄── updater_queue
│  Price Patcher       │    • Append / insert / create
│  (pandas + pyarrow)  │──► file_mover_queue
└──────────────────────┘
               │
               ▼ (N workers)
┌──────────────────────┐
│  Step 5              │◄── file_mover_queue
│  File Mover          │    • Move to target storage/path
│  (fsspec)            │──► done_queue (audit log)
└──────────────────────┘
```

---

## Payload Design

Each message passed between queues is a JSON envelope that accumulates results as it flows through the pipeline:

```json
{
  "file_id": "uuid",
  "source_path": "s3://raw/EURUSD_2024.csv",
  "instrument": "EUR/USD",
  "data_type": "tick | ohlc",
  "steps": {
    "normalizer":  { "status": "ok", "output_path": "...", "rows": 50000 },
    "quality":     { "status": "ok", "gaps_found": 3, "report_path": "..." },
    "patcher":     { "status": "ok", "action": "append", "output_path": "..." },
    "file_mover":  { "status": "ok", "target_path": "..." }
  }
}
```

---

## Key Libraries

| Concern             | Library                                |
|---------------------|----------------------------------------|
| Queue               | `redis` (Redis Streams)                |
| Data processing     | `pandas`, `polars` (optional)          |
| Parquet I/O         | `pyarrow`                              |
| Storage abstraction | `fsspec`, `s3fs`                       |
| Async workers       | `asyncio`, `anyio`                     |
| Config management   | `pydantic-settings` (already in stack) |
| Containerization    | Docker + Kubernetes (already in repo)  |
| Linting/formatting  | `ruff` (already configured)            |

