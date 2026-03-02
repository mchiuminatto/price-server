# Prce Pipeline

## Overview

The goal of this pipeline is to pre-process price to check quality, reduce its size by converting it to parquet format, patching existing price dataset and buiding higer price abstractions like:

regular OHLC time series
non-regular OHLC like pip bars (fixed pip range), rick bars (fix number of ticks), renko bars (fixed number of body pips)


Can read price from multiple storage types:

- AWS S3
- Local file system (default)


## Architecture considerations.

1. Each step of this process must be decoupled with queues
2. Each step must be a workload the is fired when at least one message is in its input queue.
3. Operation must be vectorized as much as possible.
4. This process will be implemented with python and all its ecosystem.


## Step architecture

Each step will: 

- Take an event from a queue (with previous even payload)
- Eventually open an input file (generate from previuls step) and specified in the payload
- Take step parameters from a configuration file.
- Execute the transformations
- Save an output file
- Enrich the recived payload
- Send an envent to the next step queue


# Steps

There are 6 steps in the price pipeline:

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

### Step 3: Price quality checker

Each worker will take a message from the queue: "quality_checker_queue"

Based on a set of validation rules will apply validations to each time series, basically detecting price gaps. Will enrich the received payload with the quality report and put it in the next processing queue named: "updater_queue"

### Step 4: Price patcher

Each worker for this step will pull a message from the queue: "updater_queue"

Will verify if an existing price exists and will append the new price into the existing one or will create output the new one. This will be append only and not insert in between.

Once finished, will enrich the received payload and put it in the queue for the next step: "abstraction_distributor_queue"

### Step 5: Abstractinos distributor

The abstraction distributor objective is to take the events generated on the previous step from the queue: "abstraction_distributor_queue", take all the abstractins to be built from the settings and distribute the abstraction generation events in the respective ab    straction queues: "ohlc_queue", "tick_bar", "pipo_bar", "renko_bar".


### Step 6: Abstractions builders.

For this step, each woeker: ohlc, tick-bar, pip bar, renko, will take the event from its repsective queues generate the prices abstraction files store them and add the event, abstraction generated to the queue: "price_abstraction"queue.


# Recommended Stack & Architecture

## Message Queue

### Redis Streams (via `redis-py`)

- One Redis Stream per step — maps exactly to the queue names in the spec
- Consumer groups give at-least-once delivery and allow N workers per step with no extra config
- The fan-out in Step 5 (one event → many abstraction queues) is a natural `XADD` to multiple streams
- **Alternatives:** RabbitMQ (topic exchanges for the fan-out), AWS SQS (fully managed, per-queue cost)

---

## Workers

### Python `asyncio` + `concurrent.futures.ProcessPoolExecutor`

- Each worker is an async loop: poll queue → process → publish → ack
- CPU-bound work (normalization, bar building) is offloaded to a `ProcessPoolExecutor` to bypass the GIL
- Workers are stateless → horizontally scalable via k8s HPA (already in repo)
- Step 5 (distributor) is pure fan-out logic — lightweight, single async worker is enough
- Step 6 spawns **four independent worker types** (OHLC, tick-bar, pip-bar, renko), each with its own stream and consumer group

---

## Data Processing (vectorized)

### Pandas + PyArrow

- **Pandas** for all time series transformations: normalization, gap detection, bar building
- **PyArrow** for Parquet I/O — efficient columnar storage, fast read/write
- All operations are vectorized (no row-by-row loops)
- Upgrade path: swap Pandas for **Polars** if individual files exceed available RAM

---

## Storage Abstraction

### `fsspec`

- Single unified API over local filesystem and AWS S3 via `s3fs`
- Same `storage.open(path)` call whether the backend is local or cloud
- PyArrow reads/writes Parquet directly through `fsspec` file objects

---

## Configuration

### `pydantic-settings` + YAML/TOML per step

- Global settings (Redis URL, storage backend, paths) via environment variables / `.env`
- Per-step parameters (gap thresholds, bar sizes, pip values) loaded from a **YAML config file** specified in the payload — allows different configs per instrument/run without redeploying workers

---

## Architecture Diagram

```
                         ┌──────────────────────────┐
                         │   Storage (fsspec)        │
                         │   Local FS  │  AWS S3     │
                         └──────┬───────────────┬────┘
                                │ list files    │ read/write
                                ▼               │
                   ┌─────────────────────┐      │
                   │ Step 1              │      │
                   │ Work Queue          │──► normalizer_queue
                   │ Serializer          │
                   └─────────────────────┘
                                │
                                ▼ (N workers)
                   ┌─────────────────────┐
                   │ Step 2              │◄── normalizer_queue
                   │ Price Normalizer    │    • CSV → Parquet
                   │ (pandas + pyarrow)  │    • Merge bid/ask cols
                   │                     │    • Rename columns
                   │                     │──► quality_checker_queue
                   └─────────────────────┘
                                │
                                ▼ (N workers)
                   ┌─────────────────────┐
                   │ Step 3              │◄── quality_checker_queue
                   │ Quality Checker     │    • Detect price gaps
                   │ (pandas)            │    • Produce QC report
                   │                     │──► updater_queue
                   └─────────────────────┘
                                │
                                ▼ (N workers)
                   ┌─────────────────────┐
                   │ Step 4              │◄── updater_queue
                   │ Price Patcher       │    • Append-only merge
                   │ (pandas + pyarrow)  │    • Create if not exists
                   │                     │──► abstraction_distributor_queue
                   └─────────────────────┘
                                │
                                ▼ (1 worker)
                   ┌─────────────────────┐
                   │ Step 5              │◄── abstraction_distributor_queue
                   │ Abstraction         │    • Read abstraction list
                   │ Distributor         │      from config
                   │                     │──► ohlc_queue
                   │                     │──► tick_bar_queue
                   │                     │──► pip_bar_queue
                   │                     │──► renko_bar_queue
                   └─────────────────────┘
                         │    │    │    │
             ┌───────────┘    │    │    └──────────────┐
             ▼                ▼    ▼                   ▼
   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
   │ Step 6a      │  │ Step 6b      │  │ Step 6c      │  │ Step 6d      │
   │ OHLC Builder │  │ Tick Bar     │  │ Pip Bar      │  │ Renko Bar    │
   │              │  │ Builder      │  │ Builder      │  │ Builder      │
   └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘
          └──────────────────┴──────────────────┴─────────────────┘
                                       │
                                       ▼
                              price_abstraction_queue
                              (audit / downstream consumers)
```

---

## Payload Design

A single JSON envelope flows through all steps, accumulating results:

```json
{
  "file_id": "uuid",
  "source_path": "s3://raw/EURUSD_2024.csv",
  "instrument": "EUR/USD",
  "data_type": "tick",
  "config_path": "s3://config/pipeline_config.yaml",
  "steps": {
    "normalizer":    { "status": "ok", "output_path": "...", "rows": 50000 },
    "quality":       { "status": "ok", "gaps_found": 3, "report_path": "..." },
    "patcher":       { "status": "ok", "action": "append", "output_path": "..." },
    "distributor":   { "status": "ok", "abstractions": ["ohlc_1m", "pip_bar_10", "renko_5"] },
    "abstractions":  {
      "ohlc_1m":     { "status": "ok", "output_path": "..." },
      "pip_bar_10":  { "status": "ok", "output_path": "..." },
      "renko_5":     { "status": "ok", "output_path": "..." }
    }
  }
}
```

---

## Step 5 — Abstraction Distributor Detail

The distributor reads the list of abstractions to build from `config_path` in the payload.
This allows per-instrument or per-run configuration without redeploying workers:

```yaml
# pipeline_config.yaml
abstractions:
  - type: ohlc
    timeframe: 1m
  - type: ohlc
    timeframe: 5m
  - type: pip_bar
    pip_range: 10
  - type: renko
    body_pips: 5
  - type: tick_bar
    tick_count: 1000
```

Each entry generates one message onto the corresponding queue.

---

## Key Libraries

| Concern                  | Library                                    |
|--------------------------|--------------------------------------------|
| Queue                    | `redis` (Redis Streams)                    |
| Data processing          | `pandas`, `polars` (optional upgrade)      |
| Parquet I/O              | `pyarrow`                                  |
| Storage abstraction      | `fsspec`, `s3fs`                           |
| Async workers            | `asyncio`, `anyio`                         |
| Config management        | `pydantic-settings` + `PyYAML`             |
| Containerization         | Docker + Kubernetes (already in repo)      |
| Linting/formatting       | `ruff` (already configured)                |

