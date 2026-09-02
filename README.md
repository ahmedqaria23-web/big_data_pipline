# 🚀 Big Data Midterm Pipeline — Hybrid ELT Data Engineering System

A **production-grade**, academic-quality Data Engineering pipeline that processes dirty e-commerce order datasets through a fully automated ELT (Extract → Load → Transform) architecture. Built for university evaluation, this system demonstrates mastery of distributed computing, data quality engineering, MongoDB integration, and stream-based processing.

---

## 1. Project Overview

This pipeline processes real-world dirty CSV/JSONL order datasets, **automatically routing** processing between:

- **Python Streaming Batch Loader** — for files ≤ 50 MB (`FILE_SIZE_THRESHOLD_MB = 50.0`)
- **PySpark Distributed Engine** — for files > 50 MB

The system strictly follows **ELT**: every raw record is stored in MongoDB `orders_raw` **before** any cleaning or filtering — preserving pristine historical lineage. After ingestion, **14 deterministic data quality rules** are applied with full audit trails. Records are classified as `VALID`, `CORRECTED`, or `QUARANTINED` with idempotent upserts into separate MongoDB collections.

---

## 2. System Requirements

| Component | Version / Requirement |
| :--- | :--- |
| **Python** | 3.10+ |
| **Java JDK** | 17+ (required for PySpark only) |
| **PySpark** | 3.5.0+ |
| **MongoDB** | 6.0+ (running on `localhost:27017` by default) |
| **OS** | Windows 10/11, Linux, macOS |
| **RAM** | 4 GB minimum; 8 GB recommended for PySpark |

---

## 3. Architecture

```
Dirty CSV / JSONL
        │
        ▼
File Discovery
(path validation, file size, extension check)
        │
        ▼
Generate id_run
(unique: run_YYYYMMDD_HHMMSS_<hex6>)
        │
        ▼
File Router
(compare file_size vs FILE_SIZE_THRESHOLD_MB: 50.0 MB)
        │
   ┌────┴────┐
   │         │
   ▼         ▼
Python     PySpark
Batch      Distributed
(≤50MB)    (>50MB)
   │         │
   └────┬────┘
        │
        ▼
  orders_raw (MongoDB)
  [id_run + number_row_source + record_raw]
  _id = id_run:number_row_source (Historical Trace)
        │
        ▼
Quality / Transform
(14 Cleaning Rules + Audit Trail Logging)
        │
   ┌────┴────┐
   │         │
   ▼         ▼
VALID /   QUARANTINED
CORRECTED
   │         │
   ▼         ▼
orders_   quarantine_
validated  orders
(Unique    (error_codes
 id_order  + record_raw)
 Upsert)
        │
        ▼
Idempotency Verification
(raw_count = valid + corrected + quarantine)
        │
        ▼
Metrics Engine
        │
        ▼
reports/results.json
```

**Single entry point**: `run_pipeline.py` (CLI) or Streamlit dashboard (`app.py`).

---

## 4. Project Structure

```
half_project/
├── app.py                          # Streamlit multi-page GUI dashboard
├── run_pipeline.py                 # CLI entry point (run / status / metrics)
├── create_small_sample.py          # Streaming sample generator (no Pandas)
├── benchmark.py                    # Performance benchmark runner
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment variables template
├── .gitignore                      # Git exclusion rules
├── LICENSE                         # MIT License
│
├── config/
│   ├── settings.py                 # Global configuration (reads from .env)
│   └── requirements_matrix.py      # Requirements compliance matrix engine
│
├── src/
│   ├── routing/
│   │   └── file_router.py          # File size-based engine router
│   ├── ingestion/
│   │   ├── batch_loader.py         # csv.DictReader streaming + checkpoint
│   │   └── spark_loader.py         # PySpark fixed StructType schema loader
│   ├── quality/
│   │   ├── quality_rules.py        # 14 cleaning rules with audit trail
│   │   ├── validator.py            # Business validation rules (9 error codes)
│   │   ├── classifier.py           # VALID / CORRECTED / QUARANTINED logic
│   │   └── quarantine_manager.py   # Quarantine inspection utilities
│   ├── mongodb/
│   │   ├── mongo_setup.py          # DB init: schema + unique indexes
│   │   └── repositories.py         # Idempotent upsert + bulk operations
│   ├── pipeline/
│   │   ├── elt_pipeline.py         # Main ELT orchestration controller
│   │   └── pipeline_controller.py  # CLI command dispatcher
│   ├── incremental/
│   │   └── incremental_loader.py   # Path B: Incremental / watermark loading
│   └── monitoring/
│       └── metrics.py              # Consistency equation + run telemetry
│
├── tests/                          # Pytest test suite (28 tests)
│   ├── test_router.py
│   ├── test_cleaning_rules.py
│   ├── test_classifier.py
│   ├── test_validator.py
│   ├── test_idempotency.py
│   ├── test_performance_and_direct_upsert.py
│   └── test_spark_loader_idempotency.py
│
├── docs/                           # Project documentation
│   ├── architecture.md
│   ├── requirements_compliance.md  # PDF requirements matrix
│   ├── requirements_traceability.md
│   ├── data_quality_rules.md
│   ├── idempotency.md
│   ├── incremental_loading.md
│   ├── performance.md
│   └── final_review.md
│
├── reports/
│   ├── results.json                # All pipeline run metrics (real data)
│   └── performance_comparison.md   # Performance benchmark report
│
└── schemas/
    └── orders_schema.json          # MongoDB $jsonSchema validator
```

---

## 5. Installation

### Step 1 — Clone Repository
```bash
git clone https://github.com/<your-username>/big-data-midterm-pipeline.git
cd big-data-midterm-pipeline
```

### Step 2 — Create Virtual Environment
```bash
python -m venv .venv
```

### Step 3 — Activate Virtual Environment
```bash
# Windows
.venv\Scripts\activate

# Linux / macOS
source .venv/bin/activate
```

### Step 4 — Install Dependencies
```bash
pip install -r requirements.txt
```

---

## 6. MongoDB Setup

1. Start MongoDB server (default: `localhost:27017`):
   ```bash
   mongod --dbpath ./data/db
   ```
2. The pipeline **auto-initializes** all collections and indexes on first run. No manual setup required.

Collections created automatically:
| Collection | Purpose |
| :--- | :--- |
| `orders_raw` | All raw records (`_id = id_run:number_row_source`, historical trace) |
| `orders_validated` | Cleaned/valid records (`ux_id_order` unique index, idempotent upsert) |
| `quarantine_orders` | Uncorrectable records (`codes_error`, `details_error`, `record_raw`) |
| `meta_state` | Watermark tracking + checkpoint state |

---

## 7. Configuration

Copy the environment template:
```bash
cp .env.example .env
```

Settings in `config/settings.py` (overridable via `.env`):
```env
MONGO_URI=mongodb://localhost:27017
DB_NAME=ecommerce_bigdata_db
FILE_SIZE_THRESHOLD_MB=50.0
BATCH_SIZE_DEFAULT=5000
SPARK_BATCH_SIZE=10000
```

---

## 8. Create Small Sample Dataset

Generate a reproducible sample (streaming, no Pandas, no full-file RAM load):
```bash
python create_small_sample.py --input data/orders_huge_mixed_quality.csv --rows 5000
```

---

## 9. Run Pipeline (CLI)

### Automatic Routing:
```bash
python run_pipeline.py --file data/sample_5000_orders.csv
```

### Force Engine Selection:
```bash
# Force Python Streaming Batch
python run_pipeline.py --file data/sample_5000_orders.csv --engine python_batch

# Force PySpark Distributed Loader
python run_pipeline.py --file data/sample_5000_orders.csv --engine pyspark
```

---

## 10. Streamlit Dashboard

```bash
streamlit run app.py
```

Opens at `http://localhost:8501` with real-time monitoring and inspection pages:
- 🏠 Dashboard & Run Status
- 📦 Raw Data Lineage
- ✨ Validated Orders
- 🛡️ Quarantine Manager
- 🔍 Data Quality & Audit Trail
- 📊 Performance & Telemetry
- 🔁 Idempotency & Incremental Engine

---

## 11. Run Tests

```bash
python -m pytest -q
```

Output:
```
............................                                             [100%]
28 passed in 29.39s (100% SUCCESS)
```

Run with verbose output:
```bash
python -m pytest -v
```

---

## 12. Verified Idempotency (3-Stage Proof)

- **Run 1 (Fresh Data)**: `inserted = N, updated = 0, unchanged = 0`
- **Run 2 (Identical Re-run)**: `inserted = 0, updated = 0, unchanged = N` (Zero duplicate growth)
- **Run 3 (Modified Record)**: `inserted = 0, updated = 1, unchanged = N-1` (Only modified record updated)

---

## 13. Incremental Loading (Path B)

Path B implements Watermark-based Change Data Capture (CDC):
- Tracks `last_watermark` in `meta_state` based on `updated_at`.
- Resolves conflicting updates using `version` comparison ("Latest-Wins").
- Performs atomic upsert on delta records only.
