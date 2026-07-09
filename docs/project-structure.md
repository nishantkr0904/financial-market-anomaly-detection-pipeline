# Project Structure

This document describes every top-level directory in the repository, the purpose it serves, and the kinds of modules it will contain once the pipeline is implemented. It is the reference an engineer should read first before writing or moving any file.

The layout is intentionally flat and function-oriented: each directory maps to one clear responsibility in the pipeline. Files are grouped by *what they do* (ingest, transform, detect, visualize) rather than by technical layer (models, controllers, utils), which keeps the mental model close to the data flow.

---

## Top-level layout

```
financial-market-anomaly-detection-pipeline/
├── analytics/
├── bigquery/
├── configs/
├── data/
│   ├── raw/
│   ├── processed/
│   └── sample/
├── docs/
├── logs/
├── notebooks/
├── output/
├── scripts/
├── spark_jobs/
├── tests/
├── visualizations/
├── .env
├── .gitignore
├── CLAUDE.md
├── LICENSE
├── README.md
└── requirements.txt
```

---

## Directory reference

### `analytics/`

**Purpose.** Pure-Python analytics modules that operate on already-cleaned data — the "brains" of the anomaly detection system.

**Rationale.** Statistical logic (rolling z-score, volatility regime detection, return-distribution tests) is easier to develop, unit-test, and iterate on with pandas / NumPy than with a Spark DataFrame API. Keeping analytics decoupled from Spark lets us reuse the same functions inside notebooks, inside Spark UDFs, and inside batch jobs.

**Expected modules.**

| Module | Responsibility |
| --- | --- |
| `returns.py` | Log return, simple return, rolling return computations. |
| `volatility.py` | Realized volatility, rolling standard deviation, EWMA volatility. |
| `zscore.py` | Rolling z-score of returns and volume; parameterized window size. |
| `distribution_tests.py` | Return-distribution diagnostics (skew, kurtosis, Jarque–Bera). |
| `anomaly_rules.py` | Composable rule engine that combines detectors into a labelled anomaly flag. |
| `__init__.py` | Public API surface for the package. |

**What does *not* belong here.** No Spark session code, no BigQuery clients, no file I/O beyond what a helper function needs for testing.

---

### `bigquery/`

**Purpose.** All code that talks to Google BigQuery — the cloud warehouse where processed bars and detected anomalies are persisted for downstream querying and dashboarding.

**Rationale.** Warehouse integration is a well-defined boundary: it has its own credentials, its own error semantics (quotas, streaming limits, schema drift), and its own testing needs (mocked clients, dry-run queries). Isolating it prevents warehouse concerns from leaking into transformation or analytics code.

**Expected modules.**

| Module | Responsibility |
| --- | --- |
| `client.py` | Thin factory that constructs an authenticated `bigquery.Client` from environment variables. |
| `schemas.py` | Declarative schema definitions for every table the pipeline writes. |
| `loader.py` | Batch upload routines (load-job from GCS or from local Parquet). |
| `queries.py` | Parameterized SQL used for post-load validation and analytics reads. |
| `ddl/` | `.sql` files for `CREATE TABLE` / `CREATE VIEW` statements checked into version control. |

**Design decision.** BigQuery writes go through **load jobs** (Parquet → GCS → BigQuery), not streaming inserts. Load jobs are free, idempotent by design when a run-id is embedded in the filename, and fit the daily batch cadence of this pipeline.

---

### `configs/`

**Purpose.** Environment- and job-level configuration held as data, not code.

**Rationale.** Every parameter that could plausibly change between runs — window sizes, threshold multipliers, input paths, output tables — should live in a config file. This keeps job code stable across parameter sweeps and makes reruns reproducible from the config alone.

**Expected files.**

| File | Contents |
| --- | --- |
| `pipeline.yaml` | Default parameters for the end-to-end run: input path, output table, detector thresholds. |
| `spark.yaml` | Spark tunables: executor memory, shuffle partitions, adaptive query execution flags. |
| `logging.yaml` | Log level, format, and sink configuration. |
| `schemas/` | Optional per-source schema overrides. |

**Design decision.** YAML over JSON for human editability and comments; no secrets in configs (secrets live in `.env`).

---

### `data/`

**Purpose.** All on-disk data the pipeline reads or writes locally. Split into three lifecycle stages.

#### `data/raw/`
Immutable source data as delivered. For this project, the canonical file is `1_min_SPY_2008-2021.csv`. Contents of `raw/` are **never** edited in place; if a source is bad, it is re-downloaded, not patched. Gitignored.

#### `data/processed/`
Output of the Spark cleaning and enrichment stages, written as partitioned Parquet (partition key: `date`). This is the layer that anomaly detection reads from. Gitignored.

#### `data/sample/`
Small, versioned samples (typically one trading day) used for fast local iteration, unit tests, and notebook tutorials. Committed to git because they are small and reproducibility-critical.

**Rationale for three tiers.** Separating raw / processed / sample follows the well-established medallion pattern (bronze / silver / gold). It makes retention policies obvious (raw = long-term archive, processed = derived and rebuildable, sample = tiny and permanent) and gives every downstream module an unambiguous read source.

---

### `docs/`

**Purpose.** Human-readable design documentation. This directory. Not gitignored. Contents are the source of truth for architecture, tech choices, and data-flow decisions; when the code diverges from these documents, the documents are updated in the same pull request.

**Files.**

- `architecture.md` — system-level design and component responsibilities.
- `data-flow.md` — end-to-end data lineage and stage semantics.
- `tech-stack.md` — technology choices and rationale.
- `project-structure.md` — this file.

---

### `logs/`

**Purpose.** Runtime log output from Spark jobs, batch scripts, and BigQuery loaders. Gitignored.

**Convention.** One log file per job run, named `{job_name}_{yyyymmdd_hhmmss}.log`. Log format is structured JSON via `python-json-logger` so logs can later be shipped to a log aggregator without reparsing.

**Rationale.** Filesystem logs are the lowest-friction sink for a single-node development environment; structured JSON keeps the door open for production log shipping without a rewrite.

---

### `notebooks/`

**Purpose.** Jupyter notebooks for exploratory data analysis (EDA), prototype detectors, and reproducible investigation writeups. Notebooks are for exploration, not for production execution.

**Convention.**

- Notebooks import from `analytics/`, `bigquery/`, and `visualizations/`; they do not redefine core logic inline. Anything worth keeping graduates into a module.
- Outputs are cleared before commit (`jupyter nbconvert --clear-output`) to keep diffs reviewable.
- One notebook per investigation topic; names are prefixed with a two-digit ordinal (`01_eda.ipynb`, `02_return_distribution.ipynb`).

**Rationale.** Notebooks are excellent for the analyst loop but terrible for automated re-execution. Enforcing a strict "import, don't define" rule prevents a notebook from becoming the accidental source of truth for a detector.

---

### `output/`

**Purpose.** Terminal artifacts of a full pipeline run: exported anomaly reports (CSV / Parquet), rendered charts (PNG / HTML), and summary tables. Gitignored.

**Rationale.** Runtime output is separated from `data/processed/` because it targets humans and downstream consumers, not the next pipeline stage. Different lifecycle, different retention, different consumers.

---

### `scripts/`

**Purpose.** One-off and operational scripts: smoke tests, data downloads, ad-hoc backfills, dev-environment health checks.

**Expected files.**

| Script | Responsibility |
| --- | --- |
| `test_spark.py` | Existing smoke test that verifies a Spark session can be created. |
| `download_data.py` | Fetch the SPY CSV from its canonical source into `data/raw/`. |
| `run_pipeline.py` | Thin CLI entry point that wires configs → Spark jobs → BigQuery loader for an end-to-end run. |

**Rationale.** Anything that would otherwise be pasted into a README as a copy-paste command belongs here. Scripts are the executable form of documentation.

---

### `spark_jobs/`

**Purpose.** PySpark job definitions. Each file corresponds to one distinct stage of the batch pipeline and is independently runnable.

**Expected modules.**

| Module | Responsibility |
| --- | --- |
| `session.py` | Constructs the `SparkSession` with appName `"FinancialPipeline"` and configuration from `configs/spark.yaml`. |
| `ingest.py` | Reads `data/raw/1_min_SPY_2008-2021.csv`, parses the `YYYYMMDD  HH:MM:SS` date format, applies the declared schema. |
| `clean.py` | Deduplicates, drops or flags rows with missing OHLCV, enforces high ≥ low ≥ 0 invariants. |
| `enrich.py` | Adds derived columns: returns, rolling means, session-of-day markers, holiday flags. |
| `detect.py` | Runs anomaly detectors (delegating statistical logic to `analytics/`) over the enriched dataset. |
| `export.py` | Writes partitioned Parquet to `data/processed/` and stages files for BigQuery upload. |

**Design decision.** Stages are **separate jobs**, not stages of one monolithic pipeline, so any one can be rerun in isolation. Intermediate state lives on disk (Parquet), not in memory, which enables cheap reruns from any stage.

---

### `tests/`

**Purpose.** Automated tests — unit tests for `analytics/`, integration tests for Spark jobs against sample data, and contract tests for BigQuery schemas.

**Convention.**

- Mirrors the source layout: `tests/analytics/`, `tests/spark_jobs/`, `tests/bigquery/`.
- Spark integration tests use a local SparkSession and read from `data/sample/`.
- BigQuery tests use dry-run queries; no test writes to a real dataset.

**Rationale.** Analytics functions must be testable without Spark; Spark jobs must be testable without a cluster; BigQuery code must be testable without incurring cost. Splitting the test tree by module lets each tier meet its own bar.

---

### `visualizations/`

**Purpose.** Reusable plotting modules — one function, one chart. Used by both notebooks (interactive) and `scripts/run_pipeline.py` (rendered PNG / HTML output).

**Expected modules.**

| Module | Responsibility |
| --- | --- |
| `price.py` | OHLC and close-price time-series plots. |
| `volume.py` | Volume bar charts with anomaly overlays. |
| `returns.py` | Return distributions, QQ plots. |
| `anomalies.py` | Anomaly markers overlaid on price / return charts. |
| `theme.py` | Shared matplotlib / plotly styling. |

**Rationale.** Charts are a first-class output of this pipeline, not an afterthought. Isolating them makes it possible to regenerate every figure from a config-driven script and keeps notebook code focused on narrative.

---

## Root-level files

| File | Purpose |
| --- | --- |
| `.env` | Local-only environment variables (BigQuery project, credentials path). Gitignored. |
| `.gitignore` | Excludes data, logs, output, virtual environment, and IDE artifacts. |
| `CLAUDE.md` | Guidance for the Claude Code assistant when working in this repository. |
| `LICENSE` | Project license. |
| `README.md` | High-level orientation and quickstart. Points to `docs/` for depth. |
| `requirements.txt` | Pinned Python dependencies for the `.venv` virtual environment. |

---

## What is deliberately *not* in the structure

- **No `src/` wrapper.** The functional directories (`analytics/`, `spark_jobs/`, etc.) are themselves the source tree. A `src/` layer would add navigation cost without benefit at this project size.
- **No `models/` directory.** This pipeline is statistical, not ML-driven. If a trained model artifact ever needs to be persisted, it will live under `output/models/` — not as a top-level concept.
- **No `airflow/` or `dags/` directory.** Orchestration is out of scope for the first cut; `scripts/run_pipeline.py` is the entry point. A future orchestrator (Airflow, Prefect, Dagster) would wrap the same script.
- **No `api/` or `web/` directory.** This is a batch pipeline. Serving is not in scope.

Keeping the structure tight to the agreed scope is intentional: every extra directory is a place engineers have to reason about.

## Repository Growth Roadmap

Phase 1
- Project setup
- EDA

Phase 2
- Spark ingestion

Phase 3
- Data cleaning

Phase 4
- Feature engineering

Phase 5
- Statistical anomaly detection

Phase 6
- BigQuery integration

Phase 7
- Visualization

Phase 8
- Testing

Phase 9
- CI/CD

Phase 10
- Docker