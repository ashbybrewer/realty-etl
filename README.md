# RealtyETL — Property Profitability Intelligence Pipeline

> **Production-grade ETL + BI Dashboard** | Python 3.11 · DuckDB · Prefect · Streamlit

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://python.org)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10-yellow.svg)](https://duckdb.org)
[![Prefect](https://img.shields.io/badge/Prefect-2.x-purple.svg)](https://prefect.io)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.30-red.svg)](https://streamlit.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Business Problem Statement & Impact

Real estate investors and acquisition teams routinely face a data quality catastrophe: MLS aggregator APIs return **deeply nested, volatile JSON** with inconsistent date formats, mixed numeric/string fields, missing financials, and duplicate records across pagination calls. Analysts waste 60–80% of their time cleaning spreadsheets rather than making acquisition decisions.

**RealtyETL solves this in three ways:**

1. **Automated ingestion** — Fetches listings across multiple markets with retry logic, rate limiting, and raw payload archiving. No manual downloads.

2. **Standardised profitability scoring** — Every listing is automatically scored on Cap Rate, NOI, GRM, and Cash-on-Cash return using configurable financial assumptions. A traffic-light deal flag (GREEN / YELLOW / RED) surfaces actionable opportunities instantly.

3. **Analytical dashboard** — A Streamlit BI interface allows non-technical stakeholders (property managers, fund analysts, acquisitions staff) to filter, compare, and export the data without touching SQL.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL DATA SOURCE                            │
│           MLS / Property Listings API  (REST, paginated JSON)           │
│           Volatile schema · Mixed types · Missing fields                │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │  HTTP GET /listings
                                │  (retry + rate limit)
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         EXTRACT LAYER  (extractor.py)                   │
│                                                                         │
│  ┌─────────────────┐   ┌──────────────────┐   ┌──────────────────────┐ │
│  │  Rate Limiter   │   │  Retry w/Backoff │   │  Pydantic Validator  │ │
│  │ (token bucket)  │──▶│  (exp. backoff)  │──▶│  RawPropertyListing  │ │
│  └─────────────────┘   └──────────────────┘   └──────────┬───────────┘ │
│                                                           │             │
│                         ┌─────────────────────────────────▼───────┐    │
│                         │   Raw Landing Zone  (data/raw/*.ndjson)  │    │
│                         │   Timestamped NDJSON per market per run  │    │
│                         └─────────────────────────────────────────┘    │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │  list[RawPropertyListing]
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       TRANSFORM LAYER  (transformer.py)                 │
│                                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌────────────┐  │
│  │  Dedup       │  │  Date Parse  │  │  Type Norm.  │  │  KPI Calc  │  │
│  │ (listing_id  │─▶│  (8 formats) │─▶│  (prop_type  │─▶│  Cap Rate  │  │
│  │  + updated)  │  │  → ISO UTC   │  │   status)    │  │  NOI / GRM │  │
│  └──────────────┘  └──────────────┘  └──────────────┘  └─────┬──────┘  │
│                                                               │         │
│                                              ┌────────────────▼──────┐  │
│                                              │   Deal Flag Engine    │  │
│                                              │  GREEN / YELLOW / RED │  │
│                                              │  UNSCORED             │  │
│                                              └───────────────────────┘  │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │  list[CleanPropertyListing]
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         LOAD LAYER  (loader.py)                         │
│                                                                         │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                     DuckDB  (data/processed/realty.duckdb)        │  │
│  │                                                                   │  │
│  │   TABLE: listings            ── INSERT OR REPLACE (upsert)        │  │
│  │   TABLE: pipeline_run_log    ── append-only audit log             │  │
│  │                                                                   │  │
│  │   VIEW:  vw_market_summary        VIEW: vw_top_deals              │  │
│  │   VIEW:  vw_deal_funnel           VIEW: vw_price_distribution     │  │
│  │   VIEW:  vw_ingestion_trend                                       │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │  SQL queries via DuckDB connection
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    BI DASHBOARD  (app.py — Streamlit)                   │
│                                                                         │
│  KPI Cards · Deal Alert Feed · Market Scatter · Deal Funnel Bar Chart  │
│  Price Distribution · NOI Histogram · Top Deals Table · CSV Export     │
│  Pipeline Run Log · On-demand ETL Trigger                              │
└─────────────────────────────────────────────────────────────────────────┘
                                ▲
                    Streamlit session (port 8501)
                    Browser — no infrastructure required

────────────────────────────────────────────────────────────────────────────

ORCHESTRATION  (orchestrator.py — Prefect 2.x)

    @flow: run_realty_etl_pipeline
        │
        ├─ @task: extract_task    (calls extractor.extract)
        ├─ @task: transform_task  (calls transformer.transform)
        └─ @task: load_task       (calls loader.load)

    Prefect handles: task state · retry policies · run history · scheduling
    Runs standalone (python -m pipeline.orchestrator) or via Prefect server.
```

---

## Repository Structure

```
realty_etl/
│
├── pipeline/
│   ├── __init__.py          # Public API exports
│   ├── config.py            # Settings, logging, Pydantic schemas, DDL
│   ├── extractor.py         # Extract: fetch, retry, rate-limit, validate
│   ├── transformer.py       # Transform: clean, deduplicate, score
│   ├── loader.py            # Load: DuckDB upsert, views, run log
│   └── orchestrator.py      # Prefect flow tying E→T→L together
│
├── data/
│   ├── raw/                 # NDJSON raw dumps (auto-created, gitignored)
│   └── processed/
│       └── realty.duckdb    # Analytical database (auto-created, gitignored)
│
├── logs/                    # Rotating log files (auto-created, gitignored)
│
├── tests/
│   └── test_pipeline.py     # Pytest unit + integration tests
│
├── app.py                   # Streamlit dashboard entry point
├── requirements.txt         # Pinned dependencies
├── .env.example             # Environment template
└── README.md
```

---

## Setup & Execution

### Prerequisites

- Python 3.11+
- pip (or uv / poetry)
- No database server, no cloud services, no Docker required.

### 1. Clone & Install

```bash
git clone https://github.com/yourhandle/realty-etl.git
cd realty-etl

python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your API key and target markets.
# All variables are optional — defaults run in mock mode with no API key.
```

### 3. Run the Pipeline (Standalone)

```bash
python -m pipeline.orchestrator
```

This executes the full Extract → Transform → Load cycle against the mock API,
creates `data/processed/realty.duckdb`, and writes run logs.

### 4. Launch the Dashboard

```bash
streamlit run app.py
```

Open `http://localhost:8501` in your browser. Click **▶ Run ETL Now** in the
sidebar if the database is empty. Filters, charts, and tables update immediately.

### 5. Schedule via Prefect (Optional)

```bash
# Start a local Prefect server
prefect server start

# In another terminal:
prefect deployment build pipeline/orchestrator.py:run_realty_etl_pipeline \
    --name "nightly-realty-etl" \
    --interval 86400          # 24 hours in seconds

prefect deployment apply run_realty_etl_pipeline-deployment.yaml
prefect agent start --work-queue default
```

### 6. Run Tests

```bash
pytest tests/ -v
```

All 20+ unit tests and one end-to-end integration test should pass.
The integration test uses a tmp_path-scoped DuckDB so it never touches production data.

---

## Financial Model Reference

| KPI | Formula |
|-----|---------|
| **Gross Rent Annual (GRA)** | `monthly_rent × 12` |
| **Effective Gross Income (EGI)** | `GRA × (1 − vacancy_rate)` |
| **Total OPEX** | `EGI × expense_ratio + HOA×12 + prop_tax + insurance` |
| **Net Operating Income (NOI)** | `EGI − Total OPEX` |
| **Cap Rate** | `NOI / list_price` |
| **Gross Rent Multiplier (GRM)** | `list_price / GRA` |
| **Price per Sq Ft** | `list_price / square_feet` |
| **Cash-on-Cash (est.)** | `NOI / (list_price × (0.25 + closing_cost_pct))` |

Default assumptions (all overridable via `.env`):

| Assumption | Default |
|-----------|---------|
| Vacancy rate | 8% |
| Operating expense ratio | 40% of EGI |
| Target minimum cap rate | 6.0% |
| Closing costs | 3% of purchase price |

### Deal Flag Logic

| Flag | Condition |
|------|-----------|
| 🟢 **GREEN** | `cap_rate ≥ target_min` AND `NOI > 0` |
| 🟡 **YELLOW** | `cap_rate ≥ target_min × 0.75` AND `NOI > 0` |
| 🔴 **RED** | `NOI ≤ 0` OR `cap_rate < yellow_threshold` |
| ⚫ **UNSCORED** | Insufficient data to compute cap rate or NOI |

---

## Configuration Reference

All settings are prefixed `REALTY_` in the environment / `.env` file.

| Variable | Default | Description |
|----------|---------|-------------|
| `REALTY_API_BASE_URL` | `https://api.mocklistings.io/v2` | API endpoint |
| `REALTY_API_KEY` | `MOCK_KEY_000` | Bearer token |
| `REALTY_API_MAX_RETRIES` | `5` | Retry attempts per page |
| `REALTY_API_RETRY_BACKOFF_FACTOR` | `1.5` | Exponential backoff multiplier |
| `REALTY_API_RATE_LIMIT_PER_MINUTE` | `60` | Max calls/minute |
| `REALTY_API_PAGE_SIZE` | `100` | Records per API page |
| `REALTY_TARGET_MARKETS` | `["Nashville, TN", ...]` | JSON list of market strings |
| `REALTY_DEFAULT_VACANCY_RATE` | `0.08` | Vacancy assumption (0–1) |
| `REALTY_DEFAULT_EXPENSE_RATIO` | `0.40` | Variable OPEX ratio (0–1) |
| `REALTY_TARGET_CAP_RATE_MIN` | `0.06` | GREEN threshold (0–1) |
| `REALTY_CLOSING_COST_PCT` | `0.03` | CoC equity calculation |
| `REALTY_DB_THREADS` | `4` | DuckDB parallelism |
| `REALTY_DB_MEMORY_LIMIT` | `2GB` | DuckDB memory cap |
| `REALTY_LOG_LEVEL` | `INFO` | DEBUG / INFO / WARNING / ERROR |

---

## Business Value & ROI

### Time Savings

| Task | Manual (Today) | With RealtyETL |
|------|----------------|----------------|
| Download + clean MLS export | 3–5 hrs/week | 0 min (automated) |
| Normalise date formats, types | 1–2 hrs/week | 0 min (automated) |
| Calculate cap rate / GRM per listing | 30 sec/listing × hundreds | 0 min (derived at load) |
| Find GREEN deals across markets | 2+ hrs manual sorting | < 10 sec (live filter) |
| Share deal list with partners | Manual Excel export | One-click CSV download |
| **Weekly total** | **~8 hrs/analyst** | **~0 hrs** |

At a fully-loaded analyst cost of $50/hr, one analyst saves **~$20,000/year**. A team of four saves **$80,000+/year** before accounting for faster acquisition cycle time.

### Decision Quality

The pipeline's consistent financial model eliminates ad-hoc spreadsheet errors that lead to mispriced acquisitions. The deal flag system surfaces the top 5–15% of listings by cap rate in real-time, ensuring acquisition teams focus effort where ROI is highest.

### Operational Observability

Every pipeline run writes a structured audit row to `pipeline_run_log` with record counts, rejection rates, and duration. This surfaces data quality degradation (e.g., API schema changes) before it corrupts downstream decisions.

### Scalability Path

| Stage | Infrastructure | Throughput |
|-------|---------------|-----------|
| **Now** (this repo) | Single machine, DuckDB file | 100K+ listings/run |
| **Next** | Swap DuckDB for MotherDuck (serverless DuckDB cloud) | Unlimited, zero ops |
| **Scale** | Add Dagster/Airflow, swap Streamlit for Metabase | Enterprise-grade |

The modular design (config / extractor / transformer / loader / orchestrator) means any layer can be swapped independently. The transformer's Pydantic contract is the stable interface — the underlying storage, orchestrator, and API client are all replaceable without touching business logic.

---

## Architecture Decision Log

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Storage | DuckDB | Zero-infrastructure OLAP; columnar for analytical queries; Python-native; outperforms SQLite 10–100× on aggregations |
| Validation | Pydantic v2 | Fastest Python validator; declarative schemas; excellent error messages for debugging API chaos |
| Orchestration | Prefect 2.x | Python-native; standalone mode needs no server; trivial Kubernetes deployment when needed |
| Dashboard | Streamlit | Fastest path from DataFrame to interactive BI; no JavaScript required |
| Retry pattern | Exponential backoff | Industry standard for rate-limited REST APIs; configurable per environment |
| Upsert strategy | INSERT OR REPLACE | DuckDB-native; idempotent; safe to re-run without duplicating records |
| Date handling | Try all known formats | More robust than requiring API contract compliance; logs failures for API monitoring |

---

## License

MIT — see [LICENSE](LICENSE)

---

*Built with Python 3.11 · DuckDB · Prefect · Streamlit · Pydantic v2*
