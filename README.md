# RealtyETL

**An ETL pipeline and dashboard for property profitability — DuckDB, Prefect, Streamlit.**

Acquisition analysts spend most of their week cleaning listing exports instead of underwriting deals: nested JSON, inconsistent dates, mixed types, duplicates across pages. RealtyETL is a working answer to that problem — a pipeline that ingests listings, standardizes them, scores every property on the metrics an investor actually uses, and puts the result in front of a non-technical user.

```bash
pip install -r requirements.txt
python -m pipeline.orchestrator   # builds the DuckDB warehouse (mock API by default)
streamlit run app.py              # dashboard at localhost:8501
```

No database server, no cloud services, no API keys required — the demo runs against a built-in mock MLS API so the whole system is testable end to end. Point `REALTY_API_BASE_URL` at a real listings API and the same pipeline runs against live data.

## What it does

**Extract** (`extractor.py`) — paginated fetch with token-bucket rate limiting, exponential-backoff retries, and Pydantic validation. Raw payloads are archived as timestamped NDJSON per market per run, so any batch can be replayed.

**Transform** (`transformer.py`) — deduplication on `listing_id + updated`, date normalization across eight formats, type coercion, and per-listing financial scoring: Cap Rate, NOI, GRM, Cash-on-Cash. Each listing gets a deal flag — GREEN / YELLOW / RED / UNSCORED — from configurable thresholds.

**Load** (`loader.py`) — idempotent upsert into DuckDB plus an append-only `pipeline_run_log` audit table. Analytical views (`vw_market_summary`, `vw_top_deals`, `vw_deal_funnel`, and others) are rebuilt on every load.

**Orchestrate** (`orchestrator.py`) — a Prefect 2 flow ties E→T→L together with task-level retries and run history. Runs standalone or on a Prefect server schedule.

**Dashboard** (`app.py`) — Streamlit BI layer: KPI cards, deal alert feed, market scatter, funnel, distributions, top-deals table, CSV export, and an on-demand pipeline trigger. Built so a property manager or fund analyst can filter and export without touching SQL.

## The financial model

| KPI | Formula |
|-----|---------|
| Gross Rent Annual (GRA) | `monthly_rent × 12` |
| Effective Gross Income (EGI) | `GRA × (1 − vacancy_rate)` |
| Total OPEX | `EGI × expense_ratio + HOA×12 + prop_tax + insurance` |
| Net Operating Income (NOI) | `EGI − Total OPEX` |
| Cap Rate | `NOI / list_price` |
| Gross Rent Multiplier (GRM) | `list_price / GRA` |
| Cash-on-Cash (est.) | `NOI / (list_price × (0.25 + closing_cost_pct))` |

Default assumptions (8% vacancy, 40% expense ratio, 6% target cap rate, 3% closing costs) are deliberately conservative and all overridable via `.env`.

| Flag | Condition |
|------|-----------|
| 🟢 GREEN | `cap_rate ≥ target_min` and `NOI > 0` |
| 🟡 YELLOW | `cap_rate ≥ target_min × 0.75` and `NOI > 0` |
| 🔴 RED | `NOI ≤ 0` or below the yellow threshold |
| ⚫ UNSCORED | Not enough data to compute cap rate or NOI |

## Architecture notes

```
API → extract (validate, archive raw) → transform (clean, dedupe, score)
    → load (DuckDB upsert + audit log) → Streamlit dashboard
                 Prefect flow orchestrates all three
```

Why these pieces:

- **DuckDB** — zero-infrastructure columnar OLAP; dramatically faster than SQLite on aggregations, and the whole warehouse is one file.
- **Pydantic v2** — the schema contract that keeps API chaos out of the warehouse. The transformer's output model is the stable interface; storage, orchestrator, and API client can each be swapped without touching business logic.
- **Prefect 2** — Python-native orchestration that needs no server for local runs.
- **Streamlit** — the shortest path from DataFrame to something a stakeholder can use.

Every run writes record counts, rejection rates, and duration to the audit log, so data-quality drift (an API schema change, a spike in rejects) is visible before it corrupts downstream numbers.

## Repository layout

```
pipeline/
  config.py        # settings, logging, Pydantic schemas, DDL
  extractor.py     # fetch, retry, rate-limit, validate
  transformer.py   # clean, dedupe, score
  loader.py        # DuckDB upsert, views, run log
  orchestrator.py  # Prefect flow
tests/
  test_pipeline.py # 20+ unit tests + an end-to-end integration test
app.py             # Streamlit dashboard
```

`pytest tests/ -v` runs the suite; the integration test uses a temp-scoped DuckDB.

## Configuration

All settings are environment variables prefixed `REALTY_` (see `.env.example`): API endpoint and credentials, retry/rate-limit tuning, target markets, and every financial assumption in the model. Defaults run the mock pipeline with no configuration at all.

## License

MIT — see [LICENSE](LICENSE).
