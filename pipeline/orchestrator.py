"""
orchestrator.py — Prefect 2.x pipeline orchestration for RealtyETL.

Defines:
  - @task wrappers around extract / transform / load
  - @flow `run_realty_etl_pipeline` as the top-level entry point
  - CLI runner at the bottom (also callable from Prefect deployments)

Run locally:
    python -m pipeline.orchestrator

Schedule via Prefect:
    prefect deployment build pipeline/orchestrator.py:run_realty_etl_pipeline \\
        --name "nightly-realty-etl" --interval 86400
    prefect deployment apply run_realty_etl_pipeline-deployment.yaml
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone

from prefect import flow, get_run_logger, task

# Prefect 3.x renamed SequentialTaskRunner → ThreadPoolTaskRunner
try:
    from prefect.task_runners import ThreadPoolTaskRunner as _DefaultTaskRunner
except ImportError:
    from prefect.task_runners import SequentialTaskRunner as _DefaultTaskRunner  # type: ignore

from .config import Settings, get_logger, settings
from .extractor import extract
from .loader import load
from .transformer import transform

# Module-level logger for non-Prefect contexts
_logger = get_logger(__name__, settings.log_level)


# ─────────────────────────────────────────────────────────────────
# Prefect tasks (thin wrappers — business logic lives in modules)
# ─────────────────────────────────────────────────────────────────
@task(
    name="extract-property-listings",
    description="Fetch messy property listings from the API across all configured markets.",
    retries=0,          # retry is handled inside extractor._fetch_with_retry
    tags=["etl", "extract"],
)
def extract_task(cfg: Settings):
    logger = get_run_logger()
    logger.info("Extract task starting | markets=%s", cfg.target_markets)
    raw_records, fetched, rejected = extract(cfg)
    logger.info("Extract task done | fetched=%d valid=%d rejected=%d", fetched, len(raw_records), rejected)
    return raw_records, fetched, rejected


@task(
    name="transform-property-listings",
    description="Clean, normalise, deduplicate, and compute profitability KPIs.",
    retries=0,
    tags=["etl", "transform"],
)
def transform_task(raw_records, cfg: Settings):
    logger = get_run_logger()
    logger.info("Transform task starting | raw_records=%d", len(raw_records))
    clean_records, error_count = transform(raw_records, cfg)
    logger.info(
        "Transform task done | clean=%d errors=%d",
        len(clean_records), error_count,
    )
    return clean_records, error_count


@task(
    name="load-property-listings",
    description="Upsert clean listings into DuckDB and write pipeline run log.",
    retries=2,
    retry_delay_seconds=5,
    tags=["etl", "load"],
)
def load_task(clean_records, records_fetched, records_rejected, transform_errors, cfg: Settings):
    logger = get_run_logger()
    logger.info("Load task starting | clean_records=%d", len(clean_records))
    metrics = load(
        clean_records=clean_records,
        records_fetched=records_fetched,
        records_rejected=records_rejected,
        transform_errors=transform_errors,
        cfg=cfg,
    )
    logger.info("Load task done | metrics=%s", metrics)
    return metrics


# ─────────────────────────────────────────────────────────────────
# Top-level Prefect flow
# ─────────────────────────────────────────────────────────────────
@flow(
    name="realty-etl-pipeline",
    description=(
        "End-to-end ETL: ingest property listings from the mock MLS API, "
        "clean and score them for rental profitability, and load into DuckDB "
        "for Streamlit analytics."
    ),
    task_runner=_DefaultTaskRunner(),
    log_prints=True,
)
def run_realty_etl_pipeline(cfg: Settings = settings) -> dict:
    """
    Orchestrates the full Extract → Transform → Load cycle.

    Args:
        cfg: Settings object. Injected by Prefect or the CLI.

    Returns:
        Final load metrics dict for downstream inspection.
    """
    flow_start = datetime.now(timezone.utc)
    pf_logger = get_run_logger()
    pf_logger.info(
        "Pipeline STARTED | markets=%s | ts=%s",
        cfg.target_markets, flow_start.isoformat(),
    )

    # ── Extract ──────────────────────────────────────────────────
    raw_records, fetched, rejected = extract_task(cfg)

    if not raw_records:
        pf_logger.error("No valid records returned from extraction — aborting.")
        return {"status": "ABORTED", "reason": "no_valid_records"}

    # ── Transform ────────────────────────────────────────────────
    clean_records, transform_errors = transform_task(raw_records, cfg)

    if not clean_records:
        pf_logger.error("Transform produced 0 clean records — aborting load.")
        return {"status": "ABORTED", "reason": "no_clean_records"}

    # ── Load ─────────────────────────────────────────────────────
    metrics = load_task(clean_records, fetched, rejected, transform_errors, cfg)

    duration = (datetime.now(timezone.utc) - flow_start).total_seconds()
    pf_logger.info(
        "Pipeline COMPLETE | status=%s | upserted=%d | duration=%.2fs",
        metrics.get("status"), metrics.get("upserted"), duration,
    )

    return metrics


# ─────────────────────────────────────────────────────────────────
# CLI entry point (no Prefect server required)
# ─────────────────────────────────────────────────────────────────
def _run_standalone() -> None:
    """Run the pipeline synchronously without a Prefect server."""
    _logger.info("Running RealtyETL pipeline in standalone mode.")
    result = run_realty_etl_pipeline(cfg=settings)
    _logger.info("Standalone run result: %s", result)
    sys.exit(0 if result.get("status") in ("SUCCESS", "PARTIAL") else 1)


if __name__ == "__main__":
    _run_standalone()
