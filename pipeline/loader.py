"""
loader.py — Data loading layer for the RealtyETL pipeline.

Responsibilities:
  1. Connect to (or create) the DuckDB analytical database.
  2. Create tables and analytical views via DDL if they don't exist.
  3. Stage clean records in a temp table, then UPSERT into the main listings
     table using DuckDB's INSERT OR REPLACE semantics.
  4. Write a pipeline run log entry to `pipeline_run_log`.
  5. Create materialised analytical views over the loaded data.
  6. Return load metrics for the orchestrator.

DuckDB is used as the analytical store because it offers:
  - Columnar OLAP storage with zero infrastructure overhead
  - Full SQL including window functions, PIVOT, UNNEST
  - Parquet/CSV export out of the box
  - Thread-safe reads from multiple Streamlit sessions
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import duckdb

from .config import (
    LISTINGS_DDL,
    PIPELINE_RUN_LOG_DDL,
    CleanPropertyListing,
    Settings,
    get_logger,
    settings,
)

logger = get_logger(__name__, settings.log_level)

# ─────────────────────────────────────────────────────────────────
# Analytical view DDL
# ─────────────────────────────────────────────────────────────────
_MARKET_SUMMARY_VIEW_DDL = """
CREATE OR REPLACE VIEW vw_market_summary AS
SELECT
    source_market,
    COUNT(*)                                        AS total_listings,
    COUNT(*) FILTER (WHERE status = 'active')       AS active_listings,
    COUNT(*) FILTER (WHERE deal_flag = 'GREEN')     AS green_deals,
    COUNT(*) FILTER (WHERE deal_flag = 'YELLOW')    AS yellow_deals,
    COUNT(*) FILTER (WHERE deal_flag = 'RED')       AS red_deals,
    COUNT(*) FILTER (WHERE deal_flag = 'UNSCORED')  AS unscored_deals,
    ROUND(AVG(list_price), 0)                       AS avg_list_price,
    ROUND(MEDIAN(list_price), 0)                    AS median_list_price,
    ROUND(AVG(estimated_rent_monthly), 0)           AS avg_rent_monthly,
    ROUND(AVG(cap_rate) * 100, 2)                   AS avg_cap_rate_pct,
    ROUND(AVG(gross_rent_multiplier), 2)            AS avg_grm,
    ROUND(AVG(price_per_sqft), 2)                   AS avg_price_per_sqft,
    ROUND(AVG(net_operating_income), 0)             AS avg_noi,
    ROUND(AVG(days_on_market), 1)                   AS avg_dom,
    MIN(list_price)                                 AS min_price,
    MAX(list_price)                                 AS max_price,
    MAX(ingested_at)                                AS last_updated
FROM listings
WHERE list_price > 0
GROUP BY source_market
ORDER BY avg_cap_rate_pct DESC NULLS LAST;
"""

_DEAL_FUNNEL_VIEW_DDL = """
CREATE OR REPLACE VIEW vw_deal_funnel AS
SELECT
    deal_flag,
    property_type,
    source_market,
    COUNT(*)                                AS count,
    ROUND(AVG(list_price), 0)               AS avg_price,
    ROUND(AVG(cap_rate) * 100, 2)           AS avg_cap_rate_pct,
    ROUND(AVG(net_operating_income), 0)     AS avg_noi,
    ROUND(AVG(cash_on_cash_estimate)*100,2) AS avg_coc_pct,
    ROUND(AVG(gross_rent_multiplier), 2)    AS avg_grm,
    ROUND(AVG(days_on_market), 1)           AS avg_dom
FROM listings
WHERE list_price > 0
GROUP BY deal_flag, property_type, source_market
ORDER BY deal_flag, avg_cap_rate_pct DESC NULLS LAST;
"""

_TOP_DEALS_VIEW_DDL = """
CREATE OR REPLACE VIEW vw_top_deals AS
SELECT
    listing_id,
    mls_number,
    COALESCE(street || ', ' || city || ', ' || state, city || ', ' || state, listing_id) AS full_address,
    property_type,
    status,
    bedrooms,
    bathrooms,
    square_feet,
    year_built,
    source_market,
    list_price,
    estimated_rent_monthly,
    gross_rent_annual,
    net_operating_income,
    ROUND(cap_rate * 100, 2)                   AS cap_rate_pct,
    gross_rent_multiplier,
    price_per_sqft,
    ROUND(cash_on_cash_estimate * 100, 2)      AS coc_pct,
    deal_flag,
    days_on_market,
    listing_date,
    ingested_at
FROM listings
WHERE deal_flag IN ('GREEN', 'YELLOW')
  AND status = 'active'
  AND list_price > 0
ORDER BY cap_rate DESC NULLS LAST
LIMIT 200;
"""

_PRICE_DISTRIBUTION_VIEW_DDL = """
CREATE OR REPLACE VIEW vw_price_distribution AS
SELECT
    source_market,
    property_type,
    CASE
        WHEN list_price < 100000  THEN 'Under $100K'
        WHEN list_price < 150000  THEN '$100K–$150K'
        WHEN list_price < 200000  THEN '$150K–$200K'
        WHEN list_price < 250000  THEN '$200K–$250K'
        WHEN list_price < 300000  THEN '$250K–$300K'
        WHEN list_price < 400000  THEN '$300K–$400K'
        WHEN list_price < 500000  THEN '$400K–$500K'
        ELSE 'Over $500K'
    END                         AS price_bucket,
    COUNT(*)                    AS count,
    ROUND(AVG(cap_rate)*100,2)  AS avg_cap_rate_pct
FROM listings
WHERE list_price > 0
GROUP BY source_market, property_type, price_bucket
ORDER BY source_market, MIN(list_price);
"""

_TREND_VIEW_DDL = """
CREATE OR REPLACE VIEW vw_ingestion_trend AS
SELECT
    DATE_TRUNC('day', ingested_at)  AS ingest_date,
    source_market,
    COUNT(*)                        AS records_loaded,
    ROUND(AVG(cap_rate)*100, 2)     AS avg_cap_rate_pct,
    ROUND(AVG(list_price), 0)       AS avg_list_price
FROM listings
GROUP BY DATE_TRUNC('day', ingested_at), source_market
ORDER BY ingest_date DESC, source_market;
"""

_ALL_VIEW_DDL: list[str] = [
    _MARKET_SUMMARY_VIEW_DDL,
    _DEAL_FUNNEL_VIEW_DDL,
    _TOP_DEALS_VIEW_DDL,
    _PRICE_DISTRIBUTION_VIEW_DDL,
    _TREND_VIEW_DDL,
]


# ─────────────────────────────────────────────────────────────────
# Connection factory
# ─────────────────────────────────────────────────────────────────
def get_connection(cfg: Settings = settings) -> duckdb.DuckDBPyConnection:
    """
    Open and configure a DuckDB connection to the analytical database.

    Args:
        cfg: Settings object for path and performance tuning.

    Returns:
        Configured duckdb.DuckDBPyConnection (caller must close).
    """
    con = duckdb.connect(str(cfg.db_path))
    con.execute(f"PRAGMA threads={cfg.db_threads};")
    con.execute(f"PRAGMA memory_limit='{cfg.db_memory_limit}';")
    logger.debug("DuckDB connection opened | path=%s", cfg.db_path)
    return con


# ─────────────────────────────────────────────────────────────────
# Schema initialisation
# ─────────────────────────────────────────────────────────────────
def initialise_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create tables and views if they do not already exist.

    Args:
        con: Active DuckDB connection.
    """
    con.execute(LISTINGS_DDL)
    con.execute(PIPELINE_RUN_LOG_DDL)
    for view_ddl in _ALL_VIEW_DDL:
        con.execute(view_ddl)
    logger.info("Schema initialised | tables=listings,pipeline_run_log | views=%d", len(_ALL_VIEW_DDL))


# ─────────────────────────────────────────────────────────────────
# UPSERT logic
# ─────────────────────────────────────────────────────────────────
def _records_to_rows(records: list[CleanPropertyListing]) -> list[tuple[Any, ...]]:
    """
    Convert Pydantic model instances to plain tuples matching the listings DDL
    column order. DuckDB's executemany takes tuples, not dicts.

    Column order must exactly match LISTINGS_DDL.

    Args:
        records: Cleaned listing objects.

    Returns:
        List of tuples ready for bulk insert.
    """
    rows = []
    for r in records:
        rows.append((
            r.listing_id,
            r.mls_number,
            r.property_type.value,
            r.status,
            r.bedrooms,
            r.bathrooms,
            r.square_feet,
            r.lot_size_sqft,
            r.year_built,
            r.days_on_market,
            r.listing_date,    # ISO string → DuckDB casts to TIMESTAMP
            r.updated_at,
            r.last_sold_date,
            r.street,
            r.city,
            r.state,
            r.zip_code,
            r.county,
            r.latitude,
            r.longitude,
            r.list_price,
            r.estimated_rent_monthly,
            r.hoa_monthly,
            r.property_tax_annual,
            r.insurance_annual,
            r.last_sold_price,
            r.gross_rent_annual,
            r.effective_gross_income,
            r.total_opex_annual,
            r.net_operating_income,
            r.cap_rate,
            r.gross_rent_multiplier,
            r.price_per_sqft,
            r.rent_per_sqft,
            r.cash_on_cash_estimate,
            r.deal_flag,
            r.source_market,
            r.ingested_at,
            r.pipeline_version,
        ))
    return rows


_UPSERT_SQL = """
INSERT OR REPLACE INTO listings VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
    ?, ?, ?, ?, ?, ?, ?, ?, ?
);
"""


def upsert_listings(
    con: duckdb.DuckDBPyConnection,
    records: list[CleanPropertyListing],
    batch_size: int = 500,
) -> int:
    """
    Bulk upsert clean listings into the `listings` table in batches.

    Uses INSERT OR REPLACE which updates any existing row with a matching
    PRIMARY KEY (listing_id) and inserts new rows otherwise.

    Args:
        con: Active DuckDB connection.
        records: Clean listing objects to persist.
        batch_size: Records per executemany batch. Default 500.

    Returns:
        Total records upserted.
    """
    if not records:
        logger.warning("upsert_listings called with 0 records — skipping.")
        return 0

    rows = _records_to_rows(records)
    total = 0

    for start in range(0, len(rows), batch_size):
        batch = rows[start: start + batch_size]
        con.executemany(_UPSERT_SQL, batch)
        total += len(batch)
        logger.debug("Upserted batch | start=%d batch_size=%d running_total=%d", start, len(batch), total)

    # Refresh analytical views after write
    for view_ddl in _ALL_VIEW_DDL:
        con.execute(view_ddl)

    logger.info("Upsert complete | total_upserted=%d", total)
    return total


# ─────────────────────────────────────────────────────────────────
# Run log
# ─────────────────────────────────────────────────────────────────
def log_pipeline_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    records_fetched: int,
    records_clean: int,
    records_upserted: int,
    records_rejected: int,
    markets: list[str],
    status: str,
    error_message: Optional[str] = None,
) -> None:
    """
    Write one row to the pipeline_run_log table for observability.

    Args:
        con: Active DuckDB connection.
        run_id: UUID string for this run.
        started_at: UTC datetime when the run started.
        finished_at: UTC datetime when the run finished.
        records_fetched: Raw records from the API (before validation).
        records_clean: Records after cleaning (before load).
        records_upserted: Records successfully loaded.
        records_rejected: Records rejected by schema validation or transform.
        markets: List of markets ingested.
        status: "SUCCESS", "PARTIAL", or "FAILED".
        error_message: Optional error string if status != SUCCESS.
    """
    con.execute(
        """
        INSERT OR REPLACE INTO pipeline_run_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            started_at.isoformat(),
            finished_at.isoformat(),
            records_fetched,
            records_clean,
            records_upserted,
            records_rejected,
            ", ".join(markets),
            status,
            error_message,
        ],
    )
    logger.info(
        "Run log written | run_id=%s status=%s fetched=%d clean=%d upserted=%d rejected=%d",
        run_id, status, records_fetched, records_clean, records_upserted, records_rejected,
    )


# ─────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────
def load(
    clean_records: list[CleanPropertyListing],
    records_fetched: int,
    records_rejected: int,
    transform_errors: int,
    cfg: Settings = settings,
) -> dict[str, Any]:
    """
    Full load pass: open DB, init schema, upsert records, write run log.

    Args:
        clean_records: Output from transformer.transform().
        records_fetched: Raw count from extractor (for run log).
        records_rejected: Validation rejects from extractor (for run log).
        transform_errors: Transform failures (for run log).
        cfg: Settings singleton (injectable for testing).

    Returns:
        Dict with load metrics: {run_id, upserted, status, duration_seconds}.
    """
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    status = "SUCCESS"
    error_message: Optional[str] = None
    upserted = 0

    try:
        con = get_connection(cfg)
        initialise_schema(con)
        upserted = upsert_listings(con, clean_records)
        finished_at = datetime.now(timezone.utc)

        total_rejects = records_rejected + transform_errors
        if total_rejects > 0 and upserted == 0:
            status = "FAILED"
        elif total_rejects > 0:
            status = "PARTIAL"

        log_pipeline_run(
            con=con,
            run_id=run_id,
            started_at=started_at,
            finished_at=finished_at,
            records_fetched=records_fetched,
            records_clean=len(clean_records),
            records_upserted=upserted,
            records_rejected=total_rejects,
            markets=cfg.target_markets,
            status=status,
            error_message=error_message,
        )
        con.close()

    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        status = "FAILED"
        error_message = str(exc)
        logger.error("Load FAILED | run_id=%s | error=%s", run_id, exc, exc_info=True)

    duration = (finished_at - started_at).total_seconds()
    logger.info(
        "Load complete | run_id=%s status=%s upserted=%d duration=%.2fs",
        run_id, status, upserted, duration,
    )

    return {
        "run_id": run_id,
        "upserted": upserted,
        "status": status,
        "duration_seconds": duration,
        "error_message": error_message,
    }
