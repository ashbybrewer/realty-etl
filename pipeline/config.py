"""
config.py — Environment configuration, logging setup, and schema validation
for the RealtyETL Property Profitability Analytics Pipeline.

All environment variables are loaded from .env via python-dotenv.
Schema validation uses Pydantic v2 for strict type enforcement.
"""

from __future__ import annotations

import logging
import os
import sys
from enum import Enum
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings

# ─────────────────────────────────────────────
# Path resolution
# ─────────────────────────────────────────────
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
DB_PATH = PROCESSED_DIR / "realty.duckdb"
LOG_DIR = ROOT_DIR / "logs"

for _d in (RAW_DIR, PROCESSED_DIR, LOG_DIR):
    _d.mkdir(parents=True, exist_ok=True)

load_dotenv(ROOT_DIR / ".env")

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
LOG_LEVEL_MAP: dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}

_LOG_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)-30s | %(funcName)-25s | %(message)s"
)
_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Factory for named, pre-configured loggers that write to both
    stdout and a rotating file. Call once per module at module level.

    Args:
        name: Logger name (use __name__ in caller modules).
        level: Log level string. Defaults to INFO.

    Returns:
        Configured logging.Logger instance.
    """
    from logging.handlers import RotatingFileHandler

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured — avoid duplicate handlers

    resolved_level = LOG_LEVEL_MAP.get(level.upper(), logging.INFO)
    logger.setLevel(resolved_level)

    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    # stdout handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(resolved_level)
    logger.addHandler(stream_handler)

    # rotating file handler — 5 MB per file, 3 backups
    log_file = LOG_DIR / f"{name.replace('.', '_')}.log"
    file_handler = RotatingFileHandler(
        log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(resolved_level)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger


logger = get_logger(__name__)

# ─────────────────────────────────────────────
# Settings (reads from env / .env)
# ─────────────────────────────────────────────
class PropertyType(str, Enum):
    SINGLE_FAMILY = "single_family"
    MULTI_FAMILY = "multi_family"
    CONDO = "condo"
    TOWNHOUSE = "townhouse"
    MOBILE_HOME = "mobile_home"
    LAND = "land"
    COMMERCIAL = "commercial"
    UNKNOWN = "unknown"


class Settings(BaseSettings):
    """
    Central settings object. All fields have safe defaults so the pipeline
    can run without a .env file in local/mock mode.
    """

    # API surface
    api_base_url: str = Field(
        default="https://api.mocklistings.io/v2",
        description="Base URL for the property listings API.",
    )
    api_key: str = Field(
        default="MOCK_KEY_000",
        description="Bearer token for the listings API.",
    )
    api_timeout_seconds: int = Field(default=30, ge=5, le=120)
    api_max_retries: int = Field(default=5, ge=1, le=10)
    api_retry_backoff_factor: float = Field(default=1.5, ge=0.5, le=5.0)
    api_rate_limit_per_minute: int = Field(default=60, ge=1, le=600)
    api_page_size: int = Field(default=100, ge=10, le=500)

    # Markets to ingest
    target_markets: list[str] = Field(
        default=["Nashville, TN", "Hopkinsville, KY", "Clarksville, TN"],
        description="List of market strings passed to the API.",
    )

    # ETL behaviour
    raw_dump_enabled: bool = Field(
        default=True,
        description="If True, raw API payloads are written to data/raw/.",
    )
    dedup_window_days: int = Field(
        default=7,
        description="Look-back window (days) used for deduplication keying.",
    )
    log_level: str = Field(default="INFO")

    # Financial assumptions (used in transformer profitability calculations)
    default_vacancy_rate: float = Field(
        default=0.08, ge=0.0, le=1.0,
        description="Default vacancy rate assumption if not provided by source.",
    )
    default_expense_ratio: float = Field(
        default=0.40, ge=0.0, le=1.0,
        description="Operating expense ratio (OPEX / Gross Rent).",
    )
    target_cap_rate_min: float = Field(
        default=0.06, ge=0.0, le=1.0,
        description="Minimum acceptable cap rate for deal flagging.",
    )
    closing_cost_pct: float = Field(
        default=0.03, ge=0.0, le=0.10,
        description="Estimated closing costs as pct of purchase price.",
    )
    annual_appreciation_assumption: float = Field(
        default=0.03, ge=0.0, le=0.20,
        description="Annual appreciation rate assumption for IRR modelling.",
    )

    # DuckDB
    db_path: Path = Field(default=DB_PATH)
    db_threads: int = Field(default=4, ge=1, le=32)
    db_memory_limit: str = Field(default="2GB")

    class Config:
        env_prefix = "REALTY_"
        env_file = str(ROOT_DIR / ".env")
        env_file_encoding = "utf-8"
        case_sensitive = False


# Singleton — import `settings` everywhere
settings = Settings()
logger.info(
    "Settings loaded | markets=%s | db=%s | log_level=%s",
    settings.target_markets,
    settings.db_path,
    settings.log_level,
)

# ─────────────────────────────────────────────
# Pydantic schema definitions for raw API data
# ─────────────────────────────────────────────
class RawAddress(BaseModel):
    """Nested address block — tolerates missing sub-fields."""

    street: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    county: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    @field_validator("zip_code", mode="before")
    @classmethod
    def coerce_zip(cls, v: object) -> Optional[str]:
        if v is None:
            return None
        return str(v).strip().zfill(5)[:5]

    @field_validator("state", mode="before")
    @classmethod
    def normalise_state(cls, v: object) -> Optional[str]:
        if v is None:
            return None
        return str(v).strip().upper()[:2]


class RawFinancials(BaseModel):
    """Financial sub-document — all optional because APIs lie."""

    list_price: Optional[float] = None
    estimated_rent_monthly: Optional[float] = None
    hoa_monthly: Optional[float] = None
    property_tax_annual: Optional[float] = None
    insurance_annual: Optional[float] = None
    last_sold_price: Optional[float] = None
    last_sold_date: Optional[str] = None  # raw string — transformer will parse

    @field_validator("list_price", "estimated_rent_monthly", "hoa_monthly",
                     "property_tax_annual", "insurance_annual", "last_sold_price",
                     mode="before")
    @classmethod
    def coerce_numeric(cls, v: object) -> Optional[float]:
        if v is None or v == "" or v == "N/A":
            return None
        try:
            return float(str(v).replace("$", "").replace(",", "").strip())
        except (ValueError, TypeError):
            return None


class RawPropertyListing(BaseModel):
    """
    Top-level schema for one raw listing record from the API.
    Strict validation is intentionally relaxed at this layer —
    the transformer enforces business rules downstream.
    """

    listing_id: str
    mls_number: Optional[str] = None
    property_type: Optional[str] = None
    status: Optional[str] = None
    bedrooms: Optional[int] = None
    bathrooms: Optional[float] = None
    square_feet: Optional[float] = None
    lot_size_sqft: Optional[float] = None
    year_built: Optional[int] = None
    days_on_market: Optional[int] = None
    listing_date: Optional[str] = None   # raw — various formats
    updated_at: Optional[str] = None     # raw — various formats
    address: Optional[RawAddress] = None
    financials: Optional[RawFinancials] = None
    description: Optional[str] = None
    agent_name: Optional[str] = None
    brokerage: Optional[str] = None
    photos: Optional[list[str]] = Field(default_factory=list)
    tags: Optional[list[str]] = Field(default_factory=list)
    source_market: Optional[str] = None
    raw_metadata: Optional[dict] = Field(default_factory=dict)

    @field_validator("listing_id", mode="before")
    @classmethod
    def ensure_string_id(cls, v: object) -> str:
        if v is None or str(v).strip() == "":
            raise ValueError("listing_id must be a non-empty string.")
        return str(v).strip()

    @field_validator("bedrooms", "year_built", "days_on_market", mode="before")
    @classmethod
    def coerce_int_or_none(cls, v: object) -> Optional[int]:
        if v is None or v == "" or v == "N/A":
            return None
        try:
            return int(float(str(v)))
        except (ValueError, TypeError):
            return None

    @field_validator("bathrooms", "square_feet", "lot_size_sqft", mode="before")
    @classmethod
    def coerce_float_or_none(cls, v: object) -> Optional[float]:
        if v is None or v == "" or v == "N/A":
            return None
        try:
            return float(str(v).replace(",", "").strip())
        except (ValueError, TypeError):
            return None


class CleanPropertyListing(BaseModel):
    """
    Output schema after transformation. All analytical fields are typed,
    financial KPIs are derived, and datetimes are ISO-8601 strings.
    """

    listing_id: str
    mls_number: Optional[str]
    property_type: PropertyType
    status: str
    bedrooms: Optional[int]
    bathrooms: Optional[float]
    square_feet: Optional[float]
    lot_size_sqft: Optional[float]
    year_built: Optional[int]
    days_on_market: Optional[int]
    listing_date: Optional[str]      # ISO-8601
    updated_at: Optional[str]        # ISO-8601
    last_sold_date: Optional[str]    # ISO-8601

    # Address
    street: Optional[str]
    city: Optional[str]
    state: Optional[str]
    zip_code: Optional[str]
    county: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]

    # Raw financials
    list_price: Optional[float]
    estimated_rent_monthly: Optional[float]
    hoa_monthly: Optional[float]
    property_tax_annual: Optional[float]
    insurance_annual: Optional[float]
    last_sold_price: Optional[float]

    # Derived KPIs (computed in transformer)
    gross_rent_annual: Optional[float]
    effective_gross_income: Optional[float]    # vacancy-adjusted
    total_opex_annual: Optional[float]
    net_operating_income: Optional[float]
    cap_rate: Optional[float]
    gross_rent_multiplier: Optional[float]
    price_per_sqft: Optional[float]
    rent_per_sqft: Optional[float]
    cash_on_cash_estimate: Optional[float]
    deal_flag: str                             # GREEN / YELLOW / RED / UNSCORED

    source_market: Optional[str]
    ingested_at: str                           # ISO-8601 UTC
    pipeline_version: str = "1.0.0"


# ─────────────────────────────────────────────
# DuckDB DDL — single source of truth
# ─────────────────────────────────────────────
LISTINGS_DDL = """
CREATE TABLE IF NOT EXISTS listings (
    listing_id               VARCHAR PRIMARY KEY,
    mls_number               VARCHAR,
    property_type            VARCHAR,
    status                   VARCHAR,
    bedrooms                 INTEGER,
    bathrooms                DOUBLE,
    square_feet              DOUBLE,
    lot_size_sqft            DOUBLE,
    year_built               INTEGER,
    days_on_market           INTEGER,
    listing_date             TIMESTAMP,
    updated_at               TIMESTAMP,
    last_sold_date           TIMESTAMP,
    street                   VARCHAR,
    city                     VARCHAR,
    state                    VARCHAR,
    zip_code                 VARCHAR,
    county                   VARCHAR,
    latitude                 DOUBLE,
    longitude                DOUBLE,
    list_price               DOUBLE,
    estimated_rent_monthly   DOUBLE,
    hoa_monthly              DOUBLE,
    property_tax_annual      DOUBLE,
    insurance_annual         DOUBLE,
    last_sold_price          DOUBLE,
    gross_rent_annual        DOUBLE,
    effective_gross_income   DOUBLE,
    total_opex_annual        DOUBLE,
    net_operating_income     DOUBLE,
    cap_rate                 DOUBLE,
    gross_rent_multiplier    DOUBLE,
    price_per_sqft           DOUBLE,
    rent_per_sqft            DOUBLE,
    cash_on_cash_estimate    DOUBLE,
    deal_flag                VARCHAR,
    source_market            VARCHAR,
    ingested_at              TIMESTAMP,
    pipeline_version         VARCHAR
);
"""

PIPELINE_RUN_LOG_DDL = """
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id          VARCHAR PRIMARY KEY,
    started_at      TIMESTAMP,
    finished_at     TIMESTAMP,
    records_fetched INTEGER,
    records_clean   INTEGER,
    records_upserted INTEGER,
    records_rejected INTEGER,
    markets         VARCHAR,
    status          VARCHAR,
    error_message   VARCHAR
);
"""
