"""
extractor.py — Data extraction layer for the RealtyETL pipeline.

Responsibilities:
  1. Generate (or fetch) realistic, deeply nested, messy JSON property listings.
  2. Simulate real-world API chaos: missing fields, mixed date formats,
     numeric strings, nulls, and occasional server errors.
  3. Enforce retry-with-backoff and per-minute rate limiting.
  4. Dump raw payloads to data/raw/ as timestamped NDJSON files.
  5. Return a list of raw dicts ready for the transformer.

In production, replace `_mock_api_fetch` with a real `requests.Session` call.
The contract (return type, error handling, dump behaviour) stays identical.
"""

from __future__ import annotations

import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import (
    RAW_DIR,
    RawPropertyListing,
    Settings,
    get_logger,
    settings,
)

logger = get_logger(__name__, settings.log_level)

# ─────────────────────────────────────────────────────────────────
# Mock data seed material — realistic for Nashville / Hopkinsville
# ─────────────────────────────────────────────────────────────────
_STREETS = [
    "Acklen Ave", "Charlotte Pike", "Gallatin Rd", "Nolensville Pike",
    "Lebanon Rd", "Old Hickory Blvd", "Murfreesboro Rd", "Dickerson Pike",
    "Main St", "Pembroke Rd", "Fort Campbell Blvd", "Russellville Rd",
    "Canton Pike", "Guthrie Hwy", "Hopkinsville St", "Madison St",
    "Destiny Ln", "Millbrook Dr", "College St", "Spring St",
]
_CITIES_STATES = [
    ("Nashville", "TN"), ("Hopkinsville", "KY"), ("Clarksville", "TN"),
    ("Oak Grove", "KY"), ("Cadiz", "KY"), ("Springfield", "TN"),
    ("Brentwood", "TN"), ("Madison", "TN"),
]
_PROPERTY_TYPES_RAW = [
    "single_family", "Single Family", "SFR", "multi_family", "Multi Family",
    "MF", "condo", "Condo", "townhouse", "TH", "mobile_home",
    "Mobile Home", "MH", None, "unknown",
]
_STATUSES = ["active", "Active", "ACTIVE", "pending", "Pending", "sold", "SOLD",
             "off_market", "withdrawn", None]
_BROKERAGES = [
    "Keller Williams Realty", "RE/MAX Premier", "Coldwell Banker",
    "Benchmark Realty", "Parks", "eXp Realty", "Century 21",
    "Zeitlin Sotheby's", "Berkshire Hathaway HomeServices", None,
]
_TAGS = [
    "investor_special", "cash_flow_positive", "fixer_upper", "turnkey",
    "new_roof", "section_8_approved", "short_term_rental", "motivated_seller",
    "price_reduced", "corner_lot", "historic", "new_construction",
]

# Messy date format catalogue — mirrors real-world API chaos
_DATE_FORMATS_MESSY = [
    "%Y-%m-%d",            # 2024-03-15
    "%m/%d/%Y",            # 03/15/2024
    "%m-%d-%Y",            # 03-15-2024
    "%d %b %Y",            # 15 Mar 2024
    "%B %d, %Y",           # March 15, 2024
    "%Y%m%d",              # 20240315
    "%Y-%m-%dT%H:%M:%S",   # ISO with time
    "%Y-%m-%dT%H:%M:%SZ",  # ISO UTC
]


def _messy_date(days_ago_max: int = 730) -> str | None:
    """Return a date string in a random messy format, or None."""
    if random.random() < 0.07:
        return None
    dt = datetime.now(timezone.utc) - timedelta(
        days=random.randint(0, days_ago_max),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    fmt = random.choice(_DATE_FORMATS_MESSY)
    return dt.strftime(fmt)


def _messy_price(base: float, variance: float = 0.25) -> str | float | None:
    """Return a price in one of several messy representations."""
    if random.random() < 0.04:
        return None
    if random.random() < 0.03:
        return "N/A"
    raw = base * random.uniform(1 - variance, 1 + variance)
    coin = random.random()
    if coin < 0.33:
        return raw                                     # float
    if coin < 0.66:
        return f"${raw:,.2f}"                         # dollar string
    return f"{raw:.0f}"                               # bare numeric string


def _messy_int(value: int) -> int | str | None:
    """Randomly corrupt an integer."""
    if random.random() < 0.05:
        return None
    if random.random() < 0.05:
        return "N/A"
    jitter = value + random.randint(-1, 1)
    return str(jitter) if random.random() < 0.2 else jitter


def _generate_mock_listing(market: str) -> dict[str, Any]:
    """
    Generate one realistic, intentionally messy property listing dict.

    This simulates what a third-party MLS aggregator API actually returns:
    mixed types, missing fields, inconsistent formats, and occasional
    completely garbage values.
    """
    city, state = random.choice(_CITIES_STATES)
    street_num = random.randint(100, 9999)
    street = random.choice(_STREETS)
    sqft = round(random.uniform(600, 4500), 1)
    beds = random.randint(1, 6)
    baths = random.choice([1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0])
    list_price_base = random.uniform(75_000, 650_000)
    rent_base = list_price_base * random.uniform(0.006, 0.011)

    # Occasionally inject full nulls for the nested blocks
    address_block: dict[str, Any] | None
    if random.random() < 0.03:
        address_block = None
    else:
        address_block = {
            "street": f"{street_num} {street}" if random.random() > 0.02 else None,
            "city": city,
            "state": state if random.random() > 0.03 else state.lower(),
            "zip_code": str(random.randint(37000, 42799)),
            "county": random.choice(["Davidson", "Christian", "Montgomery",
                                     "Trigg", "Robertson", None]),
            "latitude": round(random.uniform(36.3, 36.9), 6) if random.random() > 0.1 else None,
            "longitude": round(random.uniform(-88.0, -86.5), 6) if random.random() > 0.1 else None,
        }

    financials_block: dict[str, Any] | None
    if random.random() < 0.05:
        financials_block = None
    else:
        financials_block = {
            "list_price": _messy_price(list_price_base),
            "estimated_rent_monthly": _messy_price(rent_base, variance=0.15),
            "hoa_monthly": _messy_price(
                random.uniform(0, 300), variance=0.3
            ) if random.random() > 0.55 else None,
            "property_tax_annual": _messy_price(
                list_price_base * random.uniform(0.008, 0.022), variance=0.1
            ) if random.random() > 0.1 else None,
            "insurance_annual": _messy_price(
                random.uniform(600, 3000), variance=0.2
            ) if random.random() > 0.1 else None,
            "last_sold_price": _messy_price(
                list_price_base * random.uniform(0.70, 0.98)
            ) if random.random() > 0.4 else None,
            "last_sold_date": _messy_date(days_ago_max=3650),
        }

    return {
        "listing_id": str(uuid.uuid4()),
        "mls_number": f"MLS{random.randint(1000000, 9999999)}" if random.random() > 0.08 else None,
        "property_type": random.choice(_PROPERTY_TYPES_RAW),
        "status": random.choice(_STATUSES),
        "bedrooms": _messy_int(beds),
        "bathrooms": baths if random.random() > 0.06 else str(baths),
        "square_feet": round(sqft, 1) if random.random() > 0.05 else f"{sqft:,.0f}",
        "lot_size_sqft": round(sqft * random.uniform(1.1, 6.0), 0) if random.random() > 0.2 else None,
        "year_built": _messy_int(random.randint(1920, 2024)),
        "days_on_market": _messy_int(random.randint(0, 365)),
        "listing_date": _messy_date(days_ago_max=365),
        "updated_at": _messy_date(days_ago_max=30),
        "address": address_block,
        "financials": financials_block,
        "description": (
            f"{beds}BR/{baths}BA in {city}. {'Investor special. ' if random.random() > 0.7 else ''}"
            f"{'Turnkey condition. ' if random.random() > 0.6 else ''}"
            f"{'Price reduced! ' if random.random() > 0.8 else ''}"
        ) if random.random() > 0.1 else None,
        "agent_name": f"Agent {random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Davis'])}"
                      if random.random() > 0.1 else None,
        "brokerage": random.choice(_BROKERAGES),
        "photos": [f"https://cdn.mock.io/{uuid.uuid4()}.jpg"
                   for _ in range(random.randint(0, 24))],
        "tags": random.sample(_TAGS, k=random.randint(0, 4)),
        "source_market": market,
        "raw_metadata": {
            "api_version": "v2",
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "page": random.randint(1, 20),
        },
    }


# ─────────────────────────────────────────────────────────────────
# Rate limiter
# ─────────────────────────────────────────────────────────────────
class _RateLimiter:
    """
    Token-bucket rate limiter for API calls.

    Attributes:
        calls_per_minute: Maximum calls allowed per 60-second window.
    """

    def __init__(self, calls_per_minute: int) -> None:
        self._calls_per_minute = calls_per_minute
        self._min_interval = 60.0 / calls_per_minute
        self._last_call_time: float = 0.0

    def wait(self) -> None:
        """Block until the next call is permitted."""
        elapsed = time.monotonic() - self._last_call_time
        wait_time = self._min_interval - elapsed
        if wait_time > 0:
            logger.debug("Rate limiter sleeping %.3fs", wait_time)
            time.sleep(wait_time)
        self._last_call_time = time.monotonic()


_rate_limiter = _RateLimiter(settings.api_rate_limit_per_minute)


# ─────────────────────────────────────────────────────────────────
# Core fetch function
# ─────────────────────────────────────────────────────────────────
def _mock_api_fetch(
    market: str,
    page: int,
    page_size: int,
    cfg: Settings,
) -> list[dict[str, Any]]:
    """
    Simulate one paginated API call for a given market.

    In production, replace this body with a `requests.Session.get(...)` call
    to `{cfg.api_base_url}/listings?market={market}&page={page}&size={page_size}`
    with `Authorization: Bearer {cfg.api_key}` and
    `timeout=cfg.api_timeout_seconds`.

    Randomly raises exceptions to exercise retry logic.

    Args:
        market: Market string (e.g., "Nashville, TN").
        page: Page number (1-indexed).
        page_size: Records per page.
        cfg: Settings singleton.

    Returns:
        List of raw listing dicts for this page.

    Raises:
        ConnectionError: ~8% of the time (simulates network hiccup).
        TimeoutError: ~3% of the time (simulates slow response).
    """
    _rate_limiter.wait()

    # Simulate network failures
    roll = random.random()
    if roll < 0.08:
        raise ConnectionError(f"[Mock] Connection refused for market={market} page={page}")
    if roll < 0.11:
        raise TimeoutError(f"[Mock] Request timed out for market={market} page={page}")

    # Last page returns empty list
    max_pages = 3
    if page > max_pages:
        return []

    count = page_size if page < max_pages else random.randint(1, page_size)
    logger.debug("Mock API | market=%s page=%d returning %d records", market, page, count)
    return [_generate_mock_listing(market) for _ in range(count)]


def _fetch_with_retry(
    market: str,
    page: int,
    page_size: int,
    cfg: Settings,
) -> list[dict[str, Any]]:
    """
    Wrap `_mock_api_fetch` with exponential backoff retry logic.

    Args:
        market: Target market string.
        page: Page number.
        page_size: Records per page.
        cfg: Settings singleton.

    Returns:
        List of raw listing dicts, or empty list after exhausting retries.
    """
    last_exc: Exception | None = None
    for attempt in range(1, cfg.api_max_retries + 1):
        try:
            result = _mock_api_fetch(market, page, page_size, cfg)
            if attempt > 1:
                logger.info(
                    "Fetch succeeded on attempt %d | market=%s page=%d",
                    attempt, market, page,
                )
            return result
        except (ConnectionError, TimeoutError) as exc:
            last_exc = exc
            sleep_duration = cfg.api_retry_backoff_factor ** attempt
            logger.warning(
                "Fetch attempt %d/%d failed | market=%s page=%d | error=%s | "
                "sleeping=%.2fs",
                attempt, cfg.api_max_retries, market, page, exc, sleep_duration,
            )
            time.sleep(sleep_duration)
        except Exception as exc:
            logger.error(
                "Unrecoverable fetch error | market=%s page=%d | error=%s",
                market, page, exc,
            )
            raise

    logger.error(
        "All %d retries exhausted | market=%s page=%d | last_error=%s",
        cfg.api_max_retries, market, page, last_exc,
    )
    return []


# ─────────────────────────────────────────────────────────────────
# Raw dump
# ─────────────────────────────────────────────────────────────────
def _dump_raw(records: list[dict[str, Any]], market: str) -> Path:
    """
    Write raw records to a timestamped NDJSON file in data/raw/.

    Args:
        records: List of raw dicts to persist.
        market: Market label used in filename.

    Returns:
        Path to the written file.
    """
    safe_market = market.replace(", ", "_").replace(" ", "_").lower()
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = RAW_DIR / f"{safe_market}_{ts}.ndjson"

    with out_path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, default=str) + "\n")

    logger.info("Raw dump | path=%s | records=%d", out_path, len(records))
    return out_path


# ─────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────
def _validate_raw_records(
    raw_records: list[dict[str, Any]],
) -> tuple[list[RawPropertyListing], list[dict[str, Any]]]:
    """
    Run each raw dict through the Pydantic `RawPropertyListing` schema.

    Returns:
        Tuple of (valid_records, rejected_records).
        Rejected records carry a `_validation_error` key explaining failure.
    """
    valid: list[RawPropertyListing] = []
    rejected: list[dict[str, Any]] = []

    for rec in raw_records:
        try:
            parsed = RawPropertyListing.model_validate(rec)
            valid.append(parsed)
        except Exception as exc:
            rec_copy = dict(rec)
            rec_copy["_validation_error"] = str(exc)
            rejected.append(rec_copy)
            logger.debug(
                "Validation rejected | listing_id=%s | error=%s",
                rec.get("listing_id", "UNKNOWN"),
                exc,
            )

    logger.info(
        "Validation complete | valid=%d rejected=%d total=%d",
        len(valid), len(rejected), len(raw_records),
    )
    return valid, rejected


# ─────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────
def extract(cfg: Settings = settings) -> tuple[list[RawPropertyListing], int, int]:
    """
    Full extraction pass: fetch all pages for all configured markets,
    optionally dump to raw zone, validate against schema.

    Args:
        cfg: Settings object (injectable for testing).

    Returns:
        Tuple of:
          - list[RawPropertyListing]: validated raw listing objects
          - int: total records fetched (before validation)
          - int: total records rejected by schema validation
    """
    logger.info(
        "Extraction starting | markets=%s | page_size=%d",
        cfg.target_markets, cfg.api_page_size,
    )

    all_raw: list[dict[str, Any]] = []

    for market in cfg.target_markets:
        logger.info("Fetching market: %s", market)
        page = 1
        market_total = 0

        while True:
            records = _fetch_with_retry(market, page, cfg.api_page_size, cfg)
            if not records:
                logger.info(
                    "Market %s exhausted at page %d | total_records=%d",
                    market, page, market_total,
                )
                break

            all_raw.extend(records)
            market_total += len(records)
            logger.info(
                "Fetched page %d for %s | page_records=%d | market_running_total=%d",
                page, market, len(records), market_total,
            )
            page += 1

    total_fetched = len(all_raw)
    logger.info("All markets fetched | total_raw_records=%d", total_fetched)

    if cfg.raw_dump_enabled and all_raw:
        for market in cfg.target_markets:
            market_records = [r for r in all_raw if r.get("source_market") == market]
            if market_records:
                _dump_raw(market_records, market)

    valid_records, rejected_records = _validate_raw_records(all_raw)

    if rejected_records:
        rejected_path = RAW_DIR / f"rejected_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
        with rejected_path.open("w", encoding="utf-8") as fh:
            json.dump(rejected_records, fh, indent=2, default=str)
        logger.warning(
            "Rejected records written | path=%s | count=%d",
            rejected_path, len(rejected_records),
        )

    logger.info(
        "Extraction complete | fetched=%d valid=%d rejected=%d",
        total_fetched, len(valid_records), len(rejected_records),
    )
    return valid_records, total_fetched, len(rejected_records)
