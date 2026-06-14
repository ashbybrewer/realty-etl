"""
transformer.py — Data transformation layer for the RealtyETL pipeline.

Responsibilities:
  1. Parse all raw date strings into ISO-8601 UTC regardless of input format.
  2. Normalise property types, statuses, and categorical fields.
  3. Enforce numeric type contracts; replace sentinel nulls with None.
  4. Deduplicate on listing_id (latest updated_at wins).
  5. Derive financial KPIs:
       - Gross Rent Annual
       - Effective Gross Income (vacancy-adjusted)
       - Total OPEX
       - Net Operating Income (NOI)
       - Cap Rate
       - Gross Rent Multiplier (GRM)
       - Price per Square Foot
       - Estimated Cash-on-Cash Return
  6. Assign deal flag (GREEN / YELLOW / RED / UNSCORED).
  7. Return a list of CleanPropertyListing objects ready for the loader.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Optional

from .config import (
    CleanPropertyListing,
    PropertyType,
    RawPropertyListing,
    Settings,
    get_logger,
    settings,
)

logger = get_logger(__name__, settings.log_level)

# ─────────────────────────────────────────────────────────────────
# Date parsing
# ─────────────────────────────────────────────────────────────────
_DATE_FORMATS: list[str] = [
    "%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m-%d-%Y",
    "%d %b %Y",
    "%B %d, %Y",
    "%b %d, %Y",
    "%Y%m%d",
]

_TIMEZONE_SUFFIX_RE = re.compile(r"[+-]\d{2}:\d{2}$")


def _parse_datetime(raw: Optional[str]) -> Optional[str]:
    """
    Attempt to parse a messy date string into an ISO-8601 UTC string.

    Tries each known format in order. Strips timezone suffixes before
    attempting naive parsing and coerces the result to UTC.

    Args:
        raw: Raw date string from the API. May be None, empty, or garbage.

    Returns:
        ISO-8601 UTC string (e.g. "2024-03-15T14:30:00+00:00"),
        or None if the string cannot be parsed.
    """
    if not raw or not isinstance(raw, str):
        return None

    cleaned = raw.strip()

    # Strip timezone offset suffix so naive parsers don't choke
    cleaned_for_parse = _TIMEZONE_SUFFIX_RE.sub("", cleaned).strip()

    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(cleaned_for_parse, fmt)
            # Attach UTC — we treat all API dates as UTC unless they contain
            # explicit TZ info (handled via replace below)
            dt_utc = dt.replace(tzinfo=timezone.utc)
            return dt_utc.isoformat()
        except ValueError:
            continue

    logger.debug("Date parse failed | raw=%r", raw)
    return None


# ─────────────────────────────────────────────────────────────────
# Property type normalisation
# ─────────────────────────────────────────────────────────────────
_TYPE_MAP: dict[str, PropertyType] = {
    "single_family": PropertyType.SINGLE_FAMILY,
    "sfr": PropertyType.SINGLE_FAMILY,
    "sf": PropertyType.SINGLE_FAMILY,
    "single family": PropertyType.SINGLE_FAMILY,
    "single-family": PropertyType.SINGLE_FAMILY,
    "multi_family": PropertyType.MULTI_FAMILY,
    "multifamily": PropertyType.MULTI_FAMILY,
    "mf": PropertyType.MULTI_FAMILY,
    "multi family": PropertyType.MULTI_FAMILY,
    "multi-family": PropertyType.MULTI_FAMILY,
    "duplex": PropertyType.MULTI_FAMILY,
    "triplex": PropertyType.MULTI_FAMILY,
    "fourplex": PropertyType.MULTI_FAMILY,
    "condo": PropertyType.CONDO,
    "condominium": PropertyType.CONDO,
    "townhouse": PropertyType.TOWNHOUSE,
    "townhome": PropertyType.TOWNHOUSE,
    "th": PropertyType.TOWNHOUSE,
    "mobile_home": PropertyType.MOBILE_HOME,
    "mobile home": PropertyType.MOBILE_HOME,
    "mh": PropertyType.MOBILE_HOME,
    "manufactured": PropertyType.MOBILE_HOME,
    "land": PropertyType.LAND,
    "lot": PropertyType.LAND,
    "acreage": PropertyType.LAND,
    "commercial": PropertyType.COMMERCIAL,
}


def _normalise_property_type(raw: Optional[str]) -> PropertyType:
    if not raw:
        return PropertyType.UNKNOWN
    key = raw.strip().lower()
    return _TYPE_MAP.get(key, PropertyType.UNKNOWN)


# ─────────────────────────────────────────────────────────────────
# Status normalisation
# ─────────────────────────────────────────────────────────────────
_STATUS_MAP: dict[str, str] = {
    "active": "active",
    "for sale": "active",
    "available": "active",
    "pending": "pending",
    "under contract": "pending",
    "contingent": "pending",
    "sold": "sold",
    "closed": "sold",
    "off_market": "off_market",
    "off market": "off_market",
    "withdrawn": "off_market",
    "cancelled": "off_market",
    "expired": "off_market",
}


def _normalise_status(raw: Optional[str]) -> str:
    if not raw:
        return "unknown"
    return _STATUS_MAP.get(raw.strip().lower(), raw.strip().lower())


# ─────────────────────────────────────────────────────────────────
# Financial KPI derivation
# ─────────────────────────────────────────────────────────────────
def _derive_financials(
    raw: RawPropertyListing,
    cfg: Settings,
) -> dict[str, Optional[float]]:
    """
    Compute property profitability KPIs from raw listing data.

    Financial model:
        Gross Rent Annual      = monthly_rent × 12
        Effective Gross Income = GRA × (1 - vacancy_rate)
        Total OPEX             = EGI × expense_ratio
                                 + hoa_monthly × 12
                                 + property_tax_annual
                                 + insurance_annual
        NOI                    = EGI - Total OPEX
        Cap Rate               = NOI / list_price
        GRM                    = list_price / GRA
        Price/SqFt             = list_price / sqft
        Rent/SqFt              = monthly_rent / sqft
        Cash-on-Cash (est.)    = NOI / (list_price × (down_pmt_pct + closing_cost_pct))
                                 — simplified; assumes 25% down, no debt service

    Args:
        raw: Validated raw listing object.
        cfg: Settings for financial assumption constants.

    Returns:
        Dict of derived metric names → values (or None if uncomputable).
    """
    fin = raw.financials

    list_price: Optional[float] = fin.list_price if fin else None
    rent_monthly: Optional[float] = fin.estimated_rent_monthly if fin else None
    hoa_monthly: Optional[float] = (fin.hoa_monthly or 0.0) if fin else 0.0
    tax_annual: Optional[float] = (fin.property_tax_annual or 0.0) if fin else 0.0
    insurance_annual: Optional[float] = (fin.insurance_annual or 0.0) if fin else 0.0
    sqft: Optional[float] = raw.square_feet

    result: dict[str, Optional[float]] = {
        "gross_rent_annual": None,
        "effective_gross_income": None,
        "total_opex_annual": None,
        "net_operating_income": None,
        "cap_rate": None,
        "gross_rent_multiplier": None,
        "price_per_sqft": None,
        "rent_per_sqft": None,
        "cash_on_cash_estimate": None,
    }

    # --- GRA ---
    if rent_monthly and rent_monthly > 0:
        gra = rent_monthly * 12.0
        result["gross_rent_annual"] = round(gra, 2)
    else:
        return result  # can't compute further without rent

    gra = result["gross_rent_annual"]  # type: ignore[assignment]

    # --- EGI ---
    egi = gra * (1.0 - cfg.default_vacancy_rate)
    result["effective_gross_income"] = round(egi, 2)

    # --- OPEX ---
    # Operating expense ratio covers variable OPEX (mgmt, maintenance, capex reserves)
    # Fixed items (HOA, tax, insurance) are added explicitly if available
    variable_opex = egi * cfg.default_expense_ratio
    fixed_opex = (hoa_monthly or 0.0) * 12.0 + (tax_annual or 0.0) + (insurance_annual or 0.0)
    total_opex = variable_opex + fixed_opex
    result["total_opex_annual"] = round(total_opex, 2)

    # --- NOI ---
    noi = egi - total_opex
    result["net_operating_income"] = round(noi, 2)

    # --- Cap Rate ---
    if list_price and list_price > 0:
        result["cap_rate"] = round(noi / list_price, 6)
        result["gross_rent_multiplier"] = round(list_price / gra, 4)

        # Simplified CoC: assume 25% down + closing costs as invested equity
        equity_invested = list_price * (0.25 + cfg.closing_cost_pct)
        if equity_invested > 0:
            result["cash_on_cash_estimate"] = round(noi / equity_invested, 6)

    # --- Per-SqFt ---
    if sqft and sqft > 0:
        if list_price and list_price > 0:
            result["price_per_sqft"] = round(list_price / sqft, 2)
        if rent_monthly and rent_monthly > 0:
            result["rent_per_sqft"] = round(rent_monthly / sqft, 4)

    return result


# ─────────────────────────────────────────────────────────────────
# Deal flagging
# ─────────────────────────────────────────────────────────────────
def _assign_deal_flag(
    financials: dict[str, Optional[float]],
    cfg: Settings,
) -> str:
    """
    Assign a traffic-light deal flag based on computed KPIs.

    Rules (in priority order):
        UNSCORED — cap rate or NOI could not be computed.
        GREEN    — cap rate >= target_cap_rate_min AND NOI > 0.
        YELLOW   — cap rate >= (target_cap_rate_min × 0.75) AND NOI > 0.
        RED      — NOI <= 0 or cap rate below yellow threshold.

    Args:
        financials: Output dict from `_derive_financials`.
        cfg: Settings for thresholds.

    Returns:
        One of "GREEN", "YELLOW", "RED", "UNSCORED".
    """
    cap_rate = financials.get("cap_rate")
    noi = financials.get("net_operating_income")

    if cap_rate is None or noi is None:
        return "UNSCORED"

    if noi <= 0:
        return "RED"

    if cap_rate >= cfg.target_cap_rate_min:
        return "GREEN"

    if cap_rate >= cfg.target_cap_rate_min * 0.75:
        return "YELLOW"

    return "RED"


# ─────────────────────────────────────────────────────────────────
# Deduplication
# ─────────────────────────────────────────────────────────────────
def _deduplicate(records: list[RawPropertyListing]) -> list[RawPropertyListing]:
    """
    Deduplicate on listing_id.  When duplicates exist, the record with
    the most-recently-parsed `updated_at` string is kept. Ties keep
    the last-seen record (effectively arbitrary but deterministic).

    Args:
        records: Full list of validated raw listings (may contain dupes).

    Returns:
        Deduplicated list.
    """
    seen: dict[str, RawPropertyListing] = {}
    dupe_count = 0

    for rec in records:
        lid = rec.listing_id
        if lid not in seen:
            seen[lid] = rec
        else:
            dupe_count += 1
            # Keep whichever has a later updated_at (compare as strings — ISO sorts correctly)
            existing_ts = seen[lid].updated_at or ""
            incoming_ts = rec.updated_at or ""
            if incoming_ts > existing_ts:
                seen[lid] = rec
                logger.debug("Dedup | kept newer record for listing_id=%s", lid)

    logger.info(
        "Deduplication complete | input=%d unique=%d dupes_removed=%d",
        len(records), len(seen), dupe_count,
    )
    return list(seen.values())


# ─────────────────────────────────────────────────────────────────
# Single record transformation
# ─────────────────────────────────────────────────────────────────
def _transform_record(
    raw: RawPropertyListing,
    cfg: Settings,
    ingested_at: str,
) -> CleanPropertyListing:
    """
    Transform one validated raw listing into a clean, typed output record.

    Args:
        raw: Pydantic-validated raw listing.
        cfg: Settings singleton.
        ingested_at: ISO-8601 UTC timestamp for this pipeline run.

    Returns:
        CleanPropertyListing ready for the loader.
    """
    addr = raw.address
    fin = raw.financials

    financials_kpis = _derive_financials(raw, cfg)
    deal_flag = _assign_deal_flag(financials_kpis, cfg)

    return CleanPropertyListing(
        listing_id=raw.listing_id,
        mls_number=raw.mls_number,
        property_type=_normalise_property_type(raw.property_type),
        status=_normalise_status(raw.status),
        bedrooms=raw.bedrooms,
        bathrooms=raw.bathrooms,
        square_feet=raw.square_feet,
        lot_size_sqft=raw.lot_size_sqft,
        year_built=raw.year_built,
        days_on_market=raw.days_on_market,
        listing_date=_parse_datetime(raw.listing_date),
        updated_at=_parse_datetime(raw.updated_at),
        last_sold_date=_parse_datetime(fin.last_sold_date if fin else None),

        # Address
        street=addr.street if addr else None,
        city=addr.city if addr else None,
        state=addr.state if addr else None,
        zip_code=addr.zip_code if addr else None,
        county=addr.county if addr else None,
        latitude=addr.latitude if addr else None,
        longitude=addr.longitude if addr else None,

        # Raw financials
        list_price=fin.list_price if fin else None,
        estimated_rent_monthly=fin.estimated_rent_monthly if fin else None,
        hoa_monthly=fin.hoa_monthly if fin else None,
        property_tax_annual=fin.property_tax_annual if fin else None,
        insurance_annual=fin.insurance_annual if fin else None,
        last_sold_price=fin.last_sold_price if fin else None,

        # Derived KPIs
        gross_rent_annual=financials_kpis["gross_rent_annual"],
        effective_gross_income=financials_kpis["effective_gross_income"],
        total_opex_annual=financials_kpis["total_opex_annual"],
        net_operating_income=financials_kpis["net_operating_income"],
        cap_rate=financials_kpis["cap_rate"],
        gross_rent_multiplier=financials_kpis["gross_rent_multiplier"],
        price_per_sqft=financials_kpis["price_per_sqft"],
        rent_per_sqft=financials_kpis["rent_per_sqft"],
        cash_on_cash_estimate=financials_kpis["cash_on_cash_estimate"],
        deal_flag=deal_flag,

        source_market=raw.source_market,
        ingested_at=ingested_at,
    )


# ─────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────
def transform(
    raw_records: list[RawPropertyListing],
    cfg: Settings = settings,
) -> tuple[list[CleanPropertyListing], int]:
    """
    Full transformation pass over validated raw records.

    Steps:
      1. Deduplicate.
      2. Transform each record individually, collecting per-record errors.
      3. Log and return clean records plus a failure count.

    Args:
        raw_records: Validated raw listings from extractor.
        cfg: Settings singleton (injectable for testing).

    Returns:
        Tuple of (clean_records, transform_error_count).
    """
    ingested_at = datetime.now(timezone.utc).isoformat()

    logger.info(
        "Transformation starting | input_records=%d | ingested_at=%s",
        len(raw_records), ingested_at,
    )

    deduped = _deduplicate(raw_records)
    clean_records: list[CleanPropertyListing] = []
    error_count = 0

    for raw in deduped:
        try:
            clean = _transform_record(raw, cfg, ingested_at)
            clean_records.append(clean)
        except Exception as exc:
            error_count += 1
            logger.error(
                "Transform error | listing_id=%s | error=%s",
                raw.listing_id, exc,
                exc_info=True,
            )

    # Summary stats
    green = sum(1 for r in clean_records if r.deal_flag == "GREEN")
    yellow = sum(1 for r in clean_records if r.deal_flag == "YELLOW")
    red = sum(1 for r in clean_records if r.deal_flag == "RED")
    unscored = sum(1 for r in clean_records if r.deal_flag == "UNSCORED")
    scorable = [r for r in clean_records if r.cap_rate is not None]
    avg_cap = (
        sum(r.cap_rate for r in scorable) / len(scorable)  # type: ignore[arg-type]
        if scorable else 0.0
    )

    logger.info(
        "Transformation complete | "
        "clean=%d errors=%d | "
        "deal_flags=[GREEN=%d YELLOW=%d RED=%d UNSCORED=%d] | "
        "avg_cap_rate=%.4f",
        len(clean_records), error_count,
        green, yellow, red, unscored,
        avg_cap,
    )

    return clean_records, error_count
