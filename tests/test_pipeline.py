"""
tests/test_pipeline.py — Unit tests for the RealtyETL pipeline modules.

Run with:
    pytest tests/ -v
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

# ─────────────────────────────────────────────────────────────────
# Transformer tests
# ─────────────────────────────────────────────────────────────────
class TestDateParsing:
    """Verify _parse_datetime handles all messy formats correctly."""

    def setup_method(self):
        from pipeline.transformer import _parse_datetime
        self.parse = _parse_datetime

    def test_iso_z(self):
        result = self.parse("2024-03-15T14:30:00Z")
        assert result is not None
        assert "2024-03-15" in result

    def test_slash_us_format(self):
        result = self.parse("03/15/2024")
        assert result is not None
        assert "2024-03-15" in result

    def test_day_month_year_text(self):
        result = self.parse("15 Mar 2024")
        assert result is not None
        assert "2024-03-15" in result

    def test_long_month_name(self):
        result = self.parse("March 15, 2024")
        assert result is not None
        assert "2024" in result

    def test_compact_date(self):
        result = self.parse("20240315")
        assert result is not None
        assert "2024-03-15" in result

    def test_none_returns_none(self):
        assert self.parse(None) is None

    def test_empty_string_returns_none(self):
        assert self.parse("") is None

    def test_garbage_string_returns_none(self):
        assert self.parse("not-a-date-at-all-xyz") is None

    def test_na_sentinel_returns_none(self):
        assert self.parse("N/A") is None


class TestPropertyTypeNormalisation:
    def setup_method(self):
        from pipeline.transformer import _normalise_property_type
        from pipeline.config import PropertyType
        self.normalise = _normalise_property_type
        self.PT = PropertyType

    def test_sfr_variants(self):
        for v in ("single_family", "SFR", "Single Family", "single-family", "sf"):
            assert self.normalise(v) == self.PT.SINGLE_FAMILY, f"Failed for: {v}"

    def test_mf_variants(self):
        for v in ("multi_family", "MF", "Duplex", "fourplex", "Multi Family"):
            assert self.normalise(v) == self.PT.MULTI_FAMILY, f"Failed for: {v}"

    def test_mobile_home_variants(self):
        for v in ("mobile_home", "Mobile Home", "MH", "Manufactured"):
            assert self.normalise(v) == self.PT.MOBILE_HOME, f"Failed for: {v}"

    def test_none_returns_unknown(self):
        assert self.normalise(None) == self.PT.UNKNOWN

    def test_gibberish_returns_unknown(self):
        assert self.normalise("xyz_not_a_type") == self.PT.UNKNOWN


class TestStatusNormalisation:
    def setup_method(self):
        from pipeline.transformer import _normalise_status
        self.normalise = _normalise_status

    def test_active_variants(self):
        for v in ("active", "Active", "ACTIVE", "for sale", "available"):
            assert self.normalise(v) == "active", f"Failed for: {v}"

    def test_pending_variants(self):
        for v in ("pending", "Pending", "under contract", "contingent"):
            assert self.normalise(v) == "pending", f"Failed for: {v}"

    def test_sold_variants(self):
        for v in ("sold", "SOLD", "closed", "Closed"):
            assert self.normalise(v) == "sold", f"Failed for: {v}"

    def test_none_returns_unknown(self):
        assert self.normalise(None) == "unknown"


class TestFinancialKPIs:
    """Validate KPI derivation math."""

    def _make_raw(self, list_price, rent_monthly, hoa=0, tax=0, insurance=0):
        from pipeline.config import RawPropertyListing, RawFinancials, RawAddress, Settings
        return RawPropertyListing(
            listing_id=str(uuid.uuid4()),
            property_type="single_family",
            status="active",
            square_feet=1500.0,
            financials=RawFinancials(
                list_price=list_price,
                estimated_rent_monthly=rent_monthly,
                hoa_monthly=hoa,
                property_tax_annual=tax,
                insurance_annual=insurance,
            ),
            address=RawAddress(city="Nashville", state="TN"),
        )

    def test_basic_cap_rate(self):
        from pipeline.transformer import _derive_financials
        from pipeline.config import settings

        # Simple case: $200k property, $1,600/mo rent
        raw = self._make_raw(list_price=200_000, rent_monthly=1_600)
        result = _derive_financials(raw, settings)

        # GRA = 1600 * 12 = 19200
        assert result["gross_rent_annual"] == pytest.approx(19_200.0, rel=0.01)
        # EGI = 19200 * (1 - 0.08) = 17664
        assert result["effective_gross_income"] == pytest.approx(17_664.0, rel=0.01)
        # NOI = EGI - (EGI * 0.40) = EGI * 0.60 = 10598.4
        assert result["net_operating_income"] == pytest.approx(10_598.4, rel=0.01)
        # Cap rate = NOI / price = 10598.4 / 200000 ≈ 5.3%
        assert result["cap_rate"] == pytest.approx(0.05299, rel=0.01)

    def test_no_rent_returns_nones(self):
        from pipeline.transformer import _derive_financials
        from pipeline.config import settings

        raw = self._make_raw(list_price=200_000, rent_monthly=None)
        result = _derive_financials(raw, settings)
        assert result["cap_rate"] is None
        assert result["gross_rent_annual"] is None

    def test_no_price_no_cap_rate(self):
        from pipeline.transformer import _derive_financials
        from pipeline.config import settings

        raw = self._make_raw(list_price=None, rent_monthly=1_500)
        result = _derive_financials(raw, settings)
        assert result["gross_rent_annual"] is not None  # can still compute
        assert result["cap_rate"] is None               # can't compute without price


class TestDealFlagging:
    def setup_method(self):
        from pipeline.transformer import _assign_deal_flag
        from pipeline.config import settings
        self.flag = _assign_deal_flag
        self.cfg = settings

    def test_green_high_cap(self):
        financials = {"cap_rate": 0.09, "net_operating_income": 12_000}
        assert self.flag(financials, self.cfg) == "GREEN"

    def test_yellow_mid_cap(self):
        financials = {"cap_rate": 0.048, "net_operating_income": 8_000}
        assert self.flag(financials, self.cfg) == "YELLOW"

    def test_red_negative_noi(self):
        financials = {"cap_rate": 0.05, "net_operating_income": -500}
        assert self.flag(financials, self.cfg) == "RED"

    def test_unscored_missing_cap(self):
        financials = {"cap_rate": None, "net_operating_income": 5_000}
        assert self.flag(financials, self.cfg) == "UNSCORED"


class TestDeduplication:
    def setup_method(self):
        from pipeline.transformer import _deduplicate
        self.dedup = _deduplicate

    def _make_listing(self, lid: str, updated_at: str | None):
        from pipeline.config import RawPropertyListing
        return RawPropertyListing(listing_id=lid, updated_at=updated_at)

    def test_no_dupes_unchanged(self):
        records = [
            self._make_listing("A", "2024-01-01"),
            self._make_listing("B", "2024-01-02"),
        ]
        result = self.dedup(records)
        assert len(result) == 2

    def test_keeps_newer(self):
        records = [
            self._make_listing("A", "2024-01-01T00:00:00"),
            self._make_listing("A", "2024-06-01T00:00:00"),  # newer
        ]
        result = self.dedup(records)
        assert len(result) == 1
        assert result[0].updated_at == "2024-06-01T00:00:00"

    def test_handles_none_timestamps(self):
        records = [
            self._make_listing("A", None),
            self._make_listing("A", "2024-01-01"),
        ]
        result = self.dedup(records)
        assert len(result) == 1


# ─────────────────────────────────────────────────────────────────
# Extractor tests
# ─────────────────────────────────────────────────────────────────
class TestRawAddressCoercion:
    def test_zip_zero_pads(self):
        from pipeline.config import RawAddress
        addr = RawAddress(zip_code="1234")
        assert addr.zip_code == "01234"

    def test_zip_truncates(self):
        from pipeline.config import RawAddress
        addr = RawAddress(zip_code="123456789")
        assert addr.zip_code == "12345"

    def test_state_uppercased(self):
        from pipeline.config import RawAddress
        addr = RawAddress(state="tn")
        assert addr.state == "TN"


class TestRawFinancialsCoercion:
    def test_dollar_string_parsed(self):
        from pipeline.config import RawFinancials
        fin = RawFinancials(list_price="$250,000.00")
        assert fin.list_price == pytest.approx(250_000.0)

    def test_na_sentinel_becomes_none(self):
        from pipeline.config import RawFinancials
        fin = RawFinancials(list_price="N/A")
        assert fin.list_price is None

    def test_numeric_string_parsed(self):
        from pipeline.config import RawFinancials
        fin = RawFinancials(estimated_rent_monthly="1450")
        assert fin.estimated_rent_monthly == pytest.approx(1_450.0)


class TestValidateRawRecords:
    def test_valid_records_pass(self):
        from pipeline.extractor import _validate_raw_records
        records = [
            {
                "listing_id": str(uuid.uuid4()),
                "property_type": "single_family",
                "status": "active",
            }
            for _ in range(5)
        ]
        valid, rejected = _validate_raw_records(records)
        assert len(valid) == 5
        assert len(rejected) == 0

    def test_missing_id_rejected(self):
        from pipeline.extractor import _validate_raw_records
        records = [{"property_type": "condo", "status": "active"}]  # no listing_id
        valid, rejected = _validate_raw_records(records)
        assert len(valid) == 0
        assert len(rejected) == 1
        assert "_validation_error" in rejected[0]

    def test_empty_id_rejected(self):
        from pipeline.extractor import _validate_raw_records
        records = [{"listing_id": "  ", "status": "active"}]
        valid, rejected = _validate_raw_records(records)
        assert len(valid) == 0
        assert len(rejected) == 1


# ─────────────────────────────────────────────────────────────────
# Integration test — full ETL pass
# ─────────────────────────────────────────────────────────────────
class TestIntegrationETLPass:
    """End-to-end: extract → transform → load → query."""

    def test_full_pass(self, tmp_path):
        import duckdb
        from pipeline.config import Settings
        from pipeline.extractor import extract
        from pipeline.loader import get_connection, initialise_schema, upsert_listings
        from pipeline.transformer import transform

        cfg = Settings(
            target_markets=["Nashville, TN"],
            api_page_size=20,
            raw_dump_enabled=False,
            db_path=tmp_path / "test.duckdb",
        )

        raw_records, fetched, rejected = extract(cfg)
        assert fetched > 0
        assert len(raw_records) > 0

        clean_records, errors = transform(raw_records, cfg)
        assert len(clean_records) > 0

        con = get_connection(cfg)
        initialise_schema(con)
        upserted = upsert_listings(con, clean_records)
        assert upserted == len(clean_records)

        count = con.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        assert count == upserted

        # Verify KPIs present
        cap_rates = con.execute(
            "SELECT cap_rate FROM listings WHERE cap_rate IS NOT NULL LIMIT 10"
        ).fetchall()
        assert len(cap_rates) > 0

        con.close()
