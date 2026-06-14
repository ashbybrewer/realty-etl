"""
app.py — Streamlit analytics dashboard for RealtyETL.

Provides:
  - KPI scorecards: total listings, green deals, avg cap rate, avg NOI
  - Deal funnel bar chart (GREEN / YELLOW / RED by market)
  - Top deals table (sortable, filterable)
  - Market comparison chart (cap rate vs median price by market)
  - Price bucket distribution
  - Pipeline run history log
  - On-demand ETL trigger button

Run:
    streamlit run app.py
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Optional

import duckdb
import pandas as pd
import streamlit as st

# ─────────────────────────────────────────────────────────────────
# Page config — must be first Streamlit call
# ─────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="RealtyETL · Property Profit Intelligence",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────
# Custom CSS — dark data-room aesthetic
# ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    /* ── Base ── */
    .stApp {
        background-color: #0e1117;
        color: #e2e8f0;
    }
    .main .block-container {
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }

    /* ── Metric cards ── */
    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, #1a1f2e 0%, #12172a 100%);
        border: 1px solid #2d3748;
        border-radius: 10px;
        padding: 1rem 1.2rem;
        box-shadow: 0 2px 12px rgba(0,0,0,0.4);
    }
    div[data-testid="metric-container"] label {
        color: #718096 !important;
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        color: #63b3ed !important;
        font-size: 1.8rem !important;
        font-weight: 700 !important;
    }
    div[data-testid="metric-container"] div[data-testid="stMetricDelta"] {
        color: #68d391 !important;
    }

    /* ── Section headers ── */
    .section-header {
        font-size: 0.70rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: #4a5568;
        border-bottom: 1px solid #2d3748;
        padding-bottom: 6px;
        margin-bottom: 1rem;
        margin-top: 1.5rem;
    }

    /* ── Deal flag badges ── */
    .badge-green  { background:#22543d; color:#9ae6b4; padding:2px 10px; border-radius:12px; font-size:0.78rem; font-weight:700; }
    .badge-yellow { background:#744210; color:#fbd38d; padding:2px 10px; border-radius:12px; font-size:0.78rem; font-weight:700; }
    .badge-red    { background:#742a2a; color:#feb2b2; padding:2px 10px; border-radius:12px; font-size:0.78rem; font-weight:700; }
    .badge-grey   { background:#2d3748; color:#a0aec0; padding:2px 10px; border-radius:12px; font-size:0.78rem; font-weight:700; }

    /* ── Sidebar ── */
    section[data-testid="stSidebar"] {
        background-color: #0d1117;
        border-right: 1px solid #1e2532;
    }
    section[data-testid="stSidebar"] h1,
    section[data-testid="stSidebar"] h2,
    section[data-testid="stSidebar"] h3 {
        color: #90cdf4;
    }

    /* ── Tables ── */
    .stDataFrame { border: 1px solid #2d3748; border-radius: 8px; }

    /* ── Alert boxes ── */
    .alert-box {
        background: #1a202c;
        border-left: 4px solid #63b3ed;
        border-radius: 0 8px 8px 0;
        padding: 0.75rem 1rem;
        margin: 0.5rem 0;
        font-size: 0.88rem;
        color: #e2e8f0;
    }
    .alert-box.green  { border-left-color: #68d391; }
    .alert-box.yellow { border-left-color: #f6e05e; }
    .alert-box.red    { border-left-color: #fc8181; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────
# Imports (after st.set_page_config)
# ─────────────────────────────────────────────────────────────────
try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    st.warning("plotly not installed — charts will be limited. Run: pip install plotly")

from pipeline.config import DB_PATH, settings
from pipeline.loader import get_connection, initialise_schema


# ─────────────────────────────────────────────────────────────────
# Database helpers
# ─────────────────────────────────────────────────────────────────
@st.cache_resource(ttl=120, show_spinner=False)
def _get_db_connection() -> duckdb.DuckDBPyConnection:
    """Cached, read-only-safe DuckDB connection for the dashboard."""
    con = get_connection(settings)
    initialise_schema(con)
    return con


def _query(sql: str, con: Optional[duckdb.DuckDBPyConnection] = None) -> pd.DataFrame:
    """Execute a SQL query and return a DataFrame. Handles connection lifecycle."""
    if con is None:
        con = _get_db_connection()
    return con.execute(sql).df()


def _scalar(sql: str) -> object:
    return _get_db_connection().execute(sql).fetchone()[0]


# ─────────────────────────────────────────────────────────────────
# Pipeline trigger
# ─────────────────────────────────────────────────────────────────
def _run_pipeline_in_app() -> None:
    """Run the ETL pipeline synchronously from within the dashboard."""
    from pipeline.extractor import extract
    from pipeline.loader import load
    from pipeline.transformer import transform

    with st.spinner("Running ETL pipeline… (this may take 15–30 seconds)"):
        progress = st.progress(0, text="Extracting data…")
        raw_records, fetched, rejected = extract(settings)
        progress.progress(33, text="Transforming data…")
        clean_records, transform_errors = transform(raw_records, settings)
        progress.progress(66, text="Loading data…")
        metrics = load(
            clean_records=clean_records,
            records_fetched=fetched,
            records_rejected=rejected,
            transform_errors=transform_errors,
            cfg=settings,
        )
        progress.progress(100, text="Done.")
        time.sleep(0.5)
        progress.empty()

    if metrics.get("status") in ("SUCCESS", "PARTIAL"):
        st.success(
            f"✅ Pipeline complete | "
            f"Upserted: {metrics['upserted']:,} | "
            f"Duration: {metrics['duration_seconds']:.1f}s | "
            f"Status: {metrics['status']}"
        )
        st.cache_resource.clear()
        st.rerun()
    else:
        st.error(f"❌ Pipeline failed: {metrics.get('error_message')}")


# ─────────────────────────────────────────────────────────────────
# Chart helpers
# ─────────────────────────────────────────────────────────────────
_FLAG_COLORS = {
    "GREEN": "#68d391",
    "YELLOW": "#f6e05e",
    "RED": "#fc8181",
    "UNSCORED": "#718096",
}

_PLOTLY_LAYOUT = dict(
    paper_bgcolor="#0e1117",
    plot_bgcolor="#0e1117",
    font_color="#e2e8f0",
    font_size=12,
    margin=dict(l=10, r=10, t=35, b=10),
    legend=dict(
        bgcolor="#1a1f2e",
        bordercolor="#2d3748",
        borderwidth=1,
    ),
)


def _deal_funnel_chart(df: pd.DataFrame) -> "go.Figure":
    """Stacked bar: deal flags per market."""
    fig = px.bar(
        df,
        x="source_market",
        y="count",
        color="deal_flag",
        color_discrete_map=_FLAG_COLORS,
        title="Deal Funnel by Market",
        labels={"source_market": "Market", "count": "Listings", "deal_flag": "Flag"},
        barmode="stack",
    )
    fig.update_layout(**_PLOTLY_LAYOUT)
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="#2d3748")
    return fig


def _cap_rate_scatter(df: pd.DataFrame) -> "go.Figure":
    """Bubble: median price vs avg cap rate, sized by listing count."""
    fig = px.scatter(
        df,
        x="median_list_price",
        y="avg_cap_rate_pct",
        size="total_listings",
        color="source_market",
        hover_name="source_market",
        title="Market Comparison: Median Price vs Avg Cap Rate",
        labels={
            "median_list_price": "Median List Price ($)",
            "avg_cap_rate_pct": "Avg Cap Rate (%)",
            "total_listings": "Total Listings",
        },
        size_max=55,
    )
    fig.update_layout(**_PLOTLY_LAYOUT)
    fig.add_hline(
        y=settings.target_cap_rate_min * 100,
        line_dash="dash",
        line_color="#63b3ed",
        annotation_text=f"Target cap rate ({settings.target_cap_rate_min*100:.0f}%)",
        annotation_font_color="#63b3ed",
    )
    return fig


def _price_bucket_chart(df: pd.DataFrame, market: str) -> "go.Figure":
    mdf = df[df["source_market"] == market] if market != "All Markets" else df
    if market != "All Markets":
        grouped = mdf.groupby("price_bucket")["count"].sum().reset_index()
    else:
        grouped = mdf.groupby("price_bucket")["count"].sum().reset_index()

    fig = px.bar(
        grouped,
        x="price_bucket",
        y="count",
        title=f"Price Distribution — {market}",
        labels={"price_bucket": "Price Range", "count": "Listings"},
        color_discrete_sequence=["#4a90d9"],
    )
    fig.update_layout(**_PLOTLY_LAYOUT)
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(showgrid=True, gridcolor="#2d3748")
    return fig


def _noi_histogram(df: pd.DataFrame) -> "go.Figure":
    fig = px.histogram(
        df[df["net_operating_income"].notna()],
        x="net_operating_income",
        nbins=40,
        title="NOI Distribution (Active Listings)",
        labels={"net_operating_income": "Net Operating Income ($)"},
        color_discrete_sequence=["#63b3ed"],
    )
    fig.add_vline(x=0, line_dash="dash", line_color="#fc8181", annotation_text="Break-even")
    fig.update_layout(**_PLOTLY_LAYOUT)
    return fig


# ─────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────
def _render_sidebar() -> dict:
    """Render sidebar controls and return filter selections."""
    with st.sidebar:
        st.markdown("## 🏠 RealtyETL")
        st.markdown(
            "<span style='color:#718096;font-size:0.78rem;'>Property Profit Intelligence</span>",
            unsafe_allow_html=True,
        )
        st.divider()

        # Pipeline controls
        st.markdown("### Pipeline")
        if st.button("▶ Run ETL Now", use_container_width=True, type="primary"):
            _run_pipeline_in_app()

        last_run_df = _query("""
            SELECT started_at, status, records_upserted
            FROM pipeline_run_log
            ORDER BY started_at DESC
            LIMIT 1
        """)
        if not last_run_df.empty:
            row = last_run_df.iloc[0]
            ts = str(row["started_at"])[:19]
            st.caption(f"Last run: {ts} UTC")
            color = "#68d391" if row["status"] == "SUCCESS" else "#f6e05e" if row["status"] == "PARTIAL" else "#fc8181"
            st.markdown(
                f"<span style='color:{color};font-weight:700;'>{row['status']}</span> "
                f"· {int(row['records_upserted']):,} records",
                unsafe_allow_html=True,
            )

        st.divider()

        # Filters
        st.markdown("### Filters")
        try:
            markets = _query("SELECT DISTINCT source_market FROM listings WHERE source_market IS NOT NULL ORDER BY 1")
            market_options = ["All Markets"] + markets["source_market"].tolist()
        except Exception:
            market_options = ["All Markets"]

        selected_market = st.selectbox("Market", market_options)

        prop_types = ["All Types", "single_family", "multi_family", "condo",
                      "townhouse", "mobile_home", "unknown"]
        selected_type = st.selectbox("Property Type", prop_types)

        deal_flags = st.multiselect(
            "Deal Flags",
            options=["GREEN", "YELLOW", "RED", "UNSCORED"],
            default=["GREEN", "YELLOW"],
        )

        min_cap, max_cap = st.slider(
            "Cap Rate Range (%)",
            min_value=0.0, max_value=25.0,
            value=(0.0, 25.0), step=0.5,
        )

        max_price = st.number_input(
            "Max List Price ($)", value=1_000_000, step=50_000, format="%d"
        )

        st.divider()
        st.markdown(
            "<span style='color:#4a5568;font-size:0.7rem;'>"
            f"DB: {DB_PATH.name}<br>"
            f"Version: 1.0.0"
            "</span>",
            unsafe_allow_html=True,
        )

    return {
        "market": selected_market,
        "prop_type": selected_type,
        "deal_flags": deal_flags,
        "min_cap": min_cap / 100.0,
        "max_cap": max_cap / 100.0,
        "max_price": max_price,
    }


# ─────────────────────────────────────────────────────────────────
# Main dashboard
# ─────────────────────────────────────────────────────────────────
def main() -> None:
    filters = _render_sidebar()

    # Check if DB has any data
    try:
        row_count = _scalar("SELECT COUNT(*) FROM listings")
    except Exception:
        row_count = 0

    if row_count == 0:
        st.markdown("## 🏠 RealtyETL · Property Profit Intelligence")
        st.info(
            "No data found in the database. Click **▶ Run ETL Now** in the sidebar to "
            "ingest property listings and populate the dashboard.",
            icon="ℹ️",
        )
        return

    # ── Build WHERE clause from filters ──────────────────────────
    where_parts = ["list_price > 0"]
    if filters["market"] != "All Markets":
        where_parts.append(f"source_market = '{filters['market']}'")
    if filters["prop_type"] != "All Types":
        where_parts.append(f"property_type = '{filters['prop_type']}'")
    if filters["deal_flags"]:
        flags_sql = ", ".join(f"'{f}'" for f in filters["deal_flags"])
        where_parts.append(f"deal_flag IN ({flags_sql})")
    where_parts.append(f"(cap_rate IS NULL OR cap_rate BETWEEN {filters['min_cap']} AND {filters['max_cap']})")
    where_parts.append(f"list_price <= {filters['max_price']}")
    where_clause = "WHERE " + " AND ".join(where_parts)

    # ── Page header ───────────────────────────────────────────────
    col_title, col_ts = st.columns([5, 1])
    with col_title:
        market_label = filters["market"] if filters["market"] != "All Markets" else "All Markets"
        st.markdown(f"## 🏠 Property Profit Intelligence — {market_label}")
    with col_ts:
        st.markdown(
            f"<div style='text-align:right;color:#4a5568;font-size:0.75rem;padding-top:1.2rem;'>"
            f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC</div>",
            unsafe_allow_html=True,
        )

    # ── KPI Scorecards ────────────────────────────────────────────
    st.markdown("<div class='section-header'>Portfolio Overview</div>", unsafe_allow_html=True)

    kpi_df = _query(f"""
        SELECT
            COUNT(*)                                           AS total_listings,
            COUNT(*) FILTER (WHERE deal_flag = 'GREEN')       AS green_deals,
            COUNT(*) FILTER (WHERE deal_flag = 'YELLOW')      AS yellow_deals,
            COUNT(*) FILTER (WHERE deal_flag = 'RED')         AS red_deals,
            ROUND(AVG(cap_rate) * 100, 2)                     AS avg_cap_rate_pct,
            ROUND(AVG(net_operating_income), 0)               AS avg_noi,
            ROUND(MEDIAN(list_price), 0)                      AS median_price,
            ROUND(AVG(gross_rent_multiplier), 2)              AS avg_grm,
            ROUND(AVG(days_on_market), 1)                     AS avg_dom,
            ROUND(AVG(estimated_rent_monthly), 0)             AS avg_rent
        FROM listings
        {where_clause}
    """)

    if kpi_df.empty or kpi_df.iloc[0]["total_listings"] == 0:
        st.warning("No listings match the current filters. Adjust filters in the sidebar.")
        return

    k = kpi_df.iloc[0]

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.metric("Total Listings", f"{int(k['total_listings']):,}")
    with c2:
        st.metric("🟢 Green Deals", f"{int(k['green_deals']):,}")
    with c3:
        st.metric("🟡 Yellow Deals", f"{int(k['yellow_deals']):,}")
    with c4:
        st.metric("Avg Cap Rate", f"{k['avg_cap_rate_pct'] or 0:.2f}%")
    with c5:
        st.metric("Avg NOI / yr", f"${(k['avg_noi'] or 0):,.0f}")
    with c6:
        st.metric("Median Price", f"${(k['median_price'] or 0):,.0f}")

    c7, c8, c9, _ = st.columns(4)
    with c7:
        st.metric("Avg Rent/mo", f"${(k['avg_rent'] or 0):,.0f}")
    with c8:
        st.metric("Avg GRM", f"{k['avg_grm'] or 0:.2f}×")
    with c9:
        st.metric("Avg Days on Market", f"{k['avg_dom'] or 0:.0f}")

    # ── Cap Rate Alert Triggers ───────────────────────────────────
    st.markdown("<div class='section-header'>Active Deal Alerts</div>", unsafe_allow_html=True)
    alert_df = _query(f"""
        SELECT
            COALESCE(street || ', ' || city || ', ' || state, city || ', ' || state) AS address,
            property_type, list_price, estimated_rent_monthly,
            ROUND(cap_rate * 100, 2) AS cap_rate_pct,
            ROUND(cash_on_cash_estimate * 100, 2) AS coc_pct,
            net_operating_income,
            deal_flag, source_market, days_on_market
        FROM listings
        {where_clause}
          AND status = 'active'
          AND deal_flag = 'GREEN'
          AND cap_rate IS NOT NULL
        ORDER BY cap_rate DESC
        LIMIT 5
    """)

    if alert_df.empty:
        st.markdown(
            "<div class='alert-box'>No GREEN deals match current filters.</div>",
            unsafe_allow_html=True,
        )
    else:
        for _, row in alert_df.iterrows():
            st.markdown(
                f"<div class='alert-box green'>"
                f"🟢 <b>{row['address']}</b> — {row['property_type']} · "
                f"${row['list_price']:,.0f} list · "
                f"<b>{row['cap_rate_pct']:.2f}% cap</b> · "
                f"CoC: {row['coc_pct'] or 0:.1f}% · "
                f"NOI: ${row['net_operating_income']:,.0f}/yr · "
                f"{int(row['days_on_market'] or 0)}d on market"
                f"</div>",
                unsafe_allow_html=True,
            )

    # ── Charts row 1 ─────────────────────────────────────────────
    st.markdown("<div class='section-header'>Market Analytics</div>", unsafe_allow_html=True)
    if PLOTLY_AVAILABLE:
        col_left, col_right = st.columns(2)

        with col_left:
            funnel_df = _query("""
                SELECT source_market, deal_flag, COUNT(*) AS count
                FROM listings
                WHERE list_price > 0
                GROUP BY source_market, deal_flag
            """)
            st.plotly_chart(_deal_funnel_chart(funnel_df), use_container_width=True)

        with col_right:
            mkt_df = _query("SELECT * FROM vw_market_summary")
            st.plotly_chart(_cap_rate_scatter(mkt_df), use_container_width=True)

    # ── Charts row 2 ─────────────────────────────────────────────
    if PLOTLY_AVAILABLE:
        col_l2, col_r2 = st.columns(2)

        with col_l2:
            price_df = _query("SELECT * FROM vw_price_distribution")
            selected_mkt_for_dist = filters["market"]
            avail_mkts = ["All Markets"] + price_df["source_market"].unique().tolist()
            st.plotly_chart(
                _price_bucket_chart(price_df, selected_mkt_for_dist),
                use_container_width=True,
            )

        with col_r2:
            noi_df = _query(f"""
                SELECT net_operating_income
                FROM listings
                {where_clause}
                  AND status = 'active'
            """)
            st.plotly_chart(_noi_histogram(noi_df), use_container_width=True)

    # ── Top Deals Table ───────────────────────────────────────────
    st.markdown("<div class='section-header'>Top Deals — Detailed View</div>", unsafe_allow_html=True)

    top_deals_df = _query(f"""
        SELECT
            COALESCE(street || ', ' || city || ', ' || state, city || ', ' || state,
                     listing_id)                                              AS address,
            property_type                                                     AS type,
            status,
            CAST(bedrooms AS VARCHAR) || 'bd/' || CAST(bathrooms AS VARCHAR) || 'ba' AS beds_baths,
            ROUND(square_feet, 0)                                             AS sqft,
            year_built,
            source_market                                                     AS market,
            list_price,
            estimated_rent_monthly                                            AS rent_mo,
            ROUND(cap_rate * 100, 2)                                         AS cap_pct,
            ROUND(gross_rent_multiplier, 2)                                  AS grm,
            ROUND(net_operating_income, 0)                                   AS noi,
            ROUND(cash_on_cash_estimate * 100, 2)                            AS coc_pct,
            ROUND(price_per_sqft, 0)                                         AS price_sqft,
            deal_flag,
            days_on_market                                                    AS dom
        FROM listings
        {where_clause}
          AND status = 'active'
        ORDER BY cap_rate DESC NULLS LAST
        LIMIT 100
    """)

    def _style_flag(val: str) -> str:
        color_map = {
            "GREEN": "color:#68d391;font-weight:700;",
            "YELLOW": "color:#f6e05e;font-weight:700;",
            "RED": "color:#fc8181;font-weight:700;",
            "UNSCORED": "color:#718096;",
        }
        return color_map.get(val, "")

    styled = top_deals_df.style.applymap(_style_flag, subset=["deal_flag"])
    styled = styled.format({
        "list_price": "${:,.0f}",
        "rent_mo": "${:,.0f}",
        "noi": "${:,.0f}",
        "sqft": "{:,.0f}",
        "cap_pct": "{:.2f}%",
        "coc_pct": "{:.2f}%",
    }, na_rep="—")

    st.dataframe(
        styled,
        use_container_width=True,
        height=400,
        hide_index=True,
    )

    # ── Market Summary Table ──────────────────────────────────────
    st.markdown("<div class='section-header'>Market Summary</div>", unsafe_allow_html=True)
    mkt_summary = _query("SELECT * FROM vw_market_summary")
    st.dataframe(
        mkt_summary.style.format({
            "avg_list_price": "${:,.0f}",
            "median_list_price": "${:,.0f}",
            "avg_rent_monthly": "${:,.0f}",
            "avg_cap_rate_pct": "{:.2f}%",
            "avg_noi": "${:,.0f}",
        }, na_rep="—"),
        use_container_width=True,
        hide_index=True,
    )

    # ── Pipeline Run Log ──────────────────────────────────────────
    with st.expander("📋 Pipeline Run History", expanded=False):
        run_log = _query("""
            SELECT
                run_id,
                started_at,
                finished_at,
                records_fetched,
                records_clean,
                records_upserted,
                records_rejected,
                markets,
                status,
                error_message
            FROM pipeline_run_log
            ORDER BY started_at DESC
            LIMIT 20
        """)
        if run_log.empty:
            st.info("No pipeline runs recorded yet.")
        else:
            st.dataframe(run_log, use_container_width=True, hide_index=True)

    # ── Export ────────────────────────────────────────────────────
    with st.expander("⬇ Export Data", expanded=False):
        export_df = _query(f"SELECT * FROM listings {where_clause} ORDER BY cap_rate DESC NULLS LAST")
        csv = export_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download filtered listings as CSV",
            data=csv,
            file_name=f"realty_etl_export_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True,
        )


# ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
