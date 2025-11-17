import datetime as dt
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from db_config import get_sql_engine

# -------------------------
# Cached filter lookups
# -------------------------

# All towns from Towns table for filters.
@st.cache_data(ttl=600)
def load_towns() -> list[str]:   
    engine = get_sql_engine()
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT town_name FROM Towns ORDER BY town_name;")).fetchall()
    return [r[0] for r in rows]

# All distinct flat types from Transactions for filters.
@st.cache_data(ttl=600)
def load_flat_types() -> list[str]:
    engine = get_sql_engine()
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT DISTINCT flat_type FROM Transactions ORDER BY flat_type;")).fetchall()
    return [r[0] for r in rows]

# Min/max month available in Transactions (txn_month).
@st.cache_data(ttl=600)
def load_date_bounds() -> tuple[dt.date | None, dt.date | None]:
    engine = get_sql_engine()
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT MIN(txn_month)::date AS dmin, MAX(txn_month)::date AS dmax FROM Transactions;")
        ).mappings().first()
    dmin = row.get("dmin") if row else None
    dmax = row.get("dmax") if row else None
    return dmin, dmax


# -------------------------
# Data retrieval (filtered)
# -------------------------

def _build_in_clause(column: str, values: list[str], param_prefix: str) -> tuple[str, dict]:
    """
    Safe IN (...) clause builder with bound parameters.
    Returns (clause_sql, params_dict).
    """
    placeholders = []
    params = {}
    for i, v in enumerate(values):
        key = f"{param_prefix}{i}"
        placeholders.append(f":{key}")
        params[key] = v
    clause = f"{column} IN ({', '.join(placeholders)})" if placeholders else "1=1"
    return clause, params


def _run_query(engine, sql_text: str, params: dict) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.DataFrame(conn.execute(text(sql_text), params).mappings())


@st.cache_data(ttl=300, show_spinner=False)
def fetch_transactions(
    towns: tuple[str, ...],
    flat_types: tuple[str, ...],
    date_start: dt.date,
    date_end: dt.date,
) -> pd.DataFrame:
    
    # Pull filtered rows with month, price, flat_type, town, and (if available) area_sqm.
    # Attempts Flats.floor_area_sqm first, falls back to Transactions.floor_area_sqm.
    
    engine = get_sql_engine()

    where_sql = "WHERE t.txn_month BETWEEN :d_start AND :d_end"
    params = {"d_start": date_start, "d_end": date_end}

    # Dynamic filters
    if towns:
        town_clause, town_params = _build_in_clause("tn.town_name", list(towns), "tw_")
        where_sql += f" AND {town_clause}"
        params.update(town_params)

    if flat_types:
        ft_clause, ft_params = _build_in_clause("t.flat_type", list(flat_types), "ft_")
        where_sql += f" AND {ft_clause}"
        params.update(ft_params)

    # Try 1: area from Flats
    sql_flats_area = f"""
        SELECT
            date_trunc('month', t.txn_month)::date AS month,
            t.txn_price::numeric             AS price,
            t.flat_type                      AS flat_type,
            tn.town_name                     AS town,
            f.floor_area_sqm::numeric        AS area_sqm
        FROM Transactions t
        JOIN Flats f ON f.flat_id = t.flat_id
        JOIN Towns tn ON tn.town_id = f.town_id
        {where_sql}
        ORDER BY month ASC;
    """

    # Try 2: area from Transactions
    sql_tx_area = f"""
        SELECT
            date_trunc('month', t.txn_month)::date AS month,
            t.txn_price::numeric             AS price,
            t.flat_type                      AS flat_type,
            tn.town_name                     AS town,
            t.floor_area_sqm::numeric        AS area_sqm
        FROM Transactions t
        JOIN Flats f ON f.flat_id = t.flat_id
        JOIN Towns tn ON tn.town_id = f.town_id
        {where_sql}
        ORDER BY month ASC;
    """

    # Try 3: no area
    sql_no_area = f"""
        SELECT
            date_trunc('month', t.txn_month)::date AS month,
            t.txn_price::numeric             AS price,
            t.flat_type                      AS flat_type,
            tn.town_name                     AS town
        FROM Transactions t
        JOIN Flats f ON f.flat_id = t.flat_id
        JOIN Towns tn ON tn.town_id = f.town_id
        {where_sql}
        ORDER BY month ASC;
    """

    # Execute with graceful fallback
    try:
        df = _run_query(engine, sql_flats_area, params)
    except Exception:
        try:
            df = _run_query(engine, sql_tx_area, params)
        except Exception:
            df = _run_query(engine, sql_no_area, params)

    if df.empty:
        return df

    # Types and derived metrics
    df["month"] = pd.to_datetime(df["month"]).dt.date
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price"])
    df["flat_type"] = df["flat_type"].astype(str)
    df["town"] = df["town"].astype(str)

    if "area_sqm" in df.columns:
        df["area_sqm"] = pd.to_numeric(df["area_sqm"], errors="coerce")
        # Only compute ppsm for valid positive areas
        valid = df["area_sqm"].fillna(0) > 0
        df.loc[valid, "ppsm"] = df.loc[valid, "price"] / df.loc[valid, "area_sqm"]
        # Keep rows without area so non-ppsm views still work
    return df

# --- Main Page Function ---
def app():
    st.title("📊 Analytics Dashboard")
    st.caption("Explore resale price trends, volumes, and distributions with rich filters.")

    # --- Filters row ---
    towns_all = load_towns()
    ftypes_all = load_flat_types()
    dmin, dmax = load_date_bounds()

    if not dmin or not dmax:
        st.warning("No date range detected in the Transactions table.")
        return

    # Full-month bounds
    month_start_min = dt.date(dmin.year, dmin.month, 1)
    month_end_max = (pd.Timestamp(year=dmax.year, month=dmax.month, day=1) + pd.offsets.MonthEnd(0)).date()

    # Safe defaults
    start_default = month_start_min
    end_default = month_end_max

    col1, col2, col3 = st.columns([1.2, 1.2, 1])

    with col1:
        sel_towns = st.multiselect(
            "Town",
            options=towns_all,
            default=[],
            help="Leave empty to include all towns.",
        )

    with col2:
        sel_ftypes = st.multiselect(
            "Flat Type",
            options=ftypes_all,
            default=[],
            help="Leave empty to include all flat types.",
        )

    with col3:
        start = st.date_input("Start month", value=start_default, min_value=month_start_min, max_value=month_end_max)
        end = st.date_input("End month", value=end_default, min_value=month_start_min, max_value=month_end_max)

        # Normalize to month bounds
        start = start.date() if isinstance(start, dt.datetime) else start
        end = end.date() if isinstance(end, dt.datetime) else end
        start = dt.date(start.year, start.month, 1)
        end = (pd.Timestamp(year=end.year, month=end.month, day=1) + pd.offsets.MonthEnd(0)).date()

        if start > end:
            st.error("Start month cannot be after End month.")
            return

    # Fetch data
    df = fetch_transactions(tuple(sel_towns), tuple(sel_ftypes), start, end)
    if df.empty:
        st.info("No transactions found for the selected filters.")
        return

    # -------------------------
    # Advanced: $/sqm filter
    # -------------------------
    ppsm_min, ppsm_max = None, None

    with st.expander("Advanced Filters"):
        if "ppsm" in df.columns:
            # Compute slider range from available rows with valid ppsm
            valid_ppsm = df["ppsm"].dropna()
            if not valid_ppsm.empty:
                ppsm_min = int(valid_ppsm.min())
                ppsm_max = int(valid_ppsm.max())
                ppsm_range = st.slider(
                    "$/sqm range",
                    min_value=ppsm_min,
                    max_value=ppsm_max,
                    value=(ppsm_min, ppsm_max),
                    step=10,
                    help="Filter transactions by computed price per square metre.",
                )
                # Apply $/sqm filter
                lo, hi = ppsm_range
                before = len(df)
                df = df[(df["ppsm"].isna()) | ((df["ppsm"] >= lo) & (df["ppsm"] <= hi))]
                after = len(df)
                ppsm_filter_active = (before != after)
            else:
                st.info("$/sqm cannot be computed because floor areas are missing in the current selection.")
        else:
            st.info("Floor area is not available in the connected tables; $/sqm filter is hidden.")

    # KPIs (always on price; add $/sqm when available)
    c1, c2, c3, c4 = st.columns(4)
    total_tx = len(df)
    med_price = int(df["price"].median()) if not df["price"].empty else 0
    avg_price = int(df["price"].mean()) if not df["price"].empty else 0
    unique_towns = df["town"].nunique()
    c1.metric("Transactions", f"{total_tx:,}")
    c2.metric("Median Price", f"${med_price:,}")
    c3.metric("Average Price", f"${avg_price:,}")
    c4.metric("Towns in View", f"{unique_towns:,}")

    if "ppsm" in df.columns and df["ppsm"].notna().any():
        c5, c6 = st.columns(2)
        med_ppsm = int(df["ppsm"].median())
        avg_ppsm = int(df["ppsm"].mean())
        c5.metric("Median $/sqm", f"${med_ppsm:,}")
        c6.metric("Average $/sqm", f"${avg_ppsm:,}")

    st.divider()

    # Grouping control for line charts
    grp = st.radio(
        "Line chart grouping",
        options=["Overall", "Town", "Flat Type"],
        horizontal=True,
        help="Choose how to split the monthly median line.",
    )

    # Metric control (Price vs $/sqm) – only show if ppsm exists
    metric = "Price"
    if "ppsm" in df.columns and df["ppsm"].notna().any():
        metric = st.radio(
            "Chart metric",
            options=["Price", "$/sqm"],
            horizontal=True,
            index=0,
            help="Switch charts between transaction price and price per sqm.",
        )

    # Choose Y field and labels
    ycol = "price" if metric == "Price" else "ppsm"
    ylabel = "Median Price" if metric == "Price" else "Median $/sqm"

    # Monthly aggregations
    agg_colname = "median_value"
    if grp == "Town":
        df_line = (
            df.groupby(["month", "town"], as_index=False)
              .agg(**{agg_colname: (ycol, "median")})
              .sort_values(["town", "month"])
        )
        color_col = "town"
        df_overall = (
            df.groupby(["month"], as_index=False)
              .agg(median_price=("price", "median"), txn_count=("price", "count"))
              .sort_values("month")
        )
    elif grp == "Flat Type":
        df_line = (
            df.groupby(["month", "flat_type"], as_index=False)
              .agg(**{agg_colname: (ycol, "median")})
              .sort_values(["flat_type", "month"])
        )
        color_col = "flat_type"
        df_overall = (
            df.groupby(["month"], as_index=False)
              .agg(median_price=("price", "median"), txn_count=("price", "count"))
              .sort_values("month")
        )
    else:
        df_line = (
            df.groupby(["month"], as_index=False)
              .agg(**{agg_colname: (ycol, "median")})
              .sort_values("month")
        )
        color_col = None
        df_overall = (
            df.groupby(["month"], as_index=False)
              .agg(median_price=("price", "median"), txn_count=("price", "count"))
              .sort_values("month")
        )

    # Line: monthly median (Price or $/sqm)
    st.subheader(f"Monthly Median ({metric})")
    fig_line = px.line(
        df_line,
        x="month",
        y=agg_colname,
        color=color_col,
        markers=True,
        labels={"month": "Month", agg_colname: ylabel},
        title=f"Median {metric.lower()} over time",
    )
    fig_line.update_traces(mode="lines+markers")
    st.plotly_chart(fig_line, config={'displayModeBar': True}, use_container_width=True)

    # Bars: monthly transaction volume (always count of rows)
    st.subheader("Monthly Transaction Volume")
    fig_bar = px.bar(
        df_overall,
        x="month",
        y="txn_count",
        labels={"month": "Month", "txn_count": "Transactions"},
        title="Transactions per month",
    )
    st.plotly_chart(fig_bar, config={'displayModeBar': True}, use_container_width=True)

    st.divider()

    # Distributions
    colA, colB = st.columns(2)

    with colA:
        st.subheader(f"{metric} Distribution")
        fig_hist = px.histogram(
            df.dropna(subset=[ycol]),
            x=ycol,
            nbins=40,
            color=(color_col if color_col in df.columns else None),
            labels={ycol: metric},
            title=f"Histogram of {metric.lower()}",
        )
        st.plotly_chart(fig_hist, config={'displayModeBar': True}, use_container_width=True)

    with colB:
        st.subheader(f"{metric} by Category")
        # Pick category consistent with grouping when possible
        if color_col == "town":
            cat = "town"
            title = f"{metric} by Town"
        elif color_col == "flat_type":
            cat = "flat_type"
            title = f"{metric} by Flat Type"
        else:
            # Default to flat_type for categorical box if diverse
            cat = "flat_type" if df["flat_type"].nunique() > 1 else "town"
            title = f"{metric} by {cat.replace('_', ' ').title()}"

        fig_box = px.box(
            df.dropna(subset=[ycol]),
            x=cat,
            y=ycol,
            points=False,
            labels={cat: cat.replace("_", " ").title(), ycol: metric},
            title=title,
        )
        st.plotly_chart(fig_box, config={'displayModeBar': True}, use_container_width=True)

    st.divider()

    # Optional: Tabular view
    with st.expander("Show aggregated tables"):
        st.write("Monthly medians and volumes (price-based)")
        st.dataframe(df_overall, width='stretch')
        st.write(f"Grouped monthly medians ({metric})")
        st.dataframe(df_line, width='stretch')
        st.write("Sample of raw filtered transactions")
        cols = ["month", "town", "flat_type", "price"]
        if "area_sqm" in df.columns:
            cols += ["area_sqm"]
        if "ppsm" in df.columns:
            cols += ["ppsm"]
        st.dataframe(df[cols].head(1000), width='stretch')
