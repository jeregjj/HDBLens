# views/watchlist.py

import os
import datetime as dt

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from db_config import get_sql_engine, get_mongo_collection

# -------------------------
# Config / connections
# -------------------------

WATCH_COLL_NAME = os.getenv("MONGO_WATCHLIST_COLLECTION", "watchlists")

def _watch_coll():
    coll = get_mongo_collection(WATCH_COLL_NAME)
    if coll is None:
        st.error("MongoDB watchlist collection is unavailable. Check your Mongo configuration.")
        st.stop()
    return coll

def _require_login():
    if not st.session_state.get("logged_in"):
        st.error("Please log in to use your Watchlist.")
        st.stop()

# -------------------------
# Cached lookups (SQL)
# -------------------------

@st.cache_data(ttl=600)
def load_towns() -> list[str]:
    """Fetch town names from SQL."""
    engine = get_sql_engine()
    if engine is None:
        return []
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT town_name FROM Towns ORDER BY town_name;")).fetchall()
    return [r[0] for r in rows]

@st.cache_data(ttl=600)
def load_flat_types() -> list[str]:
    """Fetch distinct flat types from Transactions."""
    engine = get_sql_engine()
    if engine is None:
        return []
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT DISTINCT flat_type FROM Transactions ORDER BY flat_type;")).fetchall()
    return [r[0] for r in rows]

@st.cache_data(ttl=600)
def load_date_bounds() -> tuple[dt.date | None, dt.date | None]:
    """Min/max txn_month from Transactions."""
    engine = get_sql_engine()
    if engine is None:
        return None, None
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT MIN(txn_month)::date AS dmin, MAX(txn_month)::date AS dmax FROM Transactions;")
        ).mappings().first()
    dmin = row.get("dmin") if row else None
    dmax = row.get("dmax") if row else None
    return dmin, dmax

# -------------------------
# Watchlist storage (Mongo)
# Support both legacy doc {towns:[...]} and new {items:[{town, flat_type}]}
# -------------------------

def _get_doc(user_id: str) -> dict | None:
    return _watch_coll().find_one({"user_id": user_id})

def get_items(user_id: str) -> list[dict]:
    """Return normalized favorites [{town: str, flat_type: str|None}, ...]."""
    doc = _get_doc(user_id)
    if not doc:
        return []
    if "items" in doc and isinstance(doc["items"], list):
        # Normalize entries
        norm = []
        for it in doc["items"]:
            if not it:
                continue
            town = it.get("town")
            ft = it.get("flat_type")
            if town:
                norm.append({"town": str(town), "flat_type": (str(ft) if ft else None)})
        return norm
    # Legacy migration (on-read)
    if "towns" in doc and isinstance(doc["towns"], list):
        return [{"town": str(t), "flat_type": None} for t in doc["towns"] if t]
    return []

def _save_items(user_id: str, items: list[dict]) -> None:
    _watch_coll().update_one(
        {"user_id": user_id},
        {"$set": {"user_id": user_id, "items": items}},
        upsert=True,
    )

def add_item(user_id: str, town: str, flat_type: str | None) -> None:
    items = get_items(user_id)
    new_item = {"town": town, "flat_type": (flat_type or None)}
    if new_item not in items:
        items.append(new_item)
        _save_items(user_id, items)

def remove_item(user_id: str, idx: int) -> None:
    items = get_items(user_id)
    if 0 <= idx < len(items):
        items.pop(idx)
        _save_items(user_id, items)

# -------------------------
# Data fetch and transforms
# -------------------------

def _build_in_clause(column: str, values: list[str], prefix: str) -> tuple[str, dict]:
    placeholders = []
    params = {}
    for i, v in enumerate(values):
        key = f"{prefix}{i}"
        placeholders.append(f":{key}")
        params[key] = v
    clause = f"{column} IN ({', '.join(placeholders)})" if placeholders else "1=1"
    return clause, params

@st.cache_data(ttl=600, show_spinner=False)
def fetch_prices_full_history(towns: tuple[str, ...], start: dt.date, end: dt.date) -> pd.DataFrame:
    """
    Query ALL rows for the provided towns across the entire DB bounds once.
    Downstream views can subset by lookback without re-querying.
    """
    if not towns:
        return pd.DataFrame(columns=["month", "town", "flat_type", "price"])
    engine = get_sql_engine()
    if engine is None:
        return pd.DataFrame(columns=["month", "town", "flat_type", "price"])

    where = ["t.txn_month BETWEEN :d_start AND :d_end"]
    params = {"d_start": start, "d_end": end}

    town_clause, town_params = _build_in_clause("tn.town_name", list(towns), "tw_")
    where.append(town_clause)
    params.update(town_params)

    sql = text(f"""
        SELECT
            date_trunc('month', t.txn_month)::date AS month,
            tn.town_name AS town,
            t.flat_type AS flat_type,
            t.txn_price::numeric AS price
        FROM Transactions t
        JOIN Flats f ON f.flat_id = t.flat_id
        JOIN Towns tn ON tn.town_id = f.town_id
        WHERE {' AND '.join(where)}
        ORDER BY month ASC;
    """)

    with engine.begin() as conn:
        df = pd.DataFrame(conn.execute(sql, params).mappings())

    if df.empty:
        return df
    df["month"] = pd.to_datetime(df["month"]).dt.date
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["price"])
    df["town"] = df["town"].astype(str)
    df["flat_type"] = df["flat_type"].astype(str)
    return df

def monthly_median(df: pd.DataFrame, by: list[str]) -> pd.DataFrame:
    if df.empty:
        return df
    return (
        df.groupby(by, as_index=False)
          .agg(median_price=("price", "median"), txn_count=("price", "count"))
          .sort_values(by)
    )

def project_linear(series: pd.Series, periods: int) -> np.ndarray | None:
    """Lightweight linear projection using numpy polyfit."""
    y = series.dropna().values.astype(float)
    n = len(y)
    if n < 2:
        return None
    x = np.arange(n, dtype=float)
    coef = np.polyfit(x, y, deg=1)
    x_fut = np.arange(n, n + periods, dtype=float)
    return coef[0] * x_fut + coef[1]

def future_months(last_month: dt.date, periods: int) -> list[dt.date]:
    ts = pd.Timestamp(last_month)
    return [(ts + pd.offsets.MonthBegin(i+1)).date() for i in range(periods)]

# -------------------------
# Page
# -------------------------

def app():
    _require_login()
    st.title("👀 My Watchlist")
    st.caption("Favorites always query the full available history; use the toggles to show all months or a recent window with a projection.")  # UX note

    user_id = str(st.session_state.get("user_id"))
    all_towns = load_towns()
    flat_types = load_flat_types()
    dmin, dmax = load_date_bounds()

    if not dmin or not dmax:
        st.warning("Price data is not available yet.")
        return

    # Full DB bounds
    month_start_min = dt.date(dmin.year, dmin.month, 1)
    month_end_max = (pd.Timestamp(year=dmax.year, month=dmax.month, day=1) + pd.offsets.MonthEnd(0)).date()

    # --- Add favorites ---
    st.subheader("Add favorites")
    c1, c2, c3 = st.columns([1.5, 1.2, 0.8])
    with c1:
        pick_town = st.selectbox("Town", options=["Select town"] + all_towns, index=0)
    with c2:
        pick_flat = st.selectbox("Flat type (optional)", options=["ALL"] + flat_types, index=0)
    with c3:
        if st.button("Add to watchlist", type="primary"):
            if pick_town != "Select town":
                add_item(user_id, pick_town, None if pick_flat == "ALL" else pick_flat)
                st.success("Added to watchlist.")
                st.rerun()
            else:
                st.warning("Please select a town before adding.")

    # --- List favorites ---
    items = get_items(user_id)
    if items:
        st.subheader("Your favorites")
        for idx, it in enumerate(items):
            town = it.get("town", "")
            ft = it.get("flat_type") or "ALL"
            colL, colR = st.columns([0.9, 0.1])
            with colL:
                st.write(f"• {town} — {ft}")
            with colR:
                if st.button("Remove", key=f"rm_{idx}_{town}_{ft}"):
                    remove_item(user_id, idx)
                    st.rerun()
    else:
        st.info("No favorites yet. Add a town above to begin.")
        return

    # --- Controls (display only; query is full history) ---
    cc1, cc2, cc3 = st.columns([1, 1, 1])
    with cc1:
        all_history = st.checkbox("Use all history", value=True, help="When on, charts include every available month.")
    with cc2:
        lookback = st.slider("Lookback (months)", min_value=6, max_value=120, value=24, step=1,
                             help="If all history is off, show only the last N months.")
    with cc3:
        horizon = st.slider("Forecast horizon", min_value=1, max_value=12, value=3, step=1,
                            help="Months to project ahead on the median series.")

    # --- One full-history query for all favorited towns ---
    towns_unique = sorted({it["town"] for it in items if it.get("town")})
    df_all = fetch_prices_full_history(tuple(towns_unique), start=month_start_min, end=month_end_max)
    if df_all.empty:
        st.info("No transactions found across favorites in the database.")
        return

    # Show each favorite as a card
    st.subheader("Personalized view")
    for it in items:
        town = it.get("town")
        ft = it.get("flat_type")  # None => ALL
        label = f"{town} — {ft or 'ALL'}"

        base = df_all[df_all["town"] == town].copy()
        if base.empty:
            with st.container():
                st.markdown(f"#### {label}")
                st.info("No data exists for this town in the database.")
            continue

        df_sel = base if not ft else base[base["flat_type"] == ft].copy()
        # If specific flat_type has no rows, auto-fallback to ALL with a notice
        used_ft = ft
        if df_sel.empty:
            df_sel = base.copy()
            used_ft = None
            st.info(f"No rows for {town} — {ft}; showing ALL flat types instead.")

        # Apply display window if not all_history
        if not all_history:
            end = month_end_max
            start = (pd.Timestamp(end) - pd.offsets.MonthBegin(lookback)).date()
            df_sel = df_sel[(df_sel["month"] >= start) & (df_sel["month"] <= end)].copy()

        if df_sel.empty:
            with st.container():
                st.markdown(f"#### {town} — {used_ft or 'ALL'}")
                st.info("No rows in the chosen display window.")
            continue

        # Aggregate monthly medians
        df_m = monthly_median(df_sel, by=["month"]).rename(columns={"median_price": "value"})
        latest_val = int(df_m["value"].iloc[-1])

        # Forecast
        fut_vals = project_linear(df_m["value"], periods=horizon)
        fut_months = future_months(df_m["month"].max(), horizon) if fut_vals is not None else []
        if fut_vals is not None:
            df_fore = pd.DataFrame({"month": fut_months, "value": fut_vals, "series": "Forecast"})
            df_hist = df_m.assign(series="Historical")
            df_plot = pd.concat([df_hist, df_fore], ignore_index=True)
        else:
            df_plot = df_m.assign(series="Historical")

        # Card
        with st.container():
            st.markdown(f"#### {town} — {used_ft or 'ALL'}")
            k1, k2, k3 = st.columns(3)
            k1.metric("Latest median", f"${latest_val:,}")
            if fut_vals is not None:
                delta_abs = float(fut_vals[-1]) - float(latest_val)
                k2.metric(f"{horizon}‑mo change", f"${int(delta_abs):,}")
                pct = (delta_abs / latest_val * 100) if latest_val else 0.0
                k3.metric("Change (%)", f"{pct:.1f}%")
            else:
                k2.metric(f"{horizon}‑mo change", "N/A")
                k3.metric("Change (%)", "N/A")

            title = "All history" if all_history else f"Last {lookback} months"
            fig = px.line(
                df_plot,
                x="month",
                y="value",
                color="series",
                markers=True,
                labels={"month": "Month", "value": "Median Price"},
                title=f"{title} with {horizon}-month projection",
            )
            for tr in fig.data:
                if getattr(tr, "name", "") == "Forecast":
                    tr.line["dash"] = "dash"
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True})

    # Optional: combined view
    with st.expander("Show combined overview"):
        def tag(row):
            for it in items:
                if row["town"] != it.get("town"):
                    continue
                ft = it.get("flat_type")
                if ft and row["flat_type"] != ft:
                    continue
                return f"{row['town']} · {(ft or 'ALL')}"
            return None

        df_tag = df_all.copy()
        df_tag["series"] = df_tag.apply(tag, axis=1)
        df_tag = df_tag[~df_tag["series"].isna()]
        if not all_history:
            end = month_end_max
            start = (pd.Timestamp(end) - pd.offsets.MonthBegin(lookback)).date()
            df_tag = df_tag[(df_tag["month"] >= start) & (df_tag["month"] <= end)]
        if not df_tag.empty:
            df_series = monthly_median(df_tag, by=["month", "series"]).rename(columns={"median_price": "value"})
            fig = px.line(
                df_series,
                x="month",
                y="value",
                color="series",
                markers=True,
                labels={"month": "Month", "value": "Median Price"},
                title="Combined favorites overview",
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": True})
        else:
            st.info("No matching rows across favorites in the chosen display window.")
