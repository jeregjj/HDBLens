import math
import datetime as dt
import pandas as pd
import streamlit as st
from sqlalchemy import text

from db_config import get_sql_engine

# -------------------------
# Session guard
# -------------------------
def _require_login():
    if not st.session_state.get("logged_in") or st.session_state.get("user_id") is None:
        st.error("Please log in to use your Watchlist.")
        st.stop()

# -------------------------
# Constants
# -------------------------
PAGE_SIZE = 20  # fixed 20 per page

# -------------------------
# Global CSS for key:value layout
# -------------------------
KV_STYLE = """
<style>
.kv {
  display: grid;
  grid-template-columns: 140px auto;
  row-gap: 6px;
  column-gap: 12px;
  margin: 6px 0 2px 0;
}
.kv .k {
  color: var(--text-color, #8a8f98);
  text-transform: capitalize;
  letter-spacing: .2px;
}
.kv .v {
  color: inherit;
  font-weight: 600;
}
</style>
"""

def _format_address(street_name, block_no) -> str | None:
    street = (street_name or "").strip()
    block = (str(block_no) if block_no is not None else "").strip()
    if street and block:
        return f"{street} Blk {block}"
    if street:
        return street
    if block:
        return f"Blk {block}"
    return None

def _format_lease_mm(mm) -> str | None:
    """Convert months to 'X years Y months' with correct pluralization."""
    try:
        mm = int(mm)
    except (TypeError, ValueError):
        return None
    mm = max(mm, 0)
    years, months = divmod(mm, 12)
    parts = []
    if years:
        parts.append(f"{years} year" + ("s" if years != 1 else ""))
    if months or not parts:
        parts.append(f"{months} month" + ("s" if months != 1 else ""))
    return " ".join(parts)

def render_kv(rec: dict):
    # Build a key:value grid with friendly labels; hides txn_id and any empty values
    address = _format_address(rec.get("street_name"), rec.get("block_no"))
    lease_str = _format_lease_mm(rec.get("remaining_lease_months"))
    fields = [
        ("location",         rec.get("town_name")),
        ("type",             rec.get("flat_type")),
        ("floor area",       f"{rec.get('floor_area_sqm')} sqm" if rec.get("floor_area_sqm") is not None else None),
        ("price",            f"${float(rec.get('txn_price') or 0):,.0f}"),
        ("transaction date", str(rec.get("txn_month") or "")),
        ("model",            rec.get("flat_model")),
        ("storey",           rec.get("storey_range")),
        ("address",          address),
        ("remaining lease",  lease_str),
    ]
    rows = [f"<div class='k'>{k}</div><div class='v'>{v}</div>" for k, v in fields if v not in (None, "")]
    html = f"<div class='kv'>{''.join(rows)}</div>"
    st.markdown(html, unsafe_allow_html=True)

# -------------------------
# Cached lookups (Analytics tables)
# -------------------------
@st.cache_data(ttl=600)
def load_towns() -> list[str]:
    eng = get_sql_engine()
    if eng is None:
        return []
    with eng.begin() as conn:
        rows = conn.execute(text("SELECT town_name FROM Towns ORDER BY town_name;")).fetchall()
    return [r[0] for r in rows]

@st.cache_data(ttl=600)
def load_flat_types() -> list[str]:
    eng = get_sql_engine()
    if eng is None:
        return []
    with eng.begin() as conn:
        rows = conn.execute(text("SELECT DISTINCT flat_type FROM Transactions ORDER BY flat_type;")).fetchall()
    return [r[0] for r in rows]

@st.cache_data(ttl=600)
def load_date_bounds() -> tuple[dt.date | None, dt.date | None]:
    eng = get_sql_engine()
    if eng is None:
        return None, None
    with eng.begin() as conn:
        row = conn.execute(
            text("SELECT MIN(txn_month)::date AS dmin, MAX(txn_month)::date AS dmax FROM Transactions;")
        ).mappings().first()
    return (row.get("dmin") if row else None, row.get("dmax") if row else None)

# -------------------------
# Watchlist (same DB)
# -------------------------
def add_to_watchlist(user_id: int, txn_id: int) -> tuple[bool, str]:
    eng = get_sql_engine()
    if eng is None:
        return False, "Database is unavailable."
    with eng.begin() as conn:
        row = conn.execute(
            text("""
                INSERT INTO Watchlist (UserID, txn_id)
                VALUES (:uid, :tx)
                ON CONFLICT (UserID, txn_id) DO NOTHING
                RETURNING WatchlistID;
            """),
            {"uid": int(user_id), "tx": int(txn_id)},
        ).fetchone()
        if row:
            return True, "Added to watchlist."            
        return False, "Already in your watchlist."

def remove_from_watchlist(user_id: int, watchlist_id: int) -> tuple[bool, str]:
    eng = get_sql_engine()
    if eng is None:
        return False, "Database is unavailable."
    with eng.begin() as conn:
        conn.execute(
            text("DELETE FROM Watchlist WHERE WatchlistID = :wid AND UserID = :uid;"),
            {"wid": int(watchlist_id), "uid": int(user_id)},
        )
    return True, "Removed from watchlist."


@st.cache_data(ttl=300, show_spinner=False)
def list_watchlist(user_id: int) -> pd.DataFrame:
    """
    Join Watchlist -> Transactions -> Flats -> Towns (+ dimensions) with stable aliases,
    using LEFT JOINs and casting Watchlist.txn_id (int) to bigint to match Transactions.txn_id.
    """
    eng = get_sql_engine()
    if eng is None:
        return pd.DataFrame()
    sql = text("""
        SELECT
            w.watchlistid            AS watchlist_id,
            w.createdat              AS created_at,
            t.txn_id                 AS txn_id,
            t.txn_month::date        AS txn_month,
            t.txn_price              AS txn_price,
            t.floor_area_sqm         AS floor_area_sqm,
            t.flat_type              AS flat_type,
            t.remaining_lease_months AS remaining_lease_months,
            tn.town_name             AS town_name,
            f.street_name            AS street_name,
            f.block_no               AS block_no,
            fm.flat_model            AS flat_model,
            sr.storey_range          AS storey_range
        FROM Watchlist w
        LEFT JOIN Transactions t ON t.txn_id = w.txn_id::bigint
        LEFT JOIN Flats f        ON f.flat_id = t.flat_id
        LEFT JOIN Towns tn       ON tn.town_id = f.town_id
        LEFT JOIN FlatModel fm   ON fm.flat_model_id = t.flat_model_id
        LEFT JOIN StoreyRange sr ON sr.storey_range_id = t.storey_range_id
        WHERE w.userid = :uid
        ORDER BY w.createdat DESC, t.txn_month DESC NULLS LAST, t.txn_id DESC NULLS LAST
        LIMIT 500
    """)
    with eng.begin() as conn:
        return pd.DataFrame(conn.execute(sql, {"uid": int(user_id)}).mappings())

# -------------------------
# Search helpers (pagination)
# -------------------------
def _build_filters(town, flat_type, start, end, min_price, max_price, min_sqm, max_sqm):
    where = ["1=1"]
    params: dict = {}
    if town and town != "ALL":
        where.append("tn.town_name = :town")
        params["town"] = town
    if flat_type and flat_type != "ALL":
        where.append("t.flat_type = :ft")
        params["ft"] = flat_type
    if start:
        where.append("t.txn_month >= :d_start")
        params["d_start"] = start
    if end:
        where.append("t.txn_month <= :d_end")
        params["d_end"] = end
    if min_price is not None and min_price > 0:
        where.append("t.txn_price >= :pmin")
        params["pmin"] = min_price
    if max_price is not None:
        where.append("t.txn_price <= :pmax")
        params["pmax"] = max_price
    if min_sqm is not None:
        where.append("t.floor_area_sqm >= :smin")
        params["smin"] = min_sqm
    if max_sqm is not None:
        where.append("t.floor_area_sqm <= :smax")
        params["smax"] = max_sqm
    return " AND ".join(where), params

@st.cache_data(ttl=300, show_spinner=False)
def search_transactions_count(town, flat_type, start, end, min_price, max_price, min_sqm, max_sqm) -> int:
    eng = get_sql_engine()
    if eng is None:
        return 0
    where_sql, params = _build_filters(town, flat_type, start, end, min_price, max_price, min_sqm, max_sqm)
    sql = text(f"""
        SELECT COUNT(*) AS n
        FROM Transactions t
        JOIN Flats f  ON f.flat_id = t.flat_id
        JOIN Towns tn ON tn.town_id = f.town_id
        LEFT JOIN FlatModel fm   ON fm.flat_model_id = t.flat_model_id
        LEFT JOIN StoreyRange sr ON sr.storey_range_id = t.storey_range_id
        WHERE {where_sql}
    """)
    with eng.begin() as conn:
        return int(conn.execute(sql, params).scalar() or 0)

@st.cache_data(ttl=300, show_spinner=False)
def search_transactions_page(town, flat_type, start, end, min_price, max_price, min_sqm, max_sqm, page: int, page_size: int = PAGE_SIZE) -> pd.DataFrame:
    eng = get_sql_engine()
    if eng is None:
        return pd.DataFrame()
    where_sql, params = _build_filters(town, flat_type, start, end, min_price, max_price, min_sqm, max_sqm)
    params = dict(params)
    params["lim"] = int(page_size)
    params["off"] = int((max(1, page) - 1) * page_size)
    sql = text(f"""
        SELECT
            t.txn_id,
            t.txn_month::date AS txn_month,
            t.txn_price,
            t.floor_area_sqm,
            t.flat_type,
            t.remaining_lease_months,
            tn.town_name,
            fm.flat_model,
            sr.storey_range,
            f.street_name,
            f.block_no
        FROM Transactions t
        JOIN Flats f        ON f.flat_id = t.flat_id
        JOIN Towns tn       ON tn.town_id = f.town_id
        LEFT JOIN FlatModel fm   ON fm.flat_model_id = t.flat_model_id
        LEFT JOIN StoreyRange sr ON sr.storey_range_id = t.storey_range_id
        WHERE {where_sql}
        ORDER BY t.txn_month DESC, t.txn_id DESC
        LIMIT :lim OFFSET :off
    """)
    with eng.begin() as conn:
        return pd.DataFrame(conn.execute(sql, params).mappings())

# -------------------------
# Page
# -------------------------
def app():
    _require_login()

    # Inject CSS for the key:value layout
    st.markdown(KV_STYLE, unsafe_allow_html=True)

    st.title("👀 My Watchlist")
    st.caption("Save individual resale transactions to your watchlist, and view their full details below.")

    try:
        user_id = int(st.session_state.get("user_id"))
    except Exception:
        st.error("User profile is missing a valid numeric ID.")
        st.stop()

    towns = load_towns()
    flat_types = load_flat_types()
    dmin, dmax = load_date_bounds()
    if not dmin or not dmax:
        st.warning("Price data is not available yet.")
        return

    # Persistent UI/session keys
    st.session_state.setdefault("watchlist_dirty", False)
    st.session_state.setdefault("search_filters", None)   # dict of current filters
    st.session_state.setdefault("search_page", 1)         # current page number

    # -------------------------
    # Watchlist FIRST (top of page)
    # -------------------------
    st.subheader("Your watchlist")

    if st.session_state.get("watchlist_dirty"):
        try:
            list_watchlist.clear()
        except Exception:
            pass
        st.session_state["watchlist_dirty"] = False

    df_watch = list_watchlist(user_id)
    if df_watch.empty:
        st.info("Your watchlist is empty.")
    else:
        for _, row in df_watch.iterrows():
            with st.container(border=True):
                left, right = st.columns([0.85, 0.15])
                with left:
                    render_kv(row)
                with right:
                    if st.button("Remove", key=f"rm_{int(row['watchlist_id'])}"):
                        ok, msg = remove_from_watchlist(user_id, int(row["watchlist_id"]))
                        if ok:
                            st.toast("Removed from watchlist ✅")
                            try:
                                list_watchlist.clear()
                                st.rerun()
                            except Exception:
                                pass
                            st.session_state["watchlist_dirty"] = True
                        else:
                            st.toast(msg, icon="⚠️")

    st.divider()

    # -------------------------
    # Search and add section (with pagination)
    # -------------------------
    st.subheader("Find transactions to add")

    c1, c2, c3, c4 = st.columns([1.2, 1.2, 1.0, 1.0])
    with c1: pick_town = st.selectbox("Town", options=["ALL"] + towns, index=0)
    with c2: pick_flat = st.selectbox("Flat type", options=["ALL"] + flat_types, index=0)
    with c3: start = st.date_input("From month", value=dmin, min_value=dmin, max_value=dmax)
    with c4: end   = st.date_input("To month", value=dmax, min_value=dmin, max_value=dmax)

    c5, c6, c7, c8 = st.columns([1.0, 1.0, 1.0, 1.0])
    with c5: min_price = st.number_input("Min price", min_value=0, value=0, step=1000)
    with c6:
        max_price_val = st.number_input("Max price (0=none)", min_value=0, value=0, step=1000)
        max_price = None if max_price_val == 0 else max_price_val
    with c7:
        min_sqm_val = st.number_input("Min sqm (0=none)", min_value=0, value=0, step=1)
        min_sqm = None if min_sqm_val == 0 else min_sqm_val
    with c8:
        max_sqm_val = st.number_input("Max sqm (0=none)", min_value=0, value=0, step=1)
        max_sqm = None if max_sqm_val == 0 else max_sqm_val

    a1, a2, a3 = st.columns([0.25, 0.25, 0.5])
    with a1:
        if st.button("Search", type="primary"):
            st.session_state["search_filters"] = {
                "town": pick_town,
                "flat_type": pick_flat,
                "start": start,
                "end": end,
                "min_price": (min_price if min_price and min_price > 0 else None),
                "max_price": max_price,
                "min_sqm": min_sqm,
                "max_sqm": max_sqm,
            }
            st.session_state["search_page"] = 1  # reset to first page on new search
    with a2:
        if st.button("Clear search"):
            st.session_state["search_filters"] = None
            st.session_state["search_page"] = 1

    filters = st.session_state.get("search_filters")
    if filters:
        # Count + pagination controls
        total = search_transactions_count(**filters)
        total_pages = max(1, math.ceil(total / PAGE_SIZE))
        page = min(max(1, st.session_state.get("search_page", 1)), total_pages)

        # Controls
        pc1, pc2, pc3, pc4 = st.columns([0.2, 0.35, 0.25, 0.2])
        with pc1:
            if st.button("◀ Prev", disabled=(page <= 1)):
                st.session_state["search_page"] = max(1, page - 1)
                page = st.session_state["search_page"]
        with pc2:
            st.caption(f"{total} results • Page {page} of {total_pages}")
        with pc3:
            go_to = st.number_input("Go to page", min_value=1, max_value=total_pages, value=page, step=1, label_visibility="collapsed")
            if go_to != page:
                st.session_state["search_page"] = int(go_to)
                page = st.session_state["search_page"]
        with pc4:
            if st.button("Next ▶", disabled=(page >= total_pages)):
                st.session_state["search_page"] = min(total_pages, page + 1)
                page = st.session_state["search_page"]

        # Fetch current page
        df_page = search_transactions_page(page=page, page_size=PAGE_SIZE, **filters)

        if df_page.empty:
            st.info("No transactions match your filters.")
        else:
            for _, rec in df_page.iterrows():
                with st.container(border=True):
                    left, right = st.columns([0.85, 0.15])
                    with left:
                        render_kv(rec)
                    with right:
                        if st.button("Add", key=f"add_{int(rec['txn_id'])}"):
                            ok, msg = add_to_watchlist(user_id, int(rec["txn_id"]))
                            if ok:
                                st.toast("Added to watchlist ✅")
                                try:
                                    list_watchlist.clear()
                                    st.rerun()
                                except Exception:
                                    pass
                                st.session_state["watchlist_dirty"] = True
                            else:
                                st.toast(msg, icon="⚠️")
