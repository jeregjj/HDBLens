import streamlit as st
import plotly.express as px
from sqlalchemy import text
from db_config import get_sql_engine
from hybrid_queries import town_profile, hybrid_overview, hybrid_affordability

# ---------- small helpers ----------
@st.cache_data(ttl=300)
def _load_towns():
    engine = get_sql_engine()
    with engine.begin() as conn:
        return [r[0] for r in conn.execute(text("SELECT town_name FROM Towns ORDER BY town_name;"))]

@st.cache_data(ttl=300)
def _overview_cached():
    return hybrid_overview()

def app():
    # --- Greet the logged-in user ---
    if st.session_state['logged_in']:
        st.title(f"Welcome back, {st.session_state['username']}!")
        st.subheader("You can manage your watchlist or explore HDB analytics.")
        # We can add more dashboard-like elements here later
    else:
        # --- Default welcome message for logged-out users ---
        st.title("Welcome to HDBLens 🏠")
        st.subheader("Your one-stop platform for HDB resale insights.")
    
    st.markdown("---")
    st.header("Project Overview")
    st.markdown("""
    HDBLens is a web platform that consolidates official resale transactions from data.gov.sg 
    and provides dynamic analytics, showing town and flat-type price trends, 
    storey and floor-area patterns, and remaining-lease effects via fast dashboards 
    and rich filters.
    """)
    
def render():
    st.title("Hybrid Snapshot")
    st.caption("Combining market data (Postgres) with community sentiment (MongoDB)")

    # =========================
    # 1) OVERVIEW TILES (Hybrid)
    # =========================
    snap = _overview_cached()  # {tx_this_month, avg_price_all, avg_rating, most_reviewed_town, most_reviewed_count}

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown("**Txns (This Month)**")
    c1.markdown(f"<span style='font-size:1.5rem;'>{snap.get('tx_this_month',0):,}</span>", unsafe_allow_html=True)

    c2.markdown("**Avg Price (Islandwide)**")
    c2.markdown(f"<span style='font-size:1.5rem;'>${(snap.get('avg_price_all') or 0):,.0f}</span>", unsafe_allow_html=True)

    c3.markdown("**Avg Rating**")
    c3.markdown(f"<span style='font-size:1.5rem;'>{snap.get('avg_rating','—')}/5</span>", unsafe_allow_html=True)

    c4.markdown("**Most Reviewed Town**")
    c4.markdown(f"<span style='font-size:1.5rem;'>{snap.get('most_reviewed_town','—')} ({snap.get('most_reviewed_count',0)})</span>", unsafe_allow_html=True)

    st.divider()

    # ==========================================
    # 2) HYBRID BUBBLE: Affordability × Ratings
    # ==========================================
    st.subheader("Affordable & Well-Rated Towns")
    colA, colB, colC = st.columns([1,1,2])
    with colA:
        ft_for_rank = st.selectbox("Flat type", ["3 ROOM","4 ROOM","5 ROOM"], index=1)
    with colB:
        budget = st.number_input("Budget (SGD)", value=550_000, step=10_000)
    with colC:
        st.write("Rank towns by how well prices fit your budget and how well residents rate them.")

    df_rank = hybrid_affordability(ft_for_rank, float(budget), months=12)
    if df_rank.empty:
        st.info("No towns found for the selected filters.")
    else:
        # table
        st.dataframe(
            df_rank[["town","median_price","txn_count","avg_rating","reviews_count","hybrid_score"]],
            width='stretch', height=320
        )
        # bubble chart
        fig = px.scatter(
            df_rank,
            x="median_price", y="avg_rating",
            size="reviews_count", color="hybrid_score",
            hover_name="town",
            labels={"median_price":"Median Price (12m)", "avg_rating":"Avg Rating"},
            title="Median Price vs Avg Rating (bubble = review count)"
        )
        st.plotly_chart(fig, width='stretch')

    st.divider()

    # =========================
    # 3) TOWN PROFILE (Hybrid)
    # =========================
    st.subheader("Town Profile")
    # towns dropdown (from SQL)
    towns = _load_towns() or ["ANG MO KIO", "BEDOK", "CENTRAL AREA", "TAMPINES"]
    town = st.selectbox("Select town", towns, index=(towns.index("CENTRAL AREA") if "CENTRAL AREA" in towns else 0))
    ft = st.selectbox("Flat type (optional)", ["(All)", "3 ROOM", "4 ROOM", "5 ROOM"], index=0)
    ft_filter = None if ft == "(All)" else ft

    data = town_profile(town, ft_filter, months=12)

    # compact metrics row (smaller text)
    st.markdown(
        f"""
        <div style="display:flex;justify-content:space-between;">
            <div style="flex:1;padding:0.5rem;">
                <div><b>Median Price</b></div>
                <div style="font-size:1.05rem;">${(data.get('median_price') or 0):,.0f}</div>
            </div>
            <div style="flex:1;padding:0.5rem;">
                <div><b>25th to 75th Percentile</b></div>
                <div style="font-size:1.05rem;">${(data.get('p25') or 0):,.0f} – ${(data.get('p75') or 0):,.0f}</div>
            </div>
            <div style="flex:1;padding:0.5rem;">
                <div><b>Transactions</b></div>
                <div style="font-size:1.05rem;">{data.get('txn_count') or 0:,}</div>
            </div>
            <div style="flex:1;padding:0.5rem;">
                <div><b>Avg Rating</b></div>
                <div style="font-size:1.05rem;">{data.get('avg_rating') or '—'}/5 ({data.get('reviews_count',0)} reviews)</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    st.markdown("**Latest Reviews**")
    reviews = data.get("latest_reviews", [])
    if not reviews:
        st.write("_No recent reviews_")
    else:
        for r in reviews:
            user = r.get("user") or r.get("username") or "Anonymous"
            stars = "⭐" * int(r.get("rating", 0) or 0)
            created = r.get("created_at")
            created_str = str(created)[:10] if created else ""
            st.markdown(f"**{user}** wrote:")
            st.caption(f"{created_str} | {stars}")
            st.write(r.get("review_text", ""))
            st.write("---")