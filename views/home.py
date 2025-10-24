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
    
    st.title("Hybrid Snapshot")
    st.caption("Combining market data (Postgres) with community sentiment (MongoDB)")

    # =========================
    # 1) OVERVIEW TILES (Hybrid)
    # =========================
    snap = _overview_cached()  # {tx_this_month, avg_price_all, avg_rating, most_reviewed_town, most_reviewed_count}

    c1, c2, c3, c4 = st.columns(4)
    c1.markdown("**Transactions (This Month)**")
    c1.markdown(f"<span style='font-size:1.5rem;'>{snap.get('tx_this_month',0):,}</span>", unsafe_allow_html=True)

    c2.markdown("**Avg Price (Islandwide)**")
    c2.markdown(f"<span style='font-size:1.5rem;'>${(snap.get('avg_price_all') or 0):,.0f}</span>", unsafe_allow_html=True)

    c3.markdown("**Avg Rating (Islandwide)**")
    c3.markdown(f"<span style='font-size:1.5rem;'>{snap.get('avg_rating','—')}/5</span>", unsafe_allow_html=True)

    c4.markdown("**Most Reviewed Town**")
    c4.markdown(f"<span style='font-size:1.5rem;'>{snap.get('most_reviewed_town','—')} ({snap.get('most_reviewed_count',0)})</span>", unsafe_allow_html=True)

    st.divider()

    # ==========================================
    # 2) HYBRID BUBBLE: Affordability × Ratings
    # ==========================================
    st.subheader("Affordable & Well-Rated Towns")
    st.caption("Rank towns by how well prices fit your budget and how well residents rate them.")
    colA, colB= st.columns([1,1])
    with colA:
        ft_for_rank = st.selectbox("Flat type", ["3 ROOM","4 ROOM","5 ROOM"], index=1)
    with colB:
        budget = st.number_input("Budget (SGD)", value=550_000, step=10_000)

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
        st.plotly_chart(fig, config={'displayModeBar': True}, use_container_width=True)

    st.divider()
