import streamlit as st
import pandas as pd
from hybrid_queries import town_profile  
from db_config import get_sql_engine

# --- Helper functions to populate filters ---
# We cache these so they only run once
@st.cache_data
def get_town_list():
    """Fetches all town names from the SQL database."""
    engine = get_sql_engine()
    with engine.connect() as conn:
        df = pd.read_sql("SELECT town_name FROM towns ORDER BY town_name", conn)
    return ["ALL"] + df['town_name'].tolist() # Add "ALL" option

@st.cache_data
def get_flat_types():
    """Fetches all flat types from the SQL database."""
    engine = get_sql_engine()
    with engine.connect() as conn:
        df = pd.read_sql("SELECT DISTINCT flat_type FROM transactions ORDER BY flat_type", conn)
    return ["ALL"] + df['flat_type'].tolist()

# --- Main Page Function ---
def app():
    st.title("🏘️ Town Sentiment Dashboard")

    st.header("Town-Specific Sentiment Deep Dive")
    
    # 1. Get filter options
    towns = get_town_list()
    flat_types = get_flat_types()

    # 2. Add filters
    col1, col2 = st.columns(2)
    with col1:
        selected_town = st.selectbox(
            "Select a Town", 
            options=towns, 
            index=0 # Default to "ALL"
        )
    with col2:
        selected_flat_type = st.selectbox(
            "Select a Flat Type", 
            options=flat_types, 
            index=0 # Default to "ALL"
        )
    
    # We can't show a profile for "ALL" towns, so we prompt the user to select one.
    if selected_town == "ALL":
        st.info("Please select a specific town to see its detailed profile.")
        return

    # 3. Call town_profile()
    # Format the flat_type parameter: pass None if "ALL", otherwise pass the string
    flat_type_query = None if selected_flat_type == "ALL" else selected_flat_type
    
    # Get the data from the correct hybrid function
    details = town_profile(town=selected_town, flat_type=flat_type_query)
    
    # 4. Display the results
    st.subheader(f"Snapshot for: {details.get('town')}")
    if flat_type_query:
        st.markdown(f"*(Filtered for: {flat_type_query})*")

    # --- Display SQL Price Data ---
    st.subheader("Price Analytics")
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Median Price (Last 12mo)", f"${details.get('median_price', 0):,}" if details.get('median_price') else "N/A")
    with col2:
        st.metric("25th Percentile Price", f"${details.get('p25', 0):,}" if details.get('p25') else "N/A")
    with col3:
        st.metric("75th Percentile Price", f"${details.get('p75', 0):,}" if details.get('p75') else "N/A")
    with col4:
        st.metric("Transactions (Last 12mo)", f"{details.get('txn_count', 0):,}")
        
    # --- Display Mongo Review Data ---
    st.subheader("Community Sentiment")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Average Rating", f"{details.get('avg_rating', 0):.1f} ★" if details.get('avg_rating') else "N/A")
    with col2:
        st.metric("Total Reviews", f"{details.get('reviews_count', 0):,}")

    # --- Display Latest Reviews ---
    st.subheader("Latest Community Reviews")
    latest_reviews = details.get('latest_reviews', [])
    
    if not latest_reviews:
        st.info("No reviews found for this town (and filter) yet.")
    else:
        for review in latest_reviews:
            with st.container(border=True):
                st.write(f"**Rating: {review.get('rating', 'N/A')} ★**")
                st.write(f"*{review.get('review_text', 'No text')}*")
                # Format the date if it exists
                if review.get('created_at'):
                    st.caption(f"Posted on: {review['created_at'].strftime('%Y-%m-%d')}")