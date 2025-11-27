import streamlit as st
from views import home, reviews, analytics, town_sentiment , watchlist
from views import login, register, profile  # Import our new login/register pages
from db_config import init_sql_db, init_mongo # Import all init functions

# --- Initialize session state ---
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'username' not in st.session_state:
    st.session_state['username'] = ""
if 'user_id' not in st.session_state:
    st.session_state['user_id'] = None

st.set_page_config(
    page_title="HDBLens",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Define all possible pages ---
PAGES = {
    "Home": home,
    "Analytics": analytics,
    "Reviews": reviews,
    "Town Sentiment": town_sentiment, 
    "Login": login,
    "Register": register,
    "Profile": profile,
    "My Watchlist": watchlist,
}

# --- Helper function for DB status ---
def _alert(ok: bool, msg: str):
    (st.success if ok else st.error)(msg, icon="✅" if ok else "🚨")

def main():
    with st.sidebar:
        st.title("HDBLens 🏠")  
        
        # --- Database Status ---
        st.subheader("System Status")
        ok_pg_analytics, msg_pg_analytics = init_sql_db()
        ok_mg, msg_mg = init_mongo()
        
        _alert(ok_pg_analytics, msg_pg_analytics)
        _alert(ok_mg, msg_mg)
        
        st.markdown("---")
        
        # --- DYNAMIC NAVIGATION LOGIC ---
        if st.session_state['logged_in']:
            st.success(f"Welcome, {st.session_state['username']}!")
            pages_to_show = ["Home", "Profile", "Analytics", "Reviews", "Town Sentiment", "My Watchlist"]
            
            selection = st.radio("Navigation", pages_to_show)

            if st.button("Logout"):
                st.session_state['logged_in'] = False
                st.session_state['username'] = ""
                st.session_state['user_id'] = None
                st.success("You have successfully logged out!")
                st.rerun()

        else:
            st.info("Please log in or register to access all features.")
            pages_to_show = ["Login", "Register"]
            selection = st.radio("Navigation", pages_to_show)
    
    # --- Run the selected page ---
    page_module = PAGES[selection]
    
    if selection == "Town Sentiment":
        town_sentiment.app() 
    elif selection == "Home":
        home.app()
    else:
        # This calls the app() function from your view files
        # (e.g., views/home.py, views/login.py, etc.)
        page_module.app()

    if selection == "Login" and st.session_state['logged_in']:
        st.rerun()

if __name__ == "__main__":
    main()