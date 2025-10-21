# This file creates the Streamlit UI for the login page.

import streamlit as st
import user_auth # Import our new user authentication module

def app():
    st.title("Login to HDBLens")

    # Prevent logged-in users from seeing the login page again
    if st.session_state.get('logged_in', False):
        st.info("You are already logged in.")
        return

    with st.form(key='login_form'):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        submit_button = st.form_submit_button(label="Login")

    if submit_button:
        if not username or not password:
            st.error("Please enter both username and password.")
        else:
            # Call the login function from our auth module
            # --- Capture user_id ---
            success, user_id, message = user_auth.login_user(username, password)

            if success:
                # Set session state to mark user as logged in
                st.session_state['logged_in'] = True
                st.session_state['username'] = username
                st.session_state['user_id'] = user_id # Store the user's ID
                st.success(message)
                st.info("Welcome! Navigate to another page to continue.")
                # --- Remove rerun ---
                # st.experimental_rerun() # Let Streamlit handle the rerun naturally
            else:
                st.error(message)