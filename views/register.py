import streamlit as st
import user_auth # Import our user authentication module

def app():
    st.title("Register for HDBLens")

    with st.form(key='register_form'):
        email = st.text_input("Email")
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        confirm_password = st.text_input("Confirm Password", type="password")
        
        submit_button = st.form_submit_button(label="Register")

    if submit_button:
        if not email or not username or not password or not confirm_password:
            st.error("Please fill in all fields.")
        elif password != confirm_password:
            st.error("Passwords do not match.")
        elif len(password) < 8:
            st.error("Password must be at least 8 characters long.")
        else:
            success, message = user_auth.register_user(username, password, email)
            
            if success:
                st.success(message)
                st.info("You can now navigate to the Login page.")
            else:
                st.error(message)