# pages/01_Profile.py
import streamlit as st
from user_auth import get_user_by_id, update_user_email, update_user_password, delete_user
from utils.ui_helpers import confirm_prompt

def require_login():
    if "user_id" not in st.session_state or st.session_state.user_id is None:
        st.error("You must be logged in to view this page.")
        st.stop()

def app():
    st.title("Profile")

    require_login()
    uid = st.session_state.user_id

    user = get_user_by_id(uid)
    if not user:
        st.error("User not found.")
        st.stop()

    user_id, username, email, _ = user

    with st.expander("Account Info", expanded=True):
        st.write(f"**Username:** {username}")
        st.write(f"**Email:** {email}")

    st.divider()

    # --- Update Email ---
    st.subheader("Update Email")
    new_email = st.text_input("New email", value=email, key="profile_new_email")
    if st.button("Save Email"):
        ok, msg = update_user_email(uid, new_email.strip())
        (st.success if ok else st.error)(msg)

    st.divider()

    # --- Reset Password ---
    st.subheader("Reset Password")
    newp = st.text_input("New password", type="password", key="pw_new")
    conf = st.text_input("Confirm new password", type="password", key="pw_conf")
    if st.button("Reset Password"):
        ok, msg = update_user_password(uid, newp, conf)
        (st.success if ok else st.error)(msg)
        st.divider()

    # --- Danger Zone: Delete Account ---
    st.markdown("---")
    st.subheader("Account Deletion")
    st.caption("Deleting your account is permanent and cannot be undone.")

    # First click — triggers confirmation prompt
    if st.button("Delete Account", key="btn_delete_acct", type="primary"):
        st.session_state["confirm_delete_acct"] = True
        st.rerun()

    # Confirmation prompt appears after clicking Delete Account
    if confirm_prompt(
        state_key="confirm_delete_acct",
        title="Are you sure you want to delete your account?",
        message="This will permanently delete your account and all associated data. Type DELETE below to proceed.",
        require_text="DELETE"
    ):
        del_pw = st.text_input("Enter your password to confirm deletion", type="password", key="del_pw")
        if st.button("Yes, permanently delete my account", key="btn_delete_final", type="primary"):
            ok, msg = delete_user(uid, del_pw)
            if ok:
                st.success(msg)
                st.session_state.clear()
                st.info("Your account has been deleted and you have been logged out.")
            else:
                st.error(msg)