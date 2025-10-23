import streamlit as st

# ------- Delete Confirmation Prompt (Account/Review deletion) ------------
def confirm_prompt(state_key: str, title: str, message: str, require_text: str | None = None) -> bool:
    if not st.session_state.get(state_key):
        return False

    st.warning(title)
    st.write(message)

    ok = False
    if require_text:
        val = st.text_input(f'Type "{require_text}" to confirm', key=f"{state_key}_input")
        disabled = (val.strip() != require_text)
    else:
        disabled = False

    c1, c2 = st.columns(2)
    with c1:
        if st.button("Cancel", key=f"{state_key}_cancel"):
            st.session_state[state_key] = False
            st.rerun()
    with c2:
        if st.button("Yes, delete", key=f"{state_key}_ok", disabled=disabled):
            ok = True
            st.session_state[state_key] = False
    return ok