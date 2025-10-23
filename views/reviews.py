# views/reviews.py
# CRUD for MongoDB with ownership checks (no updated_at field).

import os
from datetime import datetime, timezone
from bson.objectid import ObjectId
import streamlit as st
from utils.ui_helpers import confirm_prompt

from db_config import get_mongo_collection, get_sql_engine
from sqlalchemy import text

MONGO_COLL = os.getenv("MONGO_COLLECTION", "town_reviews")
MONGO_META = os.getenv("MONGO_META_COLLECTION", "meta")

def get_collections():
    reviews_collection = get_mongo_collection(MONGO_COLL)
    meta_collection = get_mongo_collection(MONGO_META)
    return reviews_collection, meta_collection

# ---------- auth helpers ----------
def is_logged_in() -> bool:
    return bool(st.session_state.get("logged_in"))

def current_user_id():
    return st.session_state.get("user_id")

def current_username():
    return st.session_state.get("username")

# ---------- town list: SQL -> meta -> distinct(reviews) ----------
def get_town_list(reviews_collection, meta_collection):
    # 1) SQL Towns
    try:
        engine = get_sql_engine()
        if engine is not None:
            with engine.connect() as conn:
                rows = conn.execute(text("SELECT town_name FROM towns ORDER BY town_name")).fetchall()
                if rows:
                    return [r[0] for r in rows]
    except Exception as e:
        st.info(f"(Town list from SQL not available: {e})")

    # 2) Mongo meta
    try:
        doc = meta_collection.find_one({"_id": "distinct_towns"})
        if doc and isinstance(doc.get("values"), list) and doc["values"]:
            return sorted({t for t in doc["values"] if t})
    except Exception as e:
        st.info(f"(Town list from Mongo meta not available: {e})")

    # 3) Distinct from reviews
    try:
        towns = reviews_collection.distinct("town")
        towns = [t for t in towns if t]
        if towns:
            return sorted(set(towns))
    except Exception as e:
        st.info(f"(Town list from reviews not available: {e})")

    return ["ANG MO KIO", "BEDOK", "PUNGGOL", "TAMPINES", "YISHUN"]

# ---------- CRUD helpers (use your existing fields) ----------
def create_review(col, *, town, user_id, username, rating, review_text):
    doc = {
        "town": town,
        "ID": user_id,                  # your existing schema
        "username": username,           # your existing schema
        "rating": int(rating),
        "review_text": review_text,
        # timezone-aware to match your sample: 2025-05-14T16:30:00.000+00:00
        "created_at": datetime.now(timezone.utc),
    }
    return col.insert_one(doc)

def _owner_match_filter(requester_id, requester_name):
    ors = []
    if requester_id is not None:
        ors.append({"ID": requester_id})
        ors.append({"ID": str(requester_id)})  # in case historical docs used string
    if requester_name:
        ors.append({"username": requester_name})
    return {"$or": ors} if ors else {"_id": {"$exists": False}}  # never matches if no identity

def update_review(col, *, review_id: str, requester_id, requester_name, new_text, new_rating):
    _id = ObjectId(review_id) if not isinstance(review_id, ObjectId) else review_id
    owner_filter = _owner_match_filter(requester_id, requester_name)
    return col.update_one(
        {"_id": _id, **owner_filter},
        {"$set": {"review_text": new_text, "rating": int(new_rating)}}
    )

def delete_review(col, *, review_id: str, requester_id, requester_name):
    _id = ObjectId(review_id) if not isinstance(review_id, ObjectId) else review_id
    owner_filter = _owner_match_filter(requester_id, requester_name)
    return col.delete_one({"_id": _id, **owner_filter})

# ---------- Page ----------
def app():
    st.title("✍️ User Reviews")
    st.markdown("Read community reviews and share your own experience.")

    reviews_collection, meta_collection = get_collections()
    if reviews_collection is None:
        st.error("Failed to connect to MongoDB. Please check the server connection.")
        return

    # --- Town list loading ---
    town_list = get_town_list(reviews_collection, meta_collection)

    # ---------- Tabs ----------
    browse_tab, add_tab, mine_tab = st.tabs(["Browse", "Add Review", "My Reviews"])

    # =========================================================
    # TAB 1: Browse (read-only)
    # =========================================================
    with browse_tab:
        st.subheader("Browse Reviews")

        selected_town = st.selectbox("Filter by Town", options=["All Towns"] + town_list, key="browse_town")

        c1, c2 = st.columns([2, 1])
        with c1:
            sort_field_label = st.selectbox("Sort by", ["Most Recent", "Rating"], index=0, key="browse_sort_field")
        with c2:
            sort_order_label = st.selectbox("Order", ["Descending", "Ascending"], index=0, key="browse_sort_order")

        sort_key = "created_at" if sort_field_label == "Most Recent" else "rating"
        sort_dir = -1 if sort_order_label == "Descending" else 1

        PAGE_SIZE = 10
        fp = (selected_town, sort_key, sort_dir)
        if st.session_state.get("browse_fp") != fp:
            st.session_state["browse_fp"] = fp
            st.session_state["browse_page"] = 1

        page = max(1, int(st.session_state.get("browse_page", 1)))

        query = {}
        if selected_town != "All Towns":
            query["town"] = selected_town

        total_count = reviews_collection.count_documents(query)
        total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)
        if page > total_pages:
            page = total_pages
            st.session_state["browse_page"] = page

        skip = (page - 1) * PAGE_SIZE
        docs = list(
            reviews_collection.find(query).sort(sort_key, sort_dir).skip(skip).limit(PAGE_SIZE)
        )

        st.caption(f"{total_count} review(s) • Page {page} of {total_pages}")
        from datetime import datetime, timezone

        for review in docs:
            rid = str(review["_id"])
            author_name = review.get("username") or review.get("user") or "N/A"
            town = review.get("town", "N/A")
            rating = review.get("rating", "N/A")
            created = review.get("created_at")

            # Stars + header
            stars = "⭐" * int(rating if str(rating).isdigit() else 0)
            st.markdown(
                f"**{author_name}** on **{town}** — "
                f"<span style='color:gold;font-size:18px;'>{stars}</span> "
                f"<span style='color:gray;'>({rating}/5)</span>",
                unsafe_allow_html=True
            )

            # Timestamp
            if isinstance(created, datetime):
                try:
                    ts = created.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                except Exception:
                    ts = created.strftime("%Y-%m-%d %H:%M")
            else:
                ts = str(created) if created is not None else "N/A"
            st.caption(f"Posted: {ts}")

            # Review card
            review_text = review.get("review_text", "")
            st.markdown(
                f"""
                <div style="
                    background-color:#111827;
                    border:1px solid #2e2e2e;
                    border-radius:0.75rem;
                    box-shadow:0 0 6px rgba(0,0,0,0.3);
                    padding:1rem;
                    margin-top:0.5rem;
                    color:#f1f1f1;
                ">
                    {review_text}
                </div>
                """,
                unsafe_allow_html=True
            )
            st.markdown("---")

        # Bottom pager
        left, right = st.columns(2)
        with left:
            if st.button("◀ Previous", disabled=(page <= 1), key="browse_prev"):
                st.session_state["browse_page"] = page - 1
                st.rerun()
        with right:
            if st.button("Next ▶", disabled=(page >= total_pages), key="browse_next"):
                st.session_state["browse_page"] = page + 1
                st.rerun()

    # =========================================================
    # TAB 2: Add Review
    # =========================================================
    with add_tab:
        st.subheader("Add a Review")
        if not is_logged_in():
            st.info("Log in to post a review.")
        else:
            with st.form(key="review_form", clear_on_submit=True):
                town_choice = st.selectbox("Town", options=town_list, key="add_town")
                rating = st.slider("Rating (1-5)", 1, 5, 3, key="add_rating")
                review_text = st.text_area("Your Review", key="add_text")
                submit_button = st.form_submit_button("Post Review")

            if submit_button:
                if not review_text.strip():
                    st.error("Please write a review before posting.")
                else:
                    try:
                        reviews_collection.insert_one({
                            "town": town_choice,
                            "username": current_username(),
                            "rating": int(rating),
                            "review_text": review_text.strip(),
                            "created_at": datetime.now(timezone.utc),
                        })
                        st.success("Review posted successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Failed to post review: {e}")

    # =========================================================
    # TAB 3: My Reviews
    # =========================================================
    with mine_tab:
        st.subheader("My Reviews")
        if not is_logged_in():
            st.info("Log in to view and manage your reviews.")
        else:
            uname = current_username()

            c1, c2 = st.columns([2, 1])
            with c1:
                my_sort_field = st.selectbox("Sort by", ["Most Recent", "Rating"], index=0, key="my_sort_field")
            with c2:
                my_sort_order = st.selectbox("Order", ["Descending", "Ascending"], index=0, key="my_sort_order")

            my_sort_key = "created_at" if my_sort_field == "Most Recent" else "rating"
            my_sort_dir = -1 if my_sort_order == "Descending" else 1

            MY_PAGE_SIZE = 10
            my_fp = (uname, my_sort_key, my_sort_dir)
            if st.session_state.get("my_fp") != my_fp:
                st.session_state["my_fp"] = my_fp
                st.session_state["my_page"] = 1

            my_page = max(1, int(st.session_state.get("my_page", 1)))

            owner_filter = {"$or": [{"username": uname}, {"user": uname}]}
            my_total = reviews_collection.count_documents(owner_filter)
            my_total_pages = max(1, (my_total + MY_PAGE_SIZE - 1) // MY_PAGE_SIZE)
            if my_page > my_total_pages:
                my_page = my_total_pages
                st.session_state["my_page"] = my_page

            my_skip = (my_page - 1) * MY_PAGE_SIZE
            my_docs = list(
                reviews_collection.find(owner_filter).sort(my_sort_key, my_sort_dir).skip(my_skip).limit(MY_PAGE_SIZE)
            )

            st.caption(f"{my_total} review(s) • Page {my_page} of {my_total_pages}")
            st.markdown("---")
            for review in my_docs:
                rid = str(review["_id"])
                town = review.get("town", "N/A")
                rating = int(review.get("rating", 0) or 0)
                created = review.get("created_at")

                stars = "⭐" * rating
                st.markdown(
                    f"**You** on **{town}** — "
                    f"<span style='color:gold;font-size:18px;'>{stars}</span> "
                    f"<span style='color:gray;'>({rating}/5)</span>",
                    unsafe_allow_html=True
                )

                if isinstance(created, datetime):
                    ts = created.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                else:
                    ts = str(created) if created is not None else "N/A"
                st.caption(f"Posted: {ts}")

                st.markdown(
                    f"""
                    <div style="
                        background-color:#111827;
                        border:1px solid #2e2e2e;
                        border-radius:0.75rem;
                        box-shadow:0 0 6px rgba(0,0,0,0.3);
                        padding:1rem;
                        margin-top:0.5rem;
                        color:#f1f1f1;
                    ">
                        {review.get("review_text","")}
                    </div>
                    """,
                    unsafe_allow_html=True
                )
                st.markdown("---")
                col1, col2 = st.columns(2)
                with col1:
                    with st.expander("Edit your review"):
                        new_text = st.text_area("Edit Review", value=review.get("review_text", ""), key=f"my_txt_{rid}")
                        new_rating = st.slider("Edit Rating", 1, 5, value=int(review.get("rating", 3) or 3), key=f"my_sld_{rid}")
                        if st.button("Save Changes", key=f"my_upd_{rid}"):
                            try:
                                res = reviews_collection.update_one(
                                    {"_id": review["_id"], "$or": [{"username": uname}, {"user": uname}]},
                                    {"$set": {"review_text": new_text.strip(), "rating": int(new_rating)}}
                                )
                                if res.modified_count == 1:
                                    st.success("Review updated!")
                                else:
                                    st.warning("No changes saved.")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Failed to update: {e}")

                with col2:
                    # 1) First click -> ask for confirmation
                    if st.button("Delete Review", key=f"del_{rid}"):
                        st.session_state[f"confirm_del_review_{rid}"] = True
                        st.rerun()

                    # 2) Render confirmation UI (appears inline)
                    if confirm_prompt(
                        state_key=f"confirm_del_review_{rid}",
                        title="Delete this review?",
                        message="This action cannot be undone."
                    ):
                        try:
                            # username-only ownership check
                            uname = st.session_state.get("username")
                            res = reviews_collection.delete_one(
                                {"_id": review["_id"], "$or": [{"username": uname}, {"user": uname}]}
                            )
                            if res.deleted_count == 1:
                                st.success("Review deleted!")
                            else:
                                st.warning("Delete failed (ownership mismatch).")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Failed to delete: {e}")

                st.markdown("---")

            # Bottom pager for My Reviews
            ml, mr = st.columns(2)
            with ml:
                if st.button("◀ Previous", disabled=(my_page <= 1), key="my_prev"):
                    st.session_state["my_page"] = my_page - 1
                    st.rerun()
            with mr:
                if st.button("Next ▶", disabled=(my_page >= my_total_pages), key="my_next"):
                    st.session_state["my_page"] = my_page + 1
                    st.rerun()