# views/reviews.py
# Full CRUD implementation for MongoDB, refactored to use db_config.

import streamlit as st
import os
from db_config import get_mongo_collection # Import our new centralized function
from datetime import datetime
from bson.objectid import ObjectId

# Get collection names from environment variables, with defaults
MONGO_COLL = os.getenv("MONGO_COLLECTION", "town_reviews")
MONGO_META = os.getenv("MONGO_META_COLLECTION", "meta")

def get_collections():
    """Helper function to get both collections."""
    reviews_collection = get_mongo_collection(MONGO_COLL)
    meta_collection = get_mongo_collection(MONGO_META)
    return reviews_collection, meta_collection

def app():
    st.title("✍️ User Reviews")
    st.markdown("Share and read reviews from other users.")

    reviews_collection, meta_collection = get_collections()

    if reviews_collection is None:
        st.error("Failed to connect to MongoDB. Please check the server connection.")
        return

    # --- 1. CREATE: Form to add a new review ---
    st.subheader("Leave a Review")
    
    # Get list of towns from meta collection for the dropdown
    # We can pre-populate this or get it from the SQL DB later
    try:
        town_data = meta_collection.find_one({"_id": "distinct_towns"})
        if town_data:
            town_list = town_data.get("values", ["ANG MO KIO"]) # Default
        else:
            town_list = ["ANG MO KIO", "BEDOK", "PUNGGOL", "TAMPINES", "YISHUN"]
    except Exception as e:
        st.error(f"Could not load town list: {e}")
        town_list = []

    with st.form(key="review_form", clear_on_submit=True):
        town = st.selectbox("Town", options=town_list)
        # Use session_state to pre-fill username if logged in
        default_user = st.session_state['username'] if st.session_state['logged_in'] else "Anonymous"
        user = st.text_input("Your Name", value=default_user)
        
        rating = st.slider("Rating (1-5)", 1, 5, 3)
        review_text = st.text_area("Your Review")
        
        submit_button = st.form_submit_button("Post Review")

    if submit_button:
        if not review_text:
            st.error("Please write a review before posting.")
        else:
            try:
                review_doc = {
                    "town": town,
                    "user": user,
                    "rating": rating,
                    "review_text": review_text,
                    "created_at": datetime.now()
                }
                reviews_collection.insert_one(review_doc)
                st.success("Review posted successfully!")
            except Exception as e:
                st.error(f"Failed to post review: {e}")

    st.markdown("---")

    # --- 2. READ: Display existing reviews ---
    st.subheader("Recent Reviews")

    # Filter by town
    selected_town = st.selectbox("Filter by Town", options=["All Towns"] + town_list)

    try:
        query = {}
        if selected_town != "All Towns":
            query = {"town": selected_town}
        
        # Sort by most recent
        all_reviews = list(reviews_collection.find(query).sort("created_at", -1).limit(50))

        if not all_reviews:
            st.info("No reviews found for this town.")
        
        for review in all_reviews:
            st.markdown(f"**{review.get('user', 'N/A')}** on {review.get('town', 'N/A')} ({review.get('rating', 'N/A')}★)")
            st.text(f"Posted on: {review.get('created_at', 'N/A').strftime('%Y-%m-%d %H:%M')}")
            st.info(f"{review.get('review_text', '')}")
            
            # --- 3. DELETE & 4. UPDATE ---
            # Only show delete/edit buttons if the user is logged in AND is the author
            if st.session_state['logged_in'] and st.session_state['username'] == review.get('user'):
                col1, col2 = st.columns([1, 1])
                
                with col1:
                    # UPDATE
                    with st.expander("Edit"):
                        new_text = st.text_area("Edit Review", value=review.get('review_text'), key=f"txt_{review['_id']}")
                        new_rating = st.slider("Edit Rating", 1, 5, value=review.get('rating'), key=f"sld_{review['_id']}")
                        if st.button("Save Changes", key=f"upd_{review['_id']}"):
                            try:
                                reviews_collection.update_one(
                                    {"_id": review['_id']},
                                    {"$set": {"review_text": new_text, "rating": new_rating}}
                                )
                                st.success("Review updated!")
                                st.experimental_rerun()
                            except Exception as e:
                                st.error(f"Failed to update: {e}")

                with col2:
                    # DELETE
                    if st.button("Delete", key=f"del_{review['_id']}"):
                        try:
                            reviews_collection.delete_one({"_id": review['_id']})
                            st.success("Review deleted!")
                            st.experimental_rerun()
                        except Exception as e:
                            st.error(f"Failed to delete: {e}")

            st.markdown("---")

    except Exception as e:
        st.error(f"An error occurred while fetching reviews: {e}")