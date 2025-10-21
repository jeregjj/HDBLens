import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import SQLAlchemyError
from pymongo import MongoClient, ASCENDING, UpdateOne
from pymongo.server_api import ServerApi
from pymongo.errors import ConnectionFailure
from datetime import datetime
import pandas as pd
import streamlit as st
from filelock import FileLock

load_dotenv()

# ---- Config & paths (relative to this file) ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
APP_LOCK_KEY = int(os.getenv("DB_APP_LOCK_KEY", "742031"))

# --- Analytics SQL DB (Read-Only) ---
ANALYTICS_SCHEMA_SQL_PATH = os.getenv("DB_SETUP_SQL", os.path.join(BASE_DIR, "assets/database_tables_setup.sql"))
ANALYTICS_INSERT_SQL_PATH = os.getenv("DB_INSERT_SQL", os.path.join(BASE_DIR, "assets/insert_table_data.sql"))
ANALYTICS_CSV_PATH = os.getenv("DB_CSV_PATH", os.path.join(BASE_DIR, "assets/hdb-resale-prices.csv"))
ANALYTICS_SQL_DSN = os.getenv("SQL_DSN")

# --- User SQL DB (Transactional) ---
USER_SCHEMA_SQL_PATH = os.getenv("USER_DB_SETUP_SQL", os.path.join(BASE_DIR, "assets/user_database_setup.sql"))
USER_SQL_DSN = os.getenv("USER_SQL_DSN")

# --- Mongo DB (Transactional) ---
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_URI     = os.getenv("MONGO_URI")
REVIEWS_XLSX  = os.getenv("REVIEWS_XLSX", os.path.join(BASE_DIR, "assets"))
MONGO_COLL    = os.getenv("MONGO_COLLECTION")
MONGO_META    = os.getenv("MONGO_META_COLLECTION")

# ---- Single engine factory for ANALYTICS DB ----
_SQL_ENGINE = None
def get_sql_engine() -> Engine | None: # Added | None to type hint
    """
    Return a SQLAlchemy engine for the READ-ONLY Analytics Postgres DB.
    """
    global _SQL_ENGINE
    if _SQL_ENGINE is None:
        if not ANALYTICS_SQL_DSN:
            st.error("FATAL: SQL_DSN not found in .env file for Analytics DB.")
            print("FATAL: SQL_DSN not found in .env file for Analytics DB.")
            return None # Return None on config error
        try:
            print("Creating NEW Analytics SQL Engine")
            _SQL_ENGINE = create_engine(ANALYTICS_SQL_DSN, pool_size=5, max_overflow=5)
            # Test connection briefly
            with _SQL_ENGINE.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Analytics SQL Engine connected successfully.")
        except SQLAlchemyError as e:
            st.error(f"FATAL: Failed to connect to Analytics SQL DB: {e}")
            print(f"FATAL: Failed to connect to Analytics SQL DB: {e}")
            _SQL_ENGINE = None # Ensure it remains None on failure
        except Exception as e: # Catch any other unexpected errors
            st.error(f"FATAL: Unexpected error creating Analytics SQL Engine: {e}")
            print(f"FATAL: Unexpected error creating Analytics SQL Engine: {e}")
            _SQL_ENGINE = None
            
    return _SQL_ENGINE

# ---- Single engine factory for USER DB ----
_USER_SQL_ENGINE = None
def get_user_sql_engine() -> Engine | None: # Added | None to type hint
    """
    Return a SQLAlchemy engine for the TRANSACTIONAL User Postgres DB.
    Handles connection errors gracefully.
    """
    global _USER_SQL_ENGINE
    if _USER_SQL_ENGINE is None:
        if not USER_SQL_DSN:
            # Use st.error for user feedback in Streamlit context
            st.error("FATAL: USER_SQL_DSN not found in .env file.") 
            print("FATAL: USER_SQL_DSN not found in .env file.") # Also print for logs
            return None # Return None on config error
        
        # --- Try/except for engine creation ---
        try:
            print("Creating NEW User SQL Engine")
            _USER_SQL_ENGINE = create_engine(USER_SQL_DSN, pool_size=5, max_overflow=5)
            # Test the connection immediately to catch errors early
            with _USER_SQL_ENGINE.connect() as conn:
                conn.execute(text("SELECT 1")) # Simple query to test connection
            print("User SQL Engine connected successfully.")
        except SQLAlchemyError as e:
            st.error(f"FATAL: Failed to connect to User SQL DB: {e}")
            print(f"FATAL: Failed to connect to User SQL DB: {e}")
            _USER_SQL_ENGINE = None # Ensure it remains None on failure
        except Exception as e: # Catch any other unexpected errors
            st.error(f"FATAL: Unexpected error creating User SQL Engine: {e}")
            print(f"FATAL: Unexpected error creating User SQL Engine: {e}")
            _USER_SQL_ENGINE = None
            
    return _USER_SQL_ENGINE

# ---- Centralized Mongo Client ----
_MONGO_CLIENT = None
def get_mongo_collection(collection_name: str):
    """
    Returns a specific collection object from the MongoDB database.
    Manages a single, shared MongoClient. Handles connection errors gracefully.
    """
    global _MONGO_CLIENT
    if _MONGO_CLIENT is None:
        if not MONGO_URI:
            st.error("FATAL: MONGO_URI not found in .env file.")
            print("FATAL: MONGO_URI not found in .env file.")
            return None # Return None on config error

        # --- Try/except for client creation and ping ---
        try:
            print("Creating NEW Mongo Client")
            _MONGO_CLIENT = MongoClient(MONGO_URI, 
                                        server_api=ServerApi('1'),
                                        serverSelectionTimeoutMS=5000) # Timeout after 5s
            
            # The ismaster command is cheap and does not require auth.
            _MONGO_CLIENT.admin.command('ismaster') 
            print("Pinged MongoDB deployment. Successful connection!")

        except ConnectionFailure as e:
            st.error(f"FATAL: Failed to connect to MongoDB: {e}")
            print(f"FATAL: Failed to connect to MongoDB: {e}")
            _MONGO_CLIENT = None # Ensure it remains None on failure
        except Exception as e: # Catch any other unexpected errors
            st.error(f"FATAL: Unexpected error creating Mongo Client: {e}")
            print(f"FATAL: Unexpected error creating Mongo Client: {e}")
            _MONGO_CLIENT = None

    # Proceed only if the client was successfully created
    if _MONGO_CLIENT:
        try:
            db = _MONGO_CLIENT[MONGO_DB_NAME]
            return db[collection_name]
        except Exception as e:
            st.error(f"Error accessing MongoDB database/collection: {e}")
            print(f"Error accessing MongoDB database/collection: {e}")
            return None # Return None if DB/collection access fails
    
    # If client creation failed earlier
    return None

# ---- Initialization logic ----

@st.cache_resource(ttl=3600)
def init_sql_db() -> tuple[bool, str]:
    """
    Idempotent function to initialize the ANALYTICS SQL DB.
    Uses a lock to prevent race conditions.
    """
    lock = FileLock(f"{__file__}.analytics.lock")
    try:
        with lock.acquire(timeout=5):
            print("Attempting to init Analytics SQL DB...")
            engine = get_sql_engine()
            meta_table = "db_meta"
            schema_version = "v1.0"
            
            with engine.connect() as conn:
                with conn.begin() as trans:
                    conn.execute(text(f"""
                        CREATE TABLE IF NOT EXISTS {meta_table} (
                            key VARCHAR(50) PRIMARY KEY,
                            val VARCHAR(100) NOT NULL,
                            updated_at TIMESTAMPTZ DEFAULT NOW()
                        );
                    """))
                    res = conn.execute(text(f"SELECT val FROM {meta_table} WHERE key = 'schema_version'")).fetchone()
                    
                    if res and res[0] == schema_version:
                        return (True, "Analytics SQL DB: Already initialized.")
                    
                    print(f"Analytics SQL DB: Schema not found or version mismatch. Applying setup...")
                    
                    # Run the main schema setup
                    with open(ANALYTICS_SCHEMA_SQL_PATH, "r") as f:
                        conn.execute(text(f.read()))
                    
                    # Run the data insertion
                    with open(ANALYTICS_INSERT_SQL_PATH, "r") as f:
                        sql = f.read().replace(":csv_path", pg_escape(ANALYTICS_CSV_PATH))
                        conn.execute(text(sql))

                    # Update meta version
                    conn.execute(text(f"""
                        INSERT INTO {meta_table} (key, val) VALUES ('schema_version', :version)
                        ON CONFLICT (key) DO UPDATE SET val = :version;
                    """), {"version": schema_version})
                    # trans.commit()
                    
            return (True, "Analytics SQL DB: Initialized successfully.")
    except Exception as e:
        return (False, f"Analytics SQL DB Error: {e}")

@st.cache_resource(ttl=3600)
def init_user_db() -> tuple[bool, str]:
    """
    Idempotent function to initialize the USER SQL DB.
    Uses a lock to prevent race conditions.
    """
    lock = FileLock(f"{__file__}.user.lock")
    try:
        with lock.acquire(timeout=5):
            print("Attempting to init User SQL DB...")
            engine = get_user_sql_engine()
            meta_table = "db_meta"
            schema_version = "v1.0"
            
            with engine.connect() as conn:
                with conn.begin() as trans:
                    # Create meta table
                    conn.execute(text(f"""
                        CREATE TABLE IF NOT EXISTS {meta_table} (
                            key VARCHAR(50) PRIMARY KEY,
                            val VARCHAR(100) NOT NULL,
                            updated_at TIMESTAMPTZ DEFAULT NOW()
                        );
                    """))
                    # Check version
                    res = conn.execute(text(f"SELECT val FROM {meta_table} WHERE key = 'schema_version'")).fetchone()
                    
                    if res and res[0] == schema_version:
                        return (True, "User SQL DB: Already initialized.")
                    
                    print(f"User SQL DB: Schema not found or version mismatch. Applying setup...")
                    
                    # Run the user schema setup
                    with open(USER_SCHEMA_SQL_PATH, "r") as f:
                        conn.execute(text(f.read()))

                    # Update meta version
                    conn.execute(text(f"""
                        INSERT INTO {meta_table} (key, val) VALUES ('schema_version', :version)
                        ON CONFLICT (key) DO UPDATE SET val = :version;
                    """), {"version": schema_version})
                    # trans.commit()
                    
            return (True, "User SQL DB: Initialized successfully.")
    except Exception as e:
        return (False, f"User SQL DB Error: {e}")

@st.cache_resource(ttl=3600)
def init_mongo() -> tuple[bool, str]:
    """
    Idempotent function to initialize Mongo DB (load data).
    """
    lock = FileLock(f"{__file__}.mongo.lock")
    try:
        with lock.acquire(timeout=5):
            print("Attempting to init Mongo DB...")
            coll = get_mongo_collection(MONGO_COLL)
            meta = get_mongo_collection(MONGO_META)
            schema_version = "v1.0_excel"

            if coll is None or meta is None:
                return (False, "Mongo DB: Connection failed.")

            res = meta.find_one({"_id": "mongo_schema_version"})
            if res and res.get("val") == schema_version:
                return (True, "Mongo DB: Already initialized.")

            print(f"Mongo DB: Schema not found or version mismatch. Applying setup...")
            
            # Check if excel file exists
            if not os.path.exists(REVIEWS_XLSX):
                return(False, f"Mongo DB Error: Reviews Excel file not found at {REVIEWS_XLSX}")
            
            df = pd.read_excel(REVIEWS_XLSX)
            df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

            ops = []
            for r in df.to_dict("records"):
                if pd.notna(r.get("review_id")):
                    filt = {"review_id": int(r["review_id"])}
                else:
                    filt = {
                        "town": r.get("town"),
                        "username": r.get("username "),
                        "created_at": r.get("created_at"),
                        "review_text": r.get("review_text"),
                    }
                doc = {k: v for k, v in r.items() if pd.notna(v)}
                ops.append(UpdateOne(filt, {"$set": doc}, upsert=True))

            if ops:
                coll.bulk_write(ops, ordered=False)

            # Idempotent index creation
            try: # Use try-except as create_index raises error if options differ for existing index
                coll.create_index([("review_id", ASCENDING)], unique=True, sparse=True)
            except Exception as idx_err:
                 print(f"Info: Index on review_id likely exists or options differ: {idx_err}")
            try:
                coll.create_index([("town", ASCENDING)])
            except Exception as idx_err:
                 print(f"Info: Index on town likely exists or options differ: {idx_err}")
            try:
                coll.create_index([("created_at", ASCENDING)])
            except Exception as idx_err:
                 print(f"Info: Index on created_at likely exists or options differ: {idx_err}")

            meta.update_one(
                {"_id": "mongo_schema_version"},
                {"$set": {"val": schema_version, "updated_at": datetime.now()}},
                upsert=True
            )
            return (True, "Mongo DB: Initialized successfully.")
    except Exception as e:
        return (False, f"Mongo DB Error: {e}")

# Helper for init_sql_db
def pg_escape(s: str) -> str:
    return s.replace("'", "''")
