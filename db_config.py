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
#USER_SCHEMA_SQL_PATH = os.getenv("USER_DB_SETUP_SQL", os.path.join(BASE_DIR, "assets/user_database_setup.sql"))
#USER_SQL_DSN = os.getenv("USER_SQL_DSN")

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


def load_staging_from_csv(engine, csv_path: str):
        """
        Fast bulk-load into staging_hdb using COPY (requires psycopg2 driver).
        Assumes CSV has a header row with these columns:
        month,town,flat_type,block,street_name,storey_range,floor_area_sqm,
        flat_model,lease_commence_date,remaining_lease,resale_price
        """
        copy_sql = """
            COPY staging_hdb (
                month,
                town,
                flat_type,
                block,
                street_name,
                storey_range,
                floor_area_sqm,
                flat_model,
                lease_commence_date,
                remaining_lease,
                resale_price
            )
            FROM STDIN WITH (FORMAT csv, HEADER true)
        """
        # Manually handle connection lifecycle (no context manager)
        raw_conn = engine.raw_connection()
        try:
            cur = raw_conn.cursor()
            # Make it idempotent
            cur.execute("TRUNCATE TABLE staging_hdb;")
            with open(ANALYTICS_CSV_PATH, "r", encoding="utf-8") as f:
                cur.copy_expert(copy_sql, f)
            raw_conn.commit()
        finally:
            try:
                cur.close()
            except Exception:
                pass
            raw_conn.close()

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
                with conn.begin():
                    # Ensure meta table
                    conn.execute(text(f"""
                        CREATE TABLE IF NOT EXISTS {meta_table} (
                            key VARCHAR(50) PRIMARY KEY,
                            val VARCHAR(100) NOT NULL,
                            updated_at TIMESTAMPTZ DEFAULT NOW()
                        );
                    """))
                    res = conn.execute(text(f"SELECT val FROM {meta_table} WHERE key = 'schema_version'")).fetchone()

                    if res and res[0] == schema_version:
                        return (True, "Analytics SQL DB: Successfully initialized.")

                    print("Analytics SQL DB: Schema not found or version mismatch. Applying setup...")

                    # 1) Run the main schema setup (creates staging_hdb + final tables)
                    with open(ANALYTICS_SCHEMA_SQL_PATH, "r", encoding="utf-8") as f:
                        conn.execute(text(f.read()))

            # 2) Load CSV into staging_hdb (outside the previous transaction; COPY needs raw_connection)
            load_staging_from_csv(engine, ANALYTICS_CSV_PATH)

            # 3) Run the propagation SQL to fill Towns/Flats/…/Transactions from staging_hdb
            with engine.connect() as conn:
                with conn.begin():
                    with open(ANALYTICS_INSERT_SQL_PATH, "r", encoding="utf-8") as f:
                        # If your file previously expected :csv_path, it’s no longer needed.
                        sql = f.read()
                        conn.execute(text(sql))

                    # 4) Update meta version
                    conn.execute(
                        text(f"""
                            INSERT INTO {meta_table} (key, val) VALUES ('schema_version', :version)
                            ON CONFLICT (key) DO UPDATE SET val = :version, updated_at = NOW();
                        """),
                        {"version": schema_version},
                    )

            return (True, "Analytics SQL DB: Initialized successfully.")
    except Exception as e:
        return (False, f"Analytics SQL DB Error: {e}")

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
                return (True, "Mongo DB: Successfully initialized.")

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
