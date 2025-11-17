-- =========================================================
-- Create tables + load CSV (IDs start at 1)
-- =========================================================

-- Optional: clean slate (only uncomment if you want to wipe everything)
-- DROP VIEW IF EXISTS Transactions_WithRemainingLease;
-- DROP TABLE IF EXISTS Transactions CASCADE;
-- DROP TABLE IF EXISTS Flats CASCADE;
-- DROP TABLE IF EXISTS FlatModel CASCADE;
-- DROP TABLE IF EXISTS StoreyRange CASCADE;
-- DROP TABLE IF EXISTS Towns CASCADE;
-- DROP TABLE IF EXISTS staging_hdb CASCADE;

-- =========================================================
-- 1) Final schema (IDENTITY columns start at 1)
-- =========================================================
CREATE TABLE IF NOT EXISTS Towns (
  town_id INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY,
  town_name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS Flats (
  flat_id INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY,
  town_id INT NOT NULL REFERENCES Towns(town_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  street_name TEXT NOT NULL,
  block_no TEXT NOT NULL,
  lease_start_year INT NOT NULL,
  CONSTRAINT uq_flat UNIQUE (town_id, street_name, block_no)
);

CREATE TABLE IF NOT EXISTS StoreyRange (
  storey_range_id INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY,
  storey_range TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS FlatModel (
  flat_model_id INT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY,
  flat_model TEXT NOT NULL UNIQUE
);

-- =========================================================
-- Transactions table (persist remaining_lease_months)
-- =========================================================
CREATE TABLE IF NOT EXISTS Transactions (
  txn_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY,
  txn_month DATE NOT NULL,
  txn_price NUMERIC(12,2) NOT NULL,
  floor_area_sqm NUMERIC(8,2) NOT NULL,
  flat_type TEXT NOT NULL,      -- e.g., '3 ROOM'
  flat_model_id INT NOT NULL REFERENCES FlatModel(flat_model_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  storey_range_id INT NOT NULL REFERENCES StoreyRange(storey_range_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  flat_id INT NOT NULL REFERENCES Flats(flat_id) ON UPDATE CASCADE ON DELETE RESTRICT,

  -- Stored (computed via triggers)
  remaining_lease_months INT NOT NULL
);

-- =========================================================
-- 2) Compute + propagate logic
-- =========================================================
CREATE OR REPLACE FUNCTION fn_compute_remaining_lease_months(p_flat_id INT, p_txn_month DATE)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
  v_lease_start_year INT;
  v_year  INT;
  v_month INT;
  v_result INT;
BEGIN
  SELECT lease_start_year INTO v_lease_start_year
  FROM Flats
  WHERE flat_id = p_flat_id;

  IF v_lease_start_year IS NULL OR p_txn_month IS NULL THEN
    RETURN 0;
  END IF;

  v_year  := EXTRACT(YEAR  FROM p_txn_month)::INT;
  v_month := EXTRACT(MONTH FROM p_txn_month)::INT;

  -- 99-year original lease, floor at zero
  v_result := ((v_lease_start_year + 99) - v_year) * 12 - v_month + 12;
  IF v_result < 0 THEN
    v_result := 0;
  END IF;

  RETURN v_result;
END;
$$;

CREATE OR REPLACE FUNCTION trg_transactions_set_remaining_lease()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.remaining_lease_months :=
    fn_compute_remaining_lease_months(NEW.flat_id, NEW.txn_month);
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS before_set_remaining_lease ON Transactions;
CREATE TRIGGER before_set_remaining_lease
BEFORE INSERT OR UPDATE OF txn_month, flat_id
ON Transactions
FOR EACH ROW
EXECUTE FUNCTION trg_transactions_set_remaining_lease();

CREATE OR REPLACE FUNCTION trg_flats_propagate_remaining_lease()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.lease_start_year IS DISTINCT FROM OLD.lease_start_year THEN
    UPDATE Transactions t
    SET remaining_lease_months =
          fn_compute_remaining_lease_months(t.flat_id, t.txn_month)
    WHERE t.flat_id = NEW.flat_id;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS after_flats_update_lease ON Flats;
CREATE TRIGGER after_flats_update_lease
AFTER UPDATE OF lease_start_year
ON Flats
FOR EACH ROW
EXECUTE FUNCTION trg_flats_propagate_remaining_lease();

-- =========================================================
-- 3) Helpful indexes
-- =========================================================
CREATE INDEX IF NOT EXISTS ix_transactions_month   ON Transactions (txn_month);
CREATE INDEX IF NOT EXISTS ix_transactions_flat    ON Transactions (flat_id);
CREATE INDEX IF NOT EXISTS ix_transactions_model   ON Transactions (flat_model_id);
CREATE INDEX IF NOT EXISTS ix_transactions_storey  ON Transactions (storey_range_id);
CREATE INDEX IF NOT EXISTS ix_transactions_remaining ON Transactions (remaining_lease_months);

-- =========================================================
-- 4) Staging table (drop/recreate each run)
-- =========================================================
DROP TABLE IF EXISTS staging_hdb CASCADE;
CREATE TABLE staging_hdb (
  month TEXT,
  town TEXT,
  flat_type TEXT,
  block TEXT,
  street_name TEXT,
  storey_range TEXT,
  floor_area_sqm NUMERIC,
  flat_model TEXT,
  lease_commence_date INTEGER,
  remaining_lease TEXT,
  resale_price NUMERIC
);

-- =========================================================
-- Create User Database Tables
-- These are for transactional user data (logins, watchlists)
-- =========================================================

-- 1) Create Users table
CREATE TABLE IF NOT EXISTS Users (
    UserID SERIAL PRIMARY KEY,
    Username VARCHAR(50) NOT NULL UNIQUE,
    Email VARCHAR(100) NOT NULL UNIQUE,
    PasswordHash VARCHAR(255) NOT NULL, -- Storing a strong hash
    CreatedAt TIMESTAMPTZ DEFAULT NOW()
);

-- 2) Create Watchlist table
CREATE TABLE IF NOT EXISTS Watchlist (
    WatchlistID SERIAL PRIMARY KEY,
    -- Foreign key to our new Users table. Deleting a user deletes their watchlist.
    UserID INTEGER NOT NULL REFERENCES Users(UserID) ON DELETE CASCADE,
    -- This is the "foreign key" to the *other* database (Analytics DB).
    -- It is a plain integer, as we cannot enforce a real cross-database constraint.
    txn_id INTEGER NOT NULL,
    CreatedAt TIMESTAMPTZ DEFAULT NOW(),
    -- Ensures a user cannot add the same flat to their watchlist multiple times
    CONSTRAINT uq_user_txn UNIQUE (UserID, txn_id)
);

-- 3) Create helpful indexes
CREATE INDEX IF NOT EXISTS ix_watchlist_user_id ON Watchlist (UserID);





