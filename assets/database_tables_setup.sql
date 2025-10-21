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

-- 1) Create final schema (IDENTITY columns start at 1)
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

CREATE TABLE IF NOT EXISTS Transactions (
  txn_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY,
  txn_month DATE NOT NULL,
  txn_price NUMERIC(12,2) NOT NULL,
  floor_area_sqm NUMERIC(8,2) NOT NULL,
  flat_type TEXT NOT NULL,      -- e.g., '3 ROOM'
  flat_model_id INT NOT NULL REFERENCES FlatModel(flat_model_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  storey_range_id INT NOT NULL REFERENCES StoreyRange(storey_range_id) ON UPDATE CASCADE ON DELETE RESTRICT,
  flat_id INT NOT NULL REFERENCES Flats(flat_id) ON UPDATE CASCADE ON DELETE RESTRICT
);

-- View: remaining lease in months (assumes 99-year original lease)
CREATE OR REPLACE VIEW Transactions_WithRemainingLease AS
SELECT
  t.txn_id,
  t.txn_month,
  t.txn_price,
  t.floor_area_sqm,
  t.flat_type,
  fm.flat_model,
  sr.storey_range,
  t.flat_id,
  GREATEST(
    0,
    ((f.lease_start_year + 99) - EXTRACT(YEAR FROM t.txn_month)) * 12
    - EXTRACT(MONTH FROM t.txn_month) + 12
  )::INT AS remaining_lease_months_calc
FROM Transactions t
JOIN Flats f        ON t.flat_id = f.flat_id
JOIN StoreyRange sr ON t.storey_range_id = sr.storey_range_id
JOIN FlatModel fm   ON t.flat_model_id = fm.flat_model_id;

-- Helpful indexes
CREATE INDEX IF NOT EXISTS ix_transactions_month   ON Transactions (txn_month);
CREATE INDEX IF NOT EXISTS ix_transactions_flat    ON Transactions (flat_id);
CREATE INDEX IF NOT EXISTS ix_transactions_model   ON Transactions (flat_model_id);
CREATE INDEX IF NOT EXISTS ix_transactions_storey  ON Transactions (storey_range_id);

-- 2) Create staging table (drop/recreate each run for reproducibility)
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

-- 3) Load CSV into staging (psql client-side COPY)
-- Replace the path below and run this line in psql after the file executes:
-- \COPY staging_hdb FROM '/absolute/path/hdb-resale-prices.csv' CSV HEADER

-- If want IDs to start at 1 again, run:
-- TRUNCATE TABLE Transactions, Flats, FlatModel, StoreyRange, Towns RESTART IDENTITY CASCADE;





