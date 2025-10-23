-- 4) Seed lookups
INSERT INTO Towns (town_name)
SELECT DISTINCT TRIM(town)
FROM staging_hdb
WHERE town IS NOT NULL
ON CONFLICT (town_name) DO NOTHING;

INSERT INTO StoreyRange (storey_range)
SELECT DISTINCT TRIM(storey_range)
FROM staging_hdb
WHERE storey_range IS NOT NULL
ON CONFLICT (storey_range) DO NOTHING;

INSERT INTO FlatModel (flat_model)
SELECT DISTINCT TRIM(flat_model)
FROM staging_hdb
WHERE flat_model IS NOT NULL
ON CONFLICT (flat_model) DO NOTHING;

-- 5) UPSERT Flats with a de-duplicated source (prevents ON CONFLICT double-hit)
WITH flats_src AS (
  SELECT
    TRIM(town)        AS town,
    TRIM(street_name) AS street_name,
    TRIM(block)       AS block_no,
    MIN(lease_commence_date) AS lease_start_year
  FROM staging_hdb
  WHERE town IS NOT NULL
    AND street_name IS NOT NULL
    AND block IS NOT NULL
    AND lease_commence_date IS NOT NULL
  GROUP BY TRIM(town), TRIM(street_name), TRIM(block)
)
INSERT INTO Flats (town_id, street_name, block_no, lease_start_year)
SELECT t.town_id, s.street_name, s.block_no, s.lease_start_year
FROM flats_src s
JOIN Towns t ON t.town_name = s.town
ON CONFLICT (town_id, street_name, block_no)
DO UPDATE SET lease_start_year = EXCLUDED.lease_start_year;

-- 6) Insert facts (Transactions) with remaining_lease_months computed
INSERT INTO Transactions (
  txn_month,
  txn_price,
  floor_area_sqm,
  flat_type,
  flat_model_id,
  storey_range_id,
  flat_id,
  remaining_lease_months
)
SELECT
  TO_DATE(TRIM(s.month), 'YYYY-MM') AS txn_month,
  s.resale_price,
  s.floor_area_sqm,
  TRIM(s.flat_type) AS flat_type,
  fm.flat_model_id,
  sr.storey_range_id,
  f.flat_id,
  GREATEST(
    0,
    ((f.lease_start_year + 99) - EXTRACT(YEAR FROM TO_DATE(TRIM(s.month), 'YYYY-MM'))) * 12
    - EXTRACT(MONTH FROM TO_DATE(TRIM(s.month), 'YYYY-MM')) + 12
  )::INT AS remaining_lease_months
FROM staging_hdb s
JOIN Towns t  ON t.town_name = TRIM(s.town)
JOIN Flats f  ON f.town_id = t.town_id
             AND f.street_name = TRIM(s.street_name)
             AND f.block_no    = TRIM(s.block)
JOIN StoreyRange sr ON sr.storey_range = TRIM(s.storey_range)
JOIN FlatModel fm   ON fm.flat_model   = TRIM(s.flat_model);