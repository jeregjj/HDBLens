-- =========================================================
-- Create User Database Tables
-- This schema is for transactional user data (logins, watchlists)
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