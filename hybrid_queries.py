import os
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from db_config import get_sql_engine, get_mongo_collection

ENGINE = get_sql_engine()
REV = get_mongo_collection(os.getenv("MONGO_COLLECTION"))

# ---------- Hybrid Overview (tiles on Home) ----------
def hybrid_overview(months: int = 1) -> dict:
    """
    Returns a tiny snapshot for the Home tiles:
      - tx_this_month    (Postgres: Transactions in current calendar month)
      - avg_price_all    (Postgres: overall average txn_price)
      - avg_rating       (Mongo: overall average rating)
      - most_reviewed_*  (Mongo: town with most reviews)
    """
    # SQL side
    with ENGINE.begin() as conn:
        row = conn.execute(text("""
            SELECT
              COUNT(*) FILTER (
                WHERE date_trunc('month', txn_month) = date_trunc('month', CURRENT_DATE)
              ) AS tx_this_month,
              ROUND(AVG(txn_price))::bigint AS avg_price_all
            FROM Transactions;
        """)).mappings().first() or {}
    tx_this_month = int(row.get("tx_this_month") or 0)
    avg_price_all = int(row.get("avg_price_all") or 0)

    # Mongo side
    avg_doc = next(REV.aggregate([
        {"$group": {"_id": None, "avg_rating": {"$avg": "$rating"}}}
    ]), None)
    avg_rating = round(avg_doc["avg_rating"], 2) if avg_doc and avg_doc.get("avg_rating") is not None else None

    top = next(REV.aggregate([
        {"$group": {"_id": "$town", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}, {"$limit": 1}
    ]), {"_id": "—", "count": 0})

    return {
        "tx_this_month": tx_this_month,
        "avg_price_all": avg_price_all,
        "avg_rating": avg_rating,
        "most_reviewed_town": top["_id"],
        "most_reviewed_count": top["count"],
    }

# ---------- Hybrid Affordability (table + bubble on Home) ----------
def hybrid_affordability(flat_type: str, budget: float, months: int = 12) -> pd.DataFrame:
    """
    Rank towns by a hybrid score combining:
      Postgres (last N months):
        - median_price per Towns.town_name
        - txn_count
      Mongo:
        - avg_rating, reviews_count
        - recent_reviews (last 12 months) for a small recency boost

    Returns DataFrame with:
      ['town','median_price','txn_count','avg_rating','reviews_count','hybrid_score', ...]
    """
    # ---- Postgres: median price + volume by town (last N months) ----
    sql = text(f"""
    WITH recent AS (
      SELECT
        tn.town_name AS town,
        t.txn_price::numeric AS price
      FROM Transactions t
      JOIN Flats f   ON f.flat_id  = t.flat_id
      JOIN Towns tn  ON tn.town_id = f.town_id
      WHERE t.txn_month >= (date_trunc('month', current_date) - interval '{months} months')::date
        AND t.flat_type = :ft
    ),
    agg AS (
      SELECT
        town,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
        COUNT(*) AS txn_count
      FROM recent
      GROUP BY town
    )
    SELECT town, median_price, txn_count
    FROM agg
    WHERE txn_count >= 10
    ORDER BY town;
    """)
    with ENGINE.begin() as conn:
        df_pg = pd.DataFrame(conn.execute(sql, {"ft": flat_type}).mappings())
    if df_pg.empty:
        return df_pg  # nothing to rank

    # ---- Mongo: ratings volume + recency by town ----
    since = datetime.utcnow() - timedelta(days=365)
    mg_rows = list(REV.aggregate([
        {"$group": {
            "_id": "$town",
            "avg_rating": {"$avg": "$rating"},
            "reviews_count": {"$sum": 1},
            "recent_reviews": {
                "$sum": {"$cond": [{"$gte": ["$created_at", since]}, 1, 0]}
            },
            "last_review_at": {"$max": "$created_at"}
        }}
    ]))
    df_mg = pd.DataFrame([{
        "town": d["_id"],
        "avg_rating": d.get("avg_rating"),
        "reviews_count": d.get("reviews_count", 0),
        "recent_reviews": d.get("recent_reviews", 0),
        "last_review_at": d.get("last_review_at"),
    } for d in mg_rows])

    # ---- Merge + score ----
    out = df_pg.merge(df_mg, on="town", how="left")

    # Price index: closer/below budget is better → use inverse
    out["price_index"] = out["median_price"] / float(budget)

    # Rating index: scale to [0..1], add small boosts for volume and recency
    out["rating_scaled"] = (out["avg_rating"].fillna(0) / 5.0)
    vol_boost = (out["reviews_count"].fillna(0) / 100.0).clip(0, 0.30)      # up to +30%
    rec_boost = (
        (out["recent_reviews"].fillna(0) / out["reviews_count"].replace(0, 1))
        .clip(0, 1) * 0.20                                                  # up to +20%
    )
    out["rating_index"] = out["rating_scaled"] * (1 + vol_boost + rec_boost)

    # Final hybrid score (tune weights as desired)
    w_price, w_rating = 0.60, 0.40
    out["hybrid_score"] = w_price * (1 / out["price_index"]) + w_rating * out["rating_index"]

    # Nice ordering
    out = out.sort_values("hybrid_score", ascending=False).reset_index(drop=True)
    return out

# -- Town Profile -- 
def town_profile(town: str, flat_type: str | None = None, months: int = 12) -> dict:
    """
    Hybrid profile for a town:
      • Postgres: median, p25, p75, txn_count in last N months
      • Mongo: avg rating, reviews_count, latest 3 reviews
    Uses your real schema: Transactions → Flats → Towns (town_name), txn_price, txn_month.
    """
    town_u = (town or "").strip().upper()

    # --- Postgres side (uses your actual tables/columns) ---
    sql = text(f"""
    WITH recent AS (
      SELECT
        t.txn_price::numeric AS price
      FROM Transactions t
      JOIN Flats f  ON f.flat_id  = t.flat_id
      JOIN Towns tn ON tn.town_id = f.town_id
      WHERE tn.town_name = :town
        AND t.txn_month >= (date_trunc('month', current_date) - interval '{months} months')::date
        {"AND t.flat_type = :ft" if flat_type else ""}
    )
    SELECT
      percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median_price,
      percentile_cont(0.25) WITHIN GROUP (ORDER BY price) AS p25,
      percentile_cont(0.75) WITHIN GROUP (ORDER BY price) AS p75,
      COUNT(*) AS txn_count
    FROM recent;
    """)
    params = {"town": town_u}
    if flat_type:
        params["ft"] = flat_type  # note: in your data this looks like '3 ROOM', '4 ROOM', etc. :contentReference[oaicite:4]{index=4}

    with ENGINE.begin() as conn:
        row = conn.execute(sql, params).mappings().first() or {}

    # --- Mongo side (Atlas): ratings + latest 3 reviews ---
    mg = list(REV.aggregate([
        {"$match": {"town": town_u}},
        {"$group": {"_id": "$town", "avg_rating": {"$avg": "$rating"}, "reviews_count": {"$sum": 1}}},
    ]))
    avg_rating = round(mg[0]["avg_rating"], 2) if mg and mg[0].get("avg_rating") is not None else None
    reviews_count = mg[0]["reviews_count"] if mg else 0

    latest = list(
        REV.find({"town": town_u}, {"_id": 0})
           .sort("created_at", -1)
           .limit(3)
    )

    return {
        "town": town_u,
        "median_price": row.get("median_price"),
        "p25": row.get("p25"),
        "p75": row.get("p75"),
        "txn_count": row.get("txn_count", 0),
        "avg_rating": avg_rating,
        "reviews_count": reviews_count,
        "latest_reviews": latest,
    }