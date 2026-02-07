-- ============================================================
-- Restaurant Review Data Warehouse
-- Star Schema Design
-- ============================================================

-- ─── Dimension: Restaurants ────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_restaurants (
    place_id        VARCHAR(255) PRIMARY KEY,
    name            VARCHAR(500) NOT NULL,
    address         TEXT,
    latitude        DECIMAL(10, 7),
    longitude       DECIMAL(10, 7),
    rating          DECIMAL(3, 2),
    total_ratings   INTEGER DEFAULT 0,
    price_level     INTEGER,
    types           TEXT,           -- comma-separated list
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_restaurants_rating
    ON dim_restaurants (rating DESC);

CREATE INDEX IF NOT EXISTS idx_dim_restaurants_location
    ON dim_restaurants (latitude, longitude);


-- ─── Dimension: Time ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_time (
    full_date       DATE PRIMARY KEY,
    year            INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    day             INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,
    week_of_year    INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_time_year_month
    ON dim_time (year, month);


-- ─── Dimension: Users (Reviewers) ──────────────────────────
CREATE TABLE IF NOT EXISTS dim_users (
    user_id         SERIAL PRIMARY KEY,
    author_name     VARCHAR(500) NOT NULL,
    first_seen      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    review_count    INTEGER DEFAULT 0,
    UNIQUE (author_name)
);


-- ─── Fact: Reviews ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_reviews (
    review_id       VARCHAR(64) PRIMARY KEY,
    place_id        VARCHAR(255) NOT NULL REFERENCES dim_restaurants(place_id),
    author_name     VARCHAR(500),
    rating          DECIMAL(3, 2),
    review_text     TEXT,
    review_length   INTEGER,
    word_count      INTEGER,
    review_date     TIMESTAMP WITH TIME ZONE,
    review_age_days INTEGER,
    loaded_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_reviews_place_id
    ON fact_reviews (place_id);

CREATE INDEX IF NOT EXISTS idx_fact_reviews_rating
    ON fact_reviews (rating);

CREATE INDEX IF NOT EXISTS idx_fact_reviews_date
    ON fact_reviews (review_date DESC);


-- ─── Fact: NLP Predictions ─────────────────────────────────
CREATE TABLE IF NOT EXISTS fact_predictions (
    review_id       VARCHAR(64) PRIMARY KEY REFERENCES fact_reviews(review_id),
    place_id        VARCHAR(255) NOT NULL REFERENCES dim_restaurants(place_id),
    predicted_score DECIMAL(3, 2) NOT NULL,
    sentiment_label VARCHAR(50) NOT NULL,
    confidence      DECIMAL(5, 4),
    model_version   VARCHAR(100),
    predicted_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fact_predictions_place_id
    ON fact_predictions (place_id);

CREATE INDEX IF NOT EXISTS idx_fact_predictions_sentiment
    ON fact_predictions (sentiment_label);

CREATE INDEX IF NOT EXISTS idx_fact_predictions_score
    ON fact_predictions (predicted_score DESC);


-- ─── Materialized View: Restaurant Sentiment Summary ───────
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_restaurant_sentiment AS
SELECT
    r.place_id,
    r.name,
    r.address,
    r.rating               AS google_rating,
    r.total_ratings,
    r.price_level,
    COUNT(p.review_id)      AS nlp_review_count,
    ROUND(AVG(p.predicted_score)::numeric, 2)  AS avg_nlp_score,
    ROUND(AVG(p.confidence)::numeric, 3)       AS avg_confidence,
    SUM(CASE WHEN p.sentiment_label IN ('positive', 'very_positive')
             THEN 1 ELSE 0 END)               AS positive_count,
    SUM(CASE WHEN p.sentiment_label IN ('negative', 'very_negative')
             THEN 1 ELSE 0 END)               AS negative_count,
    SUM(CASE WHEN p.sentiment_label = 'neutral'
             THEN 1 ELSE 0 END)               AS neutral_count,
    ROUND(
        SUM(CASE WHEN p.sentiment_label IN ('positive', 'very_positive')
                 THEN 1 ELSE 0 END)::numeric
        / NULLIF(COUNT(p.review_id), 0), 3
    )                                          AS positive_ratio
FROM dim_restaurants r
LEFT JOIN fact_predictions p ON r.place_id = p.place_id
GROUP BY r.place_id, r.name, r.address, r.rating, r.total_ratings, r.price_level;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_restaurant_sentiment_place_id
    ON mv_restaurant_sentiment (place_id);
