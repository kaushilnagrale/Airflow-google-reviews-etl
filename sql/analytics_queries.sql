-- ============================================================
-- Analytical Query Library
-- For Power BI dashboards and ad-hoc analysis
-- ============================================================

-- ─── 1. Top Restaurants by NLP Score ───────────────────────
-- Used by: Power BI "Top Restaurants" visual
SELECT
    r.name,
    r.address,
    r.google_rating,
    r.avg_nlp_score,
    r.nlp_review_count,
    r.positive_ratio,
    r.price_level,
    RANK() OVER (ORDER BY r.avg_nlp_score DESC) AS nlp_rank,
    RANK() OVER (ORDER BY r.google_rating DESC) AS google_rank
FROM mv_restaurant_sentiment r
WHERE r.nlp_review_count >= 5
ORDER BY r.avg_nlp_score DESC
LIMIT 50;


-- ─── 2. Sentiment Distribution Over Time ──────────────────
-- Used by: Power BI time-series chart
SELECT
    t.year,
    t.month,
    p.sentiment_label,
    COUNT(*)                                    AS review_count,
    ROUND(AVG(p.predicted_score)::numeric, 2)   AS avg_score
FROM fact_predictions p
JOIN fact_reviews fr ON p.review_id = fr.review_id
JOIN dim_time t ON DATE(fr.review_date) = t.full_date
GROUP BY t.year, t.month, p.sentiment_label
ORDER BY t.year, t.month;


-- ─── 3. NLP Score vs Google Rating Comparison ──────────────
-- Used by: Power BI scatter plot
SELECT
    r.name,
    r.google_rating,
    r.avg_nlp_score,
    r.nlp_review_count,
    ABS(r.google_rating - r.avg_nlp_score) AS score_gap,
    CASE
        WHEN r.avg_nlp_score > r.google_rating + 0.5 THEN 'Underrated by Google'
        WHEN r.avg_nlp_score < r.google_rating - 0.5 THEN 'Overrated by Google'
        ELSE 'Aligned'
    END AS score_alignment
FROM mv_restaurant_sentiment r
WHERE r.nlp_review_count >= 5;


-- ─── 4. Review Volume Trends ──────────────────────────────
-- Used by: Power BI area chart with anomaly detection
SELECT
    DATE(fr.review_date)                          AS review_day,
    COUNT(*)                                      AS daily_reviews,
    ROUND(AVG(p.predicted_score)::numeric, 2)     AS avg_daily_score,
    AVG(COUNT(*)) OVER (
        ORDER BY DATE(fr.review_date)
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                             AS rolling_7d_avg
FROM fact_reviews fr
JOIN fact_predictions p ON fr.review_id = p.review_id
WHERE fr.review_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY DATE(fr.review_date)
ORDER BY review_day;


-- ─── 5. Price Level vs Sentiment Analysis ──────────────────
SELECT
    r.price_level,
    COUNT(DISTINCT r.place_id)                    AS restaurant_count,
    ROUND(AVG(r.avg_nlp_score)::numeric, 2)       AS avg_sentiment_score,
    ROUND(AVG(r.positive_ratio)::numeric, 3)       AS avg_positive_ratio,
    ROUND(AVG(r.google_rating)::numeric, 2)        AS avg_google_rating
FROM mv_restaurant_sentiment r
WHERE r.price_level IS NOT NULL
GROUP BY r.price_level
ORDER BY r.price_level;


-- ─── 6. Model Confidence Analysis ──────────────────────────
SELECT
    CASE
        WHEN p.confidence >= 0.9 THEN 'High (≥0.9)'
        WHEN p.confidence >= 0.7 THEN 'Medium (0.7-0.9)'
        WHEN p.confidence >= 0.5 THEN 'Low (0.5-0.7)'
        ELSE 'Very Low (<0.5)'
    END                                           AS confidence_tier,
    COUNT(*)                                      AS prediction_count,
    ROUND(AVG(p.predicted_score)::numeric, 2)     AS avg_predicted,
    ROUND(AVG(fr.rating)::numeric, 2)             AS avg_actual,
    ROUND(AVG(ABS(p.predicted_score - fr.rating))::numeric, 2) AS avg_error
FROM fact_predictions p
JOIN fact_reviews fr ON p.review_id = fr.review_id
GROUP BY confidence_tier
ORDER BY confidence_tier;


-- ─── 7. Refresh Materialized View ──────────────────────────
-- Run after loading new predictions
REFRESH MATERIALIZED VIEW CONCURRENTLY mv_restaurant_sentiment;
