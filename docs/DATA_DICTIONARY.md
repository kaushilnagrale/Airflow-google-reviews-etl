# Data Dictionary

## MongoDB Collections

### `raw_reviews`
| Field | Type | Description |
|-------|------|-------------|
| review_id | string | MD5 hash of place_id + author + timestamp |
| place_id | string | Google Places unique identifier |
| restaurant_name | string | Restaurant name |
| author_name | string | Reviewer name |
| rating | float | Original star rating (1-5) |
| text | string | Raw review text |
| language | string | Review language code |
| time_epoch | int | Unix timestamp of review |
| review_time | string | ISO formatted review time |
| ingested_at | string | Pipeline ingestion timestamp |

### `restaurants`
| Field | Type | Description |
|-------|------|-------------|
| place_id | string | Google Places unique identifier |
| name | string | Restaurant name |
| address | string | Full formatted address |
| latitude | float | Geographic latitude |
| longitude | float | Geographic longitude |
| rating | float | Google average rating |
| total_ratings | int | Total Google review count |
| price_level | int | Price level (1-4) |
| types | array | Restaurant type tags |

### `processed_reviews`
Same as `raw_reviews` plus:
| Field | Type | Description |
|-------|------|-------------|
| predicted_score | int | NLP predicted star rating (1-5) |
| sentiment_label | string | Sentiment category |
| confidence | float | Model prediction confidence |
| text_cleaned | string | Preprocessed review text |
| model_version | string | Model identifier used |
| key_phrases | array | Extracted topic keywords |
| predicted_at | string | Prediction timestamp |

---

## PostgreSQL Warehouse Tables

### `dim_restaurants`
Primary key: `place_id`

### `dim_time`
Primary key: `full_date`

### `fact_reviews`
Primary key: `review_id` | FK: `place_id` → `dim_restaurants`

### `fact_predictions`
Primary key: `review_id` | FK: `place_id` → `dim_restaurants`, `review_id` → `fact_reviews`

### `mv_restaurant_sentiment` (Materialized View)
Pre-computed restaurant-level sentiment aggregation joining all tables.
