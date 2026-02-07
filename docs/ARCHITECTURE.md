# Architecture Documentation

## System Architecture

This document describes the end-to-end architecture of the Restaurant Review Intelligence Pipeline.

---

## Data Flow

### 1. Ingestion Layer

The pipeline begins with the **Google Places API Extractor** (`scripts/etl/google_api_extractor.py`), which searches for restaurants across configured geographic locations. For each restaurant, it fetches detailed information including user reviews. The extractor implements rate limiting, retry logic with exponential backoff, and pagination handling.

Raw data flows to two storage targets simultaneously: **AWS S3** serves as the data lake for archival and reprocessing, while **MongoDB** provides the operational data store for fast access during processing.

### 2. Processing Layer

**Apache Spark** handles the heavy data transformation work. The `SparkTransformer` loads raw reviews into DataFrames, deduplicates by review ID, cleans text (removing HTML, normalizing whitespace), and engineers features like review length, word count, and temporal attributes. It also computes restaurant-level aggregate statistics and prepares data in star-schema format for the warehouse.

### 3. NLP / ML Layer

The **ReviewScorePredictor** uses a pre-trained BERT model (`nlptown/bert-base-multilingual-uncased-sentiment`) to predict star ratings from review text. It processes reviews in configurable batch sizes with GPU support when available. Each review receives a predicted score (1-5), sentiment label, and confidence probability.

The **SentimentAnalyzer** aggregates review-level predictions to the restaurant level, computing Bayesian-weighted scores (to handle restaurants with few reviews), recency-weighted scores (newer reviews count more), and a hybrid recommendation score.

### 4. Storage Layer

The **PostgreSQL Data Warehouse** uses a star schema design with fact tables (`fact_reviews`, `fact_predictions`) and dimension tables (`dim_restaurants`, `dim_time`, `dim_users`). A materialized view (`mv_restaurant_sentiment`) pre-computes restaurant-level sentiment summaries for fast dashboard queries.

**MongoDB** stores raw reviews, processed reviews with NLP predictions, and generated recommendations for API access.

### 5. Serving Layer

A **Flask REST API** provides endpoints for listing restaurants, fetching reviews, retrieving recommendations, and querying sentiment analytics. The frontend renders an interactive dashboard with filtering capabilities.

**Power BI** connects directly to the PostgreSQL warehouse for executive-level dashboards and ad-hoc analytics.

### 6. Orchestration Layer

**Apache Airflow** manages three DAGs that run in sequence daily:

1. `review_ingestion_pipeline` (2 AM) — Extract → S3 + MongoDB → Spark transform → Warehouse
2. `nlp_processing_pipeline` (4 AM) — Fetch unprocessed → BERT inference → Save predictions
3. `recommendation_pipeline` (6 AM) — Load data → Analyze sentiments → Generate rankings

---

## Star Schema Design

```
                    ┌──────────────┐
                    │  dim_time    │
                    │──────────────│
                    │ full_date PK │
                    │ year         │
                    │ month        │
                    │ quarter      │
                    └──────┬───────┘
                           │
┌──────────────────┐   ┌───┴──────────────┐   ┌────────────────────┐
│ dim_restaurants   │   │  fact_reviews     │   │ fact_predictions    │
│──────────────────│   │──────────────────│   │────────────────────│
│ place_id PK      │◄──│ review_id PK     │──►│ review_id PK/FK    │
│ name             │   │ place_id FK      │   │ place_id FK        │
│ address          │   │ author_name      │   │ predicted_score    │
│ latitude         │   │ rating           │   │ sentiment_label    │
│ longitude        │   │ review_text      │   │ confidence         │
│ rating           │   │ review_length    │   │ model_version      │
│ price_level      │   │ review_date FK   │   │ predicted_at       │
│ types            │   │ loaded_at        │   └────────────────────┘
└──────────────────┘   └──────────────────┘
```

---

## Scalability Considerations

- **Horizontal Spark scaling**: Add Spark workers via docker-compose scale
- **MongoDB sharding**: Shard on `place_id` for large datasets
- **Airflow parallelism**: Adjust executor concurrency for faster DAG runs
- **Model serving**: For production, consider dedicated model serving with TorchServe or Triton
- **Caching**: Add Redis for API response caching if traffic grows
