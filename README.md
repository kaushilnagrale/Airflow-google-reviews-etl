# 🍽️ Restaurant Review Intelligence Pipeline

> **End-to-end Data Engineering & Data Science pipeline** that ingests Google restaurant reviews, performs NLP-based sentiment analysis and score prediction, warehouses structured data, and serves personalized restaurant recommendations through a real-time dashboard.

![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.7-red?logo=apacheairflow)
![Spark](https://img.shields.io/badge/Apache_Spark-3.5-orange?logo=apachespark)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)
![MongoDB](https://img.shields.io/badge/MongoDB-7.0-green?logo=mongodb)
![AWS S3](https://img.shields.io/badge/AWS-S3-orange?logo=amazonaws)
![Power BI](https://img.shields.io/badge/Power_BI-Analytics-yellow?logo=powerbi)

---

## 📌 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Setup & Installation](#setup--installation)
- [Configuration](#configuration)
- [Pipeline Stages](#pipeline-stages)
- [NLP Model](#nlp-model)
- [Recommendation Engine](#recommendation-engine)
- [API Endpoints](#api-endpoints)
- [Power BI Dashboard](#power-bi-dashboard)
- [Testing](#testing)
- [Contributing](#contributing)

---

## 🏗️ Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATION (Apache Airflow)                    │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────┐ │
│  │ Google API   │───▶│  Raw Storage  │───▶│ Spark ETL   │───▶│ MongoDB  │ │
│  │ Ingestion    │    │  (AWS S3)     │    │ Processing  │    │ (NoSQL)  │ │
│  └─────────────┘    └──────────────┘    └──────┬──────┘    └────┬─────┘ │
│                                                 │                │       │
│                                          ┌──────▼──────┐   ┌────▼─────┐ │
│                                          │ NLP Model   │   │ SQL Data │ │
│                                          │ (Sentiment  │   │ Warehouse│ │
│                                          │  + Scoring) │   │(Postgres)│ │
│                                          └──────┬──────┘   └────┬─────┘ │
│                                                 │                │       │
│                                          ┌──────▼────────────────▼─────┐ │
│                                          │   Recommendation Engine     │ │
│                                          │   (Content + Collaborative) │ │
│                                          └──────────────┬──────────────┘ │
│                                                         │                │
│                            ┌─────────────┐     ┌────────▼───────┐       │
│                            │  Power BI    │◀────│  Flask REST API │       │
│                            │  Dashboard   │     │  + Frontend     │       │
│                            └─────────────┘     └────────────────┘       │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## ✨ Features

- **Automated Data Ingestion** — Scheduled extraction of restaurant reviews via Google Places API
- **Stream Processing** — Near real-time ingestion pipeline with configurable batch windows
- **NLP Sentiment Analysis** — BERT-based model fine-tuned for restaurant review score prediction
- **Smart Recommendations** — Hybrid content-based + collaborative filtering restaurant recommender
- **Data Warehousing** — Star-schema PostgreSQL warehouse with fact/dimension tables
- **NoSQL Flexibility** — MongoDB for raw review storage and flexible querying
- **Cloud Storage** — AWS S3 integration for raw data lake and model artifact storage
- **Real-time Dashboard** — Power BI connected to warehouse for live business intelligence
- **Fully Containerized** — Docker Compose orchestration for all services
- **CI/CD Ready** — Modular design with comprehensive test coverage

---

## 🛠️ Tech Stack

| Layer              | Technology                          |
|--------------------|-------------------------------------|
| Orchestration      | Apache Airflow 2.7                  |
| Processing         | Apache Spark 3.5 (PySpark)          |
| NLP/ML             | HuggingFace Transformers, scikit-learn |
| NoSQL Database     | MongoDB 7.0                         |
| Data Warehouse     | PostgreSQL 15                       |
| Cloud Storage      | AWS S3 (boto3)                      |
| API/Backend        | Flask + Flask-RESTful               |
| Frontend           | HTML/CSS/JS (Jinja2 templates)      |
| Visualization      | Power BI / Plotly                   |
| Containerization   | Docker + Docker Compose             |
| Language           | Python 3.10, SQL                    |

---

## 📁 Project Structure

```
restaurant-review-pipeline/
│
├── dags/                          # Airflow DAG definitions
│   ├── review_ingestion_dag.py    # Main ETL orchestration DAG
│   ├── nlp_processing_dag.py      # NLP model inference DAG
│   └── recommendation_dag.py      # Recommendation refresh DAG
│
├── scripts/
│   ├── etl/
│   │   ├── google_api_extractor.py    # Google Places API client
│   │   ├── s3_manager.py              # AWS S3 upload/download
│   │   ├── spark_transformer.py       # Spark data transformations
│   │   ├── mongo_loader.py            # MongoDB CRUD operations
│   │   └── warehouse_loader.py        # PostgreSQL warehouse loader
│   ├── nlp/
│   │   ├── sentiment_analyzer.py      # Sentiment analysis module
│   │   ├── score_predictor.py         # Review score prediction model
│   │   └── text_preprocessor.py       # Text cleaning & tokenization
│   └── utils/
│       ├── config_loader.py           # Configuration management
│       └── logger.py                  # Centralized logging
│
├── spark_jobs/
│   └── review_processing_job.py   # Standalone Spark job
│
├── sql/
│   ├── create_warehouse.sql       # Data warehouse DDL
│   └── analytics_queries.sql      # Analytical query library
│
├── frontend/
│   ├── app.py                     # Flask application
│   ├── templates/
│   │   └── index.html             # Restaurant recommendation UI
│   └── static/
│       └── style.css              # Stylesheet
│
├── config/
│   ├── airflow.cfg                # Airflow configuration
│   ├── pipeline_config.yaml       # Pipeline parameters
│   └── .env.example               # Environment variables template
│
├── docker/
│   ├── Dockerfile.airflow         # Airflow image
│   ├── Dockerfile.spark           # Spark image
│   └── Dockerfile.frontend        # Frontend image
│
├── tests/
│   ├── test_etl.py                # ETL unit tests
│   ├── test_nlp.py                # NLP model tests
│   └── test_api.py                # API endpoint tests
│
├── notebooks/
│   └── eda_and_modeling.ipynb     # Exploratory analysis notebook
│
├── docs/
│   ├── ARCHITECTURE.md            # Detailed architecture doc
│   ├── DATA_DICTIONARY.md         # Schema documentation
│   └── DEPLOYMENT.md              # Production deployment guide
│
├── docker-compose.yml             # Full stack orchestration
├── requirements.txt               # Python dependencies
├── Makefile                       # Build automation
└── README.md                      # This file
```

---

## 🚀 Setup & Installation

### Prerequisites
- Docker & Docker Compose (v2.0+)
- Python 3.10+
- AWS Account with S3 access
- Google Cloud Platform account with Places API enabled

### Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/restaurant-review-pipeline.git
cd restaurant-review-pipeline

# 2. Configure environment variables
cp config/.env.example .env
# Edit .env with your API keys and credentials

# 3. Build and start all services
make build
make up

# 4. Access services
#    Airflow UI:  http://localhost:8080  (admin/admin)
#    Frontend:    http://localhost:5000
#    MongoDB:     localhost:27017
#    PostgreSQL:  localhost:5432
```

### Manual Setup (Without Docker)

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Start Airflow
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow db init
airflow webserver -p 8080 &
airflow scheduler &
```

---

## ⚙️ Configuration

Create a `.env` file from the template:

```env
# Google API
GOOGLE_API_KEY=your_google_places_api_key
GOOGLE_SEARCH_RADIUS=5000
GOOGLE_MAX_RESULTS=100

# AWS S3
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET_NAME=restaurant-reviews-lake

# MongoDB
MONGO_URI=mongodb://mongo:27017
MONGO_DB=restaurant_reviews

# PostgreSQL (Warehouse)
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=review_warehouse
POSTGRES_USER=pipeline_user
POSTGRES_PASSWORD=secure_password

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False

# NLP Model
MODEL_NAME=nlptown/bert-base-multilingual-uncased-sentiment
BATCH_SIZE=32
```

---

## 🔄 Pipeline Stages

### Stage 1: Data Ingestion
Extracts restaurant data and reviews from Google Places API with rate limiting, pagination, and error handling. Raw JSON is stored in AWS S3 and MongoDB.

### Stage 2: Spark ETL Processing
PySpark jobs clean, deduplicate, and transform raw reviews. Handles schema normalization, text cleaning, and feature engineering.

### Stage 3: NLP Score Prediction
Fine-tuned BERT model analyzes review text to predict sentiment scores (1-5). Generates sentiment labels, confidence scores, and topic extraction.

### Stage 4: Data Warehousing
Transformed data loads into a PostgreSQL star schema with fact tables (reviews, predictions) and dimension tables (restaurants, users, time).

### Stage 5: Recommendation Generation
Hybrid recommendation engine combines content-based filtering (cuisine, location, sentiment) with collaborative signals to rank restaurants per user.

### Stage 6: API & Visualization
Flask REST API serves recommendations. Power BI connects to the warehouse for executive dashboards.

---

## 🧠 NLP Model

The pipeline uses a **BERT-based multilingual sentiment model** fine-tuned on restaurant reviews:

- **Base Model**: `nlptown/bert-base-multilingual-uncased-sentiment`
- **Task**: Predict review star rating (1-5) from review text
- **Preprocessing**: Lowercasing, special character removal, tokenization
- **Output**: Predicted score, sentiment label, confidence probability

```python
# Example usage
from scripts.nlp.score_predictor import ReviewScorePredictor

predictor = ReviewScorePredictor()
result = predictor.predict("Amazing food and great atmosphere!")
# {'predicted_score': 5, 'sentiment': 'positive', 'confidence': 0.94}
```

---

## 🎯 Recommendation Engine

The recommendation system uses a **hybrid approach**:

1. **Content-Based Filtering**: Matches user preferences (cuisine type, price range, location) with restaurant attributes and aggregated sentiment scores
2. **Sentiment-Weighted Scoring**: Restaurants ranked by NLP-predicted scores weighted by review recency and reviewer credibility
3. **Popularity Penalty**: Balances popular spots with hidden gems using a Bayesian average

---

## 📡 API Endpoints

| Method | Endpoint                          | Description                        |
|--------|-----------------------------------|------------------------------------|
| GET    | `/api/v1/restaurants`             | List all restaurants               |
| GET    | `/api/v1/restaurants/<id>`        | Get restaurant details             |
| GET    | `/api/v1/restaurants/<id>/reviews`| Get reviews for a restaurant       |
| GET    | `/api/v1/recommendations`        | Get personalized recommendations   |
| POST   | `/api/v1/preferences`            | Submit user preferences            |
| GET    | `/api/v1/analytics/sentiment`    | Sentiment distribution analytics   |
| GET    | `/api/v1/health`                 | Pipeline health check              |

---

## 📊 Power BI Dashboard

The Power BI dashboard connects to the PostgreSQL warehouse and visualizes:

- **Sentiment Distribution** across restaurants and time periods
- **Top-Rated Restaurants** by NLP-predicted scores vs raw ratings
- **Review Volume Trends** with anomaly detection
- **Geographic Heatmaps** of restaurant quality by area
- **Recommendation Performance** tracking click-through and satisfaction

> Connect Power BI Desktop → PostgreSQL → `review_warehouse` database

---

## 🧪 Testing

```bash
# Run all tests
make test

# Run specific test suites
pytest tests/test_etl.py -v
pytest tests/test_nlp.py -v
pytest tests/test_api.py -v

# Coverage report
pytest --cov=scripts tests/ --cov-report=html
```

---

## 📜 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

## 🙏 Acknowledgments

- Google Places API for review data
- HuggingFace for pre-trained NLP models
- Apache Software Foundation for Airflow & Spark
- The open-source community
