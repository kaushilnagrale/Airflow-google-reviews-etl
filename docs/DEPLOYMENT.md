# Deployment Guide

## Local Development

```bash
# Clone and configure
git clone https://github.com/yourusername/restaurant-review-pipeline.git
cd restaurant-review-pipeline
cp config/.env.example .env
# Edit .env with your credentials

# Start all services
make build && make up

# Verify services
make logs
curl http://localhost:5000/api/v1/health
```

## Production Deployment (AWS)

### Infrastructure Requirements
- **EC2**: t3.large minimum (8GB RAM for Spark + NLP model)
- **RDS PostgreSQL**: db.t3.medium for warehouse
- **DocumentDB**: MongoDB-compatible for operational store
- **S3**: Standard bucket for data lake
- **ECR**: Container registry for Docker images

### Steps
1. Push Docker images to ECR
2. Deploy with ECS/Fargate or EC2 with docker-compose
3. Configure RDS and DocumentDB endpoints in .env
4. Set up Airflow connections for each service
5. Enable Airflow DAGs via the UI
6. Connect Power BI to RDS PostgreSQL endpoint

### Monitoring
- Airflow UI: DAG run history, task logs, SLA tracking
- CloudWatch: Container metrics, S3 access patterns
- PostgreSQL: `pg_stat_activity` for warehouse query performance

## Environment Variables

All required variables are documented in `config/.env.example`. Never commit the `.env` file.
