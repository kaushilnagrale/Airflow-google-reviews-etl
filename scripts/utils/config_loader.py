"""
Configuration loader for the Restaurant Review Pipeline.
Loads YAML config and environment variables.
"""

import os
import yaml
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = BASE_DIR / "config" / "pipeline_config.yaml"


def load_config(config_path: str = None) -> dict:
    """Load pipeline configuration from YAML file."""
    path = Path(config_path) if config_path else CONFIG_PATH
    if not path.exists():
        # Fallback for Airflow container paths
        path = Path("/opt/airflow/config/pipeline_config.yaml")
    with open(path, "r") as f:
        return yaml.safe_load(f)


def get_env(key: str, default: str = None) -> str:
    """Get environment variable with optional default."""
    value = os.getenv(key, default)
    if value is None:
        raise EnvironmentError(f"Required environment variable '{key}' is not set.")
    return value


class PipelineConfig:
    """Centralized configuration access for all pipeline components."""

    def __init__(self):
        self._config = load_config()

    # --- Google API ---
    @property
    def google_api_key(self) -> str:
        return get_env("GOOGLE_API_KEY")

    @property
    def google_api(self) -> dict:
        return self._config["google_api"]

    # --- AWS S3 ---
    @property
    def aws_access_key(self) -> str:
        return get_env("AWS_ACCESS_KEY_ID")

    @property
    def aws_secret_key(self) -> str:
        return get_env("AWS_SECRET_ACCESS_KEY")

    @property
    def aws_region(self) -> str:
        return get_env("AWS_REGION", "us-east-1")

    @property
    def s3_bucket(self) -> str:
        return get_env("S3_BUCKET_NAME", self._config["aws_s3"]["bucket"])

    @property
    def s3_config(self) -> dict:
        return self._config["aws_s3"]

    # --- MongoDB ---
    @property
    def mongo_uri(self) -> str:
        return get_env("MONGO_URI", "mongodb://localhost:27017")

    @property
    def mongo_db(self) -> str:
        return get_env("MONGO_DB", self._config["mongodb"]["database"])

    @property
    def mongo_collections(self) -> dict:
        return self._config["mongodb"]["collections"]

    # --- PostgreSQL Warehouse ---
    @property
    def postgres_host(self) -> str:
        return get_env("POSTGRES_HOST", "localhost")

    @property
    def postgres_port(self) -> int:
        return int(get_env("POSTGRES_PORT", "5432"))

    @property
    def postgres_db(self) -> str:
        return get_env("POSTGRES_DB", "review_warehouse")

    @property
    def postgres_user(self) -> str:
        return get_env("POSTGRES_USER", "pipeline_user")

    @property
    def postgres_password(self) -> str:
        return get_env("POSTGRES_PASSWORD", "secure_password")

    @property
    def postgres_conn_string(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def warehouse_config(self) -> dict:
        return self._config["warehouse"]

    # --- NLP ---
    @property
    def nlp_config(self) -> dict:
        return self._config["nlp"]

    # --- Recommendation ---
    @property
    def recommendation_config(self) -> dict:
        return self._config["recommendation"]

    # --- Spark ---
    @property
    def spark_config(self) -> dict:
        return self._config["spark"]

    # --- Airflow ---
    @property
    def airflow_config(self) -> dict:
        return self._config["airflow"]


# Singleton instance
config = PipelineConfig()
