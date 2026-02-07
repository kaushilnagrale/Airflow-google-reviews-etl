"""
AWS S3 Manager
Handles upload/download of raw and processed data to S3 data lake.
"""

import json
import io
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from scripts.utils.logger import get_logger
from scripts.utils.config_loader import config

logger = get_logger("s3_manager")


class S3Manager:
    """Manages AWS S3 operations for the data lake."""

    def __init__(self):
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=config.aws_access_key,
            aws_secret_access_key=config.aws_secret_key,
            region_name=config.aws_region,
        )
        self.bucket = config.s3_bucket
        self.s3_config = config.s3_config
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Create the S3 bucket if it doesn't exist."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
            logger.info(f"S3 bucket '{self.bucket}' exists")
        except ClientError as e:
            error_code = int(e.response["Error"]["Code"])
            if error_code == 404:
                logger.info(f"Creating S3 bucket '{self.bucket}'")
                try:
                    if config.aws_region == "us-east-1":
                        self.s3_client.create_bucket(Bucket=self.bucket)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket,
                            CreateBucketConfiguration={
                                "LocationConstraint": config.aws_region
                            },
                        )
                except ClientError as create_err:
                    logger.error(f"Failed to create bucket: {create_err}")
                    raise
            else:
                logger.warning(f"Could not verify bucket: {e}")

    def _generate_key(self, prefix: str, filename: str) -> str:
        """Generate a date-partitioned S3 key."""
        now = datetime.now(timezone.utc)
        partition = now.strftime("year=%Y/month=%m/day=%d")
        return f"{prefix}{partition}/{filename}"

    def upload_raw_reviews(self, data: list[dict], location: str) -> str:
        """Upload raw review data to S3 as JSON."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{location.lower().replace(' ', '_')}_{timestamp}.json"
        key = self._generate_key(self.s3_config["raw_prefix"], filename)

        json_bytes = json.dumps(data, indent=2, default=str).encode("utf-8")

        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json_bytes,
                ContentType="application/json",
                Metadata={
                    "location": location,
                    "record_count": str(len(data)),
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                },
            )
            logger.info(f"Uploaded {len(data)} records to s3://{self.bucket}/{key}")
            return key
        except ClientError as e:
            logger.error(f"Failed to upload to S3: {e}")
            raise

    def upload_processed_data(self, data: list[dict], batch_name: str) -> str:
        """Upload processed/transformed data to S3."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{batch_name}_{timestamp}.json"
        key = self._generate_key(self.s3_config["processed_prefix"], filename)

        json_bytes = json.dumps(data, indent=2, default=str).encode("utf-8")

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json_bytes,
            ContentType="application/json",
        )
        logger.info(f"Uploaded processed data to s3://{self.bucket}/{key}")
        return key

    def download_json(self, key: str) -> list[dict]:
        """Download and parse a JSON file from S3."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            return json.loads(content)
        except ClientError as e:
            logger.error(f"Failed to download s3://{self.bucket}/{key}: {e}")
            raise

    def list_keys(self, prefix: str) -> list[str]:
        """List all keys under a prefix."""
        keys = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def upload_model_artifact(self, local_path: str, model_name: str) -> str:
        """Upload a trained model artifact to S3."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        key = f"{self.s3_config['model_prefix']}{model_name}/{timestamp}/model.pkl"

        self.s3_client.upload_file(local_path, self.bucket, key)
        logger.info(f"Uploaded model artifact to s3://{self.bucket}/{key}")
        return key
