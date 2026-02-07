"""
Google Places API Extractor
Fetches restaurant data and reviews from Google Places API
with rate limiting, pagination, and error handling.
"""

import time
import json
import hashlib
from datetime import datetime, timezone
from typing import Generator

import requests
from scripts.utils.logger import get_logger
from scripts.utils.config_loader import config

logger = get_logger("google_api_extractor")


class GoogleAPIExtractor:
    """Extracts restaurant reviews from the Google Places API."""

    BASE_URL = "https://maps.googleapis.com/maps/api/place"

    def __init__(self):
        self.api_key = config.google_api_key
        self.api_config = config.google_api
        self.session = requests.Session()
        self._request_count = 0
        self._last_request_time = 0

    def _rate_limit(self):
        """Enforce rate limiting between API calls."""
        rps = self.api_config["rate_limit"]["requests_per_second"]
        min_interval = 1.0 / rps
        elapsed = time.time() - self._last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_time = time.time()
        self._request_count += 1

    def _make_request(self, endpoint: str, params: dict) -> dict:
        """Make a rate-limited API request with retry logic."""
        url = f"{self.BASE_URL}/{endpoint}/json"
        params["key"] = self.api_key
        max_retries = self.api_config["rate_limit"]["retry_max"]
        backoff = self.api_config["rate_limit"]["retry_backoff"]

        for attempt in range(max_retries):
            self._rate_limit()
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                if data.get("status") == "OK":
                    return data
                elif data.get("status") == "ZERO_RESULTS":
                    logger.warning(f"No results for params: {params}")
                    return {"results": [], "status": "ZERO_RESULTS"}
                elif data.get("status") in ("OVER_QUERY_LIMIT", "REQUEST_DENIED"):
                    logger.error(f"API error: {data.get('status')} - {data.get('error_message', '')}")
                    raise Exception(f"API error: {data['status']}")
                else:
                    logger.warning(f"Unexpected status: {data.get('status')}")
                    return data

            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(backoff ** (attempt + 1))
                else:
                    raise

        return {}

    def search_restaurants(self, lat: float, lng: float, radius: int = None) -> list[dict]:
        """
        Search for restaurants near a given location.
        Returns a list of place summaries with place_id.
        """
        radius = radius or self.api_config["search_radius"]
        all_results = []
        next_page_token = None

        logger.info(f"Searching restaurants near ({lat}, {lng}), radius={radius}m")

        while True:
            params = {
                "location": f"{lat},{lng}",
                "radius": radius,
                "type": "restaurant",
            }
            if next_page_token:
                params["pagetoken"] = next_page_token
                time.sleep(2)  # Google requires delay between page token requests

            data = self._make_request("nearbysearch", params)
            results = data.get("results", [])
            all_results.extend(results)

            logger.info(f"  Fetched {len(results)} restaurants (total: {len(all_results)})")

            next_page_token = data.get("next_page_token")
            if (
                not next_page_token
                or len(all_results) >= self.api_config["max_results_per_query"]
            ):
                break

        return all_results

    def get_place_details(self, place_id: str) -> dict:
        """
        Get detailed information and reviews for a specific place.
        """
        fields = ",".join(self.api_config["review_fields"])
        params = {"place_id": place_id, "fields": fields}
        data = self._make_request("details", params)
        return data.get("result", {})

    def extract_reviews(self, place_details: dict) -> list[dict]:
        """
        Extract and normalize reviews from place details.
        """
        reviews = []
        place_id = place_details.get("place_id", "unknown")
        restaurant_name = place_details.get("name", "Unknown")

        for review in place_details.get("reviews", []):
            review_id = hashlib.md5(
                f"{place_id}_{review.get('author_name', '')}_{review.get('time', '')}".encode()
            ).hexdigest()

            reviews.append({
                "review_id": review_id,
                "place_id": place_id,
                "restaurant_name": restaurant_name,
                "author_name": review.get("author_name", "Anonymous"),
                "rating": review.get("rating"),
                "text": review.get("text", ""),
                "language": review.get("language", "en"),
                "time_epoch": review.get("time"),
                "review_time": datetime.fromtimestamp(
                    review.get("time", 0), tz=timezone.utc
                ).isoformat(),
                "relative_time": review.get("relative_time_description", ""),
                "profile_photo_url": review.get("profile_photo_url", ""),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            })

        return reviews

    def extract_restaurant_metadata(self, place_details: dict) -> dict:
        """Extract structured restaurant metadata."""
        geometry = place_details.get("geometry", {}).get("location", {})
        return {
            "place_id": place_details.get("place_id"),
            "name": place_details.get("name"),
            "address": place_details.get("formatted_address", ""),
            "latitude": geometry.get("lat"),
            "longitude": geometry.get("lng"),
            "rating": place_details.get("rating"),
            "total_ratings": place_details.get("user_ratings_total", 0),
            "price_level": place_details.get("price_level"),
            "types": place_details.get("types", []),
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

    def run_full_extraction(self) -> Generator[dict, None, None]:
        """
        Run full extraction pipeline across all configured locations.
        Yields batches of {restaurant_metadata, reviews} dicts.
        """
        locations = self.api_config["search_locations"]
        total_reviews = 0
        total_restaurants = 0

        for location in locations:
            logger.info(f"Processing location: {location['name']}")
            restaurants = self.search_restaurants(location["lat"], location["lng"])

            for restaurant in restaurants:
                place_id = restaurant.get("place_id")
                if not place_id:
                    continue

                try:
                    details = self.get_place_details(place_id)
                    metadata = self.extract_restaurant_metadata(details)
                    reviews = self.extract_reviews(details)

                    total_restaurants += 1
                    total_reviews += len(reviews)

                    yield {
                        "restaurant": metadata,
                        "reviews": reviews,
                        "location": location["name"],
                    }

                except Exception as e:
                    logger.error(f"Failed to process place {place_id}: {e}")
                    continue

        logger.info(
            f"Extraction complete: {total_restaurants} restaurants, "
            f"{total_reviews} reviews from {len(locations)} locations"
        )
