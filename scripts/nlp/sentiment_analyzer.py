"""
Sentiment Analyzer
High-level sentiment analysis with restaurant-level aggregation
and recommendation scoring.
"""

from collections import defaultdict
from datetime import datetime, timezone
import math

from scripts.nlp.score_predictor import ReviewScorePredictor
from scripts.utils.logger import get_logger
from scripts.utils.config_loader import config

logger = get_logger("sentiment_analyzer")


class SentimentAnalyzer:
    """
    Restaurant-level sentiment analysis and scoring.
    Aggregates review-level predictions into restaurant scores.
    """

    def __init__(self, predictor: ReviewScorePredictor = None):
        self.predictor = predictor or ReviewScorePredictor()
        self.rec_config = config.recommendation_config

    def analyze_restaurant(self, reviews: list[dict]) -> dict:
        """
        Analyze all reviews for a single restaurant.

        Returns aggregate sentiment metrics:
        - Average predicted score
        - Sentiment distribution
        - Bayesian-weighted score
        - Recency-weighted score
        - Overall recommendation score
        """
        if not reviews:
            return self._empty_analysis()

        processed = self.predictor.process_reviews(reviews)

        scores = [r["predicted_score"] for r in processed]
        confidences = [r["confidence"] for r in processed]

        # Sentiment distribution
        sentiment_dist = defaultdict(int)
        for r in processed:
            sentiment_dist[r["sentiment_label"]] += 1

        # Bayesian average (handles restaurants with few reviews)
        prior_count = self.rec_config["bayesian_prior_count"]
        prior_mean = self.rec_config["bayesian_prior_mean"]
        n = len(scores)
        bayesian_score = (prior_count * prior_mean + sum(scores)) / (prior_count + n)

        # Recency-weighted score (newer reviews weighted more)
        recency_weighted = self._compute_recency_weighted_score(processed)

        # Overall recommendation score (hybrid)
        rec_score = (
            self.rec_config["sentiment_weight"] * bayesian_score
            + self.rec_config["recency_weight"] * recency_weighted
            + self.rec_config["popularity_weight"] * min(n / 50, 1.0) * 5
        )

        return {
            "review_count": n,
            "avg_predicted_score": round(sum(scores) / n, 3),
            "avg_confidence": round(sum(confidences) / n, 3),
            "bayesian_score": round(bayesian_score, 3),
            "recency_weighted_score": round(recency_weighted, 3),
            "recommendation_score": round(rec_score, 3),
            "sentiment_distribution": dict(sentiment_dist),
            "positive_ratio": round(
                (sentiment_dist.get("positive", 0) + sentiment_dist.get("very_positive", 0)) / n, 3
            ),
            "processed_reviews": processed,
            "analyzed_at": datetime.now(timezone.utc).isoformat(),
        }

    def _compute_recency_weighted_score(self, reviews: list[dict]) -> float:
        """
        Weight scores by recency using exponential decay.
        More recent reviews have higher weight.
        """
        if not reviews:
            return 0.0

        now = datetime.now(timezone.utc)
        weighted_sum = 0.0
        weight_total = 0.0

        for review in reviews:
            score = review["predicted_score"]
            # Use review_age_days if available, otherwise estimate
            age_days = review.get("review_age_days", 30)
            # Exponential decay: half-life of 90 days
            weight = math.exp(-0.693 * age_days / 90)
            weighted_sum += score * weight
            weight_total += weight

        return weighted_sum / max(weight_total, 1e-8)

    def rank_restaurants(self, restaurant_analyses: list[dict]) -> list[dict]:
        """
        Rank restaurants by recommendation score.
        Filters out restaurants with too few reviews.
        """
        min_reviews = self.rec_config["min_reviews"]
        top_n = self.rec_config["top_n"]

        filtered = [
            a for a in restaurant_analyses
            if a["review_count"] >= min_reviews
        ]

        ranked = sorted(
            filtered,
            key=lambda x: x["recommendation_score"],
            reverse=True,
        )

        for i, r in enumerate(ranked):
            r["rank"] = i + 1

        logger.info(
            f"Ranked {len(ranked)} restaurants "
            f"(filtered {len(restaurant_analyses) - len(filtered)} with < {min_reviews} reviews)"
        )
        return ranked[:top_n]

    def generate_recommendations(
        self,
        restaurant_analyses: list[dict],
        user_preferences: dict = None,
    ) -> list[dict]:
        """
        Generate personalized restaurant recommendations.

        user_preferences can include:
        - cuisine_types: list of preferred cuisine types
        - price_range: (min, max) price level
        - min_rating: minimum acceptable rating
        - location: (lat, lng) for proximity scoring
        """
        ranked = self.rank_restaurants(restaurant_analyses)

        if user_preferences:
            ranked = self._apply_user_preferences(ranked, user_preferences)

        recommendations = []
        for r in ranked:
            recommendations.append({
                "rank": r["rank"],
                "place_id": r.get("place_id"),
                "restaurant_name": r.get("restaurant_name"),
                "recommendation_score": r["recommendation_score"],
                "avg_predicted_score": r["avg_predicted_score"],
                "review_count": r["review_count"],
                "positive_ratio": r["positive_ratio"],
                "sentiment_summary": r["sentiment_distribution"],
            })

        return recommendations

    def _apply_user_preferences(
        self, ranked: list[dict], preferences: dict
    ) -> list[dict]:
        """Apply user preference filters to ranked restaurants."""
        filtered = ranked

        if "min_rating" in preferences:
            filtered = [
                r for r in filtered
                if r["avg_predicted_score"] >= preferences["min_rating"]
            ]

        if "cuisine_types" in preferences:
            preferred = set(t.lower() for t in preferences["cuisine_types"])
            filtered = [
                r for r in filtered
                if any(
                    t.lower() in preferred
                    for t in r.get("types", [])
                )
            ]

        return filtered

    @staticmethod
    def _empty_analysis() -> dict:
        return {
            "review_count": 0,
            "avg_predicted_score": 0.0,
            "avg_confidence": 0.0,
            "bayesian_score": 0.0,
            "recency_weighted_score": 0.0,
            "recommendation_score": 0.0,
            "sentiment_distribution": {},
            "positive_ratio": 0.0,
            "processed_reviews": [],
            "analyzed_at": datetime.now(timezone.utc).isoformat(),
        }
