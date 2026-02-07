"""
Unit tests for NLP modules.
"""

import pytest
from unittest.mock import patch, MagicMock


class TestTextPreprocessor:
    """Tests for text preprocessing."""

    def test_contraction_expansion(self):
        from scripts.nlp.text_preprocessor import TextPreprocessor
        p = TextPreprocessor()

        assert "will not" in p.clean("I won't go back")
        assert "cannot" in p.clean("You can't miss this")

    def test_max_length_truncation(self):
        from scripts.nlp.text_preprocessor import TextPreprocessor
        p = TextPreprocessor(max_length=10)

        long_text = " ".join(["word"] * 20)
        result = p.clean(long_text)
        assert len(result.split()) <= 10

    def test_batch_cleaning(self):
        from scripts.nlp.text_preprocessor import TextPreprocessor
        p = TextPreprocessor()

        texts = ["Great food!", "", "  Nice place  ", None]
        cleaned = p.clean_batch(texts)
        assert len(cleaned) == 4
        assert cleaned[0] == "Great food!"
        assert cleaned[1] == ""
        assert cleaned[3] == ""


class TestReviewScorePredictor:
    """Tests for the BERT score predictor (mocked model)."""

    @patch("scripts.nlp.score_predictor.AutoModelForSequenceClassification")
    @patch("scripts.nlp.score_predictor.AutoTokenizer")
    @patch("scripts.nlp.score_predictor.config")
    def test_predict_single_review(self, mock_config, mock_tokenizer_cls, mock_model_cls):
        import torch

        mock_config.nlp_config = {
            "model_name": "test-model",
            "batch_size": 32,
            "max_length": 512,
        }

        # Mock tokenizer
        mock_tokenizer = MagicMock()
        mock_tokenizer.return_value = {
            "input_ids": torch.tensor([[1, 2, 3]]),
            "attention_mask": torch.tensor([[1, 1, 1]]),
        }
        mock_tokenizer_cls.from_pretrained.return_value = mock_tokenizer

        # Mock model
        mock_model = MagicMock()
        mock_outputs = MagicMock()
        # Simulate 5-class output, class 4 (score 5) has highest logit
        mock_outputs.logits = torch.tensor([[0.1, 0.1, 0.1, 0.2, 2.5]])
        mock_model.return_value = mock_outputs
        mock_model.to.return_value = mock_model
        mock_model.eval.return_value = None
        mock_model_cls.from_pretrained.return_value = mock_model

        from scripts.nlp.score_predictor import ReviewScorePredictor
        predictor = ReviewScorePredictor()

        result = predictor.predict("Amazing food!")

        assert result["predicted_score"] == 5
        assert result["sentiment"] == "very_positive"
        assert 0 <= result["confidence"] <= 1

    @patch("scripts.nlp.score_predictor.AutoModelForSequenceClassification")
    @patch("scripts.nlp.score_predictor.AutoTokenizer")
    @patch("scripts.nlp.score_predictor.config")
    def test_predict_empty_text(self, mock_config, mock_tokenizer_cls, mock_model_cls):
        mock_config.nlp_config = {
            "model_name": "test-model",
            "batch_size": 32,
            "max_length": 512,
        }
        mock_tokenizer_cls.from_pretrained.return_value = MagicMock()
        mock_model = MagicMock()
        mock_model.to.return_value = mock_model
        mock_model_cls.from_pretrained.return_value = mock_model

        from scripts.nlp.score_predictor import ReviewScorePredictor
        predictor = ReviewScorePredictor()

        result = predictor.predict("")
        assert result["predicted_score"] == 3
        assert result["sentiment"] == "neutral"
        assert result["confidence"] == 0.0


class TestSentimentAnalyzer:
    """Tests for restaurant-level sentiment analysis."""

    @patch("scripts.nlp.sentiment_analyzer.ReviewScorePredictor")
    @patch("scripts.nlp.sentiment_analyzer.config")
    def test_empty_restaurant_analysis(self, mock_config, mock_predictor_cls):
        mock_config.recommendation_config = {
            "min_reviews": 5,
            "recency_weight": 0.3,
            "sentiment_weight": 0.5,
            "popularity_weight": 0.2,
            "top_n": 20,
            "bayesian_prior_count": 10,
            "bayesian_prior_mean": 3.0,
        }

        from scripts.nlp.sentiment_analyzer import SentimentAnalyzer
        analyzer = SentimentAnalyzer()

        result = analyzer.analyze_restaurant([])
        assert result["review_count"] == 0
        assert result["recommendation_score"] == 0.0

    @patch("scripts.nlp.sentiment_analyzer.ReviewScorePredictor")
    @patch("scripts.nlp.sentiment_analyzer.config")
    def test_rank_restaurants_filters_low_review_count(self, mock_config, mock_predictor_cls):
        mock_config.recommendation_config = {
            "min_reviews": 5,
            "recency_weight": 0.3,
            "sentiment_weight": 0.5,
            "popularity_weight": 0.2,
            "top_n": 20,
            "bayesian_prior_count": 10,
            "bayesian_prior_mean": 3.0,
        }

        from scripts.nlp.sentiment_analyzer import SentimentAnalyzer
        analyzer = SentimentAnalyzer()

        analyses = [
            {"review_count": 3, "recommendation_score": 4.5},  # Should be filtered
            {"review_count": 10, "recommendation_score": 4.0},
            {"review_count": 20, "recommendation_score": 3.5},
        ]

        ranked = analyzer.rank_restaurants(analyses)
        assert len(ranked) == 2
        assert ranked[0]["recommendation_score"] == 4.0
