"""
Review Score Predictor
Uses a pre-trained BERT model to predict review star ratings (1-5)
from review text. Supports batch inference.
"""

from datetime import datetime, timezone

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from tqdm import tqdm

from scripts.nlp.text_preprocessor import TextPreprocessor
from scripts.utils.logger import get_logger
from scripts.utils.config_loader import config

logger = get_logger("score_predictor")


class ReviewScorePredictor:
    """
    BERT-based review score prediction model.
    Predicts star ratings (1-5) from review text.
    """

    SENTIMENT_MAP = {
        1: "very_negative",
        2: "negative",
        3: "neutral",
        4: "positive",
        5: "very_positive",
    }

    def __init__(self, model_name: str = None):
        self.model_name = model_name or config.nlp_config["model_name"]
        self.batch_size = config.nlp_config.get("batch_size", 32)
        self.max_length = config.nlp_config.get("max_length", 512)
        self.preprocessor = TextPreprocessor(max_length=self.max_length)

        # Determine device
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info(f"Using device: {self.device}")

        # Load model and tokenizer
        logger.info(f"Loading model: {self.model_name}")
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
        self.model.to(self.device)
        self.model.eval()
        logger.info("Model loaded successfully")

    def predict(self, text: str) -> dict:
        """
        Predict score for a single review text.

        Returns:
            dict with predicted_score, sentiment, confidence
        """
        cleaned = self.preprocessor.clean(text)
        if not cleaned:
            return {
                "predicted_score": 3,
                "sentiment": "neutral",
                "confidence": 0.0,
                "text_cleaned": "",
            }

        inputs = self.tokenizer(
            cleaned,
            return_tensors="pt",
            truncation=True,
            max_length=self.max_length,
            padding=True,
        ).to(self.device)

        with torch.no_grad():
            outputs = self.model(**inputs)
            probabilities = torch.softmax(outputs.logits, dim=-1)

        # Weighted average (expected value) over classes 1-5 gives a continuous,
        # nuanced score instead of argmax which collapses to 5 for all positive reviews.
        scores_tensor = torch.arange(1, 6, dtype=torch.float32).to(self.device)
        predicted_score = (probabilities[0] * scores_tensor).sum().item()

        # Argmax class is used only for the discrete sentiment label
        predicted_class = torch.argmax(probabilities, dim=-1).item()
        confidence = probabilities[0][predicted_class].item()
        sentiment_score = predicted_class + 1  # 1-5 label bucket

        return {
            "predicted_score": round(predicted_score, 3),
            "sentiment": self.SENTIMENT_MAP.get(sentiment_score, "neutral"),
            "confidence": round(confidence, 4),
            "text_cleaned": cleaned,
        }

    def predict_batch(self, texts: list[str]) -> list[dict]:
        """
        Predict scores for a batch of review texts.
        Processes in mini-batches for memory efficiency.
        """
        results = []
        cleaned_texts = self.preprocessor.clean_batch(texts)
        scores_tensor = torch.arange(1, 6, dtype=torch.float32).to(self.device)

        for i in tqdm(range(0, len(cleaned_texts), self.batch_size),
                      desc="Predicting scores"):
            batch = cleaned_texts[i: i + self.batch_size]

            # Pre-allocate results to preserve original ordering
            batch_results = [None] * len(batch)
            valid_texts = []
            valid_indices = []

            for j, text in enumerate(batch):
                if text:
                    valid_texts.append(text)
                    valid_indices.append(j)
                else:
                    batch_results[j] = {
                        "predicted_score": 3.0,
                        "sentiment": "neutral",
                        "confidence": 0.0,
                        "text_cleaned": "",
                    }

            if valid_texts:
                inputs = self.tokenizer(
                    valid_texts,
                    return_tensors="pt",
                    truncation=True,
                    max_length=self.max_length,
                    padding=True,
                ).to(self.device)

                with torch.no_grad():
                    outputs = self.model(**inputs)
                    probabilities = torch.softmax(outputs.logits, dim=-1)

                # Weighted average gives a continuous score (e.g. 4.72) instead of
                # argmax which collapses most positive reviews to 5.
                weighted_scores = (probabilities * scores_tensor).sum(dim=-1)
                predicted_classes = torch.argmax(probabilities, dim=-1)
                confidences = probabilities.max(dim=-1).values

                for local_idx, orig_idx in enumerate(valid_indices):
                    score = weighted_scores[local_idx].item()
                    label_cls = predicted_classes[local_idx].item()
                    label_score = label_cls + 1  # 1-5 for sentiment label only
                    batch_results[orig_idx] = {
                        "predicted_score": round(score, 3),
                        "sentiment": self.SENTIMENT_MAP.get(label_score, "neutral"),
                        "confidence": round(confidences[local_idx].item(), 4),
                        "text_cleaned": valid_texts[local_idx],
                    }

            results.extend(batch_results)

        logger.info(f"Predicted scores for {len(results)} reviews")
        return results

    def process_reviews(self, reviews: list[dict]) -> list[dict]:
        """
        Process a list of review documents with NLP predictions.
        Adds prediction fields to each review dict.
        """
        texts = [r.get("text", "") for r in reviews]
        predictions = self.predict_batch(texts)

        processed = []
        model_version = self.model_name.split("/")[-1]
        now = datetime.now(timezone.utc).isoformat()

        for review, prediction in zip(reviews, predictions):
            processed_review = {
                **review,
                "predicted_score": prediction["predicted_score"],
                "sentiment_label": prediction["sentiment"],
                "confidence": prediction["confidence"],
                "text_cleaned": prediction["text_cleaned"],
                "model_version": model_version,
                "predicted_at": now,
                "key_phrases": self.preprocessor.extract_key_phrases(
                    review.get("text", "")
                ),
            }
            processed.append(processed_review)

        logger.info(
            f"Processed {len(processed)} reviews | "
            f"Avg confidence: {sum(p['confidence'] for p in processed) / max(len(processed), 1):.3f}"
        )
        return processed
