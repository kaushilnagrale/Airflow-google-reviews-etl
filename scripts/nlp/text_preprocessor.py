"""
Text Preprocessor
Handles text cleaning, normalization, and tokenization
for the NLP pipeline.
"""

import re
import unicodedata

from scripts.utils.logger import get_logger

logger = get_logger("text_preprocessor")


class TextPreprocessor:
    """Cleans and preprocesses review text for NLP models."""

    # Common contractions expansion map
    CONTRACTIONS = {
        "won't": "will not", "can't": "cannot", "n't": " not",
        "'re": " are", "'s": " is", "'d": " would",
        "'ll": " will", "'ve": " have", "'m": " am",
    }

    def __init__(self, max_length: int = 512):
        self.max_length = max_length

    def clean(self, text: str) -> str:
        """
        Full text cleaning pipeline:
        1. Normalize unicode
        2. Remove HTML tags
        3. Expand contractions
        4. Remove special characters (keep basic punctuation)
        5. Normalize whitespace
        6. Truncate to max length
        """
        if not text or not isinstance(text, str):
            return ""

        # Normalize unicode characters
        text = unicodedata.normalize("NFKD", text)

        # Remove HTML tags
        text = re.sub(r"<[^>]+>", " ", text)

        # Remove URLs
        text = re.sub(r"https?://\S+|www\.\S+", " ", text)

        # Expand contractions
        for contraction, expansion in self.CONTRACTIONS.items():
            text = text.replace(contraction, expansion)

        # Remove special characters but keep basic punctuation
        text = re.sub(r"[^a-zA-Z0-9\s.,!?'-]", " ", text)

        # Normalize whitespace
        text = re.sub(r"\s+", " ", text).strip()

        # Truncate (approximate word-based truncation for BERT)
        words = text.split()
        if len(words) > self.max_length:
            text = " ".join(words[: self.max_length])

        return text

    def clean_batch(self, texts: list[str]) -> list[str]:
        """Clean a batch of texts."""
        cleaned = [self.clean(t) for t in texts]
        valid_count = sum(1 for t in cleaned if t)
        logger.info(f"Cleaned batch: {len(texts)} input, {valid_count} valid output")
        return cleaned

    def extract_key_phrases(self, text: str) -> list[str]:
        """
        Extract simple key phrases (noun phrases) from text.
        Basic approach using regex patterns for common patterns.
        """
        text = self.clean(text).lower()

        # Food-related keywords to look for
        food_patterns = [
            r'\b(?:food|pizza|burger|sushi|pasta|steak|chicken|fish|salad|'
            r'soup|dessert|appetizer|cocktail|wine|beer|coffee|tea)\b',
            r'\b(?:service|staff|waiter|waitress|server|manager|chef)\b',
            r'\b(?:atmosphere|ambiance|decor|music|view|location|parking)\b',
            r'\b(?:price|value|portion|quality|taste|flavor|fresh)\b',
        ]

        phrases = []
        for pattern in food_patterns:
            matches = re.findall(pattern, text)
            phrases.extend(matches)

        return list(set(phrases))
