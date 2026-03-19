"""Shared configuration for the News ScoreModel pipeline."""

from pathlib import Path

# ── Project root ──────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ── Directory paths ───────────────────────────────────────────────────────────
NEWS_DIR = PROJECT_ROOT / "NewsCleaning" / "News"
PROMPTS_DIR = PROJECT_ROOT / "ScoreModel" / "Prompt"
NEWS_EVIDENCES_DIR = PROJECT_ROOT / "ScoreModel" / "NewsEvidences"

# ── Prompt file paths ─────────────────────────────────────────────────────────
PROMPT_NEWS_PATH = PROMPTS_DIR / "PromptNews.txt"
PROMPT_RATE_NEWS_PATH = PROMPTS_DIR / "RateNews.txt"

# ── Company folder names (match NewsCleaning/News/ subdirectory names) ────────
COMPANIES = ["Berjaya", "F&N", "Power", "QL"]

COMPANY_FULL_NAMES = {
    "Berjaya": "Berjaya Food Berhad",
    "F&N":     "Fraser & Neave Holdings Bhd",
    "Power":   "Power Root Berhad",
    "QL":      "QL Resources Berhad",
}

COMPANY_INDUSTRY = {
    "Berjaya": "food and beverage",
    "F&N":     "food and beverage",
    "Power":   "food and beverage",
    "QL":      "food and beverage",
}

# ── Model settings ────────────────────────────────────────────────────────────
MODEL_NAME = "qwen3:8b"
NUM_CTX = 32768

# ── Rest settings ─────────────────────────────────────────────────────────────
REST_EVERY_N = 30   # scored articles between rests
REST_SECONDS = 5 * 60  # 5 minutes
