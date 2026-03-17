"""Shared configuration for ScoreModel pipeline."""

import os
import re
from pathlib import Path

# ── Project root (repo root) ──────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ── Directory paths ───────────────────────────────────────────────────
CHUNKS_DIR = PROJECT_ROOT / "AnnualReportCleaning" / "Chunks"
CHUNKS_CSV = CHUNKS_DIR / "chunks_summary.csv"
PROMPTS_DIR = PROJECT_ROOT / "ScoreModel" / "Prompt"
EVIDENCES_DIR = PROJECT_ROOT / "ScoreModel" / "Evidences"

# ── Prompt file paths ────────────────────────────────────────────────
PROMPT_AR_PATH = PROMPTS_DIR / "PromptAR.txt"
PROMPT_NEWS_PATH = PROMPTS_DIR / "PromptNews.txt"

# ── Company mappings ─────────────────────────────────────────────────
COMPANY_CODE_TO_NAME = {
    "B": "Berjaya",
    "F": "F&N",
    "P": "Power",
    "Q": "QL",
}

COMPANY_FULL_NAMES = {
    "B": "Berjaya Food Berhad",
    "F": "Fraser & Neave Holdings Bhd",
    "P": "Power Root Berhad",
    "Q": "QL Resources Berhad",
}

COMPANY_INDUSTRY = {
    "B": "food and beverage",
    "F": "food and beverage",
    "P": "food and beverage",
    "Q": "food and beverage",
}

# ── Model settings ───────────────────────────────────────────────────
MODEL_NAME = "qwen3:4b"
NUM_CTX = 32768

# ── Filename parsing ─────────────────────────────────────────────────
# Matches source filenames like "cleaned_B18-1.txt" or "cleaned_B19.txt"
SOURCE_PATTERN = re.compile(r"cleaned_([BFPQ])(\d{2})(?:-\d+)?\.txt")


def parse_source(source: str):
    """Extract company code and 2-digit year from a source filename.

    Returns (company_code, year_2digit) or (None, None) on failure.
    Example: "cleaned_B18-1.txt" → ("B", "18")
    """
    m = SOURCE_PATTERN.match(source)
    if not m:
        return None, None
    return m.group(1), m.group(2)


def year_2to4(yy: str) -> int:
    """Convert 2-digit year string to 4-digit int.  '16' → 2016."""
    return 2000 + int(yy)


def chunk_filepath(file_path_col: str) -> Path:
    """Resolve the actual filesystem path for a chunk from the CSV file_path column.

    The CSV stores paths like 'chunks/cleaned_B18-1_chunk_000.txt',
    but the actual files live directly under CHUNKS_DIR.
    """
    basename = os.path.basename(file_path_col)
    return CHUNKS_DIR / basename
