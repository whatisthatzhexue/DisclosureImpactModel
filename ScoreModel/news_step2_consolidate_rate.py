"""News Step 2 — Consolidate article-level evidence and produce a final rating.

For each {company}/{fiscal_year}/ folder under NewsEvidences/:
  1. Concatenate all part_*.txt → sum.txt
  2. Count tokens; if within limit, score directly with RateNews.txt
  3. If over limit, compress first → sum_zipped.txt, then score
  4. Save final model output as rate.txt

Usage:  python ScoreModel/news_step2_consolidate_rate.py
"""

import logging
import sys
import time
from pathlib import Path

import ollama
import tiktoken

_CLIENT = ollama.Client(timeout=1800)  # 30-minute timeout

from news_config import (
    MODEL_NAME,
    NEWS_EVIDENCES_DIR,
    NUM_CTX,
    PROMPT_RATE_NEWS_PATH,
    REST_EVERY_N,
    REST_SECONDS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_ENC = tiktoken.get_encoding("cl100k_base")

TOKEN_BUDGET = NUM_CTX          # 32 768
PROMPT_OVERHEAD = 2000          # tokens used by the RateNews template itself
MAX_TEXT_TOKENS = TOKEN_BUDGET - PROMPT_OVERHEAD
COMPRESS_PROMPT_OVERHEAD = 300


def count_tokens(text: str) -> int:
    return len(_ENC.encode(text))


def call_model(prompt: str) -> str:
    response = _CLIENT.chat(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        options={"num_ctx": NUM_CTX},
        keep_alive="30m",
    )
    return response["message"]["content"]


def _split_text_by_tokens(text: str, max_tokens: int) -> list[str]:
    tokens = _ENC.encode(text)
    parts = []
    for i in range(0, len(tokens), max_tokens):
        parts.append(_ENC.decode(tokens[i: i + max_tokens]))
    return parts


def compress_text(text: str, target_tokens: int) -> str:
    """Compress text to fit within target_tokens, splitting if necessary."""
    max_input = TOKEN_BUDGET - COMPRESS_PROMPT_OVERHEAD
    parts = _split_text_by_tokens(text, max_input)
    num_parts = len(parts)
    per_part_target = max(target_tokens // num_parts, 200)
    per_part_chars = per_part_target * 3

    log.info("    Compression: %d parts, target %d tokens/part", num_parts, per_part_target)

    compressed = []
    for i, part in enumerate(parts):
        log.info("    Compressing part %d/%d (%d tokens) …",
                 i + 1, num_parts, count_tokens(part))
        prompt = (
            "You are a financial text compressor. Compress the following text "
            f"to at most {per_part_chars} characters while preserving ALL key "
            "information relevant to scoring news quality: credibility, "
            "strategic relevance, and depth. Keep specific facts, numbers, "
            "quotes, and scores. Remove only redundancy and filler.\n\n"
            f"TEXT:\n{part}"
        )
        compressed.append(call_model(prompt))

    return "\n\n".join(compressed)


def build_rate_prompt(template: str, scores_text: str) -> str:
    return template.replace("[MULTIPLE_SCORES]", scores_text)


def process_folder(company_dir: Path, year_dir: Path, template: str) -> bool:
    """Process a folder. Returns True if model was actually called, False if skipped."""
    company = company_dir.name
    year = year_dir.name

    # Resume: skip if rate.txt already exists and is non-empty
    rate_path = year_dir / "rate.txt"
    if rate_path.exists() and rate_path.stat().st_size > 0:
        log.info("  [skip] %s/%s — rate.txt already exists", company, year)
        return False

    parts = sorted(year_dir.glob("part_*.txt"))
    if not parts:
        log.warning("  No part files in %s/%s — skipping", company, year)
        return False

    # 1. Concatenate → sum.txt
    combined = "\n\n".join(p.read_text(encoding="utf-8") for p in parts)
    sum_path = year_dir / "sum.txt"
    sum_path.write_text(combined, encoding="utf-8")
    log.info("  Wrote sum.txt (%d chars, %d parts)", len(combined), len(parts))

    # 2. Token count
    text_tokens = count_tokens(combined)
    log.info("  Estimated tokens: %d (budget: %d)", text_tokens, MAX_TEXT_TOKENS)

    scores_text = combined
    if text_tokens > MAX_TEXT_TOKENS:
        log.info("  Over budget — compressing …")
        scores_text = compress_text(combined, MAX_TEXT_TOKENS)
        zip_path = year_dir / "sum_zipped.txt"
        zip_path.write_text(scores_text, encoding="utf-8")
        log.info("  Wrote sum_zipped.txt (%d chars)", len(scores_text))

    # 3. Final rating
    rate_path = year_dir / "rate.txt"
    prompt = build_rate_prompt(template, scores_text)
    try:
        log.info("  Calling model for final rating …")
        reply = call_model(prompt)
        rate_path.write_text(reply, encoding="utf-8")
        log.info("  ✓ Wrote rate.txt (%d chars)", len(reply))
        return True
    except Exception:
        log.exception("  ✗ Failed to produce rating for %s/%s", company, year)
        return False


def main() -> None:
    template = PROMPT_RATE_NEWS_PATH.read_text(encoding="utf-8")

    if not NEWS_EVIDENCES_DIR.exists():
        log.error("NewsEvidences directory not found: %s", NEWS_EVIDENCES_DIR)
        sys.exit(1)

    company_dirs = sorted(d for d in NEWS_EVIDENCES_DIR.iterdir() if d.is_dir())
    folders_processed = 0

    for company_dir in company_dirs:
        year_dirs = sorted(d for d in company_dir.iterdir() if d.is_dir())
        for year_dir in year_dirs:
            log.info("=== %s / %s ===", company_dir.name, year_dir.name)
            did_work = process_folder(company_dir, year_dir, template)

            if did_work:
                folders_processed += 1
                if folders_processed >= REST_EVERY_N:
                    log.info("⏸ Resting %d s after %d folders …", REST_SECONDS, REST_EVERY_N)
                    time.sleep(REST_SECONDS)
                    folders_processed = 0

    log.info("Done.")


if __name__ == "__main__":
    main()
