"""Step 2 — Consolidate chunk-level evidence and produce a final rating.

For each {company}/{year}/ folder under Evidences/:
  1. Concatenate all part_*.txt → sum.txt
  2. Count tokens; if within limit, score directly with RateAR.txt
  3. If over limit, compress first → sum_zipped.txt, then score
  4. Save final model output as rate.txt

Usage:  python ScoreModel/step2_consolidate_rate.py
"""

import logging
import sys
import time
from pathlib import Path

import ollama
import tiktoken

_CLIENT = ollama.Client(timeout=1800)  # 30-minute timeout

from config import (
    EVIDENCES_DIR,
    MODEL_NAME,
    NUM_CTX,
    PROMPT_RATE_AR_PATH,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Rest settings: sleep REST_SECONDS every REST_EVERY_N folders to protect hardware
REST_EVERY_N = 30
REST_SECONDS = 5 * 60  # 5 minutes

# Tiktoken encoder for accurate token counting (cl100k_base is a good approximation)
_ENC = tiktoken.get_encoding("cl100k_base")

# Reserve tokens for prompt template + model output
TOKEN_BUDGET = NUM_CTX  # 32 768
PROMPT_OVERHEAD = 2000  # approximate tokens used by the template itself
MAX_TEXT_TOKENS = TOKEN_BUDGET - PROMPT_OVERHEAD  # tokens available for [TEXT]
# Overhead for the compress prompt itself (instructions before the text)
COMPRESS_PROMPT_OVERHEAD = 300


def count_tokens(text: str) -> int:
    """Count tokens using tiktoken."""
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
    """Split text into parts where each part has at most max_tokens tokens."""
    tokens = _ENC.encode(text)
    parts = []
    for i in range(0, len(tokens), max_tokens):
        part_tokens = tokens[i : i + max_tokens]
        parts.append(_ENC.decode(part_tokens))
    return parts


def compress_text(text: str, target_tokens: int) -> str:
    """Compress text so the result fits within target_tokens.

    Because qwen3:8b has a fixed 32K context window, we must ensure each
    compression call (prompt + input text) fits within that window.
    Strategy:
      1. Split the text into parts that each fit in the model context
         (TOKEN_BUDGET - COMPRESS_PROMPT_OVERHEAD).
      2. Compress each part to (target_tokens / num_parts) tokens.
      3. Concatenate the compressed parts.
    """
    max_input_tokens = TOKEN_BUDGET - COMPRESS_PROMPT_OVERHEAD
    parts = _split_text_by_tokens(text, max_input_tokens)
    num_parts = len(parts)
    per_part_target = max(target_tokens // num_parts, 200)  # at least 200 tokens
    per_part_chars = per_part_target * 3  # rough char estimate for the prompt

    log.info("    Compression: %d parts, target %d tokens/part", num_parts, per_part_target)

    compressed_parts = []
    for i, part in enumerate(parts):
        log.info("    Compressing part %d/%d (%d tokens) …", i + 1, num_parts, count_tokens(part))
        prompt = (
            "You are a financial text compressor. Compress the following text "
            f"to at most {per_part_chars} characters while preserving ALL key "
            "information relevant to scoring disclosure quality: reliability, "
            "relevance, understandability, credibility, strategic relevance, "
            "and depth. Keep specific facts, numbers, quotes, and scores. "
            "Remove only redundancy and filler.\n\n"
            f"TEXT:\n{part}"
        )
        compressed_parts.append(call_model(prompt))

    return "\n\n".join(compressed_parts)


def build_rate_prompt(template: str, scores_text: str) -> str:
    """Fill RateAR template: replace [MULTIPLE_SCORES] with the concatenated part scores."""
    return template.replace("[MULTIPLE_SCORES]", scores_text)


def process_folder(company_dir: Path, year_dir: Path, template: str):
    """Process a single company/year folder."""
    company_name = company_dir.name
    year = year_dir.name

    # Resume: skip if rate.txt already exists and is non-empty
    rate_path = year_dir / "rate.txt"
    if rate_path.exists() and rate_path.stat().st_size > 0:
        log.info("  [skip] %s/%s — rate.txt already exists", company_name, year)
        return

    # 1. Collect and sort part files
    parts = sorted(year_dir.glob("part_*.txt"))
    if not parts:
        log.warning("  No part files in %s/%s — skipping", company_name, year)
        return

    # 2. Concatenate → sum.txt
    sum_path = year_dir / "sum.txt"
    combined = "\n\n".join(p.read_text(encoding="utf-8") for p in parts)
    sum_path.write_text(combined, encoding="utf-8")
    log.info("  Wrote sum.txt (%d chars, %d parts)", len(combined), len(parts))

    # 3. Token count (tiktoken-based)
    text_tokens = count_tokens(combined)
    log.info("  Estimated tokens: %d (budget: %d)", text_tokens, MAX_TEXT_TOKENS)

    scores_text = combined

    if text_tokens > MAX_TEXT_TOKENS:
        log.info("  Over budget — compressing …")
        compressed = compress_text(combined, MAX_TEXT_TOKENS)
        zip_path = year_dir / "sum_zipped.txt"
        zip_path.write_text(compressed, encoding="utf-8")
        log.info("  Wrote sum_zipped.txt (%d chars)", len(compressed))
        scores_text = compressed

    # 4. Final rating via RateAR prompt
    prompt = build_rate_prompt(template, scores_text)

    try:
        log.info("  Calling model for final rating …")
        reply = call_model(prompt)
        rate_path.write_text(reply, encoding="utf-8")
        log.info("  ✓ Wrote rate.txt (%d chars)", len(reply))
    except Exception:
        log.exception("  ✗ Failed to produce rating for %s/%s", company_name, year)


def main():
    template = PROMPT_RATE_AR_PATH.read_text(encoding="utf-8")

    if not EVIDENCES_DIR.exists():
        log.error("Evidences directory not found: %s", EVIDENCES_DIR)
        sys.exit(1)

    # Walk company/year directories
    company_dirs = sorted(
        d for d in EVIDENCES_DIR.iterdir() if d.is_dir()
    )

    folders_processed = 0  # count of actually-processed folders since last rest

    for company_dir in company_dirs:
        year_dirs = sorted(
            d for d in company_dir.iterdir() if d.is_dir()
        )
        for year_dir in year_dirs:
            log.info("=== %s / %s ===", company_dir.name, year_dir.name)
            process_folder(company_dir, year_dir, template)

            folders_processed += 1
            if folders_processed >= REST_EVERY_N:
                log.info("⏸ Resting %d seconds after %d folders …",
                         REST_SECONDS, REST_EVERY_N)
                time.sleep(REST_SECONDS)
                folders_processed = 0

    log.info("Done.")


if __name__ == "__main__":
    main()
