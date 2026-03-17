"""Step 2 — Consolidate chunk-level evidence and produce a final rating.

For each {company}/{year}/ folder under Evidences/:
  1. Concatenate all part_*.txt → sum.txt
  2. Estimate tokens; if within limit, score directly with PromptNews.txt
  3. If over limit, compress first → sum_zipped.txt, then score
  4. Save final model output as rate.txt

Usage:  python ScoreModel/step2_consolidate_rate.py
"""

import logging
import sys
from pathlib import Path

import ollama

from config import (
    COMPANY_FULL_NAMES,
    COMPANY_CODE_TO_NAME,
    COMPANY_INDUSTRY,
    EVIDENCES_DIR,
    MODEL_NAME,
    NUM_CTX,
    PROMPT_NEWS_PATH,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Reverse lookup: folder name → company code
_NAME_TO_CODE = {v: k for k, v in COMPANY_CODE_TO_NAME.items()}

# Reserve tokens for prompt template + model output
TOKEN_BUDGET = NUM_CTX  # 32 768
PROMPT_OVERHEAD = 2000  # approximate tokens used by the template itself
MAX_TEXT_TOKENS = TOKEN_BUDGET - PROMPT_OVERHEAD  # tokens available for [TEXT]


def estimate_tokens(text: str) -> int:
    """Rough token estimate: ~1 token per 3 characters."""
    return len(text) // 3


def call_model(prompt: str) -> str:
    response = ollama.chat(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        options={"num_ctx": NUM_CTX},
    )
    return response["message"]["content"]


def compress_text(text: str, target_tokens: int) -> str:
    """Ask the model to compress text to fit within target_tokens."""
    target_chars = target_tokens * 3
    prompt = (
        "You are a financial text compressor. Compress the following text "
        f"to at most {target_chars} characters while preserving ALL key "
        "information relevant to scoring disclosure quality: reliability, "
        "relevance, understandability, credibility, strategic relevance, "
        "and depth. Keep specific facts, numbers, quotes, and scores. "
        "Remove only redundancy and filler.\n\n"
        f"TEXT:\n{text}"
    )
    return call_model(prompt)


def build_news_prompt(template: str, evidence_text: str,
                      company_code: str) -> str:
    """Fill PromptNews template with evidence text and company context."""
    full_name = COMPANY_FULL_NAMES.get(company_code, "Unknown Company")
    industry = COMPANY_INDUSTRY.get(company_code, "food and beverage")

    prompt = template.replace("[TEXT]News Article[/TEXT]",
                              f"[TEXT]{evidence_text}[/TEXT]")
    prompt = prompt.replace("[Company Name]", full_name)
    prompt = prompt.replace("[industry]", industry)
    return prompt


def process_folder(company_dir: Path, year_dir: Path, template: str):
    """Process a single company/year folder."""
    company_name = company_dir.name
    company_code = _NAME_TO_CODE.get(company_name)
    year = year_dir.name

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

    # 3. Token estimate
    text_tokens = estimate_tokens(combined)
    log.info("  Estimated tokens: %d (budget: %d)", text_tokens, MAX_TEXT_TOKENS)

    evidence_text = combined

    if text_tokens > MAX_TEXT_TOKENS:
        log.info("  Over budget — compressing …")
        compressed = compress_text(combined, MAX_TEXT_TOKENS)
        zip_path = year_dir / "sum_zipped.txt"
        zip_path.write_text(compressed, encoding="utf-8")
        log.info("  Wrote sum_zipped.txt (%d chars)", len(compressed))
        evidence_text = compressed

    # 4. Final rating
    rate_path = year_dir / "rate.txt"
    prompt = build_news_prompt(template, evidence_text, company_code)

    try:
        log.info("  Calling model for final rating …")
        reply = call_model(prompt)
        rate_path.write_text(reply, encoding="utf-8")
        log.info("  ✓ Wrote rate.txt (%d chars)", len(reply))
    except Exception:
        log.exception("  ✗ Failed to produce rating for %s/%s", company_name, year)


def main():
    template = PROMPT_NEWS_PATH.read_text(encoding="utf-8")

    if not EVIDENCES_DIR.exists():
        log.error("Evidences directory not found: %s", EVIDENCES_DIR)
        sys.exit(1)

    # Walk company/year directories
    company_dirs = sorted(
        d for d in EVIDENCES_DIR.iterdir() if d.is_dir()
    )

    for company_dir in company_dirs:
        year_dirs = sorted(
            d for d in company_dir.iterdir() if d.is_dir()
        )
        for year_dir in year_dirs:
            log.info("=== %s / %s ===", company_dir.name, year_dir.name)
            process_folder(company_dir, year_dir, template)

    log.info("Done.")


if __name__ == "__main__":
    main()
