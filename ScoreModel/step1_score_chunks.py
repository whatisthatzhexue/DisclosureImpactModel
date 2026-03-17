"""Step 1 — Score each annual-report chunk with qwen3:8b via ollama.

Usage:  python ScoreModel/step1_score_chunks.py
"""

import csv
import logging
import sys
from pathlib import Path

import ollama

from config import (
    CHUNKS_CSV,
    COMPANY_CODE_TO_NAME,
    EVIDENCES_DIR,
    MODEL_NAME,
    NUM_CTX,
    PROMPT_AR_PATH,
    chunk_filepath,
    parse_source,
    year_2to4,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def build_prompt(template: str, chunk_text: str, section: str) -> str:
    """Fill the PromptAR template with the actual chunk text and section name."""
    prompt = template.replace("[TEXT]Annual Report[/TEXT]", f"[TEXT]{chunk_text}[/TEXT]")
    prompt = prompt.replace("[section]", section)
    return prompt


def call_model(prompt: str) -> str:
    """Send a prompt to the local ollama model and return the response text."""
    response = ollama.chat(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        options={"num_ctx": NUM_CTX},
    )
    return response["message"]["content"]


def main():
    # Load prompt template
    prompt_template = PROMPT_AR_PATH.read_text(encoding="utf-8")

    # Read CSV
    with open(CHUNKS_CSV, encoding="utf-8", newline="") as f:
        rows = list(csv.DictReader(f))

    log.info("Loaded %d chunk rows from CSV", len(rows))

    # Group rows by (company_code, year) to process one source-file at a time
    groups: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        code, yy = parse_source(row["source"])
        if code is None:
            log.warning("Cannot parse source: %s — skipping", row["source"])
            continue
        groups.setdefault((code, yy), []).append(row)

    total_chunks = sum(len(v) for v in groups.values())
    processed = 0

    for (code, yy), chunk_rows in groups.items():
        company = COMPANY_CODE_TO_NAME[code]
        year4 = year_2to4(yy)
        out_dir = EVIDENCES_DIR / company / str(year4)
        out_dir.mkdir(parents=True, exist_ok=True)

        log.info("=== %s / %d  (%d chunks) ===", company, year4, len(chunk_rows))

        for idx, row in enumerate(chunk_rows):
            out_file = out_dir / f"part_{yy}{idx:02d}.txt"

            # Skip if already scored
            if out_file.exists() and out_file.stat().st_size > 0:
                log.info("  [skip] %s (already exists)", out_file.name)
                processed += 1
                continue

            # Read chunk text
            chunk_path = chunk_filepath(row["file_path"])
            if not chunk_path.exists():
                log.error("  Chunk file missing: %s — skipping", chunk_path)
                processed += 1
                continue

            chunk_text = chunk_path.read_text(encoding="utf-8")
            section = row.get("sections", "Annual Report")

            prompt = build_prompt(prompt_template, chunk_text, section)

            try:
                log.info("  [%d/%d] Scoring %s → %s …",
                         processed + 1, total_chunks, chunk_path.name, out_file.name)
                reply = call_model(prompt)
                out_file.write_text(reply, encoding="utf-8")
                log.info("  ✓ saved %s (%d chars)", out_file.name, len(reply))
            except Exception:
                log.exception("  ✗ Failed on %s", chunk_path.name)

            processed += 1

    log.info("Done. %d / %d chunks processed.", processed, total_chunks)


if __name__ == "__main__":
    main()
