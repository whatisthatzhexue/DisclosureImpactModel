"""News Step 1 — Score each news article with qwen3:8b via ollama.

Reads preprocessed news CSVs ({company}_news_fy.csv) from NewsCleaning/News/,
scores each article using PromptNews.txt, and saves results to
ScoreModel/NewsEvidences/{company}/{fiscal_year}/part_{FY2}{idx:02d}.txt

Prerequisites:
  Run NewsCleaning/news_fiscal_year.py first to generate *_news_fy.csv files.

Usage:  python ScoreModel/news_step1_score_articles.py
"""

import logging
import time
from pathlib import Path

import ollama
import pandas as pd

from news_config import (
    COMPANIES,
    COMPANY_FULL_NAMES,
    COMPANY_INDUSTRY,
    MODEL_NAME,
    NEWS_DIR,
    NEWS_EVIDENCES_DIR,
    NUM_CTX,
    PROMPT_NEWS_PATH,
    REST_EVERY_N,
    REST_SECONDS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def build_prompt(template: str, article_text: str, company: str) -> str:
    """Fill PromptNews template with article text and company context."""
    full_name = COMPANY_FULL_NAMES[company]
    industry = COMPANY_INDUSTRY[company]
    prompt = template.replace("[TEXT]News Article[/TEXT]", f"[TEXT]{article_text}[/TEXT]")
    prompt = prompt.replace(
        "[Company Name] is a [industry] company listed in Malaysia.",
        f"{full_name} is a {industry} company listed in Malaysia.",
    )
    return prompt


def call_model(prompt: str) -> str:
    response = ollama.chat(
        model=MODEL_NAME,
        messages=[{"role": "user", "content": prompt}],
        options={"num_ctx": NUM_CTX},
    )
    return response["message"]["content"]


def format_article(row: pd.Series) -> str:
    """Combine title and text into a single string for scoring."""
    parts = []
    title = str(row.get("title", "") or "").strip()
    text = str(row.get("text", "") or "").strip()
    if title:
        parts.append(f"[TITLE] {title}")
    if text:
        parts.append(text)
    return "\n".join(parts)


def main() -> None:
    prompt_template = PROMPT_NEWS_PATH.read_text(encoding="utf-8")

    scored_since_rest = 0

    for company in COMPANIES:
        fy_csv = NEWS_DIR / company / f"{company}_news_fy.csv"
        if not fy_csv.exists():
            log.warning("Missing %s — run news_fiscal_year.py first", fy_csv)
            continue

        df = pd.read_csv(fy_csv, encoding="utf-8")
        df = df[df["fiscal_year"].notna()].copy()
        df["fiscal_year"] = df["fiscal_year"].astype(int)

        log.info("=== %s: %d articles with fiscal year ===", company, len(df))

        for fy, group in df.groupby("fiscal_year"):
            fy_str = str(fy)
            fy2 = fy_str[-2:]  # last 2 digits, e.g. "25"
            out_dir = NEWS_EVIDENCES_DIR / company / fy_str
            out_dir.mkdir(parents=True, exist_ok=True)

            log.info("  %s / FY%s  (%d articles)", company, fy_str, len(group))

            for idx, (_, row) in enumerate(group.iterrows()):
                out_file = out_dir / f"part_{fy2}{idx:02d}.txt"

                if out_file.exists() and out_file.stat().st_size > 0:
                    log.info("    [skip] %s", out_file.name)
                    continue

                article_text = format_article(row)
                if not article_text.strip():
                    log.warning("    [skip] empty article at index %d", idx)
                    continue

                prompt = build_prompt(prompt_template, article_text, company)

                try:
                    log.info("    Scoring article %d → %s …", idx, out_file.name)
                    reply = call_model(prompt)
                    out_file.write_text(reply, encoding="utf-8")
                    log.info("    ✓ saved %s (%d chars)", out_file.name, len(reply))

                    scored_since_rest += 1
                    if scored_since_rest >= REST_EVERY_N:
                        log.info("  ⏸ Resting %d s after %d scored articles …",
                                 REST_SECONDS, REST_EVERY_N)
                        time.sleep(REST_SECONDS)
                        scored_since_rest = 0
                except Exception:
                    log.exception("    ✗ Failed on article %d for %s/FY%s", idx, company, fy_str)

    log.info("Done.")


if __name__ == "__main__":
    main()
