"""News Step 3 — Parse rate.txt files and export scores to a CSV table.

Walks all {company}/{fiscal_year}/rate.txt files under NewsEvidences/, parses
the JSON output from RateNews.txt, and writes a summary CSV with columns:
  Company, FiscalYear, Credibility Score, StrategicRelevance Score, Depth Score, Mean Score

Usage:  python ScoreModel/news_step3_scores_to_csv.py [output.csv]
        Default output: ScoreModel/news_scores.csv
"""

import csv
import json
import logging
import re
import sys
from pathlib import Path

from news_config import NEWS_EVIDENCES_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

SCORE_FIELDS = ["Credibility", "StrategicRelevance", "Depth"]
CSV_HEADERS = [
    "Company", "FiscalYear",
    "Credibility Score", "StrategicRelevance Score", "Depth Score",
    "Mean Score",
]


def _extract_json(text: str) -> dict | None:
    text = text.strip()
    fenced = re.search(r"```(?:json)?\s*([\s\S]+?)\s*```", text)
    if fenced:
        text = fenced.group(1)
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        m = re.search(r"\{[\s\S]+\}", text)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass
    return None


def parse_rate_file(rate_path: Path) -> dict | None:
    text = rate_path.read_text(encoding="utf-8")
    data = _extract_json(text)
    if data is None:
        log.warning("  Cannot parse JSON from %s", rate_path)
        return None

    scores = {}
    for field in SCORE_FIELDS:
        entry = data.get(field, {})
        if isinstance(entry, dict):
            scores[field] = entry.get("score")
        elif isinstance(entry, (int, float)):
            scores[field] = entry
        else:
            scores[field] = None
    return scores


def main() -> None:
    output_path = (
        Path(sys.argv[1]) if len(sys.argv) > 1
        else Path(__file__).parent / "news_scores.csv"
    )

    if not NEWS_EVIDENCES_DIR.exists():
        log.error("NewsEvidences directory not found: %s", NEWS_EVIDENCES_DIR)
        sys.exit(1)

    rows = []
    for company_dir in sorted(d for d in NEWS_EVIDENCES_DIR.iterdir() if d.is_dir()):
        for year_dir in sorted(d for d in company_dir.iterdir() if d.is_dir()):
            rate_path = year_dir / "rate.txt"
            if not rate_path.exists():
                log.warning("Missing rate.txt: %s", rate_path)
                continue

            scores = parse_rate_file(rate_path)
            if scores is None:
                continue

            c = scores.get("Credibility")
            sr = scores.get("StrategicRelevance")
            d = scores.get("Depth")

            valid = [s for s in [c, sr, d] if s is not None]
            mean = round(sum(valid) / len(valid), 4) if valid else None

            rows.append({
                "Company": company_dir.name,
                "FiscalYear": year_dir.name,
                "Credibility Score": c,
                "StrategicRelevance Score": sr,
                "Depth Score": d,
                "Mean Score": mean,
            })
            log.info("  %s / %s → C=%.2f SR=%.2f D=%.2f Mean=%.2f",
                     company_dir.name, year_dir.name,
                     c or 0, sr or 0, d or 0, mean or 0)

    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writeheader()
        writer.writerows(rows)

    log.info("Wrote %d rows to %s", len(rows), output_path)


if __name__ == "__main__":
    main()
