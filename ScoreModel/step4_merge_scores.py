"""Step 4 — Merge two scores CSV files into a combined comparison table.

The output CSV has a two-row header:
  Row 1: "", "", "第一次打分", "", "", "", "第二次打分", "", "", ""
  Row 2: Company, Year, Reliability Score, Relevance Score,
          Understandability Score, Mean Score,
          Reliability Score, Relevance Score,
          Understandability Score, Mean Score

Rows are matched by (Company, Year). Companies/years present in only one CSV
are still included, with the missing side left blank.

Usage:  python ScoreModel/step4_merge_scores.py [csv1] [csv2] [output.csv]
        If csv1/csv2 are omitted, the script prompts for them interactively.
        Default output: ScoreModel/scores_merged.csv
"""

import csv
import sys
from pathlib import Path

SCORE_COLS = [
    "Reliability Score",
    "Relevance Score",
    "Understandability Score",
    "Mean Score",
]


def read_csv(path: Path) -> dict[tuple[str, str], dict]:
    """Read a scores CSV and return a dict keyed by (Company, Year)."""
    result = {}
    with open(path, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            key = (row["Company"], row["Year"])
            result[key] = row
    return result


def prompt_csv_path(label: str) -> Path:
    """Prompt the user to enter a CSV file path, retrying until valid."""
    while True:
        raw = input(f"{label}: ").strip().strip('"').strip("'")
        p = Path(raw)
        if p.exists() and p.suffix.lower() == ".csv":
            return p
        print(f"  File not found or not a CSV: {raw!r}  — please try again.")


def main():
    # Accept paths from command-line args or interactively
    if len(sys.argv) >= 3:
        csv1_path = Path(sys.argv[1])
        csv2_path = Path(sys.argv[2])
    else:
        print("请输入两个 CSV 文件路径（可直接拖入文件）：")
        csv1_path = prompt_csv_path("第一次打分 CSV")
        csv2_path = prompt_csv_path("第二次打分 CSV")

    out_path = Path(sys.argv[3]) if len(sys.argv) > 3 else Path(__file__).parent / "scores_merged.csv"

    data1 = read_csv(csv1_path)
    data2 = read_csv(csv2_path)

    # Union of all (Company, Year) keys, sorted
    all_keys = sorted(set(data1) | set(data2))

    # Build header rows
    label_row = ["", "", "1st Scores", "", "", "", "2nd Scores", "", "", ""]
    field_row = ["Company", "Year"] + SCORE_COLS + SCORE_COLS

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(label_row)
        writer.writerow(field_row)

        for company, year in all_keys:
            r1 = data1.get((company, year), {})
            r2 = data2.get((company, year), {})
            row = [company, year]
            for col in SCORE_COLS:
                row.append(r1.get(col, ""))
            for col in SCORE_COLS:
                row.append(r2.get(col, ""))
            writer.writerow(row)

    print(f"Wrote {len(all_keys)} rows to {out_path}")


if __name__ == "__main__":
    main()
