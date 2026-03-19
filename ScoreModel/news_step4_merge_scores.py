"""News Step 4 — Merge two news scores CSV files into a combined comparison table.

The output CSV has a two-row header:
  Row 1: "", "", "第一次打分", "", "", "", "第二次打分", "", "", ""
  Row 2: Company, FiscalYear, Credibility Score, StrategicRelevance Score,
          Depth Score, Mean Score,
          Credibility Score, StrategicRelevance Score,
          Depth Score, Mean Score

Rows are matched by (Company, FiscalYear). Entries present in only one CSV
are still included, with the missing side left blank.

Usage:  python ScoreModel/news_step4_merge_scores.py <csv1> <csv2> [output.csv]
        Default output: ScoreModel/news_scores_merged.csv
"""

import csv
import sys
from pathlib import Path

SCORE_COLS = [
    "Credibility Score",
    "StrategicRelevance Score",
    "Depth Score",
    "Mean Score",
]


def read_csv(path: Path) -> dict[tuple[str, str], dict]:
    result = {}
    with open(path, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            key = (row["Company"], row["FiscalYear"])
            result[key] = row
    return result


def main() -> None:
    if len(sys.argv) < 3:
        print("Usage: python news_step4_merge_scores.py <csv1> <csv2> [output.csv]")
        sys.exit(1)

    csv1_path = Path(sys.argv[1])
    csv2_path = Path(sys.argv[2])
    out_path = (
        Path(sys.argv[3]) if len(sys.argv) > 3
        else Path(__file__).parent / "news_scores_merged.csv"
    )

    data1 = read_csv(csv1_path)
    data2 = read_csv(csv2_path)

    all_keys = sorted(set(data1) | set(data2))

    label_row = ["", "", "第一次打分", "", "", "", "第二次打分", "", "", ""]
    field_row = ["Company", "FiscalYear"] + SCORE_COLS + SCORE_COLS

    with open(out_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(label_row)
        writer.writerow(field_row)
        for company, fy in all_keys:
            r1 = data1.get((company, fy), {})
            r2 = data2.get((company, fy), {})
            row = [company, fy]
            for col in SCORE_COLS:
                row.append(r1.get(col, ""))
            for col in SCORE_COLS:
                row.append(r2.get(col, ""))
            writer.writerow(row)

    print(f"Wrote {len(all_keys)} rows to {out_path}")


if __name__ == "__main__":
    main()
