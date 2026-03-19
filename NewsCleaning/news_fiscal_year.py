"""News fiscal-year preprocessing.

Reads each company's news CSV from NewsCleaning/News/{company}/{company}_news.csv,
assigns a fiscal year label to every article based on the company's fiscal year end,
and writes the result to NewsCleaning/News/{company}/{company}_news_fy.csv.

Fiscal year end dates (month, day):
  Berjaya Food Berhad   — April 30   → FY label = year in which April 30 falls
  Fraser & Neave (F&N)  — September 30
  Power Root Berhad     — April 30
  QL Resources Berhad   — March 31

Rule: an article published on date D belongs to fiscal year FY if
  FY_start < D <= FY_end
where FY_end = (fy_end_month, fy_end_day) of that FY year.

Example (Berjaya, FY end = Apr 30):
  FY2024 covers 2023-05-01 … 2024-04-30
  FY2025 covers 2024-05-01 … 2025-04-30

Output columns: all original columns + "fiscal_year" (int, e.g. 2025).

Usage:  python NewsCleaning/news_fiscal_year.py
"""

import pandas as pd
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
NEWS_DIR = SCRIPT_DIR / "News"

# ── Fiscal year end configuration ─────────────────────────────────────────────
# (month, day) of fiscal year end for each company folder name
FISCAL_YEAR_END: dict[str, tuple[int, int]] = {
    "Berjaya": (4, 30),   # April 30
    "F&N":     (9, 30),   # September 30
    "Power":   (4, 30),   # April 30
    "QL":      (3, 31),   # March 31
}


def assign_fiscal_year(date: pd.Timestamp, fy_end_month: int, fy_end_day: int) -> int | None:
    """Return the fiscal year label for a given publication date.

    The fiscal year whose end date is (fy_end_month, fy_end_day) in year Y
    covers the period from the previous year's end + 1 day through that end date.

    Returns None if date is NaT.
    """
    if pd.isna(date):
        return None

    # Try the FY ending in the same calendar year as the article
    for candidate_year in [date.year, date.year + 1]:
        try:
            fy_end = pd.Timestamp(candidate_year, fy_end_month, fy_end_day)
        except ValueError:
            # e.g. Feb 29 in a non-leap year — use last valid day
            fy_end = pd.Timestamp(candidate_year, fy_end_month, 28)

        fy_start = fy_end - pd.DateOffset(years=1) + pd.Timedelta(days=1)
        if fy_start <= date <= fy_end:
            return candidate_year

    return None


def process_company(company: str, fy_end_month: int, fy_end_day: int) -> None:
    csv_in = NEWS_DIR / company / f"{company}_news.csv"
    csv_out = NEWS_DIR / company / f"{company}_news_fy.csv"

    if not csv_in.exists():
        print(f"[WARN] {csv_in} not found — skipping {company}")
        return

    df = pd.read_csv(csv_in, encoding="utf-8")
    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce")

    df["fiscal_year"] = df["published_date"].apply(
        assign_fiscal_year, fy_end_month=fy_end_month, fy_end_day=fy_end_day
    )

    df.to_csv(csv_out, index=False, encoding="utf-8-sig")

    fy_counts = df["fiscal_year"].value_counts().sort_index()
    print(f"{company}: {len(df)} articles → {csv_out.name}")
    for fy, cnt in fy_counts.items():
        print(f"  FY{fy}: {cnt} articles")
    na_count = df["fiscal_year"].isna().sum()
    if na_count:
        print(f"  (unassigned: {na_count})")


def main() -> None:
    for company, (month, day) in FISCAL_YEAR_END.items():
        process_company(company, month, day)
    print("Done.")


if __name__ == "__main__":
    main()
