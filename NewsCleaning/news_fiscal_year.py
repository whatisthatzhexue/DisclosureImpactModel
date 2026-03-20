"""News fiscal-year preprocessing.

Reads each company's news CSV from NewsCleaning/News/{company}/{company}_news.csv,
assigns a fiscal year label to every article based on the company's fiscal year end,
and writes the result to NewsCleaning/News/{company}/{company}_news_fy.csv.

Fiscal year end dates:
  QL Resources Berhad   — March 31
  Power Root Berhad     — March 31
  Berjaya Food Berhad   — April 30 from 2016 to 2018, June 30 from 2019 to 2024
  Fraser & Neave (F&N)  — September 30

Rule: an article published on date D belongs to fiscal year FY if
  FY_start < D <= FY_end
where FY_end = (fy_end_month, fy_end_day) of that FY year.

For Berjaya, the transition between fiscal year ends is handled explicitly:
  - Dates before 2018-05-01 use the April 30 rule (FY ends in year Y).
  - Dates from 2018-05-01 to 2019-06-30 are assigned to FY2019 (ending June 30, 2019).
  - Dates from 2019-07-01 onward use the June 30 rule (FY ends in year Y).

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
# For Berjaya, the logic is handled separately; the tuple here is a placeholder.
FISCAL_YEAR_END: dict[str, tuple[int, int]] = {
    "Berjaya": (4, 30),   # April 30 (only used as fallback)
    "F&N":     (9, 30),   # September 30
    "Power":   (3, 31),   # March 31
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


def assign_berjaya_fiscal_year(date: pd.Timestamp) -> int | None:
    """Return fiscal year label for Berjaya, handling the transition from
    April 30 to June 30 end dates.
    """
    if pd.isna(date):
        return None

    # 1. Period before May 1, 2018: use April 30 rule (FY ends April 30)
    if date < pd.Timestamp(2018, 5, 1):
        # Try with April 30 for candidate_year = date.year and date.year+1
        for candidate_year in [date.year, date.year + 1]:
            fy_end = pd.Timestamp(candidate_year, 4, 30)
            fy_start = fy_end - pd.DateOffset(years=1) + pd.Timedelta(days=1)
            if fy_start <= date <= fy_end:
                return candidate_year
        return None

    # 2. Transition period: May 1, 2018 – June 30, 2019 → FY2019 (ends June 30, 2019)
    if date < pd.Timestamp(2019, 7, 1):
        # Check if date falls within FY2019: 2018-05-01 to 2019-06-30
        if date >= pd.Timestamp(2018, 5, 1):
            return 2019
        else:
            # Should not happen because of the previous condition, but fallback
            return None

    # 3. Period from July 1, 2019 onward: use June 30 rule
    # FY Y covers from July 1, Y-1 to June 30, Y
    # For a given date, the fiscal year label is the year in which June 30 falls
    # that contains the date.
    # Example: 2019-07-15 → FY2020 (June 30, 2020)
    candidate_year = date.year if date.month >= 7 else date.year + 1
    # Limit to years up to 2024 as per description, but allow beyond
    if candidate_year < 2019:
        candidate_year = 2019  # adjust for very early dates (should not occur)
    fy_end = pd.Timestamp(candidate_year, 6, 30)
    fy_start = pd.Timestamp(candidate_year - 1, 7, 1)
    if fy_start <= date <= fy_end:
        return candidate_year
    # If not matched, try the adjacent year (safety)
    for offset in [-1, 1]:
        alt_year = candidate_year + offset
        if alt_year < 2019:
            continue
        fy_end = pd.Timestamp(alt_year, 6, 30)
        fy_start = pd.Timestamp(alt_year - 1, 7, 1)
        if fy_start <= date <= fy_end:
            return alt_year
    return None


def process_company(company: str, fy_end_month: int, fy_end_day: int) -> None:
    csv_in = NEWS_DIR / company / f"{company}_news.csv"
    csv_out = NEWS_DIR / company / f"{company}_news_fy.csv"

    if not csv_in.exists():
        print(f"[WARN] {csv_in} not found — skipping {company}")
        return

    df = pd.read_csv(csv_in, encoding="utf-8")
    df["published_date"] = pd.to_datetime(df["published_date"], errors="coerce")

    # Special handling for Berjaya because of fiscal year end transition
    if company == "Berjaya":
        df["fiscal_year"] = df["published_date"].apply(assign_berjaya_fiscal_year)
    else:
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