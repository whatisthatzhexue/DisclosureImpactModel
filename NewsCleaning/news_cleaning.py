import re
import os
import pandas as pd
from functools import partial

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
script_dir = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(script_dir, 'news_v1.csv')
news_output_dir = os.path.join(script_dir, 'News')
artxt_output_dir = os.path.join(script_dir, '..', 'ChunkAndScore', 'ARTXTs')

# ---------------------------------------------------------------------------
# Company matching patterns with weights.
# Higher weight = more specific / more reliable signal for the company.
# Patterns are checked in title, summary, keywords, and text fields.
# Each article is assigned to at most ONE company (the one with the highest
# total score), so no article will appear in two companies' datasets.
# ---------------------------------------------------------------------------
COMPANY_PATTERNS = {
    "Power": [
        (r"Power Root Berhad",       5),
        (r"Power Root Bhd",          5),
        (r"PowerRoot",               4),
        (r"PWROOT",                  4),
        (r"Power[\s\-]root",         3),
        (r"\b7237\b",                2),  # Bursa Malaysia stock code
    ],
    "QL": [
        (r"QL Resources Berhad",     5),
        (r"QL Resources Bhd",        5),
        (r"QL Resources",            4),
        (r"\bQL\b",                  2),
        (r"\b7084\b",                2),  # Bursa Malaysia stock code
    ],
    "Berjaya": [
        (r"Berjaya Food Berhad",     5),
        (r"Berjaya Food Bhd",        5),
        (r"Berjaya Food",            4),
        (r"\bBFood\b",               4),
        (r"\bBFOOD\b",               4),
        (r"\b5196\b",                2),  # Bursa Malaysia stock code
    ],
    "F&N": [
        (r"Fraser & Neave Holdings Berhad", 5),
        (r"Fraser & Neave Holdings Bhd",    5),
        (r"Fraser & Neave Holdings",        4),
        (r"Fraser\s*&\s*Neave",             3),
        (r"\bF\s*&\s*N\b",                  2),
        (r"\bFNH\b",                        2),
        (r"\b3689\b",                       2),  # Bursa Malaysia stock code
    ],
}

# ---------------------------------------------------------------------------
# Compile regex patterns
# ---------------------------------------------------------------------------
compiled_patterns = {
    comp: [(re.compile(pat, re.IGNORECASE), weight) for pat, weight in patterns]
    for comp, patterns in COMPANY_PATTERNS.items()
}


def get_combined_text(row):
    """Concatenate all searchable fields into a single string."""
    parts = [
        str(row.get('title', '') or ''),
        str(row.get('summary', '') or ''),
        str(row.get('keywords', '') or ''),
        str(row.get('text', '') or ''),
    ]
    return ' '.join(parts)


def score_article(text, comp_patterns):
    """Sum weighted match counts for a company's patterns in the given text."""
    score = 0
    for pattern, weight in comp_patterns:
        score += len(pattern.findall(text)) * weight
    return score


# ---------------------------------------------------------------------------
# Load dataset
# ---------------------------------------------------------------------------
print(f"Loading {csv_path} ...")
df = pd.read_csv(csv_path, encoding='utf-8')
print(f"  Loaded {len(df)} articles.")

for col in ('title', 'text', 'summary', 'keywords'):
    if col in df.columns:
        df[col] = df[col].fillna('')

# ---------------------------------------------------------------------------
# Score every article against every company
# ---------------------------------------------------------------------------
print("Scoring articles ...")
combined_texts = df.apply(get_combined_text, axis=1)

scores_df = pd.DataFrame(
    {comp: combined_texts.map(partial(score_article, comp_patterns=compiled_patterns[comp]))
     for comp in COMPANY_PATTERNS},
    index=df.index,
)

# Assign each article to the company with the highest score.
# Articles with no match (all scores == 0) are discarded.
max_scores = scores_df.max(axis=1)
assigned_company = scores_df.idxmax(axis=1)
assigned_company[max_scores == 0] = None

# ---------------------------------------------------------------------------
# Save outputs
# ---------------------------------------------------------------------------
for comp in COMPANY_PATTERNS:
    df_comp = df[assigned_company == comp].copy()
    print(f"{comp}: {len(df_comp)} articles")

    if df_comp.empty:
        print(f"  Warning: No articles found for {comp}. Check matching patterns.")
        continue

    # --- News/{comp}/{comp}_news.csv ---
    comp_news_dir = os.path.join(news_output_dir, comp)
    os.makedirs(comp_news_dir, exist_ok=True)
    csv_out = os.path.join(comp_news_dir, f"{comp}_news.csv")
    df_comp.to_csv(csv_out, index=False, encoding='utf-8-sig')
    print(f"  Saved CSV  -> {csv_out}")

    # --- ChunkAndScore/ARTXTs/{comp}/{year_2digit}_cleaned.txt ---
    df_comp['year'] = pd.to_datetime(df_comp['published_date'], errors='coerce').dt.year

    comp_artxt_dir = os.path.join(artxt_output_dir, comp)
    os.makedirs(comp_artxt_dir, exist_ok=True)

    for year, group in df_comp.groupby('year', dropna=True):
        year_int = int(year)
        if not (2000 <= year_int <= 2099):
            print(f"  Skipping unexpected year {year_int} for {comp}")
            continue
        year_2digit = str(year_int)[-2:]
        lines = []
        for _, row in group.iterrows():
            title = str(row.get('title', '')).strip()
            text = str(row.get('text', '')).strip()
            if title:
                lines.append(f"[TITLE] {title}")
            if text:
                lines.append(text)
            lines.append('')   # blank line between articles

        txt_out = os.path.join(comp_artxt_dir, f"{year_2digit}_cleaned.txt")
        with open(txt_out, mode='w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        print(f"  Saved TXT  -> {txt_out}  ({len(group)} articles)")

print("Done!")
