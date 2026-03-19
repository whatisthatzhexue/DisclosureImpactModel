# DisclosureImpactModel

This repository contains code and data for investigating corporate transparency (including news and annual report disclosure).

## AnnualReportCleaning
- This section provides the manually annotated file `pdf_pages.xlsx` that first filters out the chapters related to information disclosure in annual reports.
- Then, it uses machine cleaning methods to remove residual elements such as headers and footers that do not provide any textual information, facilitating subsequent analysis.
- Last but not least, it splits the annual report text into smaller chunks to fit within the 32K context window.

## CorporateBHAR
- The BHAR (Buy-and-Hold Abnormal Returns) for the following four companies from 2017 to 2025 is calculated based on stock price data from the WRDS platform:
    - Fraser & Neave Holdings Bhd
    - QL Resources Berhad
    - Berjaya Food Berhad
    - Power Root Berhad
- Calculation method: Access and process the stock price data for the specified period via the Yahoo Finance, then compute BHAR accordingly. The resulting data files are saved in the `CorporateBHAR` directory.

## NewsCleaning
- `news_v1.csv` is from the Kaggle dataset: News Article (Weekly Updated) \- Malaysia biggest online news collection on Kaggle (https://www.kaggle.com/datasets/azraimohamad/news-article-weekly-updated/versions/98)
- Download method: Use the Kaggle API or download directly from the above link, and place it in the `NewsCleaning` directory.
- **Fiscal-year preprocessing** (`news_fiscal_year.py`):
  - Assigns each article to a fiscal year based on the company's fiscal year end date.
  - Fiscal year end dates: Berjaya Food — April 30; F&N — September 30; Power Root — April 30; QL Resources — March 31.
  - Outputs `{company}_news_fy.csv` with an added `fiscal_year` column to `NewsCleaning/News/{company}/`.
  - Must be run before the news scoring pipeline.

## ScoreModel

All models are `qwen3:8b` (Ollama, 32K context window). Two independent scoring pipelines share the same `ScoreModel/Prompt/` templates.

### Annual Report Scoring Pipeline

- **Step 1 — Per-chunk scoring** (`step1_score_chunks.py`):
  - Reads `AnnualReportCleaning/Chunks/chunks_summary.csv` to iterate over every annual-report chunk.
  - For each chunk, fills `PromptAR.txt` with the chunk text and section name, then calls `qwen3:8b`.
  - Saves the model response to `Evidences/{company}/{year}/part_{YY}{id}.txt`.
  - Scores three dimensions: **Reliability**, **Relevance**, and **Understandability**.
- **Step 2 — Consolidation & final rating** (`step2_consolidate_rate.py`):
  - Concatenates all `part_*.txt` files for a company-year into `sum.txt`.
  - Estimates token count (tiktoken); if it exceeds the 32K budget, compresses into `sum_zipped.txt`.
  - Inserts the (possibly compressed) evidence into `RateAR.txt` and calls `qwen3:8b` for a final rating.
  - Saves the result as `rate.txt`.
- **Step 3 — Export scores** (`step3_scores_to_csv.py`):
  - Parses all `rate.txt` files and writes `scores.csv` with columns: Company, Year, Reliability Score, Relevance Score, Understandability Score, Mean Score.
- **Step 4 — Merge runs** (`step4_merge_scores.py`):
  - Merges two `scores.csv` files (e.g. two scoring runs) into a side-by-side comparison table `scores_merged.csv`.

### News Scoring Pipeline

Prerequisites: run `NewsCleaning/news_fiscal_year.py` first.

- **Config** (`news_config.py`): shared paths, company names, model settings.
- **Step 1 — Per-article scoring** (`news_step1_score_articles.py`):
  - Reads `{company}_news_fy.csv` and scores each article using `PromptNews.txt`.
  - Saves results to `NewsEvidences/{company}/{fiscal_year}/part_{FY2}{id}.txt`.
  - Scores three dimensions: **Credibility**, **Strategic Relevance**, and **Depth**.
- **Step 2 — Consolidation & final rating** (`news_step2_consolidate_rate.py`):
  - Concatenates all `part_*.txt` for a company-fiscal_year into `sum.txt`.
  - Compresses to `sum_zipped.txt` if token count exceeds budget.
  - Inserts evidence into `RateNews.txt` and calls `qwen3:8b` for a final rating.
  - Saves the result as `rate.txt`.
- **Step 3 — Export scores** (`news_step3_scores_to_csv.py`):
  - Parses all `rate.txt` files and writes `news_scores.csv` with columns: Company, FiscalYear, Credibility Score, StrategicRelevance Score, Depth Score, Mean Score.
- **Step 4 — Merge runs** (`news_step4_merge_scores.py`):
  - Merges two `news_scores.csv` files into a side-by-side comparison table `news_scores_merged.csv`.
