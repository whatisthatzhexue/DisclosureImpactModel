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

## ScoreModel
- This section of code supports automatically invoking the `qwen3:4b` model in Ollama (32K context window).
- **Step 1 — Per-chunk scoring** (`step1_score_chunks.py`):
  - Reads `chunks_summary.csv` to iterate over every annual-report chunk.
  - For each chunk, fills `PromptAR.txt` with the chunk text and section name, then calls `qwen3:8b`.
  - Saves the model response to `Evidences/{company}/{year}/part_{YY}{id}.txt`.
  - Scores three dimensions: **reliability**, **understandability**, and **relevance**.
- **Step 2 — Consolidation & final rating** (`step2_consolidate_rate.py`):
  - Concatenates all `part_*.txt` files for a company-year into `sum.txt`.
  - Estimates token count; if it exceeds the 32K budget, compresses into `sum_zipped.txt`.
  - Inserts the (possibly compressed) evidence into `PromptNews.txt` and calls `qwen3:4b` for a final rating.
  - Saves the result as `rate.txt`, scoring three dimensions: **credibility**, **strategic relevance**, and **depth**.