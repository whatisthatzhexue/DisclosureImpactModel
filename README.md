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
- `news\_v1.csv` is from the Kaggle dataset: News Article (Weekly Updated) \- Malaysia biggest online news collection on Kaggle (https://www.kaggle.com/datasets/azraimohamad/news-article-weekly-updated/versions/98)
- Download method: Use the Kaggle API or download directly from the above link, and place it in the `NewsCleaning` directory.

## ScoreModel
- This section of code supports automatically invoking the `qwen3:4b` model in Ollama.
- Each chunk that is filtered in AnnualReportCleaning section, is processed using adjusted prompts to summarize and score the annual report of any company in any year within the research scope according to three dimensions: reliability, understandability, and relevance.
- Each chunk that is filtered in NewsCleaning section, is processed using adjusted prompts to summarize and score the annual report of any company in any year within the research scope according to three dimensions: credibility, strategic relevance, and depth.