# 01 BRFSS Downloader (1990–2023)

This project downloads the **CDC BRFSS annual survey ZIP archives** for years 1990–2023. Each ZIP contains a SAS transport (`.XPT`) file, which can later be parsed into CSV/Parquet for analysis. The notebook is designed to download decade blocks safely and resume if files already exist.

## Quickstart

1. Create/activate a virtualenv (optional but recommended)
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate   # Windows: .venv\Scripts\activate
   python -m pip install --upgrade pip
   ```

2. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```

3. Open the notebook
   ```bash
   jupyter notebook BRFSS_downloader.ipynb
   ```

4. Run the **smoke test** cells (e.g., 1990 and 1991) to verify downloads.

5. Run decade blocks (uncomment each block in order):
   - 1990–1999
   - 2000–2009
   - 2010–2019
   - 2020–2023

## Output layout

- Raw ZIPs: `data/raw/brfss_zips/`
- Files are named as published by CDC, e.g.:
  - `CDBRFS90XPT.zip`
  - `LLCP2019XPT.zip`

## Notes

- At this stage, the notebook **only downloads ZIP files**.  
- Parsing `.XPT` files into CSV/Parquet will be added in a later step (using `pyreadstat`).  
- If a download fails, rerun the corresponding block; existing files will be skipped automatically.
