# BRFSS Parsing Step

This adds **Step 2: Parse downloaded ZIPs into per-year CSV/Parquet**.

## Where files should be

- Downloaded ZIPs from Step 1 live in: `data/raw/brfss_zips/`
- Parsed outputs will be written to: `data/raw/brfss_year/`

## Install dependencies

```bash
pip install pyreadstat pandas pyarrow
```

> `pyarrow` is optional (only needed for Parquet). CSVs are always written.

## Run

Parse one or more specific years (that you have already downloaded in Step 1):

```bash
python scripts/parse_brfss_xpt.py 2019
python scripts/parse_brfss_xpt.py 1990 1991 1992
```

The script will:
- locate the correct ZIP in `data/raw/brfss_zips/` (works for both `CDBRFSYYXPT.zip` and `LLCPYYYYXPT.zip` names),
- extract the first `.xpt` inside,
- parse it with a robust reader (uses `pyreadstat`, falls back to `pandas.read_sas`),
- do minimal normalization (lowercase cols, add `year`, derive `state_fips`/`fips` when available),
- save `brfss_<year>.csv` and `brfss_<year>.parquet` to `data/raw/brfss_year/` (CSV always, Parquet if `pyarrow` installed).

Resume‑safe: if `brfss_<year>.csv` already exists, that year is skipped.

## Troubleshooting

- **ZIP not found**: Make sure you ran the download step and see the corresponding ZIP in `data/raw/brfss_zips/`.
- **xpt_not_in_zip**: The ZIP didn’t contain an `.xpt` file. Inspect the ZIP manually.
- **parse_error**: Ensure `pyreadstat` is installed; otherwise install it or rely on the built-in pandas fallback (`pandas.read_sas`).
- **write_error**: Check disk space/permissions.
