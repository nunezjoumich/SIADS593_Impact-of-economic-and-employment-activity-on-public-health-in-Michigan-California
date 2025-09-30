#!/usr/bin/env python3
"""
02_parse_brfss_xpt.py
---------------------
Parse downloaded BRFSS ZIPs (CDC) that contain SAS XPT files, one year at a time.

- Reads each ZIP from data/raw/brfss_zips/
- Extracts the first .XPT inside (case-insensitive; tolerates trailing spaces)
- Saves the raw .XPT to data/raw/brfss_year/raw_xpt/brfss_<year>.xpt
- Parses to pandas (pyreadstat preferred; pandas.read_sas fallback)
- Minimal normalization: lowercase columns, add `year`, derive FIPS if _STATE/_CNTY exist
- Writes per-year CSV and Parquet to data/raw/brfss_year/
- Resume-safe: if a year's CSV already exists, it's skipped.

Usage:
  python scripts/02_parse_brfss_xpt.py 2010 2011 2012
"""

from __future__ import annotations

import io
import sys
import json
import zipfile
from pathlib import Path
from typing import Optional, List

import pandas as pd

ZIPS_DIR = Path("data/raw/brfss_zips")
OUT_DIR  = Path("data/raw/brfss_year")
RAW_XPT_DIR = OUT_DIR / "raw_xpt"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RAW_XPT_DIR.mkdir(parents=True, exist_ok=True)


# ---------- helpers ----------

def find_zip_for_year(year: int) -> Optional[Path]:
    """
    Find a ZIP in ZIPS_DIR for the given year, case-insensitively.
    Accepts names like CDBRFS00XPT.ZIP, LLCP2019XPT.zip, etc.
    Prefers files whose name contains both the year fragment and 'xpt'.
    """
    if not ZIPS_DIR.exists():
        return None

    y2, y4 = f"{year%100:02d}", str(year)

    # collect candidates (any .zip/.zipx, any case)
    cands: List[Path] = []
    for p in ZIPS_DIR.iterdir():
        if not p.is_file():
            continue
        name = p.name.lower()
        if not (name.endswith(".zip") or name.endswith(".zipx")):
            continue
        if (y2 in name or y4 in name):
            cands.append(p)

    if not cands:
        return None

    # prefer ones that also include 'xpt' in the filename
    with_xpt = [p for p in cands if "xpt" in p.name.lower()]
    if with_xpt:
        cands = with_xpt

    # deterministic: sort by name and pick the first
    cands.sort(key=lambda p: p.name.lower())
    return cands[0]


def extract_and_save_xpt(zip_path: Path, year: int) -> Optional[bytes]:
    """
    Return bytes of the first *.xpt file in the ZIP (case-insensitive),
    tolerating entries with trailing spaces (e.g., 'LLCP2019.XPT '),
    and also save a verbatim copy as data/raw/brfss_year/raw_xpt/brfss_<year>.xpt
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            # normalize names for matching (strip whitespace) but keep original for reading
            for original in names:
                cleaned = original.strip()
                if cleaned.lower().endswith(".xpt"):
                    raw_bytes = zf.read(original)  # use original entry name
                    # save raw XPT for provenance
                    out_path = RAW_XPT_DIR / f"brfss_{year}.xpt"
                    out_path.write_bytes(raw_bytes)
                    return raw_bytes
            return None
    except Exception as e:
        raise RuntimeError(f"Error reading ZIP {zip_path.name}: {e}")


def read_xpt_robust(xpt_bytes: bytes) -> pd.DataFrame:
    """
    Try pyreadstat (new/old signatures), else pandas.read_sas(format='xport').
    Returns a pandas DataFrame or raises RuntimeError.
    """
    try:
        import pyreadstat
        try:
            df, _ = pyreadstat.read_xport(
                io.BytesIO(xpt_bytes),
                apply_value_formats=False,
                formats_as_category=False,
            )
            return df
        except TypeError:
            # older pyreadstat signature without keyword args
            df, _ = pyreadstat.read_xport(io.BytesIO(xpt_bytes))
            return df
    except Exception:
        pass

    # pandas fallback
    try:
        df = pd.read_sas(io.BytesIO(xpt_bytes), format="xport")
        return df
    except Exception as e:
        raise RuntimeError(f"Failed to read XPT with pyreadstat and pandas: {e}")


def normalize(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Minimal normalization:
      - lowercase all column names
      - add `year`
      - if _STATE/_CNTY exist, create `state_fips` (2-digit) and `fips` (5-digit)
    """
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    df["year"] = year

    # _STATE and _CNTY variable names are uppercase in many BRFSS years
    if "_state" in df.columns:
        s = pd.to_numeric(df["_state"], errors="coerce").astype("Int64")
        df["state_fips"] = s.astype("string").str.zfill(2)

    if "_cnty" in df.columns:
        c = pd.to_numeric(df["_cnty"], errors="coerce").astype("Int64")
        df["county_fips3"] = c.astype("string").str.zfill(3)
        if "state_fips" in df.columns:
            df["fips"] = (df["state_fips"].fillna("") + df["county_fips3"].fillna(""))

    return df


def parse_year(year: int) -> dict:
    """
    Parse one year's ZIP -> XPT -> DataFrame -> CSV/Parquet.
    Returns a summary dict with status/rows/cols.
    """
    zip_path = find_zip_for_year(year)
    if not zip_path:
        return {"year": year, "status": "zip_not_found", "rows": None, "cols": None}

    csv_path = OUT_DIR / f"brfss_{year}.csv"
    pq_path  = OUT_DIR / f"brfss_{year}.parquet"
    if csv_path.exists():
        return {"year": year, "status": "skipped_existing", "rows": None, "cols": None}

    xpt_bytes = extract_and_save_xpt(zip_path, year)
    if not xpt_bytes:
        return {"year": year, "status": "xpt_not_in_zip", "rows": None, "cols": None}

    try:
        df = read_xpt_robust(xpt_bytes)
        df = normalize(df, year)
    except Exception as e:
        return {"year": year, "status": f"parse_error: {e}", "rows": None, "cols": None}

    try:
        df.to_csv(csv_path, index=False)
        try:
            df.to_parquet(pq_path, index=False)
        except Exception:
            pass
    except Exception as e:
        return {"year": year, "status": f"write_error: {e}", "rows": None, "cols": None}

    return {"year": year, "status": "ok", "rows": int(len(df)), "cols": int(df.shape[1])}


# ---------- CLI ----------

def main(argv: list[str]) -> None:
    if not ZIPS_DIR.exists():
        print("ZIPS_DIR not found:", ZIPS_DIR.resolve())
        sys.exit(1)

    if not argv:
        print("Usage: python scripts/02_parse_brfss_xpt.py YEAR [YEAR ...]")
        print("Example: python scripts/02_parse_brfss_xpt.py 2014 2015 2016 2017 2018 2019 2020")
        sys.exit(1)

    years: List[int] = []
    for a in argv:
        try:
            years.append(int(a))
        except ValueError:
            print(f"Skipping non-year argument: {a}")

    results = []
    for y in years:
        res = parse_year(y)
        print(res)
        results.append(res)

    # append to summary file
    summary_path = OUT_DIR / "_parse_summary.json"
    try:
        existing = []
        if summary_path.exists():
            existing = json.loads(summary_path.read_text())
        existing.extend(results)
        summary_path.write_text(json.dumps(existing, indent=2))
    except Exception:
        pass


if __name__ == "__main__":
    main(sys.argv[1:])


















# #!/usr/bin/env python3
# """
# 02_parse_brfss_xpt.py
# ---------------------
# Parse downloaded BRFSS ZIPs (CDC) that contain SAS XPT files, one year at a time.

# - Reads each ZIP from data/raw/brfss_zips/
# - Extracts the .XPT inside (robust case-insensitive / whitespace-tolerant matching)
# - Parses to pandas (pyreadstat preferred; pandas.read_sas fallback)
# - Minimal normalization: lowercase columns, add `year`, derive FIPS if _STATE/_CNTY exist
# - Writes per-year CSV and Parquet to data/raw/brfss_year/
# - Resume-safe: if a year's CSV already exists, it's skipped.

# Usage:
#   python scripts/02_parse_brfss_xpt.py 2010 2011 2012
# """

# from __future__ import annotations

# import io
# import sys
# import json
# import zipfile
# import unicodedata
# import re
# from pathlib import Path
# from typing import Optional, List

# import pandas as pd

# ZIPS_DIR = Path("data/raw/brfss_zips")
# OUT_DIR  = Path("data/raw/brfss_year")
# OUT_DIR.mkdir(parents=True, exist_ok=True)


# # ---------- helpers ----------

# def find_zip_for_year(year: int) -> Optional[Path]:
#     """
#     Find a ZIP in ZIPS_DIR for the given year, case-insensitively.
#     Accepts names like CDBRFS00XPT.ZIP, LLCP2019XPT.zip, etc.
#     Prefers files whose name contains both the year fragment and 'xpt'.
#     """
#     if not ZIPS_DIR.exists():
#         return None

#     y2, y4 = f"{year%100:02d}", str(year)

#     # collect candidates (any .zip/.zipx, any case)
#     cands: List[Path] = []
#     for p in ZIPS_DIR.iterdir():
#         if not p.is_file():
#             continue
#         name = p.name.lower()
#         if not (name.endswith(".zip") or name.endswith(".zipx")):
#             continue
#         if (y2 in name or y4 in name):
#             cands.append(p)

#     if not cands:
#         return None

#     # prefer ones that also include 'xpt' in the filename
#     with_xpt = [p for p in cands if "xpt" in p.name.lower()]
#     if with_xpt:
#         cands = with_xpt

#     # deterministic: sort by name and pick the first
#     cands.sort(key=lambda p: p.name.lower())
#     return cands[0]


# def extract_xpt_from_zip(zip_path: Path) -> Optional[bytes]:
#     """
#     Return bytes of the XPT payload from a BRFSS zip.

#     Robust matching:
#       - case-insensitive
#       - trims and removes *all* whitespace (incl. NBSP) for matching
#       - accepts names with trailing spaces or odd punctuation
#       - if not found, tries the largest few files and sniffs XPORT header
#     """
#     def clean_key(s: str) -> str:
#         s = unicodedata.normalize("NFKC", s)
#         s = s.lower()
#         s = re.sub(r"\s+", "", s)
#         return s

#     try:
#         with zipfile.ZipFile(zip_path, "r") as zf:
#             names = zf.namelist()
#             if not names:
#                 return None

#             # map cleaned -> original
#             cleaned_to_orig = {clean_key(n): n for n in names}

#             # 1) canonical '*.xpt'
#             xpt_like = [orig for ck, orig in cleaned_to_orig.items() if ck.endswith(".xpt")]
#             if xpt_like:
#                 with zf.open(xpt_like[0]) as f:
#                     return f.read()

#             # 2) anything containing 'xpt'
#             contains_xpt = [orig for ck, orig in cleaned_to_orig.items() if "xpt" in ck]
#             if contains_xpt:
#                 with zf.open(contains_xpt[0]) as f:
#                     return f.read()

#             # 3) sniff a few largest files for SAS XPORT header
#             infos = sorted(zf.infolist(), key=lambda i: i.file_size, reverse=True)
#             for info in infos[:5]:
#                 try:
#                     with zf.open(info) as f:
#                         blob = f.read()
#                     if b"LIBRARY HEADER RECORD" in blob[:4096]:
#                         return blob
#                 except Exception:
#                     continue

#             return None
#     except Exception as e:
#         raise RuntimeError(f"Error reading ZIP {zip_path.name}: {e}")


# def read_xpt_robust(xpt_bytes: bytes) -> pd.DataFrame:
#     """Try pyreadstat (new/old signatures), else pandas.read_sas(format='xport')."""
#     try:
#         import pyreadstat
#         try:
#             df, _ = pyreadstat.read_xport(
#                 io.BytesIO(xpt_bytes),
#                 apply_value_formats=False,
#                 formats_as_category=False,
#             )
#             return df
#         except TypeError:
#             df, _ = pyreadstat.read_xport(io.BytesIO(xpt_bytes))
#             return df
#     except Exception:
#         pass

#     # pandas fallback
#     df = pd.read_sas(io.BytesIO(xpt_bytes), format="xport")
#     return df


# def normalize(df: pd.DataFrame, year: int) -> pd.DataFrame:
#     """
#     Minimal normalization:
#       - lowercase all column names
#       - add `year`
#       - if _STATE/_CNTY exist, create `state_fips` (2-digit) and `fips` (5-digit)
#     """
#     df = df.copy()
#     df.columns = [c.strip().lower() for c in df.columns]
#     df["year"] = year

#     if "_state" in df.columns:
#         s = pd.to_numeric(df["_state"], errors="coerce").astype("Int64")
#         df["state_fips"] = s.astype("string").str.zfill(2)

#     if "_cnty" in df.columns:
#         c = pd.to_numeric(df["_cnty"], errors="coerce").astype("Int64")
#         df["county_fips3"] = c.astype("string").str.zfill(3)
#         if "state_fips" in df.columns:
#             df["fips"] = (df["state_fips"].fillna("") + df["county_fips3"].fillna(""))

#     return df


# def parse_year(year: int) -> dict:
#     """Parse one year's ZIP -> XPT -> DataFrame -> CSV/Parquet."""
#     zip_path = find_zip_for_year(year)
#     if not zip_path:
#         return {"year": year, "status": "zip_not_found", "rows": None, "cols": None}

#     csv_path = OUT_DIR / f"brfss_{year}.csv"
#     pq_path  = OUT_DIR / f"brfss_{year}.parquet"
#     if csv_path.exists():
#         return {"year": year, "status": "skipped_existing", "rows": None, "cols": None}

#     xpt_bytes = extract_xpt_from_zip(zip_path)
#     if not xpt_bytes:
#         return {"year": year, "status": "xpt_not_in_zip", "rows": None, "cols": None}

#     try:
#         df = read_xpt_robust(xpt_bytes)
#         df = normalize(df, year)
#     except Exception as e:
#         return {"year": year, "status": f"parse_error: {e}", "rows": None, "cols": None}

#     try:
#         df.to_csv(csv_path, index=False)
#         try:
#             df.to_parquet(pq_path, index=False)
#         except Exception:
#             pass
#     except Exception as e:
#         return {"year": year, "status": f"write_error: {e}", "rows": None, "cols": None}

#     return {"year": year, "status": "ok", "rows": int(len(df)), "cols": int(df.shape[1])}


# # ---------- CLI ----------

# def main(argv: list[str]) -> None:
#     if not ZIPS_DIR.exists():
#         print("ZIPS_DIR not found:", ZIPS_DIR.resolve())
#         sys.exit(1)

#     if not argv:
#         print("Usage: python scripts/02_parse_brfss_xpt.py YEAR [YEAR ...]")
#         sys.exit(1)

#     years: List[int] = []
#     for a in argv:
#         try:
#             years.append(int(a))
#         except ValueError:
#             print(f"Skipping non-year argument: {a}")

#     results = []
#     for y in years:
#         res = parse_year(y)
#         print(res)
#         results.append(res)

#     # append to summary file
#     summary_path = OUT_DIR / "_parse_summary.json"
#     try:
#         existing = []
#         if summary_path.exists():
#             existing = json.loads(summary_path.read_text())
#         existing.extend(results)
#         summary_path.write_text(json.dumps(existing, indent=2))
#     except Exception:
#         pass


# if __name__ == "__main__":
#     main(sys.argv[1:])


















# # #!/usr/bin/env python3
# # """
# # 02_parse_brfss_xpt.py
# # ---------------------
# # Parse downloaded BRFSS ZIPs (CDC) that contain SAS XPT files, one year at a time.

# # - Reads each ZIP from data/raw/brfss_zips/
# # - Extracts the first .XPT inside (case-insensitive)
# # - Parses to pandas (pyreadstat preferred; pandas.read_sas fallback)
# # - Minimal normalization: lowercase columns, add `year`, derive FIPS if _STATE/_CNTY exist
# # - Writes per-year CSV and Parquet to data/raw/brfss_year/
# # - Resume-safe: if a year's CSV already exists, it's skipped.

# # Usage:
# #   python scripts/02_parse_brfss_xpt.py 2010 2011 2012
# # """

# # from __future__ import annotations

# # import io
# # import sys
# # import json
# # import zipfile
# # from pathlib import Path
# # from typing import Optional, List

# # import pandas as pd

# # ZIPS_DIR = Path("data/raw/brfss_zips")
# # OUT_DIR  = Path("data/raw/brfss_year")
# # OUT_DIR.mkdir(parents=True, exist_ok=True)


# # # ---------- helpers ----------

# # def find_zip_for_year(year: int) -> Optional[Path]:
# #     """
# #     Find a ZIP in ZIPS_DIR for the given year, case-insensitively.
# #     Accepts names like CDBRFS00XPT.ZIP, LLCP2019XPT.zip, etc.
# #     Prefers files whose name contains both the year fragment and 'xpt'.
# #     """
# #     if not ZIPS_DIR.exists():
# #         return None

# #     y2, y4 = f"{year%100:02d}", str(year)

# #     # collect candidates (any .zip/.zipx, any case)
# #     cands: List[Path] = []
# #     for p in ZIPS_DIR.iterdir():
# #         if not p.is_file():
# #             continue
# #         name = p.name.lower()
# #         if not (name.endswith(".zip") or name.endswith(".zipx")):
# #             continue
# #         if (y2 in name or y4 in name):
# #             cands.append(p)

# #     if not cands:
# #         return None

# #     # prefer ones that also include 'xpt' in the filename
# #     with_xpt = [p for p in cands if "xpt" in p.name.lower()]
# #     if with_xpt:
# #         cands = with_xpt

# #     # deterministic: sort by name and pick the first
# #     cands.sort(key=lambda p: p.name.lower())
# #     return cands[0]


# # def extract_xpt_from_zip(zip_path: Path) -> Optional[bytes]:
# #     """Return bytes of the first *.xpt file in the ZIP (case-insensitive), or None if not found."""
# #     try:
# #         with zipfile.ZipFile(zip_path, "r") as zf:
# #             names = zf.namelist()
# #             # find any entry that ends with .xpt (case-insensitive)
# #             xpt_candidates = [n for n in names if n.lower().endswith(".xpt")]
# #             if not xpt_candidates:
# #                 return None
# #             with zf.open(xpt_candidates[0]) as f:
# #                 return f.read()
# #     except Exception as e:
# #         raise RuntimeError(f"Error reading ZIP {zip_path.name}: {e}")


# # def read_xpt_robust(xpt_bytes: bytes) -> pd.DataFrame:
# #     """
# #     Try pyreadstat (new/old signatures), else pandas.read_sas(format='xport').
# #     Returns a pandas DataFrame or raises RuntimeError.
# #     """
# #     # pyreadstat path
# #     try:
# #         import pyreadstat
# #         try:
# #             df, _ = pyreadstat.read_xport(
# #                 io.BytesIO(xpt_bytes),
# #                 apply_value_formats=False,
# #                 formats_as_category=False,
# #             )
# #             return df
# #         except TypeError:
# #             # older pyreadstat signature without keyword args
# #             df, _ = pyreadstat.read_xport(io.BytesIO(xpt_bytes))
# #             return df
# #     except Exception:
# #         pass

# #     # pandas fallback
# #     try:
# #         df = pd.read_sas(io.BytesIO(xpt_bytes), format="xport")
# #         return df
# #     except Exception as e:
# #         raise RuntimeError(f"Failed to read XPT with pyreadstat and pandas: {e}")


# # def normalize(df: pd.DataFrame, year: int) -> pd.DataFrame:
# #     """
# #     Minimal normalization:
# #       - lowercase all column names
# #       - add `year`
# #       - if _STATE/_CNTY exist, create `state_fips` (2-digit) and `fips` (5-digit)
# #     """
# #     df = df.copy()
# #     df.columns = [c.strip().lower() for c in df.columns]
# #     df["year"] = year

# #     if "_state" in df.columns:
# #         s = pd.to_numeric(df["_state"], errors="coerce").astype("Int64")
# #         df["state_fips"] = s.astype("string").str.zfill(2)

# #     if "_cnty" in df.columns:
# #         c = pd.to_numeric(df["_cnty"], errors="coerce").astype("Int64")
# #         df["county_fips3"] = c.astype("string").str.zfill(3)
# #         if "state_fips" in df.columns:
# #             df["fips"] = (df["state_fips"].fillna("") + df["county_fips3"].fillna(""))

# #     return df


# # def parse_year(year: int) -> dict:
# #     """
# #     Parse one year's ZIP -> XPT -> DataFrame -> CSV/Parquet.
# #     Returns a summary dict with status/rows/cols.
# #     """
# #     zip_path = find_zip_for_year(year)
# #     if not zip_path:
# #         return {"year": year, "status": "zip_not_found", "rows": None, "cols": None}

# #     csv_path = OUT_DIR / f"brfss_{year}.csv"
# #     pq_path  = OUT_DIR / f"brfss_{year}.parquet"
# #     if csv_path.exists():
# #         return {"year": year, "status": "skipped_existing", "rows": None, "cols": None}

# #     xpt_bytes = extract_xpt_from_zip(zip_path)
# #     if not xpt_bytes:
# #         return {"year": year, "status": "xpt_not_in_zip", "rows": None, "cols": None}

# #     try:
# #         df = read_xpt_robust(xpt_bytes)
# #         df = normalize(df, year)
# #     except Exception as e:
# #         return {"year": year, "status": f"parse_error: {e}", "rows": None, "cols": None}

# #     try:
# #         df.to_csv(csv_path, index=False)
# #         try:
# #             df.to_parquet(pq_path, index=False)
# #         except Exception:
# #             pass
# #     except Exception as e:
# #         return {"year": year, "status": f"write_error: {e}", "rows": None, "cols": None}

# #     return {"year": year, "status": "ok", "rows": int(len(df)), "cols": int(df.shape[1])}


# # # ---------- CLI ----------

# # def main(argv: list[str]) -> None:
# #     if not ZIPS_DIR.exists():
# #         print("ZIPS_DIR not found:", ZIPS_DIR.resolve())
# #         sys.exit(1)

# #     if not argv:
# #         print("Usage: python scripts/02_parse_brfss_xpt.py YEAR [YEAR ...]")
# #         print("Example: python scripts/02_parse_brfss_xpt.py 2010 2011 2012")
# #         sys.exit(1)

# #     years: List[int] = []
# #     for a in argv:
# #         try:
# #             years.append(int(a))
# #         except ValueError:
# #             print(f"Skipping non-year argument: {a}")

# #     results = []
# #     for y in years:
# #         res = parse_year(y)
# #         print(res)
# #         results.append(res)

# #     # append to summary file
# #     summary_path = OUT_DIR / "_parse_summary.json"
# #     try:
# #         existing = []
# #         if summary_path.exists():
# #             existing = json.loads(summary_path.read_text())
# #         existing.extend(results)
# #         summary_path.write_text(json.dumps(existing, indent=2))
# #     except Exception:
# #         pass


# # if __name__ == "__main__":
# #     main(sys.argv[1:])