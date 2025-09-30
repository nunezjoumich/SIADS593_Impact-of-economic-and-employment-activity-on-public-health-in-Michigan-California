#!/usr/bin/env python3
"""
03_build_var_index.py
---------------------
Scan saved XPTs, extract variable metadata (names, labels, value label names),
and write per-year CSVs + a combined Parquet index.

Usage:
  python scripts/03_build_var_index.py
"""

from pathlib import Path
import json
import pandas as pd

# ---- config
RAW_XPT_DIR = Path("data/raw/brfss_year/raw_xpt")
OUT_DIR = Path("data/metadata/vars_by_year")
OUT_DIR.mkdir(parents=True, exist_ok=True)
COMBINED_PATH = Path("data/metadata/brfss_var_index.parquet")
SUMMARY_JSON  = Path("data/metadata/brfss_var_index_summary.json")

def list_xpts():
    return sorted(RAW_XPT_DIR.glob("brfss_*.xpt"))

def extract_year(p: Path) -> int:
    # expects brfss_<year>.xpt
    return int(p.stem.split("_")[1])

def read_xpt_meta(xpt_path: Path):
    """
    Prefer pyreadstat reading by filepath with metadataonly=True.
    Fall back to pandas.read_sas on full read (slower) if pyreadstat unavailable.
    """
    try:
        import pyreadstat
        # IMPORTANT: pass the path (string), not BytesIO
        _, meta = pyreadstat.read_xport(str(xpt_path), metadataonly=True)
        return {
            "column_names": meta.column_names,
            "column_labels": meta.column_labels,
            "variable_value_labels": meta.variable_value_labels or {}
        }
    except Exception:
        # Fallback: do a light full read via pandas just to get columns (no labels)
        try:
            df = pd.read_sas(str(xpt_path), format="xport", chunksize=None)
            cols = list(df.columns)
            return {
                "column_names": cols,
                "column_labels": cols,        # no labels available via pandas
                "variable_value_labels": {}
            }
        except Exception as e:
            raise RuntimeError(f"Failed to read metadata for {xpt_path.name}: {e}")

def main():
    rows = []
    per_year_counts = {}
    xpts = list_xpts()
    if not xpts:
        print("No XPTs found in", RAW_XPT_DIR.resolve())
        return

    for p in xpts:
        year = extract_year(p)
        try:
            meta = read_xpt_meta(p)
        except Exception as e:
            print(f"{year}: meta_error -> {e}")
            continue

        var_names = meta["column_names"]
        var_labels = meta["column_labels"]
        value_map  = meta["variable_value_labels"]

        df = pd.DataFrame({
            "year": year,
            "var_name": var_names,
            "var_label": var_labels
        })
        # value_map: var -> value label table name (string)
        df["value_label_table"] = df["var_name"].map(value_map).fillna("")

        # per-year CSV
        out_csv = OUT_DIR / f"vars_{year}.csv"
        df.to_csv(out_csv, index=False)
        per_year_counts[year] = len(df)

        rows.extend(df.to_dict(orient="records"))

    if rows:
        all_df = pd.DataFrame(rows)
        all_df["var_name_lc"]  = all_df["var_name"].str.lower()
        all_df["var_label_lc"] = all_df["var_label"].astype(str).str.lower()
        all_df.sort_values(["year", "var_name_lc"], inplace=True)

        COMBINED_PATH.parent.mkdir(parents=True, exist_ok=True)
        all_df.to_parquet(COMBINED_PATH, index=False)

        SUMMARY_JSON.parent.mkdir(parents=True, exist_ok=True)
        SUMMARY_JSON.write_text(json.dumps({
            "files_indexed": len(xpts),
            "per_year_var_counts": per_year_counts,
            "unique_vars": int(all_df["var_name_lc"].nunique())
        }, indent=2))

        print("Wrote:", COMBINED_PATH)
        print("Vars/year sample:", list(per_year_counts.items())[:5])
        print("Unique var names:", all_df["var_name_lc"].nunique())
    else:
        print("No rows created; check XPTs or pyreadstat install.")

if __name__ == "__main__":
    main()