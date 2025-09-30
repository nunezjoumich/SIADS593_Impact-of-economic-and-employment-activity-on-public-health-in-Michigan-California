#!/usr/bin/env python3
"""
02a_extract_xpts.py
-------------------
Extract and save BRFSS SAS XPT files from downloaded CDC ZIPs.

- Scans: data/raw/brfss_zips/
- Extracts the first .XPT in each ZIP (case-insensitive; trims trailing spaces)
- Saves to: data/raw/brfss_year/raw_xpt/brfss_<YEAR>.xpt
- Resume-safe: skips if the XPT for that year already exists (unless --force)

Usage:
  # Extract for specific years (infer ZIP by filename)
  python scripts/02a_extract_xpts.py 2014 2015 2016

  # Process all ZIPs found (infers year from filename)
  python scripts/02a_extract_xpts.py --all

  # Force overwrite existing .xpt
  python scripts/02a_extract_xpts.py 2019 --force
"""

from __future__ import annotations
import argparse
import re
import zipfile
from pathlib import Path
from typing import Optional, List

ZIPS_DIR = Path("data/raw/brfss_zips")
OUT_DIR  = Path("data/raw/brfss_year/raw_xpt")
OUT_DIR.mkdir(parents=True, exist_ok=True)

YEAR_4 = re.compile(r"(19|20)\d{2}")
YEAR_2 = re.compile(r"(?<!\d)\d{2}(?!\d)")  # two digits not attached to others

def infer_year_from_name(name: str) -> Optional[int]:
    """Infer a 4-digit year from a filename; fall back to 2-digit (00→2000..29→2029; 90→1990..99→1999)."""
    name_low = name.lower()
    m4 = YEAR_4.search(name_low)
    if m4:
        return int(m4.group(0))
    m2 = YEAR_2.search(name_low)
    if m2:
        yy = int(m2.group(0))
        # Heuristic: 90–99 => 1990s; 00–29 => 2000s/2010s/2020s
        return 1900 + yy if yy >= 90 else 2000 + yy
    return None

def find_zip_for_year(year: int) -> Optional[Path]:
    """Find a ZIP in ZIPS_DIR matching the given year (handles .zip/.ZIP/.zipx; case-insensitive)."""
    if not ZIPS_DIR.exists():
        return None
    y2, y4 = f"{year%100:02d}", str(year)
    cands: List[Path] = []
    for p in ZIPS_DIR.iterdir():
        if not p.is_file():
            continue
        nm = p.name.lower()
        if (nm.endswith(".zip") or nm.endswith(".zipx")) and (y4 in nm or y2 in nm):
            cands.append(p)
    if not cands:
        return None
    # prefer those with "xpt" in the filename
    with_xpt = [p for p in cands if "xpt" in p.name.lower()]
    cands = with_xpt or cands
    cands.sort(key=lambda x: x.name.lower())
    return cands[0]

def extract_xpt_bytes(zip_path: Path) -> Optional[bytes]:
    """
    Return bytes of the first .xpt in the ZIP.
    Handles entries like 'LLCP2019.XPT ' (trailing spaces).
    """
    with zipfile.ZipFile(zip_path, "r") as zf:
        for original in zf.namelist():
            cleaned = original.strip()
            if cleaned.lower().endswith(".xpt"):
                return zf.read(original)  # use original entry as stored
    return None

def save_xpt_for_year(year: int, force: bool = False) -> dict:
    """
    Extracts and saves brfss_<year>.xpt if present. Returns a status dict.
    """
    out_path = OUT_DIR / f"brfss_{year}.xpt"
    if out_path.exists() and not force:
        return {"year": year, "status": "skipped_existing", "path": str(out_path)}

    zip_path = find_zip_for_year(year)
    if not zip_path:
        return {"year": year, "status": "zip_not_found", "path": None}

    xpt_bytes = extract_xpt_bytes(zip_path)
    if not xpt_bytes:
        return {"year": year, "status": "xpt_not_in_zip", "path": None}

    out_path.write_bytes(xpt_bytes)
    return {"year": year, "status": "ok", "path": str(out_path), "size_bytes": out_path.stat().st_size}

def process_all(force: bool = False) -> list[dict]:
    """
    Iterate every ZIP in ZIPS_DIR, infer the year from the filename, and save brfss_<year>.xpt.
    """
    results = []
    if not ZIPS_DIR.exists():
        return [{"year": None, "status": "zips_dir_missing", "path": str(ZIPS_DIR)}]

    zips = [p for p in ZIPS_DIR.iterdir()
            if p.is_file() and p.suffix.lower() in (".zip", ".zipx")]
    zips.sort(key=lambda x: x.name.lower())

    for z in zips:
        year = infer_year_from_name(z.name)
        if year is None:
            results.append({"year": None, "status": "year_infer_failed", "zip": z.name})
            continue

        out_path = OUT_DIR / f"brfss_{year}.xpt"
        if out_path.exists() and not force:
            results.append({"year": year, "status": "skipped_existing", "path": str(out_path)})
            continue

        xpt_bytes = extract_xpt_bytes(z)
        if not xpt_bytes:
            results.append({"year": year, "status": "xpt_not_in_zip", "zip": z.name})
            continue

        out_path.write_bytes(xpt_bytes)
        results.append({"year": year, "status": "ok", "path": str(out_path), "size_bytes": out_path.stat().st_size})

    return results

def main():
    ap = argparse.ArgumentParser(description="Extract and save BRFSS XPT files from downloaded ZIPs.")
    ap.add_argument("years", nargs="*", help="Years to process (e.g., 2014 2015 2016).")
    ap.add_argument("--all", action="store_true", help="Process all ZIPs in data/raw/brfss_zips.")
    ap.add_argument("--force", action="store_true", help="Overwrite existing .xpt files.")
    args = ap.parse_args()

    if not ZIPS_DIR.exists():
        print("ZIPS_DIR not found:", ZIPS_DIR.resolve())
        raise SystemExit(1)

    results = []
    if args.all:
        results = process_all(force=args.force)
    else:
        yrs = []
        for y in args.years:
            try:
                yrs.append(int(y))
            except ValueError:
                print(f"Skipping non-year argument: {y}")
        if not yrs:
            print("Provide years or use --all")
            raise SystemExit(2)
        for y in yrs:
            res = save_xpt_for_year(y, force=args.force)
            results.append(res)

    for r in results:
        print(r)

if __name__ == "__main__":
    main()