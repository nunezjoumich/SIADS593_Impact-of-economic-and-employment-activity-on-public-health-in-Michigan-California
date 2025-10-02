# 🧪 BRFSS × LAUS — Michigan vs. California (1990–2010)

**SIADS 593: Milestone I — Impact of Economic & Employment Activity on Public Health**

This repository contains a reproducible pipeline and exploratory analysis investigating how **economic conditions** and **public health outcomes** interact across **Michigan and California** from 1990 to 2010.  

We combine:
- **CDC Behavioral Risk Factor Surveillance System (BRFSS)** survey data  
- **Bureau of Labor Statistics (BLS) Local Area Unemployment Statistics (LAUS)**  

to explore whether **economic shocks and unemployment trends** align spatially and temporally with **population health patterns** at both **state** and **county** levels.

---

## 🧭 Project Overview

- **Goal:**  
  Examine how labor market conditions (e.g., unemployment rates) and public health outcomes (e.g., insurance coverage, obesity rates) co-evolve geographically and over time in **Michigan vs. California**.

- **Why Michigan & California:**  
  Michigan experienced major industrial and economic shocks during this period. California serves as a contrasting case with different economic structures, population patterns, and health policy environments.

- **Why 1990–2010:**  
  - BRFSS underwent a major redesign in 2011 (cell phone sampling, weighting), breaking comparability with earlier data.  
  - County-level BRFSS data before 2010 has suppression of detailed FIPS identifiers, so we focus on **state-year aggregates** with some county-level analyses where feasible.

- **Scope Note:**  
  While our methods could extend post-2011, the BRFSS redesign and LAUS data format changes make cross-period comparisons non-trivial. We focus on pre-2011 Michigan vs. California as a clean baseline.

---

## 📊 Datasets

| Dataset | Description | Years | Format | Access |
|---------|-------------|-------|--------|--------|
| **BRFSS** | Behavioral Risk Factor Surveillance System survey | 1990–2010 | ZIP → XPT → CSV/Parquet | [CDC](https://www.cdc.gov/brfss/annual_data/) |
| **LAUS** | Local Area Unemployment Statistics | 1990–2010 | CSV, XLSX, JSON | [BLS LAU Portal](https://www.bls.gov/lau/data.htm) & [Direct Download](https://download.bls.gov/pub/time.series/la/) |
| **FIPS / Geocodes** | County crosswalks & identifiers | 2017 vintage | XLSX | [Census Geocodes](https://www2.census.gov/programs-surveys/popest/geographies/2017/all-geocodes-v2017.xlsx) |
| **(Historical)** Datalumos | Alternative LAUS source with header previews | 1990–2010 | CSV | [Datalumos Repository](https://www.datalumos.org/datalumos/project/227042/version/V1/view) *(downloads currently unreliable)* |

Key BRFSS variables: demographics, insurance coverage, chronic conditions, behavioral health factors.  
Key LAUS variables: unemployment rates, labor force size, employment ratios.

---

## 🛠️ Pipeline Overview

1. **Download BRFSS ZIPs** containing `.XPT` SAS transport files  
2. **Parse XPT → CSV/Parquet** for analysis  
3. **Aggregate state & county data** for Michigan and California  
4. **Download & prepare LAUS** unemployment data  
5. **Join BRFSS + LAUS datasets** on geographic and temporal keys  
6. **Run exploratory analyses & visualizations**

---

## 📂 Project Structure

```
project_root/
│
├── 01_BRFSS_Downloader.ipynb       # Step 1: Download raw BRFSS ZIPs
├── 01_requirements.txt
│
├── scripts/
│   ├── 02_parse_brfss_xpt.py
│   ├── 02a_extract_xpts.py         # Optional helper
│   ├── 05_download_laus.py         # Planned LAUS download helper (WIP)
│   ├── 06_merge_brfss_laus.py      # Planned merge script (WIP)
│   └── deprecated/                 # Old scripts (03_, 04_) not in pipeline
│
├── data/
│   ├── raw/
│   │   ├── brfss_zips/
│   │   └── brfss_year/
│   ├── silver/
│   │   ├── brfss_state_year.csv
│   │   ├── laus_state_year.csv
│   │   └── state_year_merged.csv
│   └── metadata/
│       └── canonical_map.yaml
│
└── notebooks/
    ├── 03_analysis.ipynb
    └── viz_experiments.ipynb
```

---

## ⚡ Quickstart

### 1️⃣ Environment

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r 01_requirements.txt
pip install pandas pyreadstat pyarrow
```

### 2️⃣ Download BRFSS Data

```bash
jupyter notebook 01_BRFSS_Downloader.ipynb
```

### 3️⃣ Parse XPT Files

```bash
python scripts/02_parse_brfss_xpt.py 1990 1991 ... 2010
```

---

## 🧠 Planned Analyses

* Descriptive statistics for unemployment & health variables
* Correlation analyses
* Time series alignment of economic downturns vs. health trends
* Spatial choropleths, time-lapse, and difference maps
* Regression models to test associations

---

## 📚 Sources

* [CDC BRFSS Annual Data](https://www.cdc.gov/brfss/annual_data/)
* [BLS LAUS Portal](https://www.bls.gov/lau/data.htm)
* [BLS LAUS Direct Downloads](https://download.bls.gov/pub/time.series/la/)
* [Census Geocodes (FIPS Crosswalk)](https://www2.census.gov/programs-surveys/popest/geographies/2017/all-geocodes-v2017.xlsx)
* [Harvard Dataverse Metadata](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi%3A10.7910%2FDVN%2FZ4YTA6)
* [Datalumos Repository (historical LAUS)](https://www.datalumos.org/datalumos/project/227042/version/V1/view)

---

## 👥 Team

| Name                   | Role                                 |
| ---------------------- | ------------------------------------ |
| **Sophia Boettcher**   | Data cleaning & visualization        |
| **Josue Nunez**        | Analytical modeling                  |
| **Benjamin Strelzoff** | Pipeline refactoring & documentation |

---

## 📝 License

* **Code:** MIT License
* **Data:** Public CDC, BLS, and Census data — follow respective usage terms
