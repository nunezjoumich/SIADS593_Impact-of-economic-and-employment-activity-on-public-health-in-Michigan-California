#!/usr/bin/env python3
"""
04_suggest_canonical_map.py
---------------------------
Build initial canonical variable mapping by matching across years using names
and labels (exact + fuzzy). Produces a YAML mapping and a coverage CSV.

Usage:
  python scripts/04_suggest_canonical_map.py
"""

from pathlib import Path
import pandas as pd
from rapidfuzz import fuzz, process
import yaml

VAR_INDEX = Path("data/metadata/brfss_var_index.parquet")
OUT_YAML  = Path("data/metadata/canonical_map_suggested.yaml")
OUT_CSV   = Path("data/metadata/canonical_coverage.csv")

# ---- seed canonical concepts you care about (extend this list!)
SEED_CANONICAL = {
    # ---------------- Demographics & Design ----------------
    "state_fips": {
        "aliases_exact": ["_STATE", "STATE", "state_fips"],
        "label_terms":  ["state", "fips"]
    },
    "county_fips3": {
        "aliases_exact": ["_CNTY", "CNTY", "county_fips3"],
        "label_terms":  ["county", "fips"]
    },
    "psu": {
        "aliases_exact": ["_PSU"],
        "label_terms":  ["psu", "cluster", "primary sampling unit"]
    },
    "strata": {
        "aliases_exact": ["_STSTR"],
        "label_terms":  ["strata", "stratum", "design"]
    },
    "weight_final": {
        "aliases_exact": ["_FINALWT", "_LLCPWT", "LLCPWT", "FINALWT"],
        "label_terms":  ["weight", "final", "survey weight"]
    },
    "sex": {
        "aliases_exact": ["SEX", "SEXVAR", "SEX1"],
        "label_terms":  ["sex", "gender"]
    },
    "age_5yr_band": {
        "aliases_exact": ["_AGEG5YR", "AGEG5YR"],
        "label_terms":  ["age", "5-year", "age group", "age band"]
    },
    "age_65plus": {
        "aliases_exact": ["_AGE65YR", "AGE65YR"],
        "label_terms":  ["age", "65", "elderly", "older adult"]
    },
    "race_ethnicity": {
        "aliases_exact": ["_RACE", "_RACEGR3", "RACE2", "RACEGR3", "RACE"],
        "label_terms":  ["race", "ethnicity", "hispanic", "origin"]
    },
    "marital_status": {
        "aliases_exact": ["MARITAL", "MRACE"],
        "label_terms":  ["marital", "married", "divorced", "widowed", "separated", "partner"]
    },
    "education": {
        "aliases_exact": ["EDUCA", "EDUCA2"],
        "label_terms":  ["education", "school", "grade", "hs", "college"]
    },
    "income_category": {
        "aliases_exact": ["INCOME2", "INCOME", "INCOMG", "X_INCOMG"],
        "label_terms":  ["income", "household income", "income category"]
    },

    # ---------------- General Health & Access ----------------
    "general_health": {
        "aliases_exact": ["GENHLTH", "GENHLTH2"],
        "label_terms":  ["general health", "overall health", "self-rated health"]
    },
    "health_plan": {
        "aliases_exact": ["HLTHPLN1", "HLTHPLN2", "HLTHPLN3"],
        "label_terms":  ["health plan", "insurance", "coverage"]
    },
    "personal_doctor": {
        "aliases_exact": ["PERSDOC2", "PERSDOC3", "PERSDOC4"],
        "label_terms":  ["personal doctor", "provider", "primary care"]
    },
    "mental_health_days": {
        "aliases_exact": ["MENTHLTH"],
        "label_terms":  ["mental health", "not good", "days"]
    },
    "physical_health_days": {
        "aliases_exact": ["PHYSHLTH"],
        "label_terms":  ["physical health", "not good", "days"]
    },
    "poor_health_days": {
        "aliases_exact": ["POORHLTH", "POORHLTH2"],
        "label_terms":  ["poor health", "days poor health"]
    },

    # ---------------- Health Behaviors ----------------
    "smoked_100_cigs": {
        "aliases_exact": ["SMOKE100", "SMOK100"],
        "label_terms":  ["smoke 100", "ever smoked", "100 cigarettes"]
    },
    "current_smoking": {
        "aliases_exact": ["SMOKDAY2", "SMOKDAY3", "SMOKER3", "SMOKER"],
        "label_terms":  ["current smoker", "smoking frequency", "days"]
    },
    "quit_attempt_past_year": {
        "aliases_exact": ["STOPSMK2", "STOPSMK3"],
        "label_terms":  ["quit smoking", "stopped smoking", "attempt"]
    },
    "alcohol_days": {
        "aliases_exact": ["ALCDAY5", "ALCDAY4"],
        "label_terms":  ["alcohol", "drinking days", "per month"]
    },
    "alcohol_avg_drinks": {
        "aliases_exact": ["AVEDRNK2", "AVEDRNK3"],
        "label_terms":  ["average drinks", "per day", "alcohol"]
    },
    "binge_drinking": {
        "aliases_exact": ["DRNK3GE5", "BINGE5", "BINGE_DRINK"],
        "label_terms":  ["binge", "5 drinks", "heavy episodic"]
    },
    "physical_activity_any": {
        "aliases_exact": ["EXERANY2", "EXERANY3", "PA1", "PAQ050"],
        "label_terms":  ["exercise", "physical activity", "any activity"]
    },
    "fruit_consumption": {
        "aliases_exact": ["FRUITJU1", "FRUIT1", "FRUIT2"],
        "label_terms":  ["fruit", "consumption", "servings"]
    },
    "vegetable_consumption": {
        "aliases_exact": ["VEGETAB1", "VEGETAB2", "VEGETAB"],
        "label_terms":  ["vegetable", "consumption", "servings"]
    },
    "sugar_sweetened_beverage": {
        "aliases_exact": ["SSBFRUT1", "SSBSODA1", "SSB_SUGAR"],
        "label_terms":  ["sugar-sweetened beverage", "soda", "sweet drinks"]
    },
    "seatbelt_use": {
        "aliases_exact": ["SEATBELT", "SEATBELT2"],
        "label_terms":  ["seat belt", "safety belt", "always"]
    },
    "drinking_and_driving": {
        "aliases_exact": ["DRNKDRV", "DRNKDRV2"],
        "label_terms":  ["drinking and driving", "drive after drinking"]
    },

    # ---------------- Chronic Conditions ----------------
    "diabetes": {
        "aliases_exact": ["DIABETE3", "DIABETE4", "DIABAGE2"],
        "label_terms":  ["diabetes", "told diabetes"]
    },
    "asthma": {
        "aliases_exact": ["ASTHMA3", "ASTHMA4", "ASTHNOW", "ASTHMAEV"],
        "label_terms":  ["asthma", "ever told asthma", "current asthma"]
    },
    "copd": {
        "aliases_exact": ["COPD", "COPDEV", "CHCCOPD"],
        "label_terms":  ["copd", "chronic lung disease", "emphysema"]
    },
    "hypertension": {
        "aliases_exact": ["BPHIGH4", "BPHIGH6", "BPHIGH"],
        "label_terms":  ["high blood pressure", "hypertension"]
    },
    "high_cholesterol": {
        "aliases_exact": ["TOLDHI2", "CHOLCHK", "HIGHCHOL"],
        "label_terms":  ["high cholesterol", "told high chol"]
    },
    "heart_attack": {
        "aliases_exact": ["CVDINFR4", "CVDINFR5", "MI"],
        "label_terms":  ["heart attack", "myocardial infarction"]
    },
    "coronary_heart_disease": {
        "aliases_exact": ["CVDCRHD4", "CVDCRHD5", "CHD"],
        "label_terms":  ["coronary heart disease", "angina"]
    },
    "stroke": {
        "aliases_exact": ["CVDSTRK3", "CVDSTRK4"],
        "label_terms":  ["stroke", "cerebrovascular"]
    },
    "arthritis": {
        "aliases_exact": ["HAVARTH3", "HAVARTH4", "ARTHRTIS"],
        "label_terms":  ["arthritis", "doctor told arthritis"]
    },
    "kidney_disease": {
        "aliases_exact": ["CHCKIDNY", "KIDNEY", "KIDNEYDS"],
        "label_terms":  ["kidney disease", "chronic kidney"]
    },
    "depression_diagnosis": {
        "aliases_exact": ["ADDEPEV3", "ADDEPEV2", "DEPRESS"],
        "label_terms":  ["depression", "ever told"]
    },
    "cancer_any": {
        "aliases_exact": ["CNCRHAVE", "CNCRHAVE2"],
        "label_terms":  ["cancer (any)", "ever told cancer"]
    },
    "cancer_skin": {
        "aliases_exact": ["CNCRSKN2", "CNCRSKN3"],
        "label_terms":  ["skin cancer"]
    },

    # ---------------- Anthropometrics ----------------
    "bmi_value": {
        "aliases_exact": ["_BMI5", "_BMI"],
        "label_terms":  ["bmi", "body mass index"]
    },
    "bmi_category": {
        "aliases_exact": ["_BMI5CAT", "BMICAT"],
        "label_terms":  ["bmi category", "underweight", "obese", "overweight"]
    },
    "height_inches": {
        "aliases_exact": ["HTIN4", "HEIGHT3", "X_HTIN4"],
        "label_terms":  ["height", "inches"]
    },
    "weight_pounds": {
        "aliases_exact": ["WTKG3", "WEIGHT2", "X_WT2"],
        "label_terms":  ["weight", "pounds", "kg"]
    },

    # ---------------- Preventive Services ----------------
    "flu_vaccine": {
        "aliases_exact": ["FLUSHOT6", "FLUSHOT7", "FLUSHOT"],
        "label_terms":  ["flu shot", "influenza vaccine"]
    },
    "pneumonia_vaccine": {
        "aliases_exact": ["PNEUVAC3", "PNEUVAC4"],
        "label_terms":  ["pneumonia vaccine", "pneumococcal"]
    },
    "mammogram_ever": {
        "aliases_exact": ["HADMAM", "HADMAM2"],
        "label_terms":  ["mammogram", "ever had"]
    },
    "pap_test_ever": {
        "aliases_exact": ["HADPAP2", "HADPAP3"],
        "label_terms":  ["pap test", "cervical screening"]
    },
    "colorectal_screening": {
        "aliases_exact": ["HADSIGM3", "HADSIGM4", "HADSGCO1", "HADSIGM", "COLSCREEN"],
        "label_terms":  ["colonoscopy", "sigmoidoscopy", "colorectal cancer screening"]
    },
    "prostate_screening": {
        "aliases_exact": ["PSATEST1", "PSATEST2", "PSA"],
        "label_terms":  ["psa", "prostate screening"]
    },
    "dental_visit_past_year": {
        "aliases_exact": ["DENVST3", "DENTAL", "DENTVST"],
        "label_terms":  ["dental visit", "dentist", "past year"]
    }
}








# SEED_CANONICAL = {
#     # Demographics & Design
#     "state_fips": {
#         "aliases_exact": ["_STATE", "STATE", "state_fips"],
#         "label_terms":  ["state", "fips"]
#     },
#     "county_fips3": {
#         "aliases_exact": ["_CNTY", "CNTY", "county_fips3"],
#         "label_terms":  ["county", "fips"]
#     },
#     "psu": {
#         "aliases_exact": ["_PSU"],
#         "label_terms":  ["psu", "cluster"]
#     },
#     "strata": {
#         "aliases_exact": ["_STSTR"],
#         "label_terms":  ["strata", "weight", "design"]
#     },
#     "weight_final": {
#         "aliases_exact": ["_FINALWT", "_LLCPWT", "LLCPWT", "finalwt"],
#         "label_terms":  ["weight", "final"]
#     },
#     "sex": {
#         "aliases_exact": ["SEX"],
#         "label_terms":  ["sex", "gender"]
#     },
#     "age_5yr_band": {
#         "aliases_exact": ["_AGEG5YR"],
#         "label_terms":  ["age", "5-year", "agegroup"]
#     },
#     "age_65plus": {
#         "aliases_exact": ["_AGE65YR"],
#         "label_terms":  ["age", "65"]
#     },
#     "race_ethnicity": {
#         "aliases_exact": ["_RACE", "_RACEGR3", "RACE2"],
#         "label_terms":  ["race", "ethnicity"]
#     },
#     "marital_status": {
#         "aliases_exact": ["MARITAL"],
#         "label_terms":  ["marital", "married", "divorced", "widowed"]
#     },
#     "education": {
#         "aliases_exact": ["EDUCA"],
#         "label_terms":  ["educ", "education", "school"]
#     },
#     "income_category": {
#         "aliases_exact": ["INCOME2", "INCOME"],
#         "label_terms":  ["income", "household income"]
#     },

#     # General Health & Access
#     "general_health": {
#         "aliases_exact": ["GENHLTH"],
#         "label_terms":  ["general health", "overall health"]
#     },
#     "health_plan": {
#         "aliases_exact": ["HLTHPLN1"],
#         "label_terms":  ["health plan", "insurance"]
#     },
#     "personal_doctor": {
#         "aliases_exact": ["PERSDOC2", "PERSDOC3"],
#         "label_terms":  ["personal doctor", "provider"]
#     },

#     # Health Behaviors
#     "smoked_100_cigs": {
#         "aliases_exact": ["SMOKE100"],
#         "label_terms":  ["smoke", "100"]
#     },
#     "current_smoking": {
#         "aliases_exact": ["SMOKDAY2"],
#         "label_terms":  ["smoking", "days"]
#     },
#     "alcohol_drinks_month": {
#         "aliases_exact": ["ALCDAY5"],
#         "label_terms":  ["alcohol", "drinks"]
#     },
#     "binge_drinking": {
#         "aliases_exact": ["DRNK3GE5"],
#         "label_terms":  ["binge", "5 drinks"]
#     },
#     "physical_activity": {
#         "aliases_exact": ["EXERANY2"],
#         "label_terms":  ["exercise", "physical activity"]
#     },
#     "fruit_veg_consumption": {
#         "aliases_exact": ["FRUITJU1", "VEGETAB1"],
#         "label_terms":  ["fruit", "vegetable", "consumption"]
#     },

#     # Chronic Conditions
#     "diabetes": {
#         "aliases_exact": ["DIABETE3", "DIABETE4"],
#         "label_terms":  ["diabetes"]
#     },
#     "asthma": {
#         "aliases_exact": ["ASTHMA3"],
#         "label_terms":  ["asthma"]
#     },
#     "copd": {
#         "aliases_exact": ["COPD"],
#         "label_terms":  ["copd", "chronic lung disease"]
#     },
#     "cancer_history": {
#         "aliases_exact": ["CNCRDIFF", "CNCRDIAG"],
#         "label_terms":  ["cancer", "diagnosis"]
#     },
#     "heart_attack": {
#         "aliases_exact": ["CVDINFR4"],
#         "label_terms":  ["heart attack", "infarction"]
#     },
#     "stroke": {
#         "aliases_exact": ["CVDSTRK3"],
#         "label_terms":  ["stroke"]
#     },
#     "bmi": {
#         "aliases_exact": ["_BMI5", "_BMI5CAT"],
#         "label_terms":  ["bmi", "body mass index"]
#     }
# }








# SEED_CANONICAL = {
#     "state_fips": {
#         "aliases_exact": ["_STATE", "STATE", "state_fips"],
#         "label_terms":  ["state", "fips"]
#     },
#     "county_fips3": {
#         "aliases_exact": ["_CNTY", "CNTY", "county_fips3"],
#         "label_terms":  ["county", "fips"]
#     },
#     "age_5yr_band": {
#         "aliases_exact": ["_AGEG5YR"],
#         "label_terms":  ["age", "5-year", "agegroup"]
#     },
#     "age_65plus": {
#         "aliases_exact": ["_AGE65YR"],
#         "label_terms":  ["age", "65"]
#     },
#     "sex": {
#         "aliases_exact": ["SEX"],
#         "label_terms":  ["sex", "gender"]
#     },
#     "education": {
#         "aliases_exact": ["EDUCA"],
#         "label_terms":  ["educ", "education", "school"]
#     },
#     "smoked_100_cigs": {
#         "aliases_exact": ["SMOKE100"],
#         "label_terms":  ["smoke", "100"]
#     },
#     "psu": {
#         "aliases_exact": ["_PSU"],
#         "label_terms":  ["psu", "cluster"]
#     },
#     "strata": {
#         "aliases_exact": ["_STSTR"],
#         "label_terms":  ["strata", "weight", "design"]
#     },
#     "weight_final": {
#         "aliases_exact": ["_FINALWT", "_LLCPWT", "LLCPWT", "finalwt"],
#         "label_terms":  ["weight", "final"]
#     },
# }

# thresholds (tune if needed)
NAME_SIM_THRESH  = 90
LABEL_SIM_THRESH = 80

def main():
    if not VAR_INDEX.exists():
        raise SystemExit("Run 03_build_var_index.py first (missing var index).")

    df = pd.read_parquet(VAR_INDEX)
    # Keep unique (name,label) pairs with years aggregated
    agg = (df.groupby(["var_name_lc", "var_label_lc"], as_index=False)
             .agg(years=("year", lambda s: sorted(set(s))),
                  occurrences=("year", "size")))

    # Build pools for fuzzy search
    name_pool  = agg["var_name_lc"].tolist()
    label_pool = agg["var_label_lc"].fillna("").tolist()

    mapping = {}       # canonical -> {aliases: [{name,label,years,via}]}
    coverage_rows = [] # rows for CSV

    for canon, cfg in SEED_CANONICAL.items():
        aliases = []

        # 1) exact name matches
        for alias in cfg.get("aliases_exact", []):
            aln = alias.lower()
            hits = agg[agg["var_name_lc"] == aln]
            for _, r in hits.iterrows():
                aliases.append({
                    "var_name": r["var_name_lc"],
                    "var_label": r["var_label_lc"],
                    "years": r["years"],
                    "via": "exact_name"
                })

        # 2) fuzzy by var_name
        for term in cfg.get("aliases_exact", []):
            aln = term.lower()
            for cand, score, idx in process.extract(aln, name_pool, scorer=fuzz.ratio, limit=30):
                if score >= NAME_SIM_THRESH:
                    r = agg.iloc[idx]
                    aliases.append({
                        "var_name": r["var_name_lc"],
                        "var_label": r["var_label_lc"],
                        "years": r["years"],
                        "via": f"fuzzy_name:{score}"
                    })

        # 3) fuzzy by var_label
        for term in cfg.get("label_terms", []):
            t = term.lower()
            for cand, score, idx in process.extract(t, label_pool, scorer=fuzz.partial_ratio, limit=80):
                if score >= LABEL_SIM_THRESH:
                    r = agg.iloc[idx]
                    aliases.append({
                        "var_name": r["var_name_lc"],
                        "var_label": r["var_label_lc"],
                        "years": r["years"],
                        "via": f"fuzzy_label:{score}"
                    })

        # de-duplicate by var_name
        if aliases:
            seen = set()
            dedup = []
            for a in aliases:
                k = a["var_name"]
                if k in seen:
                    continue
                seen.add(k)
                dedup.append(a)

            mapping[canon] = {"aliases": dedup}

            for a in dedup:
                coverage_rows.append({
                    "canonical": canon,
                    "alias_var": a["var_name"],
                    "alias_label": a["var_label"],
                    "years": ",".join(map(str, a["years"])),
                    "via": a["via"]
                })

    # write outputs
    OUT_YAML.parent.mkdir(parents=True, exist_ok=True)
    with OUT_YAML.open("w") as f:
        yaml.safe_dump(mapping, f, sort_keys=True, allow_unicode=True)

    pd.DataFrame(coverage_rows).to_csv(OUT_CSV, index=False)

    print("Wrote:", OUT_YAML)
    print("Wrote:", OUT_CSV)
    print("Canonical concepts covered:", len(mapping))

if __name__ == "__main__":
    main()


















# #!/usr/bin/env python3
# """
# 04_suggest_canonical_map.py
# ---------------------------
# Build an initial canonical variable mapping by matching across BRFSS years
# using normalized names and labels (exact + fuzzy).

# Outputs:
#   - data/metadata/canonical_map_suggested.yaml
#   - data/metadata/canonical_coverage.csv
#   - data/metadata/unmatched_common.csv

# Usage:
#   python scripts/04_suggest_canonical_map.py
# """

# from __future__ import annotations

# from pathlib import Path
# import re
# import yaml
# import pandas as pd
# from rapidfuzz import fuzz, process
# from rapidfuzz.string_metric import jaro_winkler_similarity
# from rapidfuzz.utils import default_process

# VAR_INDEX = Path("data/metadata/brfss_var_index.parquet")
# OUT_YAML  = Path("data/metadata/canonical_map_suggested.yaml")
# OUT_COV   = Path("data/metadata/canonical_coverage.csv")
# OUT_UNM   = Path("data/metadata/unmatched_common.csv")

# # ----------------------------
# # Helpers
# # ----------------------------

# def norm_name(s: str) -> str:
#     """
#     Normalize a variable name for robust matching:
#     - lowercase
#     - strip leading underscores
#     - collapse underscores
#     - drop trailing digits (e.g., height2/3 -> height)
#     """
#     s = (s or "").strip().lower()
#     s = re.sub(r"^_+", "", s)
#     s = re.sub(r"_+", "_", s)
#     s = re.sub(r"\d+$", "", s)
#     return s

# def norm_label(s: str) -> str:
#     """
#     Normalize labels: lowercase, tokenize, dedupe tokens (order-insensitive).
#     Improves robustness to verbose/terse label variants.
#     """
#     s = default_process(s or "")
#     toks = list({t for t in s.split() if t})  # unique tokens
#     toks.sort()
#     return " ".join(toks)

# def label_sim(a: str, b: str) -> float:
#     """
#     Token-deduped Jaro-Winkler similarity (0..100) for labels.
#     """
#     aa = norm_label(a)
#     bb = norm_label(b)
#     return jaro_winkler_similarity(aa, bb) * 100.0

# def dedup_aliases(rows):
#     """
#     Deduplicate alias dicts by var_name (keep first).
#     """
#     seen = set()
#     out = []
#     for r in rows:
#         key = r["var_name"]
#         if key in seen:
#             continue
#         seen.add(key)
#         out.append(r)
#     return out

# # ----------------------------
# # Seeds (extend as needed)
# # ----------------------------

# SEED_CANONICAL = {
#     # Design/weights
#     "weight_final": {
#         "aliases_exact": ["_finalwt", "_llcpwt", "llcpwt", "finalwt"],
#         "label_terms":  ["weight", "final", "analysis", "wgt"],
#     },
#     "psu": {
#         "aliases_exact": ["_psu"],
#         "label_terms":  ["psu", "cluster"],
#     },
#     "strata": {
#         "aliases_exact": ["_ststr"],
#         "label_terms":  ["strata", "stratum", "design"],
#     },

#     # Geography
#     "state_fips": {
#         "aliases_exact": ["_state", "state", "state_fips"],
#         "label_terms":  ["state", "fips"],
#     },
#     "county_fips3": {
#         "aliases_exact": ["_cnty", "cnty", "county_fips3"],
#         "label_terms":  ["county", "fips"],
#     },

#     # Demographics
#     "age_5yr_band": {
#         "aliases_exact": ["_ageg5yr"],
#         "label_terms":  ["age", "5-year", "agegroup", "age group"],
#     },
#     "age_65plus": {
#         "aliases_exact": ["_age65yr"],
#         "label_terms":  ["age", "65"],
#     },
#     "sex": {
#         "aliases_exact": ["sex"],
#         "label_terms":  ["sex", "gender"],
#     },
#     "race_ethnicity": {
#         "aliases_exact": ["_imprace", "race", "racegr3", "racegr4"],
#         "label_terms":  ["race", "ethnicity"],
#     },
#     "education": {
#         "aliases_exact": ["educa"],
#         "label_terms":  ["educ", "education", "school"],
#     },
#     "income": {
#         "aliases_exact": ["income2", "incomg"],
#         "label_terms":  ["income", "household income"],
#     },

#     # General health & conditions
#     "gen_health": {
#         "aliases_exact": ["genhlth"],
#         "label_terms":  ["general health", "health status"],
#     },
#     "diabetes": {
#         "aliases_exact": ["diabete3", "diabete2", "diabete4"],
#         "label_terms":  ["diabetes"],
#     },

#     # Tobacco / alcohol / activity
#     "smoked_100_cigs": {
#         "aliases_exact": ["smoke100"],
#         "label_terms":  ["smoke", "100"],
#     },
#     "current_smoker": {
#         "aliases_exact": ["smoker3", "smoker2", "smoker4"],
#         "label_terms":  ["smoker", "currently"],
#     },
#     "binge_drink": {
#         "aliases_exact": ["_rfbing5", "binge", "drnk3ge5", "drnkwek"],
#         "label_terms":  ["binge", "alcohol"],
#     },
#     "phys_activity": {
#         "aliases_exact": ["exerany2", "exerany3"],
#         "label_terms":  ["exercise", "physical activity"],
#     },

#     # Anthropometrics
#     "weight_pounds": {
#         "aliases_exact": ["weight2", "weight", "_weight2"],
#         "label_terms":  ["weight", "pounds", "lb"],
#     },
#     "height_inches": {
#         "aliases_exact": ["height3", "height2", "height"],
#         "label_terms":  ["height", "inches", "ft", "in"],
#     },
#     "bmi": {
#         "aliases_exact": ["_bmi5", "_bmi4", "bmi"],
#         "label_terms":  ["bmi", "body mass index"],
#     },
# }

# # Thresholds
# NAME_EXACT_BONUS   = 100
# NAME_TOKEN_THRESH  = 92   # token_set_ratio threshold on normalized names
# LABEL_JW_THRESH    = 85   # Jaro-Winkler threshold on tokenized labels

# def main():
#     if not VAR_INDEX.exists():
#         raise SystemExit("Run 03_build_var_index.py first (missing var index).")

#     df = pd.read_parquet(VAR_INDEX)

#     # Aggregate unique (name,label) with collected years for matching
#     agg = (
#         df.groupby(["var_name_lc", "var_label_lc"], as_index=False)
#           .agg(years=("year", lambda s: sorted(set(s))),
#                occurrences=("year", "size"))
#     )

#     # Precompute normalized names for robust matching
#     agg["var_name_norm"]  = agg["var_name_lc"].map(norm_name)
#     agg["var_label_norm"] = agg["var_label_lc"].map(norm_label)

#     name_pool  = agg["var_name_norm"].tolist()
#     label_pool = agg["var_label_norm"].tolist()

#     mapping = {}      # canonical -> {aliases: [{var_name, var_label, years, via}]}
#     coverage_rows = []

#     for canon, cfg in SEED_CANONICAL.items():
#         hits = []

#         # --- 1) Exact name aliases (after normalization)
#         for alias in cfg.get("aliases_exact", []):
#             aln = norm_name(alias)
#             exact = agg[agg["var_name_norm"] == aln]
#             for _, r in exact.iterrows():
#                 hits.append({
#                     "var_name":  r["var_name_lc"],
#                     "var_label": r["var_label_lc"],
#                     "years":     r["years"],
#                     "via":       "exact_name",
#                     "score":     NAME_EXACT_BONUS
#                 })

#         # --- 2) Fuzzy name (token_set_ratio) around each alias
#         for alias in cfg.get("aliases_exact", []):
#             aln = norm_name(alias)
#             for cand, score, idx in process.extract(aln, name_pool, scorer=fuzz.token_set_ratio, limit=50):
#                 if score >= NAME_TOKEN_THRESH:
#                     r = agg.iloc[idx]
#                     hits.append({
#                         "var_name":  r["var_name_lc"],
#                         "var_label": r["var_label_lc"],
#                         "years":     r["years"],
#                         "via":       f"fuzzy_name:{score}",
#                         "score":     float(score),
#                     })

#         # --- 3) Fuzzy label matching using tokenized Jaro-Winkler
#         for term in cfg.get("label_terms", []):
#             t = norm_label(term)
#             # brute-force scan; label space is manageable
#             for i, lbl in enumerate(label_pool):
#                 score = label_sim(t, lbl)
#                 if score >= LABEL_JW_THRESH:
#                     r = agg.iloc[i]
#                     hits.append({
#                         "var_name":  r["var_name_lc"],
#                         "var_label": r["var_label_lc"],
#                         "years":     r["years"],
#                         "via":       f"fuzzy_label:{int(score)}",
#                         "score":     float(score),
#                     })

#         # De-duplicate by var_name; keep best scoring match per name
#         if hits:
#             by_name = {}
#             for h in hits:
#                 k = h["var_name"]
#                 if (k not in by_name) or (h.get("score", 0) > by_name[k].get("score", 0)):
#                     by_name[k] = h
#             aliases = list(by_name.values())
#             aliases.sort(key=lambda a: (-len(a["years"]), -a.get("score", 0), a["var_name"]))
#             mapping[canon] = {"aliases": [
#                 {"var_name": a["var_name"], "var_label": a["var_label"], "years": a["years"], "via": a["via"]}
#                 for a in aliases
#             ]}

#             for a in aliases:
#                 coverage_rows.append({
#                     "canonical":  canon,
#                     "alias_var":  a["var_name"],
#                     "alias_label":a["var_label"],
#                     "years":      ",".join(map(str, a["years"])),
#                     "n_years":    len(a["years"]),
#                     "via":        a["via"],
#                 })

#     # ----------------------------
#     # Save outputs
#     # ----------------------------
#     OUT_YAML.parent.mkdir(parents=True, exist_ok=True)

#     with OUT_YAML.open("w") as f:
#         yaml.safe_dump(mapping, f, sort_keys=True, allow_unicode=True)

#     cov_df = pd.DataFrame(coverage_rows).sort_values(["canonical", "n_years", "alias_var"], ascending=[True, False, True])
#     cov_df.to_csv(OUT_COV, index=False)

#     # Also surface the most common *unmapped* variables (to curate next)
#     mapped_names = {a["alias_var"] for _, row in cov_df.iterrows() for a in [row]}
#     # NB: alias_var are lowercase already
#     mapped_names |= set(sum([[a["var_name"].lower() for a in v["aliases"]] for v in mapping.values()], []))

#     # For prevalence, count in how many years each var_name appears
#     var_years = (
#         df.groupby("var_name_lc")["year"]
#           .apply(lambda s: len(set(s)))
#           .rename("years_present")
#           .reset_index()
#     )
#     unmapped = var_years[~var_years["var_name_lc"].isin(mapped_names)].sort_values("years_present", ascending=False)
#     unmapped.to_csv(OUT_UNM, index=False)

#     print(f"Wrote: {OUT_YAML}")
#     print(f"Wrote: {OUT_COV}  (rows={len(cov_df)})")
#     print(f"Wrote: {OUT_UNM}  (rows={len(unmapped)})")
#     print("Canonical concepts covered:", len(mapping))

# if __name__ == "__main__":
#     main()