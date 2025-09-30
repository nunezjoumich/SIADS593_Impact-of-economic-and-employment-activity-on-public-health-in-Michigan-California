import os, glob, re
import pandas as pd

def process_brfss(TEST_YEARS=None):
    RAW_DIR = "data/raw/brfss_year/"
    OUT_DIR = "data/processed/"
    os.makedirs(OUT_DIR, exist_ok=True)
    
    print("Current working directory:", os.getcwd())
    print("Looking in:", os.path.abspath(RAW_DIR))

    # Load crosswalk
    crosswalk = pd.read_csv("county_fips_crosswalk.csv", dtype=str)
    crosswalk["fips"] = crosswalk["fips"].astype(str).str.zfill(5)

    # Collect yearly files
    all_files = glob.glob(os.path.join(RAW_DIR, "brfss_*.csv"))
    csv_files = [f for f in all_files if re.search(r"brfss_\d{4}\.csv$", f)]
    csv_files = sorted(csv_files)

    if TEST_YEARS:
        csv_files = [f for f in csv_files if any(y in f for y in TEST_YEARS)]
    print(f"Found {len(csv_files)} files to process:", [os.path.basename(f) for f in csv_files])

    # --- Column maps ---
    job_a_map = {
        # Survey design & metadata
        "_state": "state_fips_code",
        "_geostr": "geographic_stratum",
        "_denstr": "density_stratum",
        "_psu": "primary_sampling_unit",
        "idate": "interview_date",
        "imonth": "interview_month",
        "iday": "interview_day",
        "iyear": "interview_year",
        
        # Household composition
        "numadult": "num_adults_in_household",
        "nummen": "num_men_in_household",
        "numwomen": "num_women_in_household",
        
        # General health
        "genhlth": "general_health_status",
        "physhlth": "days_physical_health_not_good",
        "menthlth": "days_mental_health_not_good",
        "poorhlth": "days_poor_health_limited_activities",
        
        # Insurance / coverage
        "hlthplan": "has_any_health_plan",
        "medicar2": "covered_by_medicare",
        "medcost": "could_not_see_doctor_due_to_cost",
        
        # Blood pressure / cholesterol
        "bphigh": "ever_told_high_bp",
        "toldhi": "ever_told_high_cholesterol",
        "diabetes": "ever_told_diabetes",
        
        # Tobacco
        "smoke100": "smoked_100_cigarettes",
        "smokeday": "current_smoking_frequency",
        
        # Alcohol use
        "drinkany": "any_alcohol_past_month",
        "alcohol": "avg_drinks_per_week",
        
        # Core demographics
        "age": "respondent_age",
        "sex": "respondent_sex",
        "orace": "self_reported_race",
        "hispanic": "hispanic_ethnicity",
        "marital": "marital_status",
        "educa": "education_level",
        "employ": "employment_status",
        "income2": "household_income_category",
        "weight": "respondent_weight_pounds",
        "height": "respondent_height_inches",
        "ctycode": "county_code",
        "numhhold": "num_households",
        "numphons": "num_phones",
    }
    
    job_b_map = {
        # Additional health variables
        "cholchk": "cholesterol_checked_5yr",
        "flushot": "flu_shot_past_year",
        "asthma": "ever_told_asthma",
        "asthnow": "currently_has_asthma",
    }
    
    job_c_map = {
        # Nutrition & physical activity
        "fruit": "eats_fruit",
        "vegetabl": "eats_other_vegetables", 
        "exerany": "any_physical_activity",
        "renthome": "housing_tenure_rent_vs_own",
        "enghfood": "food_security",
    }
    
    job_d_map = {
        # Derived variables
        "_bmi": "body_mass_index",
        "_smoker2": "smoking_status_recode",
        "_rfhype2": "high_blood_pressure_flag",
        "_rfobese": "obesity_flag",
        "_ageg": "age_category",
        "_raceg": "race_group",
        "year": "survey_year",
    }

    column_map = {}
    column_map.update(job_a_map)
    column_map.update(job_b_map)
    column_map.update(job_c_map)
    column_map.update(job_d_map)

    # Thematic variable lists
    geo_cols = ["county_name", "state_name", "survey_year"]
    socio_vars_core = [
        "respondent_age","respondent_sex","self_reported_race","hispanic_ethnicity",
        "marital_status","education_level","employment_status","household_income_category",
        "num_adults_in_household","num_men_in_household","num_women_in_household",
        "housing_tenure_rent_vs_own","food_security",
        "has_any_health_plan","covered_by_medicare","could_not_see_doctor_due_to_cost"
    ]
    health_vars_core = [
        "general_health_status","days_physical_health_not_good","days_mental_health_not_good",
        "days_poor_health_limited_activities","ever_told_high_bp","ever_told_diabetes",
        "smoked_100_cigarettes","current_smoking_frequency","any_alcohol_past_month",
        "avg_drinks_per_week","body_mass_index","respondent_weight_pounds",
        "respondent_height_inches","eats_fruit","eats_other_vegetables","any_physical_activity"
    ]
    socio_vars_expanded = socio_vars_core + ["county_code","num_households","num_phones"]
    health_vars_expanded = health_vars_core + ["cholesterol_checked_5yr","ever_told_high_cholesterol"]

    # --- Loop through years ---
    for fpath in csv_files:
        year = os.path.basename(fpath).split("_")[1].split(".")[0]
        print(f"\n🔄 Processing {year}...")

        brfss = pd.read_csv(fpath, dtype=str)

        # FIPS build
        brfss["_state"] = brfss["_state"].astype(str).str.extract(r"(\\d+)").iloc[:, 0].fillna("").str.zfill(2)
        brfss["ctycode"] = brfss["ctycode"].astype(str).str.extract(r"(\\d+)").iloc[:, 0].fillna("").str.zfill(3)
        brfss["fips"] = brfss["_state"] + brfss["ctycode"]
        brfss = brfss[brfss["ctycode"] != "000"].copy()

        # Merge
        brfss = brfss.merge(crosswalk, on="fips", how="left")
        brfss = brfss.rename(columns=column_map)
        brfss["survey_year"] = year

        # Build thematic DataFrames
        brfss_socio_core = brfss[geo_cols + [c for c in socio_vars_core if c in brfss.columns]].copy()
        brfss_health_core = brfss[geo_cols + [c for c in health_vars_core if c in brfss.columns]].copy()
        brfss_socio_exp   = brfss[geo_cols + [c for c in socio_vars_expanded if c in brfss.columns]].copy()
        brfss_health_exp  = brfss[geo_cols + [c for c in health_vars_expanded if c in brfss.columns]].copy()

        # Save
        brfss_socio_core.to_csv(os.path.join(OUT_DIR, f"full_brfss_{year}_socioeconomics_core.csv"), index=False)
        brfss_health_core.to_csv(os.path.join(OUT_DIR, f"full_brfss_{year}_health_core.csv"), index=False)
        brfss_socio_exp.to_csv(os.path.join(OUT_DIR, f"full_brfss_{year}_socioeconomics_expanded.csv"), index=False)
        brfss_health_exp.to_csv(os.path.join(OUT_DIR, f"full_brfss_{year}_health_expanded.csv"), index=False)

        print(f"✔️ Saved 4 outputs for {year} into {OUT_DIR}")

if __name__ == "__main__":
    # Example: run all years
    process_brfss()
    # Example: run just 1999 + 2000
    # process_brfss(TEST_YEARS=["1999", "2000"])