import pandas as pd
import math

# ==========================================================
# CHANGE THIS PATH
# ==========================================================
INPUT_FILE = r"D:\dataset\silver\weather.csv"

DEDUP_FILE = r"D:\dataset\silver\weather_deduplicated.csv"

OUTPUT_FILE = r"D:\dataset\silver\city_master_strategy_c.csv"

# ==========================================================

print("="*70)
print("      STRATEGY C : STRATIFIED PERCENTAGE SAMPLING")
print("="*70)

# ----------------------------------------------------------
# STEP 1 : LOAD DATA
# ----------------------------------------------------------

df = pd.read_csv(INPUT_FILE)

print(f"\nOriginal rows : {len(df):,}")

# ----------------------------------------------------------
# STEP 2 : REMOVE DUPLICATE CITY NAMES
# ----------------------------------------------------------

duplicates = df['city'].duplicated().sum()

print(f"Duplicate city names : {duplicates}")

df_dedup = df.drop_duplicates(subset=['city'], keep=False).copy()

print(f"Rows after deduplication : {len(df_dedup):,}")

# Save intermediate dataset

df_dedup.to_csv(DEDUP_FILE, index=False)

print(f"Saved : {DEDUP_FILE}")

# ----------------------------------------------------------
# STEP 3 : STRATEGIFIED STATE-WISE SAMPLING
# ----------------------------------------------------------

PERCENTAGE = 0.15
MIN_CITIES = 5
RANDOM_STATE = 42

sampled = []

summary = []

print("\nSampling from each state...\n")

for state, group in df_dedup.groupby("state"):

    total = len(group)

    sample_size = max(
        MIN_CITIES,
        math.ceil(total * PERCENTAGE)
    )

    sample_size = min(sample_size, total)

    sample = group.sample(
        n=sample_size,
        random_state=RANDOM_STATE
    )

    sampled.append(sample)

    summary.append({
        "State": state,
        "Total Cities": total,
        "Selected": sample_size,
        "Percentage": round(sample_size/total*100,2)
    })

# ----------------------------------------------------------
# STEP 4 : CREATE FINAL DATASET
# ----------------------------------------------------------

df_final = pd.concat(sampled, ignore_index=True)

df_final.to_csv(OUTPUT_FILE, index=False)

summary_df = pd.DataFrame(summary)

# ----------------------------------------------------------
# STEP 5 : VALIDATION
# ----------------------------------------------------------

print("="*70)

print("\nVALIDATION\n")

print("States in original :", df_dedup["state"].nunique())
print("States in sample   :", df_final["state"].nunique())

print()

print("Regions in original :", df_dedup["region"].nunique())
print("Regions in sample   :", df_final["region"].nunique())

print()

print("Duplicate cities in sample :", df_final["city"].duplicated().sum())

print()

print("Missing values\n")
print(df_final.isnull().sum())

print("="*70)

print("\nSAMPLING SUMMARY\n")

print(summary_df.to_string(index=False))

print("\n====================================================")

print(f"Original Cities     : {len(df):,}")
print(f"After Dedup         : {len(df_dedup):,}")
print(f"After Sampling      : {len(df_final):,}")

reduction = (1-len(df_final)/len(df_dedup))*100

print(f"Reduction           : {reduction:.2f}%")

print(f"\nSaved Final Dataset :\n{OUTPUT_FILE}")

print("="*70)

# ----------------------------------------------------------
# STEP 6 : SHOW SELECTED CITIES STATE-WISE
# ----------------------------------------------------------

print("\n" + "="*70)
print("SELECTED CITIES BY STATE")
print("="*70)

for state in sorted(df_final["state"].unique()):

    cities = sorted(df_final[df_final["state"] == state]["city"].tolist())

    print(f"\n{state} ({len(cities)} cities)")
    print(cities)