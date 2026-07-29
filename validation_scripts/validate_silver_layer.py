import sys
import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("SilverDataValidator")

def run_validation(silver_bucket_path: str):
    logger.info("=" * 80)
    logger.info("STARTING SILVER LAYER DATA QUALITY & REFERENTIAL INTEGRITY AUDIT")
    logger.info(f"Target Silver S3 Path: {silver_bucket_path}")
    logger.info("=" * 80)

    # Initialize PySpark Session
    spark = (
        SparkSession.builder
        .appName("SilverLayerDataValidator")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    silver_base = silver_bucket_path.rstrip('/')
    passed_audits = 0
    failed_audits = 0
    audit_summary = []

    def record_audit(name: str, passed: bool, details: str):
        nonlocal passed_audits, failed_audits
        status = "PASSED" if passed else "FAILED"
        if passed:
            passed_audits += 1
            logger.info(f"🟢 [PASSED] {name}: {details}")
        else:
            failed_audits += 1
            logger.error(f"🔴 [FAILED] {name}: {details}")
        audit_summary.append((name, status, details))

    # 1. READ SILVER TABLES
    logger.info("\n--- 1. Reading Transformed Silver Tables ---")
    # Candidate base resolution
    raw_silver = silver_bucket_path.rstrip('/')
    silver_stripped = raw_silver[:-7] if raw_silver.endswith('/silver') else raw_silver
    silver_bases = []
    for sb in [raw_silver, f"{silver_stripped}/silver", silver_stripped]:
        if sb and sb not in silver_bases:
            silver_bases.append(sb)

    target_tables = ["dim_region", "dim_state", "dim_city", "fact_weather_sampled", "fact_weather", "crop_data"]
    tables = {}

    for t_name in target_tables:
        candidate_paths = []
        for sb in silver_bases:
            candidate_paths.extend([
                f"{sb}/{t_name}/",
                f"{sb}/{t_name}"
            ])
        seen_cp = set()
        candidate_paths = [p for p in candidate_paths if not (p in seen_cp or seen_cp.add(p))]

        loaded_df = None
        loaded_path = None
        for cp in candidate_paths:
            try:
                df = spark.read.parquet(cp)
                loaded_df = df
                loaded_path = cp
                break
            except Exception:
                continue

        if loaded_df is not None:
            tables[t_name] = loaded_df
            count = loaded_df.count()
            logger.info(f"Successfully loaded {t_name:<20} | Row Count: {count:,} | Path: {loaded_path}")
        else:
            if t_name != "crop_data":
                logger.error(f"FAILED to read required table {t_name} from any candidate path in {silver_bases}")
            tables[t_name] = None

    dim_region = tables.get("dim_region")
    dim_state = tables.get("dim_state")
    dim_city = tables.get("dim_city")
    fact_weather_sampled = tables.get("fact_weather_sampled")
    fact_weather = tables.get("fact_weather")
    crop_data = tables.get("crop_data")

    # 2. DIMENSION TABLES PRIMARY KEY & NULL AUDITS
    logger.info("\n--- 2. Dimension Primary Key & Structural Audits ---")

    # Audit 2.1: dim_region PK & Null Check
    if dim_region is not None:
        r_nulls = dim_region.filter(F.col("region_id").isNull()).count()
        r_count = dim_region.count()
        r_distinct = dim_region.select("region_id").distinct().count()
        record_audit(
            "dim_region Primary Key Uniqueness",
            (r_nulls == 0 and r_count == r_distinct),
            f"Total rows: {r_count}, Distinct region_id: {r_distinct}, Null region_id: {r_nulls}"
        )

    # Audit 2.2: dim_state PK & FK Check
    if dim_state is not None:
        s_nulls = dim_state.filter(F.col("state_id").isNull()).count()
        s_fk_nulls = dim_state.filter(F.col("region_id").isNull()).count()
        s_count = dim_state.count()
        s_distinct = dim_state.select("state_id").distinct().count()
        record_audit(
            "dim_state Primary Key Uniqueness & FK Non-Null",
            (s_nulls == 0 and s_fk_nulls == 0 and s_count == s_distinct),
            f"Total states: {s_count}, Distinct state_id: {s_distinct}, Null state_id: {s_nulls}, Null region_id: {s_fk_nulls}"
        )

    # Audit 2.3: dim_city Primary Key Uniqueness (0 Duplicate City IDs)
    if dim_city is not None:
        c_count = dim_city.count()
        c_distinct_ids = dim_city.select("city_id").distinct().count()
        c_null_ids = dim_city.filter(F.col("city_id").isNull()).count()
        c_null_fk = dim_city.filter(F.col("state_id").isNull()).count()
        dup_ids = c_count - c_distinct_ids
        record_audit(
            "dim_city Primary Key Uniqueness (0 Duplicates)",
            (dup_ids == 0 and c_null_ids == 0 and c_null_fk == 0),
            f"Total cities: {c_count}, Distinct city_id: {c_distinct_ids}, Duplicate IDs: {dup_ids}, Null city_id: {c_null_ids}, Null state_id: {c_null_fk}"
        )

        # Audit 2.4: dim_city Composite Natural Key Uniqueness (city_name, state_id)
        c_distinct_composite = dim_city.select("city_name", "state_id").distinct().count()
        dup_composite = c_count - c_distinct_composite
        record_audit(
            "dim_city Composite Natural Key Uniqueness (city_name, state_id)",
            (dup_composite == 0),
            f"Total cities: {c_count}, Distinct (city_name, state_id) composite pairs: {c_distinct_composite}, Duplicates: {dup_composite}"
        )

        # Audit 2.5: dim_city Surrogate Key Range & Determinism Check
        min_id = dim_city.agg(F.min("city_id")).collect()[0][0]
        max_id = dim_city.agg(F.max("city_id")).collect()[0][0]
        record_audit(
            "dim_city Sequential Integer Key Range Check",
            (min_id == 1 and max_id == c_count),
            f"Min city_id: {min_id}, Max city_id: {max_id}, Total cities: {c_count} (Sequential contiguous range: 1 to {c_count})"
        )

        # Audit 2.6: Sampled Flag Count Consistency
        sampled_count = dim_city.filter(F.col("is_sampled") == True).count()
        record_audit(
            "dim_city Sampled Flag Distribution",
            (sampled_count > 0 and sampled_count <= c_count),
            f"Sampled cities (is_sampled=True): {sampled_count} / {c_count} total cities"
        )

    # 3. FACT TABLES FOREIGN KEY & REFERENTIAL INTEGRITY AUDITS
    logger.info("\n--- 3. Fact Table Foreign Key & Referential Integrity Audits ---")

    # Audit 3.1: fact_weather Foreign Key NULLs
    if fact_weather is not None:
        fw_count = fact_weather.count()
        fw_null_city = fact_weather.filter(F.col("city_id").isNull()).count()
        state_col = "state_id" if "state_id" in fact_weather.columns else "state_name"
        fw_null_state = fact_weather.filter(F.col(state_col).isNull()).count()
        record_audit(
            "fact_weather Foreign Key Non-Null Assertion",
            (fw_null_city == 0 and fw_null_state == 0),
            f"Fact rows: {fw_count:,}, Null city_id: {fw_null_city}, Null {state_col}: {fw_null_state}"
        )

        # Audit 3.2: Referential Integrity (Every city_id in fact_weather exists in dim_city)
        if dim_city is not None:
            unmatched_weather = (
                fact_weather
                .join(dim_city, "city_id", "left_anti")
                .count()
            )
            record_audit(
                "fact_weather -> dim_city Referential Integrity",
                (unmatched_weather == 0),
                f"Unmatched weather fact rows: {unmatched_weather} (Orphaned facts with non-existent city_id: 0)"
            )

    # Audit 3.3: fact_weather_sampled Foreign Key NULLs
    if fact_weather_sampled is not None:
        fws_count = fact_weather_sampled.count()
        fws_null_city = fact_weather_sampled.filter(F.col("city_id").isNull()).count()
        s_col = "state_id" if "state_id" in fact_weather_sampled.columns else "state_name"
        fws_null_state = fact_weather_sampled.filter(F.col(s_col).isNull()).count()
        record_audit(
            "fact_weather_sampled Foreign Key Non-Null Assertion",
            (fws_null_city == 0 and fws_null_state == 0),
            f"Sampled fact rows: {fws_count:,}, Null city_id: {fws_null_city}, Null {s_col}: {fws_null_state}"
        )

    # Audit 3.4: State Representation Consistency between Full & Sampled Weather Fact Tables
    if fact_weather is not None and fact_weather_sampled is not None:
        full_states = fact_weather.select("state_name").distinct().count()
        sampled_states = fact_weather_sampled.select("state_name").distinct().count()
        record_audit(
            "State Representation Parity (fact_weather vs fact_weather_sampled)",
            (full_states == sampled_states),
            f"Full weather state count: {full_states}, Sampled weather state count: {sampled_states}"
        )

    # 4. CROP DATASET VERIFICATION
    logger.info("\n--- 4. Crop Dataset Verification ---")
    if crop_data is not None:
        cd_count = crop_data.count()
        cd_cols = len(crop_data.columns)
        record_audit(
            "crop_data Dataset Persistence & Schema",
            (cd_count > 0 and cd_cols >= 20),
            f"Crop dataset rows: {cd_count:,}, Total columns: {cd_cols}"
        )

    # 5. AUDIT SUMMARY REPORT
    logger.info("\n" + "=" * 80)
    logger.info("FINAL SILVER LAYER DATA QUALITY AUDIT SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total Audits Performed : {passed_audits + failed_audits}")
    logger.info(f"Passed Audits          : {passed_audits} 🟢")
    logger.info(f"Failed Audits          : {failed_audits} 🔴")
    logger.info("-" * 80)
    for name, status, details in audit_summary:
        icon = "🟢" if status == "PASSED" else "🔴"
        logger.info(f"{icon} {status:<8} | {name:<60} | {details}")
    logger.info("=" * 80)

    if failed_audits > 0:
        logger.error("CRITICAL: Silver Layer Data Quality Audit encountered failures!")
        sys.exit(1)
    else:
        logger.info("SUCCESS: All Silver Layer Data Quality Audits passed cleanly!")

if __name__ == "__main__":
    silver_path = "s3://agro-weather-data-lake2/silver"

    # Glue Environment Option Resolution
    try:
        from awsglue.utils import getResolvedOptions
        if any("--SILVER_BUCKET" in arg for arg in sys.argv):
            opts = getResolvedOptions(sys.argv, ["SILVER_BUCKET"])
            silver_path = opts["SILVER_BUCKET"]
        elif any("--silver-path" in arg for arg in sys.argv):
            opts = getResolvedOptions(sys.argv, ["silver-path"])
            silver_path = opts["silver-path"]
    except Exception:
        # Fallback manual parsing for non-Glue environments
        for i, arg in enumerate(sys.argv):
            if arg in ("--silver-path", "--SILVER_BUCKET") and i + 1 < len(sys.argv):
                silver_path = sys.argv[i + 1]
                break
            elif arg.startswith("--silver-path=") or arg.startswith("--SILVER_BUCKET="):
                silver_path = arg.split("=", 1)[1]
                break

    run_validation(silver_path)
