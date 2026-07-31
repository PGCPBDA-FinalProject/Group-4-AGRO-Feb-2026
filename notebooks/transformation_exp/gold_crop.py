# ============================================================
# GOLD LAYER - CROP x WEATHER SEASON MATCHING
# AWS GLUE 4.0
# Standalone script - run AFTER gold_layer.py (weather) has produced
# gold/daily_weather and AFTER crop CSV is landed in Silver as
# fact_crop_yield.
#
# NO ML. This is a deterministic, explainable scoring/matching engine:
#   1. Build a "crop requirement profile" per crop (from historical
#      yield data: what Rainfall/Temp/Humidity/pH/Wind/Solar values
#      that crop has actually been grown/yielded well under).
#   2. Build a "season profile" per city+season (from your existing
#      daily_weather gold table, averaged season-wise).
#   3. Score the distance between each city-season profile and each
#      crop profile on shared features.
#   4. Rank crops per city-season by score -> "this city, this
#      season, can likely grow these crops".
# ============================================================

import sys
import logging
import time

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("CropWeatherMatch")

# ============================================================
# DEFAULT CONFIGURATION
# ============================================================

DEFAULT_ARGS = {
    "JOB_NAME": "gold_crop_weather_match",
    "SILVER_BUCKET": "s3://silver-bronze-s3/silver-data/",
    "GOLD_BUCKET": "s3://silver-bronze-s3/gold-data/"
}

PIPELINE_CONFIG = {

    # Features shared between crop data and weather data.
    # These MUST exist (or be derivable) on both sides.
    # tuple = (weather_gold_column, crop_silver_column, default_weight)
    # These weights are the FALLBACK/global default used for any
    # crop that doesn't have a custom weight set in the optional
    # dim_crop_feature_weights Silver table (see
    # read_crop_feature_weights() below). Defaults should sum to 1.0.
    "MATCH_FEATURES": [
        ("avg_temperature_2m", "Temperature_C", 0.30),
        ("avg_humidity", "Humidity_%", 0.20),
        ("season_rainfall", "Rainfall_mm", 0.30),
        ("avg_wind_speed_10m", "Wind_Speed_m_s", 0.10),
        # pH and Solar_Radiation have no direct weather-fact
        # equivalent today. pH is soil data (not weather) -
        # kept as a crop-only filter, not a match feature, unless
        # you bring in a soil dataset. Solar_Radiation could be
        # approximated from (100 - avg_cloud_cover) if you want to
        # include it - see "Extension ideas" below.
        ("solar_proxy", "Solar_Radiation_MJ_m2_day", 0.10),
    ],

    # Optional Silver table with per-crop weight overrides.
    # Expected schema: crop (string), feature (string - must match
    # a crop_silver_column above, e.g. "Rainfall_mm"), weight (double).
    # Example rows:
    #   ("Paddy", "Rainfall_mm", 0.45)
    #   ("Paddy", "Temperature_C", 0.25)
    #   ("Wheat", "Temperature_C", 0.35)
    # A crop only needs rows for the features it wants to override -
    # any feature not listed for that crop falls back to the global
    # default weight above. If the table doesn't exist at all, every
    # crop just uses the global defaults.
    "CROP_WEIGHTS_TABLE": "dim_crop_feature_weights",

    # Below this composite score (0-100), we don't consider it a
    # viable match at all, regardless of rank.
    "MIN_VIABILITY_SCORE": 60,

    # A city-season needs at least this many observed days in that
    # season for its profile to be considered reliable. Below this,
    # matches are still produced but flagged low-confidence.
    "MIN_DAYS_FOR_CONFIDENCE": 60,

    # Yield tiers (absolute kg/ha cutoffs). Adjust to your dataset's
    # real yield distribution - check build_crop_profile() output
    # stats before finalizing.
    "YIELD_TIER_HIGH": 3000,
    "YIELD_TIER_MEDIUM": 1500,

    # How many top crops to keep per city-season
    "TOP_N_CROPS": 5,

    "OUTPUT_PARTITIONS": 10,
    "SHUFFLE_PARTITIONS": 200,
    "PARQUET_COMPRESSION": "snappy"
}


# ============================================================
# INITIALIZE GLUE
# ============================================================

def initialize_glue():
    options = DEFAULT_ARGS.copy()

    try:
        glue_args = getResolvedOptions(
            sys.argv,
            ["JOB_NAME", "SILVER_BUCKET", "GOLD_BUCKET"]
        )
        options.update(glue_args)

    except Exception:
        logger.info("Using default job parameters.")

    sc = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(options["JOB_NAME"], options)

    return spark, job, options


# ============================================================
# CONFIGURE SPARK
# ============================================================

def configure_spark(spark):
    logger.info("Applying Spark optimizations...")

    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.sql.broadcastTimeout", "1200")
    spark.conf.set("spark.sql.parquet.compression.codec", PIPELINE_CONFIG["PARQUET_COMPRESSION"])
    spark.conf.set("spark.sql.shuffle.partitions", PIPELINE_CONFIG["SHUFFLE_PARTITIONS"])
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark configured successfully.")


# ============================================================
# READ INPUTS
# ============================================================

def read_inputs(spark, silver_bucket, gold_bucket):
    logger.info("Reading inputs...")

    # Your existing weather gold output (already daily, city-level,
    # enriched with city/state/region names)
    daily_weather = spark.read.parquet(
        f"{gold_bucket}/daily_weather"
    )

    # Crop yield data - raw Kaggle CSV re-saved as Parquet in Silver
    # (same column names/headers as the original CSV, no cleaning
    # applied yet). If you later run silver_crop_ingestion.py to
    # clean this properly, point this back at "fact_crop_yield".
    fact_crop_yield = spark.read.parquet(
        f"{silver_bucket}/crop_data"
    )

    logger.info(f"Weather Records : {daily_weather.count():,}")
    logger.info(f"Crop Records    : {fact_crop_yield.count():,}")

    daily_weather.persist(StorageLevel.MEMORY_AND_DISK)
    fact_crop_yield.persist(StorageLevel.MEMORY_AND_DISK)

    return daily_weather, fact_crop_yield


# ============================================================
# BUILD CITY-SEASON WEATHER PROFILE
# (season-wise average of daily gold weather, per city)
# ============================================================

def build_city_season_profile(daily_weather: DataFrame) -> DataFrame:
    logger.info("=" * 70)
    logger.info("STEP 1 : CITY-SEASON WEATHER PROFILE")
    logger.info("=" * 70)

    # Reuse the same season logic as your weather gold script so
    # season definitions stay consistent across both pipelines.
    season_df = (
        daily_weather
        .withColumn(
            "season",
            F.when(F.col("month").isin(6, 7, 8, 9), "Kharif")
             .when(F.col("month").isin(10, 11, 12, 1, 2), "Rabi")
             .otherwise("Zaid")
        )
    )

    # solar_proxy: rough stand-in for solar radiation using cloud
    # cover, since we don't have a direct solar_radiation field in
    # the weather fact today. See "Extension ideas" for a real fix.
    season_df = season_df.withColumn(
        "solar_proxy",
        F.round((100 - F.col("avg_cloud_cover")) / 100 * 25, 2)
        # scaled roughly to a 0-25 MJ/m2/day range to be comparable
        # to typical Solar_Radiation_MJ_m2_day values. This is a
        # placeholder, not a scientific conversion.
    )

    city_season_profile = (
        season_df
        .groupBy(
            "region_name",
            "state_name",
            "district",
            "city_name",
            "season"
        )
        .agg(
            F.avg("avg_temperature_2m").alias("avg_temperature_2m"),
            F.avg("avg_humidity").alias("avg_humidity"),
            F.sum("daily_rain").alias("season_rainfall"),
            F.avg("avg_wind_speed_10m").alias("avg_wind_speed_10m"),
            F.avg("solar_proxy").alias("solar_proxy"),
            F.count("*").alias("days_observed")
        )
    )

    city_season_profile.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info(
        f"City-Season Profiles : {city_season_profile.count():,}"
    )

    return city_season_profile


# ============================================================
# BUILD CROP REQUIREMENT PROFILE
# (historical average of conditions crop was actually grown under -
#  weighted by yield, so "good yield years" count more)
# ============================================================

def build_crop_profile(fact_crop_yield: DataFrame) -> DataFrame:
    logger.info("=" * 70)
    logger.info("STEP 2 : CROP REQUIREMENT PROFILE")
    logger.info("=" * 70)

    # Basic cleaning - drop rows with missing core fields, filter
    # out impossible values. Adjust ranges to your data's reality.
    # Trim Crop since this table hasn't been through a Silver
    # cleaning step - raw CSV values may have stray whitespace,
    # which would otherwise split "Wheat" and "Wheat " into two
    # separate crops.
    cleaned = (
        fact_crop_yield
        .withColumn("Crop", F.trim(F.col("Crop")))
        .filter(F.col("Crop").isNotNull())
        .filter(F.col("Crop") != "")
        .filter(F.col("Yield_kg_per_ha").isNotNull())
        .filter(F.col("Yield_kg_per_ha") > 0)
        .filter(F.col("Temperature_C").between(-10, 55))
        .filter(F.col("Humidity_%").between(0, 100))
        .filter(F.col("Rainfall_mm") >= 0)
    )

    # Weight each observation by its yield relative to that crop's
    # own max yield, so we lean the "ideal profile" toward the
    # conditions where the crop actually did WELL, not just where
    # it was planted (planting != success).
    yield_window = Window.partitionBy("Crop")

    weighted = cleaned.withColumn(
        "yield_weight",
        F.col("Yield_kg_per_ha") / F.max("Yield_kg_per_ha").over(yield_window)
    )

    crop_profile = (
        weighted
        .groupBy("Crop")
        .agg(
            # Weighted averages (manual weighted mean since Spark
            # has no built-in weighted avg)
            (F.sum(F.col("Temperature_C") * F.col("yield_weight")) /
             F.sum("yield_weight")).alias("Temperature_C"),

            (F.sum(F.col("Humidity_%") * F.col("yield_weight")) /
             F.sum("yield_weight")).alias("Humidity_%"),

            (F.sum(F.col("Rainfall_mm") * F.col("yield_weight")) /
             F.sum("yield_weight")).alias("Rainfall_mm"),

            (F.sum(F.col("Wind_Speed_m_s") * F.col("yield_weight")) /
             F.sum("yield_weight")).alias("Wind_Speed_m_s"),

            (F.sum(F.col("Solar_Radiation_MJ_m2_day") * F.col("yield_weight")) /
             F.sum("yield_weight")).alias("Solar_Radiation_MJ_m2_day"),

            F.avg("pH").alias("pH"),

            F.avg("Yield_kg_per_ha").alias("avg_yield_kg_per_ha"),
            F.count("*").alias("observations")
        )
    )

    crop_profile.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info(f"Crop Profiles : {crop_profile.count():,}")

    return crop_profile


# ============================================================
# PER-CROP FEATURE WEIGHTS
# Reads the optional dim_crop_feature_weights Silver table and
# attaches a weight_<crop_col> column per match feature onto the
# crop profile. Any crop/feature combo not present in the override
# table falls back to the global default weight from
# PIPELINE_CONFIG["MATCH_FEATURES"]. If the table doesn't exist at
# all, everything just uses global defaults - this function never
# fails the job.
# ============================================================

def attach_feature_weights(
        spark,
        silver_bucket: str,
        crop_profile: DataFrame,
        match_features
) -> DataFrame:
    logger.info("Attaching per-crop feature weights...")

    weights_path = f"{silver_bucket}/{PIPELINE_CONFIG['CROP_WEIGHTS_TABLE']}"

    try:
        overrides = spark.read.parquet(weights_path)
        override_count = overrides.count()

        if override_count == 0:
            raise ValueError("empty override table")

        logger.info(
            f"Found {override_count:,} custom crop weight overrides."
        )

        # Pivot to one column per feature: weight_<crop_col>
        pivoted = (
            overrides
            .groupBy("crop")
            .pivot("feature", [f[1] for f in match_features])
            .agg(F.first("weight"))
        )

        for _, crop_col, _ in match_features:
            pivoted = pivoted.withColumnRenamed(
                crop_col, f"weight_{crop_col}"
            )

        crop_profile = crop_profile.join(
            F.broadcast(pivoted),
            crop_profile["Crop"] == pivoted["crop"],
            "left"
        ).drop(pivoted["crop"])

    except Exception:
        logger.info(
            "No custom crop weight overrides found - "
            "using global defaults for all crops."
        )
        for _, crop_col, _ in match_features:
            crop_profile = crop_profile.withColumn(
                f"weight_{crop_col}", F.lit(None).cast("double")
            )

    # Fill any still-missing weight (crop not in override table, or
    # crop present but this particular feature not overridden) with
    # the global default for that feature.
    for _, crop_col, default_weight in match_features:
        wcol = f"weight_{crop_col}"
        crop_profile = crop_profile.withColumn(
            wcol,
            F.coalesce(F.col(wcol), F.lit(float(default_weight)))
        )

    return crop_profile


# ============================================================
# NORMALIZATION HELPERS
# For fair scoring, differences need to be on a comparable scale.
# We use min-max range per feature (from the crop side, since
# that's our reference population) to normalize the absolute gap.
# ============================================================

def get_feature_ranges(crop_profile: DataFrame, crop_columns) -> dict:
    ranges = {}

    stats = crop_profile.select(
        *[F.min(c).alias(f"min_{c}") for c in crop_columns],
        *[F.max(c).alias(f"max_{c}") for c in crop_columns]
    ).collect()[0]

    for c in crop_columns:
        lo = stats[f"min_{c}"]
        hi = stats[f"max_{c}"]
        # Avoid divide-by-zero if a feature is constant
        ranges[c] = (lo, hi if hi != lo else lo + 1)

    return ranges


# ============================================================
# SCORE + MATCH CITY-SEASON TO CROPS (cross join + scoring)
# ============================================================

def build_crop_weather_match(
        city_season_profile: DataFrame,
        crop_profile: DataFrame
) -> DataFrame:
    logger.info("=" * 70)
    logger.info("STEP 3 : CROSS-MATCH SCORING")
    logger.info("=" * 70)

    match_features = PIPELINE_CONFIG["MATCH_FEATURES"]
    crop_columns = [f[1] for f in match_features]

    ranges = get_feature_ranges(crop_profile, crop_columns)

    # Cross join is intentional here: every city-season needs to be
    # compared against every crop. This is small-dimension data
    # (a few hundred city-seasons x a few dozen crops), so it's
    # cheap. Broadcast the smaller side (crop_profile) to avoid
    # a shuffle-heavy join.
    matched = city_season_profile.crossJoin(
        F.broadcast(crop_profile)
    )

    # Per-feature normalized similarity score (0-100, 100 = identical),
    # multiplied by that crop's weight for this feature (per-crop
    # override if present via attach_feature_weights(), else global
    # default - see PIPELINE_CONFIG["MATCH_FEATURES"]).
    score_terms = []

    for weather_col, crop_col, _default_weight in match_features:
        lo, hi = ranges[crop_col]
        span = hi - lo

        term_col = f"score_{crop_col}"
        weight_col = f"weight_{crop_col}"

        matched = matched.withColumn(
            term_col,
            F.greatest(
                F.lit(0.0),
                F.lit(100.0) - (
                    F.abs(F.col(weather_col) - F.col(crop_col)) / F.lit(span) * 100.0
                )
            ) * F.col(weight_col)
        )

        score_terms.append(term_col)

    matched = matched.withColumn(
        "match_score",
        F.round(sum(F.col(t) for t in score_terms), 2)
    )

    matched = matched.withColumn(
        "is_viable",
        F.when(
            F.col("match_score") >= PIPELINE_CONFIG["MIN_VIABILITY_SCORE"],
            1
        ).otherwise(0)
    )

    # Confidence flag: does this city-season have enough observed
    # days to trust its weather profile? Low-confidence matches are
    # still returned (so you can see them), just clearly labeled.
    matched = matched.withColumn(
        "match_confidence",
        F.when(
            F.col("days_observed") >= PIPELINE_CONFIG["MIN_DAYS_FOR_CONFIDENCE"],
            "High"
        ).otherwise("Low")
    )

    # Expected yield tier for this crop, independent of the weather
    # match - tells you "even if viable, is this historically a
    # high or low yielding crop overall".
    matched = matched.withColumn(
        "expected_yield_tier",
        F.when(
            F.col("avg_yield_kg_per_ha") >= PIPELINE_CONFIG["YIELD_TIER_HIGH"],
            "High"
        )
        .when(
            F.col("avg_yield_kg_per_ha") >= PIPELINE_CONFIG["YIELD_TIER_MEDIUM"],
            "Medium"
        )
        .otherwise("Low")
    )

    # Rank crops per city-season by score
    rank_window = Window.partitionBy(
        "region_name", "state_name", "district", "city_name", "season"
    ).orderBy(F.desc("match_score"))

    matched = matched.withColumn(
        "crop_rank",
        F.dense_rank().over(rank_window)
    )

    top_matches = matched.filter(
        F.col("crop_rank") <= PIPELINE_CONFIG["TOP_N_CROPS"]
    )

    final = top_matches.select(
        "region_name",
        "state_name",
        "district",
        "city_name",
        "season",
        "days_observed",
        "match_confidence",
        F.col("Crop").alias("crop_name"),
        "match_score",
        "is_viable",
        "crop_rank",
        "expected_yield_tier",
        "avg_temperature_2m",
        F.col("Temperature_C").alias("crop_avg_temperature_c"),
        "avg_humidity",
        F.col("Humidity_%").alias("crop_avg_humidity_pct"),
        "season_rainfall",
        F.col("Rainfall_mm").alias("crop_avg_rainfall_mm"),
        "avg_wind_speed_10m",
        F.col("Wind_Speed_m_s").alias("crop_avg_wind_speed_ms"),
        "pH",
        "avg_yield_kg_per_ha"
    )

    final.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info(f"Match Records : {final.count():,}")

    return final


# ============================================================
# AUDIT COLUMNS
# ============================================================

def add_audit_columns(dataframe, job_name):
    return (
        dataframe
        .withColumn("etl_timestamp", F.current_timestamp())
        .withColumn("etl_layer", F.lit("gold"))
        .withColumn("etl_job", F.lit(job_name))
    )


# ============================================================
# WRITER
# ============================================================

def write_dataset(dataframe, output_path, partition_column=None):
    writer = dataframe.write.mode("overwrite")

    if partition_column:
        writer = writer.partitionBy(partition_column)

    writer.parquet(output_path)

    logger.info(f"Written -> {output_path}")


# ============================================================
# CLEANUP
# ============================================================

def cleanup(*dfs):
    logger.info("Cleaning cache...")
    for df in dfs:
        try:
            df.unpersist()
        except Exception:
            pass
    logger.info("Cleanup completed.")


# ============================================================
# MAIN
# ============================================================

def main():
    start_time = time.time()

    spark, job, args = initialize_glue()
    configure_spark(spark)

    daily_weather, fact_crop_yield = read_inputs(
        spark, args["SILVER_BUCKET"], args["GOLD_BUCKET"]
    )

    city_season_profile = build_city_season_profile(daily_weather)
    crop_profile = build_crop_profile(fact_crop_yield)

    crop_profile = attach_feature_weights(
        spark,
        args["SILVER_BUCKET"],
        crop_profile,
        PIPELINE_CONFIG["MATCH_FEATURES"]
    )

    crop_weather_match = build_crop_weather_match(
        city_season_profile, crop_profile
    )

    crop_weather_match = add_audit_columns(
        crop_weather_match, args["JOB_NAME"]
    )

    gold_bucket = args["GOLD_BUCKET"]

    write_dataset(
        crop_weather_match,
        f"{gold_bucket}/crop_weather_match",
        "season"
    )

    write_dataset(
        crop_profile,
        f"{gold_bucket}/crop_profile"
    )

    write_dataset(
        city_season_profile,
        f"{gold_bucket}/city_season_weather_profile",
        "season"
    )

    cleanup(
        daily_weather,
        fact_crop_yield,
        city_season_profile,
        crop_profile,
        crop_weather_match
    )

    job.commit()

    logger.info("=" * 70)
    logger.info("CROP-WEATHER MATCH PIPELINE COMPLETED")
    logger.info("=" * 70)
    logger.info(
        f"Execution Time : {(time.time() - start_time) / 60:.2f} minutes"
    )


if __name__ == "__main__":
    main()