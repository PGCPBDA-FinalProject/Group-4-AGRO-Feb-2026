# ============================================================
# GOLD LAYER - AGRO WEATHER ANALYTICS
# AWS GLUE 4.0
# ============================================================
#
# FINAL CONSOLIDATED VERSION
#
# ------------------------------------------------------------
# WHAT CHANGED IN THIS VERSION
# ------------------------------------------------------------
#
# 1. OUTPUT FOLDER
#       Old : s3://agro-weather-data-lake3/gold
#       New : s3://agro-weather-data-lake3/gold_updated
#    (original gold/ output is left untouched)
#
# 2. DUAL SOURCE STRATEGY (Silver -> Gold)
#       fact_weather_sampled  ->  daily_weather, ml_dataset
#           (fast, dashboard/ML friendly, smaller volume)
#       fact_weather (full)   ->  season_shift, renewable_ranking,
#                                  city_weather_profile
#           (full history needed for accurate onset detection,
#            long-term wind/solar averages, and complete city stats)
#
# 3. PER-SUBJECT FOLDER LAYOUT (star schema per PS, shared dims)
#
#       gold_updated/
#       |
#       |-- dims/                        <- written ONCE
#       |     |-- dim_region/
#       |     |-- dim_state/
#       |     `-- dim_city/
#       |
#       |-- daily_weather/
#       |     `-- fact_daily_weather/    (partitioned: state_name)
#       |
#       |-- season_shift/
#       |     `-- fact_season_shift/     (no partition)
#       |
#       |-- renewable_ranking/
#       |     `-- fact_renewable_ranking/ (no partition)
#       |
#       |-- city_weather_profile/
#       |     `-- fact_city_profile/     (no partition)
#       |
#       `-- ml_dataset/
#             `-- fact_ml_dataset/       (partitioned: state_name)
#                 (not connected to Power BI)
#
#    NOTE: the 3 dim tables are written to gold_updated/dims/ only
#    once. Each subject area gets its own Glue Catalog database with
#    4 tables: dim_region, dim_state, dim_city (all pointing at the
#    SAME S3 path under dims/) + its own fact table. This keeps every
#    Power BI dataset self-contained (region -> state -> city -> fact)
#    without physically duplicating dimension data on S3.
#
# 4. REGION STANDARDIZATION (5 regions only)
#       Northern, North              -> North
#       Southern, South              -> South
#       Eastern, East, Northeast     -> East
#       Western, West                -> West
#       North Central, Central       -> Central
#    Applied once on dim_region right after read, so every downstream
#    join and the standalone dim_region table both use the same
#    5-value mapping. Silver is untouched.
#
# 5. ETL AUDIT COLUMNS REMOVED
#       etl_timestamp, etl_layer, etl_job are no longer added to any
#       Gold table. add_audit_columns() has been removed entirely.
#
# 6. SEASON SHIFT LOGIC SIMPLIFIED
#       Kept    : A_expected_start, B_actual_start,
#                 D_delay_days, shift_category
#       Removed : season_shift_days, season_shift_percent, total_days
#
# 7. PARTITIONING
#       daily_weather        -> state_name            (was state_name, year)
#       ml_dataset            -> state_name            (was state_name, year)
#       season_shift          -> no partition          (was year, season)
#       renewable_ranking     -> no partition           (unchanged)
#       city_weather_profile  -> no partition           (unchanged)
#
# ============================================================

import sys
import logging
import time

from typing import Dict, Tuple

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

logger = logging.getLogger("GoldLayer")


# ============================================================
# DEFAULT CONFIGURATION
# ============================================================

DEFAULT_ARGS = {

    "JOB_NAME": "gold_layer",

    "SILVER_BUCKET":
        "s3://silver2-s3/silver-data",

    # UPDATED OUTPUT LOCATION
    "GOLD_BUCKET":
        "s3://agro-weather-data-lake3/gold_updated"
}


PIPELINE_CONFIG = {

    # -----------------------------
    # Season Shift Thresholds
    # -----------------------------

    "RAIN_THRESHOLD_KHARIF": 50,
    "RAIN_THRESHOLD_RABI": 10,
    "RAIN_THRESHOLD_ZAID": 20,

    # -----------------------------
    # Expected Start Month (IMD)
    # -----------------------------

    "KHARIF_START_MONTH": 6,
    "RABI_START_MONTH": 10,
    "ZAID_START_MONTH": 3,

    # -----------------------------
    # Crop Mapping
    # -----------------------------

    "SEASON_CROP_MAP": {

        "Kharif":
            "Rice, Maize, Soybean, Cotton",

        "Rabi":
            "Wheat, Mustard, Chickpea, Barley",

        "Zaid":
            "Moong, Watermelon, Cucumber, Groundnut"
    },

    # -----------------------------
    # Renewable Thresholds
    # -----------------------------

    "HIGH_WIND_SPEED_10M": 10,
    "HIGH_WIND_SPEED_100M": 20,

    "HIGH_CLOUD_COVER": 70,

    # -----------------------------
    # Spark
    # -----------------------------

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

    configs = {

        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.localShuffleReader.enabled": "true",
        "spark.sql.optimizer.dynamicPartitionPruning.enabled": "true",
        "spark.sql.broadcastTimeout": "1200",
        "spark.sql.autoBroadcastJoinThreshold": "52428800",
        "spark.sql.parquet.mergeSchema": "false",
        "spark.sql.parquet.filterPushdown": "true",
        "spark.sql.parquet.compression.codec":
            PIPELINE_CONFIG["PARQUET_COMPRESSION"],
        "spark.sql.shuffle.partitions":
            str(PIPELINE_CONFIG["SHUFFLE_PARTITIONS"]),
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
    }

    for k, v in configs.items():
        spark.conf.set(k, v)

    spark.sparkContext.setLogLevel("WARN")

    logger.info("Spark configured successfully.")


# ============================================================
# REGION STANDARDIZATION (5 regions only)
# Applied once, directly on dim_region, right after read.
# ============================================================

def standardize_regions(dim_region: DataFrame) -> DataFrame:

    return (
        dim_region.withColumn(
            "region_name",
            F.when(
                F.col("region_name").isin("Northern", "North"),
                "North"
            )
            .when(
                F.col("region_name").isin("Southern", "South"),
                "South"
            )
            .when(
                F.col("region_name").isin("Eastern", "East", "Northeast"),
                "East"
            )
            .when(
                F.col("region_name").isin("Western", "West"),
                "West"
            )
            .when(
                F.col("region_name").isin("North Central", "Central"),
                "Central"
            )
            .otherwise(F.col("region_name"))
        )
    )


# ============================================================
# READ SILVER TABLES
# Reads BOTH fact_weather (full) and fact_weather_sampled
# ============================================================

def read_silver_tables(spark, silver_bucket):

    logger.info("Reading Silver datasets...")

    fact_weather_full = spark.read.parquet(
        f"{silver_bucket}/fact_weather"
    )

    fact_weather_sampled = spark.read.parquet(
        f"{silver_bucket}/fact_weather_sampled"
    )

    dim_city = spark.read.parquet(f"{silver_bucket}/dim_city")
    dim_state = spark.read.parquet(f"{silver_bucket}/dim_state")
    dim_region = spark.read.parquet(f"{silver_bucket}/dim_region")

    # Standardize to 5 regions once, at the source
    dim_region = standardize_regions(dim_region)

    # Avoid ambiguous column collision in star schema join
    if "state_name" in fact_weather_full.columns:
        fact_weather_full = fact_weather_full.withColumnRenamed(
            "state_name", "weather_state_name"
        )

    if "state_name" in fact_weather_sampled.columns:
        fact_weather_sampled = fact_weather_sampled.withColumnRenamed(
            "state_name", "weather_state_name"
        )

    logger.info(f"Fact (full) Records    : {fact_weather_full.count():,}")
    logger.info(f"Fact (sampled) Records : {fact_weather_sampled.count():,}")
    logger.info(f"City Records           : {dim_city.count():,}")
    logger.info(f"State Records          : {dim_state.count():,}")
    logger.info(f"Region Records         : {dim_region.count():,}")

    dim_city.persist(StorageLevel.MEMORY_AND_DISK)
    dim_state.persist(StorageLevel.MEMORY_AND_DISK)
    dim_region.persist(StorageLevel.MEMORY_AND_DISK)

    logger.info("Dimension tables cached.")

    return (
        fact_weather_full,
        fact_weather_sampled,
        dim_city,
        dim_state,
        dim_region
    )


# ============================================================
# INPUT VALIDATION
# ============================================================

def validate_inputs(fact_weather: DataFrame, label: str):

    required_columns = [
        "city_id", "date",
        "temperature_2m", "relative_humidity_2m",
        "precipitation", "rain",
        "pressure_msl", "cloud_cover",
        "wind_speed_10m", "wind_speed_100m"
    ]

    missing = [c for c in required_columns if c not in fact_weather.columns]

    if missing:
        raise Exception(f"[{label}] Missing columns : {missing}")

    logger.info(f"[{label}] Input validation successful.")


# ============================================================
# GENERIC WRITER
# ============================================================

def write_dataset(dataframe, output_path, partition_columns=None):

    writer = (
    dataframe.write
    .mode("overwrite")
    .option("compression", "snappy")
    )

    if partition_columns:
        if isinstance(partition_columns, list):
            writer = writer.partitionBy(*partition_columns)
        else:
            writer = writer.partitionBy(partition_columns)

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
# STAR SCHEMA ENRICHMENT
# ============================================================

def build_star_schema(
        fact_weather: DataFrame,
        dim_city: DataFrame,
        dim_state: DataFrame,
        dim_region: DataFrame
) -> DataFrame:

    logger.info("=" * 70)
    logger.info("STAR SCHEMA ENRICHMENT")
    logger.info("=" * 70)

    fact = fact_weather.alias("f")
    city = F.broadcast(dim_city.alias("c"))
    state = F.broadcast(dim_state.alias("s"))
    region = F.broadcast(dim_region.alias("r"))

    weather_enriched = (
        fact
        .join(city, F.col("f.city_id") == F.col("c.city_id"), "left")
        .join(state, F.col("c.state_id") == F.col("s.state_id"), "left")
        .join(region, F.col("s.region_id") == F.col("r.region_id"), "left")
        .select(
            # Fact
            F.col("f.city_id"),
            F.col("f.date"),
            F.col("f.temperature_2m"),
            F.col("f.relative_humidity_2m"),
            F.col("f.dew_point_2m"),
            F.col("f.apparent_temperature"),
            F.col("f.precipitation"),
            F.col("f.rain"),
            F.col("f.snowfall"),
            F.col("f.snow_depth"),
            F.col("f.pressure_msl"),
            F.col("f.surface_pressure"),
            F.col("f.cloud_cover"),
            F.col("f.cloud_cover_low"),
            F.col("f.cloud_cover_mid"),
            F.col("f.cloud_cover_high"),
            F.col("f.wind_speed_10m"),
            F.col("f.wind_speed_100m"),
            F.col("f.wind_direction_10m"),
            F.col("f.wind_direction_100m"),
            F.col("f.wind_gusts_10m"),
            # City
            F.col("c.city_name"),
            F.col("c.district"),
            F.col("c.lat"),
            F.col("c.lng"),
            # State
            F.col("s.state_id"),
            F.col("s.state_name"),
            # Region (already standardized to 5 values)
            F.col("r.region_id"),
            F.col("r.region_name"),
        )
    )

    logger.info("Star Schema Join Completed.")
    return weather_enriched


# ============================================================
# DUPLICATE COLUMN VALIDATION
# ============================================================

def validate_duplicate_columns(dataframe: DataFrame, label: str):

    from collections import Counter

    dupes = [c for c, n in Counter(dataframe.columns).items() if n > 1]

    if dupes:
        raise Exception(f"[{label}] Duplicate columns found : {dupes}")

    logger.info(f"[{label}] No duplicate columns detected.")


# ============================================================
# DATA QUALITY VALIDATION
# ============================================================

def validate_dimension_mapping(dataframe: DataFrame, label: str) -> DataFrame:

    logger.info("=" * 70)
    logger.info(f"DATA QUALITY VALIDATION [{label}]")
    logger.info("=" * 70)

    null_city = dataframe.filter(F.col("city_name").isNull()).count()
    null_state = dataframe.filter(F.col("state_name").isNull()).count()
    null_region = dataframe.filter(F.col("region_name").isNull()).count()

    logger.info(f"Missing City   : {null_city}")
    logger.info(f"Missing State  : {null_state}")
    logger.info(f"Missing Region : {null_region}")

    if null_city:
        logger.warning("Some cities could not be mapped.")
    if null_state:
        logger.warning("Some states could not be mapped.")
    if null_region:
        logger.warning("Some regions could not be mapped.")

    dataframe.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(f"[{label}] Enriched Records : {dataframe.count():,}")

    return dataframe


# ============================================================
# DAILY WEATHER DATASET
# ============================================================

def build_daily_weather(weather_enriched: DataFrame) -> DataFrame:

    logger.info("=" * 70)
    logger.info("BUILD DAILY WEATHER")
    logger.info("=" * 70)

    daily_weather = (
        weather_enriched
        .filter(F.col("temperature_2m").isNotNull())
        .filter(F.col("relative_humidity_2m").isNotNull())
        .filter(F.col("city_id").isNotNull())
        .filter(F.col("date").isNotNull())
        .filter(F.col("temperature_2m").between(-50, 60))
        .filter(F.col("relative_humidity_2m").between(0, 100))
        .dropDuplicates()
    )

    logger.info(f"Records after cleaning : {daily_weather.count():,}")

    daily_weather = (
        daily_weather
        .groupBy(
            "city_id", "city_name", "district",
            "state_id", "state_name",
            "region_id", "region_name",
            "lat", "lng", "date"
        )
        .agg(
            F.avg("temperature_2m").alias("avg_temperature_2m"),
            F.max("temperature_2m").alias("max_temperature_2m"),
            F.min("temperature_2m").alias("min_temperature_2m"),
            F.avg("relative_humidity_2m").alias("avg_humidity"),
            F.sum("precipitation").alias("daily_precipitation"),
            F.sum("rain").alias("daily_rain"),
            F.avg("pressure_msl").alias("avg_pressure_msl"),
            F.avg("cloud_cover").alias("avg_cloud_cover"),
            F.avg("wind_speed_10m").alias("avg_wind_speed_10m"),
            F.avg("wind_speed_100m").alias("avg_wind_speed_100m"),
            F.max("wind_speed_10m").alias("max_wind_speed_10m"),
            F.max("wind_speed_100m").alias("max_wind_speed_100m"),
        )
    )

    daily_weather = (
        daily_weather
        .withColumn("date", F.to_date("date"))
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("week", F.weekofyear("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("date").isin(1, 7), 1).otherwise(0)
        )
    )

    daily_weather.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(f"Daily Weather Records : {daily_weather.count():,}")

    return daily_weather


# ============================================================
# SEASON SHIFT ANALYSIS
# Kept    : A_expected_start, B_actual_start,
#           D_delay_days, shift_category
# Removed : season_shift_days, season_shift_percent, total_days
# ============================================================

def build_season_shift(daily_weather: DataFrame) -> DataFrame:

    logger.info("=" * 70)
    logger.info("SEASON SHIFT ANALYSIS")
    logger.info("=" * 70)

    season_df = (
        daily_weather
        .withColumn(
            "season",
            F.when(F.col("month").isin(6, 7, 8, 9), "Kharif")
             .when(F.col("month").isin(10, 11, 12, 1, 2), "Rabi")
             .otherwise("Zaid")
        )
        .withColumn(
            "crop",
            F.when(F.col("month").isin(6, 7, 8, 9),
                   "Rice, Maize, Soybean, Cotton")
             .when(F.col("month").isin(10, 11, 12, 1, 2),
                   "Wheat, Mustard, Chickpea, Barley")
             .otherwise("Moong, Watermelon, Cucumber, Groundnut")
        )
    )

    season_df = (
        season_df
        .withColumn(
            "rainfall_status",
            F.when(
                (F.col("season") == "Kharif") &
                (F.col("daily_rain") < PIPELINE_CONFIG["RAIN_THRESHOLD_KHARIF"]),
                "Below Normal"
            )
            .when(
                (F.col("season") == "Rabi") &
                (F.col("daily_rain") < PIPELINE_CONFIG["RAIN_THRESHOLD_RABI"]),
                "Below Normal"
            )
            .when(
                (F.col("season") == "Zaid") &
                (F.col("daily_rain") < PIPELINE_CONFIG["RAIN_THRESHOLD_ZAID"]),
                "Below Normal"
            )
            .otherwise("Normal")
        )
        .withColumn(
            "temperature_status",
            F.when(F.col("avg_temperature_2m") > 35, "High")
             .when(F.col("avg_temperature_2m") < 15, "Low")
             .otherwise("Normal")
        )
        .withColumn(
            "season_shift",
            F.when(
                (F.col("rainfall_status") == "Below Normal") &
                (F.col("temperature_status") == "High"),
                "Likely"
            ).otherwise("No")
        )
    )

    logger.info("Season classification completed.")

    # Column A : IMD expected season start date
    season_df = (
        season_df
        .withColumn(
            "A_expected_start",
            F.when(
                F.col("season") == "Kharif",
                F.to_date(F.concat_ws(
                    "-", F.col("year").cast("string"), F.lit("06"), F.lit("01")
                ))
            )
            .when(
                F.col("season") == "Rabi",
                F.to_date(F.concat_ws(
                    "-", F.col("year").cast("string"), F.lit("10"), F.lit("15")
                ))
            )
            .otherwise(
                F.to_date(F.concat_ws(
                    "-", F.col("year").cast("string"), F.lit("03"), F.lit("01")
                ))
            )
        )
    )

    # Column B : Actual detected season start
    onset_candidates = (
        season_df
        .filter(F.col("season_shift") == "Likely")
        .filter(
            ((F.col("season") == "Kharif") & F.col("month").isin(6, 7)) |
            ((F.col("season") == "Rabi") & F.col("month").isin(10, 11)) |
            ((F.col("season") == "Zaid") & F.col("month").isin(3, 4))
        )
        .groupBy("region_name", "state_name", "city_name", "season", "year")
        .agg(F.min("date").alias("B_actual_start"))
    )

    # Season summary (season_shift_days / total_days removed)
    season_summary = (
        season_df
        .groupBy(
            "region_name", "state_name", "city_name",
            "season", "crop", "year"
        )
        .agg(
            F.avg("avg_temperature_2m").alias("avg_temperature"),
            F.avg("daily_rain").alias("avg_rainfall"),
            F.avg("avg_humidity").alias("avg_humidity"),
            F.first("A_expected_start").alias("A_expected_start"),
        )
    )

    season_summary = (
        season_summary
        .join(
            onset_candidates.select(
                "city_name", "season", "year", "B_actual_start"
            ),
            on=["city_name", "season", "year"],
            how="left"
        )
    )

    # Column D = B - A (days). Negative = Early, Positive = Late
    season_summary = (
        season_summary
        .withColumn(
            "D_delay_days",
            F.datediff(F.col("B_actual_start"), F.col("A_expected_start"))
        )
        .withColumn(
            "shift_category",
            F.when(F.col("D_delay_days").isNull(), "Not Detected")
             .when(F.col("D_delay_days") > 14, "Late (>2 weeks)")
             .when(F.col("D_delay_days") > 7, "Slightly Late")
             .when(F.col("D_delay_days") < -14, "Early (>2 weeks)")
             .when(F.col("D_delay_days") < -7, "Slightly Early")
             .otherwise("On Time")
        )
    )

    # Final explicit column set (season_shift_percent / total_days dropped)
    season_summary = season_summary.select(
        "region_name", "state_name", "city_name",
        "season", "crop", "year",
        "avg_temperature", "avg_rainfall", "avg_humidity",
        "A_expected_start", "B_actual_start",
        "D_delay_days", "shift_category"
    )

    season_summary.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(f"Season Records : {season_summary.count():,}")

    return season_summary


# ============================================================
# RENEWABLE ENERGY RANKING
# ============================================================

def build_renewable_ranking(daily_weather: DataFrame) -> DataFrame:

    logger.info("=" * 70)
    logger.info("RENEWABLE ENERGY RANKING")
    logger.info("=" * 70)

    renewable_df = (
        daily_weather
        .withColumn(
            "wind_10m_score",
            F.when(F.col("avg_wind_speed_10m") >= 20, 10)
             .when(F.col("avg_wind_speed_10m") >= 15, 8)
             .when(F.col("avg_wind_speed_10m") >= 10, 6)
             .when(F.col("avg_wind_speed_10m") >= 5, 4)
             .otherwise(2)
        )
        .withColumn(
            "wind_100m_score",
            F.when(F.col("avg_wind_speed_100m") >= 25, 10)
             .when(F.col("avg_wind_speed_100m") >= 20, 8)
             .when(F.col("avg_wind_speed_100m") >= 15, 6)
             .when(F.col("avg_wind_speed_100m") >= 10, 4)
             .otherwise(2)
        )
        .withColumn(
            "solar_score",
            F.when(F.col("avg_cloud_cover") <= 20, 10)
             .when(F.col("avg_cloud_cover") <= 40, 8)
             .when(F.col("avg_cloud_cover") <= 60, 6)
             .when(F.col("avg_cloud_cover") <= 80, 4)
             .otherwise(2)
        )
        .withColumn(
            "renewable_index",
            F.round(
                F.col("wind_10m_score") * 0.6 + F.col("solar_score") * 0.4, 2
            )
        )
    )

    renewable_summary = (
        renewable_df
        .groupBy("region_name", "state_name", "city_name", "lat", "lng")
        .agg(
            F.avg("avg_wind_speed_10m").alias("avg_wind_speed_10m"),
            F.avg("avg_wind_speed_100m").alias("avg_wind_speed_100m"),
            F.avg("avg_cloud_cover").alias("avg_cloud_cover"),
            F.avg("wind_10m_score").alias("avg_wind_10m_score"),
            F.avg("wind_100m_score").alias("avg_wind_100m_score"),
            F.avg("solar_score").alias("avg_solar_score"),
            F.avg("renewable_index").alias("renewable_index"),
            F.avg(100.0 - F.col("avg_cloud_cover")).alias("avg_solar_proxy"),
        )
    )

    ranking_window = Window.orderBy(F.desc("renewable_index"))

    renewable_summary = (
        renewable_summary
        .withColumn("renewable_rank", F.dense_rank().over(ranking_window))
        .withColumn(
            "renewable_category",
            F.when(F.col("renewable_index") >= 8, "Excellent")
             .when(F.col("renewable_index") >= 6, "Good")
             .when(F.col("renewable_index") >= 4, "Moderate")
             .otherwise("Low")
        )
        .withColumn(
            "greenhouse_wind_suitability",
            F.when(F.col("avg_wind_speed_10m") >= 10,
                   "High -- Small turbine viable")
             .when(F.col("avg_wind_speed_10m") >= 5,
                   "Medium -- Turbine with battery storage")
             .otherwise("Low -- Solar only recommended")
        )
    )

    renewable_summary.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(f"Renewable Records : {renewable_summary.count():,}")

    return renewable_summary


# ============================================================
# ML FEATURE DATASET (built from sampled daily_weather)
# ============================================================

def build_ml_dataset(daily_weather: DataFrame) -> DataFrame:

    logger.info("=" * 70)
    logger.info("ML FEATURE DATASET")
    logger.info("=" * 70)

    ml_df = (
        daily_weather
        .withColumn(
            "heavy_rain",
            F.when(F.col("daily_rain") >= 100, 1).otherwise(0)
        )
        .withColumn(
            "high_temperature",
            F.when(F.col("avg_temperature_2m") >= 35, 1).otherwise(0)
        )
        .withColumn(
            "high_wind",
            F.when(
                F.col("avg_wind_speed_10m") >=
                PIPELINE_CONFIG["HIGH_WIND_SPEED_10M"], 1
            ).otherwise(0)
        )
        .withColumn(
            "high_cloud",
            F.when(
                F.col("avg_cloud_cover") >=
                PIPELINE_CONFIG["HIGH_CLOUD_COVER"], 1
            ).otherwise(0)
        )
        .withColumn(
            "extreme_weather",
            F.when(
                (F.col("heavy_rain") == 1) |
                (F.col("high_temperature") == 1) |
                (F.col("high_wind") == 1), 1
            ).otherwise(0)
        )
    )

    ml_df.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(f"ML Dataset Records : {ml_df.count():,}")

    return ml_df


# ============================================================
# CITY WEATHER PROFILE
# ============================================================

def build_city_profile(daily_weather: DataFrame) -> DataFrame:

    logger.info("=" * 70)
    logger.info("CITY WEATHER PROFILE")
    logger.info("=" * 70)

    city_profile = (
        daily_weather
        .groupBy("region_name", "state_name", "city_name", "lat", "lng")
        .agg(
            F.avg("avg_temperature_2m").alias("avg_temperature"),
            F.avg("avg_humidity").alias("avg_humidity"),
            F.avg("daily_rain").alias("avg_rainfall"),
            F.avg("avg_pressure_msl").alias("avg_pressure"),
            F.avg("avg_cloud_cover").alias("avg_cloud_cover"),
            F.avg("avg_wind_speed_10m").alias("avg_wind_speed_10m"),
            F.avg("avg_wind_speed_100m").alias("avg_wind_speed_100m"),
            F.max("max_temperature_2m").alias("highest_temperature"),
            F.min("min_temperature_2m").alias("lowest_temperature"),
            F.countDistinct("date").alias("observation_days"),
        )
    )

    city_profile.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(f"City Profile Records : {city_profile.count():,}")

    return city_profile


# ============================================================
# OUTPUT VALIDATION
# ============================================================

def validate_outputs(datasets: Dict[str, DataFrame]):

    logger.info("=" * 70)
    logger.info("OUTPUT VALIDATION")
    logger.info("=" * 70)

    for name, df in datasets.items():
        count = df.count()
        logger.info(f"{name:<30} : {count:,}")
        if count == 0:
            raise Exception(f"{name} is empty.")

    logger.info("Output validation successful.")


# ============================================================
# WRITE SHARED DIMENSION TABLES (written ONCE)
# ============================================================

def write_dimension_tables(dim_city, dim_state, dim_region, gold_bucket):

    logger.info("=" * 70)
    logger.info("WRITING SHARED DIMENSION TABLES")
    logger.info("=" * 70)

    write_dataset(dim_region, f"{gold_bucket}/dims/dim_region")
    write_dataset(dim_state, f"{gold_bucket}/dims/dim_state")
    write_dataset(dim_city, f"{gold_bucket}/dims/dim_city")


# ============================================================
# MAIN
# ============================================================

def main():

    start_time = time.time()

    spark, job, args = initialize_glue()
    configure_spark(spark)

    (
        fact_weather_full,
        fact_weather_sampled,
        dim_city,
        dim_state,
        dim_region
    ) = read_silver_tables(spark, args["SILVER_BUCKET"])

    validate_inputs(fact_weather_full, "fact_weather_full")
    validate_inputs(fact_weather_sampled, "fact_weather_sampled")

    # ---- Sampled path: daily_weather + ml_dataset ----
    weather_sampled = build_star_schema(
        fact_weather_sampled, dim_city, dim_state, dim_region
    )
    validate_duplicate_columns(weather_sampled, "weather_sampled")
    weather_sampled = validate_dimension_mapping(weather_sampled, "weather_sampled")

    daily_weather = build_daily_weather(weather_sampled)
    ml_dataset = build_ml_dataset(daily_weather)

    # ---- Full path: season_shift + renewable_ranking + city_profile ----
    weather_full = build_star_schema(
        fact_weather_full, dim_city, dim_state, dim_region
    )
    validate_duplicate_columns(weather_full, "weather_full")
    weather_full = validate_dimension_mapping(weather_full, "weather_full")

    daily_weather_full = build_daily_weather(weather_full)

    season_shift = build_season_shift(daily_weather_full)
    renewable_ranking = build_renewable_ranking(daily_weather_full)
    city_profile = build_city_profile(daily_weather_full)

    validate_outputs({
        "daily_weather": daily_weather,
        "season_shift": season_shift,
        "renewable_ranking": renewable_ranking,
        "ml_dataset": ml_dataset,
        "city_profile": city_profile,
    })

    gold_bucket = args["GOLD_BUCKET"]

    # Shared dims -> written once
    write_dimension_tables(dim_city, dim_state, dim_region, gold_bucket)

    # Daily Weather -> partitioned by state_name only
    write_dataset(
        daily_weather,
        f"{gold_bucket}/daily_weather/fact_daily_weather",
        partition_columns="state_name"
    )

    # Season Shift -> no partition
    write_dataset(
        season_shift,
        f"{gold_bucket}/season_shift/fact_season_shift"
    )

    # Renewable Ranking -> no partition
    write_dataset(
        renewable_ranking,
        f"{gold_bucket}/renewable_ranking/fact_renewable_ranking"
    )

    # ML Dataset -> partitioned by state_name only, not connected to Power BI
    write_dataset(
        ml_dataset,
        f"{gold_bucket}/ml_dataset/fact_ml_dataset",
        partition_columns="state_name"
    )

    # City Weather Profile -> no partition
    write_dataset(
        city_profile,
        f"{gold_bucket}/city_weather_profile/fact_city_profile"
    )

    cleanup(
        dim_city, dim_state, dim_region,
        weather_sampled, weather_full,
        daily_weather, daily_weather_full,
        season_shift, renewable_ranking,
        ml_dataset, city_profile
    )

    job.commit()

    logger.info("=" * 70)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=" * 70)
    logger.info(
        f"Execution Time : {(time.time() - start_time) / 60:.2f} minutes"
    )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()