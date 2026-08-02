import sys
import logging
import time

from typing import Dict

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
        "s3://agro-weather-data-lake2/silver",

    "GOLD_BUCKET":
        "s3://agro-weather-data-lake3/gold_updated"
}


PIPELINE_CONFIG = {

    # Onset windows are now (start_month, start_day, end_month, end_day)
    # None of these wrap across a year boundary, so simple date
    # comparisons within the same calendar year work fine.
    "SEASON_THRESHOLDS": {

        "Kharif": {
            "temp_min": 27, "temp_max": 30,
            "rain_min": 0.20,
            "humidity_min": 72, "humidity_max": 85,
            "window_start": (5, 15),   # May 15
            "window_end": (6, 15),     # Jun 15
        },

        "Rabi": {
            "temp_min": 20, "temp_max": 25,
            "rain_min": 0.01,
            "humidity_min": 55, "humidity_max": 70,
            "window_start": (10, 1),   # Oct 1
            "window_end": (10, 31),    # Oct 31
        },

        "Zaid": {
            "temp_min": 28, "temp_max": 31,
            "rain_min": 0.02,
            "humidity_min": 45, "humidity_max": 65,
            "window_start": (2, 20),   # Feb 20
            "window_end": (3, 20),     # Mar 20
        },
    },

    "KHARIF_START_MONTH": 6,
    "RABI_START_MONTH": 10,
    "ZAID_START_MONTH": 3,

    "SEASON_CROP_MAP": {
        "Kharif": "Rice, Maize, Soybean, Cotton",
        "Rabi": "Wheat, Mustard, Chickpea, Barley",
        "Zaid": "Moong, Watermelon, Cucumber, Groundnut",
    },

    "HIGH_WIND_SPEED_10M": 10,
    "HIGH_WIND_SPEED_100M": 20,
    "HIGH_CLOUD_COVER": 70,

    "SHUFFLE_PARTITIONS": 200,
    "PARQUET_COMPRESSION": "snappy",
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
# REGION DEDUPLICATION
# ============================================================

def deduplicate_regions(dim_region: DataFrame, dim_state: DataFrame):

    logger.info("Deduplicating region_id values by region_name...")

    canonical = (
        dim_region
        .groupBy("region_name")
        .agg(F.min("region_id").alias("canonical_region_id"))
    )

    region_mapping = (
        dim_region
        .join(canonical, on="region_name", how="left")
        .select("region_id", "canonical_region_id")
    )

    dim_state_clean = (
        dim_state.alias("st")
        .join(
            region_mapping.alias("m"),
            F.col("st.region_id") == F.col("m.region_id"),
            "left"
        )
        .select(
            F.col("st.state_id"),
            F.col("st.state_name"),
            F.col("m.canonical_region_id").alias("region_id"),
        )
    )

    dim_region_clean = (
        dim_region
        .join(canonical, on="region_name", how="left")
        .filter(F.col("region_id") == F.col("canonical_region_id"))
        .select(
            F.col("canonical_region_id").alias("region_id"),
            "region_name",
        )
        .dropDuplicates(["region_id"])
    )

    logger.info(
        f"Region rows before dedup : {dim_region.count()} | "
        f"after dedup : {dim_region_clean.count()}"
    )

    return dim_region_clean, dim_state_clean


# ============================================================
# READ SILVER TABLES
# ============================================================

def read_silver_tables(spark, silver_bucket):

    logger.info("Reading Silver datasets...")

    fact_weather_full = spark.read.parquet(f"{silver_bucket}/fact_weather")
    fact_weather_sampled = spark.read.parquet(
        f"{silver_bucket}/fact_weather_sampled"
    )

    dim_city = spark.read.parquet(f"{silver_bucket}/dim_city")
    dim_state = spark.read.parquet(f"{silver_bucket}/dim_state")
    dim_region = spark.read.parquet(f"{silver_bucket}/dim_region")

    logger.info(f"Fact (full) Records    : {fact_weather_full.count():,}")
    logger.info(f"Fact (sampled) Records : {fact_weather_sampled.count():,}")
    logger.info(
        f"Distinct cities (full)    : "
        f"{fact_weather_full.select('city_id').distinct().count():,}"
    )
    logger.info(
        f"Distinct cities (sampled) : "
        f"{fact_weather_sampled.select('city_id').distinct().count():,}"
    )
    logger.info(f"City Records   : {dim_city.count():,}")
    logger.info(f"State Records  : {dim_state.count():,}")
    logger.info(f"Region Records : {dim_region.count():,}")

    dim_city.persist(StorageLevel.MEMORY_AND_DISK)
    dim_state.persist(StorageLevel.MEMORY_AND_DISK)
    dim_region.persist(StorageLevel.MEMORY_AND_DISK)

    return (
        fact_weather_full,
        fact_weather_sampled,
        dim_city,
        dim_state,
        dim_region,
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
        "wind_speed_10m", "wind_speed_100m",
    ]

    missing = [c for c in required_columns if c not in fact_weather.columns]

    if missing:
        raise Exception(f"[{label}] Missing columns : {missing}")

    logger.info(f"[{label}] Input validation successful.")


# ============================================================
# FOREIGN KEY VALIDATION
# ============================================================

def validate_foreign_keys(
        enriched_df: DataFrame,
        dim_city: DataFrame,
        dim_state: DataFrame,
        dim_region: DataFrame,
        label: str
):

    orphan_city = (
        enriched_df.select("city_id").distinct()
        .join(dim_city.select("city_id"), "city_id", "left_anti")
        .count()
    )

    orphan_state = (
        enriched_df.select("state_id").distinct()
        .join(dim_state.select("state_id"), "state_id", "left_anti")
        .count()
    )

    orphan_region = (
        enriched_df.select("region_id").distinct()
        .join(dim_region.select("region_id"), "region_id", "left_anti")
        .count()
    )

    logger.info(
        f"[{label}] Orphan city_id : {orphan_city} | "
        f"orphan state_id : {orphan_state} | "
        f"orphan region_id : {orphan_region}"
    )

    if orphan_city or orphan_state or orphan_region:
        logger.warning(
            f"[{label}] Orphan foreign keys detected - "
            f"investigate the join logic before trusting this output."
        )


# ============================================================
# GENERIC WRITER
# ============================================================

def write_dataset(dataframe, output_path, partition_columns=None):

    writer = dataframe.write.mode("overwrite")

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
# STAR SCHEMA ENRICHMENT (IDs only)
# ============================================================

def build_star_schema(
        fact_weather: DataFrame,
        dim_city: DataFrame,
        dim_state: DataFrame,
) -> DataFrame:

    logger.info("=" * 70)
    logger.info("STAR SCHEMA ENRICHMENT (ID-based)")
    logger.info("=" * 70)

    fact = fact_weather.alias("f")

    city = F.broadcast(dim_city.select("city_id", "state_id").alias("c"))
    state = F.broadcast(dim_state.select("state_id", "region_id").alias("s"))

    weather_enriched = (
        fact
        .join(city, F.col("f.city_id") == F.col("c.city_id"), "left")
        .join(state, F.col("c.state_id") == F.col("s.state_id"), "left")
        .select(
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
            F.col("c.state_id"),
            F.col("s.region_id"),
        )
    )

    logger.info("Star Schema Join Completed (IDs only).")
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
# KEY MAPPING VALIDATION
# ============================================================

def validate_key_mapping(dataframe: DataFrame, label: str) -> DataFrame:

    logger.info("=" * 70)
    logger.info(f"KEY MAPPING VALIDATION [{label}]")
    logger.info("=" * 70)

    null_state = dataframe.filter(F.col("state_id").isNull()).count()
    null_region = dataframe.filter(F.col("region_id").isNull()).count()

    logger.info(f"Rows missing state_id  : {null_state}")
    logger.info(f"Rows missing region_id : {null_region}")

    if null_state:
        logger.warning("Some cities could not be mapped to a state.")
    if null_region:
        logger.warning("Some states could not be mapped to a region.")

    dataframe.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(f"[{label}] Enriched Records : {dataframe.count():,}")

    return dataframe


# ============================================================
# DAILY WEATHER DATASET  (FIX: date converted BEFORE groupBy)
# True daily grain - internal source of truth for season_shift,
# ml_dataset, dim_date, and the new monthly rollup. Still written to
# Gold as fact_daily_weather.
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
        # FIX: convert to a pure date BEFORE grouping, otherwise the
        # groupBy below groups on hourly timestamps, not calendar days
        .withColumn("date", F.to_date("date"))
        .dropDuplicates()
    )

    logger.info(f"Records after cleaning : {daily_weather.count():,}")

    daily_weather = (
        daily_weather
        .groupBy("city_id", "state_id", "region_id", "date")
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
    logger.info(
        f"Distinct cities in Daily Weather : "
        f"{daily_weather.select('city_id').distinct().count():,}"
    )
    logger.info(
        f"Distinct dates in Daily Weather : "
        f"{daily_weather.select('date').distinct().count():,} "
        f"(sanity check: this should be in the low thousands, not "
        f"millions - if it's huge, the date-before-groupby fix didn't "
        f"take effect)"
    )

    return daily_weather


# ============================================================
# MONTHLY WEATHER DATASET (NEW - Power BI-facing table)
# Rolled up from the corrected daily_weather. season_shift/ml_dataset
# still use daily_weather directly, not this table.
# ============================================================

def build_monthly_weather(daily_weather: DataFrame) -> DataFrame:

    logger.info("=" * 70)
    logger.info("BUILD MONTHLY WEATHER")
    logger.info("=" * 70)

    monthly_weather = (
        daily_weather
        .groupBy("city_id", "state_id", "region_id", "year", "month")
        .agg(
            F.avg("avg_temperature_2m").alias("avg_temperature_2m"),
            F.max("max_temperature_2m").alias("max_temperature_2m"),
            F.min("min_temperature_2m").alias("min_temperature_2m"),
            F.avg("avg_humidity").alias("avg_humidity"),
            F.sum("daily_precipitation").alias("monthly_precipitation"),
            F.sum("daily_rain").alias("monthly_rain"),
            F.avg("avg_pressure_msl").alias("avg_pressure_msl"),
            F.avg("avg_cloud_cover").alias("avg_cloud_cover"),
            F.avg("avg_wind_speed_10m").alias("avg_wind_speed_10m"),
            F.avg("avg_wind_speed_100m").alias("avg_wind_speed_100m"),
            F.max("max_wind_speed_10m").alias("max_wind_speed_10m"),
            F.max("max_wind_speed_100m").alias("max_wind_speed_100m"),
            F.countDistinct("date").alias("days_observed"),
        )
        .withColumn(
            "year_month",
            (F.col("year") * F.lit(100) + F.col("month")).cast("int")
        )
        .withColumn("quarter", F.ceil(F.col("month") / F.lit(3)).cast("int"))
    )

    monthly_weather.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(f"Monthly Weather Records : {monthly_weather.count():,}")
    logger.info(
        f"Distinct cities in Monthly Weather : "
        f"{monthly_weather.select('city_id').distinct().count():,}"
    )

    return monthly_weather


# ============================================================
# DIM_DATE (daily) / DIM_MONTH / DIM_YEAR
# Shared, conformed date dimensions written ONCE to gold_updated/dims/
# ============================================================

def build_dim_date(daily_weather: DataFrame, daily_weather_full: DataFrame) -> DataFrame:

    logger.info("=" * 70)
    logger.info("BUILD DIM_DATE")
    logger.info("=" * 70)

    dates_df = (
        daily_weather.select("date")
        .union(daily_weather_full.select("date"))
        .distinct()
    )

    dim_date = (
        dates_df
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("week", F.weekofyear("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("day_name", F.date_format("date", "EEEE"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn(
            "is_weekend",
            F.when(F.dayofweek("date").isin(1, 7), 1).otherwise(0)
        )
        .withColumn(
            "year_month",
            (F.col("year") * F.lit(100) + F.col("month")).cast("int")
        )
        .orderBy("date")
    )

    dim_date.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(f"dim_date Records : {dim_date.count():,}")

    return dim_date


def build_dim_month(dim_date: DataFrame) -> DataFrame:

    logger.info("=" * 70)
    logger.info("BUILD DIM_MONTH")
    logger.info("=" * 70)

    dim_month = (
        dim_date
        .select("year", "month", "year_month", "month_name", "quarter")
        .distinct()
        .orderBy("year_month")
    )

    dim_month.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(f"dim_month Records : {dim_month.count():,}")

    return dim_month


def build_dim_year(dim_month: DataFrame) -> DataFrame:

    logger.info("=" * 70)
    logger.info("BUILD DIM_YEAR")
    logger.info("=" * 70)

    dim_year = (
        dim_month
        .select("year")
        .distinct()
        .orderBy("year")
    )

    dim_year.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(f"dim_year Records : {dim_year.count():,}")

    return dim_year


# ============================================================
# SCORE HELPERS (used by Season Shift)
# ============================================================

def _range_score(col, lo, hi, penalty_per_unit=8.0):
    return F.when(
        col.between(lo, hi), F.lit(100.0)
    ).otherwise(
        F.greatest(
            F.lit(0.0),
            F.lit(100.0) -
            F.least(F.abs(col - lo), F.abs(col - hi)) * F.lit(penalty_per_unit)
        )
    )


def _rain_floor_score(col, minimum, penalty_per_unit=40.0):
    return F.when(
        col >= minimum, F.lit(100.0)
    ).otherwise(
        F.greatest(
            F.lit(0.0),
            F.lit(100.0) - (F.lit(minimum) - col) * F.lit(penalty_per_unit)
        )
    )


def _date_bounds(season_name, year_col, thresholds):
    """Builds real calendar-date start/end columns for a season's
    onset window, for a given row's year. None of the configured
    windows cross a year boundary, so this is a plain same-year
    comparison."""
    t = thresholds[season_name]
    sm, sd = t["window_start"]
    em, ed = t["window_end"]

    start = F.to_date(F.concat_ws(
        "-", year_col.cast("string"),
        F.lpad(F.lit(str(sm)), 2, "0"), F.lpad(F.lit(str(sd)), 2, "0")
    ))
    end = F.to_date(F.concat_ws(
        "-", year_col.cast("string"),
        F.lpad(F.lit(str(em)), 2, "0"), F.lpad(F.lit(str(ed)), 2, "0")
    ))
    return start, end


# ============================================================
# SEASON SHIFT ANALYSIS
# Grain: city x season x year (confirmed). Built from the TRUE daily
# daily_weather (not the monthly rollup) - onset detection needs
# day-level precision.
# ============================================================

def build_season_shift(daily_weather: DataFrame) -> DataFrame:

    logger.info("=" * 70)
    logger.info("SEASON SHIFT ANALYSIS")
    logger.info("=" * 70)

    thresholds = PIPELINE_CONFIG["SEASON_THRESHOLDS"]

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
                   PIPELINE_CONFIG["SEASON_CROP_MAP"]["Kharif"])
             .when(F.col("month").isin(10, 11, 12, 1, 2),
                   PIPELINE_CONFIG["SEASON_CROP_MAP"]["Rabi"])
             .otherwise(PIPELINE_CONFIG["SEASON_CROP_MAP"]["Zaid"])
        )
    )

    # Column A source: IMD expected season start date
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

    # Precise, day-level onset windows (replaces month.isin(...))
    def onset_condition(season_name):
        t = thresholds[season_name]
        start, end = _date_bounds(season_name, F.col("year"), thresholds)
        return (
            (F.col("season") == season_name) &
            F.col("date").between(start, end) &
            F.col("avg_temperature_2m").between(t["temp_min"], t["temp_max"]) &
            (F.col("daily_rain") >= t["rain_min"]) &
            F.col("avg_humidity").between(t["humidity_min"], t["humidity_max"])
        )

    season_df = season_df.withColumn(
        "is_onset_candidate",
        F.when(
            onset_condition("Kharif") |
            onset_condition("Rabi") |
            onset_condition("Zaid"),
            True
        ).otherwise(False)
    )

    logger.info("Season classification & onset flagging completed.")

    # detected_start_date: first qualifying day per (city, season, year)
    onset_candidates = (
        season_df
        .filter(F.col("is_onset_candidate"))
        .groupBy("region_id", "state_id", "city_id", "season", "year")
        .agg(F.min("date").alias("detected_start_date"))
        .withColumn("detected_month", F.month("detected_start_date"))
    )

    logger.info(
        f"Onset detected for : {onset_candidates.count():,} "
        f"(city, season, year) combinations"
    )

    # season_stability_score: variance of that city+season's daily
    # weather across its FULL history
    season_stability = (
        season_df
        .groupBy("region_id", "state_id", "city_id", "season")
        .agg(
            F.stddev("avg_temperature_2m").alias("_temp_stddev"),
            F.stddev("daily_rain").alias("_rain_stddev"),
            F.stddev("avg_humidity").alias("_humidity_stddev"),
        )
        .withColumn(
            "season_stability_score",
            F.round(
                F.greatest(
                    F.lit(0.0),
                    F.lit(100.0) - (
                        F.coalesce(F.col("_temp_stddev"), F.lit(0.0)) * F.lit(3.0) +
                        F.coalesce(F.col("_rain_stddev"), F.lit(0.0)) * F.lit(0.5) +
                        F.coalesce(F.col("_humidity_stddev"), F.lit(0.0)) * F.lit(1.0)
                    )
                ), 2
            )
        )
        .select(
            "region_id", "state_id", "city_id", "season",
            "season_stability_score"
        )
    )

    # Season summary - grain: city x season x YEAR (no month)
    season_summary = (
        season_df
        .groupBy(
            "region_id", "state_id", "city_id",
            "season", "crop", "year"
        )
        .agg(
            F.avg("avg_temperature_2m").alias("avg_temperature"),
            F.avg("daily_rain").alias("avg_rainfall"),
            F.avg("avg_humidity").alias("avg_humidity"),
            F.first("A_expected_start").alias("official_start_date"),
        )
    )

    season_summary = (
        season_summary
        .join(
            onset_candidates.select(
                "city_id", "season", "year", "detected_start_date",
                "detected_month"
            ),
            on=["city_id", "season", "year"],
            how="left"
        )
        .join(
            season_stability,
            on=["region_id", "state_id", "city_id", "season"],
            how="left"
        )
    )

    season_summary = (
        season_summary
        .withColumn(
            "season_shift_days",
            F.datediff(
                F.col("detected_start_date"), F.col("official_start_date")
            )
        )
        .withColumn(
            "shift_category",
            F.when(F.col("season_shift_days").isNull(), "Not Detected")
             .when(F.col("season_shift_days") > 14, "Late (>2 weeks)")
             .when(F.col("season_shift_days") > 7, "Slightly Late")
             .when(F.col("season_shift_days") < -14, "Early (>2 weeks)")
             .when(F.col("season_shift_days") < -7, "Slightly Early")
             .otherwise("On Time")
        )
    )

    t = thresholds

    season_summary = (
        season_summary
        .withColumn(
            "_temp_score",
            F.when(F.col("season") == "Kharif",
                   _range_score(F.col("avg_temperature"),
                                t["Kharif"]["temp_min"], t["Kharif"]["temp_max"]))
             .when(F.col("season") == "Rabi",
                   _range_score(F.col("avg_temperature"),
                                t["Rabi"]["temp_min"], t["Rabi"]["temp_max"]))
             .otherwise(
                   _range_score(F.col("avg_temperature"),
                                t["Zaid"]["temp_min"], t["Zaid"]["temp_max"]))
        )
        .withColumn(
            "_humidity_score",
            F.when(F.col("season") == "Kharif",
                   _range_score(F.col("avg_humidity"),
                                t["Kharif"]["humidity_min"], t["Kharif"]["humidity_max"]))
             .when(F.col("season") == "Rabi",
                   _range_score(F.col("avg_humidity"),
                                t["Rabi"]["humidity_min"], t["Rabi"]["humidity_max"]))
             .otherwise(
                   _range_score(F.col("avg_humidity"),
                                t["Zaid"]["humidity_min"], t["Zaid"]["humidity_max"]))
        )
        .withColumn(
            "_rain_score",
            F.when(F.col("season") == "Kharif",
                   _rain_floor_score(F.col("avg_rainfall"), t["Kharif"]["rain_min"]))
             .when(F.col("season") == "Rabi",
                   _rain_floor_score(F.col("avg_rainfall"), t["Rabi"]["rain_min"]))
             .otherwise(
                   _rain_floor_score(F.col("avg_rainfall"), t["Zaid"]["rain_min"]))
        )
        .withColumn(
            "crop_suitability_score",
            F.round(
                (F.col("_temp_score") + F.col("_humidity_score") +
                 F.col("_rain_score")) / 3.0, 2
            )
        )
    )

    season_summary = season_summary.select(
        "region_id", "state_id", "city_id",
        "season", "crop", "year",
        "avg_temperature", "avg_rainfall", "avg_humidity",
        "official_start_date", "detected_start_date", "detected_month",
        "season_shift_days", "shift_category",
        "crop_suitability_score", "season_stability_score",
    )

    season_summary.persist(StorageLevel.MEMORY_AND_DISK)

    total = season_summary.count()
    detected = season_summary.filter(
        F.col("shift_category") != "Not Detected"
    ).count()

    dupe_check = (
        season_summary.groupBy("city_id", "season", "year")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    shift_range = season_summary.agg(
        F.min("season_shift_days").alias("min_shift"),
        F.max("season_shift_days").alias("max_shift"),
    ).first()

    logger.info(f"Season Records       : {total:,}")
    logger.info(f"Onset Detected Rows  : {detected:,}")
    logger.info(f"Not Detected Rows    : {total - detected:,}")
    logger.info(
        f"Duplicate (city,season,year) combos : {dupe_check} "
        f"(must be 0 - confirms grain is correct)"
    )
    logger.info(
        f"season_shift_days range : "
        f"min={shift_range['min_shift']} max={shift_range['max_shift']} "
        f"(expect roughly -30 to +30; investigate A/B logic if not)"
    )

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
        .groupBy("region_id", "state_id", "city_id")
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
        .withColumn(
            "recommended_energy_type",
            F.when(
                F.col("avg_wind_10m_score") > F.col("avg_solar_score") + 1,
                "Wind"
            )
            .when(
                F.col("avg_solar_score") > F.col("avg_wind_10m_score") + 1,
                "Solar"
            )
            .otherwise("Hybrid")
        )
    )

    renewable_summary.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(f"Renewable Records : {renewable_summary.count():,}")
    logger.info(
        f"Distinct cities in Renewable Ranking : "
        f"{renewable_summary.select('city_id').distinct().count():,} "
        f"(expect ~4379, the count of cities with full weather data)"
    )

    return renewable_summary


# ============================================================
# ML FEATURE DATASET (built from TRUE daily_weather, not monthly)
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
        .groupBy("region_id", "state_id", "city_id")
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
    logger.info(
        f"Distinct cities in City Profile : "
        f"{city_profile.select('city_id').distinct().count():,}"
    )

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

def write_dimension_tables(
        dim_city, dim_state, dim_region,
        dim_date, dim_month, dim_year, gold_bucket
):

    logger.info("=" * 70)
    logger.info("WRITING SHARED DIMENSION TABLES")
    logger.info("=" * 70)

    write_dataset(dim_region, f"{gold_bucket}/dims/dim_region")
    write_dataset(dim_state, f"{gold_bucket}/dims/dim_state")
    write_dataset(dim_city, f"{gold_bucket}/dims/dim_city")
    write_dataset(dim_date, f"{gold_bucket}/dims/dim_date")
    write_dataset(dim_month, f"{gold_bucket}/dims/dim_month")
    write_dataset(dim_year, f"{gold_bucket}/dims/dim_year")


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
        dim_region,
    ) = read_silver_tables(spark, args["SILVER_BUCKET"])

    dim_region_clean, dim_state_clean = deduplicate_regions(
        dim_region, dim_state
    )

    validate_inputs(fact_weather_full, "fact_weather_full")
    validate_inputs(fact_weather_sampled, "fact_weather_sampled")

    # ---- SAMPLED PATH: daily_weather + monthly_weather + ml_dataset ----
    weather_sampled = build_star_schema(
        fact_weather_sampled, dim_city, dim_state_clean
    )
    validate_duplicate_columns(weather_sampled, "weather_sampled")
    validate_foreign_keys(
        weather_sampled, dim_city, dim_state_clean, dim_region_clean,
        "weather_sampled"
    )
    weather_sampled = validate_key_mapping(weather_sampled, "weather_sampled")

    daily_weather = build_daily_weather(weather_sampled)
    monthly_weather = build_monthly_weather(daily_weather)
    ml_dataset = build_ml_dataset(daily_weather)

    # ---- FULL PATH: season_shift + renewable_ranking + city_profile ----
    weather_full = build_star_schema(
        fact_weather_full, dim_city, dim_state_clean
    )
    validate_duplicate_columns(weather_full, "weather_full")
    validate_foreign_keys(
        weather_full, dim_city, dim_state_clean, dim_region_clean,
        "weather_full"
    )
    weather_full = validate_key_mapping(weather_full, "weather_full")

    daily_weather_full = build_daily_weather(weather_full)

    # Shared date dimensions
    dim_date = build_dim_date(daily_weather, daily_weather_full)
    dim_month = build_dim_month(dim_date)
    dim_year = build_dim_year(dim_month)

    season_shift = build_season_shift(daily_weather_full)
    renewable_ranking = build_renewable_ranking(daily_weather_full)
    city_profile = build_city_profile(daily_weather_full)

    validate_outputs({
        "daily_weather": daily_weather,
        "monthly_weather": monthly_weather,
        "season_shift": season_shift,
        "renewable_ranking": renewable_ranking,
        "ml_dataset": ml_dataset,
        "city_profile": city_profile,
    })

    gold_bucket = args["GOLD_BUCKET"]

    write_dimension_tables(
        dim_city, dim_state_clean, dim_region_clean,
        dim_date, dim_month, dim_year, gold_bucket
    )

    write_dataset(
        daily_weather,
        f"{gold_bucket}/daily_weather/fact_daily_weather",
        partition_columns="state_id"
    )

    write_dataset(
        monthly_weather,
        f"{gold_bucket}/monthly_weather/fact_monthly_weather",
        partition_columns="state_id"
    )

    write_dataset(
        season_shift,
        f"{gold_bucket}/season_shift/fact_season_shift"
    )

    write_dataset(
        renewable_ranking,
        f"{gold_bucket}/renewable_ranking/fact_renewable_ranking"
    )

    write_dataset(
        ml_dataset,
        f"{gold_bucket}/ml_dataset/fact_ml_dataset",
        partition_columns="state_id"
    )

    write_dataset(
        city_profile,
        f"{gold_bucket}/city_weather_profile/fact_city_profile"
    )

    cleanup(
        dim_city, dim_state, dim_region,
        weather_sampled, weather_full,
        daily_weather, daily_weather_full, monthly_weather,
        dim_date, dim_month, dim_year,
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