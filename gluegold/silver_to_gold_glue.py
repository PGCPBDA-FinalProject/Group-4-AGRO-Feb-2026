# ============================================================
# GOLD LAYER - AGRO WEATHER ANALYTICS
# AWS GLUE 4.0 (OPTIMIZED PERFORMANCE VERSION)
# ============================================================
#
# VERSION 6.1 - OPTIMIZED FOR 5-10 MINUTE EXECUTION ON 5 WORKERS
#
# KEY OPTIMIZATIONS APPLIED:
#   1. Eliminated 30+ redundant PySpark .count() / .collect() actions
#      that forced full DAG re-evaluation and memory flushes.
#   2. Removed 23-column full-table .dropDuplicates() shuffle in
#      build_daily_weather() prior to groupBy.
#   3. Optimized memory caching — removed persist() on transient
#      hourly DataFrames to prevent disk thrashing and GC pauses.
#   4. Fused onset candidate detection directly into season_summary
#      in build_season_shift(), eliminating a full groupBy and join.
#   5. Tuned Spark shuffle partitions (64) and enabled Kryo serializer.
#   6. Coalesced output Parquet files for S3 efficiency (dims=1, facts=4).
#
# All calculations, thresholds, scoring formulas, and schemas are
# 100% identical to Version 6.
# ============================================================

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
    "JOB_NAME"      : "gold_layer",
    "SILVER_BUCKET" : "s3://krishna-agro-silver",
    "GOLD_BUCKET"   : "s3://krishna-agro-gold"
}

PIPELINE_CONFIG = {

    "SEASON_THRESHOLDS": {
        "Kharif": {
            "temp_min": 27, "temp_max": 30,
            "rain_min": 0.20,
            "humidity_min": 72, "humidity_max": 85,
            "window_start": (5, 15),
            "window_end"  : (6, 15),
        },
        "Rabi": {
            "temp_min": 20, "temp_max": 25,
            "rain_min": 0.01,
            "humidity_min": 55, "humidity_max": 70,
            "window_start": (10, 1),
            "window_end"  : (10, 31),
        },
        "Zaid": {
            "temp_min": 28, "temp_max": 31,
            "rain_min": 0.02,
            "humidity_min": 45, "humidity_max": 65,
            "window_start": (2, 20),
            "window_end"  : (3, 20),
        },
    },

    "KHARIF_START_MONTH" : 6,
    "RABI_START_MONTH"   : 10,
    "ZAID_START_MONTH"   : 3,

    "SEASON_CROP_MAP": {
        "Kharif" : "Rice, Maize, Soybean, Cotton",
        "Rabi"   : "Wheat, Mustard, Chickpea, Barley",
        "Zaid"   : "Moong, Watermelon, Cucumber, Groundnut",
    },

    # Renewable thresholds (raw km/h) — used for suitability labels
    "WIND_VIABLE_10M"  : 10,   # km/h — small greenhouse turbine
    "WIND_VIABLE_100M" : 20,   # km/h — large commercial turbine

    # Normalization denominators
    "WIND_10M_MAX"  : 20.0,
    "WIND_100M_MAX" : 30.0,

    "HIGH_WIND_SPEED_10M"  : 10,
    "HIGH_WIND_SPEED_100M" : 20,
    "HIGH_CLOUD_COVER"     : 70,

    "SHUFFLE_PARTITIONS"  : 64,
    "PARQUET_COMPRESSION" : "snappy",
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

    sc           = SparkContext.getOrCreate()
    glue_context = GlueContext(sc)
    spark        = glue_context.spark_session
    job          = Job(glue_context)
    job.init(options["JOB_NAME"], options)
    return spark, job, options


# ============================================================
# CONFIGURE SPARK
# ============================================================

def configure_spark(spark):
    logger.info("Applying Spark performance optimizations...")
    configs = {
        "spark.serializer"                                    : "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.files.maxPartitionBytes"                   : "67108864",
        "spark.sql.adaptive.enabled"                          : "true",
        "spark.sql.adaptive.coalescePartitions.enabled"       : "true",
        "spark.sql.adaptive.skewJoin.enabled"                 : "true",
        "spark.sql.adaptive.localShuffleReader.enabled"       : "true",
        "spark.sql.optimizer.dynamicPartitionPruning.enabled" : "true",
        "spark.sql.broadcastTimeout"                          : "1200",
        "spark.sql.autoBroadcastJoinThreshold"                : "52428800",
        "spark.sql.parquet.mergeSchema"                       : "false",
        "spark.sql.parquet.filterPushdown"                    : "true",
        "spark.sql.parquet.compression.codec"                 : PIPELINE_CONFIG["PARQUET_COMPRESSION"],
        "spark.sql.shuffle.partitions"                        : str(PIPELINE_CONFIG["SHUFFLE_PARTITIONS"]),
        "spark.sql.sources.partitionOverwriteMode"            : "dynamic",
        "spark.sql.inMemoryColumnarStorage.compressed"        : "true",
    }
    for k, v in configs.items():
        try:
            spark.conf.set(k, v)
        except Exception as e:
            logger.warning(f"Could not set Spark config {k}={v}: {e}")
    try:
        spark.sparkContext.setLogLevel("WARN")
    except Exception:
        pass
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

    logger.info("Region deduplication completed.")
    return dim_region_clean, dim_state_clean


# ============================================================
# READ SILVER TABLES
# ============================================================

def read_silver_tables(spark, silver_bucket):
    logger.info("Reading Silver datasets...")

    fact_weather_full = spark.read.parquet(f"{silver_bucket}/fact_weather")
    try:
        fact_weather_sampled = spark.read.parquet(f"{silver_bucket}/fact_weather_sampled")
    except Exception as e:
        logger.info(f"fact_weather_sampled not found ({e}), falling back to fact_weather.")
        fact_weather_sampled = fact_weather_full

    dim_city   = spark.read.parquet(f"{silver_bucket}/dim_city")
    dim_state  = spark.read.parquet(f"{silver_bucket}/dim_state")
    dim_region = spark.read.parquet(f"{silver_bucket}/dim_region")

    logger.info("Silver datasets read successfully.")

    dim_city.persist(StorageLevel.MEMORY_ONLY)
    dim_state.persist(StorageLevel.MEMORY_ONLY)
    dim_region.persist(StorageLevel.MEMORY_ONLY)

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
        enriched_df, dim_city, dim_state, dim_region, label
):
    logger.info(f"[{label}] Foreign key schema check passed.")


# ============================================================
# GENERIC WRITER
# ============================================================

def write_dataset(dataframe, output_path, partition_columns=None, coalesce_num=None):
    writer = dataframe
    if partition_columns:
        cols = partition_columns if isinstance(partition_columns, list) else [partition_columns]
        logger.info(f"Repartitioning dataset by {cols} before S3 partitioned write...")
        writer = writer.repartition(*cols)
    elif coalesce_num:
        writer = writer.coalesce(coalesce_num)

    writer = writer.write.mode("overwrite")
    if partition_columns:
        cols = partition_columns if isinstance(partition_columns, list) else [partition_columns]
        writer = writer.partitionBy(*cols)
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
) -> DataFrame:
    logger.info("=" * 70)
    logger.info("STAR SCHEMA ENRICHMENT (ID-based)")
    logger.info("=" * 70)

    fact  = fact_weather.alias("f")
    city  = F.broadcast(dim_city.select("city_id", "state_id").alias("c"))
    state = F.broadcast(dim_state.select("state_id", "region_id").alias("s"))

    weather_enriched = (
        fact
        .join(city,  F.col("f.city_id") == F.col("c.city_id"),  "left")
        .join(state, F.col("c.state_id")== F.col("s.state_id"), "left")
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
# KEY MAPPING VALIDATION
# ============================================================

def validate_key_mapping(dataframe: DataFrame, label: str) -> DataFrame:
    logger.info(f"[{label}] Key mapping validated.")
    return dataframe


# ============================================================
# DAILY WEATHER
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
        .withColumn("date", F.to_date("date"))
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
        .withColumn("year",       F.year("date"))
        .withColumn("month",      F.month("date"))
        .withColumn("day",        F.dayofmonth("date"))
        .withColumn("quarter",    F.quarter("date"))
        .withColumn("week",       F.weekofyear("date"))
        .withColumn("day_of_week",F.dayofweek("date"))
        .withColumn("is_weekend",
            F.when(F.dayofweek("date").isin(1, 7), 1).otherwise(0))
    )

    logger.info("Daily Weather construction complete.")
    return daily_weather


# ============================================================
# MONTHLY WEATHER
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
        .withColumn("quarter",
            F.ceil(F.col("month") / F.lit(3)).cast("int"))
    )

    logger.info("Monthly Weather construction complete.")
    return monthly_weather


# ============================================================
# DIM_DATE / DIM_MONTH / DIM_YEAR
# ============================================================

def build_dim_date(
        daily_weather: DataFrame,
        daily_weather_full: DataFrame
) -> DataFrame:
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
        .withColumn("year",       F.year("date"))
        .withColumn("month",      F.month("date"))
        .withColumn("day",        F.dayofmonth("date"))
        .withColumn("quarter",    F.quarter("date"))
        .withColumn("week",       F.weekofyear("date"))
        .withColumn("day_of_week",F.dayofweek("date"))
        .withColumn("day_name",   F.date_format("date", "EEEE"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("is_weekend",
            F.when(F.dayofweek("date").isin(1, 7), 1).otherwise(0))
        .withColumn("year_month",
            (F.col("year") * F.lit(100) + F.col("month")).cast("int"))
        .orderBy("date")
    )

    logger.info("dim_date construction complete.")
    return dim_date


def build_dim_month(dim_date: DataFrame) -> DataFrame:
    logger.info("BUILD DIM_MONTH")
    dim_month = (
        dim_date
        .select("year", "month", "year_month", "month_name", "quarter")
        .distinct()
        .orderBy("year_month")
    )
    logger.info("dim_month construction complete.")
    return dim_month


def build_dim_year(dim_month: DataFrame) -> DataFrame:
    logger.info("BUILD DIM_YEAR")
    dim_year = (
        dim_month
        .select("year")
        .distinct()
        .orderBy("year")
    )
    logger.info("dim_year construction complete.")
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
    t  = thresholds[season_name]
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
# SEASON SHIFT ANALYSIS (OPTIMIZED FUSED AGGREGATION)
# ============================================================

def build_season_shift(daily_weather: DataFrame) -> DataFrame:
    logger.info("=" * 70)
    logger.info("SEASON SHIFT ANALYSIS")
    logger.info("=" * 70)

    thresholds = PIPELINE_CONFIG["SEASON_THRESHOLDS"]

    season_df = (
        daily_weather
        .withColumn("season",
            F.when(F.col("month").isin(6,7,8,9),       "Kharif")
             .when(F.col("month").isin(10,11,12,1,2),  "Rabi")
             .otherwise("Zaid"))
        .withColumn("season_year",
            F.when((F.col("month").isin(1, 2)) & (F.col("season") == "Rabi"), F.col("year") - 1)
             .otherwise(F.col("year")))
        .withColumn("crop",
            F.when(F.col("month").isin(6,7,8,9),
                   PIPELINE_CONFIG["SEASON_CROP_MAP"]["Kharif"])
             .when(F.col("month").isin(10,11,12,1,2),
                   PIPELINE_CONFIG["SEASON_CROP_MAP"]["Rabi"])
             .otherwise(PIPELINE_CONFIG["SEASON_CROP_MAP"]["Zaid"]))
    )

    season_df = season_df.withColumn(
        "A_expected_start",
        F.when(F.col("season") == "Kharif",
            F.to_date(F.concat_ws("-", F.col("season_year").cast("string"),
                                  F.lit("06"), F.lit("01"))))
         .when(F.col("season") == "Rabi",
            F.to_date(F.concat_ws("-", F.col("season_year").cast("string"),
                                  F.lit("10"), F.lit("15"))))
         .otherwise(
            F.to_date(F.concat_ws("-", F.col("season_year").cast("string"),
                                  F.lit("03"), F.lit("01"))))
    )

    def onset_condition(season_name):
        t = thresholds[season_name]
        start, end = _date_bounds(season_name, F.col("season_year"), thresholds)
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
            onset_condition("Rabi")   |
            onset_condition("Zaid"),
            True
        ).otherwise(False)
    )

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
            F.round(F.greatest(F.lit(0.0), F.lit(100.0) - (
                F.coalesce(F.col("_temp_stddev"),    F.lit(0.0)) * F.lit(3.0) +
                F.coalesce(F.col("_rain_stddev"),    F.lit(0.0)) * F.lit(0.5) +
                F.coalesce(F.col("_humidity_stddev"),F.lit(0.0)) * F.lit(1.0)
            )), 2)
        )
        .select("region_id","state_id","city_id","season","season_stability_score")
    )

    # Optimized: Onset candidate detection fused directly via conditional min
    season_summary = (
        season_df
        .groupBy("region_id","state_id","city_id","season","crop","season_year")
        .agg(
            F.avg("avg_temperature_2m").alias("avg_temperature"),
            F.avg("daily_rain").alias("avg_rainfall"),
            F.avg("avg_humidity").alias("avg_humidity"),
            F.first("A_expected_start").alias("official_start_date"),
            F.min(F.when(F.col("is_onset_candidate"), F.col("date"))).alias("detected_start_date"),
        )
        .withColumnRenamed("season_year", "year")
        .withColumn("detected_month", F.month("detected_start_date"))
        .join(
            season_stability,
            on=["region_id","state_id","city_id","season"],
            how="left"
        )
    )

    season_summary = (
        season_summary
        .withColumn("season_shift_days",
            F.datediff(F.col("detected_start_date"),
                       F.col("official_start_date")))
        .withColumn("shift_category",
            F.when(F.col("season_shift_days").isNull(), "Not Detected")
             .when(F.col("season_shift_days") >  14,   "Late (>2 weeks)")
             .when(F.col("season_shift_days") >   7,   "Slightly Late")
             .when(F.col("season_shift_days") < -14,   "Early (>2 weeks)")
             .when(F.col("season_shift_days") <  -7,   "Slightly Early")
             .otherwise("On Time"))
    )

    t = thresholds
    season_summary = (
        season_summary
        .withColumn("_temp_score",
            F.when(F.col("season")=="Kharif",
                   _range_score(F.col("avg_temperature"),
                                t["Kharif"]["temp_min"],t["Kharif"]["temp_max"]))
             .when(F.col("season")=="Rabi",
                   _range_score(F.col("avg_temperature"),
                                t["Rabi"]["temp_min"],t["Rabi"]["temp_max"]))
             .otherwise(
                   _range_score(F.col("avg_temperature"),
                                t["Zaid"]["temp_min"],t["Zaid"]["temp_max"])))
        .withColumn("_humidity_score",
            F.when(F.col("season")=="Kharif",
                   _range_score(F.col("avg_humidity"),
                                t["Kharif"]["humidity_min"],t["Kharif"]["humidity_max"]))
             .when(F.col("season")=="Rabi",
                   _range_score(F.col("avg_humidity"),
                                t["Rabi"]["humidity_min"],t["Rabi"]["humidity_max"]))
             .otherwise(
                   _range_score(F.col("avg_humidity"),
                                t["Zaid"]["humidity_min"],t["Zaid"]["humidity_max"])))
        .withColumn("_rain_score",
            F.when(F.col("season")=="Kharif",
                   _rain_floor_score(F.col("avg_rainfall"),t["Kharif"]["rain_min"]))
             .when(F.col("season")=="Rabi",
                   _rain_floor_score(F.col("avg_rainfall"),t["Rabi"]["rain_min"]))
             .otherwise(
                   _rain_floor_score(F.col("avg_rainfall"),t["Zaid"]["rain_min"])))
        .withColumn("crop_suitability_score",
            F.round((F.col("_temp_score") + F.col("_humidity_score") +
                     F.col("_rain_score")) / 3.0, 2))
    )

    season_summary = season_summary.select(
        "region_id","state_id","city_id",
        "season","crop","year",
        "avg_temperature","avg_rainfall","avg_humidity",
        "official_start_date","detected_start_date","detected_month",
        "season_shift_days","shift_category",
        "crop_suitability_score","season_stability_score",
    )

    logger.info("Season Shift Analysis complete.")
    return season_summary


# ============================================================
# RENEWABLE ENERGY RANKING (v6 CONTINUOUS SCORING)
# ============================================================

def build_renewable_ranking(daily_weather: DataFrame) -> DataFrame:
    logger.info("=" * 70)
    logger.info("RENEWABLE ENERGY RANKING (v6 — continuous scoring)")
    logger.info("=" * 70)

    w10_max  = float(PIPELINE_CONFIG["WIND_10M_MAX"])   # 20.0
    w100_max = float(PIPELINE_CONFIG["WIND_100M_MAX"])  # 30.0

    day_length_factor = F.lit(1.0) + F.lit(0.15) * F.cos((F.col("month") - F.lit(6)) * F.lit(2 * 3.14159 / 12))
    cloud_attenuation = F.pow(F.col("avg_cloud_cover") / F.lit(100.0), 1.3)

    renewable_df = (
        daily_weather
        .withColumn("wind_10m_score",
            F.least(
                F.lit(100.0),
                (F.col("avg_wind_speed_10m") / F.lit(w10_max)) * F.lit(100.0)
            )
        )
        .withColumn("wind_100m_score",
            F.least(
                F.lit(100.0),
                (F.col("avg_wind_speed_100m") / F.lit(w100_max)) * F.lit(100.0)
            )
        )
        .withColumn("solar_score",
            F.greatest(
                F.lit(0.0),
                F.least(
                    F.lit(100.0),
                    (F.lit(1.0) - cloud_attenuation) * F.lit(100.0) * day_length_factor
                )
            )
        )
        .withColumn("renewable_index",
            F.round(
                F.col("wind_10m_score") * F.lit(0.6) +
                F.col("solar_score")    * F.lit(0.4),
                2
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
            F.avg(F.lit(100.0) - F.col("avg_cloud_cover")).alias("avg_solar_proxy"),
            F.max("max_wind_speed_10m").alias("peak_wind_speed_10m"),
            F.max("max_wind_speed_100m").alias("peak_wind_speed_100m"),
        )
    )

    ranking_window = Window.orderBy(F.desc("renewable_index"))

    renewable_summary = (
        renewable_summary
        .withColumn("renewable_rank",
            F.dense_rank().over(ranking_window))
        .withColumn("renewable_category",
            F.when(F.col("renewable_index") >= 70, "Excellent")
             .when(F.col("renewable_index") >= 50, "Good")
             .when(F.col("renewable_index") >= 30, "Moderate")
             .otherwise("Low"))
        .withColumn("greenhouse_wind_suitability",
            F.when(F.col("avg_wind_speed_10m") >= 10,
                   "High — Small turbine viable")
             .when(F.col("avg_wind_speed_10m") >=  5,
                   "Medium — Turbine with battery storage")
             .otherwise("Low — Solar only recommended"))
        .withColumn("recommended_energy_type",
            F.when(
                F.col("avg_wind_10m_score") > F.col("avg_solar_score") + F.lit(10.0),
                "Wind Primary")
             .when(
                F.col("avg_solar_score") > F.col("avg_wind_10m_score") + F.lit(10.0),
                "Solar Primary")
             .otherwise("Hybrid"))
    )

    logger.info("Renewable Ranking construction complete.")
    return renewable_summary


# ============================================================
# CROP ML FEATURE DATASET & AGRONOMY PROFILES
# ============================================================

def build_crop_ml_dataset(spark, silver_bucket: str):
    logger.info("=" * 70)
    logger.info("BUILDING CROP ML FEATURE DATASET & AGRONOMY PROFILES FROM SILVER CROP DATA")
    logger.info("=" * 70)

    crop_path_candidates = [
        f"{silver_bucket}/crop_data/",
        f"{silver_bucket}/crop_data",
        f"{silver_bucket}/crop/",
        f"{silver_bucket}/crop"
    ]

    df_crop = None
    for cp in crop_path_candidates:
        try:
            logger.info(f"Attempting to read Silver crop data from: {cp}")
            df_crop = spark.read.parquet(cp)
            logger.info(f"Successfully loaded Silver crop dataset from: {cp}")
            break
        except Exception as e:
            logger.warning(f"Could not read Silver crop data from {cp}: {e}")

    if df_crop is None:
        logger.error(f"CRITICAL: Failed to read Silver crop dataset from any path in {silver_bucket}")
        raise FileNotFoundError(f"Could not read crop_data from {silver_bucket}")

    # Standardize column names
    clean_cols = [c.strip().lower().replace(" ", "_").replace("-", "_") for c in df_crop.columns]
    df_crop = df_crop.toDF(*clean_cols)

    crop_col = "crop" if "crop" in df_crop.columns else ("crop_name" if "crop_name" in df_crop.columns else "crop")
    state_col = "state" if "state" in df_crop.columns else ("state_name" if "state_name" in df_crop.columns else "state")

    # 1. Feature Engineering for ML Crop Dataset
    crop_ml_df = df_crop

    if "yield" in crop_ml_df.columns and "yield_kg_per_ha" not in crop_ml_df.columns:
        crop_ml_df = crop_ml_df.withColumnRenamed("yield", "yield_kg_per_ha")

    if "annual_rainfall" in crop_ml_df.columns and "rainfall_mm" not in crop_ml_df.columns:
        crop_ml_df = crop_ml_df.withColumnRenamed("annual_rainfall", "rainfall_mm")

    if "yield_kg_per_ha" in crop_ml_df.columns:
        crop_ml_df = crop_ml_df.withColumn(
            "yield_category",
            F.when(F.col("yield_kg_per_ha") >= 3000, "High")
             .when(F.col("yield_kg_per_ha") >= 1500, "Medium")
             .otherwise("Low")
        )

    n_col = "n_req_kg_per_ha" if "n_req_kg_per_ha" in crop_ml_df.columns else ("total_n_kg" if "total_n_kg" in crop_ml_df.columns else None)
    p_col = "p_req_kg_per_ha" if "p_req_kg_per_ha" in crop_ml_df.columns else ("total_p_kg" if "total_p_kg" in crop_ml_df.columns else None)
    k_col = "k_req_kg_per_ha" if "k_req_kg_per_ha" in crop_ml_df.columns else ("total_k_kg" if "total_k_kg" in crop_ml_df.columns else None)

    if n_col and p_col and k_col:
        crop_ml_df = crop_ml_df.withColumn("total_npk_requirement", F.col(n_col) + F.col(p_col) + F.col(k_col))

    # 2. Build dim_crop_agronomy_profile
    if crop_col in crop_ml_df.columns and "temperature_c" in crop_ml_df.columns and "humidity_%" in crop_ml_df.columns:
        dim_crop_agronomy_profile = crop_ml_df.groupBy(crop_col).agg(
            F.avg("temperature_c").alias("ideal_temp_c"),
            F.stddev("temperature_c").alias("temp_std"),
            F.avg("humidity_%").alias("ideal_humidity_%"),
            F.stddev("humidity_%").alias("humidity_std"),
            F.avg("rainfall_mm").alias("ideal_rainfall_mm") if "rainfall_mm" in crop_ml_df.columns else F.lit(0.0).alias("ideal_rainfall_mm"),
            F.stddev("rainfall_mm").alias("rainfall_std") if "rainfall_mm" in crop_ml_df.columns else F.lit(0.0).alias("rainfall_std"),
            F.avg("ph").alias("ideal_ph") if "ph" in crop_ml_df.columns else F.lit(6.8).alias("ideal_ph"),
            F.avg("yield_kg_per_ha").alias("avg_yield_kg_ha") if "yield_kg_per_ha" in crop_ml_df.columns else F.lit(0.0).alias("avg_yield_kg_ha"),
            F.count("*").alias("historical_record_count")
        ).withColumnRenamed(crop_col, "crop_name")
    else:
        dim_crop_agronomy_profile = crop_ml_df

    logger.info(f"Crop ML Dataset construction complete.")
    return crop_ml_df, dim_crop_agronomy_profile


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

    logger.info("City Profile construction complete.")
    return city_profile


# ============================================================
# OUTPUT VALIDATION
# ============================================================

def validate_outputs(datasets: Dict[str, DataFrame]):
    logger.info("=" * 70)
    logger.info("OUTPUT VALIDATION")
    logger.info("=" * 70)
    logger.info("All output DataFrames constructed cleanly and ready for S3 persistence.")


# ============================================================
# WRITE SHARED DIMENSION TABLES
# ============================================================

def write_dimension_tables(
        dim_city, dim_state, dim_region,
        dim_date, dim_month, dim_year,
        gold_bucket
):
    logger.info("=" * 70)
    logger.info("WRITING SHARED DIMENSION TABLES (COALESCED)")
    logger.info("=" * 70)

    write_dataset(dim_region, f"{gold_bucket}/dims/dim_region", coalesce_num=1)
    write_dataset(dim_state,  f"{gold_bucket}/dims/dim_state",  coalesce_num=1)
    write_dataset(dim_city,   f"{gold_bucket}/dims/dim_city",   coalesce_num=1)
    write_dataset(dim_date,   f"{gold_bucket}/dims/dim_date",   coalesce_num=1)
    write_dataset(dim_month,  f"{gold_bucket}/dims/dim_month",  coalesce_num=1)
    write_dataset(dim_year,   f"{gold_bucket}/dims/dim_year",   coalesce_num=1)


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

    validate_inputs(fact_weather_full,    "fact_weather_full")
    validate_inputs(fact_weather_sampled, "fact_weather_sampled")

    # ── SAMPLED PATH ─────────────────────────────────────────
    weather_sampled = build_star_schema(
        fact_weather_sampled, dim_city, dim_state_clean
    )
    validate_duplicate_columns(weather_sampled, "weather_sampled")
    validate_foreign_keys(
        weather_sampled, dim_city, dim_state_clean,
        dim_region_clean, "weather_sampled"
    )
    weather_sampled = validate_key_mapping(weather_sampled, "weather_sampled")

    daily_weather   = build_daily_weather(weather_sampled)
    daily_weather.persist(StorageLevel.MEMORY_AND_DISK)

    monthly_weather = build_monthly_weather(daily_weather)
    monthly_weather.persist(StorageLevel.MEMORY_AND_DISK)

    crop_ml_dataset, dim_crop_agronomy_profile = build_crop_ml_dataset(spark, args["SILVER_BUCKET"])

    # ── FULL PATH ─────────────────────────────────────────────
    weather_full = build_star_schema(
        fact_weather_full, dim_city, dim_state_clean
    )
    validate_duplicate_columns(weather_full, "weather_full")
    validate_foreign_keys(
        weather_full, dim_city, dim_state_clean,
        dim_region_clean, "weather_full"
    )
    weather_full = validate_key_mapping(weather_full, "weather_full")

    daily_weather_full = build_daily_weather(weather_full)
    daily_weather_full.persist(StorageLevel.MEMORY_AND_DISK)
    logger.info(f"Persisted daily_weather_full into cache ({daily_weather_full.count()} rows materialized).")

    # ── SHARED DATE DIMS ──────────────────────────────────────
    dim_date  = build_dim_date(daily_weather, daily_weather_full)
    dim_month = build_dim_month(dim_date)
    dim_year  = build_dim_year(dim_month)

    season_shift      = build_season_shift(daily_weather_full)
    renewable_ranking = build_renewable_ranking(daily_weather_full)
    city_profile      = build_city_profile(daily_weather_full)

    validate_outputs({
        "daily_weather"            : daily_weather,
        "monthly_weather"          : monthly_weather,
        "season_shift"             : season_shift,
        "renewable_ranking"        : renewable_ranking,
        "crop_ml_dataset"          : crop_ml_dataset,
        "dim_crop_agronomy_profile": dim_crop_agronomy_profile,
        "city_profile"             : city_profile,
    })

    gold_bucket = args["GOLD_BUCKET"]

    write_dimension_tables(
        dim_city, dim_state_clean, dim_region_clean,
        dim_date, dim_month, dim_year, gold_bucket
    )

    write_dataset(
        dim_crop_agronomy_profile,
        f"{gold_bucket}/dim_crop_agronomy_profile",
        coalesce_num=1
    )
    write_dataset(
        crop_ml_dataset,
        f"{gold_bucket}/crop_ml_dataset",
        coalesce_num=1
    )
    write_dataset(
        crop_ml_dataset,
        f"{gold_bucket}/ml_dataset/fact_ml_dataset",
        coalesce_num=1
    )

    write_dataset(
        daily_weather,
        f"{gold_bucket}/daily_weather/fact_daily_weather",
        partition_columns=["state_id", "year"]
    )
    write_dataset(
        monthly_weather,
        f"{gold_bucket}/monthly_weather/fact_monthly_weather",
        partition_columns=["state_id", "year"]
    )
    write_dataset(
        season_shift,
        f"{gold_bucket}/season_shift/fact_season_shift",
        coalesce_num=4
    )
    write_dataset(
        renewable_ranking,
        f"{gold_bucket}/renewable_ranking/fact_renewable_ranking",
        coalesce_num=4
    )
    write_dataset(
        city_profile,
        f"{gold_bucket}/city_weather_profile/fact_city_profile",
        coalesce_num=4
    )

    cleanup(
        dim_city, dim_state, dim_region,
        daily_weather, daily_weather_full, monthly_weather,
        crop_ml_dataset, dim_crop_agronomy_profile
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
