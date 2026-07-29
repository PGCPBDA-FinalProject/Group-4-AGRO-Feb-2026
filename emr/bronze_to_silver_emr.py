#!/usr/bin/env python3
"""
Bronze to Silver Weather Data Transformation Pipeline (AWS EMR / PySpark)
Transforms raw Bronze weather datasets into a normalized Silver Star Schema on AWS EMR.
"""

import sys
import time
import math
import logging
import argparse
import traceback
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, DoubleType
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("BronzeToSilverEMRJob")


def parse_arguments():
    parser = argparse.ArgumentParser(description="EMR PySpark Bronze to Silver Weather Transformation")
    parser.add_argument(
        "--bronze-bucket",
        type=str,
        default="s3://agro-weather-data/bronze",
        help="S3 URI or local path for Bronze layer weather data"
    )
    parser.add_argument(
        "--silver-bucket",
        type=str,
        default="s3://agro-weather-data-lake2/silver",
        help="S3 URI or local path for Silver layer target output"
    )
    parser.add_argument(
        "--crop-data-input-path",
        type=str,
        default=None,
        help="S3 URI or local path for Bronze Crop Data (defaults to {bronze-bucket}/crop_data)"
    )
    parser.add_argument(
        "--crop-data-output-path",
        type=str,
        default=None,
        help="S3 URI or local path for Silver Crop Data Parquet output (defaults to {silver-bucket}/crop_data)"
    )
    parser.add_argument(
        "--percentage",
        type=float,
        default=0.30,
        help="Sampling percentage per state (default: 0.30 for 30%)"
    )
    parser.add_argument(
        "--min-cities",
        type=int,
        default=15,
        help="Minimum number of cities per state sample floor (default: 15)"
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible stratified sampling (default: 42)"
    )
    return parser.parse_args()


def main():
    job_start_time = time.time()
    args = parse_arguments()

    BRONZE_BUCKET = args.bronze_bucket.rstrip('/')
    SILVER_BUCKET = args.silver_bucket.rstrip('/')
    CROP_DATA_INPUT_PATH = (args.crop_data_input_path or f"{BRONZE_BUCKET}/crop_data").rstrip('/')
    CROP_DATA_OUTPUT_PATH = (args.crop_data_output_path or f"{SILVER_BUCKET}/crop_data").rstrip('/')
    PERCENTAGE = args.percentage
    MIN_CITIES = args.min_cities
    SEED = args.seed

    logger.info("=================================================================")
    logger.info("        STARTING BRONZE TO SILVER EMR TRANSFORMATION JOB         ")
    logger.info("=================================================================")
    logger.info(f"Bronze Bucket Path     : {BRONZE_BUCKET}")
    logger.info(f"Silver Bucket Path     : {SILVER_BUCKET}")
    logger.info(f"Crop Data Input Path   : {CROP_DATA_INPUT_PATH}")
    logger.info(f"Crop Data Output Path  : {CROP_DATA_OUTPUT_PATH}")
    logger.info(f"Sampling Config        : Percentage={PERCENTAGE}, MinCities={MIN_CITIES}, Seed={SEED}")

    # Initialize PySpark Session with AQE, Kryo, and Parquet Output Committer optimizations
    try:
        logger.info("Initializing PySpark Session on EMR cluster...")
        spark = (
            SparkSession.builder
            .appName("BronzeToSilverEMRJob")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )
        logger.info(f"Spark Session initialized successfully. PySpark Version: {spark.version}")
    except Exception as e:
        logger.error("FATAL ERROR: Failed to initialize PySpark Session!")
        logger.error(traceback.format_exc())
        sys.exit(1)

    # Step 1: Master Lookup & Geospatial Enrichment
    step1_start = time.time()
    logger.info("\n" + "="*70)
    logger.info("[STEP 1/6] READING CITY MASTER & EXECUTING GEOSPATIAL ENRICHMENT")
    logger.info("="*70)

    city_master_paths = [
        f"{BRONZE_BUCKET}/weather/city_master.parquet",
        f"{BRONZE_BUCKET}/city_master.parquet",
        f"{BRONZE_BUCKET}/city_master/",
        f"{BRONZE_BUCKET}/weather/city_master/"
    ]
    city_master_df = None
    for cm_path in city_master_paths:
        try:
            logger.info(f"Attempting to read city_master from: {cm_path}")
            city_master_df = spark.read.parquet(cm_path)
            logger.info(f"Parquet read completed successfully from: {cm_path}")
            break
        except Exception as e:
            logger.warning(f"Path candidate failed ({cm_path}): {e}")

    if city_master_df is None:
        raise FileNotFoundError(f"Could not read city_master parquet from any candidate path in {BRONZE_BUCKET}")

    try:
        raw_cm_count = city_master_df.count()
        logger.info(f"Count completed: {raw_cm_count}")

        cm_pd = city_master_df.toPandas()
        logger.info("toPandas completed.")
    except Exception as e:
        logger.error("ERROR: Failed to process city_master parquet from path candidate")
        logger.error(f"Details: {e}")
        logger.error(traceback.format_exc())
        spark.stop()
        sys.exit(1)

    # Vectorized Spatial Join using GeoPandas
    try:
        import geopandas as gpd
        from shapely.geometry import Point
        import subprocess

        logger.info("GeoPandas detected. Loading district boundaries from S3...")

        geojson_s3_path = f"{BRONZE_BUCKET}/district_geojson/india_district.geojson"
        geojson_local_path = "/tmp/india_district.geojson"

        logger.info(f"Copying GeoJSON from S3: {geojson_s3_path}")

        subprocess.run(
            [
                "aws",
                "s3",
                "cp",
                geojson_s3_path,
                geojson_local_path
            ],
            check=True
        )

        logger.info(f"GeoJSON copied successfully to {geojson_local_path}")

        gdf_districts = gpd.read_file(geojson_local_path)
        logger.info(f"Successfully loaded {len(gdf_districts)} district polygons.")

        geometry = gpd.points_from_xy(cm_pd["lng"], cm_pd["lat"], crs="EPSG:4326")
        gdf_cities = gpd.GeoDataFrame(cm_pd, geometry=geometry, crs="EPSG:4326")

        try:
            joined_gdf = gpd.sjoin(
                gdf_cities,
                gdf_districts[["NAME_1", "NAME_2", "geometry"]],
                how="left",
                predicate="intersects"
            )
        except (TypeError, ValueError):
            joined_gdf = gpd.sjoin(
                gdf_cities,
                gdf_districts[["NAME_1", "NAME_2", "geometry"]],
                how="left",
                op="intersects"
            )

        # Deduplicate indices if a point touched shared polygon borders
        joined_gdf = joined_gdf[~joined_gdf.index.duplicated(keep="first")]

        states = joined_gdf["NAME_1"].tolist()
        districts = joined_gdf["NAME_2"].tolist()

        unmapped_positions = [i for i, state in enumerate(states) if pd.isna(state)]

        # Nearest-Neighbor Fallback
        if unmapped_positions:
            logger.info(f"Applying nearest-neighbor fallback for {len(unmapped_positions)} coastal/edge cities...")
            dp_3857 = gdf_districts.to_crs("EPSG:3857")
            unmapped_lngs = [cm_pd.iloc[p]["lng"] for p in unmapped_positions]
            unmapped_lats = [cm_pd.iloc[p]["lat"] for p in unmapped_positions]
            uc_geom = [Point(xy) for xy in zip(unmapped_lngs, unmapped_lats)]
            uc_3857 = gpd.GeoSeries(uc_geom, crs="EPSG:4326").to_crs("EPSG:3857")

            for p, city_point_3857 in zip(unmapped_positions, uc_3857):
                distances = dp_3857.geometry.distance(city_point_3857)
                min_idx = distances.idxmin()
                states[p] = dp_3857.loc[min_idx, "NAME_1"]
                districts[p] = dp_3857.loc[min_idx, "NAME_2"]

            logger.info("Nearest-neighbor fallback complete.")

        state_renames = {
            "Orissa": "Odisha",
            "Uttaranchal": "Uttarakhand",
            "Andaman and Nicobar": "Andaman & Nicobar",
            "Jammu and Kashmir": "Jammu & Kashmir"
        }
        cm_pd["state"] = pd.Series(states, index=cm_pd.index).replace(state_renames)
        cm_pd["district"] = pd.Series(districts, index=cm_pd.index)
        logger.info("Geospatial spatial join completed successfully.")

    except Exception as e:
        logger.error("Spatial join failed.")
        logger.error(traceback.format_exc())
        spark.stop()
        raise

    # Map regions
    region_map = {
        "Jammu & Kashmir": "Northern", "Ladakh": "Northern", "Himachal Pradesh": "Northern",
        "Punjab": "Northern", "Chandigarh": "Northern", "Uttarakhand": "Northern",
        "Haryana": "Northern", "Delhi": "Northern", "Rajasthan": "Northern",
        "Uttar Pradesh": "North Central",
        "Madhya Pradesh": "Central", "Chhattisgarh": "Central",
        "Bihar": "Eastern", "Jharkhand": "Eastern", "West Bengal": "Eastern", "Odisha": "Eastern",
        "Assam": "Northeast", "Sikkim": "Northeast", "Arunachal Pradesh": "Northeast",
        "Nagaland": "Northeast", "Manipur": "Northeast", "Mizoram": "Northeast",
        "Tripura": "Northeast", "Meghalaya": "Northeast",
        "Gujarat": "Western", "Maharashtra": "Western", "Goa": "Western",
        "Andhra Pradesh": "Southern", "Telangana": "Southern", "Karnataka": "Southern",
        "Tamil Nadu": "Southern", "Kerala": "Southern", "Puducherry": "Southern",
        "Lakshadweep": "Southern", "Andaman & Nicobar": "Southern"
    }
    cm_pd["region"] = cm_pd["state"].map(region_map).fillna("Other")

    raw_city_count = len(cm_pd)
    master_lookup_pd = cm_pd.drop_duplicates(subset=["city"], keep="first").copy()
    output_city_count = len(master_lookup_pd)
    distinct_city_count = master_lookup_pd["city"].nunique()
    duplicated_city_count = output_city_count - distinct_city_count

    logger.info("="*50)
    logger.info("GEOSPATIAL ENRICHMENT AUDIT & VALIDATION:")
    logger.info(f"  Input city count:            {raw_city_count}")
    logger.info(f"  Output city count:           {output_city_count}")
    logger.info(f"  Distinct city count:         {distinct_city_count}")
    logger.info(f"  Duplicated city records:     {duplicated_city_count}")
    logger.info("="*50)

    if duplicated_city_count > 0:
        raise ValueError(f"CRITICAL ERROR: master_lookup_pd contains {duplicated_city_count} duplicate city records!")

    master_lookup_df = spark.createDataFrame(master_lookup_pd)
    logger.info(f"[STEP 1 COMPLETE] Time elapsed: {time.time() - step1_start:.2f} seconds.")

    # Step 2: Star Schema Dimensions & Native PySpark Window Stratified Sampling
    step2_start = time.time()
    logger.info("\n" + "="*70)
    logger.info("[STEP 2/6] GENERATING DIMENSIONS & STRATIFIED PERCENTAGE SAMPLING")
    logger.info("="*70)

    try:
        w_region = Window.partitionBy().orderBy("region_name")
        dim_region = (
            master_lookup_df.select("region").distinct()
            .filter(F.col("region").isNotNull())
            .withColumnRenamed("region", "region_name")
            .withColumn("region_id", F.row_number().over(w_region))
            .select("region_id", "region_name")
        )

        w_state = Window.partitionBy().orderBy("state")
        dim_state = (
            master_lookup_df.select("state", "region")
            .filter(F.col("state").isNotNull())
            .groupBy("state")
            .agg(F.first("region").alias("region_name_tmp"))
            .join(F.broadcast(dim_region), F.col("region_name_tmp") == dim_region.region_name, "left")
            .withColumn("state_id", F.row_number().over(w_state))
            .select("state_id", F.col("state").alias("state_name"), "region_id")
        )

        w_city = Window.partitionBy().orderBy("city", "state")
        dim_city_base = (
            master_lookup_df
            .join(F.broadcast(dim_state), master_lookup_df.state == dim_state.state_name, "left")
            .withColumn("city_id", F.row_number().over(w_city))
            .select(
                "city_id",
                F.col("city").alias("city_name"),
                "state_id",
                "district",
                "lat",
                "lng"
            )
        )

        logger.info(f"Base dimensions constructed: {dim_region.count()} regions, {dim_state.count()} states, {dim_city_base.count():,} cities.")

        # Native PySpark Window-Based Stratified Sampling (Zero Driver Memory Pull!)
        w_sample_order = Window.partitionBy("state_id").orderBy(F.rand(SEED))
        w_state_count = Window.partitionBy("state_id")

        dim_city = (
            dim_city_base
            .withColumn("state_city_total", F.count("city_id").over(w_state_count))
            .withColumn("city_rank", F.row_number().over(w_sample_order))
            .withColumn("sample_floor", F.greatest(F.lit(MIN_CITIES), F.ceil(F.col("state_city_total") * F.lit(PERCENTAGE))))
            .withColumn("is_sampled", F.col("city_rank") <= F.col("sample_floor"))
            .drop("state_city_total", "city_rank", "sample_floor")
        )

        total_dim_city_count = dim_city.count()
        distinct_city_id_count = dim_city.select("city_id").distinct().count()
        duplicate_city_id_count = total_dim_city_count - distinct_city_id_count
        sampled_city_count = dim_city.filter(F.col("is_sampled") == True).count()

        logger.info("="*50)
        logger.info("DIM_CITY AUDIT & VALIDATION:")
        logger.info(f"  Input city count:            {output_city_count}")
        logger.info(f"  Output dim_city count:       {total_dim_city_count}")
        logger.info(f"  Distinct city_id count:      {distinct_city_id_count}")
        logger.info(f"  Duplicated city_id count:    {duplicate_city_id_count}")
        logger.info(f"  Sampled cities count:        {sampled_city_count}")
        logger.info("="*50)

        if duplicate_city_id_count > 0:
            raise ValueError(f"CRITICAL ERROR: dim_city has {duplicate_city_id_count} duplicate city_ids!")

        logger.info(f"Upfront Stratified Sampling complete: {sampled_city_count:,} sampled cities / {total_dim_city_count:,} total cities ({sampled_city_count/total_dim_city_count*100:.1f}%).")
        logger.info(f"[STEP 2 COMPLETE] Time elapsed: {time.time() - step2_start:.2f} seconds.")

    except Exception as e:
        logger.error("ERROR: Dimension generation or upfront sampling failed!")
        logger.error(traceback.format_exc())
        spark.stop()
        sys.exit(1)

    # Step 3: Persist Dimension Tables & Validate Dimension Integrity
    step3_start = time.time()
    logger.info("\n" + "="*70)
    logger.info("[STEP 3/6] AUDITING & PERSISTING DIMENSION TABLES TO SILVER S3")
    logger.info("="*70)

    dim_validation_failures = []

    null_state_ids = dim_city.filter(F.col("state_id").isNull()).count()
    if null_state_ids > 0:
        dim_validation_failures.append(f"ERROR: dim_city contains {null_state_ids} NULL state_id values.")
    else:
        logger.info("PASSED: dim_city -> state_id FK non-null check.")

    dup_cities = dim_city.count() - dim_city.select("city_id").distinct().count()
    if dup_cities > 0:
        dim_validation_failures.append(f"ERROR: dim_city has {dup_cities} duplicate city_ids.")
    else:
        logger.info("PASSED: dim_city -> city_id primary key uniqueness check.")

    dup_city_names = dim_city.count() - dim_city.select("city_name").distinct().count()
    if dup_city_names > 0:
        dim_validation_failures.append(f"ERROR: dim_city has {dup_city_names} duplicate city names.")
    else:
        logger.info("PASSED: dim_city -> city_name strict uniqueness check.")

    if dim_validation_failures:
        for fail in dim_validation_failures:
            logger.error(fail)
        spark.stop()
        raise ValueError("Dimension validation failed. Resolving data quality errors before proceeding.")

    logger.info("ALL DIMENSION QUALITY CHECKS PASSED. Writing dimension tables to Silver S3...")

    try:
        dim_region.coalesce(1).write.mode("overwrite").parquet(f"{SILVER_BUCKET}/dim_region/")
        logger.info(f"Wrote dim_region -> {SILVER_BUCKET}/dim_region/")

        dim_state.coalesce(1).write.mode("overwrite").parquet(f"{SILVER_BUCKET}/dim_state/")
        logger.info(f"Wrote dim_state  -> {SILVER_BUCKET}/dim_state/")

        dim_city.coalesce(1).write.mode("overwrite").parquet(f"{SILVER_BUCKET}/dim_city/")
        logger.info(f"Wrote dim_city   -> {SILVER_BUCKET}/dim_city/")
        logger.info(f"[STEP 3 COMPLETE] Time elapsed: {time.time() - step3_start:.2f} seconds.")
    except Exception as e:
        logger.error("ERROR: Failed to write dimension tables to Silver S3!")
        logger.error(traceback.format_exc())
        spark.stop()
        sys.exit(1)

    # Step 4: Full Dataset Parallel Weather Fact Processing
    step4_start = time.time()
    logger.info("\n" + "="*70)
    logger.info("[STEP 4/6] FULL DATASET PARALLEL WEATHER FACT PROCESSING")
    logger.info("="*70)

    full_city_lookup = (
        dim_city
        .join(dim_state, "state_id")
        .select("city_id", "state_id", "city_name", "state_name", "is_sampled")
    )

    weather_paths = [
        f"{BRONZE_BUCKET}/wd1/",
        f"{BRONZE_BUCKET}/wd2/",
        f"{BRONZE_BUCKET}/wd3/"
    ]
    logger.info(f"Reading Bronze weather datasets lazily from: {weather_paths}")

    try:
        raw_weather_df = spark.read.parquet(*weather_paths)
        logger.info("Raw weather dataframe reader initialized.")

        # Single Broadcast Join across full dataset with memory persistence
        joined_weather = (
            raw_weather_df
            .join(
                F.broadcast(full_city_lookup),
                raw_weather_df.city == full_city_lookup.city_name,
                "inner"
            )
            .drop("city", "city_name")
            .persist(StorageLevel.MEMORY_AND_DISK_SER)
        )

        # Lazy FK Integrity check
        null_fk = joined_weather.filter(F.col("city_id").isNull() | F.col("state_id").isNull() | F.col("state_name").isNull()).limit(1).count()
        if null_fk > 0:
            raise ValueError("FK Integrity failure: null city_id, state_id, or state_name detected in weather facts!")

        # Write full fact table partitioned by state_name (repartitioned by state_name to prevent small S3 files)
        fact_target_path = f"{SILVER_BUCKET}/fact_weather/"
        logger.info(f"Writing full weather fact table partitioned by state_name -> {fact_target_path}")
        (
            joined_weather
            .drop("is_sampled")
            .repartition("state_name")
            .write
            .mode("overwrite")
            .partitionBy("state_name")
            .parquet(fact_target_path)
        )
        logger.info(f"SUCCESS: Wrote full weather fact table -> {fact_target_path}")

        # Write sampled fact table partitioned by state_name (repartitioned by state_name to prevent small S3 files)
        sampled_target_path = f"{SILVER_BUCKET}/fact_weather_sampled/"
        logger.info(f"Writing sampled weather fact table partitioned by state_name -> {sampled_target_path}")
        (
            joined_weather
            .filter(F.col("is_sampled") == True)
            .drop("is_sampled")
            .repartition("state_name")
            .write
            .mode("overwrite")
            .partitionBy("state_name")
            .parquet(sampled_target_path)
        )
        logger.info(f"SUCCESS: Wrote sampled weather fact table -> {sampled_target_path}")

        joined_weather.unpersist()
        logger.info(f"[STEP 4 COMPLETE] Time elapsed: {time.time() - step4_start:.2f} seconds.")

    except Exception as e:
        logger.error("ERROR: Failed processing weather fact tables!")
        logger.error(f"Details: {e}")
        logger.error(traceback.format_exc())
        spark.stop()
        sys.exit(1)

    # Step 5: Crop Data Schema Verification & Parquet Conversion
    step5_start = time.time()
    logger.info("\n" + "="*70)
    logger.info("[STEP 5/6] CROP DATA SCHEMA VERIFICATION & PARQUET CONVERSION")
    logger.info("="*70)
    logger.info(f"Crop Data Input Path  : {CROP_DATA_INPUT_PATH}")
    logger.info(f"Crop Data Output Path : {CROP_DATA_OUTPUT_PATH}")

    try:
        crop_candidate_paths = [
            CROP_DATA_INPUT_PATH,
            f"{BRONZE_BUCKET}/crop/Custom_Crops_yield_Historical_Dataset.csv",
            f"{BRONZE_BUCKET}/crop",
            f"{BRONZE_BUCKET}/crop_data"
        ]
        raw_crop_df = None
        for cp in crop_candidate_paths:
            try:
                logger.info(f"Attempting to read Crop Data from: {cp}")
                if cp.endswith(".parquet"):
                    raw_crop_df = spark.read.parquet(cp)
                elif cp.endswith(".csv"):
                    raw_crop_df = spark.read.option("header", "true").option("inferSchema", "true").csv(cp)
                else:
                    try:
                        raw_crop_df = spark.read.parquet(cp)
                    except Exception:
                        raw_crop_df = spark.read.option("header", "true").option("inferSchema", "true").csv(cp)
                logger.info(f"Successfully read Crop Data dataset from: {cp}")
                break
            except Exception as err:
                logger.warning(f"Crop Data candidate path ({cp}) failed: {err}")

        if raw_crop_df is None:
            raise FileNotFoundError(f"Could not read Crop Data from any candidate path in {BRONZE_BUCKET}")

        expected_crop_schema = [
            ("date", StringType()),
            ("temperature_2m", DoubleType()),
            ("relative_humidity_2m", DoubleType()),
            ("dew_point_2m", DoubleType()),
            ("apparent_temperature", DoubleType()),
            ("precipitation", DoubleType()),
            ("rain", DoubleType()),
            ("snowfall", DoubleType()),
            ("snow_depth", DoubleType()),
            ("pressure_msl", DoubleType()),
            ("surface_pressure", DoubleType()),
            ("cloud_cover", DoubleType()),
            ("cloud_cover_low", DoubleType()),
            ("cloud_cover_mid", DoubleType()),
            ("cloud_cover_high", DoubleType()),
            ("wind_speed_10m", DoubleType()),
            ("wind_speed_100m", DoubleType()),
            ("wind_direction_10m", DoubleType()),
            ("wind_direction_100m", DoubleType()),
            ("wind_gusts_10m", DoubleType()),
            ("city", StringType())
        ]

        input_columns = set(raw_crop_df.columns)
        missing_cols = [col_name for col_name, _ in expected_crop_schema if col_name not in input_columns]
        if missing_cols:
            raise ValueError(f"Crop Data Schema Verification Failed! Missing required columns: {missing_cols}")

        logger.info("PASSED: All 21 required columns are present in Crop Data.")

        select_exprs = [
            F.col(col_name).cast(col_type).alias(col_name)
            for col_name, col_type in expected_crop_schema
        ]
        crop_df_validated = raw_crop_df.select(*select_exprs)

        crop_row_count = crop_df_validated.count()
        logger.info(f"Validated Crop Data contains {crop_row_count:,} records.")

        logger.info(f"Writing Crop Data Parquet dataset to: {CROP_DATA_OUTPUT_PATH}")
        crop_df_validated.write.mode("overwrite").parquet(CROP_DATA_OUTPUT_PATH)
        logger.info(f"SUCCESS: Wrote Crop Data Parquet dataset in {time.time() - step5_start:.2f}s -> {CROP_DATA_OUTPUT_PATH}")

    except Exception as e:
        logger.error(f"ERROR: Failed processing Crop Data table from '{CROP_DATA_INPUT_PATH}'!")
        logger.error(f"Details: {e}")
        logger.error(traceback.format_exc())
        spark.stop()
        sys.exit(1)

    # Step 6: Summary & Pipeline Completion
    logger.info("\n" + "="*70)
    logger.info("[STEP 6/6] SUMMARY & PIPELINE COMPLETION")
    logger.info("="*70)
    logger.info(f"Fact Weather Target Path            : {SILVER_BUCKET}/fact_weather/")
    logger.info(f"Fact Weather Sampled Target Path    : {SILVER_BUCKET}/fact_weather_sampled/")
    logger.info(f"Crop Data Parquet Written           : {CROP_DATA_OUTPUT_PATH}")
    logger.info(f"Total Pipeline Runtime              : {(time.time() - job_start_time)/60:.2f} minutes")
    logger.info("EMR BRONZE TO SILVER TRANSFORMATION PIPELINE COMPLETED SUCCESSFULLY!")

    spark.stop()


if __name__ == "__main__":
    main()
