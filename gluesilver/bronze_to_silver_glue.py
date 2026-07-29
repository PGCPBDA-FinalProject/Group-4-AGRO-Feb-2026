import sys
import time
import math
import logging
import pandas as pd

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, DoubleType
from pyspark.sql.window import Window

# AWS Glue Specific Imports
try:
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    IS_GLUE_ENV = True
except ImportError:
    IS_GLUE_ENV = False

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("BronzeToSilverGlueJob")

# 1. PARAMETER & ENVIRONMENT INITIALIZATION
DEFAULT_ARGS = {
    'JOB_NAME': 'bronze_to_silver_transformation',
    'BRONZE_BUCKET': 's3://agro-weather-data-lake',
    'SILVER_BUCKET': 's3://agro-weather-data-lake2/silver',
    'CROP_DATA_INPUT_PATH': 's3://agro-weather-data-lake/crop/Custom_Crops_yield_Historical_Dataset.csv',
    'CROP_DATA_OUTPUT_PATH': 's3://agro-weather-data-lake2/silver/crop_data',
    'PERCENTAGE': '0.30',
    'MIN_CITIES': '15',
    'SEED': '42'
}

options = DEFAULT_ARGS.copy()
if IS_GLUE_ENV:
    try:
        glue_options = getResolvedOptions(
            sys.argv,
            ['JOB_NAME', 'BRONZE_BUCKET', 'SILVER_BUCKET', 'CROP_DATA_INPUT_PATH', 'CROP_DATA_OUTPUT_PATH', 'PERCENTAGE', 'MIN_CITIES', 'SEED']
        )
        options.update(glue_options)
    except Exception as e:
        logger.warning(f"Could not resolve Glue options from sys.argv ({e}). Using default fallback parameters.")

BRONZE_BUCKET = options['BRONZE_BUCKET'].rstrip('/')
SILVER_BUCKET = options['SILVER_BUCKET'].rstrip('/')
CROP_DATA_INPUT_PATH = options.get('CROP_DATA_INPUT_PATH', f"{BRONZE_BUCKET}/crop/Custom_Crops_yield_Historical_Dataset.csv").rstrip('/')
CROP_DATA_OUTPUT_PATH = options.get('CROP_DATA_OUTPUT_PATH', f"{SILVER_BUCKET}/crop_data").rstrip('/')
PERCENTAGE = float(options['PERCENTAGE'])
MIN_CITIES = int(options['MIN_CITIES'])
SEED = int(options['SEED'])

# Derive candidate base paths (handles s3://agro-weather-data-lake/bronze vs s3://agro-weather-data-lake without /bronze)
bronze_raw = BRONZE_BUCKET
bronze_stripped = bronze_raw[:-7] if bronze_raw.endswith('/bronze') else bronze_raw

BRONZE_BASES = []
for b in [bronze_raw, bronze_stripped, bronze_raw.replace('agro-weather-data', 'agro-weather-data-lake') if 'agro-weather-data' in bronze_raw else bronze_raw]:
    if b and b not in BRONZE_BASES:
        BRONZE_BASES.append(b)

for b in list(BRONZE_BASES):
    if b.endswith('/bronze'):
        b_strip = b[:-7]
        if b_strip not in BRONZE_BASES:
            BRONZE_BASES.append(b_strip)

logger.info(f"Bronze Bucket Path     : {BRONZE_BUCKET}")
logger.info(f"Candidate Bronze Bases : {BRONZE_BASES}")
logger.info(f"Silver Bucket Path     : {SILVER_BUCKET}")
logger.info(f"Crop Data Input Path   : {CROP_DATA_INPUT_PATH}")
logger.info(f"Crop Data Output Path  : {CROP_DATA_OUTPUT_PATH}")
logger.info(f"Sampling Config        : Percentage={PERCENTAGE}, MinCities={MIN_CITIES}, Seed={SEED}")

# Initialize Spark & Glue Context
if IS_GLUE_ENV:
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(options['JOB_NAME'], options)
else:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName(options['JOB_NAME']) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    glueContext = None
    job = None

# Enable Arrow optimization if available
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

# STEP 1: GENERATE MASTER LOOKUP (GEOSPATIAL ENRICHMENT & COMPOSITE NATURAL KEYING)
logger.info("Step 1: Reading city_master from Bronze S3 and executing geospatial enrichment...")

city_master_paths = []
for base in BRONZE_BASES:
    city_master_paths.extend([
        f"{base}/weather/city_master.parquet",
        f"{base}/weather/city_master",
        f"{base}/city_master/city_master.parquet",
        f"{base}/city_master.parquet",
        f"{base}/city_master",
        f"{base}/weather/city_master.csv",
        f"{base}/city_master.csv"
    ])

seen_cm = set()
city_master_paths = [p for p in city_master_paths if not (p in seen_cm or seen_cm.add(p))]

city_master_df = None
for cm_path in city_master_paths:
    try:
        logger.info(f"Attempting to read city_master from: {cm_path}")
        city_master_df = spark.read.parquet(cm_path)
        logger.info(f"Parquet read completed successfully from: {cm_path}")
        break
    except Exception as e:
        logger.warning(f"Parquet read failed ({cm_path}): {e}")
        try:
            city_master_df = spark.read.option("header", "true").option("inferSchema", "true").csv(cm_path)
            logger.info(f"CSV read completed successfully from: {cm_path}")
            break
        except Exception as csv_err:
            logger.warning(f"CSV read failed ({cm_path}): {csv_err}")

if city_master_df is None:
    raise FileNotFoundError(f"Could not read city_master dataset from any candidate path in {BRONZE_BASES}")

cm_pd = city_master_df.toPandas()

# Perform GeoPandas Point-in-Polygon spatial join with Fail-Fast Production Safeguard
try:
    import geopandas as gpd
    from shapely.geometry import Point

    logger.info("GeoPandas detected. Loading district boundaries...")
    gdf_districts = None

    geojson_s3_paths = []
    for base in BRONZE_BASES:
        geojson_s3_paths.extend([
            f"{base}/geojson/india_district.geojson",
            f"{base}/geojson/india_district.json",
            f"{base}/district_geojson/india_district.geojson",
            f"{base}/india_district.geojson"
        ])
    seen_gj = set()
    geojson_s3_paths = [p for p in geojson_s3_paths if not (p in seen_gj or seen_gj.add(p))]

    geojson_local_path = "/tmp/india_district.geojson"
    import subprocess
    loaded_s3 = False
    for gj_path in geojson_s3_paths:
        try:
            logger.info(f"Attempting to copy GeoJSON from S3: {gj_path}")
            subprocess.run(["aws", "s3", "cp", gj_path, geojson_local_path], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            gdf_districts = gpd.read_file(geojson_local_path)
            logger.info(f"Loaded district boundaries from S3 copy: {gj_path}")
            loaded_s3 = True
            break
        except Exception:
            continue

    if not loaded_s3:
        logger.warning("S3 GeoJSON load failed. Falling back to GitHub raw GeoJSON URL...")
        url = "https://raw.githubusercontent.com/geohacker/india/master/district/india_district.geojson"
        gdf_districts = gpd.read_file(url)
        logger.info(f"Loaded district boundaries from GitHub URL: {url}")

    geometry = [Point(xy) for xy in zip(cm_pd["lng"], cm_pd["lat"])]
    gdf_cities = gpd.GeoDataFrame(cm_pd, geometry=geometry, crs="EPSG:4326")
    joined = gpd.sjoin(gdf_cities, gdf_districts[["NAME_1", "NAME_2", "geometry"]], how="left", predicate="intersects")
    joined = joined[~joined.index.duplicated(keep="first")]

    unmapped = joined["NAME_1"].isnull()
    if unmapped.any():
        logger.info(f"Applying nearest-neighbor fallback for {unmapped.sum()} edge cities...")
        uc = gdf_cities[unmapped].to_crs("EPSG:3857")
        dp = gdf_districts.to_crs("EPSG:3857")
        nearest = gpd.sjoin_nearest(uc, dp[["NAME_1", "NAME_2", "geometry"]], how="left")
        nearest = nearest[~nearest.index.duplicated(keep="first")]
        joined.loc[unmapped, "NAME_1"] = nearest["NAME_1"].values
        joined.loc[unmapped, "NAME_2"] = nearest["NAME_2"].values

    state_renames = {
        "Orissa": "Odisha",
        "Uttaranchal": "Uttarakhand",
        "Andaman and Nicobar": "Andaman & Nicobar",
        "Jammu and Kashmir": "Jammu & Kashmir"
    }
    cm_pd["state"] = joined["NAME_1"].replace(state_renames).values
    cm_pd["district"] = joined["NAME_2"].values

except Exception as e:
    logger.error(f"CRITICAL: Spatial join encountered error ({e}). Checking if valid state data exists...")
    if "state" not in cm_pd.columns or cm_pd["state"].isnull().all() or (cm_pd["state"] == "Unknown").all():
        raise RuntimeError(f"CRITICAL ETL FAILURE: GeoSpatial enrichment failed and no valid state column exists ({e}). Pipeline aborted to protect Data Warehouse integrity.")
    logger.info("Relying on existing schema state/district attributes.")

# Map Macro Regions
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

# BUSINESS-SAFE NATURAL KEYING: Deduplicate strictly by composite natural key (city, state)
raw_city_count = len(cm_pd)
if "state" in cm_pd.columns and cm_pd["state"].notnull().any():
    master_lookup_pd = cm_pd.drop_duplicates(subset=["city", "state"], keep="first").copy()
else:
    master_lookup_pd = cm_pd.drop_duplicates(subset=["city"], keep="first").copy()

excluded_count = raw_city_count - len(master_lookup_pd)
logger.info(f"Master Lookup generated: {len(master_lookup_pd)} unique composite natural key entities (Deduplicated {excluded_count} redundant records).")

master_lookup_df = spark.createDataFrame(master_lookup_pd)

# STEP 2: GENERATE DETERMINISTIC SURROGATE KEYS & STRATIFIED SAMPLING

logger.info("Step 2: Constructing dimensions (dim_region, dim_state, dim_city) with deterministic surrogate keys...")

# 1. dim_region: Deterministic sequential ID via Window order by region_name
w_region = Window.partitionBy().orderBy("region_name")
dim_region = (
    master_lookup_df.select(F.col("region").alias("region_name")).distinct()
    .filter(F.col("region_name").isNotNull())
    .withColumn("region_id", F.row_number().over(w_region).cast("int"))
    .select("region_id", "region_name")
)

# 2. dim_state: Deterministic sequential ID via Window order by state_name
state_distinct_df = (
    master_lookup_df.select(F.col("state").alias("state_name"), F.col("region").alias("region_name")).distinct()
    .filter(F.col("state_name").isNotNull())
)

w_state = Window.partitionBy().orderBy("state_name")
dim_state = (
    state_distinct_df
    .join(dim_region, "region_name", "left")
    .withColumn("state_id", F.row_number().over(w_state).cast("int"))
    .select("state_id", "state_name", "region_id")
)

# 3. dim_city_base: Deterministic sequential ID via Window order by (state_name, city_name)
w_city = Window.partitionBy().orderBy("state_name", "city_name")
dim_city_base = (
    master_lookup_df
    .withColumnRenamed("state", "state_name")
    .withColumnRenamed("city", "city_name")
    .join(dim_state, "state_name", "left")
    .withColumn("city_id", F.row_number().over(w_city).cast("int"))
    .select(
        "city_id",
        "city_name",
        "state_id",
        "district",
        "lat",
        "lng"
    )
)

# Upfront Stratified Sampling on dim_city metadata (Strategy C)
city_state_pd = (
    dim_city_base
    .join(dim_state, "state_id")
    .select("city_id", "city_name", "state_name")
    .toPandas()
)

sampled_frames = []
for state, group in city_state_pd.groupby("state_name"):
    total = len(group)
    sample_n = min(max(MIN_CITIES, math.ceil(total * PERCENTAGE)), total)
    sample_df = group.sample(n=sample_n, random_state=SEED)
    sampled_frames.append(sample_df)

df_sampled_cities = pd.concat(sampled_frames, ignore_index=True) if sampled_frames else city_state_pd
sampled_city_ids = set(df_sampled_cities["city_id"].tolist())

# Broadcast sampled city IDs to PySpark UDF
sampled_ids_bc = spark.sparkContext.broadcast(sampled_city_ids)
is_sampled_udf = F.udf(lambda cid: cid in sampled_ids_bc.value, BooleanType())

dim_city = dim_city_base.withColumn("is_sampled", is_sampled_udf(F.col("city_id")))
logger.info(f"Sampled cities: {len(sampled_city_ids)} / {dim_city.count()}")

# STEP 3: BUILD FACT_WEATHER_SAMPLED (FILTERED READ)

logger.info("Step 3: Building fact_weather_sampled via early predicate pushdown broadcast join...")

weather_path_sets = []
for base in BRONZE_BASES:
    weather_path_sets.append([f"{base}/w_d_1/", f"{base}/w_d_2/", f"{base}/w_d_3/"])
    weather_path_sets.append([f"{base}/w_d_1/"])
    weather_path_sets.append([f"{base}/weather/"])
    weather_path_sets.append([f"{base}/weather"])
    weather_path_sets.append([f"{base}/wd1/", f"{base}/wd2/", f"{base}/wd3/"])
    weather_path_sets.append([f"{base}/wd1/"])

raw_weather_df = None
for w_paths in weather_path_sets:
    try:
        logger.info(f"Attempting to read weather datasets from candidate set: {w_paths}")
        raw_weather_df = spark.read.parquet(*w_paths)
        logger.info(f"Successfully read weather datasets from: {w_paths}")
        break
    except Exception as w_err:
        logger.warning(f"Weather path candidate set ({w_paths}) failed: {w_err}")

if raw_weather_df is None:
    raise FileNotFoundError(f"Could not read weather data from any candidate path set in {BRONZE_BASES}")

sampled_city_lookup = (
    dim_city
    .filter(F.col("is_sampled") == True)
    .join(dim_state, "state_id")
    .select("city_id", "city_name", "state_name")
    .withColumn("city_key", F.lower(F.trim(F.col("city_name"))))
)

raw_weather_clean = raw_weather_df.withColumn("city_key", F.lower(F.trim(F.col("city"))))

fact_weather_sampled = (
    raw_weather_clean
    .join(
        F.broadcast(sampled_city_lookup),
        "city_key",
        "inner"
    )
    .drop("city", "city_name", "city_key")
)


# STEP 4: BUILD FACT_WEATHER (FULL FACT TABLE)

logger.info("Step 4: Building full fact_weather table...")

full_city_lookup = (
    dim_city
    .join(dim_state, "state_id")
    .select("city_id", "city_name", "state_name")
    .withColumn("city_key", F.lower(F.trim(F.col("city_name"))))
)

fact_weather = (
    raw_weather_clean
    .join(
        F.broadcast(full_city_lookup),
        "city_key",
        "inner"
    )
    .drop("city", "city_name", "city_key")
)


# STEP 5: CROP DATA SCHEMA VERIFICATION & PARQUET CONVERSION

logger.info("Step 5: Executing Crop Data Schema Verification & Parquet Export...")
logger.info(f"Crop Data Input Path  : {CROP_DATA_INPUT_PATH}")
logger.info(f"Crop Data Output Path : {CROP_DATA_OUTPUT_PATH}")

try:
    crop_candidate_paths = [CROP_DATA_INPUT_PATH]
    for base in BRONZE_BASES:
        crop_candidate_paths.extend([
            f"{base}/crop/Custom_Crops_yield_Historical_Dataset.csv",
            f"{base}/crop_data/Custom_Crops_yield_Historical_Dataset.csv",
            f"{base}/crop_data",
            f"{base}/crop",
            f"{base}/crop/Custom_Crops_yield_Historical_Dataset.parquet"
        ])
    seen_crop = set()
    crop_candidate_paths = [p for p in crop_candidate_paths if p and not (p in seen_crop or seen_crop.add(p))]

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

    if raw_crop_df is not None:
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
            logger.warning(f"Crop Data missing columns: {missing_cols}")
        else:
            select_exprs = [
                F.col(col_name).cast(col_type).alias(col_name)
                for col_name, col_type in expected_crop_schema
            ]
            crop_df_validated = raw_crop_df.select(*select_exprs)
            crop_df_validated.write.mode("overwrite").parquet(CROP_DATA_OUTPUT_PATH)
            logger.info(f"SUCCESS: Wrote Crop Data Parquet dataset -> {CROP_DATA_OUTPUT_PATH}")
    else:
        logger.warning(f"Crop Data processing skipped (could not read from candidate paths in {BRONZE_BUCKET})")
except Exception as crop_err:
    logger.warning(f"Crop Data step encountered an error ({crop_err}). Continuing pipeline...")


# STEP 6: VALIDATE DATA QUALITY & AUDIT

logger.info("Step 6: Running pre-persistence data quality assertions...")
validation_failures = []

# Audit 1: Foreign Key Null Checks
for table_name, df_check, fk_col in [
    ("fact_weather", fact_weather, "city_id"),
    ("fact_weather_sampled", fact_weather_sampled, "city_id"),
    ("dim_city", dim_city, "state_id"),
]:
    nulls = df_check.filter(F.col(fk_col).isNull()).count()
    if nulls > 0:
        validation_failures.append(f"ERROR: {table_name}.{fk_col} contains {nulls} NULL values.")
    else:
        logger.info(f"PASSED: {table_name}.{fk_col} has 0 NULLs.")

# Audit 2: Primary Key Uniqueness
dup_cities = dim_city.count() - dim_city.select("city_id").distinct().count()
if dup_cities > 0:
    validation_failures.append(f"ERROR: dim_city has {dup_cities} duplicate city_ids.")
else:
    logger.info("PASSED: dim_city primary keys are unique.")

# Audit 2b: Composite City Key Uniqueness (city_name + state_id)
dup_composite_cities = dim_city.count() - dim_city.select("city_name", "state_id").distinct().count()
if dup_composite_cities > 0:
    validation_failures.append(f"ERROR: dim_city has {dup_composite_cities} duplicate (city_name, state_id) composite entities.")
else:
    logger.info("PASSED: dim_city composite keys (city_name, state_id) are strictly unique.")

# Audit 3: State Representation in Sampled Dataset vs Full Weather Dataset
master_state_count = dim_state.select("state_name").distinct().count()
weather_state_count = fact_weather.select("state_name").distinct().count()
samp_state_count = fact_weather_sampled.select("state_name").distinct().count()

logger.info(f"State coverage stats -> Master states: {master_state_count}, Weather states: {weather_state_count}, Sampled states: {samp_state_count}")

if samp_state_count != weather_state_count:
    validation_failures.append(
        f"ERROR: State count mismatch between full weather dataset ({weather_state_count}) and sampled dataset ({samp_state_count})."
    )
else:
    logger.info(f"PASSED: All {samp_state_count} available weather states represented in fact_weather_sampled.")

# Audit 4: Sampled Flag Consistency
sampled_flag_count = dim_city.filter(F.col("is_sampled") == True).count()
if sampled_flag_count != len(sampled_city_ids):
    validation_failures.append(f"ERROR: is_sampled flag count ({sampled_flag_count}) != expected ({len(sampled_city_ids)})")
else:
    logger.info(f"PASSED: dim_city is_sampled flag count matches metadata sample ({sampled_flag_count}).")

if validation_failures:
    for fail in validation_failures:
        logger.error(fail)
    raise ValueError(f"Pipeline validation failed with {len(validation_failures)} error(s): {'; '.join(validation_failures)}")

logger.info("ALL DATA QUALITY CHECKS PASSED SUCCESSFULLY.")


# STEP 7: WRITE SILVER LAYER TO S3 PARQUET
logger.info("Step 7: Writing transformed star schema to Silver S3 Parquet target...")

# Write Dimension Tables (Unpartitioned, Coalesced to single parquet files)
dim_region.coalesce(1).write.mode("overwrite").parquet(f"{SILVER_BUCKET}/dim_region/")
logger.info(f"Wrote dim_region -> {SILVER_BUCKET}/dim_region/")

dim_state.coalesce(1).write.mode("overwrite").parquet(f"{SILVER_BUCKET}/dim_state/")
logger.info(f"Wrote dim_state  -> {SILVER_BUCKET}/dim_state/")

dim_city.coalesce(1).write.mode("overwrite").parquet(f"{SILVER_BUCKET}/dim_city/")
logger.info(f"Wrote dim_city   -> {SILVER_BUCKET}/dim_city/")

# Write Fact Tables (Partitioned by state_name)
(
    fact_weather_sampled
    .write
    .mode("overwrite")
    .partitionBy("state_name")
    .parquet(f"{SILVER_BUCKET}/fact_weather_sampled/")
)
logger.info(f"Wrote fact_weather_sampled -> {SILVER_BUCKET}/fact_weather_sampled/")

(
    fact_weather
    .write
    .mode("overwrite")
    .partitionBy("state_name")
    .parquet(f"{SILVER_BUCKET}/fact_weather/")
)
logger.info(f"Wrote fact_weather -> {SILVER_BUCKET}/fact_weather/")

if IS_GLUE_ENV and job:
    job.commit()

logger.info("BRONZE TO SILVER TRANSFORMATION PIPELINE COMPLETE SUCCESSFULLY!")