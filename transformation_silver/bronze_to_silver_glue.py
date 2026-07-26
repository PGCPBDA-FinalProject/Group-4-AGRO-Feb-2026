
import sys
import math
import logging
import pandas as pd

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType

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
    'BRONZE_BUCKET': 's3://agro-weather-data-lake/bronze/weather',
    'SILVER_BUCKET': 's3://agro-weather-data-lake2/silver/weather-silver',
    'PERCENTAGE': '0.30',
    'MIN_CITIES': '15',
    'SEED': '42'
}

options = DEFAULT_ARGS.copy()
if IS_GLUE_ENV:
    try:
        glue_options = getResolvedOptions(
            sys.argv,
            ['JOB_NAME', 'BRONZE_BUCKET', 'SILVER_BUCKET', 'PERCENTAGE', 'MIN_CITIES', 'SEED']
        )
        options.update(glue_options)
    except Exception as e:
        logger.warning(f"Could not resolve Glue options from sys.argv ({e}). Using default fallback parameters.")

BRONZE_BUCKET = options['BRONZE_BUCKET'].rstrip('/')
SILVER_BUCKET = options['SILVER_BUCKET'].rstrip('/')
PERCENTAGE = float(options['PERCENTAGE'])
MIN_CITIES = int(options['MIN_CITIES'])
SEED = int(options['SEED'])

logger.info(f"Bronze Bucket Path : {BRONZE_BUCKET}")
logger.info(f"Silver Bucket Path : {SILVER_BUCKET}")
logger.info(f"Sampling Config    : Percentage={PERCENTAGE}, MinCities={MIN_CITIES}, Seed={SEED}")

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

# STEP 1: GENERATE MASTER LOOKUP (GEOSPATIAL ENRICHMENT)
logger.info("Step 1: Reading city_master from Bronze S3 and executing geospatial enrichment...")

city_master_path = f"{BRONZE_BUCKET}/city_master/city_master.parquet"
city_master_df = spark.read.parquet(city_master_path)

# Convert to Pandas for spatial join & regional boundary mapping
cm_pd = city_master_df.toPandas()

# Perform GeoPandas Point-in-Polygon spatial join if geopandas is available
try:
    import geopandas as gpd
    from shapely.geometry import Point
    
    logger.info("GeoPandas detected. Downloading district GeoJSON boundaries for spatial join...")
    url = "https://raw.githubusercontent.com/geohacker/india/master/district/india_district.geojson"
    gdf_districts = gpd.read_file(url)

    geometry = [Point(xy) for xy in zip(cm_pd["lng"], cm_pd["lat"])]
    gdf_cities = gpd.GeoDataFrame(cm_pd, geometry=geometry, crs="EPSG:4326")
    joined = gpd.sjoin(gdf_cities, gdf_districts[["NAME_1", "NAME_2", "geometry"]], how="left", predicate="intersects")

    # Nearest Neighbor Fallback for Coastal Edge Cities
    unmapped = joined["NAME_1"].isnull()
    if unmapped.any():
        logger.info(f"Applying nearest-neighbor fallback for {unmapped.sum()} edge cities...")
        uc = gdf_cities[unmapped].to_crs("EPSG:3857")
        dp = gdf_districts.to_crs("EPSG:3857")
        nearest = gpd.sjoin_nearest(uc, dp[["NAME_1", "NAME_2", "geometry"]], how="left")
        joined.loc[unmapped, "NAME_1"] = nearest["NAME_1"].values
        joined.loc[unmapped, "NAME_2"] = nearest["NAME_2"].values

    state_renames = {
        "Orissa": "Odisha",
        "Uttaranchal": "Uttarakhand",
        "Andaman and Nicobar": "Andaman & Nicobar",
        "Jammu and Kashmir": "Jammu & Kashmir"
    }
    cm_pd["state"] = joined["NAME_1"].replace(state_renames)
    cm_pd["district"] = joined["NAME_2"]

except Exception as e:
    logger.warning(f"Spatial join skipped ({e}). Relying on existing schema state/district attributes.")
    if "state" not in cm_pd.columns:
        cm_pd["state"] = "Unknown"
    if "district" not in cm_pd.columns:
        cm_pd["district"] = "Unknown"

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

# Strictly exclude any city name that appears more than once in city_master table
# Only include cities that have distinct and unique city names (keep=False)
raw_city_count = len(cm_pd)
master_lookup_pd = cm_pd.drop_duplicates(subset=["city"], keep=False).copy()
excluded_count = raw_city_count - len(master_lookup_pd)
logger.info(f"Master Lookup generated: {len(master_lookup_pd)} strictly unique cities (Excluded {excluded_count} cities with duplicate names in master city table).")

master_lookup_df = spark.createDataFrame(master_lookup_pd)

# STEP 2: GENERATE DIMENSIONS & UPFRONT STRATIFIED SAMPLING

logger.info("Step 2: Constructing dimensions (dim_region, dim_state, dim_city) with upfront sampling...")

# 1. dim_region
dim_region = (
    master_lookup_df.select("region").distinct()
    .filter(F.col("region").isNotNull())
    .withColumn("region_id", F.monotonically_increasing_id().cast("int"))
    .withColumnRenamed("region", "region_name")
    .select("region_id", "region_name")
)

# 2. dim_state
dim_state = (
    master_lookup_df.select("state", "region").distinct()
    .filter(F.col("state").isNotNull())
    .join(dim_region, master_lookup_df.region == dim_region.region_name, "left")
    .withColumn("state_id", F.monotonically_increasing_id().cast("int"))
    .select("state_id", F.col("state").alias("state_name"), "region_id")
)

# 3. Base dim_city
dim_city_base = (
    master_lookup_df
    .join(dim_state, master_lookup_df.state == dim_state.state_name, "left")
    .withColumn("city_id", F.monotonically_increasing_id().cast("int"))
    .select(
        "city_id",
        F.col("city").alias("city_name"),
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

raw_weather_df = spark.read.parquet(
    f"{BRONZE_BUCKET}/wd1/",
    f"{BRONZE_BUCKET}/wd2/"
)

sampled_city_lookup = (
    dim_city
    .filter(F.col("is_sampled") == True)
    .join(dim_state, "state_id")
    .select("city_id", "city_name", "state_name")
)

fact_weather_sampled = (
    raw_weather_df
    .join(
        F.broadcast(sampled_city_lookup),
        raw_weather_df.city == sampled_city_lookup.city_name,
        "inner"
    )
    .drop("city", "city_name")
)


# STEP 4: BUILD FACT_WEATHER (FULL FACT TABLE)

logger.info("Step 4: Building full fact_weather table...")

full_city_lookup = (
    dim_city
    .join(dim_state, "state_id")
    .select("city_id", "city_name", "state_name")
)

fact_weather = (
    raw_weather_df
    .join(
        F.broadcast(full_city_lookup),
        raw_weather_df.city == full_city_lookup.city_name,
        "inner"
    )
    .drop("city", "city_name")
)


# STEP 5: VALIDATE DATA QUALITY & AUDIT

logger.info("Step 5: Running pre-persistence data quality assertions...")
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

# Audit 2b: City Name Uniqueness (Strictly unique city names, no duplicates)
dup_city_names = dim_city.count() - dim_city.select("city_name").distinct().count()
if dup_city_names > 0:
    validation_failures.append(f"ERROR: dim_city has {dup_city_names} duplicate city names.")
else:
    logger.info("PASSED: dim_city city names are strictly unique.")

# Audit 3: State Representation in Sampled Dataset
orig_state_count = dim_state.select("state_name").distinct().count()
samp_state_count = fact_weather_sampled.select("state_name").distinct().count()

if orig_state_count != samp_state_count:
    validation_failures.append(
        f"ERROR: State count mismatch (Original: {orig_state_count}, Sampled: {samp_state_count})"
    )
else:
    logger.info(f"PASSED: All {orig_state_count} states represented in fact_weather_sampled.")

# Audit 4: Sampled Flag Consistency
sampled_flag_count = dim_city.filter(F.col("is_sampled") == True).count()
if sampled_flag_count != len(sampled_city_ids):
    validation_failures.append(f"ERROR: is_sampled flag count ({sampled_flag_count}) != expected ({len(sampled_city_ids)})")
else:
    logger.info(f"PASSED: dim_city is_sampled flag count matches metadata sample ({sampled_flag_count}).")

if validation_failures:
    for fail in validation_failures:
        logger.error(fail)
    raise ValueError("Pipeline validation failed. Resolve data quality errors before writing to Silver.")

logger.info("ALL DATA QUALITY CHECKS PASSED SUCCESSFULLY.")


# STEP 6: WRITE SILVER LAYER TO S3 PARQUET
logger.info("Step 6: Writing transformed star schema to Silver S3 Parquet target...")

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
