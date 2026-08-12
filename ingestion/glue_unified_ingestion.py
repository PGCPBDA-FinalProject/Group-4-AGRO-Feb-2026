# ============================================================
# UNIFIED BRONZE DATA INGESTION SCRIPT FOR AWS GLUE
# ============================================================
# AWS GLUE 4.0 / PYTHON SHELL / SPARK JOB
#
# INGESTS THREE CORE BRONZE DATASETS TO S3:
#   1. Weather Data (Kaggle: mukeshdevrath007/indian-5000-cities-weather-data)
#      -> s3://krishna-agro-bronze/weather/... (Snappy Parquet format)
#   2. Crop Yield Data (Kaggle: zoya77/indian-historical-crop-yield-and-weather-data)
#      -> s3://krishna-agro-bronze/crop/...
#   3. GeoJSON Boundaries (GeoHacker / India District GeoJSON)
#      -> s3://krishna-agro-bronze/geojson/india_district.geojson
#
# HANDLES AWS GLUE ENVIRONMENT CONDITIONS:
#   - Explicitly sets HOME, KAGGLE_CONFIG_DIR, and XDG_CONFIG_HOME to /tmp
#     BEFORE importing kaggle package to prevent permission crashes.
#   - Downloads ZIP archives (unzip=False) to avoid filling Glue local storage.
#   - Streams CSV files in chunked memory buffers (100k rows) directly to S3.
#   - Provides REST API HTTP stream fallback if Kaggle CLI auth fails.
#   - Cleans up temporary disk files and runs garbage collection after each phase.
# ============================================================

import gc
import json
import logging
import os
import sys
import tempfile
import time
import zipfile
from pathlib import Path

# ============================================================
# FIX: AWS GLUE PERMISSION DENIED ON HOME DIR
# ============================================================
_KAGGLE_HOME = Path(tempfile.gettempdir()) / "glue_kaggle_home"
_KAGGLE_CONFIG_DIR = _KAGGLE_HOME / ".kaggle"
_XDG_CONFIG_DIR = _KAGGLE_HOME / ".config"

_KAGGLE_HOME.mkdir(parents=True, exist_ok=True)
_KAGGLE_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
_XDG_CONFIG_DIR.mkdir(parents=True, exist_ok=True)

os.environ["HOME"] = str(_KAGGLE_HOME)
os.environ["KAGGLE_CONFIG_DIR"] = str(_KAGGLE_CONFIG_DIR)
os.environ["XDG_CONFIG_HOME"] = str(_XDG_CONFIG_DIR)

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

# Import Kaggle API
try:
    from kaggle.api.kaggle_api_extended import KaggleApi
    HAS_KAGGLE_API = True
except ImportError:
    HAS_KAGGLE_API = False

# AWS Glue Specific Imports
try:
    from awsglue.utils import getResolvedOptions
    IS_GLUE_ENV = True
except ImportError:
    IS_GLUE_ENV = False

# ============================================================
# LOGGING SETUP
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("UnifiedBronzeIngestionGlue")

# ============================================================
# PARAMETER & DEFAULT RESOLUTION
# ============================================================
DEFAULT_ARGS = {
    'S3_BUCKET': 'krishna-agro-bronze',
    'WEATHER_DATASET': 'mukeshdevrath007/indian-5000-cities-weather-data',
    'CROP_DATASET': 'akshatgupta7/crop-yield-in-indian-states-dataset',
    'GEOJSON_URL': 'https://raw.githubusercontent.com/geohacker/india/master/district/india_district.geojson',
    'CHUNK_SIZE': '100000',
    'KAGGLE_USERNAME': 'gawandek149',
    'KAGGLE_KEY': 'KGAT_4be03b4c9a339b3f03411bd8817b2366'
}

options = DEFAULT_ARGS.copy()

if IS_GLUE_ENV:
    glue_args = [
        'S3_BUCKET', 'WEATHER_DATASET', 'CROP_DATASET', 'GEOJSON_URL',
        'CHUNK_SIZE', 'KAGGLE_USERNAME', 'KAGGLE_KEY'
    ]
    present_args = [arg for arg in glue_args if f'--{arg}' in sys.argv]
    if present_args:
        try:
            resolved = getResolvedOptions(sys.argv, present_args)
            options.update(resolved)
            logger.info("Successfully loaded AWS Glue job arguments.")
        except Exception as e:
            logger.warning(f"Could not parse Glue options: {e}. Using defaults.")

# Override with environment variables if present
for key in options:
    if os.getenv(key):
        options[key] = os.getenv(key)

S3_BUCKET = options['S3_BUCKET'].replace("s3://", "").strip("/")
WEATHER_DATASET = options['WEATHER_DATASET']
CROP_DATASET = options['CROP_DATASET']
GEOJSON_URL = options['GEOJSON_URL']
CHUNK_SIZE = int(options['CHUNK_SIZE'])
KAGGLE_USERNAME = options.get('KAGGLE_USERNAME', '')
KAGGLE_KEY = options.get('KAGGLE_KEY', '')

# Setup local working directories in temp
TEMP_DIR = Path(tempfile.gettempdir())
DOWNLOAD_DIR = TEMP_DIR / "unified_kaggle_downloads"
TEMP_PARQUET_DIR = TEMP_DIR / "unified_parquet_temp"

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
TEMP_PARQUET_DIR.mkdir(parents=True, exist_ok=True)

# Initialize S3 client
s3_client = boto3.client("s3")


# ============================================================
# KAGGLE DOWNLOAD HELPER (UNZIP=FALSE TO PREVENT DISK OVERFLOW)
# ============================================================
def download_kaggle_dataset_zip(dataset_name: str, download_dir: Path, username: str, key: str) -> Path:
    """
    Downloads the compressed dataset ZIP without extracting full uncompressed size to disk.
    Tries KaggleApi first, falls back to direct Kaggle REST API stream.
    """
    download_dir.mkdir(parents=True, exist_ok=True)

    if not username or not key:
        raise ValueError("Kaggle credentials (KAGGLE_USERNAME and KAGGLE_KEY) are required.")

    kaggle_config_dir = Path(os.environ["KAGGLE_CONFIG_DIR"])
    kaggle_config_dir.mkdir(parents=True, exist_ok=True)
    kaggle_json_path = kaggle_config_dir / "kaggle.json"

    with open(kaggle_json_path, "w") as f:
        json.dump({"username": username, "key": key}, f)

    os.chmod(kaggle_json_path, 0o600)
    os.environ["KAGGLE_CONFIG_DIR"] = str(kaggle_config_dir)
    os.environ["KAGGLE_USERNAME"] = username
    os.environ["KAGGLE_KEY"] = key

    # Try downloading via KaggleApi (unzip=False)
    if HAS_KAGGLE_API:
        try:
            logger.info(f"Authenticating Kaggle API for '{dataset_name}'...")
            api = KaggleApi()
            api.authenticate()
            logger.info(f"Downloading dataset '{dataset_name}' ZIP archive...")
            api.dataset_download_files(dataset_name, path=str(download_dir), unzip=False, quiet=False)

            zip_candidates = list(download_dir.glob("*.zip"))
            if zip_candidates:
                zip_path = max(zip_candidates, key=lambda p: p.stat().st_mtime)
                logger.info(f"KaggleApi download successful: {zip_path.name} ({zip_path.stat().st_size / (1024**3):.2f} GB)")
                return zip_path
        except Exception as err:
            logger.warning(f"KaggleApi download failed ({err}). Falling back to Kaggle REST API stream...")

    # Direct REST API HTTP Stream Fallback
    logger.info("Downloading dataset ZIP directly via Kaggle REST API stream...")
    download_url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset_name}"
    safe_name = dataset_name.split("/")[-1] + ".zip"
    zip_path = download_dir / safe_name

    response = requests.get(download_url, auth=(username, key), stream=True, allow_redirects=True)
    response.raise_for_status()

    total_bytes = 0
    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
            if chunk:
                f.write(chunk)
                total_bytes += len(chunk)

    logger.info(f"Direct Kaggle REST API download successful: {zip_path.name} ({total_bytes / (1024**3):.2f} GB)")
    return zip_path


# ============================================================
# PHASE 1: WEATHER DATA INGESTION
# ============================================================
def extract_folder_and_city(file_path_str: str):
    path_obj = Path(file_path_str)
    city_name = path_obj.stem
    parts = path_obj.parts

    known_folders = ["w_d_1", "w_d_2", "w_d_3", "Weather_Data_Scraping_and_Analysis"]
    parent_folder = "weather"

    for part in parts:
        if part in known_folders:
            if part == "Weather_Data_Scraping_and_Analysis":
                parent_folder = "weather"
                city_name = "city_master"
            else:
                parent_folder = f"weather/{part}"
            break

    return parent_folder, city_name


def process_and_upload_weather_csv(csv_stream, folder_name: str, city_name: str):
    local_parquet_path = TEMP_PARQUET_DIR / f"{city_name}.parquet"
    writer = None
    total_rows = 0

    try:
        chunks = pd.read_csv(
            csv_stream,
            chunksize=CHUNK_SIZE,
            low_memory=False,
            encoding="utf-8-sig"
        )

        for chunk in chunks:
            if "Unnamed: 0" in chunk.columns:
                chunk.drop(columns=["Unnamed: 0"], inplace=True)

            if city_name != "city_master":
                chunk["city"] = city_name

            total_rows += len(chunk)

            table = pa.Table.from_pandas(chunk, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(
                    str(local_parquet_path),
                    table.schema,
                    compression="snappy"
                )

            writer.write_table(table)

        if writer is not None:
            writer.close()
            writer = None

        if not local_parquet_path.exists() or total_rows == 0:
            logger.warning(f"Skipping {city_name}: empty output.")
            return False, 0

        s3_key = f"{folder_name}/{city_name}.parquet"
        logger.info(f"Uploading weather {city_name} ({total_rows:,} rows) -> s3://{S3_BUCKET}/{s3_key}")

        s3_client.upload_file(
            str(local_parquet_path),
            S3_BUCKET,
            s3_key
        )
        return True, total_rows

    except Exception as err:
        logger.error(f"FAILED processing weather {city_name}: {err}", exc_info=True)
        return False, 0

    finally:
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass

        if local_parquet_path.exists():
            try:
                local_parquet_path.unlink()
            except Exception:
                pass

        gc.collect()


def ingest_weather_dataset():
    logger.info("=" * 70)
    logger.info("STAGE 1: WEATHER DATASET INGESTION")
    logger.info(f"Dataset: {WEATHER_DATASET}")
    logger.info("=" * 70)

    zip_path = download_kaggle_dataset_zip(
        dataset_name=WEATHER_DATASET,
        download_dir=DOWNLOAD_DIR,
        username=KAGGLE_USERNAME,
        key=KAGGLE_KEY
    )

    processed, failed, total_rows = 0, 0, 0

    with zipfile.ZipFile(zip_path, "r") as zf:
        csv_members = [
            m for m in zf.infolist()
            if not m.is_dir() and m.filename.lower().endswith(".csv")
        ]
        logger.info(f"Found {len(csv_members)} CSV entries in Weather dataset archive.")

        for idx, member in enumerate(csv_members, start=1):
            folder_name, city_name = extract_folder_and_city(member.filename)
            logger.info(f"[{idx}/{len(csv_members)}] Streaming [{folder_name}] {city_name}...")

            with zf.open(member) as csv_stream:
                success, rows = process_and_upload_weather_csv(
                    csv_stream=csv_stream,
                    folder_name=folder_name,
                    city_name=city_name
                )
                if success:
                    processed += 1
                    total_rows += rows
                else:
                    failed += 1

    try:
        if zip_path.exists():
            zip_path.unlink()
    except Exception as e:
        logger.warning(f"Could not delete Weather ZIP file: {e}")

    logger.info(f"STAGE 1 COMPLETED: Processed={processed}, Failed={failed}, Rows={total_rows:,}")
    return processed, total_rows


# ============================================================
# PHASE 2: CROP YIELD DATA INGESTION
# ============================================================
def ingest_crop_dataset():
    logger.info("=" * 70)
    logger.info("STAGE 2: CROP YIELD DATASET INGESTION")
    logger.info(f"Dataset: {CROP_DATASET}")
    logger.info("=" * 70)

    zip_path = download_kaggle_dataset_zip(
        dataset_name=CROP_DATASET,
        download_dir=DOWNLOAD_DIR,
        username=KAGGLE_USERNAME,
        key=KAGGLE_KEY
    )

    uploaded_files = 0

    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue

            file_name = os.path.basename(member.filename)
            if not file_name:
                continue

            s3_key = f"crop/{file_name}"
            logger.info(f"Extracting & uploading crop file: {file_name} -> s3://{S3_BUCKET}/{s3_key}")

            temp_crop_file = TEMP_DIR / file_name
            with zf.open(member) as source, open(temp_crop_file, "wb") as target:
                target.write(source.read())

            s3_client.upload_file(str(temp_crop_file), S3_BUCKET, s3_key)
            logger.info(f"SUCCESS: Uploaded s3://{S3_BUCKET}/{s3_key}")
            uploaded_files += 1

            if temp_crop_file.exists():
                temp_crop_file.unlink()

    try:
        if zip_path.exists():
            zip_path.unlink()
    except Exception as e:
        logger.warning(f"Could not delete Crop ZIP file: {e}")

    logger.info(f"STAGE 2 COMPLETED: Crop files uploaded = {uploaded_files}")
    return uploaded_files


# ============================================================
# PHASE 3: GEOJSON BOUNDARIES INGESTION
# ============================================================
def ingest_geojson_boundaries():
    logger.info("=" * 70)
    logger.info("STAGE 3: INDIA DISTRICT GEOJSON INGESTION")
    logger.info(f"GeoJSON URL: {GEOJSON_URL}")
    logger.info("=" * 70)

    local_geojson_path = TEMP_DIR / "india_district.geojson"
    s3_key = "geojson/india_district.geojson"

    response = requests.get(GEOJSON_URL, timeout=60)
    response.raise_for_status()

    # Validate JSON syntax
    geojson_data = response.json()
    features_count = len(geojson_data.get("features", []))
    logger.info(f"Downloaded GeoJSON containing {features_count} district features.")

    with open(local_geojson_path, "w", encoding="utf-8") as f:
        json.dump(geojson_data, f)

    s3_client.upload_file(str(local_geojson_path), S3_BUCKET, s3_key)
    logger.info(f"SUCCESS: Uploaded district GeoJSON -> s3://{S3_BUCKET}/{s3_key}")

    if local_geojson_path.exists():
        local_geojson_path.unlink()

    logger.info("STAGE 3 COMPLETED: GeoJSON uploaded successfully.")
    return True


# ============================================================
# MAIN ENTRY POINT
# ============================================================
def main():
    start_time = time.time()
    logger.info("=" * 70)
    logger.info("STARTING UNIFIED BRONZE DATA INGESTION PIPELINE")
    logger.info(f"Target S3 Bucket : s3://{S3_BUCKET}")
    logger.info(f"Weather Dataset  : {WEATHER_DATASET}")
    logger.info(f"Crop Dataset     : {CROP_DATASET}")
    logger.info(f"GeoJSON URL      : {GEOJSON_URL}")
    logger.info("=" * 70)

    try:
        # Phase 1: Weather
        weather_files, weather_rows = ingest_weather_dataset()
    except Exception as e:
        logger.error(f"Stage 1 (Weather) encountered an error: {e}", exc_info=True)
        weather_files, weather_rows = 0, 0

    try:
        # Phase 2: Crop
        crop_files = ingest_crop_dataset()
    except Exception as e:
        logger.error(f"Stage 2 (Crop) encountered an error: {e}", exc_info=True)
        crop_files = 0

    try:
        # Phase 3: GeoJSON
        geojson_status = ingest_geojson_boundaries()
    except Exception as e:
        logger.error(f"Stage 3 (GeoJSON) encountered an error: {e}", exc_info=True)
        geojson_status = False

    # Cleanup any remaining temp files
    for clean_dir in [TEMP_PARQUET_DIR, DOWNLOAD_DIR]:
        try:
            if clean_dir.exists():
                for f in clean_dir.iterdir():
                    if f.is_file():
                        f.unlink()
                clean_dir.rmdir()
        except Exception as e:
            logger.warning(f"Final cleanup notice: {e}")

    elapsed_minutes = round((time.time() - start_time) / 60, 2)
    logger.info("=" * 70)
    logger.info("UNIFIED INGESTION PIPELINE COMPLETED")
    logger.info(f"Total Execution Time : {elapsed_minutes} minutes")
    logger.info(f"Weather Files Ingested: {weather_files} ({weather_rows:,} rows)")
    logger.info(f"Crop Files Ingested   : {crop_files}")
    logger.info(f"GeoJSON Upload Status : {'SUCCESS' if geojson_status else 'FAILED'}")
    logger.info(f"Bronze S3 Target      : s3://{S3_BUCKET}/")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
