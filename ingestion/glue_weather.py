import gc
import json
import logging
import os
import sys
import tempfile
import time
import zipfile
from pathlib import Path

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
logger = logging.getLogger("WeatherIngestionGlue")

# ============================================================
# PARAMETER & ENVIRONMENT INITIALIZATION
# ============================================================
# NOTE: Do not hardcode real credentials here. Populate KAGGLE_USERNAME /
# KAGGLE_KEY via Glue job parameters, environment variables, or AWS Secrets
# Manager instead. Placeholders below are intentionally blank.
DEFAULT_ARGS = {
    'S3_BUCKET': 'agro-weather-data-lake1',
    'S3_PREFIX': '',
    'DATASET_NAME': 'mukeshdevrath007/indian-5000-cities-weather-data',
    'CHUNK_SIZE': '100000',
    'KAGGLE_USERNAME': '',
    'KAGGLE_KEY': ''
}

options = DEFAULT_ARGS.copy()

if IS_GLUE_ENV:
    # Resolve Glue Job parameters if running in AWS Glue environment
    glue_args = [
        'S3_BUCKET', 'S3_PREFIX', 'DATASET_NAME',
        'CHUNK_SIZE', 'KAGGLE_USERNAME', 'KAGGLE_KEY'
    ]
    present_args = [arg for arg in glue_args if f'--{arg}' in sys.argv]
    if present_args:
        try:
            resolved = getResolvedOptions(sys.argv, present_args)
            options.update(resolved)
            logger.info("Successfully loaded AWS Glue arguments.")
        except Exception as e:
            logger.warning(f"Could not parse Glue options: {e}. Using defaults.")

# Override with environment variables if present
for key in options:
    if os.getenv(key):
        options[key] = os.getenv(key)

S3_BUCKET = options['S3_BUCKET'].replace("s3://", "").strip("/")
S3_PREFIX = options['S3_PREFIX'].strip("/")
DATASET_NAME = options['DATASET_NAME']
CHUNK_SIZE = int(options['CHUNK_SIZE'])
KAGGLE_USERNAME = options.get('KAGGLE_USERNAME', '')
KAGGLE_KEY = options.get('KAGGLE_KEY', '')

# Setup local working directories in temp
TEMP_DIR = Path(tempfile.gettempdir())
DOWNLOAD_DIR = TEMP_DIR / "kaggle_download"
TEMP_PARQUET_DIR = TEMP_DIR / "weather_parquet_temp"

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
TEMP_PARQUET_DIR.mkdir(parents=True, exist_ok=True)

# Initialize S3 client
s3_client = boto3.client("s3")


# ============================================================
# KAGGLE DOWNLOAD HELPER (UNZIP=FALSE TO PREVENT DISK OVERFLOW)
# ============================================================
def download_kaggle_dataset_zip(dataset_name: str, download_dir: Path, username: str, key: str) -> Path:
    """
    Downloads the compressed dataset ZIP without extracting 29GB to disk.
    Tries KaggleApi first, falls back to direct Kaggle REST API stream.
    """
    download_dir.mkdir(parents=True, exist_ok=True)

    if not username or not key:
        raise ValueError("Kaggle credentials (KAGGLE_USERNAME and KAGGLE_KEY) are required.")

    # 1. Setup explicit kaggle.json config directory
    kaggle_config_dir = download_dir / ".kaggle"
    kaggle_config_dir.mkdir(parents=True, exist_ok=True)
    kaggle_json_path = kaggle_config_dir / "kaggle.json"

    with open(kaggle_json_path, "w") as f:
        json.dump({"username": username, "key": key}, f)

    os.chmod(kaggle_json_path, 0o600)
    os.environ["KAGGLE_CONFIG_DIR"] = str(kaggle_config_dir)
    os.environ["KAGGLE_USERNAME"] = username
    os.environ["KAGGLE_KEY"] = key

    # 2. Try downloading via KaggleApi (unzip=False)
    if HAS_KAGGLE_API:
        try:
            logger.info("Authenticating with KaggleApi...")
            api = KaggleApi()
            api.authenticate()
            logger.info(f"Downloading dataset '{dataset_name}' ZIP archive (unzip=False)...")
            api.dataset_download_files(dataset_name, path=str(download_dir), unzip=False, quiet=False)

            zip_candidates = list(download_dir.glob("*.zip"))
            if zip_candidates:
                zip_path = zip_candidates[0]
                logger.info(f"KaggleApi download successful: {zip_path.name} ({zip_path.stat().st_size / (1024**3):.2f} GB)")
                return zip_path
        except Exception as err:
            logger.warning(f"KaggleApi download failed: {err}. Falling back to direct Kaggle REST API stream...")

    # 3. Direct REST API HTTP Stream Fallback
    logger.info("Downloading dataset ZIP directly via Kaggle REST API stream...")
    download_url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset_name}"
    zip_path = download_dir / "dataset.zip"

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
# HELPER FUNCTIONS
# ============================================================
def extract_folder_and_city(file_path_str: str):
    """
    Parses a file path string inside the Kaggle dataset zip/folder to determine:
    1. Parent folder in S3 (e.g. w_d_1, w_d_2, w_d_3, weather)
    2. Output file stem (city name, or 'city_master' for the master file)

    Special case:
      Any file under 'Weather_Data_Scraping_and_Analysis/' is remapped to the
      'weather/' folder in S3, and its output filename is forced to
      'city_master.parquet' regardless of the original CSV's name.
    """
    path_obj = Path(file_path_str)
    city_name = path_obj.stem
    parts = path_obj.parts

    known_folders = ["w_d_1", "w_d_2", "w_d_3", "Weather_Data_Scraping_and_Analysis"]
    parent_folder = "weather_data"

    for part in parts:
        if part in known_folders:
            if part == "Weather_Data_Scraping_and_Analysis":
                parent_folder = "weather"
                city_name = "city_master"
            else:
                parent_folder = part
            break

    return parent_folder, city_name


def process_and_upload_csv_stream(csv_stream, folder_name: str, city_name: str):
    """
    Reads a CSV stream chunk by chunk, appends city metadata, converts to Parquet,
    and uploads directly to S3 to keep RAM usage minimal (prevents OOM).
    """
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

            # Only stamp a per-row city column for per-city files; the master
            # file already carries its own city column from the source data.
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
            logger.warning(f"Skipping {city_name}: empty or non-existent Parquet output.")
            return False, 0

        s3_key = f"{S3_PREFIX}/{folder_name}/{city_name}.parquet" if S3_PREFIX else f"{folder_name}/{city_name}.parquet"
        logger.info(f"Uploading {city_name} ({total_rows:,} rows) -> s3://{S3_BUCKET}/{s3_key}")

        s3_client.upload_file(
            str(local_parquet_path),
            S3_BUCKET,
            s3_key
        )

        logger.info(f"SUCCESS: Uploaded {city_name}.parquet to s3://{S3_BUCKET}/{s3_key}")
        return True, total_rows

    except Exception as err:
        logger.error(f"FAILED processing {city_name}: {err}", exc_info=True)
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


# ============================================================
# MAIN INGESTION WORKFLOW
# ============================================================
def main():
    start_time = time.time()
    logger.info("=" * 70)
    logger.info("STARTING KAGGLE WEATHER DATASET INGESTION TO AWS S3")
    logger.info(f"Dataset   : {DATASET_NAME}")
    logger.info(f"S3 Bucket : {S3_BUCKET}")
    logger.info(f"S3 Prefix : {S3_PREFIX}")
    logger.info(f"Chunk Size: {CHUNK_SIZE:,} rows")
    logger.info("=" * 70)

    # 1. Download raw ZIP file without extracting 29GB to disk
    logger.info("Initiating dataset download...")
    zip_file_path = download_kaggle_dataset_zip(
        dataset_name=DATASET_NAME,
        download_dir=DOWNLOAD_DIR,
        username=KAGGLE_USERNAME,
        key=KAGGLE_KEY
    )

    total_files_processed = 0
    total_files_failed = 0
    total_rows_ingested = 0

    # 2. Open ZIP stream and process CSV entries one by one
    logger.info(f"Opening archive stream: {zip_file_path.name} ({zip_file_path.stat().st_size / (1024**3):.2f} GB)")
    with zipfile.ZipFile(zip_file_path, "r") as zf:
        csv_members = [
            m for m in zf.infolist()
            if not m.is_dir() and m.filename.lower().endswith(".csv")
        ]
        logger.info(f"Total CSV entries found in archive: {len(csv_members)}")

        for idx, member in enumerate(csv_members, start=1):
            folder_name, city_name = extract_folder_and_city(member.filename)
            logger.info(f"[{idx}/{len(csv_members)}] Streaming [{folder_name}] {city_name}...")

            with zf.open(member) as csv_stream:
                success, rows = process_and_upload_csv_stream(
                    csv_stream=csv_stream,
                    folder_name=folder_name,
                    city_name=city_name
                )
                if success:
                    total_files_processed += 1
                    total_rows_ingested += rows
                else:
                    total_files_failed += 1

    # Delete downloaded ZIP file after processing to free disk space
    try:
        if zip_file_path.exists():
            zip_file_path.unlink()
            logger.info(f"Deleted downloaded ZIP file: {zip_file_path.name}")
    except Exception as e:
        logger.warning(f"Could not delete ZIP file: {e}")

    # Cleanup temporary working folders
    for cleanup_dir in [TEMP_PARQUET_DIR, DOWNLOAD_DIR]:
        try:
            if cleanup_dir.exists():
                for f in cleanup_dir.iterdir():
                    if f.is_file():
                        f.unlink()
                cleanup_dir.rmdir()
        except Exception as e:
            logger.warning(f"Cleanup notice: {e}")

    elapsed_minutes = round((time.time() - start_time) / 60, 2)
    logger.info("=" * 70)
    logger.info("INGESTION COMPLETED SUCCESSFULLY")
    logger.info(f"Total Time      : {elapsed_minutes} minutes")
    logger.info(f"Files Uploaded  : {total_files_processed}")
    logger.info(f"Files Failed    : {total_files_failed}")
    logger.info(f"Total Rows      : {total_rows_ingested:,}")
    logger.info(f"S3 Destination  : s3://{S3_BUCKET}/{S3_PREFIX}/")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()