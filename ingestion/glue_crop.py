import json
import logging
import os
import sys
import tempfile
import time
import zipfile
from pathlib import Path

import boto3
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
logger = logging.getLogger("CropYieldWeatherIngestionGlue")

# ============================================================
# PARAMETER & ENVIRONMENT INITIALIZATION
# ============================================================
# NOTE: Do not hardcode real credentials. Populate KAGGLE_USERNAME / KAGGLE_KEY
# via Glue job parameters, environment variables, or AWS Secrets Manager.
DEFAULT_ARGS = {
    'S3_BUCKET': 'krishna-agro-bronze',
    'S3_PREFIX': 'crop',
    'DATASET_NAME': 'akshatgupta7/crop-yield-in-indian-states-dataset',
    'KAGGLE_USERNAME': 'gawandek149',
    'KAGGLE_KEY': 'KGAT_4be03b4c9a339b3f03411bd8817b2366',
    'GEOJSON_URL': 'https://raw.githubusercontent.com/geohacker/india/master/district/india_district.geojson',
    'GEOJSON_S3_PREFIX': 'geojson'
}

options = DEFAULT_ARGS.copy()

if IS_GLUE_ENV:
    glue_args = ['S3_BUCKET', 'S3_PREFIX', 'DATASET_NAME', 'KAGGLE_USERNAME', 'KAGGLE_KEY',
                 'GEOJSON_URL', 'GEOJSON_S3_PREFIX']
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
KAGGLE_USERNAME = options.get('KAGGLE_USERNAME', '')
KAGGLE_KEY = options.get('KAGGLE_KEY', '')
GEOJSON_URL = options.get('GEOJSON_URL', '')
GEOJSON_S3_PREFIX = options.get('GEOJSON_S3_PREFIX', 'geojson').strip("/")

# Setup local working directories in temp
TEMP_DIR = Path(tempfile.gettempdir())
DOWNLOAD_DIR = TEMP_DIR / "kaggle_download_crop_yield"
EXTRACT_DIR = TEMP_DIR / "kaggle_extract_crop_yield"

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
EXTRACT_DIR.mkdir(parents=True, exist_ok=True)

# Initialize S3 client
s3_client = boto3.client("s3")


# ============================================================
# KAGGLE DOWNLOAD HELPER
# ============================================================
def download_kaggle_dataset_zip(dataset_name: str, download_dir: Path, username: str, key: str) -> Path:
    """
    Downloads the compressed dataset ZIP. Tries KaggleApi first, falls back
    to direct Kaggle REST API stream.
    """
    download_dir.mkdir(parents=True, exist_ok=True)

    if not username or not key:
        raise ValueError("Kaggle credentials (KAGGLE_USERNAME and KAGGLE_KEY) are required.")

    kaggle_config_dir = download_dir / ".kaggle"
    kaggle_config_dir.mkdir(parents=True, exist_ok=True)
    kaggle_json_path = kaggle_config_dir / "kaggle.json"

    with open(kaggle_json_path, "w") as f:
        json.dump({"username": username, "key": key}, f)

    os.chmod(kaggle_json_path, 0o600)
    os.environ["KAGGLE_CONFIG_DIR"] = str(kaggle_config_dir)
    os.environ["KAGGLE_USERNAME"] = username
    os.environ["KAGGLE_KEY"] = key

    if HAS_KAGGLE_API:
        try:
            logger.info("Authenticating with KaggleApi...")
            api = KaggleApi()
            api.authenticate()
            logger.info(f"Downloading dataset '{dataset_name}' ZIP archive...")
            api.dataset_download_files(dataset_name, path=str(download_dir), unzip=False, quiet=False)

            zip_candidates = list(download_dir.glob("*.zip"))
            if zip_candidates:
                zip_path = zip_candidates[0]
                logger.info(f"KaggleApi download successful: {zip_path.name} "
                            f"({zip_path.stat().st_size / (1024**2):.2f} MB)")
                return zip_path
        except Exception as err:
            logger.warning(f"KaggleApi download failed: {err}. Falling back to direct Kaggle REST API stream...")

    logger.info("Downloading dataset ZIP directly via Kaggle REST API stream...")
    download_url = f"https://www.kaggle.com/api/v1/datasets/download/{dataset_name}"
    zip_path = download_dir / "dataset.zip"

    response = requests.get(download_url, auth=(username, key), stream=True, allow_redirects=True)
    response.raise_for_status()

    total_bytes = 0
    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
                total_bytes += len(chunk)

    logger.info(f"Direct Kaggle REST API download successful: {zip_path.name} "
                f"({total_bytes / (1024**2):.2f} MB)")
    return zip_path


# ============================================================
# GEOJSON DOWNLOAD HELPER
# ============================================================
def download_and_upload_geojson(url: str, download_dir: Path, s3_bucket: str, s3_prefix: str):
    """
    Equivalent of:
        curl -L <url> -o india_district.geojson
    Downloads the file via HTTP (following redirects) and uploads it to
    S3 under the given prefix, preserving the original filename.
    """
    if not url:
        logger.info("No GEOJSON_URL configured, skipping GeoJSON download.")
        return False

    filename = url.split("/")[-1] or "district.geojson"
    local_path = download_dir / filename

    logger.info(f"Downloading GeoJSON: {url}")
    response = requests.get(url, allow_redirects=True, stream=True)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    logger.info(f"Downloaded GeoJSON: {local_path.name} ({local_path.stat().st_size / 1024:.1f} KB)")

    s3_key = f"{s3_prefix}/{filename}" if s3_prefix else filename
    logger.info(f"Uploading GeoJSON -> s3://{s3_bucket}/{s3_key}")
    s3_client.upload_file(str(local_path), s3_bucket, s3_key)
    logger.info(f"SUCCESS: Uploaded {filename} to s3://{s3_bucket}/{s3_key}")

    try:
        local_path.unlink()
    except Exception:
        pass

    return True


# ============================================================
# MAIN INGESTION WORKFLOW
# ============================================================
def main():
    start_time = time.time()
    logger.info("=" * 70)
    logger.info("STARTING KAGGLE CROP YIELD & WEATHER DATASET INGESTION TO AWS S3")
    logger.info(f"Dataset   : {DATASET_NAME}")
    logger.info(f"S3 Bucket : {S3_BUCKET}")
    logger.info(f"S3 Prefix : {S3_PREFIX}")
    logger.info("=" * 70)

    # 1. Download raw ZIP file
    logger.info("Initiating dataset download...")
    zip_file_path = download_kaggle_dataset_zip(
        dataset_name=DATASET_NAME,
        download_dir=DOWNLOAD_DIR,
        username=KAGGLE_USERNAME,
        key=KAGGLE_KEY
    )

    # 2. Extract ZIP as-is (no transformation, no format conversion)
    logger.info(f"Extracting archive: {zip_file_path.name}")
    with zipfile.ZipFile(zip_file_path, "r") as zf:
        zf.extractall(EXTRACT_DIR)
        extracted_members = [m for m in zf.infolist() if not m.is_dir()]
    logger.info(f"Extracted {len(extracted_members)} file(s).")

    # 3. Upload every extracted file to S3, preserving original folder structure
    total_uploaded = 0
    total_failed = 0

    for file_path in EXTRACT_DIR.rglob("*"):
        if file_path.is_file():
            relative_path = file_path.relative_to(EXTRACT_DIR).as_posix()
            s3_key = f"{S3_PREFIX}/{relative_path}" if S3_PREFIX else relative_path

            try:
                logger.info(f"Uploading {relative_path} -> s3://{S3_BUCKET}/{s3_key}")
                s3_client.upload_file(str(file_path), S3_BUCKET, s3_key)
                total_uploaded += 1
            except Exception as err:
                logger.error(f"FAILED uploading {relative_path}: {err}", exc_info=True)
                total_failed += 1

    # 4. Download and upload the India district GeoJSON boundary file
    geojson_uploaded = False
    try:
        geojson_uploaded = download_and_upload_geojson(
            url=GEOJSON_URL,
            download_dir=DOWNLOAD_DIR,
            s3_bucket=S3_BUCKET,
            s3_prefix=GEOJSON_S3_PREFIX
        )
    except Exception as err:
        logger.error(f"FAILED downloading/uploading GeoJSON: {err}", exc_info=True)

    # 5. Cleanup local temp files
    try:
        if zip_file_path.exists():
            zip_file_path.unlink()
            logger.info(f"Deleted downloaded ZIP file: {zip_file_path.name}")
    except Exception as e:
        logger.warning(f"Could not delete ZIP file: {e}")

    for cleanup_dir in [EXTRACT_DIR, DOWNLOAD_DIR]:
        try:
            if cleanup_dir.exists():
                for f in cleanup_dir.rglob("*"):
                    if f.is_file():
                        f.unlink()
                for d in sorted(cleanup_dir.rglob("*"), reverse=True):
                    if d.is_dir():
                        d.rmdir()
                cleanup_dir.rmdir()
        except Exception as e:
            logger.warning(f"Cleanup notice: {e}")

    elapsed_minutes = round((time.time() - start_time) / 60, 2)
    logger.info("=" * 70)
    logger.info("INGESTION COMPLETED")
    logger.info(f"Total Time      : {elapsed_minutes} minutes")
    logger.info(f"Files Uploaded  : {total_uploaded}")
    logger.info(f"Files Failed    : {total_failed}")
    logger.info(f"S3 Destination  : s3://{S3_BUCKET}/{S3_PREFIX}/")
    logger.info(f"GeoJSON Uploaded: {geojson_uploaded} -> s3://{S3_BUCKET}/{GEOJSON_S3_PREFIX}/")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()