import os
import shutil
import zipfile
from pathlib import Path

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from kaggle.api.kaggle_api_extended import KaggleApi

# ============================================================
# CONFIGURATION
# ============================================================

DATASET = "mukeshdevrath007/indian-5000-cities-weather-data"

S3_BUCKET = "agri-weather-dataset"
S3_PREFIX = "bronze-level"

DOWNLOAD_DIR = Path("/home/hadoop/kaggle_download")
EXTRACT_DIR = Path("/home/hadoop/weather_extract")
PARQUET_DIR = Path("/home/hadoop/parquet_temp")

CHUNK_SIZE = 500_000
FOLDERS = ["Weather_Data_Scraping_and_Analysis","w_d_1", "w_d_2", "w_d_3"]

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
EXTRACT_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_DIR.mkdir(parents=True, exist_ok=True)

# ============================================================
# KAGGLE AUTHENTICATION
# ============================================================
# Before running, either:
# 1) Put kaggle.json in /home/hadoop/.config/kaggle/kaggle.json
# OR
# 2) Export KAGGLE_USERNAME and KAGGLE_KEY environment variables.

print("=" * 70)
print("AUTHENTICATING WITH KAGGLE")
print("=" * 70)

api = KaggleApi()
api.authenticate()

print("Kaggle authentication successful.")

# ============================================================
# AWS S3
# ============================================================

s3 = boto3.client("s3")

# ============================================================
# DOWNLOAD DATASET ZIP ONCE - DO NOT EXTRACT WHOLE DATASET
# ============================================================

print("\n" + "=" * 70)
print("DOWNLOADING KAGGLE DATASET ZIP")
print("=" * 70)

api.dataset_download_files(
    DATASET,
    path=str(DOWNLOAD_DIR),
    unzip=False,
    quiet=False
)

zip_files = list(DOWNLOAD_DIR.glob("*.zip"))

if not zip_files:
    raise RuntimeError(f"No ZIP file found in {DOWNLOAD_DIR}")

zip_path = max(zip_files, key=lambda p: p.stat().st_size)

print("\nZIP downloaded successfully:")
print(zip_path)
print(f"ZIP size: {zip_path.stat().st_size / (1024 ** 3):.2f} GB")


# ============================================================
# EXTRACT ONLY ONE w_d_* FOLDER
# ============================================================

def extract_one_folder(zip_path, folder_name):
    if EXTRACT_DIR.exists():
        shutil.rmtree(EXTRACT_DIR)

    EXTRACT_DIR.mkdir(parents=True, exist_ok=True)

    extracted = 0

    print("\n" + "=" * 70)
    print(f"EXTRACTING ONLY {folder_name}")
    print("=" * 70)

    with zipfile.ZipFile(zip_path, "r") as zf:
        for member in zf.infolist():
            if member.is_dir():
                continue

            parts = Path(member.filename).parts

            # Handles:
            # Weather_Data_Scraping_and_Analysis/w_d_1/Abbigeri.csv
            # Weather_Data_Scraping_and_Analysis/w_d_3/w_d_3/Obra.csv
            if folder_name not in parts:
                continue

            if not member.filename.lower().endswith(".csv"):
                continue

            zf.extract(member, EXTRACT_DIR)
            extracted += 1

    print(f"Extracted {extracted} CSV files from {folder_name}")
    return extracted


# ============================================================
# PROCESS ONE CITY CSV
# ============================================================

def process_city(csv_path, folder_name):
    city = csv_path.stem
    parquet_path = PARQUET_DIR / f"{city}.parquet"

    writer = None

    try:
        for chunk_number, chunk in enumerate(
            pd.read_csv(
                csv_path,
                chunksize=CHUNK_SIZE,
                low_memory=False
            ),
            start=1
        ):
            print(
                f"    Chunk {chunk_number}: "
                f"{len(chunk):,} rows"
            )

            # City comes from CSV filename
            chunk["city"] = city

            table = pa.Table.from_pandas(
                chunk,
                preserve_index=False
            )

            if writer is None:
                writer = pq.ParquetWriter(
                    str(parquet_path),
                    table.schema,
                    compression="snappy"
                )

            writer.write_table(table)

        if writer is not None:
            writer.close()
            writer = None

        if not parquet_path.exists():
            raise RuntimeError(f"Parquet was not created for {city}")

        s3_key = (
            f"{S3_PREFIX}/"
            f"{folder_name}/"
            f"{city}.parquet"
        )

        print(
            f"    Uploading -> "
            f"s3://{S3_BUCKET}/{s3_key}"
        )

        s3.upload_file(
            str(parquet_path),
            S3_BUCKET,
            s3_key
        )

        print(f"    SUCCESS: {city}")

        # Delete local CSV only after successful S3 upload
        if csv_path.exists():
            csv_path.unlink()

        # Delete local temporary Parquet
        if parquet_path.exists():
            parquet_path.unlink()

        return True

    except Exception as e:
        print(f"    FAILED: {city}")
        print(f"    {type(e).__name__}: {e}")

        if parquet_path.exists():
            parquet_path.unlink()

        return False

    finally:
        if writer is not None:
            writer.close()


# ============================================================
# PROCESS w_d_1 -> w_d_2 -> w_d_3
# ============================================================

total_uploaded = 0
total_failed = 0

for folder_name in FOLDERS:

    extracted_count = extract_one_folder(
        zip_path,
        folder_name
    )

    if extracted_count == 0:
        raise RuntimeError(
            f"No CSV files found for {folder_name} inside ZIP"
        )

    # Recursive because w_d_3 contains another w_d_3 directory
    csv_files = sorted(EXTRACT_DIR.rglob("*.csv"))

    print(f"\nProcessing {len(csv_files)} files from {folder_name}")

    folder_failed = 0

    for index, csv_path in enumerate(csv_files, start=1):
        print("\n" + "-" * 70)
        print(
            f"[{folder_name}] "
            f"[{index}/{len(csv_files)}] "
            f"{csv_path.stem}"
        )

        success = process_city(
            csv_path,
            folder_name
        )

        if success:
            total_uploaded += 1
        else:
            total_failed += 1
            folder_failed += 1

    if folder_failed > 0:
        print("\n" + "!" * 70)
        print(f"{folder_name}: {folder_failed} files failed.")
        print("Failed source CSV files are kept locally.")
        print("Stopping before processing the next folder.")
        print("!" * 70)
        raise RuntimeError(
            f"{folder_name} has failed files."
        )

    # All files from this folder are safely in S3
    if EXTRACT_DIR.exists():
        shutil.rmtree(EXTRACT_DIR)
        EXTRACT_DIR.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 70)
    print(f"{folder_name} COMPLETED AND DELETED FROM EMR")
    print("=" * 70)


# ============================================================
# DELETE ZIP ONLY AFTER ALL FOLDERS COMPLETE
# ============================================================

if zip_path.exists():
    print("\nDeleting Kaggle dataset ZIP...")
    zip_path.unlink()
    print("ZIP deleted.")


# ============================================================
# CLEAN EMPTY DIRECTORIES
# ============================================================

for directory in [EXTRACT_DIR, PARQUET_DIR, DOWNLOAD_DIR]:
    try:
        if directory.exists():
            directory.rmdir()
    except OSError:
        pass


# ============================================================
# SUMMARY
# ============================================================

print("\n" + "=" * 70)
print("INGESTION COMPLETED")
print("=" * 70)
print(f"Uploaded : {total_uploaded}")
print(f"Failed   : {total_failed}")
print(f"S3       : s3://{S3_BUCKET}/{S3_PREFIX}/")
print("=" * 70)
