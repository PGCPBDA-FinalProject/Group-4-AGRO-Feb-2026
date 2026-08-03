import boto3
import zipfile
import pandas as pd
import os
import time

# ===========================
# CONFIGURATION
# ===========================

BUCKET = "agri-weather-dataset1"

ZIP_KEY = "raw/indian-5000-cities-weather-data.zip"

LOCAL_ZIP = "/home/hadoop/kaggle/indian-5000-cities-weather-data.zip"

TEMP_DIR = "/home/hadoop/temp"

os.makedirs(TEMP_DIR, exist_ok=True)

start = time.time()

# ===========================
# DOWNLOAD ZIP
# ===========================

s3 = boto3.client("s3")


print("Download Completed")

# ===========================
# OPEN ZIP
# ===========================

with zipfile.ZipFile(LOCAL_ZIP, "r") as z:

    csv_files = [
        f for f in z.namelist()
        if f.startswith("w_d_1/")
        and f.endswith(".csv")
    ]

    print("Total Files:", len(csv_files))

    for i, member in enumerate(csv_files, start=1):

        city = os.path.basename(member).replace(".csv", "")

        print(f"[{i}/{len(csv_files)}] {city}")

        # -----------------------
        # Extract only one file
        # -----------------------

        extracted_path = z.extract(
            member,
            TEMP_DIR
        )

        # -----------------------
        # Read CSV
        # -----------------------

        df = pd.read_csv(
            extracted_path,
            encoding="utf-8-sig"
        )

        if "Unnamed: 0" in df.columns:
            df.drop(
                columns=["Unnamed: 0"],
                inplace=True
            )

        df["city"] = city

        parquet_file = f"/home/hadoop/{city}.parquet"

        df.to_parquet(
            parquet_file,
            engine="pyarrow",
            compression="snappy",
            index=False
        )

        # -----------------------
        # Upload to S3
        # -----------------------

        s3.upload_file(
            parquet_file,
            BUCKET,
            f"bronze/wd1/{city}.parquet"
        )

        # -----------------------
        # Delete temporary files
        # -----------------------

        os.remove(parquet_file)
        os.remove(extracted_path)

# ===========================
# CLEANUP
# ===========================



print("Completed")

print(
    "Time:",
    round((time.time()-start)/60,2),
    "minutes"
)