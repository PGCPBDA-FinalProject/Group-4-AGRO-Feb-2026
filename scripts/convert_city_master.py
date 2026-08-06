import boto3
import zipfile
import pandas as pd
import os

# ==========================
# CONFIGURATION
# ==========================

BUCKET = "agri-weather-dataset1"
ZIP_KEY = "raw/indian-5000-cities-weather-data.zip"

LOCAL_ZIP = "/home/hadoop/kaggle/indian-5000-cities-weather-data.zip"

OUTPUT_PATH = "/home/hadoop/parquet/city_master"

os.makedirs(OUTPUT_PATH, exist_ok=True)

# ==========================
# DOWNLOAD ZIP FROM S3
# ==========================

s3 = boto3.client("s3")

print("Downloading ZIP...")


# ==========================
# READ FILE INSIDE ZIP
# ==========================

with zipfile.ZipFile(LOCAL_ZIP, "r") as z:

    with z.open(
        "Weather_Data_Scraping_and_Analysis/weather.csv"
    ) as f:

        df = pd.read_csv(f)

if "Unnamed: 0" in df.columns:
    df.drop(columns=["Unnamed: 0"], inplace=True)

# ==========================
# WRITE PARQUET
# ==========================

output_file = os.path.join(
    OUTPUT_PATH,
    "city_master.parquet"
)

df.to_parquet(
    output_file,
    engine="pyarrow",
    compression="snappy",
    index=False
)

print("city_master.parquet created successfully")

# ==========================
# UPLOAD PARQUET TO S3
# ==========================

s3.upload_file(
    output_file,
    BUCKET,
    "bronze/city_master/city_master.parquet"
)

print("Uploaded to S3")

# ==========================
# CLEANUP
# ==========================

os.remove(output_file)


print("Temporary files deleted")