import requests
import boto3
import os

# ==========================
# CONFIGURATION
# ==========================

URL = "https://raw.githubusercontent.com/geohacker/india/master/district/india_district.geojson"

LOCAL_FILE = "/home/hadoop/india_district.geojson"

BUCKET = "agri-weather-dataset1"

S3_KEY = "bronze/geojson/india_district.geojson"

# ==========================
# DOWNLOAD FILE
# ==========================

print("Downloading GeoJSON...")

response = requests.get(URL, stream=True)
response.raise_for_status()

with open(LOCAL_FILE, "wb") as f:
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            f.write(chunk)

print("Download Completed")

# ==========================
# UPLOAD TO S3
# ==========================

s3 = boto3.client("s3")

print("Uploading to S3...")

s3.upload_file(
    LOCAL_FILE,
    BUCKET,
    S3_KEY
)

print("Upload Completed")

# ==========================
# CLEANUP
# ==========================

os.remove(LOCAL_FILE)

print("Local file deleted")