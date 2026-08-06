import os
import subprocess
import boto3
from pathlib import Path

# ==============================
# CONFIGURATION
# ==============================
# Credentials come from environment variables (set via GitHub Secrets
# in the workflow's `env:` block) -- never hardcode API keys in source.
KAGGLE_USERNAME = os.environ["KAGGLE_USERNAME"]
KAGGLE_KEY = os.environ["KAGGLE_KEY"]

DATASET = os.environ.get("DATASET", "mukeshdevrath007/indian-5000-cities-weather-data")
S3_BUCKET = os.environ["S3_BUCKET"]
S3_KEY = "raw/indian-5000-cities-weather-data.zip"

# NOTE: this script runs on the GitHub Actions runner (ubuntu-latest),
# NOT on the EMR cluster -- there is no "hadoop" user/home dir here.
# Use a directory the runner actually owns.
DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "kaggle_download")

# ==============================
# CREATE KAGGLE CONFIG
# ==============================
kaggle_dir = os.path.expanduser("~/.kaggle")
os.makedirs(kaggle_dir, exist_ok=True)

with open(os.path.join(kaggle_dir, "kaggle.json"), "w") as f:
    f.write(f'{{"username":"{KAGGLE_USERNAME}","key":"{KAGGLE_KEY}"}}')

os.chmod(os.path.join(kaggle_dir, "kaggle.json"), 0o600)

# ==============================
# INSTALL KAGGLE CLI
# ==============================
subprocess.run(
    ["python3", "-m", "pip", "install", "--user", "kaggle==1.6.17"],
    check=True,
)
os.environ["PATH"] += ":" + os.path.expanduser("~/.local/bin")

# ==============================
# DOWNLOAD ZIP
# ==============================
Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)

print("Downloading Kaggle ZIP...")
subprocess.run(
    ["kaggle", "datasets", "download", "-d", DATASET, "-p", DOWNLOAD_DIR, "--force"],
    check=True,
)

zip_file = os.path.join(DOWNLOAD_DIR, "indian-5000-cities-weather-data.zip")

if not os.path.exists(zip_file):
    raise Exception("ZIP file not found after download.")

print("ZIP downloaded successfully.")

# ==============================
# UPLOAD TO S3
# ==============================
print("Uploading ZIP to S3...")
s3 = boto3.client("s3")
s3.upload_file(zip_file, S3_BUCKET, S3_KEY)
print(f"Upload complete: s3://{S3_BUCKET}/{S3_KEY}")

# ==============================
# CLEANUP
# ==============================
os.remove(zip_file)
print("Local ZIP deleted. Done.")