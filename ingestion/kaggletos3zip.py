import os
import subprocess
import boto3
from pathlib import Path

# ==============================
# CONFIGURATION
# ==============================

KAGGLE_USERNAME = "parighaindulkar"
KAGGLE_KEY = "845da1b03f9540303fd5283ab862777b"

# DATASET = "mukeshdevrath007/indian-5000-cities-weather-data"

# DOWNLOAD_DIR = "/home/hadoop/kaggle"

# S3_BUCKET = "agri-weather-dataset1"
# S3_KEY = "raw/indian-5000-cities-weather-data.zip"

# # ==============================
# # CREATE KAGGLE CONFIG
# # ==============================

# os.makedirs(os.path.expanduser("~/.kaggle"), exist_ok=True)

# with open(os.path.expanduser("~/.kaggle/kaggle.json"), "w") as f:
#     f.write(
#         f'{{"username":"{KAGGLE_USERNAME}","key":"{KAGGLE_KEY}"}}'
#     )

# os.chmod(os.path.expanduser("~/.kaggle/kaggle.json"), 0o600)

# # ==============================
# # INSTALL KAGGLE
# # ==============================

# subprocess.run(
#     ["python3", "-m", "pip", "install", "--user", "kaggle==1.6.17"],
#     check=True
# )

# os.environ["PATH"] += ":" + os.path.expanduser("~/.local/bin")

# # ==============================
# # DOWNLOAD ZIP
# # ==============================

# Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)

# print("Downloading Kaggle ZIP...")

# subprocess.run(
#     [
#         "kaggle",
#         "datasets",
#         "download",
#         "-d",
#         DATASET,
#         "-p",
#         DOWNLOAD_DIR,
#         "--force"
#     ],
#     check=True
# )

# zip_file = os.path.join(
#     DOWNLOAD_DIR,
#     "indian-5000-cities-weather-data.zip"
# )

# if not os.path.exists(zip_file):
#     raise Exception("ZIP file not found.")

# print("ZIP Downloaded Successfully")

# # ==============================
# # UPLOAD TO S3
# # ==============================

# print("Uploading ZIP to S3...")

# s3 = boto3.client("s3")

# s3.upload_file(
#     zip_file,
#     S3_BUCKET,
#     S3_KEY
# )

# print("Upload Completed")

# # ==============================
# # CLEANUP
# # ==============================

# os.remove(zip_file)

# print("Local ZIP Deleted")
# print("Pipeline Finished Successfully")

DATASET="mukeshdevrath007/indian-5000-cities-weather-data"

DOWNLOAD_DIR="/home/hadoop/kaggle"

BUCKET="agri-weather-dataset1"

os.makedirs(os.path.expanduser("~/.kaggle"),exist_ok=True)

with open(os.path.expanduser("~/.kaggle/kaggle.json"),"w") as f:
    f.write(
        f'{{"username":"{KAGGLE_USERNAME}","key":"{KAGGLE_KEY}"}}'
    )

os.chmod(os.path.expanduser("~/.kaggle/kaggle.json"),0o600)

subprocess.run(
    ["python3","-m","pip","install","--user","kaggle==1.6.17"],
    check=True
)

os.environ["PATH"]+=":"+os.path.expanduser("~/.local/bin")

Path(DOWNLOAD_DIR).mkdir(parents=True,exist_ok=True)

subprocess.run([
    "kaggle",
    "datasets",
    "download",
    "-d",
    DATASET,
    "-p",
    DOWNLOAD_DIR,
    "--force"
],check=True)

zip_file=os.path.join(
    DOWNLOAD_DIR,
    "indian-5000-cities-weather-data.zip"
)

s3=boto3.client("s3")

s3.upload_file(
    zip_file,
    BUCKET,
    "raw/indian-5000-cities-weather-data.zip"
)

print("ZIP Uploaded")