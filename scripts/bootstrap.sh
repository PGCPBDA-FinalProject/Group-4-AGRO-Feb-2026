#!/bin/bash
set -euo pipefail

# ---------------------------------------------------------------------
# EMR Bootstrap: Raw -> Bronze ingestion prep
#
# Runs once per node at cluster launch. This script:
#   1. Installs the Python deps the ingestion scripts need
#      (not preinstalled on the EMR AMI).
#   2. Downloads the ingestion scripts from S3 onto the master node
#      under /home/hadoop, matching the paths convert_*.py hardcode.
#   3. Downloads the raw Kaggle zip that kaggletos3zip.py already
#      uploaded to S3, so convert_wd1/2/3.py have a local file to read.
#
# NOTE: --bootstrap-actions runs on every node (master + core) by
# default. If you only need this on the master node, gate the whole
# body below on `IS_MASTER`.
# ---------------------------------------------------------------------

BUCKET="$1"   # passed in via --bootstrap-actions Args=[...]
LOCAL_DIR="/home/hadoop"

if [ -z "${BUCKET:-}" ]; then
  echo "ERROR: no bucket name passed as arg 1 to bootstrap.sh"
  exit 1
fi

# --- Determine if this node is the master ---
IS_MASTER=$(cat /mnt/var/lib/info/instance.json | python3 -c "import sys, json; print(json.load(sys.stdin)['isMaster'])")

if [ "$IS_MASTER" != "True" ]; then
  echo "Not the master node - skipping bootstrap work."
  exit 0
fi

echo "Running bootstrap on master node..."

# --- 1. Install Python dependencies ---
sudo pip3 install --upgrade pip
sudo pip3 install \
  pandas \
  boto3 \
  requests

# If geojson.py needs geospatial libs, uncomment:
# sudo pip3 install geopandas shapely

# --- 2. Download ingestion scripts from S3 ---
mkdir -p "$LOCAL_DIR"

aws s3 cp "s3://${BUCKET}/emr/scripts/kaggletos3zip.py" "${LOCAL_DIR}/kaggletos3zip.py"
aws s3 cp "s3://${BUCKET}/emr/scripts/geojson.py" "${LOCAL_DIR}/geojson.py"
aws s3 cp "s3://${BUCKET}/emr/scripts/convert_city_master.py" "${LOCAL_DIR}/convert_city_master.py"
aws s3 cp "s3://${BUCKET}/emr/scripts/convert_wd1.py" "${LOCAL_DIR}/convert_wd1.py"
aws s3 cp "s3://${BUCKET}/emr/scripts/convert_wd2.py" "${LOCAL_DIR}/convert_wd2.py"
aws s3 cp "s3://${BUCKET}/emr/scripts/convert_wd3.py" "${LOCAL_DIR}/convert_wd3.py"

# --- 3. Stage the raw Kaggle zip locally ---
# Adjust the S3 key/filename below to match what kaggletos3zip.py
# actually names the uploaded zip.
aws s3 cp "s3://${BUCKET}/raw/indian-5000-cities-weather-data.zip" \
  "${LOCAL_DIR}/indian-5000-cities-weather-data.zip"

echo "Bootstrap complete."