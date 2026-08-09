resource "aws_s3_bucket" "bronze_bucket" {
  bucket = var.bronze_bucket
}
resource "aws_s3_bucket" "silver_bucket" {
  bucket = var.silver_bucket
}
resource "aws_s3_bucket" "gold_bucket" {
  bucket = var.gold_bucket
}

resource "aws_glue_catalog_database" "etl_db" {
  name = "weather_db31"
}

locals {
  glue_role_arn  = var.glue_role_arn
  glue_role_name = element(split("/", var.glue_role_arn), length(split("/", var.glue_role_arn)) - 1)
}

resource "aws_glue_job" "weather_ingestion" {

  name     = "weather_ingestion_bronze"
  role_arn = var.glue_role_arn

  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 10
  timeout           = 180

  command {
    name            = "glueetl"
    python_version  = "3.9"
    script_location = "s3://${var.bronze_bucket}/scripts/glue_weather.py"
  }

  default_arguments = {

    "--job-language" = "python"

    "--TempDir" = "s3://${var.glue_assets_bucket}/temporary/"

    "--S3_BUCKET" = var.bronze_bucket

    "--S3_PREFIX" = ""

    "--DATASET_NAME" = var.weather_dataset_name

    "--CHUNK_SIZE" = "100000"

    "--KAGGLE_USERNAME" = var.kaggle_username

    "--KAGGLE_KEY" = var.kaggle_key

    "--additional-python-modules" = "pandas,pyarrow,requests,kaggle,boto3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  max_retries = 0
}

resource "aws_glue_job" "crop_geojson_ingestion" {

  name     = "crop_geojson_ingestion_bronze"
  role_arn = var.glue_role_arn

  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 180

  command {
    name            = "glueetl"
    python_version  = "3.9"
    script_location = "s3://${var.bronze_bucket}/scripts/glue_crop.py"
  }

  default_arguments = {

    "--job-language" = "python"

    "--TempDir" = "s3://${var.glue_assets_bucket}/temporary/"

    "--S3_BUCKET" = var.bronze_bucket

    "--S3_PREFIX" = "crop/indian-historical-crop-yield-and-weather-data"

    "--DATASET_NAME" = var.crop_dataset_name

    "--KAGGLE_USERNAME" = var.kaggle_username

    "--KAGGLE_KEY" = var.kaggle_key

    "--GEOJSON_URL" = "https://raw.githubusercontent.com/geohacker/india/master/district/india_district.geojson"

    "--GEOJSON_S3_PREFIX" = "geojson"

    "--additional-python-modules" = "requests,kaggle,boto3"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  max_retries = 0
}

resource "aws_glue_job" "bronze_to_silver" {

  name     = "bronze_to_silver_transformation"
  role_arn = var.glue_role_arn

  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 10
  timeout           = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.bronze_bucket}/scripts/bronze_to_silver_glue.py"
  }

  default_arguments = {

    "--job-language" = "python"

    "--TempDir" = "s3://${var.glue_assets_bucket}/temporary/"

    "--JOB_NAME" = "bronze_to_silver_transformation"

    "--BRONZE_BUCKET" = "s3://${var.bronze_bucket}"

    "--SILVER_BUCKET" = "s3://${var.silver_bucket}/silver"

    "--CROP_DATA_INPUT_PATH" = "s3://${var.bronze_bucket}/crop/Custom_Crops_yield_Historical_Dataset.csv"

    "--CROP_DATA_OUTPUT_PATH" = "s3://${var.silver_bucket}/silver/crop_data"

    "--PERCENTAGE" = "0.30"

    "--MIN_CITIES" = "15"

    "--SEED" = "42"

    "--additional-python-modules" = "geopandas,pyarrow,shapely,fiona,pyproj,rtree,s3fs,pandas,numpy,boto3,scipy"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  max_retries = 0
}
resource "aws_glue_job" "silver_to_gold" {

  name     = "golden_layer"
  role_arn = var.glue_role_arn

  glue_version      = "5.1"
  worker_type       = "G.1X"
  number_of_workers = 10
  timeout           = 60

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${var.bronze_bucket}/scripts/silver_to_gold_glue.py"
  }

  default_arguments = {

    "--job-language" = "python"

    "--TempDir" = "s3://${var.glue_assets_bucket}/temporary/"

    "--JOB_NAME" = "golden_layer"

    "--SILVER_BUCKET" = "s3://${var.silver_bucket}/silver"

    "--GOLD_BUCKET" = "s3://${var.gold_bucket}/gold_updated"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  max_retries = 0
}

resource "aws_glue_crawler" "etl_crawler" {
  name          = var.glue_crawler_name
  role          = local.glue_role_name
  database_name = aws_glue_catalog_database.etl_db.name

  s3_target {
    path = "s3://${var.gold_bucket}/gold_updated/"
  }

  depends_on = [
  aws_glue_job.weather_ingestion,
  aws_glue_job.crop_geojson_ingestion,
  aws_glue_job.bronze_to_silver,
  aws_glue_job.silver_to_gold
]
}
  
  
resource "aws_s3_bucket" "athena_results_bucket" {
  bucket = var.athena_results_bucket
}

resource "aws_athena_workgroup" "etl_workgroup" {
  name = var.athena_workgroup_name

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${var.athena_results_bucket}/query-results/"
    }
  }

  depends_on = [aws_s3_bucket.athena_results_bucket]
}