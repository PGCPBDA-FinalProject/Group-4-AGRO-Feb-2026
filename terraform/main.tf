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

resource "aws_glue_job" "bronze_to_silver" {

  name     = "bronze_to_silver_transformation"
  role_arn = var.glue_role_arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 10
  timeout           = 60

  command {
    name            = "glueet"
    python_version  = "3"
    script_location = "s3://${var.glue_assets_bucket}/scripts/bronze_to_silver_glue.py"
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

    "--additional-python-modules" = "geopandas,pyarrow,shapely,fiona,pyproj,rtree,s3fs"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  max_retries = 0
}
resource "aws_glue_job" "silver_to_gold" {

  name     = "golden_layer"
  role_arn = var.glue_role_arn

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 10
  timeout           = 60

  command {
    name            = "glueet"
    python_version  = "3"
    script_location = "s3://${var.glue_assets_bucket}/scripts/silver_to_gold_glue.py"
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
  aws_glue_job.bronze_to_silver,
  aws_glue_job.silver_to_gold
]
}