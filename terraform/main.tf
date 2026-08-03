# ---------------------------------------------------------------------
# Bronze bucket already exists (created by the ingestion scripts) —
# reference it, don't try to (re)create it.
# ---------------------------------------------------------------------
data "aws_s3_bucket" "bronze_bucket" {
  bucket = var.bucket_name_bronze
}

data "aws_s3_bucket" "silver_bucket" {
  bucket = var.bucket_name_silver
}

data "aws_s3_bucket" "gold_bucket" {
  bucket = var.bucket_name_gold
}

data "aws_glue_catalog_database" "etl_db" {
  name = var.glue_database_name
}

locals {
  glue_role_arn  = var.glue_role_arn
  bronze_s3_path = "s3://${var.bucket_name_bronze}/bronze"
  silver_s3_path = "s3://${var.bucket_name_silver}/silver"
  gold_s3_path   = "s3://${var.bucket_name_gold}/gold_updated"

  # Layout inside the bronze bucket used by the transient EMR ingestion
  # cluster (raw Kaggle zip in, ingestion scripts + bootstrap action,
  # cluster logs). Kept alongside bronze/ so bronze data and EMR
  # tooling/logs never collide.
  emr_raw_zip_path     = "s3://${var.bucket_name_bronze}/raw/indian-5000-cities-weather-data.zip"
  emr_scripts_prefix   = "s3://${var.bucket_name_bronze}/emr/scripts"
  emr_bootstrap_prefix = "s3://${var.bucket_name_bronze}/emr/bootstrap"
  emr_logs_prefix      = "s3://${var.bucket_name_bronze}/emr/logs"
}

# ---------------------------------------------------------------------
# Glue Job: Bronze -> Silver
# ---------------------------------------------------------------------
data "aws_glue_job" "bronze_to_silver" {
  name     = var.glue_job_name_bronze_to_silver
  role_arn = local.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name_silver}/scripts/bronze_to_silver_glue.py"
    python_version  = "3"
  }

  # These map 1:1 to the getResolvedOptions() keys read at the top of
  # bronze_to_silver_glue.py. Without these, the job silently falls back
  # to the hardcoded DEFAULT_ARGS in the script, which point at buckets
  # that don't exist in this Terraform config.
  default_arguments = {
    "--JOB_NAME"                          = var.glue_job_name_bronze_to_silver
    "--BRONZE_BUCKET"                     = local.bronze_s3_path
    "--SILVER_BUCKET"                     = local.silver_s3_path
    "--CROP_DATA_INPUT_PATH"              = "${local.bronze_s3_path}/crop/Custom_Crops_yield_Historical_Dataset.csv"
    "--CROP_DATA_OUTPUT_PATH"             = "${local.silver_s3_path}/crop_data"
    "--PERCENTAGE"                        = "0.30"
    "--MIN_CITIES"                        = "15"
    "--SEED"                              = "42"
    "--enable-continuous-cloudwatch-log"  = "true"
    "--enable-metrics"                    = "true"
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
}

# ---------------------------------------------------------------------
# Glue Job: Silver -> Gold
# ---------------------------------------------------------------------
data "aws_glue_job" "silver_to_gold" {
  name     = var.glue_job_name_silver_to_gold
  role_arn = local.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name_gold}/scripts/silver_to_gold_glue.py"
    python_version  = "3"
  }

  # Maps to the JOB_NAME / SILVER_BUCKET / GOLD_BUCKET keys read by
  # initialize_glue() in silver_to_gold_glue.py.
  default_arguments = {
    "--JOB_NAME"                          = var.glue_job_name_silver_to_gold
    "--SILVER_BUCKET"                     = local.silver_s3_path
    "--GOLD_BUCKET"                       = local.gold_s3_path
    "--enable-continuous-cloudwatch-log"  = "true"
    "--enable-metrics"                    = "true"
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 10
}

# ---------------------------------------------------------------------
# Crawler: Silver layer
# Points at the actual write path used by bronze_to_silver_glue.py
# (dim_region/, dim_state/, dim_city/, fact_weather/, fact_weather_sampled/,
# crop_data/ all live directly under <silver_bucket>/silver/).
# ---------------------------------------------------------------------
data "aws_glue_crawler" "silver_crawler" {
  name          = var.glue_crawler_name_silver
  role          = local.glue_role_arn
  database_name = aws_glue_catalog_database.etl_db.name

  s3_target {
    path = "${local.silver_s3_path}/"
  }

  depends_on = [aws_glue_job.bronze_to_silver]
}

# ---------------------------------------------------------------------
# Crawler: Gold layer
# Points at <gold_bucket>/gold_updated/ where dims/, daily_weather/,
# season_shift/, renewable_ranking/, ml_dataset/, city_weather_profile/
# all live (per silver_to_gold_glue.py's write_dataset() calls).
# ---------------------------------------------------------------------
data "aws_glue_crawler" "gold_crawler" {
  name          = var.glue_crawler_name_gold
  role          = local.glue_role_arn
  database_name = aws_glue_catalog_database.etl_db.name

  s3_target {
    path = "${local.gold_s3_path}/"
  }

  depends_on = [aws_glue_job.silver_to_gold]
}
