resource "aws_s3_bucket" "silver_bucket" {
  bucket = var.bucket_name_silver
}
resource "aws_s3_bucket" "gold_bucket" {
  bucket = var.bucket_name_gold
}

resource "aws_glue_catalog_database" "etl_db" {
  name = "weather_db28"
}

locals {
  glue_role_arn = var.glue_role_arn
}

resource "aws_glue_job" "bronze_to_silver" {
  name     = "bronze-to-silver-job"
  role_arn = local.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name_silver}/scripts/bronze_to_silver.py"
    python_version  = "3"
  }

  glue_version     = "5.0"
  worker_type      = "G.1X"
  number_of_workers = 2
}
resource "aws_glue_job" "silver_to_gold" {
  name     = "silver-to-gold-job"
  role_arn = local.glue_role_arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name_gold}/scripts/silver_to_gold.py"
    python_version  = "3"
  }

  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
}

resource "aws_glue_crawler" "etl_crawler" {
  name          = var.glue_crawler_name
  role          = local.glue_role_arn
  database_name = aws_glue_catalog_database.etl_db.name

  s3_target {
    path = "s3://${var.bucket_name_silver}/weatherdata/"
  }

  depends_on = [
  aws_glue_job.bronze_to_silver,
  aws_glue_job.silver_to_gold
]
}