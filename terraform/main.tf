resource "aws_s3_bucket" "weather-dataset" {

    bucket = "agro-weather-dataset"

}

resource "aws_glue_catalog_database" "etl_db" {
  name = "weather_db28"
}

locals {
  glue_role_arn = 
}

resource "aws_glue_job" "etl_job" {
  name     = 
  role_arn = 

  command {
    name            = "glueetl" # change not
    script_location = 
    python_version  = "3"
  }

  glue_version      = "5.0"  # GLUE VERSION
  number_of_workers = 4      # NUMBER OF WORKERS
  worker_type       = "G.1X" # WORKER TYPE  4 CPU AND 16GB
}

resource "aws_glue_crawler" "etl_crawler" {
  name          = 
  role          = 
  database_name = 

  s3_target {
    path = "s3://"
  }

  depends_on = [aws_glue_job.etl_job]
}