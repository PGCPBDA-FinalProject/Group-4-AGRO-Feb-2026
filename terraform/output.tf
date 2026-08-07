output "bronze_job_name" {
  value = aws_glue_job.bronze_to_silver.name
}

output "weather_ingestion_job_name" {
  value = aws_glue_job.weather_ingestion.name
}

output "crop_geojson_ingestion_job_name" {
  value = aws_glue_job.crop_geojson_ingestion.name
}

output "gold_job_name" {
  value = aws_glue_job.silver_to_gold.name
}

output "glue_crawler_name" {
  value = aws_glue_crawler.etl_crawler.name
}
output "athena_database_name" {
  value = aws_glue_catalog_database.etl_db.name
}

output "athena_workgroup_name" {
  value = aws_athena_workgroup.etl_workgroup.name
}

output "athena_results_bucket" {
  value = aws_s3_bucket.athena_results_bucket.bucket
}