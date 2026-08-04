output "bronze_bucket_name" {
  value = aws_s3_bucket.bronze_bucket.bucket
}

output "silver_bucket_name" {
  value = aws_s3_bucket.silver_bucket.bucket
}

output "gold_bucket_name" {
  value = aws_s3_bucket.gold_bucket.bucket
}

output "glue_database_name" {
  value = aws_glue_catalog_database.etl_db.name
}

output "bronze_job_name" {
  value = aws_glue_job.bronze_to_silver.name
}

output "silver_job_name" {
  value = aws_glue_job.silver_to_gold.name
}

output "silver_crawler_name" {
  value = aws_glue_crawler.silver_crawler.name
}

output "gold_crawler_name" {
  value = aws_glue_crawler.gold_crawler.name
}

# ---------------------------------------------------------------------
# EMR ingestion layout (consumed by the ingest-emr job in main.yml)
# ---------------------------------------------------------------------
output "emr_raw_zip_path" {
  value = local.emr_raw_zip_path
}

output "emr_scripts_prefix" {
  value = local.emr_scripts_prefix
}

output "emr_bootstrap_prefix" {
  value = local.emr_bootstrap_prefix
}

output "emr_logs_prefix" {
  value = local.emr_logs_prefix
}
