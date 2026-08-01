output "bronze_job_name" {
  value = aws_glue_job.bronze_to_silver.name
}

output "silver_job_name" {
  value = aws_glue_job.silver_to_gold.name
}

output "glue_crawler_name" {
  value = aws_glue_crawler.etl_crawler.name
}