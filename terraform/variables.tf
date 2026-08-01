#declare a region
variable "region" {
  default = "us-east-1"
}

#declare a bucket name
variable "bucket_name_silver" {
  default = "agroweatherdatalake"
}
variable "bucket_name_gold" {
  default = "agroweatherdatalake2"
}


#declare a glue job name
variable "glue_job_name" {
  default = "glue-etl-job28"
}

#declare a crawler name
variable "glue_crawler_name" {
  default = "my-etl-crawler28"
}


variable "glue_role_arn" {
  description = "IAM role ARN to use for Glue Job"
  type        = string
}