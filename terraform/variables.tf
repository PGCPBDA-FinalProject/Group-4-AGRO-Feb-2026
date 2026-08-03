# ---------------------------------------------------------------------
# Region
# ---------------------------------------------------------------------
variable "region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

# ---------------------------------------------------------------------
# S3 buckets
# ---------------------------------------------------------------------
# Bronze already exists (created/owned by the ingestion scripts:
# convert_city_master.py, convert_wd1/2/3.py, geojson.py). Terraform
# only reads it via a data source — it does not create or own it.
variable "bucket_name_bronze" {
  description = "Existing S3 bucket holding raw/bronze data"
  type        = string
  default     = "agri-weather-dataset1"
}

variable "bucket_name_silver" {
  description = "S3 bucket for the Silver layer (managed by Terraform)"
  type        = string
  default     = "agroweatherdatalake"
}

variable "bucket_name_gold" {
  description = "S3 bucket for the Gold layer (managed by Terraform)"
  type        = string
  default     = "agroweatherdatalake2"
}

# ---------------------------------------------------------------------
# Glue Catalog
# ---------------------------------------------------------------------
variable "glue_database_name" {
  description = "Glue Catalog database name shared by both crawlers"
  type        = string
  default     = "weather_db28"
}

# ---------------------------------------------------------------------
# Glue Jobs
# ---------------------------------------------------------------------
variable "glue_job_name_bronze_to_silver" {
  description = "Name of the Bronze -> Silver Glue job"
  type        = string
  default     = "bronze-to-silver-job"
}

variable "glue_job_name_silver_to_gold" {
  description = "Name of the Silver -> Gold Glue job"
  type        = string
  default     = "silver-to-gold-job"
}

# ---------------------------------------------------------------------
# Glue Crawlers
# ---------------------------------------------------------------------
variable "glue_crawler_name_silver" {
  description = "Name of the crawler that catalogs the Silver layer"
  type        = string
  default     = "silver-layer-crawler"
}

variable "glue_crawler_name_gold" {
  description = "Name of the crawler that catalogs the Gold layer"
  type        = string
  default     = "gold-layer-crawler"
}

# ---------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------
variable "glue_role_arn" {
  description = "IAM role ARN used by both Glue Jobs and both Glue Crawlers"
  type        = string
}

# ---------------------------------------------------------------------
# EMR (Raw -> Bronze ingestion: geojson.py, convert_city_master.py,
# convert_wd1/2/3.py). The cluster itself is transient and created by
# the GitHub Actions workflow (see ingest-emr job), not by Terraform —
# these variables just document/standardize the S3 layout and the
# IAM/network inputs the workflow needs to launch it.
# ---------------------------------------------------------------------
variable "emr_release_label" {
  description = "EMR release label for the transient raw-to-bronze ingestion cluster"
  type        = string
  default     = "emr-7.1.0"
}

variable "emr_master_instance_type" {
  description = "Instance type for the EMR master node"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_type" {
  description = "Instance type for EMR core nodes"
  type        = string
  default     = "m5.xlarge"
}

variable "emr_core_instance_count" {
  description = "Number of EMR core nodes"
  type        = number
  default     = 2
}

variable "emr_service_role" {
  description = "IAM role name/ARN EMR uses to manage cluster resources (e.g. EMR_DefaultRole or a custom equivalent)"
  type        = string
}

variable "emr_instance_profile" {
  description = "IAM instance profile name EC2 instances in the cluster assume (e.g. EMR_EC2_DefaultRole or a custom equivalent)"
  type        = string
}

variable "emr_subnet_id" {
  description = "Subnet ID to launch the EMR cluster into"
  type        = string
}
