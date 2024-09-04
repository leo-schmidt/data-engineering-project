variable "credentials" {
  default = "./keys/key.json"
}

variable "project" {
  description = "Project name"
  default     = "de-playground-24"
}

variable "location" {
  description = "Project location"
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "BigQuery dataset name"
  default     = "demo_dataset"
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "Storage bucket name"
  default     = "de-playground-24-terra-bucket"
}
