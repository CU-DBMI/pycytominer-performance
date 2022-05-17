# ---------------------------------------------------------------------------------------------------
# --- general google cloud inputs
# ---------------------------------------------------------------------------------------------------

variable "project" {
    default = "cuhealthai-sandbox"
}

variable "region" {
  default = "us-central1"
}

variable "zone" {
  default = "us-central1-a"
}

variable "credentials_file" {
    default = "../.secrets/sa.json"
}

variable "resource_prefix" {
    type = string
    default = "test-vm-"
    description = "prefix for all resources' names"
} 

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.10.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials_file)

  project = var.project
  region  = var.region
  zone    = var.zone
}
