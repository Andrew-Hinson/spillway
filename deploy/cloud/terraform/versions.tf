terraform {
  required_version = ">= 1.16"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.68"
    }
  }

  # State stays in this directory (gitignored). The environment lives for one
  # measurement session and is destroyed afterwards, so a remote backend would
  # be one more thing to create and remember to delete.
}

provider "aws" {
  region = var.region
  # The [default] profile in ~/.aws, unless AWS_PROFILE says otherwise.

  # Everything Terraform creates carries these, so leftovers are easy to find:
  # aws resourcegroupstaggingapi get-resources --tag-filters Key=spillway,Values=<cluster>
  default_tags {
    tags = {
      spillway = var.cluster_name
    }
  }
}
