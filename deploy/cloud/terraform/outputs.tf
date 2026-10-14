output "region" {
  value = var.region
}

output "cluster_name" {
  value = module.eks.cluster_name
}

output "cold_bucket" {
  value = aws_s3_bucket.cold.bucket
}
