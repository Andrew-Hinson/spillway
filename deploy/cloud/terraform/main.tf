# Spillway's cloud environment for M4.3: an EKS cluster that Argo CD fills
# from this repo (deploy/cloud/apps), and a real S3 bucket for the cold path.
# It's built for one measurement session and then destroyed: see
# `make cloud-plan`, `make cloud-up` and `make cloud-down`.

data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_caller_identity" "current" {}

locals {
  azs = slice(data.aws_availability_zones.available.names, 0, 2)
}

# Public subnets only: no NAT gateway to pay for by the hour and by the
# gigabyte. Nodes get public addresses for egress (Wikimedia, GitHub, image
# pulls); their security group still admits nothing from outside the cluster.
module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "6.7.3"

  name = var.cluster_name
  cidr = "10.40.0.0/16"
  azs  = local.azs

  public_subnets          = ["10.40.0.0/20", "10.40.16.0/20"]
  map_public_ip_on_launch = true
  enable_nat_gateway      = false
}

# S3 traffic stays on AWS's network and skips the internet gateway. Gateway
# endpoints are free.
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${var.region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.public_route_table_ids
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "21.29.0"

  name               = var.cluster_name
  kubernetes_version = var.kubernetes_version

  endpoint_public_access       = true
  endpoint_public_access_cidrs = var.api_allowed_cidrs
  # Whoever runs `terraform apply` is the cluster's admin.
  enable_cluster_creator_admin_permissions = true

  vpc_id = module.vpc.vpc_id
  # The control plane needs subnets in two zones.
  subnet_ids = module.vpc.public_subnets

  addons = {
    vpc-cni = {
      before_compute = true
    }
    eks-pod-identity-agent = {
      before_compute = true
    }
    kube-proxy = {}
    coredns    = {}
    aws-ebs-csi-driver = {
      pod_identity_association = [{
        role_arn        = aws_iam_role.ebs_csi.arn
        service_account = "ebs-csi-controller-sa"
      }]
      # Every volume the driver creates is tagged, so `make cloud-down` can
      # check none outlived the cluster.
      configuration_values = jsonencode({
        controller = {
          extraVolumeTags = { spillway = var.cluster_name }
        }
      })
    }
  }

  eks_managed_node_groups = {
    workers = {
      ami_type       = "AL2023_x86_64_STANDARD"
      instance_types = [var.instance_type]
      capacity_type  = var.capacity_type
      min_size       = var.node_count
      max_size       = var.node_count
      desired_size   = var.node_count
      # All workers in one zone. EBS volumes are zonal, and traffic between
      # zones is billed: at the benchmark's 48 MB/s of events, Kafka to the
      # aggregators to Loki across zones would cost more than the nodes.
      subnet_ids = [module.vpc.public_subnets[0]]
    }
  }
}
