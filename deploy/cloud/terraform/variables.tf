variable "region" {
  description = "AWS region for the cluster and the cold bucket."
  type        = string
  default     = "us-east-1"
}

variable "cluster_name" {
  description = "EKS cluster name; also the value of the spillway tag on every resource."
  type        = string
  default     = "spillway"
}

variable "kubernetes_version" {
  description = "EKS Kubernetes version. kind runs 1.37; EKS's newest confirmed is 1.36."
  type        = string
  default     = "1.36"
}

variable "instance_type" {
  description = "Worker instance type. Three m7i.xlarge are 12 vCPUs and 48 GiB, against the 16 threads and 61 GiB the kind benchmark shared with everything else on the host."
  type        = string
  default     = "m7i.xlarge"
}

variable "node_count" {
  description = "Number of workers: two aggregators, Kafka and Loki each want most of a node under load."
  type        = number
  default     = 3
}

variable "capacity_type" {
  description = "ON_DEMAND or SPOT. Spot is about a third of the price, but a reclaimed node mid-benchmark spoils the run."
  type        = string
  default     = "ON_DEMAND"
}

variable "api_allowed_cidrs" {
  description = "CIDRs allowed to reach the Kubernetes API (it also requires IAM auth). Narrow it to your own address, e.g. [\"203.0.113.7/32\"]."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "budget_email" {
  description = "If set, an AWS Budgets alert emails this address when the month's actual or forecast spend passes budget_usd. Empty: no budget."
  type        = string
  default     = ""
}

variable "budget_usd" {
  description = "Monthly spend that triggers the budget alert."
  type        = number
  default     = 25
}
