# The cold path: one bucket, written by the aggregators through EKS Pod
# Identity, so no access key exists anywhere (in kind, the cold-storage
# Secret holds Silo's).

resource "aws_s3_bucket" "cold" {
  # Bucket names are global: the account ID keeps this one ours.
  bucket = "${var.cluster_name}-cold-${data.aws_caller_identity.current.account_id}"
  # `make cloud-down` destroys it with its objects: the environment holds
  # measurements, and the results that matter are written to docs/results.
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "cold" {
  bucket                  = aws_s3_bucket.cold.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

data "aws_iam_policy_document" "pod_identity_trust" {
  statement {
    actions = ["sts:AssumeRole", "sts:TagSession"]
    principals {
      type        = "Service"
      identifiers = ["pods.eks.amazonaws.com"]
    }
  }
}

# The aggregators' service account (the Vector chart names it after the
# release) may write objects to the cold bucket. The aws_s3 sink's health
# check lists the bucket (HeadBucket needs s3:ListBucket).
resource "aws_iam_role" "aggregator" {
  name               = "${var.cluster_name}-vector-aggregator"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_trust.json
}

data "aws_iam_policy_document" "cold_write" {
  statement {
    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.cold.arn}/*"]
  }
  statement {
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.cold.arn]
  }
}

resource "aws_iam_role_policy" "aggregator_cold" {
  name   = "cold-write"
  role   = aws_iam_role.aggregator.id
  policy = data.aws_iam_policy_document.cold_write.json
}

resource "aws_eks_pod_identity_association" "aggregator" {
  cluster_name    = module.eks.cluster_name
  namespace       = "vector"
  service_account = "vector-aggregator"
  role_arn        = aws_iam_role.aggregator.arn
}

# The EBS CSI driver, for the persistent volumes (Kafka, Loki, Prometheus,
# Grafana and the aggregators' disk buffers).
resource "aws_iam_role" "ebs_csi" {
  name               = "${var.cluster_name}-ebs-csi"
  assume_role_policy = data.aws_iam_policy_document.pod_identity_trust.json
}

resource "aws_iam_role_policy_attachment" "ebs_csi" {
  role       = aws_iam_role.ebs_csi.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"
}
