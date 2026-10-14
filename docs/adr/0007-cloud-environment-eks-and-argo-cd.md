# ADR 0007: The cloud environment: EKS from Terraform, the stack from Argo CD

Status: accepted · 2026-10-14 · M4.3

## Context

M4's gate includes a cloud environment that has been "stood up, measured and torn down", built with Terraform and Argo CD. Constraints:

- **Cost:** it's billed by the hour, so it lives for one measurement session. No budget has been agreed, so nothing is created without the owner's go-ahead. `terraform plan` is as far as automation goes on its own.
- **Same system as kind:** the measurements should be of the system the rest of `docs/results` measured, not a cloud-flavoured copy of it.
- **The operator owns some live fields:** the aggregator's update partition, its pod template's `spillway.dev/config-suffix` annotation, and a file per config revision in its ConfigMap. A GitOps controller that "corrects" those would break every canary rollout (the same problem as Helm, in CLAUDE.md's field ownership note).
- **Images:** kind loads locally built images, and a cloud cluster needs a registry.

## Decision

**AWS: EKS 1.36 and an S3 bucket, from Terraform** (`deploy/cloud/terraform`), with the community VPC and EKS modules pinned:

- **Network:** two public subnets (the control plane needs two zones), **no NAT gateway**, and a free S3 gateway endpoint. Nodes get public addresses for egress, and their security group admits nothing from outside the cluster.
- **Nodes:** three m7i.xlarge (12 vCPUs, 48 GiB), on-demand, **all in one zone**. EBS volumes are zonal, and traffic between zones is billed per gigabyte: at the benchmark's 48 MB/s, Kafka to aggregators to Loki across zones would cost more than the nodes.
- **Cold storage:** `spillway-cold-<account>`. The aggregators write to it through **EKS Pod Identity**, so no access key exists. The role allows `PutObject` on the bucket and `ListBucket` for the sink's health check. In kind, the `cold-storage` Secret holds Silo's keys.
- **Volumes:** a default gp3 StorageClass (EKS no longer marks one default) with `reclaimPolicy: Delete`. The EBS CSI driver tags every volume it creates.
- **Tags:** everything carries a `spillway=<cluster>` tag. There's an optional AWS Budgets alert.
- **State:** local, since the environment is short-lived.

**Everything on the cluster comes from Argo CD**, reconciling this repo's `main` (`deploy/cloud/apps`, an app of apps):

- **Same sources as `make up`.** The Applications use the same chart versions, the same values files and the same manifests. Kustomizations in `deploy/cloud/k8s` wrap the existing files, and don't copy them.
  - **Cloud differences:** only those, as a `profile` value: the StorageClass, the aggregators' environment without the Secret (`deploy/cloud/values/aggregator-aws.yaml`), the operator's cold-bucket arguments, and the image registry.
  - **Chart versions and image tags:** `make generate` writes them into `deploy/cloud/apps/generated.yaml` from the Makefile, and CI fails if they're stale.
- **Server-side apply and server-side diff.** Argo CD then owns only the fields its manifests set, which leaves the operator's partition, annotation and ConfigMap keys alone. Server-side diff also stopped two false OutOfSyncs from API-server defaults.
- **Sync waves:**
  - 0: StorageClass (aws) or Silo (kind), and Strimzi;
  - 1: Kafka, Loki, Prometheus, Grafana;
  - 2: aggregators;
  - 3: operator, agents, feeders.
- **The kind profile** runs the same Applications on kind (`make cluster kind-images argocd REVISION=<branch>`). It's how this was tested without AWS.

**Images on GHCR**, published by `.github/workflows/images.yml` on every push to `main`. Each is tagged with a hash of its source, which is the tag `generated.yaml` names. An image is built only when its source changed.

**`make cloud-plan`, `make cloud-up`, `make cloud-down`.** Up runs `terraform apply`, which shows the plan and asks, then installs Argo CD. Down goes in this order:
1. It deletes the root Application, which deletes every Application and their resources.
2. It deletes every PersistentVolumeClaim, and waits for the volumes to go.
3. It destroys the Terraform resources.
4. It checks that nothing tagged `spillway=<cluster>` is left.

Volumes go before the cluster because the CSI driver, not Terraform, created them, so destroying the cluster first would leave them behind, billed.

## Consequences

Good:
- **What's measured in the cloud is what kind runs.** It has the same pipeline, values and manifests, and a change reaches both.
- **The GitOps setup is tested in kind** before anything is billed. That run found the false OutOfSyncs above.
- **No long-lived secret** for cold storage.
- **Teardown is a single command that checks itself.**

Bad:
- **EKS pulls from a third party:** the cluster can't start without GHCR. (This ADR first said GHCR would make new packages private; published from this public repo, all five were public, and pullable anonymously, on the first push.)
- **The image must exist before Argo CD can roll it out.** A merge that changes the operator or a feeder points `main` at a new tag a few minutes before the workflow publishes it, and the pods wait in `ImagePullBackOff` until then.
- **Every change to the operator or feeder source changes `generated.yaml`,** and `make generate` has to be rerun, as with the CRD.
- **One zone and public subnets** aren't a production layout. They're right for a measurement environment that lives for hours.
- **The chaos experiments can't run as written:** they freeze processes with `docker exec` on kind's nodes. In EKS they'd need a privileged pod or SSM.

Alternatives considered:
- **IRSA:** works, but needs an OIDC provider and a role annotation on the service account. Pod Identity binds the role to the service account from Terraform, with nothing in the chart values.
- **ECR:** in the same account as the cluster, but pushing needs AWS credentials in CI (an OIDC role, created only once the environment exists). That couples publishing to the environment's lifetime.
- **Argo CD Image Updater, or CI committing tags:** `main` is protected and takes changes only through PRs, so CI can't commit. Generating tags with the other generated files fits the repo's existing check.
- **Installing Argo CD from Terraform (the Helm provider):** this ties the cluster's creation to its contents in one apply, and makes destroy order fragile. A script after the apply keeps Terraform to infrastructure.
- **Private subnets with a NAT gateway:** the usual layout, but about $33 a month plus $0.045 per gigabyte for what the feeders and image pulls download, with nothing gained for a short-lived environment.
