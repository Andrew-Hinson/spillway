# ADR 0006: versitygw, not MinIO, for local cold storage

Status: proposed · 2026-10-10 · M3.5

## Context

The plan says "MinIO locally, S3 in cloud" for the cold path. The renderer's `cold_s3` sink is Vector's `aws_s3`, which works with any S3-compatible endpoint. For the local stack, the endpoint needs a container image that kind can pull and that's still maintained.

MinIO no longer publishes community images. `minio/minio` doesn't exist on Docker Hub any more, and `quay.io/minio/minio` doesn't pull. What remains:

| Image | State on 2026-10-10 |
|---|---|
| `bitnamilegacy/minio:2025.7.23` | Pulls, but it's a frozen build in Bitnami's legacy repository: no updates or security fixes, and it could be removed |
| `chrislusf/seaweedfs` | Maintained (built 2026-09-28). Master, volume and filer services behind the S3 gateway |
| `dxflrs/garage` | Maintained and small (12 MB), but a node must be given a cluster layout with admin commands before it serves anything |
| `rustfs/rustfs` | Maintained, positioned as a MinIO replacement, and newer |
| `versity/versitygw` | Maintained (v1.8.0, built 2026-09-04), Apache 2.0, 31 MB. One process that serves a directory as S3 buckets |

## Decision

**Run versitygw locally**, `posix` backend on a 5 Gi volume, as the `s3` Service in the `storage` namespace (`http://s3.storage.svc:7070`). `make storage` (part of `make platform`) generates random credentials into a `cold-storage` Secret in `storage` and copies it to `vector`, where the aggregators read it as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. A Job creates the `spillway-cold` bucket. The operator runs with `--cold-bucket=spillway-cold --cold-endpoint=http://s3.storage.svc:7070`.

In the cloud (M4.3), the same sink writes to S3: `--cold-endpoint` is left empty, and credentials come from the environment (IAM role).

## Consequences

Good:
- **One small, maintained process** with no cluster setup. A bucket is a directory, so an object can be inspected on the volume as well as through the S3 API.
- **Verified with Vector's real sink:** the rendered `cold_s3` config wrote gzip, newline-delimited JSON objects under `team=<team>/date=<day>/`, first against versitygw in Docker and then in kind ([results](../results/m3.5-cold-path.md)).
- **Nothing in the renderer is specific to the gateway.** Swapping it means changing an image, the endpoint flag and `make storage`.

Bad:
- **Not what the plan named.** Anything written about "MinIO" in the plan means this gateway locally.
- **A gateway, not an object store.** It has no erasure coding or replication, and it's only as durable as the volume. That's fine for a local stack and irrelevant in the cloud, where it isn't used.
- **Less widely used than MinIO**, so S3 edge cases are less trodden. Vector uses PutObject, HeadBucket and path-style addressing, and all three were tested.

Alternatives considered:
- **The frozen Bitnami MinIO image:** matches the plan's word, but builds on an image that gets no fixes and could vanish.
- **SeaweedFS or Garage:** both maintained. SeaweedFS is several services. Garage needs layout commands at start-up. Both are more than a single local bucket needs.
- **LocalStack:** emulates far more than S3, at the cost of a much larger image.

Revisit if versitygw stops being maintained, or if the local stack needs object-store features such as lifecycle rules or replication.
