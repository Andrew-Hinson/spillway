# ADR 0006: Silo, a MinIO fork, for local cold storage

Status: proposed · 2026-10-10 · M3.5

## Context

The plan says "MinIO locally, S3 in cloud" for the cold path. The renderer's `cold_s3` sink is Vector's `aws_s3`, which works with any S3-compatible endpoint. For the local stack, the endpoint needs a container image that kind can pull and that's still maintained.

MinIO no longer publishes community images. `minio/minio` doesn't exist on Docker Hub any more, and `quay.io/minio/minio` doesn't pull. What remains:

| Image | State on 2026-10-10 |
|---|---|
| `pgsty/silo` | **Silo**: a maintained fork of the MinIO server by Pigsty (formerly `pgsty/minio`), last released 2026-09-16, 1.2M pulls, AGPLv3 like MinIO. It keeps MinIO's S3 API, `MINIO_*` variables, health routes and on-disk format |
| `bitnamilegacy/minio:2025.7.23` | Pulls, but it's a frozen build in Bitnami's legacy repository: no updates or security fixes, and it could be removed |
| `versity/versitygw` | Maintained, Apache 2.0, 31 MB. An S3 gateway serving a directory as buckets, not MinIO |
| `chrislusf/seaweedfs`, `dxflrs/garage`, `rustfs/rustfs` | Maintained object stores with their own operational models (several services; cluster layout commands; a newer MinIO-like server) |

## Decision

**Run Silo locally**, in single-drive mode on a 5 Gi volume, as the `s3` Service in the `storage` namespace (`http://s3.storage.svc:9000`). The image is pinned to `pgsty/silo:RELEASE.2026-09-16T00-00-00Z`, with its web console off. `make storage` (part of `make platform`) generates random credentials into a `cold-storage` Secret in `storage` and copies it to `vector`. Silo reads them as `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`, and the aggregators read them as `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. A Job creates the `spillway-cold` bucket. The operator runs with `--cold-bucket=spillway-cold --cold-endpoint=http://s3.storage.svc:9000`.

In the cloud (M4.3), the same sink writes to S3: `--cold-endpoint` is left empty, and credentials come from the environment (IAM role).

## Consequences

Good:
- **It's MinIO in all but name.** Same server lineage, protocol, configuration and health probes, so the plan's "MinIO locally" holds, with maintained builds.
- **Verified with Vector's real sink:** the rendered `cold_s3` config wrote gzip, newline-delimited JSON objects under `team=<team>/date=<day>/`, first against Silo in Docker and then in kind ([results](../results/m3.5-cold-path.md)).
- **Nothing in the renderer is specific to the store.** Swapping it means changing an image, the endpoint flag and `make storage`. An earlier draft of this change used versitygw; moving to Silo touched only those.

Bad:
- **A community fork:** its future depends on Pigsty continuing to maintain it.
- **AGPLv3:** fine for running it unmodified as a local service. Modifying and distributing it would carry AGPL obligations.
- **Single drive, no erasure coding:** only as durable as its volume. That's fine for a local stack, and it isn't used in the cloud.

Alternatives considered:
- **The frozen Bitnami MinIO image:** matches the plan's word, but builds on an image that gets no fixes and could vanish.
- **versitygw:** smaller, Apache 2.0, and it worked. But it's a gateway rather than an object store, and further from what the plan named.
- **SeaweedFS or Garage:** more moving parts than a single local bucket needs.

Revisit if Silo stops being maintained (versitygw is the tested fallback), or if the local stack needs features beyond a single bucket.
