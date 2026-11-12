# 0008. Remote files over blocking HTTP range requests

- Status: accepted
- Date: 2026-11-12

## Context
Much Parquet data lives in object storage (Amazon S3 and S3-compatible stores, Google Cloud Storage, Azure Blob Storage) or behind plain HTTPS. Users want to open it without downloading it first. The usual Rust libraries for this (`object_store`, the AWS/Azure/Google SDKs) are built on an async runtime, which ADR 0001 rules out, and bring large dependency trees.

## Decision
`ParquetSource::open` accepts `https://`, `http://`, `s3://bucket/key`, `gs://bucket/key` and `az://account/container/blob` URLs (`parquet_reader::remote`):

- Requests are blocking HTTP GETs made by `ureq` on the worker threads, with TLS from `rustls` (`ring`) and the operating system's root certificates (`rustls-native-certs`). No async runtime.
- A Parquet object is read with `Range` requests: the footer at open, then the column chunks a page needs, exactly as for a local file. A remote object implements the `parquet` crate's `ChunkReader`, so every read path, the footer check and the dictionary-page check work unchanged. Fetched ranges are kept in a per-object cache of at most 256 MB (`REMOTE_CACHE_BYTES`) so paging through a row group doesn't download its column chunks again.
- When the server doesn't answer range requests, and for CSV, JSON and Arrow objects, the object is downloaded to a temporary file (at most 32 GiB, deleted with the source) and opened as a local file (ADR 0007).
- Credentials come from the environment and are never stored by PAVI:
  - `s3://`: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`, or the `[default]` / `AWS_PROFILE` section of `~/.aws/credentials`; requests are signed with Signature Version 4 (implemented with `ring`). Region from `AWS_REGION` / `AWS_DEFAULT_REGION`, corrected from the bucket's reply when it is wrong. `AWS_ENDPOINT_URL` selects an S3-compatible store (path-style). Without credentials, requests are anonymous (public buckets).
  - `gs://`: `GOOGLE_OAUTH_ACCESS_TOKEN` (for example from `gcloud auth print-access-token`) as a bearer token, or anonymous.
  - `az://`: `AZURE_STORAGE_SAS_TOKEN` appended to the request, or anonymous.
  - Pre-signed `https://` URLs (S3, GCS signed URLs, Azure SAS URLs) work as they are.
- URLs are not changed into local paths. A URL with a query string (it may carry a signature or token) is not saved in the recent list or the session.
- Remote folders (prefix listings) are not supported; one object is one dataset.

## Consequences
- Opening a large remote Parquet file costs a few requests for the footer; scrolling and filtering download only the column chunks they read, as a local read would read them from disk. Whole-file scans (sorts, aggregates) download the projected columns once per scan, so they are as slow as the network.
- Each request has a 30-second timeout and is retried three times on connection errors and 5xx answers; a failure is a status-bar error like any read error. A read in flight can't be interrupted, only abandoned when it returns.
- New dependencies: `ureq`, `rustls`, `ring`, `rustls-native-certs` and their dependencies, all under the permitted licenses. `webpki-roots` (CDLA) is avoided by using the system certificate store.
- Credential handling is limited to the cases above: no instance-metadata or SSO credentials for AWS, no service-account key files for Google, no Azure AD login. Those can be added later behind the same `Auth` type.
