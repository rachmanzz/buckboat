# Buckboat

Buckboat is an S3-compatible storage project. The current repository license is
[Apache-2.0](LICENSE). The planned Pro/commercial edition is not available; see
[LICENSING.md](LICENSING.md) for the licensing status.

## Development status

The current single-node profile stores metadata in SQLite and immutable
payloads on a local POSIX filesystem. It supports path-style `CreateBucket`,
`HeadBucket`, `DeleteBucket`, `PutObject`, `GetObject`, `HeadObject`,
`DeleteObject`, and a partial `ListObjectsV2` operation, authenticated with
SigV4 authorization headers. Object PUT streams to disk, verifies the supplied
SHA-256 or `Content-MD5` when present, and publishes the complete payload
before the SQLite catalog transaction. GET and HEAD return object metadata;
GET supports byte ranges and standard HTTP conditional headers. Listing
supports prefix, delimiter, bounded pagination, `start-after`, continuation
tokens, and `encoding-type=url`. These S3 operations have not been conformance-
tested against AWS.
The local profile caps stored user metadata at 2 KiB and its serialized object
metadata record at 64 KiB; these are implementation limits.

Listing values containing characters XML 1.0 cannot represent require
`encoding-type=url`. Without it, the server returns `InvalidArgument` instead
of replacing characters in an object key.

This is a limited development profile, not full S3 compatibility. It uses one
static access key/secret pair with full access to this local store. It has no
IAM policy engine or tenant isolation. Virtual-hosted requests, presigned
requests, SigV2/SigV4a, `aws-chunked` streaming signatures, multipart uploads,
`ListBuckets`, `ListObjects` V1, `ListObjectsV2` owner fields, versioning,
copy, ACLs, encryption, Object Lock, and other S3 API families are not
implemented. Unsupported operations return an explicit S3 error.

The local backend holds an advisory process lock for its data root. It is a
single-process profile: do not run multiple Buckboat instances against the
same data directory. The object file layout and local metadata representation
are implementation details for this profile; the catalog engine and future
cluster format remain open architecture decisions. Document API routes are
still unimplemented.

## Run locally

Configure a development credential pair. Buckboat does not provide a default
secret:

```sh
export BUCKBOAT_ACCESS_KEY_ID=buckboat-local
export BUCKBOAT_SECRET_ACCESS_KEY='replace-with-a-long-random-secret'
go run ./cmd/buckboat
```

By default, listeners bind to loopback:

- S3: `127.0.0.1:9400`
- Document: `127.0.0.1:9401`
- Health: `127.0.0.1:9402`

The S3 listener accepts SigV4 region `us-east-1` by default. Point an AWS CLI
profile at the local endpoint, using the same credential values:

```sh
export AWS_ACCESS_KEY_ID="$BUCKBOAT_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$BUCKBOAT_SECRET_ACCESS_KEY"
aws --endpoint-url http://127.0.0.1:9400 --region us-east-1 s3api create-bucket --bucket docs-bucket
printf 'hello from Buckboat\n' > /tmp/buckboat-object.txt
aws --endpoint-url http://127.0.0.1:9400 --region us-east-1 s3api put-object --bucket docs-bucket --key notes/hello.txt --body /tmp/buckboat-object.txt
aws --endpoint-url http://127.0.0.1:9400 --region us-east-1 s3api get-object --bucket docs-bucket --key notes/hello.txt /tmp/buckboat-download.txt
```

Environment configuration:

| Variable | Default | Purpose |
| --- | --- | --- |
| `BUCKBOAT_S3_ADDR` | `127.0.0.1:9400` | S3 listener |
| `BUCKBOAT_DOCUMENT_ADDR` | `127.0.0.1:9401` | Document listener |
| `BUCKBOAT_HEALTH_ADDR` | `127.0.0.1:9402` | Health listener |
| `BUCKBOAT_DATA_DIR` | `./.buckboat-data` | Local storage root |
| `BUCKBOAT_CATALOG_ADAPTER` | `sqlite` | Compiled Catalog adapter; unsupported names fail startup |
| `BUCKBOAT_ACCESS_KEY_ID` | required | Development access key ID |
| `BUCKBOAT_SECRET_ACCESS_KEY` | required | Development secret; never logged |
| `BUCKBOAT_REGION` | `us-east-1` | SigV4 signing region |
| `BUCKBOAT_MAX_OBJECT_BYTES` | `5000000000` | Maximum bytes in a single PUT |
| `BUCKBOAT_MAX_CONCURRENT_PUTS` | `16` | Maximum simultaneous object uploads; excess requests receive `503 SlowDown` |
| `BUCKBOAT_MAX_CONCURRENT_READS` | `128` | Maximum open object readers; excess requests receive `503 SlowDown` |

The server rejects a configured single-PUT limit above 5,000,000,000 bytes,
matching S3's 5 GB single-request upload maximum. Larger objects require
multipart upload, which is not implemented yet. See the [AWS S3 upload
limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html).

The current local profile stores bucket/object metadata in SQLite and immutable
payload files in the data directory. A non-empty catalog from the earlier
filesystem-only layout causes startup to fail; migrate it before switching to
SQLite. Keep the data directory on a local filesystem that supports atomic
rename, directory sync, and advisory file locks. The SQLite driver uses CGo;
building requires `CGO_ENABLED=1` and a C compiler. Do not expose the development
listeners publicly; terminate TLS and configure production credentials and
authorization before network exposure. This profile does not enforce total
upload/download deadlines, so a connected slow client can hold a bounded
operation slot until it disconnects.

Payloads are removed after publication errors known to precede a catalog
commit. Uncertain commit outcomes retain their payloads to preserve potentially
committed data. On startup, after acquiring the local root lock and opening the
SQLite catalog, Buckboat scans payload files in bounded batches and removes
only files confirmed to be unreferenced. A catalog lookup or cleanup error
stops startup. This recovers payloads left by crashes or uncertain commit
outcomes on the next successful startup; it does not verify same-size content
corruption or protect against device failure.

`/health/live` reports process liveness. `/health/ready` is ready after the
local store opens. The Document API returns `NotImplemented` until its session,
node, and format-adapter work is implemented.

## Validation

Run the unit and local HTTP tests with:

```sh
GOCACHE=/tmp/buckboat-go-build-cache go test ./...
```

Existing tests cover the AWS SigV4 example vector, rejected signature tampering
and unsigned behavior headers, the configured single-PUT size cap, the stored
metadata size bound, local object replacement/reopen/delete, path-safe object
keys, delete/upload admission ordering, the single-process lock, and the signed
bucket/object HTTP lifecycle including a range GET. SQLite-backed tests also
cover lifecycle/reopen, publication failure cleanup, conservative commit-error
handling, payload size integrity, object-list pagination and delimiter grouping,
operation hints, maximum escaped cursor sizes, and XML-safe listing behavior.
Directory-sync fault tests cover initialization and unpublished-file cleanup.
They do not establish AWS-wide conformance or production durability across
device failure.
