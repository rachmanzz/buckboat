# Buckboat

Buckboat is an S3-compatible storage project. The current repository license is
[Apache-2.0](LICENSE). The planned Pro/commercial edition is not available; see
[LICENSING.md](LICENSING.md) for the licensing status.

## Development status

The first single-node S3 slice is implemented on a local POSIX filesystem. It
supports path-style `CreateBucket`, `HeadBucket`, `DeleteBucket`, `PutObject`,
`GetObject`, `HeadObject`, and `DeleteObject` requests authenticated with
SigV4 authorization headers. Object PUT streams to disk, verifies the supplied
SHA-256 or `Content-MD5` when present, and publishes the complete object file
with an atomic rename. GET and HEAD return object metadata; GET supports byte
ranges and standard HTTP conditional headers. AWS-equivalent response bodies
for all conditional/error cases have not been conformance-tested.
The local profile caps stored user metadata at 2 KiB and its serialized object
metadata record at 64 KiB; these are implementation limits.

This is a limited development profile, not full S3 compatibility. It uses one
static access key/secret pair with full access to this local store. It has no
IAM policy engine or tenant isolation. Virtual-hosted requests, presigned
requests, SigV2/SigV4a, `aws-chunked` streaming signatures, multipart uploads,
bucket/object listing, versioning, copy, ACLs, encryption, Object Lock, and
other S3 API families are not implemented. Unsupported operations return an
explicit S3 error.

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

The data directory is created with private catalog, object, and staging
directories. Keep it on a local filesystem that supports atomic rename,
directory sync, and advisory file locks. Do not expose the development
listeners publicly; terminate TLS and configure production credentials and
authorization before network exposure. This profile does not enforce total
upload/download deadlines, so a connected slow client can hold a bounded
operation slot until it disconnects.

`/health/live` reports process liveness. `/health/ready` is ready after the
local store opens. The Document API returns `NotImplemented` until its session,
node, and format-adapter work is implemented.

## Validation

Run the unit and local HTTP tests with:

```sh
GOCACHE=/tmp/buckboat-go-build-cache go test ./...
```

The current tests cover the AWS SigV4 example vector, rejected signature
tampering and unsigned behavior headers, the configured single-PUT size cap,
the stored metadata size bound, local object replacement/reopen/delete,
path-safe object keys, delete/upload admission ordering, the single-process
lock, and the signed bucket/object HTTP lifecycle including a range GET. They
do not establish AWS-wide conformance or production durability across device
failure.
