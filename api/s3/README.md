# S3 API implementation status

## Implemented in the initial local profile

- Path-style `CreateBucket`, `HeadBucket`, and `DeleteBucket`.
- Path-style `PutObject`, `GetObject`, `HeadObject`, and `DeleteObject`.
- SigV4 authorization-header requests for one configured access key and region.
- Streaming single-request PUT, SHA-256 payload verification, optional
  `Content-MD5` verification, single-part ETag generation, bounded user
  metadata, selected content headers, byte-range GET, and HTTP conditional
  GET/HEAD handling.
- The local profile caps stored user metadata at 2 KiB and the serialized
  object metadata record at 64 KiB. These are profile limits, not AWS-wide
  compatibility claims.
- Partial `ListObjectsV2`: prefix and delimiter grouping, bounded pages,
  start-after, signed continuation tokens, and `encoding-type=url`.
- Supported operation hints such as `x-id=GetObject` and `x-id=PutObject` are
  accepted without rewriting the signed request target.

Listing values containing XML 1.0 control/noncharacters require
`encoding-type=url`; otherwise the handler returns `InvalidArgument` instead
of substituting a different object key.

These operations have local unit and HTTP tests. They are a narrow development
slice, not a claim of AWS conformance. Only path-style addressing is routed.

## Explicitly not implemented

Presigned/query authentication, SigV2, SigV4a, session tokens, streaming
`aws-chunked` signing, virtual-hosted addressing, ListBuckets, ListObjects V1,
ListObjectsV2 owner fields and optional object attributes,
multipart upload, CopyObject, versioning, bucket configuration APIs, ACLs,
IAM policy evaluation, server-side encryption, Object Lock, and other API
families remain unimplemented. Unsupported methods and query actions return
`NotImplemented`.
