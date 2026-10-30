// Package ports defines the object-storage contract shared by S3 transport and
// storage adapters.
package ports

import (
	"context"
	"errors"
	"io"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrBucketNotEmpty      = errors.New("bucket is not empty")
	ErrObjectNotFound      = errors.New("object not found")
	ErrInvalidBucket       = errors.New("invalid bucket name")
	ErrInvalidKey          = errors.New("invalid object key")
	ErrInvalidArgument     = errors.New("invalid argument")
	ErrIncompleteBody      = errors.New("object body was incomplete")
	ErrObjectTooLarge      = errors.New("object exceeds configured size limit")
	ErrChecksumMismatch    = errors.New("object checksum mismatch")
	ErrMetadataTooLarge    = errors.New("object metadata exceeds the supported size limit")
	ErrInvalidMetadata     = errors.New("user metadata contains non-ASCII bytes")
	ErrBusy                = errors.New("storage is busy")
	ErrPublishNotCommitted = errors.New("object publication did not commit")
	ErrCorruptObject       = errors.New("object payload does not match catalog metadata")
)

var bucketPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

const MaxObjectListPageSize = 1000

// MaxPayloadRefBatchSize bounds catalog checks and filesystem scans used by
// startup payload recovery.
const MaxPayloadRefBatchSize = 256

func ValidBucketName(name string) bool {
	if !bucketPattern.MatchString(name) || strings.Contains(name, "..") || strings.Contains(name, ".-") || strings.Contains(name, "-.") {
		return false
	}
	return len(name) >= 3 && len(name) <= 63
}

func ValidObjectKey(key string) bool {
	return key != "" && len(key) <= 1024 && utf8.ValidString(key)
}

func ValidObjectPrefix(prefix string) bool {
	return utf8.ValidString(prefix)
}

type ObjectInfo struct {
	Bucket             string
	Key                string
	Size               int64
	ETag               string
	ContentType        string
	CacheControl       string
	ContentDisposition string
	ContentEncoding    string
	ContentLanguage    string
	Expires            string
	Metadata           map[string]string
	LastModified       time.Time
}

type PutObjectInput struct {
	Bucket             string
	Key                string
	Body               io.Reader
	ContentLength      int64
	PayloadSHA256      string
	ContentMD5         string
	ContentType        string
	CacheControl       string
	ContentDisposition string
	ContentEncoding    string
	ContentLanguage    string
	Expires            string
	UserMetadata       map[string]string
}

type ObjectStore interface {
	CreateBucket(context.Context, string) error
	HeadBucket(context.Context, string) error
	DeleteBucket(context.Context, string) error
	PutObject(context.Context, PutObjectInput) (ObjectInfo, error)
	OpenObject(context.Context, string, string) (ObjectInfo, io.ReadSeekCloser, error)
	DeleteObject(context.Context, string, string) error
}

// CatalogObject stores current object metadata separately from its immutable
// payload. PayloadRef is opaque to Catalog consumers and adapters.
type CatalogObject struct {
	Info       ObjectInfo
	PayloadRef string
}

// ObjectListCursor resumes after one returned object or common prefix. The
// StartAfter field is retained only when a zero-sized page returns no entries.
type ObjectListCursor struct {
	Key            string
	IsCommonPrefix bool
	StartAfter     string
}

// ObjectListQuery requests one bounded, byte-ordered page of current objects.
type ObjectListQuery struct {
	Bucket     string
	Prefix     string
	Delimiter  string
	StartAfter string
	Cursor     *ObjectListCursor
	Limit      int
}

type ListedObject struct {
	Key          string
	Size         int64
	ETag         string
	LastModified time.Time
}

// ObjectListEntry contains either an object summary or one rolled-up prefix.
type ObjectListEntry struct {
	Object       *ListedObject
	CommonPrefix string
}

type ObjectListPage struct {
	Entries    []ObjectListEntry
	NextCursor *ObjectListCursor
	HasMore    bool
}

// ObjectLister is a bounded, metadata-only object listing capability.
type ObjectLister interface {
	ListObjects(context.Context, ObjectListQuery) (ObjectListPage, error)
}

// Catalog is Buckboat's semantic metadata contract. Implementations own their
// schema, transactions, and engine-specific types. PublishObject errors known
// to precede a commit must wrap ErrPublishNotCommitted. Other errors have an
// uncertain outcome; callers must retain the candidate payload.
type Catalog interface {
	CreateBucket(context.Context, string) error
	HeadBucket(context.Context, string) error
	DeleteBucket(context.Context, string) error
	PublishObject(context.Context, CatalogObject) (previousPayloadRef string, err error)
	GetObject(context.Context, string, string) (CatalogObject, error)
	FindPayloadRefs(context.Context, []string) (map[string]struct{}, error)
	ListObjects(context.Context, ObjectListQuery) (ObjectListPage, error)
	DeleteObject(context.Context, string, string) (payloadRef string, err error)
	Close() error
}

// PayloadStore persists immutable object bytes and returns an opaque reference
// that can be published by Catalog.
type PayloadStore interface {
	PutPayload(context.Context, PutObjectInput) (ObjectInfo, string, error)
	OpenPayload(context.Context, string) (io.ReadSeekCloser, error)
	DeletePayload(context.Context, string) error
	DeletePayloads(context.Context, []string) error
	WalkPayloadRefs(context.Context, int, func([]string) error) error
	Close() error
}
