// Package ports defines the object-storage contract shared by S3 transport and
// storage adapters.
package ports

import (
	"context"
	"errors"
	"io"
	"time"
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
)

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
