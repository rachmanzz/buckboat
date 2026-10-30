package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rachmanzz/buckboat/internal/auth/sigv4"
	"github.com/rachmanzz/buckboat/internal/storage/ports"
)

type AuthConfig struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
}

type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
}

type handler struct {
	store ports.ObjectStore
	auth  AuthConfig
}

func NewRouter(store ports.ObjectStore, auth AuthConfig) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	endpoint := &handler{store: store, auth: auth}
	// Routing is intentionally delegated to the handler. Chi path parameters,
	// path cleaning, and redirects must not rewrite the request target used by
	// SigV4 or alter S3 object keys.
	r.Handle("/", endpoint)
	r.Handle("/*", endpoint)
	return r
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	requestID := middleware.GetReqID(r.Context())
	w.Header().Set("x-amz-request-id", requestID)
	now := time.Now().UTC()
	w.Header().Set("Date", now.Format(http.TimeFormat))

	if h.store == nil {
		writeError(w, http.StatusServiceUnavailable, "ServiceUnavailable", "Storage is not ready", requestID, "")
		return
	}
	if err := sigv4.Verify(r, h.auth.AccessKeyID, h.auth.SecretAccessKey, h.auth.Region, now); err != nil {
		status, code, message := authError(err)
		writeError(w, status, code, message, requestID, requestPath(r))
		return
	}
	resource, err := parseResource(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "InvalidURI", "The request URI is invalid", requestID, requestPath(r))
		return
	}
	if resource.hasQuery {
		writeError(w, http.StatusNotImplemented, "NotImplemented", "This S3 query operation is not implemented yet", requestID, requestPath(r))
		return
	}
	if hasUnsupportedS3Header(r) {
		writeError(w, http.StatusNotImplemented, "NotImplemented", "The request uses an S3 header whose behavior is not implemented yet", requestID, requestPath(r))
		return
	}
	if resource.bucket == "" {
		writeError(w, http.StatusNotImplemented, "NotImplemented", "ListBuckets is not implemented yet", requestID, requestPath(r))
		return
	}

	if !resource.hasKey {
		h.serveBucket(w, r, requestID, resource)
		return
	}
	h.serveObject(w, r, requestID, resource)
}

type resource struct {
	bucket   string
	key      string
	hasKey   bool
	hasQuery bool
}

func parseResource(r *http.Request) (resource, error) {
	path, query := requestTarget(r)
	if path == "" || !strings.HasPrefix(path, "/") {
		return resource{}, fmt.Errorf("invalid path")
	}
	trimmed := strings.TrimPrefix(path, "/")
	if trimmed == "" {
		return resource{hasQuery: query != ""}, nil
	}
	bucketRaw, keyRaw, hasKey := strings.Cut(trimmed, "/")
	bucket, err := url.PathUnescape(bucketRaw)
	if err != nil {
		return resource{}, err
	}
	result := resource{bucket: bucket, hasKey: hasKey, hasQuery: query != ""}
	if hasKey {
		result.key, err = url.PathUnescape(keyRaw)
		if err != nil {
			return resource{}, err
		}
		if result.key == "" {
			return resource{}, fmt.Errorf("object key must not be empty")
		}
	}
	return result, nil
}

func requestTarget(r *http.Request) (string, string) {
	target := r.RequestURI
	if target == "" {
		target = r.URL.EscapedPath()
		if target == "" {
			target = "/"
		}
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
	}
	path, query, _ := strings.Cut(target, "?")
	return path, query
}

func (h *handler) serveBucket(w http.ResponseWriter, r *http.Request, requestID string, target resource) {
	switch r.Method {
	case http.MethodPut:
		if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			writeError(w, http.StatusNotImplemented, "NotImplemented", "Bucket location configuration is not implemented yet", requestID, requestPath(r))
			return
		}
		if target.bucket == "" {
			writeError(w, http.StatusBadRequest, "InvalidBucketName", "The bucket name is invalid", requestID, requestPath(r))
			return
		}
		if err := h.store.CreateBucket(r.Context(), target.bucket); err != nil {
			h.writeStorageError(w, err, requestID, requestPath(r))
			return
		}
		w.Header().Set("Location", "/"+target.bucket)
		w.WriteHeader(http.StatusOK)
	case http.MethodHead:
		if err := h.store.HeadBucket(r.Context(), target.bucket); err != nil {
			h.writeStorageError(w, err, requestID, requestPath(r))
			return
		}
		w.Header().Set("x-amz-bucket-region", h.auth.Region)
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		if err := h.store.DeleteBucket(r.Context(), target.bucket); err != nil {
			h.writeStorageError(w, err, requestID, requestPath(r))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusNotImplemented, "NotImplemented", "This bucket operation is not implemented yet", requestID, requestPath(r))
	}
}

func (h *handler) serveObject(w http.ResponseWriter, r *http.Request, requestID string, target resource) {
	switch r.Method {
	case http.MethodPut:
		if hasConditionalWrite(r) {
			writeError(w, http.StatusNotImplemented, "NotImplemented", "Conditional object writes are not implemented yet", requestID, requestPath(r))
			return
		}
		metadata := make(map[string]string)
		for name, values := range r.Header {
			lower := strings.ToLower(name)
			if strings.HasPrefix(lower, "x-amz-meta-") && len(values) != 0 {
				metadata[strings.TrimPrefix(lower, "x-amz-meta-")] = strings.Join(values, ",")
			}
		}
		info, err := h.store.PutObject(
			r.Context(), ports.PutObjectInput{
				Bucket: target.bucket, Key: target.key, Body: r.Body, ContentLength: r.ContentLength,
				PayloadSHA256: r.Header.Get("X-Amz-Content-Sha256"), ContentMD5: r.Header.Get("Content-Md5"),
				ContentType: r.Header.Get("Content-Type"), CacheControl: r.Header.Get("Cache-Control"),
				ContentDisposition: r.Header.Get("Content-Disposition"), ContentEncoding: r.Header.Get("Content-Encoding"),
				ContentLanguage: r.Header.Get("Content-Language"), Expires: r.Header.Get("Expires"), UserMetadata: metadata,
			},
		)
		if err != nil {
			h.writeStorageError(w, err, requestID, requestPath(r))
			return
		}
		w.Header().Set("ETag", info.ETag)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet, http.MethodHead:
		info, body, err := h.store.OpenObject(r.Context(), target.bucket, target.key)
		if err != nil {
			h.writeStorageError(w, err, requestID, requestPath(r))
			return
		}
		defer body.Close()
		w.Header().Set("ETag", info.ETag)
		setIfNotEmpty(w.Header(), "Cache-Control", info.CacheControl)
		setIfNotEmpty(w.Header(), "Content-Disposition", info.ContentDisposition)
		setIfNotEmpty(w.Header(), "Content-Encoding", info.ContentEncoding)
		setIfNotEmpty(w.Header(), "Content-Language", info.ContentLanguage)
		setIfNotEmpty(w.Header(), "Expires", info.Expires)
		for key, value := range info.Metadata {
			w.Header().Set("x-amz-meta-"+key, value)
		}
		contentType := info.ContentType
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		w.Header().Set("Content-Type", contentType)
		http.ServeContent(w, r, target.key, info.LastModified, body)
	case http.MethodDelete:
		if err := h.store.DeleteObject(r.Context(), target.bucket, target.key); err != nil {
			h.writeStorageError(w, err, requestID, requestPath(r))
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusNotImplemented, "NotImplemented", "This object operation is not implemented yet", requestID, requestPath(r))
	}
}

func hasUnsupportedS3Header(r *http.Request) bool {
	for name := range r.Header {
		lower := strings.ToLower(name)
		if !strings.HasPrefix(lower, "x-amz-") {
			continue
		}
		switch {
		case lower == "x-amz-date", lower == "x-amz-content-sha256":
		case strings.HasPrefix(lower, "x-amz-meta-"):
		case lower == "x-amz-user-agent", lower == "x-amz-sdk-invocation-id", lower == "x-amz-sdk-request":
		default:
			return true
		}
	}
	return false
}

func setIfNotEmpty(headers http.Header, name, value string) {
	if value != "" {
		headers.Set(name, value)
	}
}

func hasConditionalWrite(r *http.Request) bool {
	for _, name := range []string{"If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since"} {
		if r.Header.Get(name) != "" {
			return true
		}
	}
	return false
}

func (h *handler) writeStorageError(w http.ResponseWriter, err error, requestID, resource string) {
	switch {
	case errors.Is(err, ports.ErrBucketNotFound):
		writeError(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist", requestID, resource)
	case errors.Is(err, ports.ErrBucketAlreadyExists):
		writeError(w, http.StatusConflict, "BucketAlreadyExists", "The requested bucket name is not available", requestID, resource)
	case errors.Is(err, ports.ErrBucketNotEmpty):
		writeError(w, http.StatusConflict, "BucketNotEmpty", "The bucket is not empty", requestID, resource)
	case errors.Is(err, ports.ErrObjectNotFound):
		writeError(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist", requestID, resource)
	case errors.Is(err, ports.ErrInvalidBucket):
		writeError(w, http.StatusBadRequest, "InvalidBucketName", "The bucket name is invalid", requestID, resource)
	case errors.Is(err, ports.ErrInvalidKey), errors.Is(err, ports.ErrInvalidArgument):
		writeError(w, http.StatusBadRequest, "InvalidArgument", "The request contains an invalid argument", requestID, resource)
	case errors.Is(err, ports.ErrIncompleteBody):
		writeError(w, http.StatusBadRequest, "IncompleteBody", "The request body ended before the declared content length", requestID, resource)
	case errors.Is(err, ports.ErrObjectTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", "The object exceeds the configured size limit", requestID, resource)
	case errors.Is(err, ports.ErrChecksumMismatch):
		writeError(w, http.StatusBadRequest, "BadDigest", "The supplied checksum does not match the object body", requestID, resource)
	case errors.Is(err, ports.ErrMetadataTooLarge):
		writeError(w, http.StatusBadRequest, "MetadataTooLarge", "Object metadata exceeds the supported size limit", requestID, resource)
	case errors.Is(err, ports.ErrInvalidMetadata):
		writeError(w, http.StatusBadRequest, "InvalidArgument", "User metadata must use non-empty ASCII names and ASCII values", requestID, resource)
	case errors.Is(err, ports.ErrBusy):
		writeError(w, http.StatusServiceUnavailable, "SlowDown", "The storage is busy; retry the request", requestID, resource)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeError(w, http.StatusRequestTimeout, "RequestTimeout", "The request was canceled", requestID, resource)
	default:
		writeError(w, http.StatusInternalServerError, "InternalError", "The request could not be completed", requestID, resource)
	}
}

func authError(err error) (int, string, string) {
	switch {
	case errors.Is(err, sigv4.ErrUnsupportedAuth):
		return http.StatusNotImplemented, "NotImplemented", "This authentication form is not implemented yet"
	case errors.Is(err, sigv4.ErrExpiredRequest):
		return http.StatusForbidden, "RequestTimeTooSkewed", "The request timestamp is outside the accepted clock window"
	case errors.Is(err, sigv4.ErrInvalidAccessKey):
		return http.StatusForbidden, "InvalidAccessKeyId", "The access key ID is not valid"
	case errors.Is(err, sigv4.ErrMissingAuth), errors.Is(err, sigv4.ErrInvalidAuth), errors.Is(err, sigv4.ErrSignatureMismatch):
		return http.StatusForbidden, "SignatureDoesNotMatch", "The request signature is invalid"
	default:
		return http.StatusForbidden, "AccessDenied", "The request is not authorized"
	}
}

func writeError(w http.ResponseWriter, status int, code, message, requestID, resource string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(errorResponse{
		Code:      code,
		Message:   message,
		Resource:  resource,
		RequestID: requestID,
	})
}

func requestPath(r *http.Request) string {
	path, _ := requestTarget(r)
	return path
}
