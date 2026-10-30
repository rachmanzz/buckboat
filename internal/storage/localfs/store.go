// Package localfs implements the initial single-process, single-node object
// store. Object bytes and their metadata are published together by atomic
// rename; filenames are derived from hashes, never from client bucket/key text.
package localfs

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/rachmanzz/buckboat/internal/storage/ports"
)

const trailerMagic = "BBOTOBJ1"
const maxUserMetadataBytes = 2 << 10
const maxObjectMetadataBytes = 64 << 10

type Config struct {
	RootDir            string
	MaxObjectBytes     int64
	MaxConcurrentPuts  int
	MaxConcurrentReads int
}

type Store struct {
	root      string
	maxBytes  int64
	putSlots  chan struct{}
	readSlots chan struct{}
	mu        sync.RWMutex
	lockFile  *os.File
	closeOnce sync.Once
	closeErr  error
}

type bucketRecord struct {
	Name string `json:"name"`
}

type objectRecord struct {
	Bucket             string            `json:"bucket"`
	Key                string            `json:"key"`
	Size               int64             `json:"size"`
	ETag               string            `json:"etag"`
	ContentType        string            `json:"content_type,omitempty"`
	CacheControl       string            `json:"cache_control,omitempty"`
	ContentDisposition string            `json:"content_disposition,omitempty"`
	ContentEncoding    string            `json:"content_encoding,omitempty"`
	ContentLanguage    string            `json:"content_language,omitempty"`
	Expires            string            `json:"expires,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	LastModified       time.Time         `json:"last_modified"`
}

func Open(cfg Config) (*Store, error) {
	if cfg.RootDir == "" {
		return nil, fmt.Errorf("local storage root directory is required")
	}
	if cfg.MaxObjectBytes <= 0 {
		return nil, fmt.Errorf("maximum object size must be positive")
	}
	if cfg.MaxObjectBytes == int64(^uint64(0)>>1) {
		return nil, fmt.Errorf("maximum object size leaves no room for the streaming limit sentinel")
	}
	if cfg.MaxConcurrentPuts <= 0 {
		return nil, fmt.Errorf("maximum concurrent PUT count must be positive")
	}
	if cfg.MaxConcurrentReads <= 0 {
		return nil, fmt.Errorf("maximum concurrent read count must be positive")
	}

	root, err := filepath.Abs(cfg.RootDir)
	if err != nil {
		return nil, fmt.Errorf("resolve local storage root: %w", err)
	}
	if filepath.Clean(root) == string(os.PathSeparator) {
		return nil, fmt.Errorf("local storage root must not be the filesystem root")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create local storage root: %w", err)
	}
	lockFile, err := acquireRootLock(root)
	if err != nil {
		return nil, err
	}
	store := &Store{
		root:      root,
		maxBytes:  cfg.MaxObjectBytes,
		putSlots:  make(chan struct{}, cfg.MaxConcurrentPuts),
		readSlots: make(chan struct{}, cfg.MaxConcurrentReads),
		lockFile:  lockFile,
	}
	for _, dir := range []string{
		filepath.Join(root, "catalog", "buckets"),
		filepath.Join(root, "catalog", "objects"),
		filepath.Join(root, "catalog", "staging"),
		filepath.Join(root, "payloads"),
		filepath.Join(root, "payload-staging"),
	} {
		if err := ensurePrivateDir(dir); err != nil {
			_ = store.Close()
			return nil, fmt.Errorf("create local storage directory: %w", err)
		}
	}
	if err := store.cleanInterruptedUploads(); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("clean interrupted local uploads: %w", err)
	}
	if err := store.cleanInterruptedPayloadUploads(); err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("clean interrupted payload uploads: %w", err)
	}
	return store, nil
}

// Close releases the process lock. Stop all store operations before calling it.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = releaseRootLock(s.lockFile)
	})
	return s.closeErr
}

func (s *Store) CreateBucket(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validBucketName(name) {
		return ports.ErrInvalidBucket
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.readBucket(name); err == nil {
		return ports.ErrBucketAlreadyExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := ensurePrivateDir(s.bucketObjectsDir(name)); err != nil {
		return fmt.Errorf("create bucket object directory: %w", err)
	}
	if err := ensurePrivateDir(s.bucketStagingDir(name)); err != nil {
		return fmt.Errorf("create bucket staging directory: %w", err)
	}
	if err := syncDir(filepath.Dir(s.bucketObjectsDir(name))); err != nil {
		return fmt.Errorf("sync bucket directory parent: %w", err)
	}
	if err := syncDir(filepath.Dir(s.bucketStagingDir(name))); err != nil {
		return fmt.Errorf("sync staging directory parent: %w", err)
	}
	if err := s.writeJSONAtomic(s.bucketPath(name), bucketRecord{Name: name}); err != nil {
		return fmt.Errorf("persist bucket metadata: %w", err)
	}
	return nil
}

func (s *Store) HeadBucket(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validBucketName(name) {
		return ports.ErrInvalidBucket
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, err := s.readBucket(name)
	if errors.Is(err, os.ErrNotExist) {
		return ports.ErrBucketNotFound
	}
	return err
}

func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validBucketName(name) {
		return ports.ErrInvalidBucket
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.readBucket(name); errors.Is(err, os.ErrNotExist) {
		return ports.ErrBucketNotFound
	} else if err != nil {
		return err
	}
	objectsEmpty, err := isEmptyDir(s.bucketObjectsDir(name))
	if err != nil {
		return fmt.Errorf("inspect bucket objects: %w", err)
	}
	stagingEmpty, err := isEmptyDir(s.bucketStagingDir(name))
	if err != nil {
		return fmt.Errorf("inspect bucket uploads: %w", err)
	}
	if !objectsEmpty || !stagingEmpty {
		return ports.ErrBucketNotEmpty
	}
	if err := os.Remove(s.bucketPath(name)); err != nil {
		return fmt.Errorf("remove bucket metadata: %w", err)
	}
	if err := syncDir(filepath.Dir(s.bucketPath(name))); err != nil {
		return fmt.Errorf("sync bucket catalog: %w", err)
	}
	if err := os.Remove(s.bucketObjectsDir(name)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove bucket object directory: %w", err)
	}
	if err := os.Remove(s.bucketStagingDir(name)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove bucket staging directory: %w", err)
	}
	if err := syncDir(filepath.Dir(s.bucketObjectsDir(name))); err != nil {
		return fmt.Errorf("sync object catalog: %w", err)
	}
	if err := syncDir(filepath.Dir(s.bucketStagingDir(name))); err != nil {
		return fmt.Errorf("sync staging catalog: %w", err)
	}
	return nil
}

// createUploadTemp prevents bucket deletion between validation and temp creation.
func (s *Store) createUploadTemp(bucket string) (*os.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.readBucket(bucket); errors.Is(err, os.ErrNotExist) {
		return nil, ports.ErrBucketNotFound
	} else if err != nil {
		return nil, err
	}
	stagingDir := s.bucketStagingDir(bucket)
	if err := ensurePrivateDir(stagingDir); err != nil {
		return nil, fmt.Errorf("create upload staging directory: %w", err)
	}
	tmp, err := os.CreateTemp(stagingDir, ".tmp-*")
	if err != nil {
		return nil, fmt.Errorf("create upload staging file: %w", err)
	}
	return tmp, nil
}

func (s *Store) PutObject(ctx context.Context, input ports.PutObjectInput) (ports.ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ports.ObjectInfo{}, err
	}
	if err := validateUserMetadata(input.UserMetadata); err != nil {
		return ports.ObjectInfo{}, err
	}
	if !validBucketName(input.Bucket) {
		return ports.ObjectInfo{}, ports.ErrInvalidBucket
	}
	if !validObjectKey(input.Key) {
		return ports.ObjectInfo{}, ports.ErrInvalidKey
	}
	if input.Body == nil {
		if input.ContentLength != 0 {
			return ports.ObjectInfo{}, ports.ErrInvalidArgument
		}
		input.Body = strings.NewReader("")
	}
	if input.ContentLength < -1 {
		return ports.ObjectInfo{}, ports.ErrInvalidArgument
	}
	if input.ContentLength > s.maxBytes {
		return ports.ObjectInfo{}, ports.ErrObjectTooLarge
	}
	var expectedSHA256 []byte
	if input.PayloadSHA256 != "" && input.PayloadSHA256 != "UNSIGNED-PAYLOAD" {
		var err error
		expectedSHA256, err = hex.DecodeString(input.PayloadSHA256)
		if err != nil || len(expectedSHA256) != sha256.Size {
			return ports.ObjectInfo{}, ports.ErrInvalidArgument
		}
	}
	var expectedMD5 []byte
	if input.ContentMD5 != "" {
		var err error
		expectedMD5, err = base64.StdEncoding.DecodeString(input.ContentMD5)
		if err != nil || len(expectedMD5) != md5.Size {
			return ports.ObjectInfo{}, ports.ErrInvalidArgument
		}
	}
	select {
	case s.putSlots <- struct{}{}:
		defer func() { <-s.putSlots }()
	default:
		return ports.ObjectInfo{}, ports.ErrBusy
	}

	objectDir := s.bucketObjectsDir(input.Bucket)
	stagingDir := s.bucketStagingDir(input.Bucket)
	tmp, err := s.createUploadTemp(input.Bucket)
	if err != nil {
		return ports.ObjectInfo{}, err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()

	sha256Hash := sha256.New()
	md5Hash := md5.New()
	limited := io.LimitReader(input.Body, s.maxBytes+1)
	written, err := io.Copy(tmp, io.TeeReader(limited, io.MultiWriter(sha256Hash, md5Hash)))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ports.ObjectInfo{}, ctxErr
		}
		return ports.ObjectInfo{}, fmt.Errorf("%w: %v", ports.ErrIncompleteBody, err)
	}
	if written > s.maxBytes {
		return ports.ObjectInfo{}, ports.ErrObjectTooLarge
	}
	if input.ContentLength >= 0 && written != input.ContentLength {
		return ports.ObjectInfo{}, fmt.Errorf("%w: received %d bytes, expected %d", ports.ErrIncompleteBody, written, input.ContentLength)
	}
	if err := ctx.Err(); err != nil {
		return ports.ObjectInfo{}, err
	}
	if expectedSHA256 != nil && subtle.ConstantTimeCompare(expectedSHA256, sha256Hash.Sum(nil)) != 1 {
		return ports.ObjectInfo{}, ports.ErrChecksumMismatch
	}
	md5Sum := md5Hash.Sum(nil)
	if input.ContentMD5 != "" {
		if subtle.ConstantTimeCompare(expectedMD5, md5Sum) != 1 {
			return ports.ObjectInfo{}, ports.ErrChecksumMismatch
		}
	}

	info := ports.ObjectInfo{
		Bucket:             input.Bucket,
		Key:                input.Key,
		Size:               written,
		ETag:               `"` + hex.EncodeToString(md5Sum) + `"`,
		ContentType:        input.ContentType,
		CacheControl:       input.CacheControl,
		ContentDisposition: input.ContentDisposition,
		ContentEncoding:    input.ContentEncoding,
		ContentLanguage:    input.ContentLanguage,
		Expires:            input.Expires,
		Metadata:           cloneMetadata(input.UserMetadata),
		LastModified:       time.Now().UTC().Truncate(time.Second),
	}
	record := objectRecord{
		Bucket:             info.Bucket,
		Key:                info.Key,
		Size:               info.Size,
		ETag:               info.ETag,
		ContentType:        info.ContentType,
		CacheControl:       info.CacheControl,
		ContentDisposition: info.ContentDisposition,
		ContentEncoding:    info.ContentEncoding,
		ContentLanguage:    info.ContentLanguage,
		Expires:            info.Expires,
		Metadata:           info.Metadata,
		LastModified:       info.LastModified,
	}
	encodedRecord, err := json.Marshal(record)
	if err != nil {
		return ports.ObjectInfo{}, fmt.Errorf("encode object metadata: %w", err)
	}
	if len(encodedRecord) > maxObjectMetadataBytes {
		return ports.ObjectInfo{}, ports.ErrMetadataTooLarge
	}
	if _, err := tmp.Write(encodedRecord); err != nil {
		return ports.ObjectInfo{}, fmt.Errorf("write object metadata: %w", err)
	}
	var trailer [16]byte
	binary.BigEndian.PutUint64(trailer[:8], uint64(len(encodedRecord)))
	copy(trailer[8:], trailerMagic)
	if _, err := tmp.Write(trailer[:]); err != nil {
		return ports.ObjectInfo{}, fmt.Errorf("write object trailer: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return ports.ObjectInfo{}, fmt.Errorf("sync staged object: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return ports.ObjectInfo{}, fmt.Errorf("close staged object: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return ports.ObjectInfo{}, err
	}
	if _, err := s.readBucket(input.Bucket); errors.Is(err, os.ErrNotExist) {
		return ports.ObjectInfo{}, ports.ErrBucketNotFound
	} else if err != nil {
		return ports.ObjectInfo{}, err
	}
	finalPath := s.objectPath(input.Bucket, input.Key)
	if old, err := s.readObjectRecordAt(finalPath); err == nil {
		if old.Bucket != input.Bucket || old.Key != input.Key {
			return ports.ObjectInfo{}, fmt.Errorf("object key hash collision")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return ports.ObjectInfo{}, err
	}
	if err := os.Rename(tmpName, finalPath); err != nil {
		return ports.ObjectInfo{}, fmt.Errorf("publish object: %w", err)
	}
	if err := syncDir(objectDir); err != nil {
		return ports.ObjectInfo{}, fmt.Errorf("sync published object directory: %w", err)
	}
	if err := syncDir(stagingDir); err != nil {
		return ports.ObjectInfo{}, fmt.Errorf("sync upload staging directory: %w", err)
	}
	return info, nil
}

func (s *Store) OpenObject(ctx context.Context, bucket, key string) (ports.ObjectInfo, io.ReadSeekCloser, error) {
	if err := ctx.Err(); err != nil {
		return ports.ObjectInfo{}, nil, err
	}
	if !validBucketName(bucket) {
		return ports.ObjectInfo{}, nil, ports.ErrInvalidBucket
	}
	if !validObjectKey(key) {
		return ports.ObjectInfo{}, nil, ports.ErrInvalidKey
	}
	select {
	case s.readSlots <- struct{}{}:
	default:
		return ports.ObjectInfo{}, nil, ports.ErrBusy
	}
	readSlotHeld := true
	defer func() {
		if readSlotHeld {
			<-s.readSlots
		}
	}()
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, err := s.readBucket(bucket); errors.Is(err, os.ErrNotExist) {
		return ports.ObjectInfo{}, nil, ports.ErrBucketNotFound
	} else if err != nil {
		return ports.ObjectInfo{}, nil, err
	}
	file, err := os.Open(s.objectPath(bucket, key))
	if errors.Is(err, os.ErrNotExist) {
		return ports.ObjectInfo{}, nil, ports.ErrObjectNotFound
	}
	if err != nil {
		return ports.ObjectInfo{}, nil, fmt.Errorf("open object: %w", err)
	}
	record, err := readObjectRecord(file)
	if err != nil {
		_ = file.Close()
		return ports.ObjectInfo{}, nil, fmt.Errorf("read object metadata: %w", err)
	}
	if record.Bucket != bucket || record.Key != key {
		_ = file.Close()
		return ports.ObjectInfo{}, nil, fmt.Errorf("object key hash collision")
	}
	body := &sectionReadCloser{
		SectionReader: io.NewSectionReader(file, 0, record.Size),
		file:          file,
		release:       func() { <-s.readSlots },
	}
	readSlotHeld = false
	return toInfo(record), body, nil
}

func (s *Store) DeleteObject(ctx context.Context, bucket, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validBucketName(bucket) {
		return ports.ErrInvalidBucket
	}
	if !validObjectKey(key) {
		return ports.ErrInvalidKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.readBucket(bucket); errors.Is(err, os.ErrNotExist) {
		return ports.ErrBucketNotFound
	} else if err != nil {
		return err
	}
	path := s.objectPath(bucket, key)
	record, err := s.readObjectRecordAt(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read object metadata before delete: %w", err)
	}
	if record.Bucket != bucket || record.Key != key {
		return fmt.Errorf("object key hash collision")
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync object deletion: %w", err)
	}
	return nil
}

func (s *Store) bucketPath(name string) string {
	return filepath.Join(s.root, "catalog", "buckets", hashName(name)+".json")
}

func (s *Store) bucketObjectsDir(name string) string {
	return filepath.Join(s.root, "catalog", "objects", hashName(name))
}

func (s *Store) bucketStagingDir(name string) string {
	return filepath.Join(s.root, "catalog", "staging", hashName(name))
}

func (s *Store) objectPath(bucket, key string) string {
	return filepath.Join(s.bucketObjectsDir(bucket), hashName(key)+".obj")
}

func (s *Store) readBucket(name string) (bucketRecord, error) {
	var record bucketRecord
	data, err := os.ReadFile(s.bucketPath(name))
	if err != nil {
		return record, err
	}
	if err := json.Unmarshal(data, &record); err != nil {
		return record, fmt.Errorf("decode bucket metadata: %w", err)
	}
	if record.Name != name {
		return record, fmt.Errorf("bucket hash collision")
	}
	return record, nil
}

func (s *Store) readObjectRecordAt(path string) (objectRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return objectRecord{}, err
	}
	defer file.Close()
	record, err := readObjectRecord(file)
	return record, err
}

func readObjectRecord(file *os.File) (objectRecord, error) {
	var record objectRecord
	stat, err := file.Stat()
	if err != nil {
		return record, err
	}
	if stat.Size() < 16 {
		return record, fmt.Errorf("object file is shorter than its trailer")
	}
	var trailer [16]byte
	if _, err := file.ReadAt(trailer[:], stat.Size()-int64(len(trailer))); err != nil {
		return record, err
	}
	if string(trailer[8:]) != trailerMagic {
		return record, fmt.Errorf("object trailer magic mismatch")
	}
	metadataSize := binary.BigEndian.Uint64(trailer[:8])
	if metadataSize > uint64(stat.Size()-16) || metadataSize > maxObjectMetadataBytes {
		return record, fmt.Errorf("object metadata length is invalid")
	}
	dataOffset := stat.Size() - 16 - int64(metadataSize)
	metadata := make([]byte, int(metadataSize))
	if _, err := file.ReadAt(metadata, dataOffset); err != nil {
		return record, err
	}
	if err := json.Unmarshal(metadata, &record); err != nil {
		return record, fmt.Errorf("decode object metadata: %w", err)
	}
	if record.Size != dataOffset || record.Size < 0 {
		return record, fmt.Errorf("object payload size does not match metadata")
	}
	return record, nil
}

func (s *Store) writeJSONAtomic(path string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	return syncDir(dir)
}

func (s *Store) cleanInterruptedUploads() error {
	root := filepath.Join(s.root, "catalog", "staging")
	buckets, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		if !bucket.IsDir() {
			continue
		}
		dir := filepath.Join(root, bucket.Name())
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		removed := false
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".tmp-") && !entry.IsDir() {
				if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
				removed = true
			}
		}
		if removed {
			if err := syncDir(dir); err != nil {
				return err
			}
		}
	}
	return nil
}

type sectionReadCloser struct {
	*io.SectionReader
	file      *os.File
	release   func()
	closeOnce sync.Once
	closeErr  error
}

func (r *sectionReadCloser) Close() error {
	r.closeOnce.Do(func() {
		r.closeErr = r.file.Close()
		r.release()
	})
	return r.closeErr
}

func toInfo(record objectRecord) ports.ObjectInfo {
	return ports.ObjectInfo{
		Bucket:             record.Bucket,
		Key:                record.Key,
		Size:               record.Size,
		ETag:               record.ETag,
		ContentType:        record.ContentType,
		CacheControl:       record.CacheControl,
		ContentDisposition: record.ContentDisposition,
		ContentEncoding:    record.ContentEncoding,
		ContentLanguage:    record.ContentLanguage,
		Expires:            record.Expires,
		Metadata:           cloneMetadata(record.Metadata),
		LastModified:       record.LastModified,
	}
}

func cloneMetadata(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func validateUserMetadata(values map[string]string) error {
	byteCount := 0
	for key, value := range values {
		if !isHeaderToken(key) || !isHeaderValue(value) {
			return ports.ErrInvalidMetadata
		}
		byteCount += len(key) + len(value)
		if byteCount > maxUserMetadataBytes {
			return ports.ErrMetadataTooLarge
		}
	}
	return nil
}

func isHeaderToken(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		b := value[i]
		if b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' || b >= '0' && b <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", rune(b)) {
			continue
		}
		return false
	}
	return true
}

func isHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		b := value[i]
		if b != '\t' && !(b >= 0x20 && b <= 0x7e) {
			return false
		}
	}
	return true
}

func hashName(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func validBucketName(name string) bool {
	return ports.ValidBucketName(name)
}

func validObjectKey(key string) bool {
	return ports.ValidObjectKey(key)
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func ensurePrivateDir(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return err
	}
	return os.Chmod(path, 0o700)
}

func isEmptyDir(path string) (bool, error) {
	dir, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer dir.Close()
	_, err = dir.Readdirnames(1)
	if errors.Is(err, io.EOF) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}
