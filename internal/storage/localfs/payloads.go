package localfs

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rachmanzz/buckboat/internal/storage/ports"
)

func (s *Store) PutPayload(ctx context.Context, input ports.PutObjectInput) (ports.ObjectInfo, string, error) {
	if err := ctx.Err(); err != nil {
		return ports.ObjectInfo{}, "", err
	}
	if err := validateUserMetadata(input.UserMetadata); err != nil {
		return ports.ObjectInfo{}, "", err
	}
	if !validBucketName(input.Bucket) {
		return ports.ObjectInfo{}, "", ports.ErrInvalidBucket
	}
	if !validObjectKey(input.Key) {
		return ports.ObjectInfo{}, "", ports.ErrInvalidKey
	}
	if input.Body == nil {
		if input.ContentLength != 0 {
			return ports.ObjectInfo{}, "", ports.ErrInvalidArgument
		}
		input.Body = strings.NewReader("")
	}
	if input.ContentLength < -1 {
		return ports.ObjectInfo{}, "", ports.ErrInvalidArgument
	}
	if input.ContentLength > s.maxBytes {
		return ports.ObjectInfo{}, "", ports.ErrObjectTooLarge
	}
	var expectedSHA256 []byte
	if input.PayloadSHA256 != "" && input.PayloadSHA256 != "UNSIGNED-PAYLOAD" {
		var err error
		expectedSHA256, err = hex.DecodeString(input.PayloadSHA256)
		if err != nil || len(expectedSHA256) != sha256.Size {
			return ports.ObjectInfo{}, "", ports.ErrInvalidArgument
		}
	}
	var expectedMD5 []byte
	if input.ContentMD5 != "" {
		var err error
		expectedMD5, err = base64.StdEncoding.DecodeString(input.ContentMD5)
		if err != nil || len(expectedMD5) != md5.Size {
			return ports.ObjectInfo{}, "", ports.ErrInvalidArgument
		}
	}
	select {
	case s.putSlots <- struct{}{}:
		defer func() { <-s.putSlots }()
	default:
		return ports.ObjectInfo{}, "", ports.ErrBusy
	}

	stagingDir := filepath.Join(s.root, "payload-staging")
	tmp, err := os.CreateTemp(stagingDir, ".tmp-*")
	if err != nil {
		return ports.ObjectInfo{}, "", fmt.Errorf("create payload staging file: %w", err)
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
			return ports.ObjectInfo{}, "", ctxErr
		}
		return ports.ObjectInfo{}, "", fmt.Errorf("%w: %v", ports.ErrIncompleteBody, err)
	}
	if written > s.maxBytes {
		return ports.ObjectInfo{}, "", ports.ErrObjectTooLarge
	}
	if input.ContentLength >= 0 && written != input.ContentLength {
		return ports.ObjectInfo{}, "", fmt.Errorf("%w: received %d bytes, expected %d", ports.ErrIncompleteBody, written, input.ContentLength)
	}
	if err := ctx.Err(); err != nil {
		return ports.ObjectInfo{}, "", err
	}
	if expectedSHA256 != nil && subtle.ConstantTimeCompare(expectedSHA256, sha256Hash.Sum(nil)) != 1 {
		return ports.ObjectInfo{}, "", ports.ErrChecksumMismatch
	}
	md5Sum := md5Hash.Sum(nil)
	if input.ContentMD5 != "" && subtle.ConstantTimeCompare(expectedMD5, md5Sum) != 1 {
		return ports.ObjectInfo{}, "", ports.ErrChecksumMismatch
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
	metadataRecord := objectRecord{
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
	encodedMetadata, err := json.Marshal(metadataRecord)
	if err != nil {
		return ports.ObjectInfo{}, "", fmt.Errorf("encode object metadata: %w", err)
	}
	if len(encodedMetadata) > maxObjectMetadataBytes {
		return ports.ObjectInfo{}, "", ports.ErrMetadataTooLarge
	}
	if err := tmp.Sync(); err != nil {
		return ports.ObjectInfo{}, "", fmt.Errorf("sync staged payload: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return ports.ObjectInfo{}, "", fmt.Errorf("close staged payload: %w", err)
	}

	var idBytes [16]byte
	if _, err := rand.Read(idBytes[:]); err != nil {
		return ports.ObjectInfo{}, "", fmt.Errorf("generate payload reference: %w", err)
	}
	payloadRef := hex.EncodeToString(idBytes[:])
	payloadDir := filepath.Join(s.root, "payloads")
	finalPath := filepath.Join(payloadDir, payloadRef+".bin")
	if err := publishPayload(tmpName, finalPath, syncDir); err != nil {
		return ports.ObjectInfo{}, "", err
	}
	return info, payloadRef, nil
}

func publishPayload(staged, final string, syncDirectory func(string) error) (publishErr error) {
	if err := os.Rename(staged, final); err != nil {
		return fmt.Errorf("publish staged payload: %w", err)
	}
	defer func() {
		if publishErr != nil {
			if err := os.Remove(final); err != nil && !errors.Is(err, os.ErrNotExist) {
				publishErr = errors.Join(publishErr, fmt.Errorf("remove unpublished payload: %w", err))
			}
			if err := syncDirectory(filepath.Dir(final)); err != nil {
				publishErr = errors.Join(publishErr, fmt.Errorf("sync unpublished payload cleanup: %w", err))
			}
		}
	}()
	if err := syncDirectory(filepath.Dir(final)); err != nil {
		return fmt.Errorf("sync published payload directory: %w", err)
	}
	if err := syncDirectory(filepath.Dir(staged)); err != nil {
		return fmt.Errorf("sync payload staging directory: %w", err)
	}
	return nil
}

func (s *Store) OpenPayload(ctx context.Context, payloadRef string) (io.ReadSeekCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validPayloadRef(payloadRef) {
		return nil, ports.ErrInvalidArgument
	}
	select {
	case s.readSlots <- struct{}{}:
	default:
		return nil, ports.ErrBusy
	}
	file, err := os.Open(filepath.Join(s.root, "payloads", payloadRef+".bin"))
	if err != nil {
		<-s.readSlots
		return nil, fmt.Errorf("open immutable payload: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		<-s.readSlots
		return nil, fmt.Errorf("stat immutable payload: %w", err)
	}
	return &sectionReadCloser{
		SectionReader: io.NewSectionReader(file, 0, stat.Size()),
		file:          file,
		release:       func() { <-s.readSlots },
	}, nil
}

func (s *Store) DeletePayload(ctx context.Context, payloadRef string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validPayloadRef(payloadRef) {
		return ports.ErrInvalidArgument
	}
	path := filepath.Join(s.root, "payloads", payloadRef+".bin")
	if err := os.Remove(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("remove immutable payload: %w", err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync payload deletion: %w", err)
	}
	return nil
}

func (s *Store) cleanInterruptedPayloadUploads() error {
	dir := filepath.Join(s.root, "payload-staging")
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
		return syncDir(dir)
	}
	return nil
}

func validPayloadRef(payloadRef string) bool {
	if len(payloadRef) != 32 {
		return false
	}
	decoded, err := hex.DecodeString(payloadRef)
	return err == nil && hex.EncodeToString(decoded) == payloadRef
}
