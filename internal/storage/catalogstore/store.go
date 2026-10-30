// Package catalogstore joins Buckboat's metadata and immutable-payload ports
// into the ObjectStore contract used by S3.
package catalogstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/rachmanzz/buckboat/internal/storage/ports"
)

type Store struct {
	catalog    ports.Catalog
	payloads   ports.PayloadStore
	mu         sync.RWMutex
	activePuts map[string]int
	closeOnce  sync.Once
	closeErr   error
}

func New(catalog ports.Catalog, payloads ports.PayloadStore) *Store {
	return &Store{catalog: catalog, payloads: payloads, activePuts: make(map[string]int)}
}

func (s *Store) CreateBucket(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.catalog.CreateBucket(ctx, name)
}

func (s *Store) HeadBucket(ctx context.Context, name string) error {
	return s.catalog.HeadBucket(ctx, name)
}

func (s *Store) ListObjects(ctx context.Context, query ports.ObjectListQuery) (ports.ObjectListPage, error) {
	return s.catalog.ListObjects(ctx, query)
}

func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activePuts[name] != 0 {
		return ports.ErrBucketNotEmpty
	}
	return s.catalog.DeleteBucket(ctx, name)
}

func (s *Store) PutObject(ctx context.Context, input ports.PutObjectInput) (ports.ObjectInfo, error) {
	if err := s.reservePut(ctx, input.Bucket); err != nil {
		return ports.ObjectInfo{}, err
	}
	defer s.releasePut(input.Bucket)
	info, payloadRef, err := s.payloads.PutPayload(ctx, input)
	if err != nil {
		return ports.ObjectInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		if cleanupErr := s.payloads.DeletePayload(context.Background(), payloadRef); cleanupErr != nil {
			err = errors.Join(err, fmt.Errorf("remove canceled payload before catalog publish: %w", cleanupErr))
		}
		return ports.ObjectInfo{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	previousRef, err := s.catalog.PublishObject(ctx, ports.CatalogObject{Info: info, PayloadRef: payloadRef})
	if err != nil {
		if errors.Is(err, ports.ErrPublishNotCommitted) {
			if cleanupErr := s.payloads.DeletePayload(context.Background(), payloadRef); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("remove unpublished payload: %w", cleanupErr))
			}
		}
		return ports.ObjectInfo{}, err
	}
	if previousRef != "" && previousRef != payloadRef {
		if err := s.payloads.DeletePayload(context.Background(), previousRef); err != nil {
			slog.Error("remove superseded payload", "error", err)
		}
	}
	return info, nil
}

func (s *Store) reservePut(ctx context.Context, bucket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.catalog.HeadBucket(ctx, bucket); err != nil {
		return err
	}
	s.activePuts[bucket]++
	return nil
}

func (s *Store) releasePut(bucket string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activePuts[bucket] <= 1 {
		delete(s.activePuts, bucket)
		return
	}
	s.activePuts[bucket]--
}

func (s *Store) OpenObject(ctx context.Context, bucket, key string) (ports.ObjectInfo, io.ReadSeekCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	object, err := s.catalog.GetObject(ctx, bucket, key)
	if err != nil {
		return ports.ObjectInfo{}, nil, err
	}
	body, err := s.payloads.OpenPayload(ctx, object.PayloadRef)
	if err != nil {
		return ports.ObjectInfo{}, nil, err
	}
	size, err := body.Seek(0, io.SeekEnd)
	if err == nil && size != object.Info.Size {
		err = fmt.Errorf("%w: expected %d bytes, found %d", ports.ErrCorruptObject, object.Info.Size, size)
	}
	if err == nil {
		_, err = body.Seek(0, io.SeekStart)
	}
	if err != nil {
		return ports.ObjectInfo{}, nil, errors.Join(err, body.Close())
	}
	return object.Info, body, nil
}

func (s *Store) DeleteObject(ctx context.Context, bucket, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	payloadRef, err := s.catalog.DeleteObject(ctx, bucket, key)
	if err != nil {
		return err
	}
	if payloadRef != "" {
		if err := s.payloads.DeletePayload(context.Background(), payloadRef); err != nil {
			slog.Error("remove deleted object payload", "error", err)
		}
	}
	return nil
}

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = errors.Join(s.catalog.Close(), s.payloads.Close())
	})
	return s.closeErr
}
