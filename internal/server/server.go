package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"github.com/rachmanzz/buckboat/internal/storage/catalogstore"
	"github.com/rachmanzz/buckboat/internal/storage/localfs"
	"github.com/rachmanzz/buckboat/internal/storage/ports"
	"github.com/rachmanzz/buckboat/internal/storage/sqlite"
	"github.com/rachmanzz/buckboat/internal/transport/document"
	"github.com/rachmanzz/buckboat/internal/transport/health"
	"github.com/rachmanzz/buckboat/internal/transport/s3"
)

const maxSinglePutBytes int64 = 5_000_000_000

type Config struct {
	S3Addr             string
	DocumentAddr       string
	HealthAddr         string
	DataDir            string
	CatalogAdapter     string
	AccessKeyID        string
	SecretAccessKey    string
	Region             string
	MaxObjectBytes     int64
	MaxConcurrentPuts  int
	MaxConcurrentReads int
}

type Runtime struct {
	servers    []*http.Server
	store      ports.ObjectStore
	closeStore func() error
	requests   sync.WaitGroup
}

func DefaultConfig() Config {
	return Config{
		S3Addr:             "127.0.0.1:9400",
		DocumentAddr:       "127.0.0.1:9401",
		HealthAddr:         "127.0.0.1:9402",
		DataDir:            "./.buckboat-data",
		CatalogAdapter:     "sqlite",
		Region:             "us-east-1",
		MaxObjectBytes:     maxSinglePutBytes,
		MaxConcurrentPuts:  16,
		MaxConcurrentReads: 128,
	}
}

func New(cfg Config) (*Runtime, error) {
	addresses := []string{cfg.S3Addr, cfg.DocumentAddr, cfg.HealthAddr}
	seen := make(map[string]struct{}, len(addresses))
	for _, addr := range addresses {
		if addr == "" {
			return nil, fmt.Errorf("listener address must not be empty")
		}
		if _, exists := seen[addr]; exists {
			return nil, fmt.Errorf("listener address %q is configured more than once", addr)
		}
		seen[addr] = struct{}{}
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, fmt.Errorf("BUCKBOAT_ACCESS_KEY_ID and BUCKBOAT_SECRET_ACCESS_KEY are required")
	}
	if cfg.Region == "" || cfg.DataDir == "" {
		return nil, fmt.Errorf("storage region and data directory are required")
	}
	if cfg.CatalogAdapter != "sqlite" {
		return nil, fmt.Errorf("unsupported catalog adapter %q", cfg.CatalogAdapter)
	}
	if cfg.MaxObjectBytes <= 0 || cfg.MaxObjectBytes > maxSinglePutBytes {
		return nil, fmt.Errorf("maximum object size must be between 1 and %d bytes", maxSinglePutBytes)
	}
	if cfg.MaxConcurrentPuts <= 0 || cfg.MaxConcurrentReads <= 0 {
		return nil, fmt.Errorf("maximum concurrent PUT and read counts must be positive")
	}

	legacyData, err := localfs.HasLegacyCatalogData(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("inspect existing local catalog data: %w", err)
	}
	if legacyData {
		return nil, fmt.Errorf("legacy filesystem catalog data found in %q; migrate it before using the SQLite catalog adapter", cfg.DataDir)
	}

	payloads, err := localfs.Open(localfs.Config{
		RootDir:            cfg.DataDir,
		MaxObjectBytes:     cfg.MaxObjectBytes,
		MaxConcurrentPuts:  cfg.MaxConcurrentPuts,
		MaxConcurrentReads: cfg.MaxConcurrentReads,
	})
	if err != nil {
		return nil, fmt.Errorf("open local payload store: %w", err)
	}
	catalog, err := sqlite.OpenCatalog(filepath.Join(cfg.DataDir, "catalog", "buckboat.sqlite"))
	if err != nil {
		_ = payloads.Close()
		return nil, err
	}

	store := catalogstore.New(catalog, payloads)
	if err := store.RecoverPayloads(context.Background()); err != nil {
		recoveryErr := fmt.Errorf("recover local payloads: %w", err)
		if closeErr := store.Close(); closeErr != nil {
			recoveryErr = errors.Join(recoveryErr, fmt.Errorf("close storage after recovery failure: %w", closeErr))
		}
		return nil, recoveryErr
	}
	runtime := &Runtime{store: store, closeStore: store.Close}
	runtime.servers = []*http.Server{
		newHTTPServer(cfg.S3Addr, runtime.track(s3.NewRouter(store, s3.AuthConfig{
			AccessKeyID:     cfg.AccessKeyID,
			SecretAccessKey: cfg.SecretAccessKey,
			Region:          cfg.Region,
		}))),
		newHTTPServer(cfg.DocumentAddr, runtime.track(document.NewRouter())),
		newHTTPServer(cfg.HealthAddr, runtime.track(health.NewRouter())),
	}
	return runtime, nil
}

// Close releases storage resources after Run has stopped the listeners.
func (r *Runtime) Close() error {
	if r == nil || r.closeStore == nil {
		return nil
	}
	// Run closes listeners before Close is called. Wait for request handlers to
	// unwind (including forced connection-close paths) before releasing the
	// local root lock to another process.
	r.requests.Wait()
	return r.closeStore()
}

func (r *Runtime) track(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.requests.Add(1)
		defer r.requests.Done()
		next.ServeHTTP(w, req)
	})
}

func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		// Total stream deadlines are not implemented in this local profile.
	}
}
