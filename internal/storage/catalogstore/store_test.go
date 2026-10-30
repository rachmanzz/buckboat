package catalogstore

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rachmanzz/buckboat/internal/storage/localfs"
	"github.com/rachmanzz/buckboat/internal/storage/ports"
	"github.com/rachmanzz/buckboat/internal/storage/sqlite"
)

type publishHook struct {
	ports.Catalog
	publish func(context.Context, ports.CatalogObject) (string, error)
}

func (c publishHook) PublishObject(ctx context.Context, object ports.CatalogObject) (string, error) {
	return c.publish(ctx, object)
}

type failedPayloadDelete struct {
	ports.PayloadStore
	err error
}

func (p failedPayloadDelete) DeletePayload(context.Context, string) error { return p.err }

func TestFailedReplacementPreservesCurrentObjectAndReportsCleanupFailure(t *testing.T) {
	root := t.TempDir()
	store, catalog := newTestStore(t, root)
	if err := store.CreateBucket(t.Context(), "test-bucket"); err != nil {
		t.Fatal(err)
	}
	put(t, store, "key", "old")
	store.catalog = publishHook{Catalog: catalog, publish: func(context.Context, ports.CatalogObject) (string, error) {
		return "", errors.Join(ports.ErrPublishNotCommitted, ports.ErrBusy)
	}}
	input := ports.PutObjectInput{Bucket: "test-bucket", Key: "key", Body: strings.NewReader("new"), ContentLength: 3}
	if _, err := store.PutObject(t.Context(), input); !errors.Is(err, ports.ErrBusy) {
		t.Fatalf("err=%v", err)
	}
	files, err := os.ReadDir(filepath.Join(root, "payloads"))
	if err != nil || len(files) != 1 {
		t.Fatalf("files=%d err=%v", len(files), err)
	}
	_, body, err := store.OpenObject(t.Context(), "test-bucket", "key")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(body)
	body.Close()
	if err != nil || string(data) != "old" {
		t.Fatalf("current object=%q err=%v", data, err)
	}
	cleanupErr := errors.New("payload removal failed")
	store.payloads = failedPayloadDelete{PayloadStore: store.payloads, err: cleanupErr}
	input.Body = strings.NewReader("new")
	if _, err := store.PutObject(t.Context(), input); !errors.Is(err, ports.ErrBusy) || !errors.Is(err, cleanupErr) {
		t.Fatalf("cleanup failure was hidden: %v", err)
	}
}

func newTestStore(t *testing.T, root string) (*Store, *sqlite.Catalog) {
	t.Helper()
	payloads, err := localfs.Open(localfs.Config{RootDir: root, MaxObjectBytes: 4096, MaxConcurrentPuts: 2, MaxConcurrentReads: 1})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := sqlite.OpenCatalog(filepath.Join(root, "catalog", "buckboat.sqlite"))
	if err != nil {
		payloads.Close()
		t.Fatal(err)
	}
	store := New(catalog, payloads)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store, catalog
}

func put(t *testing.T, store *Store, key, data string) {
	t.Helper()
	_, err := store.PutObject(t.Context(), ports.PutObjectInput{Bucket: "test-bucket", Key: key, Body: strings.NewReader(data), ContentLength: int64(len(data))})
	if err != nil {
		t.Fatal(err)
	}
}

func TestFailedPublicationCleansOnlyDefinitelyUncommittedPayloads(t *testing.T) {
	for _, mode := range []string{"busy", "cancel-before-begin", "unknown-uncommitted", "unknown-committed"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			store, catalog := newTestStore(t, root)
			if err := store.CreateBucket(t.Context(), "test-bucket"); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			store.catalog = publishHook{Catalog: catalog, publish: func(ctx context.Context, object ports.CatalogObject) (string, error) {
				switch mode {
				case "busy":
					return "", errors.Join(ports.ErrPublishNotCommitted, ports.ErrBusy)
				case "cancel-before-begin":
					cancel()
					return catalog.PublishObject(ctx, object)
				case "unknown-committed":
					if _, err := catalog.PublishObject(ctx, object); err != nil {
						return "", err
					}
				}
				return "", errors.New("uncertain publication outcome")
			}}
			_, err := store.PutObject(ctx, ports.PutObjectInput{Bucket: "test-bucket", Key: "key", Body: strings.NewReader("abcdef"), ContentLength: 6})
			if err == nil {
				t.Fatal("expected publish failure")
			}
			files, err := os.ReadDir(filepath.Join(root, "payloads"))
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			if strings.HasPrefix(mode, "unknown-") {
				want = 1
			}
			if len(files) != want {
				t.Fatalf("payload files=%d want=%d", len(files), want)
			}
			if mode == "unknown-committed" {
				_, body, err := store.OpenObject(t.Context(), "test-bucket", "key")
				if err != nil {
					t.Fatalf("committed payload was lost: %v", err)
				}
				defer body.Close()
				data, err := io.ReadAll(body)
				if err != nil || string(data) != "abcdef" {
					t.Fatalf("data=%q err=%v", data, err)
				}
			}
		})
	}
}

func TestReadRejectsSizeMismatchAndReleasesReader(t *testing.T) {
	for _, size := range []int64{3, 9} {
		t.Run(string(rune('0'+size)), func(t *testing.T) {
			root := t.TempDir()
			store, catalog := newTestStore(t, root)
			if err := store.CreateBucket(t.Context(), "test-bucket"); err != nil {
				t.Fatal(err)
			}
			put(t, store, "key", "abcdef")
			put(t, store, "healthy", "ok")
			record, err := catalog.GetObject(t.Context(), "test-bucket", "key")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Truncate(filepath.Join(root, "payloads", record.PayloadRef+".bin"), size); err != nil {
				t.Fatal(err)
			}
			_, body, err := store.OpenObject(t.Context(), "test-bucket", "key")
			if !errors.Is(err, ports.ErrCorruptObject) || body != nil {
				t.Fatalf("body=%v err=%v", body, err)
			}
			_, body, err = store.OpenObject(t.Context(), "test-bucket", "healthy")
			if err != nil {
				t.Fatalf("reader slot was leaked: %v", err)
			}
			body.Close()
		})
	}
}

func TestLifecycleReopenAndExistingReader(t *testing.T) {
	root := t.TempDir()
	store, _ := newTestStore(t, root)
	if err := store.CreateBucket(t.Context(), "test-bucket"); err != nil {
		t.Fatal(err)
	}
	put(t, store, "key", "old")
	_, old, err := store.OpenObject(t.Context(), "test-bucket", "key")
	if err != nil {
		t.Fatal(err)
	}
	put(t, store, "key", "new")
	data, err := io.ReadAll(old)
	old.Close()
	if err != nil || string(data) != "old" {
		t.Fatalf("old data=%q err=%v", data, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, _ := newTestStore(t, root)
	_, body, err := reopened.OpenObject(t.Context(), "test-bucket", "key")
	if err != nil {
		t.Fatal(err)
	}
	data, err = io.ReadAll(body)
	body.Close()
	if err != nil || string(data) != "new" {
		t.Fatalf("reopened data=%q err=%v", data, err)
	}
	if err := reopened.DeleteObject(t.Context(), "test-bucket", "key"); err != nil {
		t.Fatal(err)
	}
	if err := reopened.DeleteBucket(t.Context(), "test-bucket"); err != nil {
		t.Fatal(err)
	}
}
