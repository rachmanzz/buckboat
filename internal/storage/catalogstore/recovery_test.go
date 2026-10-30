package catalogstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rachmanzz/buckboat/internal/storage/ports"
)

type failedPayloadRefLookup struct {
	ports.Catalog
	err error
}

func (c failedPayloadRefLookup) FindPayloadRefs(context.Context, []string) (map[string]struct{}, error) {
	return nil, c.err
}

func TestRecoverPayloadsRemovesOrphansInBoundedBatches(t *testing.T) {
	root := t.TempDir()
	store, _ := newTestStore(t, root)
	if err := store.CreateBucket(t.Context(), "test-bucket"); err != nil {
		t.Fatal(err)
	}
	put(t, store, "keep", "committed bytes")
	payloadDir := filepath.Join(root, "payloads")
	for i := 0; i < ports.MaxPayloadRefBatchSize*2+7; i++ {
		ref := fmt.Sprintf("ffff%028x", i+1)
		if err := os.WriteFile(filepath.Join(payloadDir, ref+".bin"), []byte("orphan"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	if err := store.RecoverPayloads(t.Context()); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(payloadDir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("payload count after recovery=%d err=%v, want only the committed payload", len(entries), err)
	}
	if err := store.RecoverPayloads(t.Context()); err != nil {
		t.Fatalf("second recovery: %v", err)
	}
	_, body, err := store.OpenObject(t.Context(), "test-bucket", "keep")
	if err != nil {
		t.Fatalf("open committed object after recovery: %v", err)
	}
	data, readErr := io.ReadAll(body)
	closeErr := body.Close()
	if readErr != nil || closeErr != nil || string(data) != "committed bytes" {
		t.Fatalf("committed data=%q readErr=%v closeErr=%v", data, readErr, closeErr)
	}
}

func TestRecoverPayloadsKeepsCandidatesWhenCatalogLookupFails(t *testing.T) {
	root := t.TempDir()
	store, catalog := newTestStore(t, root)
	ref := strings.Repeat("a", 32)
	path := filepath.Join(root, "payloads", ref+".bin")
	if err := os.WriteFile(path, []byte("uncertain"), 0o600); err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("catalog unavailable")
	store.catalog = failedPayloadRefLookup{Catalog: catalog, err: wantErr}
	if err := store.RecoverPayloads(t.Context()); !errors.Is(err, wantErr) {
		t.Fatalf("recovery error=%v, want catalog lookup error", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("candidate payload was removed after uncertain lookup: %v", err)
	}
}
