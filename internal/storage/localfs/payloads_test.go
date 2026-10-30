package localfs

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPayloadSyncFailureRemovesRenamedUnpublishedFile(t *testing.T) {
	for _, failedDir := range []string{"payloads", "staging"} {
		t.Run(failedDir, func(t *testing.T) {
			root := t.TempDir()
			for _, dir := range []string{"payloads", "staging"} {
				if err := os.Mkdir(filepath.Join(root, dir), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			staged, final := filepath.Join(root, "staging", "tmp"), filepath.Join(root, "payloads", "final")
			if err := os.WriteFile(staged, []byte("data"), 0o600); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("sync failed")
			failed := false
			err := publishPayload(staged, final, func(dir string) error {
				if !failed && filepath.Base(dir) == failedDir {
					failed = true
					return failure
				}
				return syncDir(dir)
			})
			if !errors.Is(err, failure) {
				t.Fatalf("err=%v", err)
			}
			if _, err := os.Stat(final); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unpublished payload retained: %v", err)
			}
		})
	}
}
