package fsdir

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestEnsurePersistsCreatedParentsBeforeCreatingTheirChildren(t *testing.T) {
	base := t.TempDir()
	first := filepath.Join(base, "new")
	child := filepath.Join(first, "data")
	failure := errors.New("directory sync failed")
	err := ensure(child, func(path string) error {
		if path == base {
			if _, err := os.Stat(first); err == nil {
				return failure
			}
		}
		return Sync(path)
	})
	if !errors.Is(err, failure) {
		t.Fatalf("err=%v", err)
	}
	if _, err := os.Stat(child); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("child created before parent entry persisted: %v", err)
	}
	if err := EnsurePrivate(child); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{first, child} {
		info, err := os.Stat(path)
		if err != nil || info.Mode().Perm() != 0o700 {
			t.Fatalf("path=%s info=%v err=%v", path, info, err)
		}
	}
}

func TestEnsureExistingDirectoryRetriesItsParentSync(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "data")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	parentSynced := false
	if err := ensure(target, func(path string) error {
		if path == base {
			parentSynced = true
		}
		return Sync(path)
	}); err != nil {
		t.Fatal(err)
	}
	if !parentSynced {
		t.Fatal("existing directory entry was not synced through its parent")
	}
	info, err := os.Stat(target)
	if err != nil || info.Mode().Perm() != 0o755 {
		t.Fatalf("Ensure changed existing permissions: %v %v", info, err)
	}
}
