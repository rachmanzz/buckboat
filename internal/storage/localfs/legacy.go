package localfs

import (
	"errors"
	"os"
	"path/filepath"
)

// HasLegacyCatalogData reports whether the pre-SQLite filesystem catalog has
// bucket or object records that need an explicit migration.
func HasLegacyCatalogData(root string) (bool, error) {
	for _, dir := range []string{
		filepath.Join(root, "catalog", "buckets"),
		filepath.Join(root, "catalog", "objects"),
	} {
		found, err := directoryHasFiles(dir)
		if err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
	}
	return false, nil
}

func directoryHasFiles(path string) (bool, error) {
	entries, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			return true, nil
		}
		found, err := directoryHasFiles(filepath.Join(path, entry.Name()))
		if err != nil {
			return false, err
		}
		if found {
			return true, nil
		}
	}
	return false, nil
}
