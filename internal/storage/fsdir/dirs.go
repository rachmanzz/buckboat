// Package fsdir persists storage-directory creation before files rely on it.
package fsdir

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// Ensure creates missing directories privately and syncs each new entry through
// its parent before creating the next child. Existing permissions are preserved.
func Ensure(path string) error {
	return ensure(path, Sync)
}

func ensure(path string, syncDirectory func(string) error) error {
	abs, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	var missing []string
	anchor := abs
	for {
		info, err := os.Stat(anchor)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("%s is not a directory", anchor)
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, anchor)
		anchor = filepath.Dir(anchor)
	}
	// Retrying a failed creation must also persist the last existing ancestor.
	if err := syncDirectory(anchor); err != nil {
		return err
	}
	if parent := filepath.Dir(anchor); parent != anchor {
		if err := syncDirectory(parent); err != nil {
			return err
		}
	}
	for i := len(missing) - 1; i >= 0; i-- {
		dir := missing[i]
		if err := os.Mkdir(dir, 0o700); err != nil {
			if !errors.Is(err, os.ErrExist) {
				return err
			}
			info, statErr := os.Stat(dir)
			if statErr != nil {
				return statErr
			}
			if !info.IsDir() {
				return fmt.Errorf("%s is not a directory", dir)
			}
		}
		if err := syncDirectory(dir); err != nil {
			return err
		}
		if err := syncDirectory(filepath.Dir(dir)); err != nil {
			return err
		}
	}
	return nil
}

func EnsurePrivate(path string) error {
	if err := Ensure(path); err != nil {
		return err
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return err
	}
	return Sync(path)
}

func Sync(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}
