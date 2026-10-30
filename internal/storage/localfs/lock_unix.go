//go:build unix

package localfs

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func acquireRootLock(root string) (*os.File, error) {
	path := filepath.Join(root, "LOCK")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open local storage lock: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("local storage root is already in use: %w", err)
	}
	return file, nil
}

func releaseRootLock(file *os.File) error {
	if file == nil {
		return nil
	}
	err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	return closeErr
}
