//go:build !unix

package localfs

import (
	"errors"
	"os"
)

func acquireRootLock(string) (*os.File, error) {
	return nil, errors.New("local filesystem storage requires an OS with advisory file locks")
}

func releaseRootLock(*os.File) error { return nil }
