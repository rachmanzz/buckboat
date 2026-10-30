package localfs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rachmanzz/buckboat/internal/storage/ports"
)

func TestObjectLifecycleSurvivesReopen(t *testing.T) {
	root := t.TempDir()
	store := openTestStore(t, root)
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "docs-bucket"); err != nil {
		t.Fatal(err)
	}

	first := []byte("first version")
	firstInfo, err := store.PutObject(ctx, ports.PutObjectInput{
		Bucket: "docs-bucket", Key: "folder/a.txt", Body: bytes.NewReader(first), ContentLength: int64(len(first)),
		PayloadSHA256: checksum(first), ContentType: "text/plain", UserMetadata: map[string]string{"owner": "editor"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if firstInfo.Size != int64(len(first)) || firstInfo.ETag == "" {
		t.Fatalf("unexpected object info: %#v", firstInfo)
	}

	second := []byte("replacement")
	if _, err := store.PutObject(ctx, ports.PutObjectInput{
		Bucket: "docs-bucket", Key: "folder/a.txt", Body: bytes.NewReader(second), ContentLength: int64(len(second)),
		PayloadSHA256: checksum(second), ContentType: "text/plain",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openTestStore(t, root)
	defer store.Close()
	info, body, err := store.OpenObject(ctx, "docs-bucket", "folder/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, second) || info.Size != int64(len(second)) || info.Metadata != nil {
		t.Fatalf("reopened object mismatch: info=%#v body=%q", info, got)
	}
	if err := store.DeleteBucket(ctx, "docs-bucket"); !errors.Is(err, ports.ErrBucketNotEmpty) {
		t.Fatalf("DeleteBucket() error = %v, want ErrBucketNotEmpty", err)
	}
	if err := store.DeleteObject(ctx, "docs-bucket", "folder/a.txt"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteObject(ctx, "docs-bucket", "folder/a.txt"); err != nil {
		t.Fatalf("repeat DeleteObject() error = %v", err)
	}
	if err := store.DeleteBucket(ctx, "docs-bucket"); err != nil {
		t.Fatal(err)
	}
}

func TestObjectKeyNeverBecomesFilesystemPath(t *testing.T) {
	root := t.TempDir()
	store := openTestStore(t, root)
	defer store.Close()
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "safe-bucket"); err != nil {
		t.Fatal(err)
	}
	key := "../../outside-file"
	payload := []byte("contained")
	if _, err := store.PutObject(ctx, ports.PutObjectInput{
		Bucket: "safe-bucket", Key: key, Body: bytes.NewReader(payload), ContentLength: int64(len(payload)), PayloadSHA256: checksum(payload),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(root), "outside-file")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("object key escaped storage root, stat error=%v", err)
	}
	info, body, err := store.OpenObject(ctx, "safe-bucket", key)
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil || !bytes.Equal(got, payload) || info.Key != key {
		t.Fatalf("retrieved key mismatch: info=%#v body=%q error=%v", info, got, err)
	}
}

func TestStoreRejectsSecondProcessForSameRoot(t *testing.T) {
	root := t.TempDir()
	first := openTestStore(t, root)
	defer first.Close()
	objectsDir := filepath.Join(root, "catalog", "objects")
	if err := os.Chmod(objectsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Config{RootDir: root, MaxObjectBytes: 1 << 20, MaxConcurrentPuts: 2, MaxConcurrentReads: 128}); err == nil {
		t.Fatal("Open() succeeded while another store holds the root lock")
	}
	info, err := os.Stat(objectsDir)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o755 {
		t.Fatalf("second Open() changed live store directory permissions to %04o", got)
	}
}

func TestReadAdmissionIsBoundedAndReleasedOnClose(t *testing.T) {
	store, err := Open(Config{
		RootDir: t.TempDir(), MaxObjectBytes: 1 << 20,
		MaxConcurrentPuts: 1, MaxConcurrentReads: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "read-bucket"); err != nil {
		t.Fatal(err)
	}
	payload := []byte("reader")
	if _, err := store.PutObject(ctx, ports.PutObjectInput{
		Bucket: "read-bucket", Key: "item", Body: bytes.NewReader(payload), ContentLength: int64(len(payload)), PayloadSHA256: checksum(payload),
	}); err != nil {
		t.Fatal(err)
	}
	_, first, err := store.OpenObject(ctx, "read-bucket", "item")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.OpenObject(ctx, "read-bucket", "item"); !errors.Is(err, ports.ErrBusy) {
		t.Fatalf("second OpenObject() error = %v, want ErrBusy", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	_, second, err := store.OpenObject(ctx, "read-bucket", "item")
	if err != nil {
		t.Fatalf("OpenObject() after releasing reader: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFailedChecksumDoesNotReplacePublishedObject(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "checksum-bucket"); err != nil {
		t.Fatal(err)
	}
	original := []byte("published")
	if _, err := store.PutObject(ctx, ports.PutObjectInput{
		Bucket: "checksum-bucket", Key: "item", Body: bytes.NewReader(original), ContentLength: int64(len(original)), PayloadSHA256: checksum(original),
	}); err != nil {
		t.Fatal(err)
	}
	replacement := []byte("invalid replacement")
	if _, err := store.PutObject(ctx, ports.PutObjectInput{
		Bucket: "checksum-bucket", Key: "item", Body: bytes.NewReader(replacement), ContentLength: int64(len(replacement)), PayloadSHA256: checksum(original),
	}); !errors.Is(err, ports.ErrChecksumMismatch) {
		t.Fatalf("PutObject() error = %v, want ErrChecksumMismatch", err)
	}
	_, body, err := store.OpenObject(ctx, "checksum-bucket", "item")
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("failed replacement changed published object: got %q", got)
	}
}

func TestEmptyObjectIsStoredAndRead(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "empty-bucket"); err != nil {
		t.Fatal(err)
	}
	empty := []byte{}
	info, err := store.PutObject(ctx, ports.PutObjectInput{
		Bucket: "empty-bucket", Key: "empty", Body: bytes.NewReader(empty), ContentLength: 0, PayloadSHA256: checksum(empty),
	})
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 0 {
		t.Fatalf("empty object size = %d", info.Size)
	}
	_, body, err := store.OpenObject(ctx, "empty-bucket", "empty")
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil || len(got) != 0 {
		t.Fatalf("empty object read = %q, error=%v", got, err)
	}
}

func TestPutRejectsObjectAboveConfiguredLimit(t *testing.T) {
	store, err := Open(Config{RootDir: t.TempDir(), MaxObjectBytes: 4, MaxConcurrentPuts: 1, MaxConcurrentReads: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "limited-bucket"); err != nil {
		t.Fatal(err)
	}
	payload := []byte("12345")
	if _, err := store.PutObject(ctx, ports.PutObjectInput{
		Bucket: "limited-bucket", Key: "too-large", Body: bytes.NewReader(payload), ContentLength: -1, PayloadSHA256: checksum(payload),
	}); !errors.Is(err, ports.ErrObjectTooLarge) {
		t.Fatalf("PutObject() error = %v, want ErrObjectTooLarge", err)
	}
	if _, _, err := store.OpenObject(ctx, "limited-bucket", "too-large"); !errors.Is(err, ports.ErrObjectNotFound) {
		t.Fatalf("OpenObject() error = %v, want ErrObjectNotFound", err)
	}
}

func TestPutRejectsOversizedStoredMetadataWithoutReplacingObject(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "metadata-bucket"); err != nil {
		t.Fatal(err)
	}
	original := []byte("published")
	if _, err := store.PutObject(ctx, ports.PutObjectInput{
		Bucket: "metadata-bucket", Key: "item", Body: bytes.NewReader(original), ContentLength: int64(len(original)), PayloadSHA256: checksum(original),
	}); err != nil {
		t.Fatal(err)
	}
	replacement := []byte("replacement")
	if _, err := store.PutObject(ctx, ports.PutObjectInput{
		Bucket: "metadata-bucket", Key: "item", Body: bytes.NewReader(replacement), ContentLength: int64(len(replacement)),
		PayloadSHA256: checksum(replacement), ContentType: strings.Repeat("x", 70<<10),
	}); !errors.Is(err, ports.ErrMetadataTooLarge) {
		t.Fatalf("PutObject() error = %v, want ErrMetadataTooLarge", err)
	}
	_, body, err := store.OpenObject(ctx, "metadata-bucket", "item")
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("oversized metadata changed published object: got %q, error=%v", got, err)
	}
}

func TestDeleteBucketRejectsInProgressUpload(t *testing.T) {
	store := openTestStore(t, t.TempDir())
	defer store.Close()
	ctx := context.Background()
	if err := store.CreateBucket(ctx, "upload-bucket"); err != nil {
		t.Fatal(err)
	}

	body := &gatedReader{started: make(chan struct{}), resume: make(chan struct{}), data: []byte("upload")}
	defer func() {
		select {
		case <-body.resume:
		default:
			close(body.resume)
		}
	}()
	putResult := make(chan error, 1)
	go func() {
		_, err := store.PutObject(ctx, ports.PutObjectInput{
			Bucket: "upload-bucket", Key: "item", Body: body, ContentLength: -1, PayloadSHA256: checksum([]byte("upload")),
		})
		putResult <- err
	}()

	select {
	case <-body.started:
	case <-time.After(5 * time.Second):
		t.Fatal("PUT did not start reading its body")
	}
	deleteErr := store.DeleteBucket(ctx, "upload-bucket")
	close(body.resume)
	putErr := <-putResult
	if !errors.Is(deleteErr, ports.ErrBucketNotEmpty) {
		t.Fatalf("DeleteBucket() during upload error = %v, want ErrBucketNotEmpty", deleteErr)
	}
	if putErr != nil {
		t.Fatalf("PutObject() error = %v", putErr)
	}
}

type gatedReader struct {
	started chan struct{}
	resume  chan struct{}
	data    []byte
	once    bool
}

func (r *gatedReader) Read(p []byte) (int, error) {
	if !r.once {
		r.once = true
		close(r.started)
		<-r.resume
	}
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func openTestStore(t *testing.T, root string) *Store {
	t.Helper()
	store, err := Open(Config{RootDir: root, MaxObjectBytes: 1 << 20, MaxConcurrentPuts: 2, MaxConcurrentReads: 128})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func checksum(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}
