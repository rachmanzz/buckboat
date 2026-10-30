package server

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rachmanzz/buckboat/internal/storage/ports"
)

func TestNewRecoversOrphanPayloadAndPreservesCommittedObject(t *testing.T) {
	dataDir := t.TempDir()
	cfg := DefaultConfig()
	cfg.DataDir = dataDir
	cfg.AccessKeyID = "test-access-key"
	cfg.SecretAccessKey = "test-secret-key"
	cfg.S3Addr = "127.0.0.1:0"
	cfg.DocumentAddr = "127.0.0.2:0"
	cfg.HealthAddr = "127.0.0.3:0"

	runtime, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.store.CreateBucket(t.Context(), "test-bucket"); err != nil {
		runtime.Close()
		t.Fatal(err)
	}
	_, err = runtime.store.PutObject(t.Context(), ports.PutObjectInput{
		Bucket: "test-bucket", Key: "keep", Body: strings.NewReader("committed bytes"), ContentLength: 15,
	})
	if err != nil {
		runtime.Close()
		t.Fatal(err)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}

	ref := strings.Repeat("b", 32)
	orphanPath := filepath.Join(dataDir, "payloads", ref+".bin")
	if err := os.WriteFile(orphanPath, []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}

	restarted, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Fatalf("orphan still exists after startup recovery: %v", err)
	}
	_, body, err := restarted.store.OpenObject(t.Context(), "test-bucket", "keep")
	if err != nil {
		t.Fatalf("committed object unavailable after startup recovery: %v", err)
	}
	data, readErr := io.ReadAll(body)
	closeErr := body.Close()
	if readErr != nil || closeErr != nil || string(data) != "committed bytes" {
		t.Fatalf("committed data=%q readErr=%v closeErr=%v", data, readErr, closeErr)
	}
}
