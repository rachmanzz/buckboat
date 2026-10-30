package sqlite

import (
	"context"
	"errors"
	"fmt"
	"github.com/rachmanzz/buckboat/internal/storage/ports"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestPublicationOutcomeSeparatesPreCommitAndCommitErrors(t *testing.T) {
	c, err := OpenCatalog(filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.CreateBucket(t.Context(), "test-bucket"); err != nil {
		t.Fatal(err)
	}
	object := ports.CatalogObject{Info: ports.ObjectInfo{Bucket: "test-bucket", Key: "key", LastModified: time.Now()}, PayloadRef: "candidate"}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.PublishObject(ctx, object); !errors.Is(err, ports.ErrPublishNotCommitted) || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-transaction error=%v", err)
	}
	// A deferred constraint fails only at COMMIT, so the adapter must keep the
	// outcome conservative instead of authorizing payload deletion.
	for _, statement := range []string{
		`CREATE TABLE commit_guard (bucket TEXT REFERENCES buckets(name) DEFERRABLE INITIALLY DEFERRED)`,
		`CREATE TRIGGER fail_commit AFTER INSERT ON objects BEGIN INSERT INTO commit_guard VALUES ('missing-bucket'); END`,
	} {
		if _, err := c.db.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.PublishObject(t.Context(), object); err == nil || errors.Is(err, ports.ErrPublishNotCommitted) {
		t.Fatalf("commit error must have uncertain outcome: %v", err)
	}
}

type expectedListEntry struct {
	Key   string
	Group bool
}

func TestListingMatchesIndependentOracle(t *testing.T) {
	c, err := OpenCatalog(filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.CreateBucket(t.Context(), "audit-bucket"); err != nil {
		t.Fatal(err)
	}
	keysMap := map[string]bool{"a": true, "a/": true, "a/b": true, "a/b/": true, "a/b/c": true, "a::b": true, "a::b::c": true, "世/a": true, "世世z": true, "z": true}
	random := rand.New(rand.NewSource(42))
	alphabet := []string{"a", "b", "c", "/", "::", "世", "<", "\x01"}
	for i := 0; i < 150; i++ {
		var b strings.Builder
		for j := 0; j < 1+random.Intn(6); j++ {
			b.WriteString(alphabet[random.Intn(len(alphabet))])
		}
		keysMap[b.String()] = true
	}
	keys := make([]string, 0, len(keysMap))
	for k := range keysMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for i, key := range keys {
		_, err := c.PublishObject(t.Context(), ports.CatalogObject{Info: ports.ObjectInfo{Bucket: "audit-bucket", Key: key, Size: int64(i), LastModified: time.Now()}, PayloadRef: "audit"})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, prefix := range []string{"", "a", "a/", "世", "missing"} {
		for _, delim := range []string{"", "/", "::", "世"} {
			for _, after := range []string{"", "a/", "a/b", "世a"} {
				for _, limit := range []int{1, 2, 7, 1000} {
					set := map[expectedListEntry]bool{}
					for _, key := range keys {
						if !strings.HasPrefix(key, prefix) {
							continue
						}
						e := expectedListEntry{Key: key}
						if delim != "" {
							if i := strings.Index(key[len(prefix):], delim); i >= 0 {
								e = expectedListEntry{Key: key[:len(prefix)+i+len(delim)], Group: true}
							}
						}
						if e.Key > after {
							set[e] = true
						}
					}
					want := make([]expectedListEntry, 0, len(set))
					for e := range set {
						want = append(want, e)
					}
					sort.Slice(want, func(i, j int) bool { return want[i].Key < want[j].Key })
					query := ports.ObjectListQuery{Bucket: "audit-bucket", Prefix: prefix, Delimiter: delim, StartAfter: after, Limit: limit}
					got := make([]expectedListEntry, 0)
					for pageNo := 0; ; pageNo++ {
						if pageNo > len(keys)+1 {
							t.Fatal("no progress")
						}
						page, err := c.ListObjects(t.Context(), query)
						if err != nil {
							t.Fatal(err)
						}
						if len(page.Entries) > limit {
							t.Fatal("limit violated")
						}
						for _, e := range page.Entries {
							if e.Object != nil {
								got = append(got, expectedListEntry{Key: e.Object.Key})
							} else {
								got = append(got, expectedListEntry{Key: e.CommonPrefix, Group: true})
							}
						}
						if !page.HasMore {
							break
						}
						if page.NextCursor == nil {
							t.Fatal("missing cursor")
						}
						query.Cursor = page.NextCursor
					}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("prefix=%q delim=%q after=%q limit=%d\ngot=%v\nwant=%v", prefix, delim, after, limit, got, want)
					}
				}
			}
		}
	}
}

func TestCancelledListingDoesNotPoisonPool(t *testing.T) {
	c, err := OpenCatalog(filepath.Join(t.TempDir(), "catalog.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.CreateBucket(t.Context(), "audit-bucket")
	for i := 0; i < 100; i++ {
		_, err = c.PublishObject(t.Context(), ports.CatalogObject{Info: ports.ObjectInfo{Bucket: "audit-bucket", Key: fmt.Sprint(i), LastModified: time.Now()}, PayloadRef: "audit"})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			defer close(done)
			c.ListObjects(ctx, ports.ObjectListQuery{Bucket: "audit-bucket", Limit: 10})
		}()
		cancel()
		<-done
		if _, err := c.ListObjects(t.Context(), ports.ObjectListQuery{Bucket: "audit-bucket", Limit: 10}); err != nil {
			t.Fatal(err)
		}
	}
}
