package s3

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/rachmanzz/buckboat/internal/storage/catalogstore"
	"github.com/rachmanzz/buckboat/internal/storage/localfs"
	"github.com/rachmanzz/buckboat/internal/storage/ports"
	"github.com/rachmanzz/buckboat/internal/storage/sqlite"
)

const testAccessKey = "BUCKBOATTEST"
const testSecretKey = "test-secret-key"

func sqliteRouter(t *testing.T) (http.Handler, *catalogstore.Store, *sqlite.Catalog, string) {
	t.Helper()
	root := t.TempDir()
	payloads, err := localfs.Open(localfs.Config{RootDir: root, MaxObjectBytes: 4096, MaxConcurrentPuts: 2, MaxConcurrentReads: 1})
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := sqlite.OpenCatalog(filepath.Join(root, "catalog", "buckboat.sqlite"))
	if err != nil {
		payloads.Close()
		t.Fatal(err)
	}
	store := catalogstore.New(catalog, payloads)
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return NewRouter(store, AuthConfig{AccessKeyID: testAccessKey, SecretAccessKey: testSecretKey, Region: "us-east-1"}), store, catalog, root
}

func TestSDKOperationHintsAndSignedQueryArePreserved(t *testing.T) {
	router, _, _, _ := sqliteRouter(t)
	for _, step := range []struct {
		method, target, body string
		status               int
	}{
		{"PUT", "/test-bucket?x-id=CreateBucket", "", 200},
		{"PUT", "/test-bucket/key?x-id=PutObject", "hello", 200},
		{"GET", "/test-bucket/key?x-id=GetObject", "", 200},
		{"HEAD", "/test-bucket/key?x-id=HeadObject", "", 200},
		{"GET", "/test-bucket?list-type=2&x-id=ListObjectsV2", "", 200},
		{"GET", "/test-bucket/key?x-id=PutObject", "", 400},
		{"GET", "/test-bucket/key?x-id=GetObject&x-id=GetObject", "", 400},
		{"GET", "/test-bucket/key?x-id=GetObject&versionId=unsupported", "", 501},
	} {
		r := signedRequest(t, step.method, step.target, []byte(step.body), nil, testAccessKey, testSecretKey)
		response := serve(router, r)
		if response.Code != step.status {
			t.Fatalf("%s %s: status=%d body=%s", step.method, step.target, response.Code, response.Body.String())
		}
	}
	r := signedRequest(t, "GET", "/test-bucket/key?x-id=GetObject", nil, nil, testAccessKey, testSecretKey)
	r.RequestURI = "/test-bucket/key?x-id=HeadObject"
	r.URL.RawQuery = "x-id=HeadObject"
	if response := serve(router, r); response.Code != 403 {
		t.Fatalf("tampered hint bypassed signature verification: %d", response.Code)
	}
	for _, step := range []struct {
		target string
		status int
	}{
		{"/test-bucket/key?x-id=DeleteObject", 204},
		{"/test-bucket?x-id=DeleteBucket", 204},
	} {
		if response := serve(router, signedRequest(t, "DELETE", step.target, nil, nil, testAccessKey, testSecretKey)); response.Code != step.status {
			t.Fatalf("delete=%d body=%s", response.Code, response.Body.String())
		}
	}
}

func TestContinuationTokensRoundTripAtEscapedKeyLimits(t *testing.T) {
	h := &handler{auth: AuthConfig{SecretAccessKey: testSecretKey}}
	for _, value := range []string{strings.Repeat("a", 1024), strings.Repeat("<", 1024), strings.Repeat("\x01", 1024)} {
		for _, zeroPage := range []bool{false, true} {
			params := listV2Parameters{bucket: "test-bucket", maxKeys: 1, tokenStartAfter: value}
			cursor := ports.ObjectListCursor{Key: value}
			if zeroPage {
				cursor = ports.ObjectListCursor{StartAfter: value}
			}
			token, err := h.encodeListV2Token(params, cursor)
			if err != nil {
				t.Fatal(err)
			}
			got, startAfter, err := h.decodeListV2Token(token, params)
			if err != nil || got != cursor || startAfter != value {
				t.Fatalf("token bytes=%d cursor=%#v err=%v", len(token), got, err)
			}
			changed := params
			changed.prefix = "different"
			if _, _, err := h.decodeListV2Token(token, changed); err == nil {
				t.Fatal("token accepted a different prefix")
			}
		}
	}
	if _, _, err := h.decodeListV2Token(strings.Repeat("a", maxListV2TokenBytes+1), listV2Parameters{}); err == nil {
		t.Fatal("oversized token accepted")
	}
	params := listV2Parameters{bucket: "test-bucket", prefix: "p/", delimiter: "/"}
	cursor := ports.ObjectListCursor{Key: "p/a/", IsCommonPrefix: true}
	token, err := h.encodeListV2Token(params, cursor)
	if err != nil {
		t.Fatal(err)
	}
	if got, _, err := h.decodeListV2Token(token, params); err != nil || got != cursor {
		t.Fatalf("prefix cursor=%#v err=%v", got, err)
	}
	if _, _, err := h.decodeListV2Token("!"+token[1:], params); err == nil {
		t.Fatal("modified token accepted")
	}
}

func TestEscapedMaximumKeyPaginationOverHTTP(t *testing.T) {
	router, store, _, _ := sqliteRouter(t)
	if err := store.CreateBucket(t.Context(), "test-bucket"); err != nil {
		t.Fatal(err)
	}
	key := strings.Repeat("<", 1024)
	for _, value := range []string{key, "z"} {
		if _, err := store.PutObject(t.Context(), ports.PutObjectInput{Bucket: "test-bucket", Key: value, Body: strings.NewReader(""), ContentLength: 0}); err != nil {
			t.Fatal(err)
		}
	}
	response := serve(router, signedRequest(t, "GET", "/test-bucket?list-type=2&max-keys=1", nil, nil, testAccessKey, testSecretKey))
	var first listV2Response
	if err := xml.Unmarshal(response.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if response.Code != 200 || len(first.Contents) != 1 || first.Contents[0].Key != key || !first.IsTruncated {
		t.Fatalf("first page=%#v status=%d", first, response.Code)
	}
	response = serve(router, signedRequest(t, "GET", "/test-bucket?list-type=2&max-keys=1&continuation-token="+url.QueryEscape(first.NextContinuationToken), nil, nil, testAccessKey, testSecretKey))
	var second listV2Response
	if err := xml.Unmarshal(response.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if response.Code != 200 || len(second.Contents) != 1 || second.Contents[0].Key != "z" || second.IsTruncated {
		t.Fatalf("second page=%#v status=%d", second, response.Code)
	}
}

func TestXMLUnsafeListingValuesRequireURLEncoding(t *testing.T) {
	h := &handler{auth: AuthConfig{SecretAccessKey: testSecretKey}}
	for _, value := range []string{"hello\x01.txt", "bad\ufffe", "bad\uffff"} {
		page := ports.ObjectListPage{Entries: []ports.ObjectListEntry{{Object: &ports.ListedObject{Key: value}}, {CommonPrefix: value}}}
		for _, encoding := range []string{"", "url"} {
			params := listV2Parameters{bucket: "test-bucket", prefix: value, delimiter: value, startAfter: value, startAfterPresent: true, maxKeys: 2, encodingType: encoding}
			rr := httptest.NewRecorder()
			h.writeListV2Response(rr, page, params, "test", "/test-bucket")
			if encoding == "" {
				if rr.Code != 400 || strings.Contains(rr.Body.String(), "�") {
					t.Fatalf("unsafe listing=%d %s", rr.Code, rr.Body.String())
				}
				continue
			}
			var result listV2Response
			if err := xml.Unmarshal(rr.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if rr.Code != 200 || len(result.Contents) != 1 || len(result.CommonPrefixes) != 1 {
				t.Fatalf("encoded response=%d %s", rr.Code, rr.Body.String())
			}
			for _, encoded := range []string{result.Prefix, result.Delimiter, result.StartAfter, result.Contents[0].Key, result.CommonPrefixes[0].Prefix} {
				decoded, err := url.QueryUnescape(encoded)
				if err != nil || decoded != value {
					t.Fatalf("round trip=%q err=%v", decoded, err)
				}
			}
		}
	}
}

func TestCorruptedPayloadReturnsHTTPError(t *testing.T) {
	router, store, catalog, root := sqliteRouter(t)
	if err := store.CreateBucket(t.Context(), "test-bucket"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutObject(t.Context(), ports.PutObjectInput{Bucket: "test-bucket", Key: "key", Body: strings.NewReader("abcdef"), ContentLength: 6}); err != nil {
		t.Fatal(err)
	}
	object, err := catalog.GetObject(t.Context(), "test-bucket", "key")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(filepath.Join(root, "payloads", object.PayloadRef+".bin"), 3); err != nil {
		t.Fatal(err)
	}
	response := serve(router, signedRequest(t, "GET", "/test-bucket/key", nil, nil, testAccessKey, testSecretKey))
	if response.Code != 500 {
		t.Fatalf("corrupted object returned status=%d body=%q", response.Code, response.Body.String())
	}
}
