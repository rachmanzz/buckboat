package s3

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rachmanzz/buckboat/internal/storage/localfs"
)

func TestSignedBucketAndObjectLifecycle(t *testing.T) {
	store, err := localfs.Open(localfs.Config{
		RootDir:            t.TempDir(),
		MaxObjectBytes:     1 << 20,
		MaxConcurrentPuts:  2,
		MaxConcurrentReads: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const accessKey = "BUCKBOATTEST"
	const secretKey = "test-secret-key"
	handler := NewRouter(store, AuthConfig{AccessKeyID: accessKey, SecretAccessKey: secretKey, Region: "us-east-1"})

	create := signedRequest(t, http.MethodPut, "/docs-bucket", nil, nil, accessKey, secretKey)
	response := serve(handler, create)
	if response.Code != http.StatusOK {
		t.Fatalf("create bucket status=%d body=%s", response.Code, response.Body.String())
	}

	payload := []byte("0123456789")
	putHeaders := http.Header{
		"Content-Type":    []string{"text/plain"},
		"X-Amz-Meta-Note": []string{"saved"},
	}
	put := signedRequest(t, http.MethodPut, "/docs-bucket/folder/a%2Fb.txt", payload, putHeaders, accessKey, secretKey)
	response = serve(handler, put)
	if response.Code != http.StatusOK || response.Header().Get("ETag") == "" {
		t.Fatalf("put object status=%d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}

	getHeaders := http.Header{"Range": []string{"bytes=2-5"}}
	get := signedRequest(t, http.MethodGet, "/docs-bucket/folder/a%2Fb.txt", nil, getHeaders, accessKey, secretKey)
	response = serve(handler, get)
	if response.Code != http.StatusPartialContent || response.Body.String() != "2345" {
		t.Fatalf("range GET status=%d body=%q headers=%v", response.Code, response.Body.String(), response.Header())
	}
	if response.Header().Get("x-amz-meta-note") != "saved" {
		t.Fatalf("metadata response header = %q", response.Header().Get("x-amz-meta-note"))
	}

	head := signedRequest(t, http.MethodHead, "/docs-bucket/folder/a%2Fb.txt", nil, nil, accessKey, secretKey)
	response = serve(handler, head)
	if response.Code != http.StatusOK || response.Header().Get("Content-Length") != "10" {
		t.Fatalf("HEAD status=%d headers=%v", response.Code, response.Header())
	}

	delete := signedRequest(t, http.MethodDelete, "/docs-bucket/folder/a%2Fb.txt", nil, nil, accessKey, secretKey)
	response = serve(handler, delete)
	if response.Code != http.StatusNoContent {
		t.Fatalf("delete object status=%d body=%s", response.Code, response.Body.String())
	}
	deleteBucket := signedRequest(t, http.MethodDelete, "/docs-bucket", nil, nil, accessKey, secretKey)
	response = serve(handler, deleteBucket)
	if response.Code != http.StatusNoContent {
		t.Fatalf("delete bucket status=%d body=%s", response.Code, response.Body.String())
	}
}

func TestInvalidSigV4DoesNotReachStorage(t *testing.T) {
	store, err := localfs.Open(localfs.Config{RootDir: t.TempDir(), MaxObjectBytes: 1 << 20, MaxConcurrentPuts: 1, MaxConcurrentReads: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	handler := NewRouter(store, AuthConfig{AccessKeyID: "BUCKBOATTEST", SecretAccessKey: "test-secret-key", Region: "us-east-1"})
	req := signedRequest(t, http.MethodPut, "/docs-bucket", nil, nil, "BUCKBOATTEST", "test-secret-key")
	req.Header.Set("X-Amz-Meta-Admin", "true")
	response := serve(handler, req)
	if response.Code != http.StatusForbidden {
		t.Fatalf("unsigned x-amz header status=%d body=%s", response.Code, response.Body.String())
	}
	if err := store.HeadBucket(req.Context(), "docs-bucket"); err == nil {
		t.Fatal("bucket was created for a request rejected by SigV4")
	}
}

func signedRequest(t *testing.T, method, target string, body []byte, headers http.Header, accessKey, secretKey string) *http.Request {
	t.Helper()
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, target, reader)
	req.Host = "localhost:9400"
	for name, values := range headers {
		for _, value := range values {
			req.Header.Add(name, value)
		}
	}
	now := time.Now().UTC()
	date := now.Format("20060102T150405Z")
	req.Header.Set("X-Amz-Date", date)
	bodyHash := sha256.Sum256(body)
	payloadHash := hex.EncodeToString(bodyHash[:])
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	names := []string{"host"}
	for name := range req.Header {
		lower := strings.ToLower(name)
		if lower != "authorization" {
			names = append(names, lower)
		}
	}
	sort.Strings(names)
	canonicalHeaders := strings.Builder{}
	for _, name := range names {
		value := req.Host
		if name != "host" {
			values := req.Header.Values(name)
			for i := range values {
				values[i] = strings.Join(strings.Fields(values[i]), " ")
			}
			value = strings.Join(values, ",")
		}
		fmt.Fprintf(&canonicalHeaders, "%s:%s\n", name, value)
	}
	canonical := strings.Join([]string{
		method,
		canonicalURIForTest(target),
		canonicalQueryForTest(t, req.URL.RawQuery),
		canonicalHeaders.String(),
		strings.Join(names, ";"),
		payloadHash,
	}, "\n")
	canonicalSum := sha256.Sum256([]byte(canonical))
	scope := now.Format("20060102") + "/us-east-1/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + date + "\n" + scope + "\n" + hex.EncodeToString(canonicalSum[:])
	key := hmacSHA256ForTest([]byte("AWS4"+secretKey), []byte(now.Format("20060102")))
	key = hmacSHA256ForTest(key, []byte("us-east-1"))
	key = hmacSHA256ForTest(key, []byte("s3"))
	key = hmacSHA256ForTest(key, []byte("aws4_request"))
	signature := hex.EncodeToString(hmacSHA256ForTest(key, []byte(stringToSign)))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+accessKey+"/"+scope+", SignedHeaders="+strings.Join(names, ";")+", Signature="+signature)
	return req
}

func canonicalQueryForTest(t *testing.T, raw string) string {
	t.Helper()
	values, err := url.ParseQuery(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, items := range values {
		sort.Strings(items)
	}
	return strings.ReplaceAll(values.Encode(), "+", "%20")
}

func canonicalURIForTest(target string) string {
	path, _, _ := strings.Cut(target, "?")
	return strings.ReplaceAll(path, "%2f", "%2F")
}

func hmacSHA256ForTest(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(data)
	return mac.Sum(nil)
}

func serve(handler http.Handler, req *http.Request) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	return response
}
