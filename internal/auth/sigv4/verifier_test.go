package sigv4

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestVerifyAWSExampleRequest(t *testing.T) {
	req := httptest.NewRequest("GET", "/test.txt", nil)
	req.Host = "examplebucket.s3.amazonaws.com"
	req.Header.Set("Range", "bytes=0-9")
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	req.Header.Set("X-Amz-Date", "20130524T000000Z")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
	if err := Verify(req, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1", time.Date(2013, 5, 24, 0, 1, 0, 0, time.UTC)); err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
}

func TestVerifyRejectsTamperedAndUnsignedAmzHeader(t *testing.T) {
	req := httptest.NewRequest("GET", "/test.txt", nil)
	req.Host = "examplebucket.s3.amazonaws.com"
	req.Header.Set("Range", "bytes=0-11")
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	req.Header.Set("X-Amz-Date", "20130524T000000Z")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
	err := Verify(req, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1", time.Date(2013, 5, 24, 0, 1, 0, 0, time.UTC))
	if err != ErrSignatureMismatch {
		t.Fatalf("tampered request error = %v, want ErrSignatureMismatch", err)
	}

	unsignedHeader := httptest.NewRequest("GET", "/test.txt", nil)
	unsignedHeader.Host = "examplebucket.s3.amazonaws.com"
	unsignedHeader.Header.Set("Range", "bytes=0-9")
	unsignedHeader.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	unsignedHeader.Header.Set("X-Amz-Date", "20130524T000000Z")
	unsignedHeader.Header.Set("X-Amz-Meta-Admin", "true")
	unsignedHeader.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
	err = Verify(unsignedHeader, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1", time.Date(2013, 5, 24, 0, 1, 0, 0, time.UTC))
	if err != ErrInvalidAuth {
		t.Fatalf("unsigned x-amz header error = %v, want ErrInvalidAuth", err)
	}
}

func TestCanonicalURIAndQueryPreserveS3PathShape(t *testing.T) {
	if got, want := canonicalURI("/bucket/a//b%2fc%20d"), "/bucket/a//b%2Fc%20d"; got != want {
		t.Fatalf("canonicalURI() = %q, want %q", got, want)
	}
	got, err := canonicalQueryString("x=2&prefix=a+b&x=1")
	if err != nil {
		t.Fatalf("canonicalQueryString() error = %v", err)
	}
	if want := "prefix=a%20b&x=1&x=2"; got != want {
		t.Fatalf("canonicalQueryString() = %q, want %q", got, want)
	}
}

func TestCanonicalRequestDoesNotMutateHeaders(t *testing.T) {
	req := httptest.NewRequest("GET", "/bucket/key", nil)
	req.Host = "examplebucket.s3.amazonaws.com"
	req.Header.Set("X-Test", "  first\tvalue  ")
	before := req.Header.Get("X-Test")

	canonical, err := canonicalRequest(req, []string{"host", "x-test"}, unsignedPayload)
	if err != nil {
		t.Fatalf("canonicalRequest() error = %v", err)
	}
	if !strings.Contains(canonical, "x-test:first value\n") {
		t.Fatalf("canonical request did not normalize header whitespace: %q", canonical)
	}
	if got := req.Header.Get("X-Test"); got != before {
		t.Fatalf("canonicalRequest() mutated request header: got %q, want %q", got, before)
	}
}

func TestVerifyReportsPresignedFormAsUnsupported(t *testing.T) {
	req := httptest.NewRequest("GET", "/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256", nil)
	err := Verify(req, "access", "secret", "us-east-1", time.Now().UTC())
	if err != ErrUnsupportedAuth {
		t.Fatalf("Verify() error = %v, want ErrUnsupportedAuth", err)
	}
}

func TestVerifyRejectsUnsignedIfRangeHeader(t *testing.T) {
	req := httptest.NewRequest("GET", "/test.txt", nil)
	req.Host = "examplebucket.s3.amazonaws.com"
	req.Header.Set("Range", "bytes=0-9")
	req.Header.Set("If-Range", `"etag"`)
	req.Header.Set("X-Amz-Content-Sha256", "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
	req.Header.Set("X-Amz-Date", "20130524T000000Z")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41")
	err := Verify(req, "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "us-east-1", time.Date(2013, 5, 24, 0, 1, 0, 0, time.UTC))
	if err != ErrInvalidAuth {
		t.Fatalf("unsigned If-Range header error = %v, want ErrInvalidAuth", err)
	}
}

func TestParseAuthorizationRejectsUnknownFields(t *testing.T) {
	value := "AWS4-HMAC-SHA256 Credential=access/20261030/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=" + strings.Repeat("0", 64) + ", Extra=unexpected"
	if _, err := parseAuthorization(value); err != ErrInvalidAuth {
		t.Fatalf("parseAuthorization() error = %v, want ErrInvalidAuth", err)
	}
}
