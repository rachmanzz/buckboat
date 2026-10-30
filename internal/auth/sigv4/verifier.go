// Package sigv4 validates the header-authentication form of AWS Signature
// Version 4 for the first path-style S3 operations.
package sigv4

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	algorithm       = "AWS4-HMAC-SHA256"
	service         = "s3"
	requestTerm     = "aws4_request"
	unsignedPayload = "UNSIGNED-PAYLOAD"
	maxClockSkew    = 15 * time.Minute
)

var (
	ErrMissingAuth       = errors.New("missing AWS Signature Version 4 authorization")
	ErrInvalidAuth       = errors.New("invalid AWS Signature Version 4 authorization")
	ErrInvalidAccessKey  = errors.New("unknown access key")
	ErrSignatureMismatch = errors.New("signature does not match")
	ErrExpiredRequest    = errors.New("request timestamp is outside the accepted clock window")
	ErrUnsupportedAuth   = errors.New("signature form is not supported by this implementation")
)

type authFields struct {
	accessKey string
	date      string
	region    string
	service   string
	term      string
	signed    []string
	signature string
}

func Verify(r *http.Request, accessKeyID, secretAccessKey, region string, now time.Time) error {
	if accessKeyID == "" || secretAccessKey == "" || region == "" {
		return fmt.Errorf("SigV4 credential configuration is incomplete")
	}
	if r.Header.Get("Authorization") == "" {
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err == nil && (query.Has("X-Amz-Algorithm") || query.Has("X-Amz-Signature")) {
			return ErrUnsupportedAuth
		}
	}
	auth, err := parseAuthorization(r.Header.Get("Authorization"))
	if err != nil {
		return err
	}
	if auth.accessKey != accessKeyID {
		return ErrInvalidAccessKey
	}
	if auth.region != region || auth.service != service || auth.term != requestTerm {
		return ErrInvalidAuth
	}
	if auth.date == "" || len(auth.date) != 8 {
		return ErrInvalidAuth
	}
	requestTime, err := requestTimestamp(r)
	if err != nil {
		return err
	}
	if requestTime.Format("20060102") != auth.date {
		return ErrInvalidAuth
	}
	if delta := now.UTC().Sub(requestTime); delta > maxClockSkew || delta < -maxClockSkew {
		return ErrExpiredRequest
	}
	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		return ErrInvalidAuth
	}
	if payloadHash != unsignedPayload {
		if strings.HasPrefix(payloadHash, "STREAMING-") {
			return ErrUnsupportedAuth
		}
		decoded, err := hex.DecodeString(payloadHash)
		if err != nil || len(decoded) != sha256.Size {
			return ErrInvalidAuth
		}
	}
	if err := validateSignedHeaders(r, auth.signed); err != nil {
		return err
	}

	canonical, err := canonicalRequest(r, auth.signed, payloadHash)
	if err != nil {
		return err
	}
	canonicalHash := sha256.Sum256([]byte(canonical))
	timestamp := requestTime.UTC().Format("20060102T150405Z")
	scope := strings.Join([]string{auth.date, auth.region, auth.service, auth.term}, "/")
	stringToSign := algorithm + "\n" + timestamp + "\n" + scope + "\n" + hex.EncodeToString(canonicalHash[:])
	signingKey := deriveSigningKey(secretAccessKey, auth.date, auth.region, auth.service)
	expected := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))
	if len(auth.signature) != sha256.Size*2 || subtle.ConstantTimeCompare([]byte(expected), []byte(strings.ToLower(auth.signature))) != 1 {
		return ErrSignatureMismatch
	}
	return nil
}

func parseAuthorization(value string) (authFields, error) {
	var result authFields
	prefix := algorithm + " "
	if value == "" {
		return result, ErrMissingAuth
	}
	if !strings.HasPrefix(value, prefix) {
		return result, ErrUnsupportedAuth
	}
	parts := strings.Split(strings.TrimPrefix(value, prefix), ",")
	fields := make(map[string]string, len(parts))
	for _, part := range parts {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || key == "" || value == "" {
			return result, ErrInvalidAuth
		}
		if _, exists := fields[key]; exists {
			return result, ErrInvalidAuth
		}
		fields[key] = value
	}
	if len(fields) != 3 {
		return result, ErrInvalidAuth
	}
	credential, ok := fields["Credential"]
	if !ok {
		return result, ErrInvalidAuth
	}
	credentialParts := strings.Split(credential, "/")
	if len(credentialParts) != 5 || credentialParts[0] == "" {
		return result, ErrInvalidAuth
	}
	if fields["SignedHeaders"] == "" || fields["Signature"] == "" {
		return result, ErrInvalidAuth
	}
	signed := strings.Split(fields["SignedHeaders"], ";")
	for i, name := range signed {
		if name == "" || name != strings.ToLower(name) || (i > 0 && signed[i-1] >= name) {
			return result, ErrInvalidAuth
		}
	}
	signature, err := hex.DecodeString(fields["Signature"])
	if err != nil || len(signature) != sha256.Size {
		return result, ErrInvalidAuth
	}
	result = authFields{
		accessKey: credentialParts[0],
		date:      credentialParts[1],
		region:    credentialParts[2],
		service:   credentialParts[3],
		term:      credentialParts[4],
		signed:    signed,
		signature: fields["Signature"],
	}
	return result, nil
}

func requestTimestamp(r *http.Request) (time.Time, error) {
	value := r.Header.Get("X-Amz-Date")
	if value == "" {
		return time.Time{}, ErrInvalidAuth
	}
	parsed, err := time.Parse("20060102T150405Z", value)
	if err != nil {
		return time.Time{}, ErrInvalidAuth
	}
	return parsed.UTC(), nil
}

func validateSignedHeaders(r *http.Request, signed []string) error {
	set := make(map[string]struct{}, len(signed))
	for _, name := range signed {
		set[name] = struct{}{}
		if name == "host" {
			if requestHost(r) == "" {
				return ErrInvalidAuth
			}
			continue
		}
		if len(r.Header.Values(name)) == 0 {
			return ErrInvalidAuth
		}
	}
	for _, required := range []string{"host", "x-amz-date"} {
		if _, ok := set[required]; !ok {
			return ErrInvalidAuth
		}
	}
	for _, name := range []string{
		"content-md5", "content-type", "content-disposition", "content-encoding",
		"content-language", "cache-control", "expires", "range", "if-match",
		"if-none-match", "if-modified-since", "if-unmodified-since", "if-range",
	} {
		if len(r.Header.Values(name)) == 0 {
			continue
		}
		if _, ok := set[name]; !ok {
			return ErrInvalidAuth
		}
	}
	for name := range r.Header {
		lower := strings.ToLower(name)
		if strings.HasPrefix(lower, "x-amz-") && lower != "x-amz-content-sha256" {
			if _, ok := set[lower]; !ok {
				return ErrInvalidAuth
			}
		}
	}
	return nil
}

func canonicalRequest(r *http.Request, signed []string, payloadHash string) (string, error) {
	path, query, err := requestTarget(r)
	if err != nil {
		return "", err
	}
	canonicalQuery, err := canonicalQueryString(query)
	if err != nil {
		return "", err
	}
	var headers strings.Builder
	for _, name := range signed {
		value := requestHost(r)
		if name != "host" {
			values := r.Header.Values(name)
			normalized := make([]string, len(values))
			for i, headerValue := range values {
				normalized[i] = normalizeHeaderValue(headerValue)
			}
			value = strings.Join(normalized, ",")
		}
		headers.WriteString(name)
		headers.WriteByte(':')
		headers.WriteString(value)
		headers.WriteByte('\n')
	}
	return strings.Join([]string{
		r.Method,
		canonicalURI(path),
		canonicalQuery,
		headers.String(),
		strings.Join(signed, ";"),
		payloadHash,
	}, "\n"), nil
}

func requestTarget(r *http.Request) (string, string, error) {
	target := r.RequestURI
	if target == "" {
		target = r.URL.EscapedPath()
		if target == "" {
			target = "/"
		}
		if r.URL.RawQuery != "" {
			target += "?" + r.URL.RawQuery
		}
	}
	if !strings.HasPrefix(target, "/") {
		return "", "", ErrInvalidAuth
	}
	path, query, _ := strings.Cut(target, "?")
	return path, query, nil
}

func canonicalURI(rawPath string) string {
	if rawPath == "" {
		return "/"
	}
	const hexDigits = "0123456789ABCDEF"
	var result strings.Builder
	for i := 0; i < len(rawPath); i++ {
		b := rawPath[i]
		if b == '%' && i+2 < len(rawPath) {
			hi, okHi := fromHex(rawPath[i+1])
			lo, okLo := fromHex(rawPath[i+2])
			if okHi && okLo {
				decoded := hi<<4 | lo
				if isUnreserved(decoded) {
					result.WriteByte(decoded)
				} else {
					result.WriteByte('%')
					result.WriteByte(hexDigits[decoded>>4])
					result.WriteByte(hexDigits[decoded&0x0f])
				}
				i += 2
				continue
			}
		}
		if isUnreserved(b) || b == '/' {
			result.WriteByte(b)
		} else {
			result.WriteByte('%')
			result.WriteByte(hexDigits[b>>4])
			result.WriteByte(hexDigits[b&0x0f])
		}
	}
	return result.String()
}

func canonicalQueryString(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	values, err := url.ParseQuery(raw)
	if err != nil {
		return "", ErrInvalidAuth
	}
	type pair struct{ key, value string }
	pairs := make([]pair, 0, len(values))
	for key, entries := range values {
		if len(entries) == 0 {
			entries = []string{""}
		}
		for _, value := range entries {
			pairs = append(pairs, pair{key: uriEncode(key), value: uriEncode(value)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key == pairs[j].key {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].key < pairs[j].key
	})
	encoded := make([]string, len(pairs))
	for i, pair := range pairs {
		encoded[i] = pair.key + "=" + pair.value
	}
	return strings.Join(encoded, "&"), nil
}

func uriEncode(value string) string {
	const hexDigits = "0123456789ABCDEF"
	var result strings.Builder
	for i := 0; i < len(value); i++ {
		b := value[i]
		if isUnreserved(b) {
			result.WriteByte(b)
		} else {
			result.WriteByte('%')
			result.WriteByte(hexDigits[b>>4])
			result.WriteByte(hexDigits[b&0x0f])
		}
	}
	return result.String()
}

func normalizeHeaderValue(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func requestHost(r *http.Request) string {
	if r.Host != "" {
		return normalizeHeaderValue(r.Host)
	}
	return normalizeHeaderValue(r.Header.Get("Host"))
}

func deriveSigningKey(secret, date, region, serviceName string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(serviceName))
	return hmacSHA256(kService, []byte(requestTerm))
}

func hmacSHA256(key, value []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(value)
	return mac.Sum(nil)
}

func fromHex(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}

func isUnreserved(value byte) bool {
	return value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z' || value >= '0' && value <= '9' || value == '-' || value == '.' || value == '_' || value == '~'
}
