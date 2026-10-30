package s3

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/rachmanzz/buckboat/internal/storage/ports"
)

const listObjectsV2XMLNamespace = "http://s3.amazonaws.com/doc/2006-03-01/"

// A cursor and start-after are at most 1024 bytes each. JSON can expand each
// byte to six characters before base64; this bound covers that worst case.
const maxListV2TokenBytes = 32 << 10

var (
	errUnsupportedListQuery = errors.New("unsupported ListObjectsV2 query parameter")
	errInvalidListQuery     = errors.New("invalid ListObjectsV2 query parameter")
	errInvalidListToken     = errors.New("invalid ListObjectsV2 continuation token")
)

type listV2Parameters struct {
	bucket                   string
	prefix                   string
	delimiter                string
	startAfter               string
	startAfterPresent        bool
	maxKeys                  int
	encodingType             string
	continuationToken        string
	continuationTokenPresent bool
	tokenStartAfter          string
	cursor                   *ports.ObjectListCursor
}

type listV2Token struct {
	Version    int                    `json:"v"`
	ScopeHash  string                 `json:"h"`
	StartAfter string                 `json:"s,omitempty"`
	Cursor     ports.ObjectListCursor `json:"c"`
}

type listV2Response struct {
	XMLName               xml.Name             `xml:"ListBucketResult"`
	XMLNS                 string               `xml:"xmlns,attr"`
	Name                  string               `xml:"Name"`
	Prefix                string               `xml:"Prefix"`
	StartAfter            string               `xml:"StartAfter,omitempty"`
	Contents              []listV2Content      `xml:"Contents"`
	CommonPrefixes        []listV2CommonPrefix `xml:"CommonPrefixes"`
	ContinuationToken     string               `xml:"ContinuationToken,omitempty"`
	Delimiter             string               `xml:"Delimiter,omitempty"`
	EncodingType          string               `xml:"EncodingType,omitempty"`
	IsTruncated           bool                 `xml:"IsTruncated"`
	KeyCount              int                  `xml:"KeyCount"`
	MaxKeys               int                  `xml:"MaxKeys"`
	NextContinuationToken string               `xml:"NextContinuationToken,omitempty"`
}

type listV2Content struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type listV2CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

func (h *handler) serveBucketQuery(w http.ResponseWriter, r *http.Request, requestID string, target resource, rawQuery string) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusNotImplemented, "NotImplemented", "This bucket query operation is not implemented yet", requestID, requestPath(r))
		return
	}
	params, err := parseListV2Parameters(rawQuery, target.bucket)
	if err != nil {
		writeListQueryError(w, err, requestID, requestPath(r))
		return
	}
	lister, ok := h.store.(ports.ObjectLister)
	if !ok {
		writeError(w, http.StatusNotImplemented, "NotImplemented", "Object listing is not available for this storage adapter", requestID, requestPath(r))
		return
	}
	if params.continuationTokenPresent {
		cursor, startAfter, err := h.decodeListV2Token(params.continuationToken, params)
		if err != nil {
			writeListQueryError(w, err, requestID, requestPath(r))
			return
		}
		params.cursor = &cursor
		params.tokenStartAfter = startAfter
		if params.startAfterPresent && params.startAfter != startAfter {
			writeListQueryError(w, errInvalidListToken, requestID, requestPath(r))
			return
		}
	}
	page, err := lister.ListObjects(r.Context(), ports.ObjectListQuery{
		Bucket: target.bucket, Prefix: params.prefix, Delimiter: params.delimiter,
		StartAfter: params.startAfter, Cursor: params.cursor, Limit: params.maxKeys,
	})
	if err != nil {
		h.writeStorageError(w, err, requestID, requestPath(r))
		return
	}
	h.writeListV2Response(w, page, params, requestID, requestPath(r))
}

func parseListV2Parameters(rawQuery, bucket string) (listV2Parameters, error) {
	values, err := url.ParseQuery(rawQuery)
	if err != nil {
		return listV2Parameters{}, fmt.Errorf("%w: malformed query string", errInvalidListQuery)
	}
	allowed := map[string]struct{}{
		"list-type": {}, "continuation-token": {}, "delimiter": {}, "encoding-type": {},
		"fetch-owner": {}, "max-keys": {}, "prefix": {}, "start-after": {},
	}
	for name, value := range values {
		if _, ok := allowed[name]; !ok {
			return listV2Parameters{}, fmt.Errorf("%w: %s", errUnsupportedListQuery, name)
		}
		if len(value) != 1 {
			return listV2Parameters{}, fmt.Errorf("%w: duplicate %s", errInvalidListQuery, name)
		}
	}
	listType, ok := singleQueryValue(values, "list-type")
	if !ok || listType != "2" {
		return listV2Parameters{}, errUnsupportedListQuery
	}

	params := listV2Parameters{bucket: bucket, maxKeys: ports.MaxObjectListPageSize}
	params.prefix, _ = singleQueryValue(values, "prefix")
	params.delimiter, _ = singleQueryValue(values, "delimiter")
	params.startAfter, params.startAfterPresent = singleQueryValue(values, "start-after")
	params.continuationToken, params.continuationTokenPresent = singleQueryValue(values, "continuation-token")
	params.encodingType, _ = singleQueryValue(values, "encoding-type")
	if !utf8.ValidString(params.prefix) || !utf8.ValidString(params.delimiter) || !utf8.ValidString(params.startAfter) || len(params.startAfter) > 1024 {
		return listV2Parameters{}, errInvalidListQuery
	}
	if maxKeys, present := singleQueryValue(values, "max-keys"); present {
		parsed, err := strconv.Atoi(maxKeys)
		if err != nil || parsed < 0 || parsed > ports.MaxObjectListPageSize {
			return listV2Parameters{}, errInvalidListQuery
		}
		params.maxKeys = parsed
	}
	if _, present := singleQueryValue(values, "encoding-type"); present && params.encodingType != "url" {
		return listV2Parameters{}, errInvalidListQuery
	}
	if owner, present := singleQueryValue(values, "fetch-owner"); present {
		switch owner {
		case "false":
		case "true":
			return listV2Parameters{}, errUnsupportedListQuery
		default:
			return listV2Parameters{}, errInvalidListQuery
		}
	}
	if params.continuationTokenPresent {
		if params.continuationToken == "" {
			return listV2Parameters{}, errInvalidListToken
		}
		return params, nil
	}
	params.tokenStartAfter = params.startAfter
	return params, nil
}

func singleQueryValue(values url.Values, name string) (string, bool) {
	items, ok := values[name]
	if !ok {
		return "", false
	}
	return items[0], true
}

func (h *handler) writeListV2Response(w http.ResponseWriter, page ports.ObjectListPage, params listV2Parameters, requestID, resource string) {
	response := listV2Response{
		XMLNS: listObjectsV2XMLNamespace, Name: params.bucket,
		Prefix: params.prefix, MaxKeys: params.maxKeys,
		IsTruncated: page.HasMore, KeyCount: len(page.Entries),
	}
	if params.startAfterPresent {
		response.StartAfter = params.startAfter
	}
	if params.delimiter != "" {
		response.Delimiter = params.delimiter
	}
	if params.encodingType == "url" {
		response.EncodingType = "url"
		response.Prefix = encodeListResponseValue(response.Prefix)
		response.StartAfter = encodeListResponseValue(response.StartAfter)
		response.Delimiter = encodeListResponseValue(response.Delimiter)
	}
	if params.continuationTokenPresent {
		response.ContinuationToken = params.continuationToken
	}
	for _, entry := range page.Entries {
		if entry.CommonPrefix != "" {
			prefix := entry.CommonPrefix
			if params.encodingType == "url" {
				prefix = encodeListResponseValue(prefix)
			}
			response.CommonPrefixes = append(response.CommonPrefixes, listV2CommonPrefix{Prefix: prefix})
			continue
		}
		if entry.Object == nil {
			writeError(w, http.StatusInternalServerError, "InternalError", "The catalog returned an invalid listing entry", requestID, resource)
			return
		}
		key := entry.Object.Key
		if params.encodingType == "url" {
			key = encodeListResponseValue(key)
		}
		response.Contents = append(response.Contents, listV2Content{
			Key: key, LastModified: entry.Object.LastModified.UTC().Format(time.RFC3339Nano),
			ETag: entry.Object.ETag, Size: entry.Object.Size, StorageClass: "STANDARD",
		})
	}
	if page.HasMore {
		if page.NextCursor == nil {
			writeError(w, http.StatusInternalServerError, "InternalError", "The catalog omitted a listing cursor", requestID, resource)
			return
		}
		token, err := h.encodeListV2Token(params, *page.NextCursor)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "InternalError", "The listing cursor could not be encoded", requestID, resource)
			return
		}
		response.NextContinuationToken = token
	}
	if !listV2XMLSafe(response) {
		writeError(w, http.StatusBadRequest, "InvalidArgument", "A response value cannot be represented in XML; use encoding-type=url", requestID, resource)
		return
	}
	body, err := xml.Marshal(response)
	if err != nil {
		writeError(w, http.StatusBadRequest, "InvalidArgument", "A response value cannot be represented in XML; use encoding-type=url", requestID, resource)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(append([]byte(xml.Header), body...))
}

func encodeListResponseValue(value string) string {
	return strings.ReplaceAll(url.QueryEscape(value), "+", "%20")
}

func listV2XMLSafe(response listV2Response) bool {
	for _, value := range []string{response.Name, response.Prefix, response.Delimiter, response.StartAfter} {
		if !validXMLText(value) {
			return false
		}
	}
	for _, object := range response.Contents {
		if !validXMLText(object.Key) {
			return false
		}
	}
	for _, prefix := range response.CommonPrefixes {
		if !validXMLText(prefix.Prefix) {
			return false
		}
	}
	return true
}

func validXMLText(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, r := range value {
		if r != '\t' && r != '\n' && r != '\r' && !(r >= 0x20 && r <= 0xd7ff) && !(r >= 0xe000 && r <= 0xfffd) && !(r >= 0x10000 && r <= 0x10ffff) {
			return false
		}
	}
	return true
}

func (h *handler) encodeListV2Token(params listV2Parameters, cursor ports.ObjectListCursor) (string, error) {
	if !validListV2Cursor(cursor, params.prefix, params.delimiter, params.tokenStartAfter) {
		return "", errInvalidListToken
	}
	payload, err := json.Marshal(listV2Token{
		Version: 1, ScopeHash: listV2ScopeHash(params.bucket, params.prefix, params.delimiter, params.tokenStartAfter),
		StartAfter: params.tokenStartAfter, Cursor: cursor,
	})
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, []byte("buckboat:list-objects-v2\x00"+h.auth.SecretAccessKey))
	_, _ = mac.Write(payload)
	sealed := append(payload, mac.Sum(nil)...)
	token := base64.RawURLEncoding.EncodeToString(sealed)
	if len(token) > maxListV2TokenBytes {
		return "", errInvalidListToken
	}
	return token, nil
}

func (h *handler) decodeListV2Token(token string, params listV2Parameters) (ports.ObjectListCursor, string, error) {
	if len(token) > maxListV2TokenBytes {
		return ports.ObjectListCursor{}, "", errInvalidListToken
	}
	sealed, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(sealed) <= sha256.Size {
		return ports.ObjectListCursor{}, "", errInvalidListToken
	}
	payload, signature := sealed[:len(sealed)-sha256.Size], sealed[len(sealed)-sha256.Size:]
	mac := hmac.New(sha256.New, []byte("buckboat:list-objects-v2\x00"+h.auth.SecretAccessKey))
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return ports.ObjectListCursor{}, "", errInvalidListToken
	}
	var decoded listV2Token
	if err := json.Unmarshal(payload, &decoded); err != nil {
		return ports.ObjectListCursor{}, "", errInvalidListToken
	}
	if decoded.Version != 1 || decoded.ScopeHash != listV2ScopeHash(params.bucket, params.prefix, params.delimiter, decoded.StartAfter) {
		return ports.ObjectListCursor{}, "", errInvalidListToken
	}
	if params.startAfterPresent && params.startAfter != decoded.StartAfter {
		return ports.ObjectListCursor{}, "", errInvalidListToken
	}
	if !validListV2Cursor(decoded.Cursor, params.prefix, params.delimiter, decoded.StartAfter) {
		return ports.ObjectListCursor{}, "", errInvalidListToken
	}
	return decoded.Cursor, decoded.StartAfter, nil
}

func validListV2Cursor(cursor ports.ObjectListCursor, prefix, delimiter, startAfter string) bool {
	for _, value := range []string{cursor.Key, cursor.StartAfter, startAfter} {
		if len(value) > 1024 || !utf8.ValidString(value) {
			return false
		}
	}
	if cursor.Key == "" {
		return !cursor.IsCommonPrefix && cursor.StartAfter == startAfter
	}
	if cursor.StartAfter != "" || !strings.HasPrefix(cursor.Key, prefix) {
		return false
	}
	return !cursor.IsCommonPrefix || delimiter != "" && strings.HasSuffix(cursor.Key, delimiter)
}

func listV2ScopeHash(bucket, prefix, delimiter, startAfter string) string {
	scope, _ := json.Marshal(struct {
		Bucket     string `json:"b"`
		Prefix     string `json:"p"`
		Delimiter  string `json:"d"`
		StartAfter string `json:"s"`
	}{Bucket: bucket, Prefix: prefix, Delimiter: delimiter, StartAfter: startAfter})
	sum := sha256.Sum256(scope)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func writeListQueryError(w http.ResponseWriter, err error, requestID, resource string) {
	switch {
	case errors.Is(err, errUnsupportedListQuery):
		writeError(w, http.StatusNotImplemented, "NotImplemented", "This ListObjectsV2 parameter is not implemented yet", requestID, resource)
	case errors.Is(err, errInvalidListToken):
		writeError(w, http.StatusBadRequest, "InvalidArgument", "The continuation token is invalid for this request", requestID, resource)
	default:
		writeError(w, http.StatusBadRequest, "InvalidArgument", "The ListObjectsV2 query parameters are invalid", requestID, resource)
	}
}
