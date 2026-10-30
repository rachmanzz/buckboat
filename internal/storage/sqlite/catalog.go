// Package sqlite implements Buckboat's Catalog contract with SQLite.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/rachmanzz/buckboat/internal/storage/fsdir"
	"github.com/rachmanzz/buckboat/internal/storage/ports"
)

const (
	applicationID = 0x4255434b
	schemaVersion = 2
)

type Catalog struct {
	db        *sql.DB
	writeMu   sync.Mutex
	closeOnce sync.Once
	closeErr  error
}

func OpenCatalog(path string) (*Catalog, error) {
	if path == "" {
		return nil, fmt.Errorf("SQLite catalog path is required")
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve SQLite catalog path: %w", err)
	}
	if err := fsdir.EnsurePrivate(filepath.Dir(absPath)); err != nil {
		return nil, fmt.Errorf("create SQLite catalog directory: %w", err)
	}

	db, err := sql.Open("sqlite3", dataSourceName(absPath))
	if err != nil {
		return nil, fmt.Errorf("open SQLite catalog: %w", err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	catalog := &Catalog{db: db}
	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("connect SQLite catalog: %w", catalogError(err))
	}
	if err := os.Chmod(absPath, 0o600); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("secure SQLite catalog file: %w", err)
	}
	if err := syncDir(filepath.Dir(absPath)); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sync SQLite catalog directory: %w", err)
	}
	if err := catalog.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return catalog, nil
}

func dataSourceName(path string) string {
	fileURL := (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String()
	query := url.Values{}
	query.Add("_journal_mode", "WAL")
	query.Add("_synchronous", "FULL")
	query.Add("_busy_timeout", "5000")
	query.Add("_foreign_keys", "on")
	query.Add("_txlock", "immediate")
	return fileURL + "?" + query.Encode()
}

func (c *Catalog) migrate(ctx context.Context) error {
	var appID, version int
	if err := c.db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&appID); err != nil {
		return fmt.Errorf("read SQLite application ID: %w", catalogError(err))
	}
	if err := c.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("read SQLite schema version: %w", catalogError(err))
	}
	if appID != 0 && appID != applicationID {
		return fmt.Errorf("SQLite file belongs to another application (application_id=%d)", appID)
	}
	if appID == applicationID {
		if version > schemaVersion {
			return fmt.Errorf("SQLite catalog schema version %d is newer than supported version %d", version, schemaVersion)
		}
		switch version {
		case schemaVersion:
			return nil
		case 1:
			return c.migrateV1ToV2(ctx)
		default:
			return fmt.Errorf("unsupported SQLite catalog schema version %d", version)
		}
	}
	if version != 0 {
		return fmt.Errorf("unrecognized SQLite catalog schema version %d", version)
	}

	var userTables int
	if err := c.db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&userTables); err != nil {
		return fmt.Errorf("inspect SQLite catalog schema: %w", catalogError(err))
	}
	if userTables != 0 {
		return fmt.Errorf("SQLite catalog file has tables but no Buckboat schema marker")
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite catalog migration: %w", catalogError(err))
	}
	defer func() { _ = tx.Rollback() }()
	statements := []string{
		`CREATE TABLE buckets (
			name TEXT NOT NULL PRIMARY KEY,
			created_at_ns INTEGER NOT NULL
		) WITHOUT ROWID`,
		`CREATE TABLE objects (
			bucket TEXT NOT NULL,
			object_key BLOB NOT NULL,
			size INTEGER NOT NULL CHECK (size >= 0),
			etag TEXT NOT NULL,
			content_type TEXT NOT NULL,
			cache_control TEXT NOT NULL,
			content_disposition TEXT NOT NULL,
			content_encoding TEXT NOT NULL,
			content_language TEXT NOT NULL,
			expires TEXT NOT NULL,
			user_metadata BLOB NOT NULL,
			last_modified_ns INTEGER NOT NULL,
			payload_ref TEXT NOT NULL,
			PRIMARY KEY (bucket, object_key),
			FOREIGN KEY (bucket) REFERENCES buckets(name) ON DELETE RESTRICT
		) WITHOUT ROWID`,
		`CREATE INDEX objects_payload_ref_idx ON objects(payload_ref)`,
		fmt.Sprintf("PRAGMA application_id = %d", applicationID),
		fmt.Sprintf("PRAGMA user_version = %d", schemaVersion),
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("apply SQLite catalog schema: %w", catalogError(err))
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite catalog migration: %w", catalogError(err))
	}
	return nil
}

func (c *Catalog) migrateV1ToV2(ctx context.Context) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite catalog migration to version 2: %w", catalogError(err))
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `CREATE INDEX objects_payload_ref_idx ON objects(payload_ref)`); err != nil {
		return fmt.Errorf("create SQLite payload reference index: %w", catalogError(err))
	}
	if _, err := tx.ExecContext(ctx, "PRAGMA user_version = 2"); err != nil {
		return fmt.Errorf("set SQLite catalog schema version 2: %w", catalogError(err))
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite catalog migration to version 2: %w", catalogError(err))
	}
	return nil
}

func (c *Catalog) CreateBucket(ctx context.Context, name string) error {
	if !ports.ValidBucketName(name) {
		return ports.ErrInvalidBucket
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	result, err := c.db.ExecContext(ctx, `INSERT INTO buckets(name, created_at_ns) VALUES(?, ?) ON CONFLICT(name) DO NOTHING`, name, time.Now().UTC().UnixNano())
	if err != nil {
		return fmt.Errorf("create SQLite bucket record: %w", catalogError(err))
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read SQLite bucket insert result: %w", err)
	}
	if count == 0 {
		return ports.ErrBucketAlreadyExists
	}
	return nil
}

func (c *Catalog) HeadBucket(ctx context.Context, name string) error {
	if !ports.ValidBucketName(name) {
		return ports.ErrInvalidBucket
	}
	var exists int
	if err := c.db.QueryRowContext(ctx, `SELECT 1 FROM buckets WHERE name = ?`, name).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return ports.ErrBucketNotFound
	} else if err != nil {
		return fmt.Errorf("read SQLite bucket record: %w", catalogError(err))
	}
	return nil
}

func (c *Catalog) DeleteBucket(ctx context.Context, name string) error {
	if !ports.ValidBucketName(name) {
		return ports.ErrInvalidBucket
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite bucket deletion: %w", catalogError(err))
	}
	defer func() { _ = tx.Rollback() }()
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM buckets WHERE name = ?`, name).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return ports.ErrBucketNotFound
	} else if err != nil {
		return fmt.Errorf("read SQLite bucket before deletion: %w", catalogError(err))
	}
	var hasObjects int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM objects WHERE bucket = ?)`, name).Scan(&hasObjects); err != nil {
		return fmt.Errorf("check SQLite bucket contents: %w", catalogError(err))
	}
	if hasObjects != 0 {
		return ports.ErrBucketNotEmpty
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM buckets WHERE name = ?`, name); err != nil {
		return fmt.Errorf("delete SQLite bucket record: %w", catalogError(err))
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite bucket deletion: %w", catalogError(err))
	}
	return nil
}

func (c *Catalog) PublishObject(ctx context.Context, object ports.CatalogObject) (previousRef string, publishErr error) {
	commitAttempted := false
	defer func() {
		if publishErr != nil && !commitAttempted {
			publishErr = errors.Join(ports.ErrPublishNotCommitted, publishErr)
		}
	}()
	info := object.Info
	if !ports.ValidBucketName(info.Bucket) {
		return "", ports.ErrInvalidBucket
	}
	if !ports.ValidObjectKey(info.Key) {
		return "", ports.ErrInvalidKey
	}
	if info.Size < 0 || object.PayloadRef == "" {
		return "", ports.ErrInvalidArgument
	}
	metadata, err := json.Marshal(info.Metadata)
	if err != nil {
		return "", fmt.Errorf("encode SQLite object metadata: %w", err)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin SQLite object publish: %w", catalogError(err))
	}
	defer func() { _ = tx.Rollback() }()
	var previousPayloadRef string
	err = tx.QueryRowContext(ctx, `SELECT payload_ref FROM objects WHERE bucket = ? AND object_key = ?`, info.Bucket, []byte(info.Key)).Scan(&previousPayloadRef)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("read previous SQLite object record: %w", catalogError(err))
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO objects (
			bucket, object_key, size, etag, content_type, cache_control,
			content_disposition, content_encoding, content_language, expires,
			user_metadata, last_modified_ns, payload_ref
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(bucket, object_key) DO UPDATE SET
			size = excluded.size,
			etag = excluded.etag,
			content_type = excluded.content_type,
			cache_control = excluded.cache_control,
			content_disposition = excluded.content_disposition,
			content_encoding = excluded.content_encoding,
			content_language = excluded.content_language,
			expires = excluded.expires,
			user_metadata = excluded.user_metadata,
			last_modified_ns = excluded.last_modified_ns,
			payload_ref = excluded.payload_ref`,
		info.Bucket, []byte(info.Key), info.Size, info.ETag, info.ContentType,
		info.CacheControl, info.ContentDisposition, info.ContentEncoding,
		info.ContentLanguage, info.Expires, metadata, info.LastModified.UTC().UnixNano(), object.PayloadRef,
	)
	if err != nil {
		if isSQLiteCode(err, sqlite3.ErrConstraint) {
			var exists int
			if checkErr := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM buckets WHERE name = ?)`, info.Bucket).Scan(&exists); checkErr == nil && exists == 0 {
				return "", ports.ErrBucketNotFound
			}
		}
		return "", fmt.Errorf("write SQLite object record: %w", catalogError(err))
	}
	commitAttempted = true
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit SQLite object publish: %w", catalogError(err))
	}
	return previousPayloadRef, nil
}

func (c *Catalog) ListObjects(ctx context.Context, query ports.ObjectListQuery) (result ports.ObjectListPage, resultErr error) {
	if !ports.ValidBucketName(query.Bucket) {
		return ports.ObjectListPage{}, ports.ErrInvalidBucket
	}
	if !ports.ValidObjectPrefix(query.Prefix) || !utf8.ValidString(query.Delimiter) || !utf8.ValidString(query.StartAfter) || len(query.StartAfter) > 1024 {
		return ports.ObjectListPage{}, ports.ErrInvalidArgument
	}
	if query.Limit < 0 || query.Limit > ports.MaxObjectListPageSize {
		return ports.ObjectListPage{}, ports.ErrInvalidArgument
	}
	startAfter := query.StartAfter
	var seekKey []byte
	seekInclusive := false
	if query.Cursor != nil {
		startAfter = query.Cursor.StartAfter
		if (query.Cursor.Key != "" && query.Cursor.StartAfter != "") || len(query.Cursor.StartAfter) > 1024 {
			return ports.ObjectListPage{}, ports.ErrInvalidArgument
		}
		if query.Cursor.Key == "" && query.StartAfter != "" && query.StartAfter != query.Cursor.StartAfter {
			return ports.ObjectListPage{}, ports.ErrInvalidArgument
		}
		if query.Cursor.Key != "" {
			if !ports.ValidObjectKey(query.Cursor.Key) || !strings.HasPrefix(query.Cursor.Key, query.Prefix) {
				return ports.ObjectListPage{}, ports.ErrInvalidArgument
			}
			if query.Cursor.IsCommonPrefix {
				if query.Delimiter == "" || !strings.HasSuffix(query.Cursor.Key, query.Delimiter) {
					return ports.ObjectListPage{}, ports.ErrInvalidArgument
				}
				seekKey = prefixUpperBound(query.Cursor.Key)
				seekInclusive = true
			} else {
				seekKey = []byte(query.Cursor.Key)
			}
		} else if query.Cursor.IsCommonPrefix {
			return ports.ObjectListPage{}, ports.ErrInvalidArgument
		}
	}
	if !utf8.ValidString(startAfter) {
		return ports.ObjectListPage{}, ports.ErrInvalidArgument
	}
	if seekKey == nil && startAfter != "" {
		seekKey = []byte(startAfter)
	}
	if query.Cursor != nil && query.Cursor.Key != "" && query.Cursor.IsCommonPrefix && seekKey == nil {
		page, err := c.emptyObjectListPage(ctx, query.Bucket)
		return page, err
	}
	conn, err := c.db.Conn(ctx)
	if err != nil {
		return ports.ObjectListPage{}, fmt.Errorf("reserve SQLite listing connection: %w", catalogError(err))
	}
	if _, err := conn.ExecContext(ctx, "BEGIN DEFERRED"); err != nil {
		beginErr := fmt.Errorf("begin SQLite listing snapshot: %w", catalogError(err))
		if closeErr := conn.Close(); closeErr != nil {
			beginErr = errors.Join(beginErr, fmt.Errorf("close SQLite listing connection: %w", closeErr))
		}
		return ports.ObjectListPage{}, beginErr
	}
	activeSnapshot := true
	defer func() {
		if activeSnapshot {
			if _, err := conn.ExecContext(context.Background(), "ROLLBACK"); err != nil {
				rollbackErr := fmt.Errorf("rollback SQLite listing snapshot: %w", catalogError(err))
				resultErr = errors.Join(resultErr, rollbackErr)
			}
		}
		if err := conn.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close SQLite listing connection: %w", err))
		}
	}()
	finish := func(page ports.ObjectListPage) (ports.ObjectListPage, error) {
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			return ports.ObjectListPage{}, fmt.Errorf("finish SQLite listing snapshot: %w", catalogError(err))
		}
		activeSnapshot = false
		return page, nil
	}

	batchSize := query.Limit + 1
	if batchSize == 0 {
		batchSize = 1
	}
	pageEntries := make([]ports.ObjectListEntry, 0, batchSize)
	for {
		objects, err := listObjectBatch(ctx, conn, query.Bucket, query.Prefix, seekKey, seekInclusive, batchSize)
		if err != nil {
			return ports.ObjectListPage{}, err
		}
		if len(objects) == 0 {
			break
		}

		jumpToNextGroup := false
		for i := 0; i < len(objects); {
			object := objects[i]
			commonPrefix := objectCommonPrefix(object.Key, query.Prefix, query.Delimiter)
			if commonPrefix != "" {
				if commonPrefix > startAfter {
					pageEntries = append(pageEntries, ports.ObjectListEntry{CommonPrefix: commonPrefix})
				}
				for i++; i < len(objects) && strings.HasPrefix(objects[i].Key, commonPrefix); i++ {
				}
				if i == len(objects) {
					seekKey = prefixUpperBound(commonPrefix)
					seekInclusive = true
					jumpToNextGroup = true
					break
				}
				continue
			}

			pageEntries = append(pageEntries, ports.ObjectListEntry{Object: &object})
			i++
			if len(pageEntries) > query.Limit {
				return finish(objectListPage(pageEntries, query.Limit, query))
			}
		}

		if len(pageEntries) > query.Limit {
			return finish(objectListPage(pageEntries, query.Limit, query))
		}
		if jumpToNextGroup {
			if seekKey == nil {
				break
			}
			continue
		}
		if len(objects) < batchSize {
			break
		}
		seekKey = []byte(objects[len(objects)-1].Key)
		seekInclusive = false
	}

	if len(pageEntries) == 0 {
		var exists int
		if err := conn.QueryRowContext(ctx, `SELECT 1 FROM buckets WHERE name = ?`, query.Bucket).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
			return ports.ObjectListPage{}, ports.ErrBucketNotFound
		} else if err != nil {
			return ports.ObjectListPage{}, fmt.Errorf("read SQLite bucket for listing: %w", catalogError(err))
		}
		return finish(ports.ObjectListPage{})
	}
	return finish(ports.ObjectListPage{Entries: pageEntries})
}

func listObjectBatch(ctx context.Context, conn *sql.Conn, bucket, prefix string, seekKey []byte, seekInclusive bool, limit int) ([]ports.ListedObject, error) {
	statement := `SELECT object_key, size, etag, last_modified_ns
		FROM objects WHERE bucket = ?`
	args := []any{bucket}
	if prefix != "" {
		statement += ` AND object_key >= ?`
		args = append(args, []byte(prefix))
		if upperBound := prefixUpperBound(prefix); upperBound != nil {
			statement += ` AND object_key < ?`
			args = append(args, upperBound)
		}
	}
	if seekKey != nil {
		if seekInclusive {
			statement += ` AND object_key >= ?`
		} else {
			statement += ` AND object_key > ?`
		}
		args = append(args, seekKey)
	}
	statement += ` ORDER BY object_key ASC LIMIT ?`
	args = append(args, limit)

	rows, err := conn.QueryContext(ctx, statement, args...)
	if err != nil {
		return nil, fmt.Errorf("list SQLite object records: %w", catalogError(err))
	}
	defer rows.Close()

	objects := make([]ports.ListedObject, 0, limit)
	for rows.Next() {
		var key []byte
		var object ports.ListedObject
		var lastModified int64
		if err := rows.Scan(&key, &object.Size, &object.ETag, &lastModified); err != nil {
			return nil, fmt.Errorf("scan SQLite object record: %w", catalogError(err))
		}
		object.Key = string(key)
		object.LastModified = time.Unix(0, lastModified).UTC()
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read SQLite object records: %w", catalogError(err))
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close SQLite object listing: %w", catalogError(err))
	}
	return objects, nil
}

func (c *Catalog) emptyObjectListPage(ctx context.Context, bucket string) (ports.ObjectListPage, error) {
	if err := c.HeadBucket(ctx, bucket); err != nil {
		return ports.ObjectListPage{}, err
	}
	return ports.ObjectListPage{}, nil
}

func objectCommonPrefix(key, prefix, delimiter string) string {
	if delimiter == "" {
		return ""
	}
	remainder := strings.TrimPrefix(key, prefix)
	position := strings.Index(remainder, delimiter)
	if position < 0 {
		return ""
	}
	return prefix + remainder[:position+len(delimiter)]
}

func objectListPage(entries []ports.ObjectListEntry, limit int, query ports.ObjectListQuery) ports.ObjectListPage {
	if limit == 0 {
		cursor := query.Cursor
		if cursor == nil {
			cursor = &ports.ObjectListCursor{StartAfter: query.StartAfter}
		}
		return ports.ObjectListPage{HasMore: true, NextCursor: cursor}
	}
	pageEntries := entries[:limit]
	last := pageEntries[len(pageEntries)-1]
	cursor := &ports.ObjectListCursor{}
	if last.Object != nil {
		cursor.Key = last.Object.Key
	} else {
		cursor.Key = last.CommonPrefix
		cursor.IsCommonPrefix = true
	}
	return ports.ObjectListPage{Entries: pageEntries, HasMore: true, NextCursor: cursor}
}

func (c *Catalog) GetObject(ctx context.Context, bucket, key string) (ports.CatalogObject, error) {
	if !ports.ValidBucketName(bucket) {
		return ports.CatalogObject{}, ports.ErrInvalidBucket
	}
	if !ports.ValidObjectKey(key) {
		return ports.CatalogObject{}, ports.ErrInvalidKey
	}
	object, err := scanObject(c.db.QueryRowContext(ctx, `
		SELECT bucket, object_key, size, etag, content_type, cache_control,
			content_disposition, content_encoding, content_language, expires,
			user_metadata, last_modified_ns, payload_ref
		FROM objects WHERE bucket = ? AND object_key = ?`, bucket, []byte(key)))
	if errors.Is(err, sql.ErrNoRows) {
		if bucketErr := c.HeadBucket(ctx, bucket); bucketErr != nil {
			return ports.CatalogObject{}, bucketErr
		}
		return ports.CatalogObject{}, ports.ErrObjectNotFound
	}
	if err != nil {
		return ports.CatalogObject{}, fmt.Errorf("read SQLite object record: %w", catalogError(err))
	}
	return object, nil
}

// FindPayloadRefs returns which of the supplied immutable payload references
// are still used by current catalog objects.
func (c *Catalog) FindPayloadRefs(ctx context.Context, payloadRefs []string) (referenced map[string]struct{}, resultErr error) {
	referenced = make(map[string]struct{})
	if len(payloadRefs) == 0 {
		return referenced, nil
	}
	if len(payloadRefs) > ports.MaxPayloadRefBatchSize {
		return nil, ports.ErrInvalidArgument
	}

	var query strings.Builder
	query.WriteString("SELECT DISTINCT payload_ref FROM objects WHERE payload_ref IN (")
	args := make([]any, len(payloadRefs))
	for i, ref := range payloadRefs {
		if i != 0 {
			query.WriteByte(',')
		}
		query.WriteByte('?')
		args[i] = ref
	}
	query.WriteByte(')')
	rows, err := c.db.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("find referenced SQLite payloads: %w", catalogError(err))
	}
	defer func() {
		resultErr = errors.Join(resultErr, rows.Close())
	}()
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			return nil, fmt.Errorf("read referenced SQLite payload: %w", catalogError(err))
		}
		referenced[ref] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate referenced SQLite payloads: %w", catalogError(err))
	}
	return referenced, nil
}

func (c *Catalog) DeleteObject(ctx context.Context, bucket, key string) (string, error) {
	if !ports.ValidBucketName(bucket) {
		return "", ports.ErrInvalidBucket
	}
	if !ports.ValidObjectKey(key) {
		return "", ports.ErrInvalidKey
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return "", fmt.Errorf("begin SQLite object deletion: %w", catalogError(err))
	}
	defer func() { _ = tx.Rollback() }()
	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT 1 FROM buckets WHERE name = ?`, bucket).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return "", ports.ErrBucketNotFound
	} else if err != nil {
		return "", fmt.Errorf("read SQLite bucket before object deletion: %w", catalogError(err))
	}
	var payloadRef string
	err = tx.QueryRowContext(ctx, `SELECT payload_ref FROM objects WHERE bucket = ? AND object_key = ?`, bucket, []byte(key)).Scan(&payloadRef)
	if errors.Is(err, sql.ErrNoRows) {
		if err := tx.Commit(); err != nil {
			return "", fmt.Errorf("commit empty SQLite object deletion: %w", catalogError(err))
		}
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read SQLite object before deletion: %w", catalogError(err))
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM objects WHERE bucket = ? AND object_key = ?`, bucket, []byte(key)); err != nil {
		return "", fmt.Errorf("delete SQLite object record: %w", catalogError(err))
	}
	if err := tx.Commit(); err != nil {
		return "", fmt.Errorf("commit SQLite object deletion: %w", catalogError(err))
	}
	return payloadRef, nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanObject(row rowScanner) (ports.CatalogObject, error) {
	var object ports.CatalogObject
	var key, metadata []byte
	var lastModified int64
	err := row.Scan(
		&object.Info.Bucket, &key, &object.Info.Size, &object.Info.ETag,
		&object.Info.ContentType, &object.Info.CacheControl,
		&object.Info.ContentDisposition, &object.Info.ContentEncoding,
		&object.Info.ContentLanguage, &object.Info.Expires, &metadata,
		&lastModified, &object.PayloadRef,
	)
	if err != nil {
		return ports.CatalogObject{}, err
	}
	object.Info.Key = string(key)
	object.Info.LastModified = time.Unix(0, lastModified).UTC()
	if err := json.Unmarshal(metadata, &object.Info.Metadata); err != nil {
		return ports.CatalogObject{}, fmt.Errorf("decode SQLite object metadata: %w", err)
	}
	return object, nil
}

func catalogError(err error) error {
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) && (sqliteErr.Code == sqlite3.ErrBusy || sqliteErr.Code == sqlite3.ErrLocked) {
		return fmt.Errorf("%w: %v", ports.ErrBusy, err)
	}
	return err
}

func isSQLiteCode(err error, code sqlite3.ErrNo) bool {
	var sqliteErr sqlite3.Error
	return errors.As(err, &sqliteErr) && sqliteErr.Code == code
}

func prefixUpperBound(prefix string) []byte {
	upperBound := []byte(prefix)
	for i := len(upperBound) - 1; i >= 0; i-- {
		if upperBound[i] != 0xff {
			upperBound[i]++
			return upperBound[:i+1]
		}
	}
	return nil
}

func (c *Catalog) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.db.Close()
	})
	return c.closeErr
}

func syncDir(path string) error {
	return fsdir.Sync(path)
}
