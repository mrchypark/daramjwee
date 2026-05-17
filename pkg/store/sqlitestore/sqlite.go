package sqlitestore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/google/uuid"
	"github.com/mrchypark/daramjwee"
	_ "modernc.org/sqlite"
)

const (
	defaultChunkSize     = 512 * 1024
	defaultMaxOpenConns  = 16
	defaultMaxIdleConns  = 4
	defaultTempChunkTTL  = 24 * time.Hour
	sqliteDriverName     = "sqlite"
	sqliteSchemaUserVers = 1
)

// Option configures SQLiteStore.
type Option func(*SQLiteStore)

// WithChunkSize sets the payload chunk size used for staged writes.
// Values less than 1 are ignored.
func WithChunkSize(size int) Option {
	return func(s *SQLiteStore) {
		if size > 0 {
			s.chunkSize = size
		}
	}
}

// WithConnectionPool sets SQLite database/sql pool limits.
// Values less than 1 are ignored independently.
func WithConnectionPool(maxOpen, maxIdle int) Option {
	return func(s *SQLiteStore) {
		if maxOpen > 0 {
			s.maxOpenConns = maxOpen
		}
		if maxIdle > 0 {
			s.maxIdleConns = maxIdle
		}
	}
}

// SQLiteStore is a SQLite-backed daramjwee.Store implementation.
type SQLiteStore struct {
	db           *sql.DB
	logger       log.Logger
	chunkSize    int
	maxOpenConns int
	maxIdleConns int
	ownerID      string
	bufPool      sync.Pool

	closeOnce sync.Once
	closeErr  error
}

var (
	_ daramjwee.Store                = (*SQLiteStore)(nil)
	_ daramjwee.GetStreamUsesContext = (*SQLiteStore)(nil)
	_ daramjwee.BeginSetUsesContext  = (*SQLiteStore)(nil)
)

// GetStreamUsesContext reports that returned readers continue to use the
// context passed to GetStream.
func (s *SQLiteStore) GetStreamUsesContext() bool { return true }

// BeginSetUsesContext reports that returned sinks continue to use the context
// passed to BeginSet.
func (s *SQLiteStore) BeginSetUsesContext() bool { return true }

// New opens or creates a SQLite database at path and initializes the store schema.
func New(path string, logger log.Logger, opts ...Option) (*SQLiteStore, error) {
	return NewWithContext(context.Background(), path, logger, opts...)
}

// NewWithContext opens or creates a SQLite database at path and initializes the store schema.
func NewWithContext(ctx context.Context, path string, logger log.Logger, opts ...Option) (*SQLiteStore, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = log.NewNopLogger()
	}
	if path == "" {
		return nil, errors.New("sqlitestore: path cannot be empty")
	}
	cleanPath := filepath.Clean(path)
	if err := os.MkdirAll(filepath.Dir(cleanPath), 0755); err != nil {
		return nil, fmt.Errorf("sqlitestore: create database directory: %w", err)
	}
	absPath, err := filepath.Abs(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("sqlitestore: resolve database path: %w", err)
	}

	db, err := sql.Open(sqliteDriverName, sqliteDSN(absPath))
	if err != nil {
		return nil, fmt.Errorf("sqlitestore: open database: %w", err)
	}

	store := &SQLiteStore{
		db:           db,
		logger:       logger,
		chunkSize:    defaultChunkSize,
		maxOpenConns: defaultMaxOpenConns,
		maxIdleConns: defaultMaxIdleConns,
		ownerID:      uuid.NewString(),
	}
	for _, opt := range opts {
		opt(store)
	}
	store.bufPool.New = func() any {
		return make([]byte, 0, store.chunkSize)
	}
	db.SetMaxOpenConns(store.maxOpenConns)
	db.SetMaxIdleConns(store.maxIdleConns)

	if err := db.PingContext(ctx); err != nil {
		logDBCloseError(logger, db, "failed to close sqlite database after ping failure")
		return nil, fmt.Errorf("sqlitestore: ping database: %w", err)
	}

	if err := store.initSchema(ctx); err != nil {
		logDBCloseError(logger, db, "failed to close sqlite database after schema initialization failure")
		return nil, err
	}
	return store, nil
}

// Close closes the underlying database handle.
func (s *SQLiteStore) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.db.Close()
	})
	return s.closeErr
}

// GetStream retrieves a committed object and its metadata as a chunked stream.
func (s *SQLiteStore) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, fmt.Errorf("sqlitestore: get stream for key %q: begin read transaction: %w", key, err)
	}

	meta, err := statInTx(ctx, tx, key)
	if err != nil {
		s.rollbackReadTx(tx, "failed to rollback sqlite read transaction after stat failure")
		return nil, nil, err
	}

	rows, err := tx.QueryContext(ctx, `SELECT data FROM chunks WHERE key = ? ORDER BY seq`, key)
	if err != nil {
		s.rollbackReadTx(tx, "failed to rollback sqlite read transaction after query failure")
		return nil, nil, err
	}
	return &chunkReader{ctx: ctx, tx: tx, rows: rows}, meta, nil
}

// BeginSet starts a staged chunked write. The committed value for key remains
// unchanged until the returned sink is closed successfully.
func (s *SQLiteStore) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	generation, err := s.nextGeneration(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlitestore: begin set for key %q: get next generation: %w", key, err)
	}
	return &sqliteSink{
		ctx:        ctx,
		store:      s,
		key:        key,
		metadata:   cloneMetadata(metadata),
		writeID:    uuid.NewString(),
		chunkSize:  s.chunkSize,
		generation: generation,
		buf:        s.getBuffer(),
	}, nil
}

// Delete removes the last committed object for key. It does not inspect or wait
// for staged writes that have not reached Close.
func (s *SQLiteStore) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		generation, err := s.nextGenerationInTx(ctx, tx)
		if err != nil {
			return fmt.Errorf("sqlitestore: delete key %q: get next generation: %w", key, err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM entries WHERE key = ?`, key); err != nil {
			return fmt.Errorf("sqlitestore: delete entry for key %q: %w", key, err)
		}
		_, err = tx.ExecContext(ctx, `
			INSERT INTO generation_floor(key, generation) VALUES(?, ?)
			ON CONFLICT(key) DO UPDATE SET generation = max(generation_floor.generation, excluded.generation)
		`, key, generation)
		if err != nil {
			return fmt.Errorf("sqlitestore: update generation floor for deleted key %q: %w", key, err)
		}
		return err
	})
}

// Stat retrieves metadata without opening the object stream.
func (s *SQLiteStore) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return statInTx(ctx, s.db, key)
}

type queryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func statInTx(ctx context.Context, q queryer, key string) (*daramjwee.Metadata, error) {
	var meta daramjwee.Metadata
	var cachedAt string
	var isNegative int
	err := q.QueryRowContext(ctx, `
		SELECT cache_tag, is_negative, cached_at
		FROM entries
		WHERE key = ?
	`, key).Scan(&meta.CacheTag, &isNegative, &cachedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, daramjwee.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if cachedAt != "" {
		parsed, err := time.Parse(time.RFC3339Nano, cachedAt)
		if err != nil {
			return nil, fmt.Errorf("sqlitestore: parse cached_at for key %q: %w", key, err)
		}
		meta.CachedAt = parsed
	}
	meta.IsNegative = isNegative != 0
	return &meta, nil
}

func (s *SQLiteStore) initSchema(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS entries (
			key TEXT PRIMARY KEY,
			cache_tag TEXT NOT NULL,
			is_negative INTEGER NOT NULL,
			cached_at TEXT NOT NULL,
			generation INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS chunks (
			key TEXT NOT NULL,
			seq INTEGER NOT NULL,
			data BLOB NOT NULL,
			PRIMARY KEY (key, seq),
			FOREIGN KEY (key) REFERENCES entries(key) ON DELETE CASCADE
		)`,
		`CREATE TABLE IF NOT EXISTS temp_chunks (
			owner_id TEXT NOT NULL,
			write_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			data BLOB NOT NULL,
			created_at INTEGER NOT NULL,
			PRIMARY KEY (owner_id, write_id, seq)
		)`,
		`CREATE INDEX IF NOT EXISTS temp_chunks_created_at_idx ON temp_chunks(created_at)`,
		`CREATE TABLE IF NOT EXISTS generation_floor (
			key TEXT PRIMARY KEY,
			generation INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS generation_sequence (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			generation INTEGER NOT NULL
		)`,
		fmt.Sprintf(`PRAGMA user_version = %d`, sqliteSchemaUserVers),
	}
	for _, stmt := range statements {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlitestore: initialize schema: %w", err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO generation_sequence(id, generation)
		SELECT 1, max(
			COALESCE((SELECT MAX(generation) FROM entries), 0),
			COALESCE((SELECT MAX(generation) FROM generation_floor), 0),
			COALESCE((SELECT generation FROM generation_sequence WHERE id = 1), 0)
		)
		ON CONFLICT(id) DO UPDATE SET generation = max(generation_sequence.generation, excluded.generation)
	`); err != nil {
		return fmt.Errorf("sqlitestore: initialize generation sequence: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM temp_chunks WHERE created_at < ?`, time.Now().Add(-defaultTempChunkTTL).UnixNano()); err != nil {
		return fmt.Errorf("sqlitestore: clean stale temporary chunks: %w", err)
	}
	return nil
}

func sqliteDSN(path string) string {
	u := url.URL{Scheme: "file", Path: path}
	q := u.Query()
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "foreign_keys(ON)")
	u.RawQuery = q.Encode()
	return u.String()
}

func (s *SQLiteStore) nextGeneration(ctx context.Context) (uint64, error) {
	var generation uint64
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		next, err := s.nextGenerationInTx(ctx, tx)
		if err != nil {
			return err
		}
		generation = next
		return nil
	})
	return generation, err
}

func (s *SQLiteStore) nextGenerationInTx(ctx context.Context, tx *sql.Tx) (uint64, error) {
	if _, err := tx.ExecContext(ctx, `UPDATE generation_sequence SET generation = generation + 1 WHERE id = 1`); err != nil {
		return 0, err
	}
	var generation uint64
	if err := tx.QueryRowContext(ctx, `SELECT generation FROM generation_sequence WHERE id = 1`).Scan(&generation); err != nil {
		return 0, err
	}
	return generation, nil
}

func (s *SQLiteStore) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) getBuffer() []byte {
	buf := s.bufPool.Get().([]byte)
	if cap(buf) < s.chunkSize {
		return make([]byte, 0, s.chunkSize)
	}
	return buf[:0]
}

func (s *SQLiteStore) putBuffer(buf []byte) {
	if cap(buf) < s.chunkSize {
		return
	}
	s.bufPool.Put(buf[:0])
}

func (s *SQLiteStore) rollbackReadTx(tx *sql.Tx, msg string) {
	if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		level.Warn(s.logger).Log("msg", msg, "err", err)
	}
}

func logDBCloseError(logger log.Logger, db *sql.DB, msg string) {
	if err := db.Close(); err != nil {
		level.Warn(logger).Log("msg", msg, "err", err)
	}
}

func cloneMetadata(meta *daramjwee.Metadata) *daramjwee.Metadata {
	if meta == nil {
		return &daramjwee.Metadata{}
	}
	cloned := *meta
	return &cloned
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

type sqliteSink struct {
	ctx        context.Context
	store      *SQLiteStore
	key        string
	metadata   *daramjwee.Metadata
	writeID    string
	chunkSize  int
	generation uint64

	mu   sync.Mutex
	buf  []byte
	seq  int
	done bool
}

func (w *sqliteSink) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return 0, io.ErrClosedPipe
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}

	written := len(p)
	for len(p) > 0 {
		remaining := w.chunkSize - len(w.buf)
		if remaining > len(p) {
			remaining = len(p)
		}
		w.buf = append(w.buf, p[:remaining]...)
		p = p[remaining:]
		if len(w.buf) == w.chunkSize {
			if err := w.flushLocked(); err != nil {
				return written - len(p), err
			}
		}
	}
	return written, nil
}

func (w *sqliteSink) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return nil
	}
	w.done = true
	if err := w.ctx.Err(); err != nil {
		return w.cleanup(context.Background(), err)
	}
	if len(w.buf) > 0 {
		if err := w.flushLocked(); err != nil {
			return w.cleanup(context.Background(), err)
		}
	}
	if err := w.commitLocked(); err != nil {
		return w.cleanup(context.Background(), err)
	}
	return w.cleanup(context.Background(), nil)
}

func (w *sqliteSink) Abort() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return nil
	}
	w.done = true
	return w.cleanup(context.Background(), nil)
}

func (w *sqliteSink) flushLocked() error {
	if len(w.buf) == 0 {
		return nil
	}
	if _, err := w.store.db.ExecContext(w.ctx, `
		INSERT INTO temp_chunks(owner_id, write_id, seq, data, created_at) VALUES(?, ?, ?, ?, ?)
	`, w.store.ownerID, w.writeID, w.seq, w.buf, time.Now().UnixNano()); err != nil {
		return fmt.Errorf("sqlitestore: flush staged chunk for key %q: %w", w.key, err)
	}
	w.seq++
	w.buf = w.buf[:0]
	return nil
}

func (w *sqliteSink) commitLocked() error {
	meta := w.metadata
	return w.store.withTx(w.ctx, func(tx *sql.Tx) error {
		var floor uint64
		err := tx.QueryRowContext(w.ctx, `SELECT generation FROM generation_floor WHERE key = ?`, w.key).Scan(&floor)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("sqlitestore: read generation floor for key %q: %w", w.key, err)
		}
		if floor > w.generation {
			return fmt.Errorf("sqlitestore: stale write rejected for key %q: %w", w.key, daramjwee.ErrTopWriteInvalidated)
		}
		var stagedChunks int
		if err := tx.QueryRowContext(w.ctx, `
			SELECT COUNT(*) FROM temp_chunks WHERE owner_id = ? AND write_id = ?
		`, w.store.ownerID, w.writeID).Scan(&stagedChunks); err != nil {
			return fmt.Errorf("sqlitestore: count staged chunks for key %q: %w", w.key, err)
		}
		if stagedChunks != w.seq {
			return fmt.Errorf("sqlitestore: staged chunk count mismatch for key %q: got %d, want %d", w.key, stagedChunks, w.seq)
		}

		if _, err := tx.ExecContext(w.ctx, `DELETE FROM entries WHERE key = ?`, w.key); err != nil {
			return fmt.Errorf("sqlitestore: delete existing entry for key %q: %w", w.key, err)
		}
		if _, err := tx.ExecContext(w.ctx, `
			INSERT INTO entries(key, cache_tag, is_negative, cached_at, generation)
			VALUES(?, ?, ?, ?, ?)
		`, w.key, meta.CacheTag, boolToInt(meta.IsNegative), meta.CachedAt.UTC().Format(time.RFC3339Nano), w.generation); err != nil {
			return fmt.Errorf("sqlitestore: insert entry for key %q: %w", w.key, err)
		}
		if _, err := tx.ExecContext(w.ctx, `
			INSERT INTO chunks(key, seq, data)
			SELECT ?, seq, data FROM temp_chunks WHERE owner_id = ? AND write_id = ? ORDER BY seq
		`, w.key, w.store.ownerID, w.writeID); err != nil {
			return fmt.Errorf("sqlitestore: publish staged chunks for key %q: %w", w.key, err)
		}
		_, err = tx.ExecContext(w.ctx, `
			INSERT INTO generation_floor(key, generation) VALUES(?, ?)
			ON CONFLICT(key) DO UPDATE SET generation = max(generation_floor.generation, excluded.generation)
		`, w.key, w.generation)
		if err != nil {
			return fmt.Errorf("sqlitestore: update generation floor for key %q: %w", w.key, err)
		}
		return nil
	})
}

func (w *sqliteSink) cleanup(ctx context.Context, cause error) error {
	w.releaseBuffer()
	_, err := w.store.db.ExecContext(ctx, `DELETE FROM temp_chunks WHERE owner_id = ? AND write_id = ?`, w.store.ownerID, w.writeID)
	if cause != nil {
		if err != nil {
			level.Warn(w.store.logger).Log("msg", "failed to clean staged sqlite chunks", "key", w.key, "err", err)
		}
		return cause
	}
	return err
}

func (w *sqliteSink) releaseBuffer() {
	if w.buf == nil {
		return
	}
	w.store.putBuffer(w.buf)
	w.buf = nil
}

type chunkReader struct {
	ctx     context.Context
	tx      *sql.Tx
	rows    *sql.Rows
	current []byte
	closed  bool
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	for len(r.current) == 0 {
		if !r.rows.Next() {
			if err := r.rows.Err(); err != nil {
				return 0, fmt.Errorf("sqlitestore: read chunk stream: iterate chunks: %w", err)
			}
			return 0, io.EOF
		}
		if err := r.rows.Scan(&r.current); err != nil {
			return 0, fmt.Errorf("sqlitestore: read chunk stream: scan chunk data: %w", err)
		}
	}
	n := copy(p, r.current)
	r.current = r.current[n:]
	return n, nil
}

func (r *chunkReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	rowsErr := r.rows.Close()
	txErr := r.tx.Rollback()
	if txErr != nil && errors.Is(txErr, sql.ErrTxDone) {
		txErr = nil
	}
	return errors.Join(rowsErr, txErr)
}
