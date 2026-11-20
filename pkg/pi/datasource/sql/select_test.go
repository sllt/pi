package sql

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/sllt/pi/pkg/pi/config"
	"github.com/sllt/pi/pkg/pi/logging"
)

type customTagScanner struct {
	Raw string
}

func (c *customTagScanner) Scan(src any) error {
	if src == nil {
		c.Raw = ""
		return nil
	}
	switch v := src.(type) {
	case string:
		c.Raw = "scanned:" + v
	case []byte:
		c.Raw = "scanned:" + string(v)
	default:
		return fmt.Errorf("unexpected type %T", src)
	}
	return nil
}

func openInMemorySQLiteDB(t *testing.T) *DB {
	t.Helper()

	rawDB, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rawDB.Close() })

	return &DB{
		DB:     rawDB,
		config: &DBConfig{Dialect: "sqlite"},
		logger: logging.NewWriterLogger(logging.ERROR, io.Discard, io.Discard),
	}
}

func TestSelect_SQLiteTypesAndPointers(t *testing.T) {
	db := openInMemorySQLiteDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `
		CREATE TABLE posts (
			id INTEGER PRIMARY KEY,
			title TEXT NOT NULL,
			subtitle TEXT,
			published INTEGER NOT NULL,
			views INTEGER,
			created_at DATETIME NOT NULL,
			published_at DATETIME,
			deleted_at DATETIME,
			extra TEXT
		)
	`)
	require.NoError(t, err)

	createdAt1 := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	pubAt1 := time.Date(2026, 11, 2, 12, 30, 0, 0, time.UTC)
	createdAt2 := time.Date(2026, 11, 3, 8, 15, 0, 0, time.UTC)

	_, err = db.ExecContext(ctx,
		`INSERT INTO posts (id, title, subtitle, published, views, created_at, published_at, deleted_at, extra) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		1, "First", "Sub1", 1, 42, createdAt1, pubAt1, nil, "alpha",
	)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx,
		`INSERT INTO posts (id, title, subtitle, published, views, created_at, published_at, deleted_at, extra) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		2, "Second", nil, 0, nil, createdAt2, nil, pubAt1, "beta",
	)
	require.NoError(t, err)

	type postRow struct {
		ID          int64
		PostTitle   string         `db:"title,omitempty"`
		Subtitle    sql.NullString `db:"subtitle"`
		Published   bool
		Views       *int64
		CreatedAt   time.Time
		PublishedAt sql.NullTime
		DeletedAt   *time.Time
		Extra       customTagScanner `db:"extra"`
		Ignored     string           `db:"-"`
		unexported  string
	}

	// Slice of structs
	var posts []postRow
	err = db.Select(ctx, &posts, `SELECT id, title, subtitle, published, views, created_at, published_at, deleted_at, extra, 'must-ignore' AS ignored FROM posts ORDER BY id ASC`)
	require.NoError(t, err)
	require.Len(t, posts, 2)

	assert.Equal(t, int64(1), posts[0].ID)
	assert.Equal(t, "First", posts[0].PostTitle)
	assert.Equal(t, sql.NullString{String: "Sub1", Valid: true}, posts[0].Subtitle)
	assert.True(t, posts[0].Published)
	require.NotNil(t, posts[0].Views)
	assert.Equal(t, int64(42), *posts[0].Views)
	assert.True(t, posts[0].CreatedAt.Equal(createdAt1))
	assert.True(t, posts[0].PublishedAt.Valid)
	assert.True(t, posts[0].PublishedAt.Time.Equal(pubAt1))
	assert.Nil(t, posts[0].DeletedAt)
	assert.Equal(t, customTagScanner{Raw: "scanned:alpha"}, posts[0].Extra)
	assert.Empty(t, posts[0].Ignored)
	assert.Empty(t, posts[0].unexported)

	assert.Equal(t, int64(2), posts[1].ID)
	assert.Equal(t, "Second", posts[1].PostTitle)
	assert.False(t, posts[1].Subtitle.Valid)
	assert.False(t, posts[1].Published)
	assert.Nil(t, posts[1].Views)
	assert.True(t, posts[1].CreatedAt.Equal(createdAt2))
	assert.False(t, posts[1].PublishedAt.Valid)
	require.NotNil(t, posts[1].DeletedAt)
	assert.True(t, posts[1].DeletedAt.Equal(pubAt1))
	assert.Equal(t, customTagScanner{Raw: "scanned:beta"}, posts[1].Extra)

	// Slice of pointers
	var ptrPosts []*postRow
	err = db.Select(ctx, &ptrPosts, `SELECT id, title, published, created_at, deleted_at FROM posts ORDER BY id ASC`)
	require.NoError(t, err)
	require.Len(t, ptrPosts, 2)
	assert.Equal(t, "First", ptrPosts[0].PostTitle)
	assert.True(t, ptrPosts[0].Published)
	assert.Nil(t, ptrPosts[0].DeletedAt)
	assert.Equal(t, "Second", ptrPosts[1].PostTitle)
	assert.False(t, ptrPosts[1].Published)
	require.NotNil(t, ptrPosts[1].DeletedAt)
	assert.True(t, ptrPosts[1].DeletedAt.Equal(pubAt1))

	// Scalar slice ([]int)
	var ids []int
	err = db.Select(ctx, &ids, `SELECT id FROM posts ORDER BY id ASC`)
	require.NoError(t, err)
	assert.Equal(t, []int{1, 2}, ids)

	// Leaf scanner & time.Time slices
	var times []time.Time
	err = db.Select(ctx, &times, `SELECT created_at FROM posts ORDER BY id ASC`)
	require.NoError(t, err)
	require.Len(t, times, 2)
	assert.True(t, times[0].Equal(createdAt1))
	assert.True(t, times[1].Equal(createdAt2))

	var nullableTimes []*time.Time
	err = db.Select(ctx, &nullableTimes, `SELECT deleted_at FROM posts ORDER BY id ASC`)
	require.NoError(t, err)
	require.Len(t, nullableTimes, 2)
	assert.Nil(t, nullableTimes[0])
	require.NotNil(t, nullableTimes[1])
	assert.True(t, nullableTimes[1].Equal(pubAt1))

	var subtitles []sql.NullString
	err = db.Select(ctx, &subtitles, `SELECT subtitle FROM posts ORDER BY id ASC`)
	require.NoError(t, err)
	assert.Equal(t, []sql.NullString{
		{String: "Sub1", Valid: true},
		{String: "", Valid: false},
	}, subtitles)

	// Single leaf struct (time.Time)
	var singleTime time.Time
	err = db.Select(ctx, &singleTime, `SELECT created_at FROM posts WHERE id = ?`, 1)
	require.NoError(t, err)
	assert.True(t, singleTime.Equal(createdAt1))
}

func TestSelect_EmbeddedStructsAndShadowing(t *testing.T) {
	db := openInMemorySQLiteDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `
		CREATE TABLE comments (
			id INTEGER PRIMARY KEY,
			post_id INTEGER NOT NULL,
			body TEXT NOT NULL,
			created_at DATETIME NOT NULL
		);
		CREATE TABLE posts (
			id INTEGER PRIMARY KEY,
			title TEXT NOT NULL
		);
	`)
	require.NoError(t, err)

	now := time.Date(2026, 11, 10, 9, 0, 0, 0, time.UTC)
	_, err = db.ExecContext(ctx, `INSERT INTO posts (id, title) VALUES (10, 'Go 1.27')`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO comments (id, post_id, body, created_at) VALUES (100, 10, 'Nice post!', ?)`, now)
	require.NoError(t, err)

	type Comment struct {
		ID        int64
		PostID    int64
		Body      string
		CreatedAt time.Time
		secret    string
	}
	type CommentWithPost struct {
		Comment
		PostTitle string
	}

	var items []CommentWithPost
	err = db.Select(ctx, &items, `
		SELECT c.id, c.post_id, c.body, c.created_at, p.title AS post_title
		FROM comments c
		JOIN posts p ON p.id = c.post_id
	`)
	require.NoError(t, err)
	require.Len(t, items, 1)
	assert.Equal(t, int64(100), items[0].ID)
	assert.Equal(t, int64(10), items[0].PostID)
	assert.Equal(t, "Nice post!", items[0].Body)
	assert.True(t, items[0].CreatedAt.Equal(now))
	assert.Equal(t, "Go 1.27", items[0].PostTitle)
	assert.Empty(t, items[0].secret)

	// Pointer embedded struct + outer field shadowing embedded field
	type EmbeddedMeta struct {
		ID    int64
		Title string
	}
	type PostView struct {
		*EmbeddedMeta
		Title string // shadows EmbeddedMeta.Title
	}

	var pv PostView
	err = db.Select(ctx, &pv, `SELECT 7 AS id, 'Outer Wins' AS title`)
	require.NoError(t, err)
	require.NotNil(t, pv.EmbeddedMeta)
	assert.Equal(t, int64(7), pv.EmbeddedMeta.ID)
	assert.Empty(t, pv.EmbeddedMeta.Title)
	assert.Equal(t, "Outer Wins", pv.Title)

	// Nil embedded pointer stays nil when none of its fields are selected
	var pvOnlyOuter PostView
	err = db.Select(ctx, &pvOnlyOuter, `SELECT 'Only Outer' AS title`)
	require.NoError(t, err)
	assert.Nil(t, pvOnlyOuter.EmbeddedMeta)
	assert.Equal(t, "Only Outer", pvOnlyOuter.Title)

	// Unexported anonymous embedded value struct promotes exported fields cleanly;
	// unexported anonymous embedded pointer struct is safely skipped without panicking.
	type baseModel struct {
		ID        int64
		CreatedAt time.Time
		internal  string
	}
	type unexportedPtrBase struct {
		Secret string
	}
	type PostWithUnexportedEmbed struct {
		baseModel
		*unexportedPtrBase
		Title string
	}

	var pue PostWithUnexportedEmbed
	err = db.Select(ctx, &pue, `SELECT 100 AS id, ? AS created_at, 'Embedded Value' AS title, 'ignored' AS secret`, now)
	require.NoError(t, err)
	assert.Equal(t, int64(100), pue.ID)
	assert.True(t, pue.CreatedAt.Equal(now))
	assert.Equal(t, "Embedded Value", pue.Title)
	assert.Empty(t, pue.internal)
	assert.Nil(t, pue.unexportedPtrBase)

	// Unexported anonymous embedded value struct with a non-empty db tag must not panic or scan into the unexported field
	type TaggedUnexportedEmbed struct {
		baseModel `db:"inner_col"`
		Title     string
	}
	var tue TaggedUnexportedEmbed
	err = db.Select(ctx, &tue, `SELECT 'ignored' AS inner_col, 'ok' AS title`)
	require.NoError(t, err)
	assert.Equal(t, "ok", tue.Title)
	assert.Zero(t, tue.baseModel)
}

func TestSelect_SliceResetsPrePopulatedAndEmptyIsNonNil(t *testing.T) {
	db := openInMemorySQLiteDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `CREATE TABLE nums (n INTEGER)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO nums (n) VALUES (10), (20)`)
	require.NoError(t, err)

	// Reused pre-populated slice must not retain stale rows
	nums := []int{999, 888}
	err = db.Select(ctx, &nums, `SELECT n FROM nums ORDER BY n ASC`)
	require.NoError(t, err)
	assert.Equal(t, []int{10, 20}, nums)

	// Reused slice with 0 rows becomes empty non-nil slice
	err = db.Select(ctx, &nums, `SELECT n FROM nums WHERE n < 0`)
	require.NoError(t, err)
	assert.NotNil(t, nums)
	assert.Empty(t, nums)
}

func TestSelect_SQLiteTextTimestampsIntoTimeAndPtr(t *testing.T) {
	db := openInMemorySQLiteDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `
		CREATE TABLE events (
			id INTEGER PRIMARY KEY,
			ts_val TEXT NOT NULL,
			ts_ptr TEXT
		)
	`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO events (id, ts_val, ts_ptr) VALUES (1, '2026-01-02 03:04:05', '2026-02-03T04:05:06Z'), (2, '2026-03-04 05:06:07.123456+08:00', NULL)`)
	require.NoError(t, err)

	type eventRow struct {
		ID    int64
		TsVal time.Time
		TsPtr *time.Time
	}

	var rows []eventRow
	err = db.Select(ctx, &rows, `SELECT id, ts_val, ts_ptr FROM events ORDER BY id ASC`)
	require.NoError(t, err)
	require.Len(t, rows, 2)

	assert.Equal(t, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), rows[0].TsVal)
	require.NotNil(t, rows[0].TsPtr)
	assert.True(t, rows[0].TsPtr.Equal(time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)))

	wantZone := time.FixedZone("", 8*3600)
	assert.True(t, rows[1].TsVal.Equal(time.Date(2026, 3, 4, 5, 6, 7, 123456000, wantZone)))
	assert.Nil(t, rows[1].TsPtr)
}

func TestSelect_SingleStructStopsAfterFirstRowAndErrNoRows(t *testing.T) {
	db := openInMemorySQLiteDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `CREATE TABLE items (seq INTEGER, val INTEGER)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO items (seq, val) VALUES (1, 100), (2, 'not-an-int')`)
	require.NoError(t, err)

	type item struct {
		Seq int
		Val int
	}

	// Row 2 has non-integer 'not-an-int' in val; if selectStruct scanned beyond row 1 it would fail.
	var got item
	err = db.Select(ctx, &got, `SELECT seq, val FROM items ORDER BY seq ASC`)
	require.NoError(t, err)
	assert.Equal(t, item{Seq: 1, Val: 100}, got)

	// Empty result returns sql.ErrNoRows
	var missing item
	err = db.Select(ctx, &missing, `SELECT seq, val FROM items WHERE seq = 999`)
	require.ErrorIs(t, err, sql.ErrNoRows)
}

func TestTx_DialectAndSelect(t *testing.T) {
	var nilTx *Tx
	assert.Equal(t, "", nilTx.Dialect())
	assert.Equal(t, "", (&Tx{}).Dialect())

	db := openInMemorySQLiteDB(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)`)
	require.NoError(t, err)

	tx, err := db.BeginTxContext(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	assert.Equal(t, "sqlite", tx.Dialect())

	_, err = tx.ExecContext(ctx, `INSERT INTO users (id, name) VALUES (?, ?)`, 1, "alice")
	require.NoError(t, err)

	type user struct {
		ID   int64
		Name string
	}
	var got user
	err = tx.Select(ctx, &got, `SELECT id, name FROM users WHERE id = ?`, 1)
	require.NoError(t, err)
	assert.Equal(t, user{ID: 1, Name: "alice"}, got)
}

func TestSlowQueryLogging(t *testing.T) {
	t.Run("config parsing", func(t *testing.T) {
		cfg := getDBConfig(config.NewMockConfig(map[string]string{
			"DB_SLOW_QUERY_THRESHOLD": "150ms",
		}))
		assert.Equal(t, 150*time.Millisecond, cfg.SlowQueryThreshold)

		for _, raw := range []string{"", "invalid", "0s", "-5ms"} {
			c := getDBConfig(config.NewMockConfig(map[string]string{
				"DB_SLOW_QUERY_THRESHOLD": raw,
			}))
			assert.Equal(t, time.Duration(0), c.SlowQueryThreshold, "raw=%q", raw)
		}
	})

	t.Run("warns with trace_id, truncates SQL, omits bind args", func(t *testing.T) {
		var buf bytes.Buffer
		logger := logging.NewWriterLogger(logging.WARN, &buf, &buf)
		db := &DB{
			logger: logger,
			config: &DBConfig{
				Dialect:            "sqlite",
				HostName:           "localhost",
				Database:           "testdb",
				SlowQueryThreshold: 10 * time.Millisecond,
			},
		}

		tid, err := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
		require.NoError(t, err)
		sid, err := trace.SpanIDFromHex("1111111111111111")
		require.NoError(t, err)
		ctx := trace.ContextWithSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    tid,
			SpanID:     sid,
			TraceFlags: trace.FlagsSampled,
		}))

		longSQL := "SELECT   id,   title   FROM   posts   WHERE   secret = ? AND note = '" + strings.Repeat("x", 600) + "'"
		secretArg := "super-secret-password-value"

		db.sendOperationStats(ctx, time.Now().Add(-25*time.Millisecond), "QueryContext", longSQL, secretArg)

		out := buf.String()
		assert.Contains(t, out, "slow SQL query")
		assert.Contains(t, out, "op=QueryContext")
		assert.Contains(t, out, "trace_id="+tid.String())
		assert.Contains(t, out, "SELECT id, title FROM posts")
		assert.Contains(t, out, "...")
		assert.NotContains(t, out, secretArg)
		assert.LessOrEqual(t, len(truncateSQL(longSQL, maxSlowQuerySQLLen)), maxSlowQuerySQLLen)
	})

	t.Run("silent when disabled or below threshold", func(t *testing.T) {
		var buf bytes.Buffer
		logger := logging.NewWriterLogger(logging.WARN, &buf, &buf)

		disabledDB := &DB{
			logger: logger,
			config: &DBConfig{SlowQueryThreshold: 0},
		}
		disabledDB.sendOperationStats(t.Context(), time.Now().Add(-500*time.Millisecond), "ExecContext", "DELETE FROM logs", "arg1")
		assert.Empty(t, buf.String())

		fastDB := &DB{
			logger: logger,
			config: &DBConfig{SlowQueryThreshold: 1 * time.Hour},
		}
		fastDB.sendOperationStats(t.Context(), time.Now(), "QueryContext", "SELECT 1", "arg1")
		assert.Empty(t, buf.String())
	})
}
