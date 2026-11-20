package qb_test

import (
	"context"
	stdsql "database/sql"
	"io"
	"math"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/config"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/datasource/sql/qb"
	"github.com/sllt/pi/pkg/pi/logging"
)

func openTestSQLite(t *testing.T) *piSQL.DB {
	t.Helper()

	cfg := config.NewMockConfig(map[string]string{
		"DB_DIALECT": "sqlite",
		"DB_NAME":    filepath.Join(t.TempDir(), "qb_query.db"),
	})
	logger := logging.NewWriterLogger(logging.ERROR, io.Discard, io.Discard)
	db, err := piSQL.OpenContext(t.Context(), cfg, logger, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type countingExecutor struct {
	inner     qb.Executor
	dialect   string
	countRows int
	selects   int
	lastQuery string
	lastArgs  []any
}

func (c *countingExecutor) Dialect() string {
	return c.dialect
}

func (c *countingExecutor) QueryRowContext(ctx context.Context, query string, args ...any) *stdsql.Row {
	c.countRows++
	c.lastQuery = query
	c.lastArgs = args
	return c.inner.QueryRowContext(ctx, query, args...)
}

func (c *countingExecutor) Select(ctx context.Context, data any, query string, args ...any) error {
	c.selects++
	c.lastQuery = query
	c.lastArgs = args
	return c.inner.Select(ctx, data, query, args...)
}

func TestQuery_ToSQLAndToCountSQL_PostgresAndMySQL(t *testing.T) {
	base := qb.From("posts p").
		Dialect("postgres").
		Select("p.id", "p.title", "u.name AS author_name").
		Join("users u ON u.id = p.author_id AND u.tenant_id = ?", 7).
		LeftJoin("categories c ON c.id = p.category_id").
		Where("p.status = ?", "published").
		Where("p.meta ?? ?", "featured").
		Where("p.note <> 'why?'").
		WhereIf(false, "p.deleted_at IS NOT NULL").
		WhereIf(true, "p.views >= ?", 100).
		WhereIn("p.id", []int64{10, 20, 30}).
		WhereInIf(false, "p.tag", []string(nil)).
		WhereInIf(true, "p.tag", []string{"go", "pi"}).
		OrderBy("p.created_at DESC", "p.id DESC").
		Page(0, 15) // page < 1 clamped to 1 -> LIMIT 15 OFFSET 0

	sqlStr, args, err := base.ToSQL()
	require.NoError(t, err)
	assert.Equal(t,
		"SELECT p.id, p.title, u.name AS author_name FROM posts p "+
			"JOIN users u ON u.id = p.author_id AND u.tenant_id = $1 "+
			"LEFT JOIN categories c ON c.id = p.category_id "+
			"WHERE (p.status = $2) AND (p.meta ? $3) AND (p.note <> 'why?') AND (p.views >= $4) AND (p.id IN ($5,$6,$7)) AND (p.tag IN ($8,$9)) "+
			"ORDER BY p.created_at DESC, p.id DESC LIMIT $10 OFFSET $11",
		sqlStr,
	)
	assert.Equal(t, []any{7, "published", "featured", 100, int64(10), int64(20), int64(30), "go", "pi", 15, 0}, args)

	// ToCountSQL strips ORDER BY / LIMIT / OFFSET and rebinds Postgres placeholders cleanly.
	countSQL, countArgs, err := base.ToCountSQL()
	require.NoError(t, err)
	assert.Equal(t,
		"SELECT COUNT(1) FROM posts p "+
			"JOIN users u ON u.id = p.author_id AND u.tenant_id = $1 "+
			"LEFT JOIN categories c ON c.id = p.category_id "+
			"WHERE (p.status = $2) AND (p.meta ? $3) AND (p.note <> 'why?') AND (p.views >= $4) AND (p.id IN ($5,$6,$7)) AND (p.tag IN ($8,$9))",
		countSQL,
	)
	assert.Equal(t, []any{7, "published", "featured", 100, int64(10), int64(20), int64(30), "go", "pi"}, countArgs)

	// Clone independence
	forked := base.Clone().Dialect("mysql").Page(3, 10)
	forkSQL, forkArgs, err := forked.ToSQL()
	require.NoError(t, err)
	assert.Contains(t, forkSQL, "LIMIT ? OFFSET ?")
	assert.Equal(t, 10, forkArgs[len(forkArgs)-2])
	assert.Equal(t, 20, forkArgs[len(forkArgs)-1])

	// Original remains unchanged
	origSQL, origArgs, err := base.ToSQL()
	require.NoError(t, err)
	assert.Contains(t, origSQL, "LIMIT $10 OFFSET $11")
	assert.Equal(t, 15, origArgs[len(origArgs)-2])
	assert.Equal(t, 0, origArgs[len(origArgs)-1])
}

func TestQuery_GroupByHavingToCountSQL(t *testing.T) {
	q := qb.From("posts p").
		Dialect("postgres").
		Select("p.author_id", "COUNT(1) AS post_count").
		Where("p.status = ?", "published").
		GroupBy("p.author_id").
		Having("COUNT(1) >= ?", 2).
		OrderBy("post_count DESC").
		Limit(10).
		Offset(5)

	countSQL, args, err := q.ToCountSQL()
	require.NoError(t, err)
	assert.Equal(t,
		"SELECT COUNT(1) FROM (SELECT p.author_id, COUNT(1) AS post_count FROM posts p WHERE (p.status = $1) GROUP BY p.author_id HAVING (COUNT(1) >= $2)) AS _qb_count",
		countSQL,
	)
	assert.Equal(t, []any{"published", 2}, args)
}

func TestQuery_ValidationErrors(t *testing.T) {
	_, _, err := qb.From("").ToSQL()
	require.Error(t, err)

	_, _, err = qb.From("posts").WhereIn("id", []int64{}).ToSQL()
	require.Error(t, err)

	_, _, err = qb.From("posts").WhereIn("id", "not-a-slice").ToSQL()
	require.Error(t, err)

	_, _, err = qb.From("posts").WhereIn("id", []byte("abc")).ToSQL()
	require.Error(t, err)

	_, _, err = qb.From("posts").Page(1, 0).ToSQL()
	require.Error(t, err)

	_, _, err = qb.From("posts").Page(math.MaxInt, 2).ToSQL()
	require.Error(t, err)

	_, _, err = qb.From("posts").Limit(-1).ToSQL()
	require.Error(t, err)

	_, _, err = qb.From("posts").Offset(-1).ToSQL()
	require.Error(t, err)

	_, _, err = qb.From("posts").Dialect("oracle").ToSQL()
	require.Error(t, err)
}

func TestQuery_EndToEndSQLite_ScanFirstCountScanPageAndTx(t *testing.T) {
	db := openTestSQLite(t)
	ctx := t.Context()

	_, err := db.ExecContext(ctx, `
		CREATE TABLE authors (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL
		);
		CREATE TABLE posts (
			id INTEGER PRIMARY KEY,
			author_id INTEGER NOT NULL,
			title TEXT NOT NULL,
			status TEXT NOT NULL,
			published INTEGER NOT NULL
		);
		CREATE TABLE post_tags (
			post_id INTEGER NOT NULL,
			tag TEXT NOT NULL
		);
	`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `INSERT INTO authors (id, name) VALUES (1, 'Alice'), (2, 'Bob')`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `
		INSERT INTO posts (id, author_id, title, status, published) VALUES
		(1, 1, 'Post One', 'published', 1),
		(2, 1, 'Post Two', 'published', 1),
		(3, 2, 'Post Three', 'published', 1),
		(4, 2, 'Draft Four', 'draft', 0)
	`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO post_tags (post_id, tag) VALUES (1, 'go'), (2, 'go'), (3, 'sql'), (4, 'go')`)
	require.NoError(t, err)

	type postView struct {
		ID         int64
		Title      string
		AuthorName string
		Published  bool
	}

	// 1. ScanPage with table alias, Join, EXISTS subquery, WhereIf, WhereInIf, Page
	tagFilter := "go"
	authorIDs := []int64{1, 2}
	q := qb.From("posts p").
		Select("p.id", "p.title", "a.name AS author_name", "p.published").
		Join("authors a ON a.id = p.author_id").
		Where("p.status = ?", "published").
		WhereIf(tagFilter != "", "EXISTS (SELECT 1 FROM post_tags t WHERE t.post_id = p.id AND t.tag = ?)", tagFilter).
		WhereInIf(len(authorIDs) > 0, "p.author_id", authorIDs).
		OrderBy("p.id ASC").
		Page(1, 1)

	counter := &countingExecutor{inner: db, dialect: db.Dialect()}
	var page1 []postView
	total, err := q.ScanPage(ctx, counter, &page1)
	require.NoError(t, err)
	assert.Equal(t, int64(2), total)
	assert.Equal(t, 1, counter.countRows)
	assert.Equal(t, 1, counter.selects)
	require.Len(t, page1, 1)
	assert.Equal(t, postView{ID: 1, Title: "Post One", AuthorName: "Alice", Published: true}, page1[0])

	// 2. ScanPage with total == 0 skips second page query and sets nil/stale slice to empty non-nil slice
	emptyCounter := &countingExecutor{inner: db, dialect: db.Dialect()}
	var emptyPosts []postView
	total, err = qb.From("posts p").
		Where("p.status = ?", "archived").
		Page(1, 10).
		ScanPage(ctx, emptyCounter, &emptyPosts)
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.Equal(t, 1, emptyCounter.countRows)
	assert.Equal(t, 0, emptyCounter.selects)
	assert.NotNil(t, emptyPosts)
	assert.Empty(t, emptyPosts)

	// Reused pre-populated slice is cleared when total == 0
	stalePosts := []postView{{ID: 99, Title: "Stale"}}
	total, err = qb.From("posts p").
		Where("p.status = ?", "archived").
		Page(1, 10).
		ScanPage(ctx, emptyCounter, &stalePosts)
	require.NoError(t, err)
	assert.Equal(t, int64(0), total)
	assert.NotNil(t, stalePosts)
	assert.Empty(t, stalePosts)

	// Invalid dest fails up front before running Count
	invalidCounter := &countingExecutor{inner: db, dialect: db.Dialect()}
	_, err = qb.From("posts p").ScanPage(ctx, invalidCounter, nil)
	require.Error(t, err)
	assert.Equal(t, 0, invalidCounter.countRows)

	var singleStruct postView
	_, err = qb.From("posts p").ScanPage(ctx, invalidCounter, &singleStruct)
	require.Error(t, err)
	assert.Equal(t, 0, invalidCounter.countRows)

	// Offset without Limit works on real SQLite (LIMIT -1 OFFSET ?)
	var offsetPosts []postView
	err = qb.From("posts p").
		Select("p.id", "p.title", "a.name AS author_name", "p.published").
		Join("authors a ON a.id = p.author_id").
		Where("p.status = ?", "published").
		OrderBy("p.id ASC").
		Offset(1).
		Scan(ctx, db, &offsetPosts)
	require.NoError(t, err)
	require.Len(t, offsetPosts, 2)
	assert.Equal(t, int64(2), offsetPosts[0].ID)
	assert.Equal(t, int64(3), offsetPosts[1].ID)

	// DISTINCT Count on real SQLite with JOIN (avoids over-counting joined rows)
	distinctCount, err := qb.From("posts p").
		Select("DISTINCT p.author_id").
		Join("post_tags t ON t.post_id = p.id").
		Where("p.status = ?", "published").
		Count(ctx, db)
	require.NoError(t, err)
	assert.Equal(t, int64(2), distinctCount)

	// 3. First returns single struct and does not mutate base query limit
	basePub := qb.From("posts p").
		Select("p.id", "p.title", "a.name AS author_name", "p.published").
		Join("authors a ON a.id = p.author_id").
		Where("p.status = ?", "published").
		OrderBy("p.id DESC")

	var latest postView
	err = basePub.First(ctx, db, &latest)
	require.NoError(t, err)
	assert.Equal(t, postView{ID: 3, Title: "Post Three", AuthorName: "Bob", Published: true}, latest)

	var allPub []*postView
	err = basePub.Scan(ctx, db, &allPub)
	require.NoError(t, err)
	require.Len(t, allPub, 3)

	// 4. GROUP BY + HAVING Count on real SQLite
	groupedCount, err := qb.From("posts p").
		Select("p.author_id", "COUNT(1) AS cnt").
		Where("p.status = ?", "published").
		GroupBy("p.author_id").
		Having("COUNT(1) >= ?", 2).
		Count(ctx, db)
	require.NoError(t, err)
	assert.Equal(t, int64(1), groupedCount)

	// 5. Works seamlessly inside *piSQL.Tx
	tx, err := db.BeginTxContext(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	txTotal, err := qb.From("posts p").Where("p.published = ?", 1).Count(ctx, tx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), txTotal)
}

func TestQuery_AutoDetectsPostgresDialectFromExecutor(t *testing.T) {
	db := openTestSQLite(t)
	pgExec := &countingExecutor{inner: db, dialect: "postgres"}

	// Even when q.Dialect(...) is not set, ScanPage/Count detect postgres from exec.Dialect().
	var dest []struct {
		ID int64
	}
	_, _ = qb.From("posts p").Where("p.status = ?", "published").ScanPage(t.Context(), pgExec, &dest)
	assert.Equal(t, "SELECT COUNT(1) FROM posts p WHERE (p.status = $1)", pgExec.lastQuery)
}

func TestQuery_OffsetWithoutLimitAndDistinctCountSQL(t *testing.T) {
	sqliteSQL, sqliteArgs, err := qb.From("posts").Dialect("sqlite").OrderBy("id ASC").Offset(5).ToSQL()
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM posts ORDER BY id ASC LIMIT -1 OFFSET ?", sqliteSQL)
	assert.Equal(t, []any{5}, sqliteArgs)

	mysqlSQL, mysqlArgs, err := qb.From("posts").Dialect("mysql").OrderBy("id ASC").Offset(5).ToSQL()
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM posts ORDER BY id ASC LIMIT 18446744073709551615 OFFSET ?", mysqlSQL)
	assert.Equal(t, []any{5}, mysqlArgs)

	pgSQL, pgArgs, err := qb.From("posts").Dialect("postgres").Where("status = ?", "ok").OrderBy("id ASC").Offset(5).ToSQL()
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM posts WHERE (status = $1) ORDER BY id ASC OFFSET $2", pgSQL)
	assert.Equal(t, []any{"ok", 5}, pgArgs)

	// DISTINCT wraps ToCountSQL in subquery
	distinctSQL, distinctArgs, err := qb.From("posts p").
		Dialect("postgres").
		Select("DISTINCT p.id").
		Join("post_tags t ON t.post_id = p.id").
		Where("t.tag = ?", "go").
		OrderBy("p.id DESC").
		Limit(10).
		ToCountSQL()
	require.NoError(t, err)
	assert.Equal(t, "SELECT COUNT(1) FROM (SELECT DISTINCT p.id FROM posts p JOIN post_tags t ON t.post_id = p.id WHERE (t.tag = $1)) AS _qb_count", distinctSQL)
	assert.Equal(t, []any{"go"}, distinctArgs)

	// Column name containing "distinct" as identifier substring does NOT wrap
	nonDistinctSQL, _, err := qb.From("posts p").Select("p.is_distinct").ToCountSQL()
	require.NoError(t, err)
	assert.Equal(t, "SELECT COUNT(1) FROM posts p", nonDistinctSQL)
}
