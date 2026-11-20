package testkit

import (
	"context"
	"os"
	"path/filepath"

	"github.com/sllt/pi/pkg/pi/config"
	piSQL "github.com/sllt/pi/pkg/pi/datasource/sql"
	"github.com/sllt/pi/pkg/pi/logging"
	"github.com/sllt/pi/pkg/pi/migration"
)

type tempDirTB interface {
	TempDir() string
}

// NewSQLite opens an isolated temporary SQLite *piSQL.DB, optionally runs the
// supplied Pi migration maps in version order, and closes/cleans up at test end.
func NewSQLite(t TB, migrations ...map[int64]migration.Migrate) *piSQL.DB {
	t.Helper()

	var dir string
	if td, ok := t.(tempDirTB); ok {
		dir = td.TempDir()
	} else {
		var err error
		dir, err = os.MkdirTemp("", "pi-testkit-sqlite-*")
		if err != nil {
			t.Fatalf("testkit: create temp dir for sqlite: %v", err)
			return nil
		}
		t.Cleanup(func() {
			_ = os.RemoveAll(dir)
		})
	}

	logger := logging.Noop()
	cfg := config.NewSnapshot(map[string]string{
		"DB_DIALECT": "sqlite",
		"DB_NAME":    filepath.Join(dir, "test.db"),
	})

	db, err := piSQL.OpenContext(context.Background(), cfg, logger, nil)
	if err != nil {
		t.Fatalf("testkit: open sqlite db: %v", err)
		return nil
	}
	t.Cleanup(func() {
		if closeErr := db.Close(); closeErr != nil {
			t.Errorf("testkit: close sqlite db: %v", closeErr)
		}
	})

	if len(migrations) > 0 {
		merged := make(map[int64]migration.Migrate)
		for _, m := range migrations {
			for version, mig := range m {
				if _, exists := merged[version]; exists {
					t.Fatalf("testkit: duplicate sqlite migration version %d", version)
					return nil
				}
				merged[version] = mig
			}
		}
		if len(merged) > 0 {
			deps := migration.Deps{
				Logger: logger,
				SQL:    db,
			}
			if _, runErr := migration.Run(context.Background(), merged, deps); runErr != nil {
				t.Fatalf("testkit: run sqlite migrations: %v", runErr)
				return nil
			}
		}
	}

	return db
}
