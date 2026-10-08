package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestForwardMigrationsBuildFreshSchema(t *testing.T) {
	databaseURL := os.Getenv("STREAMFORGE_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set STREAMFORGE_TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	database, err := sql.Open("pgx", databaseURL)
	if err != nil { t.Fatal(err) }
	defer database.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema := fmt.Sprintf("migration_test_%d", time.Now().UnixNano())
	if _, err = database.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil { t.Fatal(err) }
	t.Cleanup(func() { _, _ = database.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS `+schema+` CASCADE`) })

	paths, err := filepath.Glob(filepath.Join("..", "..", "..", "..", "db", "migrations", "*.sql"))
	if err != nil { t.Fatal(err) }
	sort.Strings(paths)
	if len(paths) != 7 { t.Fatalf("migration count=%d, want 7", len(paths)) }
	tx, err := database.BeginTx(ctx, nil)
	if err != nil { t.Fatal(err) }
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SET LOCAL search_path TO `+schema+`,public`); err != nil { t.Fatal(err) }
	for _, path := range paths {
		contents, readErr := os.ReadFile(path)
		if readErr != nil { t.Fatal(readErr) }
		migration := strings.ReplaceAll(string(contents), "CREATE EXTENSION IF NOT EXISTS pgcrypto;", "")
		if _, err = tx.ExecContext(ctx, migration); err != nil { t.Fatalf("apply %s: %v", filepath.Base(path), err) }
	}
	var sizeNullable string
	if err = tx.QueryRowContext(ctx, `SELECT is_nullable FROM information_schema.columns WHERE table_schema=$1 AND table_name='datasets' AND column_name='source_size_bytes'`, schema).Scan(&sizeNullable); err != nil { t.Fatal(err) }
	if sizeNullable != "NO" { t.Fatalf("source_size_bytes nullable=%s", sizeNullable) }
	for _, table := range []string{"datasets", "replay_runs", "trip_events", "rejected_events", "hourly_zone_stats", "run_event_outcomes"} {
		var exists bool
		if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2)`, schema, table).Scan(&exists); err != nil { t.Fatal(err) }
		if !exists { t.Fatalf("missing migrated table %s", table) }
	}
	if err = tx.Commit(); err != nil { t.Fatal(err) }
}
