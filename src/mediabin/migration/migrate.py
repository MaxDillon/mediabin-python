#!/usr/bin/env python3
import argparse
import duckdb
import re
import sys
from pathlib import Path
from datetime import datetime, timezone
import shutil

# Migrations directory is project_root/migrations
MIGRATIONS_DIR = (Path(__file__).parent / "versions").resolve()

MIGRATION_RE = re.compile(r"^(\d+)_.*_(up|down)\.sql$")

def get_migration_files():
    """Return dict: {version: {'up': Path, 'down': Path}}"""
    migrations = {}
    for f in sorted(MIGRATIONS_DIR.glob("*.sql")):
        m = MIGRATION_RE.match(f.name)
        if m:
            version = int(m.group(1))
            direction = m.group(2)
            if version not in migrations:
                migrations[version] = {}
            migrations[version][direction] = f
    return migrations

def ensure_schema_table(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS _schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

def db_has_user_data(conn: duckdb.DuckDBPyConnection) -> bool:
    """Return True if any non-_schema_migrations table contains rows."""
    tables = conn.execute("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_name != '_schema_migrations'
          AND table_schema NOT IN ('information_schema')
    """).fetchall()

    for schema, table in tables:
        full_name = f'"{schema}"."{table}"' if schema != 'main' else f'"{table}"'
        try:
            row = conn.execute(f"SELECT 1 FROM {full_name} LIMIT 1").fetchone()
            if row is not None:
                return True  # found at least one row somewhere
        except Exception:
            # If table is inaccessible for some reason, ignore it
            continue
    return False

def get_current_version(conn: duckdb.DuckDBPyConnection):
    result = conn.execute("SELECT max(version) FROM _schema_migrations").fetchone()
    return result[0] if result and result[0] is not None else 0

def apply_migration(conn, version, file_path, direction):
    print(f"Applying {direction} migration {version} from {file_path.name}...")
    sql = file_path.read_text(encoding="utf-8")
    conn.execute("BEGIN")
    try:
        conn.execute(sql)
        if direction == "up":
            conn.execute(
                "INSERT INTO _schema_migrations (version, applied_at) VALUES (?, ?)",
                (version, datetime.now(tz=timezone.utc)),
            )
        elif direction == "down":
            conn.execute(
                "DELETE FROM _schema_migrations WHERE version = ?",
                (version,),
            )
        conn.execute("COMMIT")
    except Exception as e:
        conn.execute("ROLLBACK")
        print(f"❌ Failed to apply {direction} migration {file_path.name}: {e}", file=sys.stderr)
        sys.exit(1)

def migrate_up_to(conn, migrations, current_version, target_version):
    to_apply = [v for v in sorted(migrations.keys()) if current_version < v <= target_version]
    for v in to_apply:
        if "up" not in migrations[v]:
            print(f"❌ Missing up migration for version {v}", file=sys.stderr)
            sys.exit(1)
        apply_migration(conn, v, migrations[v]["up"], "up")

def migrate_down_to(conn, migrations, current_version, target_version):
    to_apply = [v for v in sorted(migrations.keys(), reverse=True) if target_version < v <= current_version]
    for v in to_apply:
        if "down" not in migrations[v]:
            print(f"❌ Missing down migration for version {v}", file=sys.stderr)
            sys.exit(1)
        apply_migration(conn, v, migrations[v]["down"], "down")

def migrate_to_version(db_path: str | Path, target_version):
    """Migrate the database at db_path to target_version.

    db_path can be either a Path object or a duckdb.DuckDBPyConnection object.
    """
    db_file_path = Path(db_path)
    conn = duckdb.connect(str(db_file_path))
    ensure_schema_table(conn)

    current_version = get_current_version(conn)
    print(f"Current schema version: {current_version}")
    has_user_data = db_has_user_data(conn)

    if current_version != 0 and db_file_path.name != ":memory:" and has_user_data:
        backup_file_name = f"{db_file_path}.bak"
        backup_path = db_file_path.parent / backup_file_name
        try:
            shutil.copy2(db_file_path, backup_path)
            print(f"Created database backup at {backup_path}")
        except Exception as e:
            print(f"❌ Failed to create database backup: {e}", file=sys.stderr)

    migrations = get_migration_files()

    if target_version > current_version:
        migrate_up_to(conn, migrations, current_version, target_version)
    elif target_version < current_version:
        migrate_down_to(conn, migrations, current_version, target_version)
    else:
        print("No migrations to apply.")

    print(f"✅ Database migrated to version {target_version}")
    conn.close()

def get_hightest_version():
    migrations = get_migration_files()
    highest_version = max(migrations.keys()) if migrations else 0
    return highest_version


def main():
    parser = argparse.ArgumentParser(description="DuckDB schema migration tool")
    parser.add_argument("db", type=str, help="Path to DuckDB database file, or in-memory ':memory:'")
    parser.add_argument("version", type=str, help="Target schema version (e.g., '3' or 'head')")
    args = parser.parse_args()

    # The CLI always works with a path or ':memory:' string, so we can always connect
    conn = duckdb.connect(args.db)
    ensure_schema_table(conn)
    highest_version = get_hightest_version()

    if args.version.lower() == "head":
        target_version = highest_version
        print(f"Migrating to head (version {target_version})")
    else:
        try:
            target_version = int(args.version)
        except ValueError:
            print(f"❌ Invalid version: {args.version}. Must be an integer or 'head'.", file=sys.stderr)
            sys.exit(1)
        if target_version < 0:
            print(f"❌ Invalid version: {target_version}. Version cannot be negative.", file=sys.stderr)
            sys.exit(1)
        if target_version > highest_version:
            print(f"❌ Target version {target_version} is higher than the highest available migration {highest_version}.", file=sys.stderr)
            sys.exit(1)
    
    if not Path(args.db).exists() and args.db != ":memory:":
        print(f"Creating new database at {args.db}")
    migrate_to_version(args.db, target_version)

if __name__ == "__main__":
    main()
