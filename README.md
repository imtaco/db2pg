# db2pg - Database Migration Tool

Fast, parallel database migration tool from MSSQL to PostgreSQL using streaming COPY.

## Features

- Parallel table migration - Migrate multiple tables concurrently
- Dependency-aware - Automatically respects foreign key dependencies
- Streaming mode - Row-by-row streaming with minimal memory usage
- Smart filtering - Automatically excludes MSSQL system tables
- Name mapping - Flexible table and column name transformations
- Configurable - YAML config files, environment variables, or both

## Installation

```bash
go build
```

## Configuration

### Option 1: YAML Configuration File

Create `db2pg.yaml`:

```yaml
source:
  type: mssql
  conn_str: "sqlserver://user:pass@host:1433?database=mydb"

target:
  conn_str: "postgres://user:pass@host:5432/mydb"
  schema: public

all_tables: true
workers: 4
batch_size_mb: 10
prefetch_batches: 3
```

Run:
```bash
./db2pg
```

### Option 2: Environment Variables

```bash
export MSSQL_URL="sqlserver://user:pass@host:1433?database=mydb"
export PG_URL="postgres://user:pass@host:5432/mydb"
export PG_SCHEMA="public"
export WORKERS=4
export TABLES="users,orders,products"

./db2pg
```

### Option 3: Mix Both

Environment variables override config file settings.

## Examples

### Migrate All Tables

```bash
export MSSQL_URL="sqlserver://sa:password@localhost:1433?database=mydb"
export PG_URL="postgres://postgres:password@localhost:5432/mydb"
./db2pg
```

### Migrate Specific Tables

```bash
export TABLES="users,orders,products"
./db2pg
```

### High Performance Migration

```bash
export WORKERS=8
export BATCH_SIZE_MB=50
export PREFETCH_BATCHES=5
./db2pg
```
