package source

import (
	"context"
	"database/sql"
)

// SourceDB defines the interface for source database operations
type SourceDB interface {
	// Connect establishes a connection to the source database
	Connect(ctx context.Context, connStr string) error

	// Close closes the database connection
	Close() error

	// Ping verifies the connection to the database
	Ping(ctx context.Context) error

	// GetTableInfo retrieves table structure information
	GetTableInfo(ctx context.Context, tableName string) (*TableInfo, error)

	// GetTableDependencies returns all tables and their foreign key dependencies
	GetTableDependencies(ctx context.Context) ([]string, map[string][]string, error)

	// QueryRows executes a query and returns rows for a table
	QueryRows(ctx context.Context, tableName string, columns []string) (*sql.Rows, error)

	// ConvertValueForCSV converts database-specific values to PostgreSQL CSV format
	ConvertValueForCSV(value any, dataType string) string
}

type ColumnInfo struct {
	Name     string
	DataType string
}

// TableInfo holds table structure information
type TableInfo struct {
	Name    string
	Columns []ColumnInfo
}
