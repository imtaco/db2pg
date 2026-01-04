package mssql

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/imtaco/db2pg/source"
)

// MSSQLSource implements the SourceDB interface for Microsoft SQL Server
type MSSQLSource struct {
	db *sql.DB
}

// NewMSSQLSource creates a new MSSQL source database instance
func New() *MSSQLSource {
	return &MSSQLSource{}
}

// Connect establishes a connection to the MSSQL database
func (m *MSSQLSource) Connect(ctx context.Context, connStr string) error {
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to MSSQL: %w", err)
	}
	m.db = db
	return nil
}

// Close closes the database connection
func (m *MSSQLSource) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// Ping verifies the connection to the database
func (m *MSSQLSource) Ping(ctx context.Context) error {
	if m.db == nil {
		return fmt.Errorf("database not connected")
	}
	return m.db.PingContext(ctx)
}

// GetTableInfo retrieves table structure from MSSQL
func (m *MSSQLSource) GetTableInfo(ctx context.Context, tableName string) (*source.TableInfo, error) {
	info := &source.TableInfo{
		Name:    tableName,
		Columns: []source.ColumnInfo{},
	}

	// Get column information
	query := `
		SELECT COLUMN_NAME, DATA_TYPE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = @p1
		ORDER BY ORDINAL_POSITION
	`
	rows, err := m.db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var col source.ColumnInfo
		if err := rows.Scan(&col.Name, &col.DataType); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		info.Columns = append(info.Columns, col)
	}

	return info, nil
}

// GetTableDependencies returns all tables and their dependencies
func (m *MSSQLSource) GetTableDependencies(ctx context.Context) ([]string, map[string][]string, error) {
	// Get all user tables, excluding system and generated tables
	query := `
		WITH x AS (
			SELECT
				s.name AS schema_name,
				t.name AS table_name,
				t.object_id,
				t.is_ms_shipped
			FROM sys.tables t
			JOIN sys.schemas s ON s.schema_id = t.schema_id
		)
		SELECT
			x.table_name
		FROM x
		WHERE
			x.schema_name NOT IN (N'sys', N'INFORMATION_SCHEMA', N'cdc')
			AND x.is_ms_shipped = 0
			AND NOT (x.schema_name = N'dbo' AND x.table_name IN (N'sysdiagrams', N'dtproperties'))
			AND NOT EXISTS (
				SELECT 1
				FROM sys.extended_properties ep
				WHERE ep.class = 1
				  AND ep.major_id = x.object_id
				  AND ep.minor_id = 0
				  AND ep.name = N'microsoft_database_tools_support'
			)
			AND x.schema_name = N'dbo'
		ORDER BY x.table_name
	`
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get tables: %w", err)
	}
	defer rows.Close()

	var allTableList []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		allTableList = append(allTableList, tableName)
	}

	// Get foreign key relationships
	fkQuery := `
		SELECT
			fk.TABLE_NAME as dependent_table,
			pk.TABLE_NAME as referenced_table
		FROM
			INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
			INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS fk
				ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME
			INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk
				ON rc.UNIQUE_CONSTRAINT_NAME = pk.CONSTRAINT_NAME
		WHERE
			fk.TABLE_SCHEMA = 'dbo'
			AND pk.TABLE_SCHEMA = 'dbo'
	`
	fkRows, err := m.db.QueryContext(ctx, fkQuery)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get foreign keys: %w", err)
	}
	defer fkRows.Close()

	// Build dependency graph: table -> list of tables it depends on
	dependencies := make(map[string][]string)
	for _, table := range allTableList {
		dependencies[strings.ToLower(table)] = []string{}
	}

	for fkRows.Next() {
		var dependent, referenced string
		if err := fkRows.Scan(&dependent, &referenced); err != nil {
			return nil, nil, fmt.Errorf("failed to scan foreign key: %w", err)
		}
		depLower := strings.ToLower(dependent)
		refLower := strings.ToLower(referenced)

		// Skip self-references
		if depLower != refLower {
			dependencies[depLower] = append(dependencies[depLower], refLower)
		}
	}

	return allTableList, dependencies, nil
}

// QueryRows executes a query and returns rows for a table
func (m *MSSQLSource) QueryRows(ctx context.Context, tableName string, columns []string) (*sql.Rows, error) {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = fmt.Sprintf(`"%s"`, col)
	}
	columnsStr := strings.Join(quotedColumns, ", ")

	query := fmt.Sprintf("SELECT %s FROM [%s] WITH (NOLOCK) OPTION (RECOMPILE)", columnsStr, tableName)
	return m.db.QueryContext(ctx, query)
}

// ConvertValueForCSV converts MSSQL values to PostgreSQL CSV format
func (m *MSSQLSource) ConvertValueForCSV(value any, dataType string) string {
	if value == nil {
		return "\\N"
	}

	dataTypeUpper := strings.ToUpper(dataType)

	switch v := value.(type) {
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case mssql.UniqueIdentifier:
		// Use the built-in String() method which handles the endianness correctly
		return strings.ToLower(v.String())
	case []byte:
		// Check if this is a UUID/UNIQUEIDENTIFIER (16 bytes)
		if strings.Contains(dataTypeUpper, "UNIQUEIDENTIFIER") && len(v) == 16 {
			// Use Scan() method to properly handle byte order conversion
			var uid mssql.UniqueIdentifier
			if err := uid.Scan(v); err == nil {
				return strings.ToLower(uid.String())
			}
			// Fallback: if Scan fails, return as hex
			return "\\x" + hex.EncodeToString(v)
		}
		// Check if this is numeric/decimal data that came as bytes
		// MSSQL sometimes returns DECIMAL, NUMERIC, MONEY types as byte arrays
		if strings.Contains(dataTypeUpper, "DECIMAL") ||
			strings.Contains(dataTypeUpper, "NUMERIC") ||
			strings.Contains(dataTypeUpper, "MONEY") ||
			strings.Contains(dataTypeUpper, "SMALLMONEY") {
			// Convert bytes to string (it's ASCII-encoded numeric text)
			return string(v)
		}
		// Handle actual binary data (varbinary, binary, image)
		return "\\x" + hex.EncodeToString(v)
	case time.Time:
		// Handle datetime types
		return v.Format(time.RFC3339Nano)
	case string:
		// Handle UNIQUEIDENTIFIER (UUID) - convert to lowercase
		if strings.Contains(dataTypeUpper, "UNIQUEIDENTIFIER") {
			return strings.ToLower(v)
		}
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}
