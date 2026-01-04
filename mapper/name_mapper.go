package mapper

import (
	"strings"

	"github.com/iancoleman/strcase"
)

// NameMapper defines the interface for mapping table and column names
type NameMapper interface {
	// MapTableName maps source table name to target table name
	MapTableName(sourceTableName string) string

	// MapColumnName maps source column name to target column name for a specific table
	MapColumnName(tableName string, sourceColumnName string) string
}

// DefaultNameMapper provides a default implementation that doesn't change names
type DefaultNameMapper struct{}

// NewDefaultNameMapper creates a new default name mapper
func NewDefaultNameMapper() *DefaultNameMapper {
	return &DefaultNameMapper{}
}

// MapTableName returns the table name unchanged
func (m *DefaultNameMapper) MapTableName(sourceTableName string) string {
	return sourceTableName
}

// MapColumnName returns the column name unchanged
func (m *DefaultNameMapper) MapColumnName(tableName string, sourceColumnName string) string {
	return sourceColumnName
}

// CustomNameMapper allows users to define custom mappings for tables and columns
type CustomNameMapper struct {
	// TableMappings maps source table names to target table names
	TableMappings map[string]string

	// ColumnMappings maps table names to their column mappings
	// Format: map[tableName]map[sourceColumnName]targetColumnName
	ColumnMappings map[string]map[string]string

	// TableNameTransformer is an optional function to transform table names
	// Applied after checking TableMappings
	TableNameTransformer func(string) string

	// ColumnNameTransformer is an optional function to transform column names
	// Applied after checking ColumnMappings
	ColumnNameTransformer func(tableName, columnName string) string
}

// NewCustomNameMapper creates a new custom name mapper
func NewCustomNameMapper() *CustomNameMapper {
	return &CustomNameMapper{
		TableMappings:  make(map[string]string),
		ColumnMappings: make(map[string]map[string]string),
	}
}

// MapTableName maps source table name to target table name
func (m *CustomNameMapper) MapTableName(sourceTableName string) string {
	// Check explicit mapping first
	if mapped, ok := m.TableMappings[sourceTableName]; ok {
		return mapped
	}

	// Apply transformer if available
	if m.TableNameTransformer != nil {
		return m.TableNameTransformer(sourceTableName)
	}

	// Return original name
	return sourceTableName
}

// MapColumnName maps source column name to target column name for a specific table
func (m *CustomNameMapper) MapColumnName(tableName string, sourceColumnName string) string {
	// Check table-specific column mapping first
	if tableMappings, ok := m.ColumnMappings[tableName]; ok {
		if mapped, ok := tableMappings[sourceColumnName]; ok {
			return mapped
		}
	}

	// Apply transformer if available
	if m.ColumnNameTransformer != nil {
		return m.ColumnNameTransformer(tableName, sourceColumnName)
	}

	// Return original name
	return sourceColumnName
}

// AddTableMapping adds a table name mapping
func (m *CustomNameMapper) AddTableMapping(source, target string) {
	m.TableMappings[source] = target
}

// AddColumnMapping adds a column name mapping for a specific table
func (m *CustomNameMapper) AddColumnMapping(tableName, sourceColumn, targetColumn string) {
	if m.ColumnMappings[tableName] == nil {
		m.ColumnMappings[tableName] = make(map[string]string)
	}
	m.ColumnMappings[tableName][sourceColumn] = targetColumn
}

// SetTableNameTransformer sets a function to transform all table names
func (m *CustomNameMapper) SetTableNameTransformer(transformer func(string) string) {
	m.TableNameTransformer = transformer
}

// SetColumnNameTransformer sets a function to transform all column names
func (m *CustomNameMapper) SetColumnNameTransformer(transformer func(tableName, columnName string) string) {
	m.ColumnNameTransformer = transformer
}

// Common transformers that can be used

// ToLowerCaseTransformer converts names to lowercase
func ToLowerCaseTransformer(name string) string {
	return strings.ToLower(name)
}

// ToUpperCaseTransformer converts names to uppercase
func ToUpperCaseTransformer(name string) string {
	return strings.ToUpper(name)
}

// ToSnakeCaseTransformer converts PascalCase/camelCase to snake_case
// Uses the strcase library for proper conversion
func ToSnakeCaseTransformer(name string) string {
	return strcase.ToSnake(name)
}

// ToCamelCaseTransformer converts names to camelCase
func ToCamelCaseTransformer(name string) string {
	return strcase.ToLowerCamel(name)
}

// ToPascalCaseTransformer converts names to PascalCase
func ToPascalCaseTransformer(name string) string {
	return strcase.ToCamel(name)
}

// ToKebabCaseTransformer converts names to kebab-case
func ToKebabCaseTransformer(name string) string {
	return strcase.ToKebab(name)
}
