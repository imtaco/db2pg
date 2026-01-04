package main

import (
	"context"
	"log"

	_ "github.com/lib/pq"

	"github.com/imtaco/db2pg/mapper"
	"github.com/imtaco/db2pg/migration"
	"github.com/imtaco/db2pg/source"
	"github.com/imtaco/db2pg/source/mssql"
)

func main() {
	// Load configuration
	config, err := LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	// Initialize source database based on type
	var sourceDB source.SourceDB
	switch config.Source.Type {
	case "mssql":
		sourceDB = mssql.New()
	default:
		log.Fatalf("Unsupported source database type: %s", config.Source.Type)
	}

	// Initialize name mapper (can be customized)
	nameMapper := mapper.NewDefaultNameMapper()
	// Example of custom mapping (uncomment to use):
	// nameMapper := NewCustomNameMapper()
	// nameMapper.AddTableMapping("OldTableName", "new_table_name")
	// nameMapper.AddColumnMapping("TableName", "OldColumn", "new_column")
	// nameMapper.SetTableNameTransformer(ToSnakeCaseTransformer)

	// Build migration configuration
	migrationConfig := &migration.Config{
		SourceConnStr:   config.Source.ConnStr,
		TargetConnStr:   config.Target.ConnStr,
		TargetSchema:    config.Target.Schema,
		AllTables:       config.AllTables,
		TargetTables:    config.Tables,
		Workers:         config.Workers,
		BatchSizeBytes:  config.BatchSizeMB * 1024 * 1024,
		PrefetchBatches: config.PrefetchBatches,
		SourceDB:        sourceDB,
		NameMapper:      nameMapper,
	}

	// Execute migration
	ctx := context.Background()
	if err := migration.RunMigration(ctx, migrationConfig); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}
}
