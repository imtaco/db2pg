package migration

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/imtaco/db2pg/mapper"
	"github.com/imtaco/db2pg/source"
)

// MigrationConfig holds all configuration for the migration
type Config struct {
	SourceConnStr   string
	TargetConnStr   string
	TargetSchema    string
	AllTables       bool
	TargetTables    []string
	Workers         int
	BatchSizeBytes  int
	PrefetchBatches int
	SourceDB        source.SourceDB
	NameMapper      mapper.NameMapper
}

// CSVBatch holds a batch of CSV-formatted data ready for COPY
type CSVBatch struct {
	data     *bytes.Buffer // CSV data ready to write
	rowCount int64         // Number of rows in this batch
	err      error         // Error if batch processing failed
}

// RunMigration executes the database migration with the given configuration
func RunMigration(ctx context.Context, config *Config) error {
	log.Println("Using ROW-BY-ROW STREAMING mode: Data will be piped directly from source DB to PostgreSQL via io.Pipe")

	// Connect to source database
	if err := config.SourceDB.Connect(ctx, config.SourceConnStr); err != nil {
		return fmt.Errorf("failed to connect to source database: %w", err)
	}
	defer config.SourceDB.Close()

	if err := config.SourceDB.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping source database: %w", err)
	}

	// Group tables by dependency level for parallel processing
	tableLevels, err := getTablesByDependencyLevel(ctx, config.SourceDB, config.AllTables, config.TargetTables)
	if err != nil {
		return fmt.Errorf("failed to get table dependency levels: %w", err)
	}

	log.Printf("Found %d dependency levels", len(tableLevels))
	for i, level := range tableLevels {
		log.Printf("Level %d: %s", i, strings.Join(level, ", "))
	}

	// Validate workers setting
	if config.Workers < 1 {
		config.Workers = 1
	}
	log.Printf("Using %d concurrent workers for table migration", config.Workers)

	// Migrate tables level by level (respecting dependencies)
	totalStart := time.Now()
	for levelIdx, level := range tableLevels {
		if len(level) == 0 {
			continue
		}

		log.Printf("\n=== Processing Level %d: %d tables ===", levelIdx, len(level))

		// Use worker pool to process tables at this level in parallel
		if err := migrateTablesParallel(
			ctx,
			config.SourceDB,
			config.NameMapper,
			config.TargetConnStr,
			config.TargetSchema,
			level,
			config.Workers,
			config.BatchSizeBytes,
			config.PrefetchBatches,
		); err != nil {
			return fmt.Errorf("failed to migrate tables at level %d: %w", levelIdx, err)
		}

		log.Printf("=== Completed Level %d ===\n", levelIdx)
	}

	totalElapsed := time.Since(totalStart)
	log.Printf("Fast data migration completed successfully in %.2f seconds!", totalElapsed.Seconds())

	return nil
}

// migrateTablesParallel migrates multiple tables in parallel using a worker pool
func migrateTablesParallel(
	ctx context.Context,
	sourceDB source.SourceDB,
	nameMapper mapper.NameMapper,
	pgConnStr string,
	pgSchema string,
	tables []string,
	workers int,
	batchSize int,
	prefetchBatches int,
) error {
	// Limit workers to the number of tables
	if workers > len(tables) {
		workers = len(tables)
	}

	// Create channels for work distribution
	tableChan := make(chan string, len(tables))
	errChan := make(chan error, len(tables))
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for tableName := range tableChan {
				log.Printf("[Worker %d] Starting migration for table: %s", workerID, tableName)
				if err := migrateTableStreaming(ctx, sourceDB, nameMapper, pgConnStr, tableName, pgSchema, batchSize, prefetchBatches); err != nil {
					errChan <- fmt.Errorf("worker %d failed on table %s: %w", workerID, tableName, err)
					return
				}
				log.Printf("[Worker %d] Completed migration for table: %s", workerID, tableName)
			}
		}(i)
	}

	// Send tables to workers
	for _, table := range tables {
		tableChan <- table
	}
	close(tableChan)

	// Wait for all workers to finish
	wg.Wait()
	close(errChan)

	// Check for errors
	if len(errChan) > 0 {
		return <-errChan
	}

	return nil
}

// migrateTableStreaming streams data directly from source DB to PostgreSQL using prefetch queue
func migrateTableStreaming(
	ctx context.Context,
	sourceDB source.SourceDB,
	nameMapper mapper.NameMapper,
	pgConnStr string,
	tableName string,
	pgSchema string,
	batchSize int,
	prefetchBatches int,
) error {
	totalMemoryMB := float64(batchSize*prefetchBatches) / (1024 * 1024)
	batchSizeMB := float64(batchSize) / (1024 * 1024)

	log.Printf("Migrating table %s using prefetch mode (batch: %.1f MB, prefetch batches: %d, total memory: %.1f MB)",
		tableName, batchSizeMB, prefetchBatches, totalMemoryMB)

	// Get table structure
	tableInfo, err := sourceDB.GetTableInfo(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get table info: %w", err)
	}

	// Build column list with mapping
	sourceColumnNames := make([]string, len(tableInfo.Columns))
	targetColumnNames := make([]string, len(tableInfo.Columns))
	for i, col := range tableInfo.Columns {
		sourceColumnNames[i] = col.Name
		targetColumnNames[i] = nameMapper.MapColumnName(tableName, col.Name)
	}

	quotedColumns := make([]string, len(targetColumnNames))
	for i, col := range targetColumnNames {
		quotedColumns[i] = fmt.Sprintf(`"%s"`, col)
	}
	columnsStr := strings.Join(quotedColumns, ", ")

	// Map table name for target
	targetTableName := nameMapper.MapTableName(tableName)

	// Create prefetch channel
	prefetchChan := make(chan *CSVBatch, prefetchBatches)

	var producerErr error
	var consumerErr error
	var producerWg sync.WaitGroup
	var totalRows atomic.Int64

	overallStart := time.Now()

	// Producer: Fetch data from source DB, convert to CSV, and send to prefetch queue
	producerWg.Add(1)
	go func() {
		defer producerWg.Done()
		defer close(prefetchChan)

		rows, err := sourceDB.QueryRows(ctx, tableName, sourceColumnNames)
		if err != nil {
			producerErr = fmt.Errorf("failed to query source database: %w", err)
			prefetchChan <- &CSVBatch{err: err}
			return
		}
		defer rows.Close()

		// Get column types from rows
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			producerErr = fmt.Errorf("failed to get column types: %w", err)
			prefetchChan <- &CSVBatch{err: err}
			return
		}

		// Prepare scan destinations
		values := make([]interface{}, len(sourceColumnNames))
		valuePtrs := make([]interface{}, len(sourceColumnNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		rowCount := int64(0)
		batchCount := 0
		lastLogTime := time.Now()
		logInterval := 10 * time.Second

		// Current batch
		currentBatch := &bytes.Buffer{}
		csvWriter := csv.NewWriter(currentBatch)
		csvWriter.Comma = ','
		batchRowCount := int64(0)

		for rows.Next() {
			if err := rows.Scan(valuePtrs...); err != nil {
				producerErr = fmt.Errorf("failed to scan row: %w", err)
				prefetchChan <- &CSVBatch{err: err}
				return
			}

			csvRow := make([]string, len(sourceColumnNames))
			for i, val := range values {
				csvRow[i] = sourceDB.ConvertValueForCSV(val, colTypes[i].DatabaseTypeName())
			}

			if err := csvWriter.Write(csvRow); err != nil {
				producerErr = fmt.Errorf("failed to write CSV row: %w", err)
				prefetchChan <- &CSVBatch{err: err}
				return
			}

			rowCount++
			batchRowCount++

			// Send batch when it reaches target size
			if currentBatch.Len() >= batchSize {
				csvWriter.Flush()
				if err := csvWriter.Error(); err != nil {
					producerErr = fmt.Errorf("CSV writer error: %w", err)
					prefetchChan <- &CSVBatch{err: err}
					return
				}

				prefetchChan <- &CSVBatch{
					data:     currentBatch,
					rowCount: batchRowCount,
					err:      nil,
				}
				batchCount++

				// Reset for next batch
				currentBatch = &bytes.Buffer{}
				csvWriter = csv.NewWriter(currentBatch)
				csvWriter.Comma = ','
				batchRowCount = 0
			}

			// Log progress periodically
			if time.Since(lastLogTime) >= logInterval {
				log.Printf("[Producer] Processed %d rows (%d batches) from %s...", rowCount, batchCount, tableName)
				lastLogTime = time.Now()
			}
		}

		// Send remaining data as final batch
		if batchRowCount > 0 {
			csvWriter.Flush()
			if err := csvWriter.Error(); err != nil {
				producerErr = fmt.Errorf("CSV writer error: %w", err)
				prefetchChan <- &CSVBatch{err: err}
				return
			}

			prefetchChan <- &CSVBatch{
				data:     currentBatch,
				rowCount: batchRowCount,
				err:      nil,
			}
			batchCount++
		}

		if err := rows.Err(); err != nil {
			producerErr = fmt.Errorf("error iterating rows: %w", err)
			prefetchChan <- &CSVBatch{err: err}
			return
		}

		totalRows.Store(rowCount)
		log.Printf("[Producer] Finished: %d total rows (%d batches) from %s", rowCount, batchCount, tableName)
	}()

	// Consumer: Read from prefetch queue and write to PostgreSQL COPY
	pgConn, err := pgx.Connect(ctx, pgConnStr)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer pgConn.Close(ctx)

	// Apply performance settings
	performanceSettings := []string{
		"SET synchronous_commit = off",
	}
	for _, setting := range performanceSettings {
		if _, err := pgConn.Exec(ctx, setting); err != nil {
			log.Printf("WARNING: Failed to apply setting '%s': %v", setting, err)
		} else {
			log.Printf("Applied setting: %s", setting)
		}
	}

	copySql := fmt.Sprintf(`COPY "%s"."%s" (%s) FROM STDIN WITH (FORMAT CSV, NULL '\N', ENCODING 'UTF8')`,
		pgSchema, targetTableName, columnsStr)

	log.Printf("[Consumer] Starting COPY operation...")

	// Use pipe to feed COPY command
	pipeReader, pipeWriter := io.Pipe()

	// Consumer goroutine: read from prefetch queue and write to pipe
	go func() {
		defer pipeWriter.Close()

		batchesReceived := 0
		for batch := range prefetchChan {
			if batch.err != nil {
				consumerErr = batch.err
				pipeWriter.CloseWithError(batch.err)
				return
			}

			_, err := pipeWriter.Write(batch.data.Bytes())
			if err != nil {
				consumerErr = fmt.Errorf("failed to write batch to pipe: %w", err)
				pipeWriter.CloseWithError(err)
				return
			}

			batchesReceived++
			if batchesReceived%10 == 0 {
				log.Printf("[Consumer] Written %d batches to PostgreSQL", batchesReceived)
			}
		}

		log.Printf("[Consumer] All batches written (%d total)", batchesReceived)
	}()

	// Execute COPY
	tag, err := pgConn.PgConn().CopyFrom(ctx, pipeReader, copySql)
	if err != nil {
		consumerErr = fmt.Errorf("failed to copy data to PostgreSQL: %w", err)
		pipeReader.Close()
		producerWg.Wait()
		return consumerErr
	}

	log.Printf("[Consumer] COPY finished: Imported %d rows to PostgreSQL", tag.RowsAffected())

	// Wait for producer to finish
	log.Println("Waiting for producer to complete...")
	producerWg.Wait()

	if producerErr != nil {
		return fmt.Errorf("producer error: %w", producerErr)
	}
	if consumerErr != nil {
		return consumerErr
	}

	overallElapsed := time.Since(overallStart).Seconds()
	finalTotal := totalRows.Load()
	rowsPerSec := float64(finalTotal) / overallElapsed
	if overallElapsed == 0 {
		rowsPerSec = 0
	}

	log.Printf("Table %s migration completed: %d rows in %.2f seconds (%.0f rows/sec)",
		tableName, finalTotal, overallElapsed, rowsPerSec)

	return nil
}
