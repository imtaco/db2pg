package main

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

// Config holds all application configuration
type Config struct {
	Source struct {
		Type    string `mapstructure:"type"`
		ConnStr string `mapstructure:"conn_str"`
	} `mapstructure:"source"`

	Target struct {
		ConnStr string `mapstructure:"conn_str"`
		Schema  string `mapstructure:"schema"`
	} `mapstructure:"target"`

	AllTables       bool     `mapstructure:"all_tables"`
	Tables          []string `mapstructure:"tables"`
	Workers         int      `mapstructure:"workers"`
	BatchSizeMB     int      `mapstructure:"batch_size_mb"`
	PrefetchBatches int      `mapstructure:"prefetch_batches"`
	LogLevel        string   `mapstructure:"log_level"`
}

// LoadConfig loads configuration from environment variables, config file, and flags
func LoadConfig() (*Config, error) {
	v := viper.New()

	// Set defaults
	v.SetDefault("source.type", "mssql")
	v.SetDefault("target.schema", "public")
	v.SetDefault("workers", 1)
	v.SetDefault("batch_size_mb", 10)
	v.SetDefault("prefetch_batches", 3)
	v.SetDefault("log_level", "INFO")

	// Enable environment variable reading
	v.SetEnvPrefix("DB2PG")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Map legacy environment variables
	v.BindEnv("source.url", "MSSQL_URL")
	v.BindEnv("target.url", "PG_URL")
	v.BindEnv("target.schema", "PG_SCHEMA")
	v.BindEnv("workers", "WORKERS")
	v.BindEnv("batch_size_mb", "BATCH_SIZE_MB")
	v.BindEnv("prefetch_batches", "PREFETCH_BATCHES")
	v.BindEnv("log_level", "LOG_LEVEL")
	v.BindEnv("tables", "TABLES")

	// Try to read config file (optional)
	v.SetConfigName("db2pg")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")

	// Read config file if it exists (don't error if not found)
	_ = v.ReadInConfig()

	// Unmarshal into config struct
	var config Config
	if err := v.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Parse tables string if provided via environment
	if tablesStr := v.GetString("tables"); tablesStr != "" && len(config.Tables) == 0 {
		for _, t := range strings.Split(tablesStr, ",") {
			t = strings.TrimSpace(t)
			if t != "" {
				config.Tables = append(config.Tables, t)
			}
		}
	}

	return &config, nil
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.Source.ConnStr == "" {
		return fmt.Errorf("source connection string is required (set MSSQL_URL or DB2PG_SOURCE_CONN_STR)")
	}
	if c.Target.ConnStr == "" {
		return fmt.Errorf("target connection string is required (set PG_URL or DB2PG_TARGET_CONN_STR)")
	}
	if !c.AllTables && len(c.Tables) == 0 {
		return fmt.Errorf("no tables to migrate (set all_tables: true or specify tables)")
	}
	if c.Workers < 1 {
		c.Workers = 1
	}
	return nil
}
