package migration

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/imtaco/db2pg/source"
)

// getTablesByDependencyLevel returns tables grouped by dependency level
// Level 0 = no dependencies, Level 1 = depends on Level 0, etc.
func getTablesByDependencyLevel(
	ctx context.Context,
	sourceDB source.SourceDB,
	allTables bool,
	targetTables []string,
) ([][]string, error) {
	// Get table dependencies only once
	allTableList, dependencies, err := sourceDB.GetTableDependencies(ctx)
	if err != nil {
		return nil, err
	}

	// Perform topological sort
	orderedTables, err := topologicalSort(allTableList, dependencies)
	if err != nil {
		log.Printf("WARNING: Circular dependency detected, falling back to alphabetical order: %v", err)
		orderedTables = allTableList
	}

	// Filter tables based on user selection
	if !allTables && len(targetTables) > 0 {
		tableMap := make(map[string]bool)
		for _, t := range orderedTables {
			tableMap[strings.ToLower(t)] = true
		}

		targetMap := make(map[string]bool)
		for _, t := range targetTables {
			lowerT := strings.ToLower(t)
			if !tableMap[lowerT] {
				return nil, fmt.Errorf("table %s does not exist", t)
			}
			targetMap[lowerT] = true
		}

		var filteredTables []string
		for _, t := range orderedTables {
			if targetMap[strings.ToLower(t)] {
				filteredTables = append(filteredTables, t)
			}
		}
		orderedTables = filteredTables
	} else if !allTables {
		return nil, fmt.Errorf("no tables specified")
	}

	// Calculate dependency levels
	levels := make(map[string]int)
	for _, table := range orderedTables {
		levels[strings.ToLower(table)] = calculateLevel(strings.ToLower(table), dependencies, levels)
	}

	// Group tables by level
	maxLevel := 0
	for _, level := range levels {
		if level > maxLevel {
			maxLevel = level
		}
	}

	result := make([][]string, maxLevel+1)
	for i := range result {
		result[i] = []string{}
	}

	for _, table := range orderedTables {
		level := levels[strings.ToLower(table)]
		result[level] = append(result[level], table)
	}

	return result, nil
}

// calculateLevel recursively calculates the dependency level of a table
func calculateLevel(table string, dependencies map[string][]string, cache map[string]int) int {
	if level, exists := cache[table]; exists {
		return level
	}

	deps := dependencies[table]
	if len(deps) == 0 {
		cache[table] = 0
		return 0
	}

	maxDepLevel := -1
	for _, dep := range deps {
		depLevel := calculateLevel(dep, dependencies, cache)
		if depLevel > maxDepLevel {
			maxDepLevel = depLevel
		}
	}

	level := maxDepLevel + 1
	cache[table] = level
	return level
}

// topologicalSort performs topological sorting on tables based on dependencies
func topologicalSort(tables []string, dependencies map[string][]string) ([]string, error) {
	// Create maps for tracking
	inDegree := make(map[string]int)
	adjList := make(map[string][]string)

	// Initialize
	for _, table := range tables {
		lowerTable := strings.ToLower(table)
		inDegree[lowerTable] = 0
		adjList[lowerTable] = []string{}
	}

	// Build adjacency list and calculate in-degrees
	for dependent, refs := range dependencies {
		for _, referenced := range refs {
			// referenced must come before dependent
			adjList[referenced] = append(adjList[referenced], dependent)
			inDegree[dependent]++
		}
	}

	// Find all tables with no dependencies
	queue := []string{}
	for _, table := range tables {
		lowerTable := strings.ToLower(table)
		if inDegree[lowerTable] == 0 {
			queue = append(queue, table)
		}
	}

	result := []string{}
	originalNameMap := make(map[string]string)
	for _, table := range tables {
		originalNameMap[strings.ToLower(table)] = table
	}

	for len(queue) > 0 {
		// Pop from queue
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		currentLower := strings.ToLower(current)

		// Process all tables that depend on current
		for _, dependent := range adjList[currentLower] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, originalNameMap[dependent])
			}
		}
	}

	// Check for circular dependencies
	if len(result) != len(tables) {
		return nil, fmt.Errorf("circular dependency detected: got %d tables, expected %d", len(result), len(tables))
	}

	return result, nil
}
