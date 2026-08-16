package config

import "reflect"

// NonReloadableChanges lists the configuration sections that changed but cannot
// be applied without a restart. The instance ID is captured by QueryManager at
// construction, the MetaStore and the storage backends are built once in main,
// the listeners are bound once, and the concurrency semaphore is sized once.
// Reporting them as ignored is more honest than a reload that claims success
// and silently keeps the old values (spec §8).
func NonReloadableChanges(oldCfg, newCfg *Config) []string {
	if oldCfg == nil || newCfg == nil {
		return nil
	}

	var ignored []string
	if !reflect.DeepEqual(oldCfg.Instance, newCfg.Instance) {
		ignored = append(ignored, "instance")
	}
	if !reflect.DeepEqual(oldCfg.Server, newCfg.Server) {
		ignored = append(ignored, "server")
	}
	if !reflect.DeepEqual(oldCfg.Storage, newCfg.Storage) {
		ignored = append(ignored, "storage")
	}
	if oldCfg.Defaults.MaxConcurrentQueries != newCfg.Defaults.MaxConcurrentQueries {
		ignored = append(ignored, "defaults.max_concurrent_queries")
	}
	return ignored
}

// DatabaseDiff outlines changes in database configurations.
type DatabaseDiff struct {
	Added   []DatabaseConfig
	Removed []DatabaseConfig
	Updated []DatabaseConfig
}

// DiffDatabases compares two Config snapshots and computes differences in target databases.
func DiffDatabases(oldCfg, newCfg *Config) DatabaseDiff {
	diff := DatabaseDiff{}
	if oldCfg == nil {
		if newCfg != nil {
			diff.Added = append(diff.Added, newCfg.Databases...)
		}
		return diff
	}
	if newCfg == nil {
		diff.Removed = append(diff.Removed, oldCfg.Databases...)
		return diff
	}

	oldMap := make(map[string]DatabaseConfig)
	for _, db := range oldCfg.Databases {
		oldMap[db.ID] = db
	}

	newMap := make(map[string]DatabaseConfig)
	for _, db := range newCfg.Databases {
		newMap[db.ID] = db
	}

	for id, db := range newMap {
		oldDb, exists := oldMap[id]
		if !exists {
			diff.Added = append(diff.Added, db)
		} else if oldDb.Engine != db.Engine || oldDb.DSN != db.DSN || oldDb.MaxConns != db.MaxConns {
			diff.Updated = append(diff.Updated, db)
		}
	}

	for id, db := range oldMap {
		if _, exists := newMap[id]; !exists {
			diff.Removed = append(diff.Removed, db)
		}
	}

	return diff
}
