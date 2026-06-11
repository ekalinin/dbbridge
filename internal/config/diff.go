package config

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
