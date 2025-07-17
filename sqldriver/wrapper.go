package sqldriver

import (
	"database/sql"
	"time"
)

// DB wraps a *sql.DB with default settings.
type DB struct{ *sql.DB }

// New opens the database and applies pool settings.
func New(opts ...Option) (*DB, error) {
	// defaults
	cfg := &config{maxOpen: 10, maxIdle: 5}
	for _, o := range opts {
		o(cfg)
	}

	db, err := sql.Open(cfg.driverName, cfg.dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(cfg.maxOpen)
	db.SetMaxIdleConns(cfg.maxIdle)
	db.SetConnMaxLifetime(30 * time.Minute)
	return &DB{db}, nil
}
