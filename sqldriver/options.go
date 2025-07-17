package sqldriver

// Option configures the wrapper (driver name, DSN, pool size…).
type Option func(*config)

type config struct {
	driverName string
	dsn        string
	maxOpen    int
	maxIdle    int
}

// WithDriverName sets the SQL driver name (e.g. "mysql", "postgres").
func WithDriverName(name string) Option {
	return func(c *config) { c.driverName = name }
}

// WithDSN sets the connection URI.
func WithDSN(dsn string) Option {
	return func(c *config) { c.dsn = dsn }
}

// WithMaxOpenConns tunes the open-connections pool.
func WithMaxOpenConns(n int) Option {
	return func(c *config) { c.maxOpen = n }
}

// WithMaxIdleConns tunes the idle-connections pool.
func WithMaxIdleConns(n int) Option {
	return func(c *config) { c.maxIdle = n }
}
