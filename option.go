package transactor

type Config struct {
	VendorOption interface{}
	ReadOnly     bool
	RollbackOnly bool
}

func NewDefaultConfig() Config {
	return Config{
		ReadOnly:     false,
		RollbackOnly: false,
	}
}

type Option interface {
	Apply(*Config)
}

type ReadOnly bool

func (o ReadOnly) Apply(c *Config) {
	c.ReadOnly = bool(o)
}

func OptionReadOnly() ReadOnly {
	return true
}

type RollbackOnly bool

func (o RollbackOnly) Apply(c *Config) {
	c.RollbackOnly = bool(o)
}

func OptionRollbackOnly() RollbackOnly {
	return true
}
