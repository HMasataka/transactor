package transactor

import (
	"context"
)

type DoInTransaction func(ctx context.Context) error

type Transactor interface {
	// Support a current transaction, create a new one if not exists.
	Required(ctx context.Context, fn DoInTransaction, options ...Option) error
	// Create a new transaction, and suspend the current transaction if one exists.
	RequiresNew(ctx context.Context, fn DoInTransaction, options ...Option) (err error)
}
