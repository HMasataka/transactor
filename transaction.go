package transactor

import (
	"context"
	"database/sql"
	"fmt"
)

type contextTransactionKey string

var defaultShardKeyProvider = func(ctx context.Context) string {
	return "rdbms"
}

func contextKey(shardKey string) contextTransactionKey {
	return contextTransactionKey(fmt.Sprintf("current_%s_tx", shardKey))
}

type DoInTransaction func(ctx context.Context) error

type Transactor interface {
	// Support a current transaction, create a new one if none exists.
	Required(ctx context.Context, fn DoInTransaction, options ...Option) error
	// Create a new transaction, and suspend the current transaction if one exists.
	RequiresNew(ctx context.Context, fn DoInTransaction, options ...Option) (err error)
}

type transactor struct {
	shardKeyProvider   ShardKeyProvider
	connectionProvider ConnectionProvider
}

func NewTransactor(connectionProvider ConnectionProvider) Transactor {
	return NewShardTransactor(connectionProvider, defaultShardKeyProvider)
}

func NewShardTransactor(connectionProvider ConnectionProvider, shardKeyProvider ShardKeyProvider) Transactor {
	return &transactor{
		shardKeyProvider:   shardKeyProvider,
		connectionProvider: connectionProvider,
	}
}

func (t *transactor) Required(ctx context.Context, fn DoInTransaction, options ...Option) error {
	if ctx.Value(contextKey(t.shardKeyProvider(ctx))) != nil {
		return fn(ctx)
	}

	return t.RequiresNew(ctx, fn, options...)
}

func (t *transactor) RequiresNew(ctx context.Context, fn DoInTransaction, options ...Option) (err error) {
	config := NewDefaultConfig()
	for _, opt := range options {
		opt.Apply(&config)
	}

	db := t.connectionProvider.CurrentConnection(ctx)

	tx, err := db.BeginTx(ctx, &sql.TxOptions{
		ReadOnly: config.ReadOnly,
	})
	if err != nil {
		return err
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()

			panic(p)
		} else if err != nil {
			_ = tx.Rollback()
		} else {
			if config.RollbackOnly {
				_ = tx.Rollback()
			} else {
				err = tx.Commit()
			}
		}
	}()

	err = fn(context.WithValue(ctx, contextKey(t.shardKeyProvider(ctx)), tx))

	return
}
