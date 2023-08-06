package rdbms

import (
	"context"
	"database/sql"

	"github.com/HMasataka/transactor"
)

type tx struct {
	shardKeyProvider   transactor.ShardKeyProvider
	connectionProvider ConnectionProvider
}

func NewTransactor(connectionProvider ConnectionProvider) transactor.Transactor {
	return NewShardingTransactor(connectionProvider, defaultShardKeyProvider)
}

func NewShardingTransactor(connectionProvider ConnectionProvider, shardKeyProvider transactor.ShardKeyProvider) transactor.Transactor {
	return &tx{
		shardKeyProvider:   shardKeyProvider,
		connectionProvider: connectionProvider,
	}
}

func (t *tx) Required(ctx context.Context, fn transactor.DoInTransaction, options ...transactor.Option) error {
	if ctx.Value(contextKey(t.shardKeyProvider(ctx))) != nil {
		return fn(ctx)
	}
	return t.RequiresNew(ctx, fn, options...)
}

func (t *tx) RequiresNew(ctx context.Context, fn transactor.DoInTransaction, options ...transactor.Option) error {
	config := transactor.NewDefaultConfig()
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
	return err
}
