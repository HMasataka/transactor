package redis

import (
	"context"

	"github.com/HMasataka/transactor"

	"github.com/redis/go-redis/v9"
)

type Transactor struct {
	shardKeyProvider   transactor.ShardKeyProvider
	connectionProvider ConnectionProvider
}

func NewTransactor(connectionProvider ConnectionProvider) *Transactor {
	return NewShardingTransactor(connectionProvider, defaultShardKeyProvider)
}

func NewShardingTransactor(connectionProvider ConnectionProvider, shardKeyProvider transactor.ShardKeyProvider) *Transactor {
	return &Transactor{
		shardKeyProvider:   shardKeyProvider,
		connectionProvider: connectionProvider,
	}
}

func (t *Transactor) Required(ctx context.Context, fn transactor.DoInTransaction, options ...transactor.Option) error {
	if ctx.Value(contextKey(t.shardKeyProvider(ctx))) != nil {
		return fn(ctx)
	}
	return t.RequiresNew(ctx, fn, options...)
}

func (t *Transactor) RequiresNew(ctx context.Context, fn transactor.DoInTransaction, options ...transactor.Option) error {
	config := transactor.NewDefaultConfig()
	for _, opt := range options {
		opt.Apply(&config)
	}

	//TODO support optimistic locking if needed.
	redisClient := t.connectionProvider.CurrentConnection(ctx)
	_, err := redisClient.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		ctx = context.WithValue(ctx, contextKey(t.shardKeyProvider(ctx)), pipe)
		err := fn(ctx)
		if err != nil || config.RollbackOnly {
			pipe.Discard()
		}
		return err
	})

	return err
}
