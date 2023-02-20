package redis

import (
	"context"
	"fmt"

	"github.com/HMasataka/transactor"
	"github.com/redis/go-redis/v9"
)

type contextTransactionKey string

var defaultShardKeyProvider = func(ctx context.Context) string {
	return "redis"
}

func contextKey(shardKey string) contextTransactionKey {
	return contextTransactionKey(fmt.Sprintf("current_%s_tx", shardKey))
}

type ClientProvider interface {
	CurrentClient(ctx context.Context) (reader redis.Cmdable, writer redis.Cmdable)
}

type clientProvider struct {
	shardKeyProvider   transactor.ShardKeyProvider
	connectionProvider ConnectionProvider
}

func NewClientProvider(connectionProvider ConnectionProvider) ClientProvider {
	return NewShardingClientProvider(connectionProvider, defaultShardKeyProvider)
}

func NewShardingClientProvider(connectionProvider ConnectionProvider, shardKeyProvider transactor.ShardKeyProvider) ClientProvider {
	return &clientProvider{
		shardKeyProvider:   shardKeyProvider,
		connectionProvider: connectionProvider,
	}
}

func (p *clientProvider) CurrentClient(ctx context.Context) (reader redis.Cmdable, writer redis.Cmdable) {
	client := p.connectionProvider.CurrentConnection(ctx)
	transaction := ctx.Value(contextKey(p.shardKeyProvider(ctx)))
	if transaction == nil {
		return client, client
	}
	return client, transaction.(redis.Cmdable)
}
