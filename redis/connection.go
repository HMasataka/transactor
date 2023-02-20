package redis

import (
	"context"

	"github.com/HMasataka/transactor"
	"github.com/redis/go-redis/v9"
)

type ConnectionProvider interface {
	CurrentConnection(ctx context.Context) *redis.Client
}

func NewConnectionProvider(client *redis.Client) ConnectionProvider {
	return &connectionProvider{
		client: client,
	}
}

type connectionProvider struct {
	client *redis.Client
}

func (p *connectionProvider) CurrentConnection(_ context.Context) *redis.Client {
	return p.client
}

type ShardingConnectionProvider struct {
	db               []*redis.Client
	shardKeyProvider transactor.ShardKeyProvider
	maxSlot          uint32
}

func NewShardingConnectionProvider(db []*redis.Client, shardKeyProvider transactor.ShardKeyProvider) ConnectionProvider {
	return &ShardingConnectionProvider{
		db:               db,
		shardKeyProvider: shardKeyProvider,
		maxSlot:          uint32(len(db)),
	}
}

func (p *ShardingConnectionProvider) CurrentConnection(ctx context.Context) *redis.Client {
	shardKey := p.shardKeyProvider(ctx)
	index := transactor.GetShardingIndex([]byte(shardKey), p.maxSlot)
	return p.db[index]
}
