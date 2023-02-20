package rdbms

import (
	"context"
	"database/sql"

	"github.com/HMasataka/transactor"
)

type ConnectionProvider interface {
	CurrentConnection(ctx context.Context) Conn
}

type connectionProvider struct {
	db Conn
}

func NewConnectionProvider(db Conn) ConnectionProvider {
	return &connectionProvider{
		db: db,
	}
}

func (p *connectionProvider) CurrentConnection(_ context.Context) Conn {
	return p.db
}

type ShardConnectionProvider struct {
	shardKeyProvider transactor.ShardKeyProvider
	db               []*sql.DB
	maxSlot          uint32
}

func NewShardingConnectionProvider(db []*sql.DB, shardKeyProvider transactor.ShardKeyProvider) ConnectionProvider {
	return &ShardConnectionProvider{
		db:               db,
		shardKeyProvider: shardKeyProvider,
		maxSlot:          uint32(len(db)),
	}
}

func (p *ShardConnectionProvider) CurrentConnection(ctx context.Context) Conn {
	shardKey := p.shardKeyProvider(ctx)
	index := transactor.GetShardingIndex([]byte(shardKey), p.maxSlot)
	return p.db[index]
}

type Conn interface {
	Client
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}
