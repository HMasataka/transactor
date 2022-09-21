package transactor

import (
	"context"
	"database/sql"
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

type ShardKeyProvider func(ctx context.Context) string

type ShardConnectionProvider struct {
	shardKeyProvider ShardKeyProvider
	db               []*sql.DB
	maxSlot          uint32
}

func NewShardConnectionProvider(db []*sql.DB, maxSlot uint32, shardKeyProvider ShardKeyProvider) ConnectionProvider {
	return &ShardConnectionProvider{
		db:               db,
		shardKeyProvider: shardKeyProvider,
		maxSlot:          maxSlot,
	}
}

func (p *ShardConnectionProvider) CurrentConnection(ctx context.Context) Conn {
	shardKey := p.shardKeyProvider(ctx)
	index := GetShardingIndex([]byte(shardKey), p.maxSlot)

	return p.db[index]
}

type Conn interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}
