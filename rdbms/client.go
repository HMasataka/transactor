package rdbms

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/HMasataka/transactor"
)

type contextTransactionKey string

var defaultShardKeyProvider = func(ctx context.Context) string {
	return "rdbms"
}

func contextKey(shardKey string) contextTransactionKey {
	return contextTransactionKey(fmt.Sprintf("current_%s_tx", shardKey))
}

type Client interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type ClientProvider interface {
	CurrentClient(ctx context.Context) Client
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

func (p *clientProvider) CurrentClient(ctx context.Context) Client {
	key := contextKey(p.shardKeyProvider(ctx))
	transaction := ctx.Value(key)
	if transaction == nil {
		return p.connectionProvider.CurrentConnection(ctx)
	}
	return transaction.(Client)
}
