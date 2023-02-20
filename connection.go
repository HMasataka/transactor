package transactor

import (
	"context"
)

type ShardKeyProvider func(ctx context.Context) string
