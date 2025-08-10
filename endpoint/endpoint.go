package endpoint

import "context"

type Endpoint func(ctx context.Context, request any) (response any, err error)
