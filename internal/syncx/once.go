package syncx

import (
	"context"
	"sync"
	"sync/atomic"
)

// SucceedOnce is a [sync.Once] variant that allows for the operation to fail.
type SucceedOnce struct {
	fast atomic.Bool
	slow sync.Mutex
}

// Do executes the fn if and only if it has not been called successfully before.
func (o *SucceedOnce) Do(
	ctx context.Context,
	fn func(ctx context.Context) error,
) error {
	if o.fast.Load() {
		return nil
	}

	o.slow.Lock()
	defer o.slow.Unlock()

	if !o.fast.Load() {
		if err := fn(ctx); err != nil {
			return err
		}

		o.fast.Store(true)
	}

	return nil
}
