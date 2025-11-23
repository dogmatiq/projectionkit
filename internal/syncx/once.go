package syncx

import (
	"context"
	"sync"
	"sync/atomic"
)

// SucceedOnce is a [sync.Once] variant that allows for the operation to fail.
type SucceedOnce struct {
	done atomic.Bool
	m    sync.Mutex
}

// Do executes the fn if and only if it has not been called successfully before.
func (o *SucceedOnce) Do(
	ctx context.Context,
	fn func(ctx context.Context) error,
) error {
	if o.done.Load() {
		return nil
	}

	o.m.Lock()
	defer o.m.Unlock()

	if o.done.Load() {
		return nil
	}

	if err := fn(ctx); err != nil {
		return err
	}

	o.done.Store(true)

	return nil
}
