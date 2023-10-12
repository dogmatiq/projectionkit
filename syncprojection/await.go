package syncprojection

import "context"

type Awaitable[S any] interface {
	Await(ctx context.Context, instanceID string, conditions ...func(S) bool) error
}

type waiter[S any] struct {
	conditions []func(S) bool
	satisfied  chan<- struct{}
}

func (a *adaptor[S]) Await(
	ctx context.Context,
	instanceID string,
	conditions ...func(S) bool,
) error {
	satisfied := make(chan struct{})

	cancel := a.watch(instanceID, satisfied, conditions)
	defer cancel()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-satisfied:
		return nil
	}
}

func (a *adaptor[S]) watch(
	instanceID string,
	satisfied chan<- struct{},
	conditions []func(S) bool,
) func() {
	a.m.Lock()
	defer a.m.Unlock()

	inst := a.instance(instanceID)

	if inst.observers == nil {
		inst.observers = map[chan<- struct{}]struct{}{}
	}
	inst.observers[satisfied] = struct{}{}

	return func() {
		a.m.Lock()
		delete(inst.observers, satisfied)
		a.m.Unlock()
	}
}
