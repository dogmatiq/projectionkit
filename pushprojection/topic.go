package pushprojection

import "sync"

type Topic[T any] struct {
	partitions *sync.Map
	sub, unsub chan subscriber[T]
	closed     chan struct{}
}

func (t *Topic[T]) Subscribe(partition string, ch chan<- Delivery[T]) {
	sub := subscriber[T]{partition, ch}

	select {
	case t.sub <- sub:
	case <-t.closed:
		panic("topic is closed")
	}
}

func (t *Topic[T]) Unsubscribe(partition string, ch chan<- Delivery[T]) {
	sub := subscriber[T]{partition, ch}

	select {
	case t.unsub <- sub:
	case <-t.closed:
		panic("topic is closed")
	}
}

func (t *Topic[T]) Close() {
	close(t.closed)
}

func (t *Topic[T]) run() {
	for {
		select {
		case sub := <-t.sub:
		case sub := <-t.unsub:
		case up := <-t.updates:
		case <-t.closed:
			return
		}
	}
}

type partition[T any] struct {
	id      string
	state   T
	updates <-chan Update[T]
	closed  chan struct{}

	sub, unsub       chan chan<- Delivery[T]
	synced, desynced []chan<- Delivery[T]
}

type subscriber[T any] struct {
	partition string
	channel   chan<- Delivery[T]
}

// A Delivery encapsulates a change to the state of a partition, either as a
// snapshot of the entire state, or a set of incremental updates.
type Delivery[T any] struct {
	partition string
	snapshot  *T
	updates   []Update[T]
}

// Partition returns the identity of the partition that the delivery relates to.
func (d Delivery[T]) Partition() string {
	return d.partition
}

// Snapshot returns the entire state of the partition.
//
// If ok is true the snapshot is valid and the caller should replace its
// existing state with the snapshot.
//
// If ok is false, no snapshot is available and the caller should apply the
// updates returned by Updates() to its existing state.
func (d Delivery[T]) Snapshot() (_ T, ok bool) {
	if d.snapshot == nil {
		var zero T
		return zero, false
	}
	return *d.snapshot, true
}

// Updates returns the incremental updates to the partition's state, if any.
//
// There may be incremental updates in the same delivery as a snapshot.
func (d Delivery[T]) Updates() []Update[T] {
	return d.updates
}
