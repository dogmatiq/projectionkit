package pushprojection

import (
	"bytes"
	"context"
	"sync"

	"github.com/dogmatiq/dogma"
)

// type Marshalable interface {
// 	encoding.BinaryMarshaler
// 	encoding.BinaryUnmarshaler
// }

func New[H MessageHandler[S], S any](
	handler H,
) (Topic[S], dogma.ProjectionMessageHandler) {
	a := &adaptor[S]{
		handler: handler,
	}
	return a, a
}

type adaptor[S any] struct {
	dogma.NoTimeoutHintBehavior
	dogma.NoCompactBehavior

	handler MessageHandler[S]

	m         sync.Mutex
	instances map[string]*instance[S]
	resources map[string][]byte
}

type instance[S any] struct {
	state     S
	observers map[chan<- struct{}]struct{}
}

func (a *adaptor[S]) instance(id string) *instance[S] {
	inst, ok := a.instances[id]
	if !ok {
		inst = &instance[S]{
			state: a.handler.New(),
		}
		a.instances[id] = inst
	}
	return inst
}

var _ dogma.ProjectionMessageHandler = (*adaptor[any])(nil)

func (a *adaptor[S]) Configure(c dogma.ProjectionConfigurer) {
	c.DeliveryPolicy(
		dogma.BroadcastProjectionDeliveryPolicy{
			PrimaryFirst: true,
		},
	)
	a.handler.Configure(c)
}

func (a *adaptor[S]) HandleEvent(
	ctx context.Context,
	r, c, n []byte,
	s dogma.ProjectionEventScope,
	e dogma.Event,
) (bool, error) {
	a.m.Lock()
	defer a.m.Unlock()

	v := a.resources[string(r)]
	if !bytes.Equal(c, v) {
		return false, nil
	}

	if id, ok := a.handler.RouteEventToInstance(e); ok {
		inst := a.instance(id)

		a.handler.HandleEvent(inst.state, s, e)

		for obs := range inst.observers {
			select {
			case obs <- struct{}{}:
			default:
			}
		}
	}

	if a.resources == nil {
		a.resources = map[string][]byte{}
	}
	a.resources[string(r)] = n

	return true, nil
}

func (a *adaptor[S]) ResourceVersion(_ context.Context, r []byte) ([]byte, error) {
	a.m.Lock()
	defer a.m.Unlock()

	v := a.resources[string(r)]
	return v, nil
}

func (a *adaptor[S]) CloseResource(_ context.Context, r []byte) error {
	a.m.Lock()
	defer a.m.Unlock()

	delete(a.resources, string(r))
	return nil
}
