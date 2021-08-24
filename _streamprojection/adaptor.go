package streamprojection

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/projectionkit/resource"
)

type adaptor struct {
	handler MessageHandler
	repo    Repository
}

func New(r Repository, h MessageHandler) (dogma.ProjectionMessageHandler, *Consumer) {
	a := &adaptor{
		handler: h,
		repo:    r,
	}

	c := &Consumer{}

	return a, c
}

// Configure produces a configuration for this handler by calling methods on
// the configurer c.
func (a *adaptor) Configure(c dogma.ProjectionConfigurer) {
	a.handler.Configure(c)
}

// HandleEvent updates the projection to reflect the occurrence of an event.
func (a *adaptor) HandleEvent(
	ctx context.Context,
	r, c, n []byte,
	s dogma.ProjectionEventScope,
	m dogma.Message,
) (bool, error) {
	streamIDs := a.handler.RouteEventToStreams(m)
	updates := make([]StreamState, 0, len(streamIDs))

	for _, id := range streamIDs {
		ss, ok, err := a.handleEventForStream(ctx, id, s, m)
		if err != nil {
			return false, err
		}

		if ok {
			updates = append(updates, ss)
		}
	}

	return a.repo.Save(ctx, r, c, n, updates)
}

// handleEventForStream handles a Dogma event for one of the streams it has been
// routed to.
//
// If ok is true, ss is added to the list of snapshots to be saved to the
// repository.
func (a *adaptor) handleEventForStream(
	ctx context.Context,
	id string,
	s dogma.ProjectionEventScope,
	m dogma.Message,
) (ss StreamState, ok bool, _ error) {
	ss, err := a.repo.Load(ctx, id)
	if err != nil {
		return StreamState{}, false, err
	}

	sn := a.handler.New()

	if ss.Version != 0 {
		if err := sn.UnmarshalBinary(ss.Snapshot); err != nil {
			return StreamState{}, false, err
		}
	}

	sc := &scope{
		ProjectionEventScope: s,
		id:                   id,
		snapshot:             sn,
	}

	a.handler.HandleEvent(sn, sc, m)

	if sc.closed {
		ss.IsClosed = true
		return ss, true, nil
	}

	if len(sc.events) != 0 {
		ss.Snapshot, err = sn.MarshalBinary()
		return ss, true, err
	}

	return StreamState{}, false, err
}

// ResourceVersion returns the version of the resource r.
func (a *adaptor) ResourceVersion(ctx context.Context, r []byte) ([]byte, error) {
	return a.repo.ResourceVersion(ctx, r)
}

// CloseResource informs the projection that the resource r will not be
// used in any future calls to HandleEvent().
func (a *adaptor) CloseResource(ctx context.Context, r []byte) error {
	return a.repo.DeleteResource(ctx, r)
}

// TimeoutHint returns a duration that is suitable for computing a deadline
// for the handling of the given message by this handler.
func (a *adaptor) TimeoutHint(m dogma.Message) time.Duration {
	return 0
}

// Compact reduces the size of the projection's data.
func (a *adaptor) Compact(ctx context.Context, s dogma.ProjectionCompactScope) error {
	return errors.New("not implemented")
}

// ResourceRepository returns a repository that can be used to manipulate the
// handler's resource versions.
func (a *adaptor) ResourceRepository() resource.Repository {
	return a.repo
}
